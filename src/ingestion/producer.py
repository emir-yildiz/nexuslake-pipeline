"""
ASYNCHRONOUS E-COMMERCE EVENT PRODUCER
---------------------------------------
Yüksek ölçekli e-ticaret olaylarını asenkron olarak Kafka'ya aktaran ana modül.
Yapılandırma dosyasını dinamik olarak yükler, aiokafka ile non-blocking gönderim,
exponential backoff ile yeniden bağlanma ve Pydantic ile tip güvenliği sağlar.
"""

import asyncio
import json
import random
import yaml
import os
from uuid import uuid4
from datetime import datetime, timezone
from pathlib import Path
from contextlib import asynccontextmanager
from typing import Literal

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError
from faker import Faker
from fastapi import FastAPI
from pydantic import BaseModel, Field
import uvicorn

from src.common.logger import get_logger

logger = get_logger("EventProducer")
fake = Faker()

# ─── Sabitler ────────────────────────────────────────────────────────────────

CONFIG_PATH = Path(os.getenv("KAFKA_CONFIG_PATH", "/app/config/kafka_config.yaml"))
CONFIG_FALLBACK = Path(__file__).resolve().parent.parent.parent / "config" / "kafka_config.yaml"

MIN_SEND_INTERVAL = 0.1   # saniye
MAX_SEND_INTERVAL = 0.8   # saniye
MAX_RETRY_ATTEMPTS = 5
BASE_BACKOFF_SECONDS = 2

# ─── Veri Modeli ─────────────────────────────────────────────────────────────

EventType = Literal["view_product", "add_to_cart", "purchase", "remove_from_cart"]
Platform  = Literal["mobile_ios", "mobile_android", "web"]


class EcommerceEvent(BaseModel):
    event_id:   str       = Field(default_factory=lambda: str(uuid4()))
    user_id:    int       = Field(ge=1000, le=9999)
    product_id: int       = Field(ge=100,  le=500)
    event_type: EventType
    price:      float     = Field(ge=10.5, le=500.0)
    timestamp:  str
    platform:   Platform
    session_id: str       = Field(default_factory=lambda: str(uuid4()))

    def to_bytes(self) -> bytes:
        return self.model_dump_json().encode("utf-8")


class EventGenerator:
    """Gerçekçi e-ticaret verisi üretimi."""

    @staticmethod
    def generate() -> EcommerceEvent:
        return EcommerceEvent(
            user_id=random.randint(1000, 9999),
            product_id=random.randint(100, 500),
            event_type=random.choice(["view_product", "add_to_cart", "purchase", "remove_from_cart"]),
            price=round(random.uniform(10.5, 500.0), 2),
            timestamp=datetime.now(timezone.utc).isoformat(),
            platform=random.choice(["mobile_ios", "mobile_android", "web"]),
        )

# ─── Yapılandırma Yükleyici ───────────────────────────────────────────────────

def load_config() -> dict:
    """
    Kafka yapılandırmasını yükler.
    Önce KAFKA_CONFIG_PATH env değişkenine, bulamazsa fallback yola bakar.
    """
    path = CONFIG_PATH if CONFIG_PATH.exists() else CONFIG_FALLBACK

    if not path.exists():
        raise FileNotFoundError(
            f"Yapılandırma dosyası bulunamadı: {CONFIG_PATH} veya {CONFIG_FALLBACK}"
        )

    with open(path, "r") as f:
        config = yaml.safe_load(f)

    logger.info(f"Yapılandırma dosyası yüklendi: {path}")
    return config

# ─── Producer Döngüsü ─────────────────────────────────────────────────────────

async def _produce_loop(producer: AIOKafkaProducer, topic: str) -> None:
    """Kafka'ya 100 adet event gönderir ve durur."""
    RECORD_LIMIT = 100
    sent_count = 0

    logger.info(f"Kafka Producer aktif. {RECORD_LIMIT} kayıt gönderimi başlıyor...")

    while sent_count < RECORD_LIMIT:
        event = EventGenerator.generate()

        # send_and_wait: broker onayını bekler
        await producer.send_and_wait(topic, event.to_bytes())

        sent_count += 1

        logger.info(
            f"[{sent_count}/{RECORD_LIMIT}] Gönderim başarılı | "
            f"event_id={event.event_id} | "
            f"type={event.event_type}"
        )

        # Son kayıttan sonra uyumaya gerek yok
        if sent_count < RECORD_LIMIT:
            await asyncio.sleep(random.uniform(MIN_SEND_INTERVAL, MAX_SEND_INTERVAL))

    logger.info(f"Limit doldu ({RECORD_LIMIT} kayıt). Gönderim döngüsü tamamlandı.")


async def run_producer() -> None:
    """
    Kafka producer ana döngüsü.
    Bağlantı hatalarında exponential backoff ile yeniden dener.
    """
    config = load_config()

    bootstrap_servers = config["kafka"]["bootstrap_servers"]
    topic             = config["kafka"]["topics"]["raw_events"]
    acks              = config["kafka"]["producer"]["acks"]

    for attempt in range(1, MAX_RETRY_ATTEMPTS + 1):
        producer: AIOKafkaProducer | None = None
        try:
            producer = AIOKafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: v,   # Bytes doğrudan gönderilir
                acks=acks
            )

            logger.info(f"Kafka'ya bağlanılıyor: {bootstrap_servers} (deneme {attempt}/{MAX_RETRY_ATTEMPTS})")
            await producer.start()
            logger.info("Kafka bağlantısı kuruldu.")

            await _produce_loop(producer, topic)
            break  # Döngü sadece CancelledError ile çıkar; başarılı çıkışta retry gerekmez

        except asyncio.CancelledError:
            logger.info("Producer task iptal sinyali alındı.")
            raise  # Lifespan'in temiz kapanabilmesi için yeniden fırlat

        except (KafkaConnectionError, Exception) as exc:
            wait = BASE_BACKOFF_SECONDS ** attempt
            logger.warning(
                f"Bağlantı/gönderim hatası (deneme {attempt}/{MAX_RETRY_ATTEMPTS}): "
                f"{exc!r} — {wait}s sonra yeniden deneniyor..."
            )
            await asyncio.sleep(wait)

        finally:
            if producer:
                await producer.stop()
                logger.info("Kafka Producer güvenli şekilde durduruldu.")

    else:
        logger.critical(
            f"{MAX_RETRY_ATTEMPTS} denemeden sonra Kafka'ya bağlanılamadı. "
            "Producer kalıcı olarak durduruluyor."
        )

# ─── FastAPI Lifespan ─────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    producer_task: asyncio.Task | None = None
    try:
        logger.info("Lifespan: Producer task başlatılıyor...")
        producer_task = asyncio.create_task(run_producer(), name="kafka-producer")

        # Event loop'a Producer'ın en az bir adım atması için fırsat ver
        await asyncio.sleep(0)

        if producer_task.done():
            exc = producer_task.exception()
            logger.critical(f"Producer task başlatılamadı: {exc!r}")
            raise RuntimeError("Kafka Producer başlatılamadı.") from exc

        logger.info("Producer task event loop'ta aktif.")
        yield

    finally:
        if producer_task and not producer_task.done():
            logger.info("Lifespan: Uygulama kapatılıyor, task iptal ediliyor...")
            producer_task.cancel()
            try:
                await producer_task
            except asyncio.CancelledError:
                pass
        logger.info("Lifespan: Temiz kapanış tamamlandı.")

# ─── Uygulama ─────────────────────────────────────────────────────────────────

app = FastAPI(title="E-Commerce Event Producer", lifespan=lifespan)


@app.get("/health")
async def health_check():
    """Producer'ın ayakta olup olmadığını döner."""
    tasks = {t.get_name(): not t.done() for t in asyncio.all_tasks()}
    return {
        "status": "ok",
        "producer_running": tasks.get("kafka-producer", False),
    }


if __name__ == "__main__":
    uvicorn.run(
        "src.ingestion.producer:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
    )
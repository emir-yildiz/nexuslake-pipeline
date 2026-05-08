"""
BRONZE SERVICE - API WRAPPER
"""

import asyncio
import uvicorn
from contextlib import asynccontextmanager
from typing import Literal
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from src.bronze.bronze_layer import process_bronze
from src.common.logger import get_logger

logger = get_logger("BronzeService")

streaming_queries = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("NexusLake Bronze Ingestor Servisi Başlatılıyor...")
    yield
    logger.info("Servis kapatılıyor, aktif streamler durduruluyor...")
    for name, query in streaming_queries.items():
        if query.isActive:
            logger.info(f"Durduruluyor: {name}")
            query.stop()
    logger.info("Tüm streamler durduruldu.")


app = FastAPI(title="NexusLake Bronze Ingestor", lifespan=lifespan)


# ─── Modeller ────────────────────────────────────────────────────────────────

class RunRequest(BaseModel):
    # DÜZELTME: Literal ile Pydantic validasyonu — geçersiz değer 422 döner,
    # Spark'a ulaşmadan erken yakalanır.
    trigger_mode: Literal["availableNow", "processingTime"] = "availableNow"


# ─── Endpointler ─────────────────────────────────────────────────────────────

@app.post("/run")
async def start_ingestion(request: RunRequest):
    """Kafka'daki mevcut veriyi işlemek üzere Spark job'ını tetikler."""
    query = streaming_queries.get("bronze_query")
    if query and query.isActive:
        return {
            "status": "ALREADY_RUNNING",
            "query_id": str(query.id),
            # DÜZELTME: ALREADY_RUNNING'de de mode bilgisi dönüyor
            "mode": request.trigger_mode,
        }

    try:
        # DÜZELTME: process_bronze() blocking → executor'da çalıştır
        loop = asyncio.get_event_loop()
        query = await loop.run_in_executor(
            None,
            lambda: process_bronze(trigger_mode=request.trigger_mode)
        )
        streaming_queries["bronze_query"] = query

        logger.info(f"Job başlatıldı. ID: {query.id} | Mode: {request.trigger_mode}")
        return {
            "status": "STARTED",
            # DÜZELTME: str() cast — UUID JSON serialize edilemez
            "query_id": str(query.id),
            "mode": request.trigger_mode,
        }

    except Exception as e:
        logger.error(f"Job başlatılamadı: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/status")
async def get_status():
    """Airflow Sensor'ün kontrol edeceği durum endpoint'i."""
    query = streaming_queries.get("bronze_query")

    if not query:
        return {"status": "NOT_INITIALIZED"}

    exception = query.exception()
    if exception:
        logger.error(f"Bronze query hatası: {exception}")
        return {"status": "FAILED", "error": str(exception)}

    if not query.isActive:
        progress = query.lastProgress or {}
        return {
            "status": "COMPLETED",
            "rows_processed": progress.get("numInputRows", "N/A"),
            "message": "Batch processed and stopped.",
        }

    return {
        "status": "RUNNING",
        "query_id": str(query.id),
        "last_progress": query.lastProgress,
    }


@app.post("/stop")
async def stop_ingestion():
    """Aktif stream'i manuel durdurur."""
    query = streaming_queries.get("bronze_query")
    if query and query.isActive:
        query.stop()
        return {"status": "STOP_SIGNAL_SENT"}
    return {"status": "NOT_RUNNING"}

@app.get("/health")
def health_check():
    query = streaming_queries.get("bronze_query")
    return {
        "status": "healthy",
        "stream_active": query.isActive if query else False,
        "engine": "Spark & Delta Lake",
    }


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
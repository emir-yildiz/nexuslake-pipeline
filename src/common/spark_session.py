"""
SPARK SESSION MANAGER (DYNAMIC CONFIGURATION)
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

from .logger import get_logger

logger = get_logger("SparkSessionManager")

_MODULE_DIR  = Path(__file__).parent.resolve()
_CONFIG_PATH = Path(
    os.getenv("SPARK_CONFIG_PATH", _MODULE_DIR / "../../config/spark_config.yaml")
).resolve()

KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"


def _load_and_validate_config(path: Path) -> tuple[str, dict[str, Any]]:
    if not path.exists():
        raise FileNotFoundError(
            f"Spark config bulunamadı: {path}. "
            f"SPARK_CONFIG_PATH env değişkenini kontrol edin."
        )

    with open(path, "r") as f:
        raw = yaml.safe_load(f)

    if not isinstance(raw, dict) or "spark" not in raw:
        raise KeyError(f"Config dosyasında 'spark' anahtarı bulunamadı: {path}")

    spark_cfg = raw["spark"]

    if "app_name" not in spark_cfg:
        raise KeyError(f"Config eksik: spark.app_name ({path})")

    extra_configs: dict[str, Any] = spark_cfg.get("config", {})

    if not isinstance(extra_configs, dict):
        raise ValueError(
            f"spark.config bir dict olmalı, alınan tip: {type(extra_configs).__name__}"
        )

    return spark_cfg["app_name"], extra_configs


def get_spark_session(app_name_override: str | None = None) -> SparkSession:
    """
    Yapılandırılmış bir SparkSession döner.

    Tasarım:
    - getActiveSession() thread-local'dır, executor thread'lerinde None dönebilir.
      Bu nedenle session tespiti için getOrCreate() kullanılır;
      getActiveSession() yalnızca mevcut session'ı loglamak için kontrol edilir.
    - Delta ve Kafka konfigürasyonu builder üzerinde yapılır — mevcut session
      varsa bu config'ler zaten uygulanmış demektir (aynı JVM).
    """
    # Loglama amaçlı mevcut session kontrolü — karar vermek için kullanılmaz
    existing = SparkSession.getActiveSession()
    if existing is not None:
        active_name = existing.conf.get("spark.app.name", "<bilinmiyor>")
        requested   = app_name_override or "<yaml_default>"

        if app_name_override and active_name != app_name_override:
            logger.warning(
                f"Aktif Spark Session mevcut: '{active_name}'. "
                f"İstenen '{requested}' için yeni session açılamaz — "
                f"mevcut session döndürülüyor."
            )
        else:
            logger.debug(f"Mevcut Spark Session kullanılıyor: '{active_name}'")

        # DÜZELTME: Mevcut session'da erken dön — builder'ı tekrar çalıştırma
        return existing

    # ── Yeni session oluştur ──────────────────────────────────────────────────
    try:
        default_app_name, extra_configs = _load_and_validate_config(_CONFIG_PATH)
    except (FileNotFoundError, KeyError, ValueError) as e:
        raise RuntimeError(
            f"Spark Session başlatılamadı (config hatası): {e}"
        ) from e

    app_name = app_name_override or default_app_name
    builder  = SparkSession.builder.appName(app_name)

    # Kafka paketini mevcut packages listesine ekle
    current_packages = extra_configs.get("spark.jars.packages", "")
    if KAFKA_PACKAGE not in current_packages:
        extra_configs["spark.jars.packages"] = (
            f"{current_packages},{KAFKA_PACKAGE}".strip(",")
        )

    for key, value in extra_configs.items():
        builder = builder.config(key, str(value))
        logger.debug(f"Spark config eklendi: {key} = {value}")

    try:
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
    except Exception as e:
        raise RuntimeError(
            f"SparkSession oluşturulamadı (Delta/Kafka entegrasyon hatası): {e}"
        ) from e

    actual_name = spark.conf.get("spark.app.name")
    logger.info(
        f"Yeni Spark Session oluşturuldu: '{actual_name}' | "
        f"Spark: {spark.version} | "
        f"Kafka Support: Enabled ({KAFKA_PACKAGE})"
    )
    return spark


def stop_spark_session() -> bool:
    """
    Aktif SparkSession'ı güvenli şekilde kapatır.

    Returns:
        True  → session bulundu ve kapatıldı
        False → kapatılacak aktif session yoktu
    """
    session = SparkSession.getActiveSession()
    if session is None:
        logger.debug("Kapatılacak aktif Spark Session bulunamadı.")
        return False

    app_name = session.conf.get("spark.app.name", "<bilinmiyor>")
    try:
        session.stop()
        logger.info(f"Spark Session kapatıldı: '{app_name}'")
        return True
    except Exception as e:
        logger.warning(f"Spark Session kapatılırken hata: '{app_name}': {e}")
        return False
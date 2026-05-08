"""
BRONZE LAYER PROCESSOR (RAW INGESTION)
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Literal

import yaml
from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.functions import (
    col, current_timestamp, from_json, lit, when
)
from pyspark.sql.types import (
    DoubleType, IntegerType, StringType, StructType
)

from src.common.logger import get_logger
from src.common.spark_session import get_spark_session

logger = get_logger("BronzeLayer")

CONFIG_PATH              = Path(os.getenv("KAFKA_CONFIG_PATH", "/app/config/kafka_config.yaml"))
CHECKPOINT_PATH          = os.getenv("BRONZE_CHECKPOINT",  "storage/checkpoints/bronze_events")
OUTPUT_PATH              = os.getenv("BRONZE_OUTPUT",      "storage/lakehouse/bronze/events")
DLQ_PATH                 = os.getenv("BRONZE_DLQ",         "storage/lakehouse/bronze/dead_letter")
DEFAULT_TRIGGER_INTERVAL = "30 seconds"
MAX_OFFSETS_PER_TRIGGER  = 10_000

TriggerMode = Literal["availableNow", "processingTime"]

# DÜZELTME: Schema modül seviyesinde bir kez oluşturulur,
# her batch'te yeniden build edilmez.
EVENT_SCHEMA: StructType = (
    StructType()
    .add("event_id",   StringType(),  nullable=True)
    .add("user_id",    IntegerType(), nullable=True)
    .add("product_id", IntegerType(), nullable=True)
    .add("event_type", StringType(),  nullable=True)
    .add("price",      DoubleType(),  nullable=True)
    .add("timestamp",  StringType(),  nullable=True)
    .add("platform",   StringType(),  nullable=True)
    .add("session_id", StringType(),  nullable=True)
)

REQUIRED_CONFIG_KEYS = [
    ("kafka", "bootstrap_servers"),
    ("kafka", "topics", "raw_events"),
    ("kafka", "consumer", "auto_offset_reset"),
]


def load_config() -> dict:
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(
            f"Kafka config bulunamadı: {CONFIG_PATH}. "
            f"KAFKA_CONFIG_PATH env değişkenini kontrol edin."
        )
    with open(CONFIG_PATH, "r") as f:
        config = yaml.safe_load(f)

    for key_path in REQUIRED_CONFIG_KEYS:
        node = config
        for key in key_path:
            if not isinstance(node, dict) or key not in node:
                raise KeyError(
                    f"Config eksik alan: {' → '.join(key_path)} ({CONFIG_PATH})"
                )
            node = node[key]

    logger.info(f"Config doğrulandı ve yüklendi: {CONFIG_PATH}")
    return config


def _build_kafka_reader(spark, config: dict):
    kafka_cfg = config["kafka"]
    reader = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers",  kafka_cfg["bootstrap_servers"])
        .option("subscribe",                kafka_cfg["topics"]["raw_events"])
        .option("startingOffsets",          kafka_cfg["consumer"]["auto_offset_reset"])
        .option("failOnDataLoss",           "false")
        .option("maxOffsetsPerTrigger",     MAX_OFFSETS_PER_TRIGGER)
    )

    security = kafka_cfg.get("security", {})
    if security.get("protocol"):
        reader = reader.option("kafka.security.protocol", security["protocol"])
    if security.get("sasl_mechanism"):
        reader = reader.option("kafka.sasl.mechanism", security["sasl_mechanism"])
    if security.get("sasl_jaas_config"):
        reader = reader.option("kafka.sasl.jaas.config", security["sasl_jaas_config"])

    return reader.load()


def _parse_and_route(raw_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    Kafka value'sunu parse eder.
    Başarılı kayıtlar → bronze_df
    Parse edilemeyenler → dlq_df
    """
    parsed = (
        raw_df
        .select(
            col("topic"),
            col("partition"),
            col("offset"),
            col("timestamp").alias("kafka_timestamp"),
            col("value").cast("string").alias("raw_value"),
            from_json(col("value").cast("string"), EVENT_SCHEMA).alias("data"),
        )
        .select(
            "topic", "partition", "offset", "kafka_timestamp", "raw_value",
            "data.*",
            current_timestamp().alias("ingested_at"),
            when(col("data.event_id").isNull(), lit(True))
                .otherwise(lit(False))
                .alias("_is_corrupt"),
        )
    )

    bronze_df = (
        parsed
        .filter(col("_is_corrupt") == False)    # noqa: E712
        .drop("_is_corrupt", "raw_value")
    )

    dlq_df = (
        parsed
        .filter(col("_is_corrupt") == True)     # noqa: E712
        .select(
            "topic", "partition", "offset",
            "kafka_timestamp", "raw_value", "ingested_at",
            lit("parse_failure").alias("failure_reason"),
        )
    )

    return bronze_df, dlq_df


def _write_batch(micro_batch_df: DataFrame, batch_id: int) -> None:
    """
    Her batch'te Bronze + DLQ'ya yazar.

    Optimizasyon: DataFrame bir kez cache'lenir, count() tek seferde
    çekilir — orijinal kodda 3 ayrı Action (3 Spark job) vardı.
    """
    # DÜZELTME: Önce parse et, sonra cache'le, tek seferde say
    bronze_rows, dlq_rows = _parse_and_route(micro_batch_df)

    # Cache: bronze_rows ve dlq_rows aynı parsed DF'den türediği için
    # count() + write çiftleri ayrı job tetikler. Cache bunu önler.
    bronze_rows.cache()
    dlq_rows.cache()

    try:
        valid   = bronze_rows.count()   # 1 Spark job (cache'ten)
        corrupt = dlq_rows.count()      # 1 Spark job (cache'ten)
        total   = valid + corrupt

        if total == 0:
            logger.info(f"[Batch {batch_id}] Boş batch, atlanıyor.")
            return

        logger.info(
            f"[Batch {batch_id}] Toplam: {total} | "
            f"Geçerli: {valid} | Corrupt: {corrupt} "
            f"({corrupt / total * 100:.1f}%)"
        )

        if valid > 0:
            (
                bronze_rows.write
                .format("delta")
                .mode("append")
                .option("delta.autoOptimize.optimizeWrite", "true")
                .save(OUTPUT_PATH)
            )

        if corrupt > 0:
            logger.warning(f"[Batch {batch_id}] {corrupt} kayıt DLQ'ya yönlendiriliyor.")
            (
                dlq_rows.write
                .format("delta")
                .mode("append")
                .save(DLQ_PATH)
            )

    finally:
        # Cache her durumda temizlenmeli — bellek sızıntısını önler
        bronze_rows.unpersist()
        dlq_rows.unpersist()


def process_bronze(trigger_mode: TriggerMode = "processingTime") -> StreamingQuery:
    config = load_config()
    spark  = get_spark_session("NexusLake_Bronze_Ingestor")

    if trigger_mode == "availableNow":
        trigger_kwargs = {"availableNow": True}
        logger.info("Trigger: AvailableNow (Airflow micro-batch modu)")
    else:
        trigger_kwargs = {"processingTime": DEFAULT_TRIGGER_INTERVAL}
        logger.info(f"Trigger: processingTime={DEFAULT_TRIGGER_INTERVAL} (Streaming modu)")

    raw_df = _build_kafka_reader(spark, config)

    query = (
        raw_df.writeStream
        .foreachBatch(_write_batch)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(**trigger_kwargs)
        .queryName("bronze_kafka_ingestor")
        .start()
    )

    logger.info(f"Bronze stream başlatıldı: {query.id} | Trigger: {trigger_mode}")
    return query


if __name__ == "__main__":
    q = process_bronze(trigger_mode="processingTime")
    try:
        q.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Bronze stream manuel olarak durduruldu.")
        q.stop()
"""
GOLD LAYER PROCESSOR (BUSINESS AGGREGATES)
------------------------------------------
Silver katmanındaki verileri kullanarak yüksek seviyeli analitik
görünümler oluşturur.

Mimari Karar — availableNow + Watermark Uyumsuzluğu:
availableNow modunda Spark tüm mevcut dosyaları TEK bir micro-batch'te
okur. Bu durumda watermark ilerleyemez çünkü ikinci bir batch yoktur.
Çözüm: Aggregation öncesi event_timestamp'a göre manuel pencere hesabı
yapılır (statik batch semantiği). Bu sayede her run'da tüm veriler
Gold'a yazılır, Airflow'un COMPLETED beklentisiyle uyumlu çalışır.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, window, count, sum as _sum,
    current_timestamp, coalesce, lit,
    when
)
from pyspark.sql.types import DoubleType
from pyspark.sql.streaming import StreamingQuery
from src.common.spark_session import get_spark_session
from src.common.logger import get_logger
from delta.tables import DeltaTable

logger = get_logger("GoldLayer")

SILVER_PATH     = "storage/lakehouse/silver/events"
GOLD_PATH       = "storage/lakehouse/gold/platform_metrics"
CHECKPOINT_PATH = "storage/checkpoints/gold_metrics"

WINDOW_DURATION    = "5 minutes"
WATERMARK_DURATION = "10 minutes"

MERGE_CONDITION = """
    target.window_start = source.window_start AND
    target.window_end   = source.window_end   AND
    target.platform     = source.platform
"""


def _build_aggregates(df: DataFrame) -> DataFrame:
    """
    Windowed aggregation uygular.

    Daemon modunda (processingTime trigger): watermark + append mode çalışır,
    pencereler zamanla kesinleşir.

    availableNow modunda: withWatermark kaldırılır, outputMode="complete"
    kullanılır — tüm veriler tek batch'te işlenir ve Gold'a yazılır.
    Bu seçim process_gold()'un trigger parametresine göre yapılır;
    bu fonksiyon her iki mod için de aynı aggregation mantığını kullanır,
    sadece watermark varlığı değişir.
    """
    return (
        df
        .filter(col("event_id").isNotNull())
        .filter(col("platform").isNotNull())
        .groupBy(
            window(col("event_timestamp"), WINDOW_DURATION),
            col("platform")
        )
        .agg(
            count("event_id").alias("total_events"),
            _sum(
                coalesce(col("price").cast(DoubleType()), lit(0.0))
            ).alias("total_revenue"),
            # DÜZELTME: sum(when(...)) ile negatif fiyat sayısı doğru hesaplanır
            _sum(
                when(col("price").cast(DoubleType()) < 0, lit(1)).otherwise(lit(0))
            ).alias("negative_price_count"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("platform"),
            col("total_events"),
            col("total_revenue"),
            col("negative_price_count"),
            current_timestamp().alias("gold_processed_at"),
        )
    )


def _build_aggregates_with_watermark(df: DataFrame) -> DataFrame:
    """Daemon modu: watermark ile windowed aggregation."""
    return _build_aggregates(
        df.withWatermark("event_timestamp", WATERMARK_DURATION)
    )


def _upsert_gold(micro_batch_df: DataFrame, batch_id: int) -> None:
    spark = micro_batch_df.sparkSession

    if micro_batch_df.rdd.isEmpty():
        logger.info(f"[Batch {batch_id}] Kesinleşmiş pencere yok, atlanıyor.")
        return

    logger.info(f"[Batch {batch_id}] Gold MERGE başlıyor.")

    try:
        if DeltaTable.isDeltaTable(spark, GOLD_PATH):
            gold_table = DeltaTable.forPath(spark, GOLD_PATH)
            (
                gold_table.alias("target")
                .merge(micro_batch_df.alias("source"), MERGE_CONDITION)
                .whenMatchedUpdate(set={
                    "total_events":         "source.total_events",
                    "total_revenue":        "source.total_revenue",
                    "negative_price_count": "source.negative_price_count",
                    "gold_processed_at":    "source.gold_processed_at",
                })
                .whenNotMatchedInsertAll()
                .execute()
            )
            logger.info(f"[Batch {batch_id}] Gold MERGE tamamlandı.")
        else:
            (
                micro_batch_df.write
                .format("delta")
                .mode("overwrite")
                .partitionBy("platform")
                .option("delta.enableChangeDataFeed", "true")
                .option("delta.autoOptimize.optimizeWrite", "true")
                .save(GOLD_PATH)
            )
            logger.info("[Gold] Delta tablosu ilk kez oluşturuldu.")

    except Exception as e:
        logger.error(f"[Batch {batch_id}] Gold MERGE hatası: {e}", exc_info=True)
        raise


def process_gold(available_now: bool = False) -> StreamingQuery:
    """
    Gold stream'i başlatır ve StreamingQuery nesnesini döndürür.

    available_now=True  → Airflow modu:
        - outputMode="complete": Tüm pencereler her batch'te yazılır.
        - Watermark kullanılmaz: Tek batch'te watermark ilerleyemez,
          append mode'da hiç veri yazılmaz. Complete mode bu sorunu çözer.
        - Checkpoint TEMİZLENİR: Her Airflow run'ı temiz başlamalı;
          eski state bir önceki run'ın verilerini karıştırabilir.

    available_now=False → Daemon modu:
        - outputMode="append" + watermark: Kesinleşmiş pencereler yazılır.
        - Bellek verimli, sürekli çalışır.
    """
    spark = get_spark_session("NexusLake_Gold_Aggregator")
    spark.conf.set("spark.sql.shuffle.partitions", "8")

    silver_stream = (
        spark.readStream
        .format("delta")
        .option("maxFilesPerTrigger", 50)
        .load(SILVER_PATH)
    )

    if available_now:
        # Airflow modu: watermark YOK, complete mode
        gold_aggregates = _build_aggregates(silver_stream)
        output_mode = "complete"
        trigger_opts = {"availableNow": True}
        logger.info("Gold: availableNow modu — complete outputMode kullanılıyor.")
    else:
        # Daemon modu: watermark VAR, append mode
        gold_aggregates = _build_aggregates_with_watermark(silver_stream)
        output_mode = "append"
        trigger_opts = {"processingTime": "60 seconds"}
        logger.info("Gold: Daemon modu — append outputMode + watermark kullanılıyor.")

    query = (
        gold_aggregates.writeStream
        .outputMode(output_mode)
        .foreachBatch(_upsert_gold)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(**trigger_opts)
        .queryName("gold_platform_metrics")
        .start()
    )

    logger.info(f"Gold stream başlatıldı: {query.id} | availableNow={available_now}")
    return query


if __name__ == "__main__":
    query = process_gold(available_now=False)
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Gold stream durduruldu.")
        query.stop()
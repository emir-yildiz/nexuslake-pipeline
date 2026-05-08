"""
SILVER LAYER PROCESSOR (CLEANSED & ENRICHED)
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_timestamp, current_timestamp
from pyspark.sql.types import StringType
from pyspark.sql.streaming import StreamingQuery
from src.common.spark_session import get_spark_session
from src.common.logger import get_logger
from delta.tables import DeltaTable

logger = get_logger("SilverLayer")

BRONZE_PATH     = "storage/lakehouse/bronze/events"
SILVER_PATH     = "storage/lakehouse/silver/events"
CHECKPOINT_PATH = "storage/checkpoints/silver_events"
MERGE_CONDITION = "target.event_id = source.event_id"


def _cleanse(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("event_timestamp", to_timestamp(col("timestamp")))
        .withColumn("ingestion_at", current_timestamp())
        .drop("timestamp")
        .filter(col("event_id").isNotNull())
        .filter(col("event_timestamp").isNotNull())
        .withColumn("event_type", col("event_type").cast(StringType()))
    )


def _init_silver_table(spark, df: DataFrame) -> None:
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("event_type")
        .option("delta.enableChangeDataFeed", "true")
        .option("delta.autoOptimize.optimizeWrite", "true")
        .option("delta.autoOptimize.autoCompact", "true")
        .save(SILVER_PATH)
    )
    logger.info("Silver Delta tablosu ilk kez oluşturuldu.")


def _upsert_to_delta(micro_batch_df: DataFrame, batch_id: int) -> None:
    spark = micro_batch_df.sparkSession
    count = micro_batch_df.count()
    logger.info(f"[Batch {batch_id}] MERGE başlıyor. Kayıt sayısı: {count}")

    if count == 0:
        logger.info(f"[Batch {batch_id}] Boş batch, atlanıyor.")
        return

    deduped_df = micro_batch_df.dropDuplicates(["event_id"])

    try:
        if DeltaTable.isDeltaTable(spark, SILVER_PATH):
            silver_table = DeltaTable.forPath(spark, SILVER_PATH)
            (
                silver_table.alias("target")
                .merge(deduped_df.alias("source"), MERGE_CONDITION)
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
            logger.info(f"[Batch {batch_id}] MERGE tamamlandı.")
        else:
            _init_silver_table(spark, deduped_df)

    except Exception as e:
        logger.error(f"[Batch {batch_id}] MERGE hatası: {e}", exc_info=True)
        raise


def process_silver(available_now: bool = False) -> StreamingQuery:
    """
    Silver stream'i başlatır ve StreamingQuery nesnesini döndürür.
    available_now=True → Airflow tetiklemesi (mevcut veriyi işle, bitir)
    available_now=False → Sürekli çalışan daemon modu
    """
    spark = get_spark_session("NexusLake_Silver_Transformer")
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    spark.conf.set("spark.sql.shuffle.partitions", "8")

    bronze_stream = (
        spark.readStream
        .format("delta")
        .option("maxFilesPerTrigger", 100)
        .load(BRONZE_PATH)
    )

    cleansed_stream = _cleanse(bronze_stream)

    writer = (
        cleansed_stream.writeStream
        .foreachBatch(_upsert_to_delta)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .queryName("silver_events_upsert")
    )

    # Trigger modunu dışarıdan kontrol et
    if available_now:
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime="30 seconds")

    query = writer.start()
    logger.info(f"Silver stream başlatıldı: {query.id} | availableNow={available_now}")

    # awaitTermination buradan KALDIRILDI.
    # Çağıran taraf (service veya __main__) yönetir.
    return query


if __name__ == "__main__":
    query = process_silver(available_now=False)
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Stream manuel olarak durduruldu.")
        query.stop()
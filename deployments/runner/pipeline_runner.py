"""
NEXUSLAKE PIPELINE RUNNER
--------------------------
Manuel tetikleme ile medallion katmanlarını sırayla çalıştırır.
"""

import logging
import sys

sys.path.insert(0, "/app")

from src.bronze.bronze_layer import process_bronze
from src.silver.silver_layer import process_silver
from src.gold.gold_layer import process_gold

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
log = logging.getLogger(__name__)


def run_pipeline():
    log.info("=== NexusLake Pipeline Başlıyor ===")

    log.info("[1/3] Bronze katman işleniyor...")
    query = process_bronze(trigger_mode="availableNow")
    query.awaitTermination()
    log.info("[1/3] Bronze tamamlandı.")

    log.info("[2/3] Silver katman işleniyor...")
    query = process_silver(trigger_mode="availableNow")
    query.awaitTermination()
    log.info("[2/3] Silver tamamlandı.")

    log.info("[3/3] Gold katman işleniyor...")
    query = process_gold(trigger_mode="availableNow")
    query.awaitTermination()
    log.info("[3/3] Gold tamamlandı.")

    log.info("=== Pipeline başarıyla tamamlandı ===")


if __name__ == "__main__":
    run_pipeline()
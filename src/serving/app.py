"""
SERVING LAYER (FastAPI)
-----------------------
Gold katmanındaki analitik sonuçları REST API üzerinden sunar.
Analistler veya Dashboard'lar için erişim noktası sağlar.
"""

from __future__ import annotations

import asyncio
import uvicorn
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from src.common.spark_session import get_spark_session, stop_spark_session
from src.common.logger import get_logger

logger = get_logger("ServingLayer")

# ─── Sabitler ────────────────────────────────────────────────────────────────

GOLD_PATH     = "storage/lakehouse/gold/platform_metrics"
DEFAULT_LIMIT = 10
MAX_LIMIT     = 500

# ─── Spark Bağımlılığı ────────────────────────────────────────────────────────

_spark: SparkSession | None = None


def get_spark() -> SparkSession:
    if _spark is None:
        raise HTTPException(
            status_code=503,
            detail="Spark session henüz hazır değil, lütfen tekrar deneyin."
        )
    return _spark


# ─── Lifespan ────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    global _spark
    logger.info("NexusLake Serving API başlatılıyor...")

    loop = asyncio.get_event_loop()
    _spark = await loop.run_in_executor(
        None,
        lambda: get_spark_session("NexusLake_API_Server")
    )
    logger.info("Spark session hazır.")
    yield

    logger.info("Serving API kapatılıyor...")
    stopped = await loop.run_in_executor(None, stop_spark_session)
    if not stopped:
        logger.warning("Kapatılacak aktif Spark Session bulunamadı.")


app = FastAPI(title="NexusLake Serving API", lifespan=lifespan)


# ─── Yardımcı ────────────────────────────────────────────────────────────────

def _rows_to_dict(rows) -> list[dict[str, Any]]:
    """
    Spark Row nesnelerini JSON-serializable dict'e çevirir.
    Timestamp gibi non-serializable tipler string'e dönüştürülür.
    """
    result = []
    for row in rows:
        record = {}
        for key, value in row.asDict().items():
            record[key] = (
                str(value)
                if not isinstance(value, (int, float, str, bool, type(None)))
                else value
            )
        result.append(record)
    return result


# ─── Endpointler ─────────────────────────────────────────────────────────────

@app.get("/metrics/platform")
async def get_platform_metrics(
    limit: int = Query(default=DEFAULT_LIMIT, ge=1, le=MAX_LIMIT),
    platform: str | None = Query(default=None, description="Platform filtresi (opsiyonel)"),
):
    """
    Gold katmanındaki platform metriklerini döndürür.

    - **limit**: Döndürülecek maksimum kayıt sayısı (1-500)
    - **platform**: Opsiyonel platform filtresi (örn: 'mobile', 'web')
    """
    spark = get_spark()

    def _read() -> list[dict]:
        df = spark.read.format("delta").load(GOLD_PATH)

        if platform:
            df = df.filter(col("platform") == platform)

        rows = (
            df
            .orderBy(col("window_start").desc())
            .limit(limit)
            .collect()
        )
        return _rows_to_dict(rows)

    try:
        loop = asyncio.get_event_loop()
        results = await loop.run_in_executor(None, _read)
        return {"status": "ok", "count": len(results), "data": results}

    except HTTPException:
        raise

    except Exception as e:
        logger.error(f"/metrics/platform hatası: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/metrics/summary")
async def get_summary():
    """
    Gold tablosunun tamamı için özet istatistikler döndürür.
    Dashboard ana sayfası için tasarlanmıştır.
    """
    spark = get_spark()

    def _summarize() -> dict:
        from pyspark.sql.functions import sum as _sum, max as _max, min as _min

        df = spark.read.format("delta").load(GOLD_PATH)
        row = df.agg(
            _sum("total_events").alias("grand_total_events"),
            _sum("total_revenue").alias("grand_total_revenue"),
            _max("window_end").alias("latest_window"),
            _min("window_start").alias("earliest_window"),
        ).collect()[0]

        return _rows_to_dict([row])[0]

    try:
        loop = asyncio.get_event_loop()
        summary = await loop.run_in_executor(None, _summarize)
        return {"status": "ok", "summary": summary}

    except Exception as e:
        logger.error(f"/metrics/summary hatası: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health")
def health_check():
    spark_ok = _spark is not None
    return {
        "status":         "healthy" if spark_ok else "degraded",
        "spark_session":  "ready"   if spark_ok else "not_initialized",
        "engine":         "Spark & Delta Lake",
    }


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
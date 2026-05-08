"""
GOLD SERVICE - API WRAPPER
"""

import asyncio
import uvicorn
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from src.gold.gold_layer import process_gold
from src.common.logger import get_logger

logger = get_logger("GoldService")

streaming_queries = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("NexusLake Gold Aggregator Servisi Başlatılıyor...")
    yield
    logger.info("Servis kapatılıyor...")
    for name, query in streaming_queries.items():
        if query.isActive:
            logger.info(f"Durduruluyor: {name} (ID: {query.id})")
            query.stop()
    logger.info("Tüm Gold işlemleri durduruldu.")


app = FastAPI(title="NexusLake Gold Aggregator", lifespan=lifespan)


# ─── Endpointler ──────────────────────────────────────────────────────────────

@app.post("/aggregate")
async def start_aggregation():
    """Airflow tarafından tetiklenir. availableNow=True ile çalışır."""
    query = streaming_queries.get("gold_query")
    if query and query.isActive:
        return {"status": "ALREADY_RUNNING", "query_id": str(query.id)}

    try:
        loop = asyncio.get_event_loop()
        query = await loop.run_in_executor(
            None,
            lambda: process_gold(available_now=True)
        )
        streaming_queries["gold_query"] = query
        logger.info(f"Gold Aggregation başlatıldı. ID: {query.id}")
        return {"status": "STARTED", "query_id": str(query.id)}

    except Exception as e:
        logger.error(f"Gold Aggregation başlatılamadı: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/status")
async def get_status():
    """Airflow Sensor bu endpoint'i poll eder."""
    query = streaming_queries.get("gold_query")

    if not query:
        return {"status": "NOT_INITIALIZED"}

    exception = query.exception()
    if exception:
        logger.error(f"Gold query hatası: {exception}")
        return {"status": "FAILED", "error": str(exception)}

    if not query.isActive:
        progress = query.lastProgress or {}
        return {
            "status": "COMPLETED",
            "rows_processed": progress.get("numInputRows", "N/A"),
            "message": "Business aggregates calculated and saved to Gold layer."
        }

    return {
        "status": "RUNNING",
        "query_id": str(query.id),
        "last_progress": query.lastProgress
    }


@app.post("/stop")
async def stop_aggregation():
    query = streaming_queries.get("gold_query")
    if query and query.isActive:
        query.stop()
        return {"status": "STOP_SIGNAL_SENT"}
    return {"status": "NOT_RUNNING"}

@app.get("/health")
def health_check():
    query = streaming_queries.get("gold_query")  # gold için "gold_query"
    return {
        "status": "healthy",
        "stream_active": query.isActive if query else False,
        "engine": "Spark & Delta Lake",
    }


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8002)
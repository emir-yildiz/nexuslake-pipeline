"""
SILVER SERVICE - API WRAPPER
"""

import asyncio
import uvicorn
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from src.silver.silver_layer import process_silver
from src.common.logger import get_logger

logger = get_logger("SilverService")

streaming_queries = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("NexusLake Silver Transformer Servisi Başlatılıyor...")
    yield
    logger.info("Servis kapatılıyor...")
    for name, query in streaming_queries.items():
        if query.isActive:
            logger.info(f"Durduruluyor: {name} (ID: {query.id})")
            query.stop()
    logger.info("Tüm Silver işlemleri durduruldu.")


app = FastAPI(title="NexusLake Silver Transformer", lifespan=lifespan)


# ─── Endpointler ──────────────────────────────────────────────────────────────

@app.post("/transform")
async def start_transform():
    """
    Airflow tarafından tetiklenir. availableNow=True ile çalışır:
    mevcut Bronze verisini işler, biter, COMPLETED döner.
    """
    query = streaming_queries.get("silver_query")
    if query and query.isActive:
        return {"status": "ALREADY_RUNNING", "query_id": str(query.id)}

    try:
        # process_silver() Spark başlatır → blocking → executor'da çalıştır
        loop = asyncio.get_event_loop()
        query = await loop.run_in_executor(
            None,
            lambda: process_silver(available_now=True)
        )
        streaming_queries["silver_query"] = query
        logger.info(f"Silver Transform başlatıldı. ID: {query.id}")
        return {"status": "STARTED", "query_id": str(query.id)}

    except Exception as e:
        logger.error(f"Silver Transform başlatılamadı: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/status")
async def get_status():
    """Airflow Sensor bu endpoint'i poll eder."""
    query = streaming_queries.get("silver_query")

    if not query:
        return {"status": "NOT_INITIALIZED"}

    exception = query.exception()
    if exception:
        logger.error(f"Silver query hatası: {exception}")
        return {"status": "FAILED", "error": str(exception)}

    # availableNow modunda iş bitince isActive otomatik False olur
    if not query.isActive:
        progress = query.lastProgress or {}
        return {
            "status": "COMPLETED",
            "rows_processed": progress.get("numInputRows", "N/A"),
            "message": "Data cleansed and merged into Silver layer."
        }

    return {
        "status": "RUNNING",
        "query_id": str(query.id),
        "last_progress": query.lastProgress
    }


@app.post("/stop")
async def stop_transform():
    query = streaming_queries.get("silver_query")
    if query and query.isActive:
        query.stop()
        return {"status": "STOP_SIGNAL_SENT"}
    return {"status": "NOT_RUNNING"}

@app.get("/health")
def health_check():
    query = streaming_queries.get("silver_query")  # gold için "gold_query"
    return {
        "status": "healthy",
        "stream_active": query.isActive if query else False,
        "engine": "Spark & Delta Lake",
    }


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001)
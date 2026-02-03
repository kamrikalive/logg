import os
import logging
from datetime import datetime, timedelta
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from app.yc_logs import read_logs

# =========================
# LOGGING CONFIG
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("logs-service")

logger.info("🚀 Logs service booting")

app = FastAPI(title="YC Logs Service")

# CORS (можно сузить позже)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/logs")
def get_logs(
    container_id: str = Query(..., description="YC serverless container ID"),
    hours: int = Query(1, ge=1, le=168),
    limit: int = Query(100, ge=1, le=1000),
    page_token: str | None = Query(None),
):
    log_group_id = os.environ.get("YC_LOG_GROUP_ID")
    if not log_group_id:
        logger.error("YC_LOG_GROUP_ID not set")
        raise HTTPException(500, "YC_LOG_GROUP_ID is not set")

    logger.info(
        "Incoming logs request",
        extra={
            "container_id": container_id,
            "hours": hours,
            "limit": limit,
        },
    )

    now = datetime.utcnow()
    since = now - timedelta(hours=hours)

    try:
        resp = read_logs(
            log_group_id=log_group_id,
            resource_id=container_id,
            since_dt=since,
            until_dt=now,
            page_size=limit,
            page_token=page_token or "",
        )
    except Exception as e:
        logger.exception("Error reading logs from YC")
        raise HTTPException(500, f"Provider error: {str(e)}")

    logs = []

    for entry in resp.entries:
        # Protobuf Timestamp -> ISO
        dt_obj = (
            datetime.utcfromtimestamp(entry.timestamp.seconds)
            + timedelta(microseconds=entry.timestamp.nanos / 1000)
        )

        logs.append({
            "timestamp": dt_obj.isoformat() + "Z",
            "level": entry.level,
            "message": entry.message
            or (
                entry.json_payload.get("message")
                if entry.json_payload else ""
            ),
            "json": dict(entry.json_payload) if entry.json_payload else {},
            "stream": (
                entry.json_payload.get("stream")
                if entry.json_payload else "stdout"
            ),
        })

    logger.info("Response ready", extra={"count": len(logs)})

    return {
        "logs": logs,
        "nextPageToken": resp.next_page_token or None,
        "count": len(logs),
    }

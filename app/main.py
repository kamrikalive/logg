from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime, timedelta
import os
from app.yc_logs import read_logs

app = FastAPI(title="YC Logs Service")

# Разрешаем CORS, чтобы фронт (если нужно) или бэк могли ходить свободно
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
        raise HTTPException(500, "YC_LOG_GROUP_ID is not set")

    now = datetime.utcnow()
    since = now - timedelta(hours=hours)

    try:
        # Передаем datetime объекты, конвертация внутри read_logs
        resp = read_logs(
            log_group_id=log_group_id,
            resource_id=container_id,
            since_dt=since,
            until_dt=now,
            page_size=limit,
            page_token=page_token or "",
        )
    except Exception as e:
        print(f"Error reading logs: {e}")
        raise HTTPException(500, f"Provider error: {str(e)}")

    logs = []
    # gRPC ответ -> JSON
    for entry in resp.entries:
        # Конвертируем Protobuf Timestamp обратно в строку
        ts_seconds = entry.timestamp.seconds
        ts_nanos = entry.timestamp.nanos
        dt_obj = datetime.utcfromtimestamp(ts_seconds) + timedelta(microseconds=ts_nanos / 1000)
        
        logs.append({
            "timestamp": dt_obj.isoformat() + "Z",
            "level": entry.level,
            "message": entry.message or (
                entry.json_payload.get("message")
                if entry.json_payload else ""
            ),
            # json_payload в SDK это Struct, превращаем в dict
            "json": dict(entry.json_payload) if entry.json_payload else {},
            "stream": entry.json_payload.get("stream") if entry.json_payload else "stdout"
        })

    return {
        "logs": logs,
        "nextPageToken": resp.next_page_token or None,
        "count": len(logs),
    }
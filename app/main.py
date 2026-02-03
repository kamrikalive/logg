from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime, timedelta
from app.yc_logs import read_logs

app = FastAPI(title="YC Logs Service")

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
    container_id: str = Query(...),
    hours: int = Query(1, ge=1, le=168),
    limit: int = Query(100, ge=1, le=1000),
    page_token: str | None = Query(None),
):
    now = datetime.utcnow()
    since = now - timedelta(hours=hours)

    try:
        resp = read_logs(
            resource_id=container_id,
            since_dt=since,
            until_dt=now,
            page_size=limit,
            page_token=page_token or "",
        )
    except Exception as e:
        raise HTTPException(500, str(e))

    logs = []
    for entry in resp.entries:
        dt = datetime.utcfromtimestamp(entry.timestamp.seconds)
        logs.append({
            "timestamp": dt.isoformat() + "Z",
            "level": entry.level,
            "message": entry.message,
        })

    return {
        "logs": logs,
        "count": len(logs),
        "nextPageToken": resp.next_page_token or None,
    }

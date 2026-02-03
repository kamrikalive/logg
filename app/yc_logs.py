import os
import json
import logging
import requests
import yandexcloud
from google.protobuf.timestamp_pb2 import Timestamp
from yandex.cloud.logging.v1.log_reading_service_pb2 import (
    ReadRequest,
    Criteria,
)
from yandex.cloud.logging.v1.log_reading_service_pb2_grpc import (
    LogReadingServiceStub,
)

# =========================
# LOGGING CONFIG
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("yc-logs-sdk")


def get_sdk():
    """
    Создает и авторизует Yandex Cloud SDK.
    Поддерживает:
    1) IAM token
    2) Service Account JSON
    3) Metadata Service (если внутри YC)
    """
    # 1️⃣ IAM Token (быстро и просто)
    token = os.environ.get("YC_IAM_TOKEN")
    if token:
        logger.info("Auth via YC_IAM_TOKEN")
        return yandexcloud.SDK(iam_token=token)

    # 2️⃣ Service Account JSON
    sa_key_raw = os.environ.get("YC_SA_KEY_JSON")
    if sa_key_raw:
        try:
            logger.info("Auth via YC_SA_KEY_JSON")
            service_account_key = json.loads(sa_key_raw)
            return yandexcloud.SDK(service_account_key=service_account_key)
        except Exception as e:
            logger.error("Failed to parse YC_SA_KEY_JSON", exc_info=e)

    # 3️⃣ Metadata Service (внутри Yandex Cloud)
    try:
        logger.info("Trying Metadata Service auth")
        url = "http://169.254.169.254/computeMetadata/v1/instance/service-accounts/default/token"
        headers = {"Metadata-Flavor": "Google"}
        resp = requests.get(url, headers=headers, timeout=1)

        if resp.status_code == 200:
            token = resp.json().get("access_token")
            logger.info("Auth via Metadata Service")
            return yandexcloud.SDK(iam_token=token)
    except Exception:
        logger.warning("Metadata Service unavailable")

    raise RuntimeError(
        "No valid authentication found. "
        "Set YC_IAM_TOKEN, YC_SA_KEY_JSON or run inside Yandex Cloud."
    )


def read_logs(
    *,
    log_group_id: str,
    resource_id: str,
    since_dt,
    until_dt,
    page_size: int = 100,
    page_token: str = "",
):
    logger.info(
        "Reading logs",
        extra={
            "log_group_id": log_group_id,
            "resource_id": resource_id,
            "page_size": page_size,
            "page_token": bool(page_token),
        },
    )

    sdk = get_sdk()
    client = sdk.client(LogReadingServiceStub)

    # datetime -> protobuf Timestamp
    since_ts = Timestamp()
    since_ts.FromDatetime(since_dt)

    until_ts = Timestamp()
    until_ts.FromDatetime(until_dt)

    criteria = Criteria(
        log_group_id=log_group_id,
        resource_ids=[resource_id],
        since=since_ts,
        until=until_ts,
        page_size=page_size,
        page_token=page_token or "",
    )

    request = ReadRequest(criteria=criteria)
    response = client.Read(request)

    logger.info(
        "Logs fetched",
        extra={
            "entries": len(response.entries),
            "next_page_token": bool(response.next_page_token),
        },
    )

    return response

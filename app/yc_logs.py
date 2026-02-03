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
    # 1️⃣ IAM token
    token = os.getenv("YC_IAM_TOKEN")
    if token:
        logger.info("Auth via YC_IAM_TOKEN")
        return yandexcloud.SDK(iam_token=token)

    # 2️⃣ Service Account JSON
    sa_key_raw = os.getenv("YC_SA_KEY_JSON")
    if sa_key_raw:
        logger.info("Auth via YC_SA_KEY_JSON")
        return yandexcloud.SDK(service_account_key=json.loads(sa_key_raw))

    # 3️⃣ Metadata (внутри YC)
    try:
        url = "http://169.254.169.254/computeMetadata/v1/instance/service-accounts/default/token"
        headers = {"Metadata-Flavor": "Google"}
        resp = requests.get(url, headers=headers, timeout=1)
        if resp.status_code == 200:
            logger.info("Auth via Metadata Service")
            return yandexcloud.SDK(iam_token=resp.json()["access_token"])
    except Exception:
        pass

    raise RuntimeError("YC auth not found")


def get_log_group_id() -> str:
    log_group_id = (
        os.getenv("YC_LOG_GROUP_ID")
        or os.getenv("YC_DEFAULT_LOG_GROUP_ID")
    )
    if not log_group_id:
        raise RuntimeError("YC_LOG_GROUP_ID not set")
    return log_group_id


def read_logs(
    *,
    resource_id: str,
    since_dt,
    until_dt,
    page_size: int = 100,
    page_token: str | None = None,
):
    log_group_id = get_log_group_id()
    sdk = get_sdk()
    client = sdk.client(LogReadingServiceStub)

    since_ts = Timestamp()
    since_ts.FromDatetime(since_dt)

    until_ts = Timestamp()
    until_ts.FromDatetime(until_dt)

    if page_token:
        # 🔁 Пагинация — ТОЛЬКО page_token
        request = ReadRequest(page_token=page_token)
    else:
        # 🆕 Первый запрос — ТОЛЬКО criteria
        criteria = Criteria(
            log_group_id=log_group_id,
            resource_ids=[resource_id],
            since=since_ts,
            until=until_ts,
            page_size=page_size,
        )
        request = ReadRequest(criteria=criteria)

    response = client.Read(request)
    return response

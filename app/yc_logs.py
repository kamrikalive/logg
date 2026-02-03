import os
import json
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

def get_sdk():
    """
    Создает и авторизует SDK, пытаясь использовать разные методы (как в Node.js)
    """
    # 1. Прямой IAM Token (для быстрых тестов)
    token = os.environ.get("YC_IAM_TOKEN")
    if token:
        # print("[Auth] Using YC_IAM_TOKEN")
        return yandexcloud.SDK(iam_token=token)

    # 2. JSON-ключ сервисного аккаунта (Аналог вашего generateJwt в Node.js)
    # SDK сам сделает JWT, подпишет его и обменяет на IAM Token.
    sa_key_raw = os.environ.get("YC_SA_KEY_JSON")
    if sa_key_raw:
        try:
            # print("[Auth] Using YC_SA_KEY_JSON")
            service_account_key = json.loads(sa_key_raw)
            return yandexcloud.SDK(service_account_key=service_account_key)
        except Exception as e:
            print(f"[Auth] Error parsing YC_SA_KEY_JSON: {e}")

    # 3. Metadata Service (Работает внутри контейнера в Yandex Cloud)
    try:
        url = "http://169.254.169.254/computeMetadata/v1/instance/service-accounts/default/token"
        headers = {"Metadata-Flavor": "Google"}
        # print(f"[Auth] Trying Metadata Service: {url}")
        resp = requests.get(url, headers=headers, timeout=1)
        if resp.status_code == 200:
            token = resp.json().get("access_token")
            # print("[Auth] Successfully received token from Metadata")
            return yandexcloud.SDK(iam_token=token)
    except Exception as e:
        # Игнорируем ошибку, если мы не в облаке
        pass

    raise ValueError("No valid authentication found. Please set YC_SA_KEY_JSON or run inside Yandex Cloud.")

def read_logs(
    *,
    log_group_id: str,
    resource_id: str,
    since_dt,
    until_dt,
    page_size: int = 100,
    page_token: str = "",
):
    # Получаем авторизованный SDK
    sdk = get_sdk()
    client = sdk.client(LogReadingServiceStub)

    # Конвертация datetime -> Protobuf Timestamp
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

    return response
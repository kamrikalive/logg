import os
import yandexcloud
from google.protobuf.timestamp_pb2 import Timestamp
from yandex.cloud.logging.v1.log_reading_service_pb2 import (
    ReadRequest,
    Criteria,
)
from yandex.cloud.logging.v1.log_reading_service_pb2_grpc import (
    LogReadingServiceStub,
)

def read_logs(
    *,
    log_group_id: str,
    resource_id: str,
    since_dt,  # принимаем datetime объект
    until_dt,  # принимаем datetime объект
    page_size: int = 100,
    page_token: str = "",
):
    token = os.environ.get("YC_IAM_TOKEN")
    if not token:
        raise ValueError("YC_IAM_TOKEN not set")

    # Авторизация
    sdk = yandexcloud.SDK(iam_token=token)
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
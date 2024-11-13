from datetime import datetime
from typing import Any

import functions_framework
from flask import Request
from flask.typing import ResponseReturnValue
from pydantic_settings import BaseSettings, SettingsConfigDict

from elasticsearch import Elasticsearch, helpers


class Settings(BaseSettings):
    elastic_api_key: str = ""
    elastic_api_key_id: str = ""
    elastic_cloud_id: str = ""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


def doc_generator(data: list[dict[str, Any | datetime]], index: str):
    for value in data:
        yield {
            "_index": index,
            "_id": f"{value['time_received']}",
            "_source": value["data"]["readings"]
            | {"time_received": value["time_received"]},
        }


@functions_framework.http
def ingest_data(request: Request) -> ResponseReturnValue:
    if request.method != "POST":
        return "Ok"

    payload = request.get_json(silent=True)

    if payload is None:
        return "Ok"

    settings = Settings()

    es = Elasticsearch(
        cloud_id=settings.elastic_cloud_id,
        api_key=settings.elastic_api_key,
    )

    index = payload.get("component_name")
    trigger_data = payload.get("data")
    sensor_data = [
        item | {"time_received": item["metadata"]["received_at"]}
        for item in trigger_data
    ]

    print("Ingesting sensor data")
    helpers.bulk(es, doc_generator(sensor_data, index))

    print("Ingestion complete!")

    return "Ok"

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
    elastic_connection_endpoint: str = ""
    elastic_index: str = ""

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
    print(f"Current request: {request.method}")
    if request.method != "POST":
        return "Ok"

    print("Getting payload")
    payload = request.get_json(silent=True)

    if payload is None:
        print("No payload")
        return "Ok"

    settings = Settings()

    es = Elasticsearch(
        settings.elastic_connection_endpoint,
        api_key=settings.elastic_api_key,
    )

    index = settings.elastic_index or payload.get("component_name")
    print(f"Ingesting into index: {index}")
    trigger_data = payload.get("data")
    sensor_data = [
        item | {"time_received": item["metadata"]["received_at"]}
        for item in trigger_data
    ]

    print("Ingesting sensor data")
    helpers.bulk(es, doc_generator(sensor_data, index))

    print("Ingestion complete!")

    return "Ok"

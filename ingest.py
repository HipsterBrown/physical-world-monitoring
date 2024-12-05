import asyncio
from datetime import datetime

from pydantic_settings import BaseSettings, SettingsConfigDict

from elasticsearch import Elasticsearch, helpers
from viam.app.data_client import ValueTypes
from viam.app.viam_client import ViamClient
from viam.rpc.dial import DialOptions
from viam.utils import create_filter


class Settings(BaseSettings):
    elastic_api_key: str = ""
    elastic_api_key_id: str = ""
    elastic_connection_endpoint: str = ""
    viam_api_key: str = ""
    viam_api_key_id: str = ""
    viam_org_id: str = ""
    sensor_name: str = ""
    query_start_time: str = ""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


async def connect_to_viam(settings: Settings) -> ViamClient:
    opts = DialOptions.with_api_key(
        api_key=settings.viam_api_key, api_key_id=settings.viam_api_key_id
    )
    return await ViamClient.create_from_dial_options(dial_options=opts)


def doc_generator(data: list[dict[str, ValueTypes | datetime]], index: str):
    for value in data:
        yield {
            "_index": index,
            "_id": f"{value['time_received']}",
            "_source": value["data"]["readings"]
            | {"time_received": value["time_received"]},
        }


async def main():
    settings = Settings()

    es = Elasticsearch(
        settings.elastic_connection_endpoint,
        api_key=settings.elastic_api_key,
    )

    viam_client = await connect_to_viam(settings)
    data_client = viam_client.data_client

    # sensor_data = await data_client.tabular_data_by_sql(
    #     organization_id=settings.viam_org_id,
    #     sql_query=f"SELECT * from readings WHERE organization_id='{settings.viam_org_id}' AND component_name='{settings.sensor_name}' AND time_received >= CAST('{settings.query_start_time}' as TIMESTAMP)",
    # )
    print("Getting sensor data")
    sensor_data = []
    filter = create_filter(
        component_name=settings.sensor_name,
        organization_ids=[settings.viam_org_id],
        start_time=datetime.fromisoformat(settings.query_start_time),
    )
    last = None
    total = 0
    while True:
        tabular_data, count, last = await data_client.tabular_data_by_filter(
            filter=filter,
            last=last,
            limit=2000,
        )
        if not tabular_data:
            break
        total += count
        print(f"current data count: {total}")
        sensor_data.extend(
            [
                {"data": tab.data} | {"time_received": tab.time_received}
                for tab in tabular_data
            ]
        )

    index = settings.sensor_name
    print("Creating index if not created yet")
    try:
        es.indices.create(index=index)
    except Exception as e:
        print(e)

    print("Ingesting sensor data")
    helpers.bulk(es, doc_generator(sensor_data, index))

    print("Ingestion complete!")
    viam_client.close()


if __name__ == "__main__":
    asyncio.run(main())

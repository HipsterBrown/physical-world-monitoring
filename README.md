# Monitoring & Alerting in the Physical World w/ Viam & Elastic

Source code for demos presented at ElasticON NYC 2024. Slides & video coming soon.

This project repo is managed with [uv](https://docs.astral.sh/uv/).

## Overview

**Ingest data from [Viam Data]() to an Elastic Search index:** [`ingest.py`](./ingest.py)

**ETL function for indexing new sensor data from a [machine webhook]() to Elastic Search:** [`cloud-functions/ingest-sensor-data/main.py`](./cloud-functions/ingest-sensor-data/main.py)

**Alerting function triggered by Elastic observability rules to blink an LED on a Viam machine:** [`cloud-functions/movement-alert/main.py`](./cloud-functions/movement-alert/main.py)

## Development

**Prerequisites:**

- [uv](https://docs.astral.sh/uv/getting-started/installation/)
- Make copy of `.env.example` called `.env` and fill out necessary environment variable values for task

**Install project dependencies:

```console
uv pip install .
```

**Run ingestion script**:

```console
uv run ingest.py
```

**Run ingest-sensor-data function locally**:

```console
uv run functions-framework --source ./cloud-functions/ingest-sensor-data/main.py --target ingest_data --debug
```

This will start a Flash server at http://localhost:8080 which can be sent data through curl or another API testing tool.

**Run movement-alert function locally**:

```console
uv run functions-framework --source ./cloud-functions/movement-alert/main.py --target alert_movement --debug
```

This will start a Flash server at http://localhost:8080 which can be sent data through curl or another API testing tool.


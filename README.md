# krx-spark-pipeline

A data pipeline that collects daily OHLCV data for KOSPI-listed stocks, processes it with PySpark to calculate moving averages and daily returns, and stores the results in AWS S3.

## Pipeline Overview

```
Airflow (daily collection)
        ↓
KRX OHLCV data → AWS S3 (raw, CSV)
        ↓
PySpark (incremental transform)
        ↓
Moving averages (5d/20d) + daily return % → AWS S3 (processed, Parquet)
        ↓
dbt + Snowflake (planned)
```

## Tech Stack

- **Orchestration**: Apache Airflow
- **Data Collection**: Python, pykrx
- **Storage**: AWS S3
- **Processing**: PySpark (Apache Spark)
- **Infra**: Docker, Docker Compose
- **Planned**: dbt, Snowflake

## Project Structure

```
krx-spark-pipeline/
├── airflow/
│   ├── Dockerfile
│   └── dags/
│       └── krx_collect_dag.py      # Daily collection DAG
├── collectors/
│   ├── kospi_tickers.csv           # KOSPI ticker list
│   └── krx_collector.py            # OHLCV fetch logic (pykrx)
├── spark/
│   ├── Dockerfile                  # apache/spark:python3 + hadoop-aws + boto3
│   └── krx_transform.py            # Incremental PySpark transform job
├── dbt/                             # Reserved for future dbt-snowflake integration
├── image/                           # Pipeline run captures (Airflow, S3 structure)
└── docker-compose.yml
```

## What It Does

1. **Collection** (`krx_collect_dag.py`): Every weekday after market close, fetches OHLCV data for all KOSPI tickers via `pykrx` and uploads it to S3 as `market=KOSPI/date=YYYY-MM-DD/ohlcv.csv`.
2. **Transform** (`krx_transform.py`): Reads only the dates not yet processed (diffing raw vs. processed S3 prefixes), calculates 5-day/20-day moving averages and day-over-day return percentage using PySpark Window functions, and writes the result to S3 as Parquet, partitioned by ticker.

## 현재 상태

위 프로젝트는 **Airflow 수집 → PySpark 변환 → S3 Parquet 적재까지 완료**된 상태로 잠시 중단..

추후 `dbt-snowflake` 프리티어 어댑터를 학습 및 연동하여 모델링 단계를 마무리할 예정
비용 방지를 위해 운영 중인 S3 버킷은 삭제 예정이며, 버킷 구조와 Parquet 샘플은 `image/` 폴더 및 로컬 백업으로 보관
재개 시 본 README와 백업 자료 참고 예정

## Setup

```bash
git clone https://github.com/<your-account>/krx-spark-pipeline.git
cd krx-spark-pipeline
cp .env.example .env   # fill in AWS credentials and bucket names
docker compose up -d
```

Run the transform job manually:

```bash
docker exec krx-pyspark /opt/spark/bin/spark-submit /opt/spark/jobs/krx_transform.py
```

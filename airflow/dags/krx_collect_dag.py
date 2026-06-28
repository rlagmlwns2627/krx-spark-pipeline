import io
import logging
import os
import sys
from datetime import datetime, timedelta

import boto3
from airflow import DAG
from airflow.operators.python import PythonOperator

# collectors 폴더를 Python 경로에 추가
# docker-compose.yml에서 /opt/airflow/collectors 로 마운트했기 때문
sys.path.insert(0, "/opt/airflow")
from collectors.krx_collector import fetch_ohlcv

logger = logging.getLogger(__name__)

default_args = {
    "owner": "airflow",
    # 실패 시 2번 재시도, 재시도 간격 5분
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def collect_and_upload(**context):
    # execution_date 대신 data_interval_end 사용
    # DAG이 재실행되어도 같은 날짜 데이터를 수집하는 멱등성 보장
    exec_date: datetime = context["data_interval_end"]
    date_str = exec_date.strftime("%Y%m%d")
    partition = exec_date.strftime("%Y-%m-%d")

    logger.info(f"수집 날짜: {date_str}")

    df = fetch_ohlcv(date_str, market="KOSPI")

    if df.empty:
        logger.warning(f"{date_str} 휴장일이거나 데이터 없음 — 스킵")
        return

    # DataFrame → CSV → 메모리 버퍼 (디스크 저장 없이 바로 S3 업로드)
    buffer = io.StringIO()
    df.to_csv(buffer, index=False, encoding="utf-8-sig")
    body = buffer.getvalue().encode("utf-8-sig")

    # boto3 클라이언트 생성
    # AWS_ENDPOINT_URL이 설정되어 있으면 LocalStack을 바라봄
    s3 = boto3.client(
        "s3",
        # endpoint_url=os.getenv("AWS_ENDPOINT_URL"),   # LocalStack 활용 X, 지정 안해도 됨
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_DEFAULT_REGION"),
    )

    bucket = os.getenv("S3_RAW_BUCKET")

    # S3 저장 경로: market=KOSPI/date=YYYY-MM-DD/ohlcv.csv
    key = f"market=KOSPI/date={partition}/ohlcv.csv"
    s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType="text/csv")
    logger.info(f"업로드 완료: s3://{bucket}/{key} ({len(df)}개 종목)")


with DAG(
    dag_id="krx_daily_collect",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    # 평일 17:30 KST 실행 (UTC 08:30 = KST 17:30)
    schedule_interval="30 8 * * 1-5",
    # False: 과거 미실행 DAG을 소급해서 실행하지 않음
    # True: 26년 전체 데이터 보기 위해 백필
    catchup=True,
    tags=["krx", "collect"],
) as dag:

    collect_task = PythonOperator(
        task_id="collect_and_upload_to_s3",
        python_callable=collect_and_upload,
    )
FROM apache/airflow:2.9.1-python3.11

USER root
# gcc는 일부 Python 패키지 컴파일에 필요
RUN apt-get update && apt-get install -y gcc && rm -rf /var/lib/apt/lists/*

USER airflow
# pykrx: KRX 주가 데이터 수집
# boto3: S3 연동
# awscli-local: 터미널에서 LocalStack S3 확인용 (awslocal 명령어)
RUN pip install --no-cache-dir \
    pykrx==1.0.45 \
    boto3==1.34.0 \
    awscli-local==0.22.0
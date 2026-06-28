import os
import logging
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, lag, round, when
from pyspark.sql.types import LongType, StringType
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── 1. SparkSession 생성
# pandas: 별도 세션 개념 없음
spark = SparkSession.builder \
    .appName("krx-pipeline-2") \
    .config("spark.hadoop.fs.s3a.access.key", os.environ.get("AWS_ACCESS_KEY_ID")) \
    .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("AWS_SECRET_ACCESS_KEY")) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .getOrCreate()

RAW_BUCKET       = os.environ.get("S3_RAW_BUCKET")
PROCESSED_BUCKET = os.environ.get("S3_PROCESSED_BUCKET")
RAW_BASE         = f"s3a://{RAW_BUCKET}/market=KOSPI"
PROCESSED_BASE   = f"s3a://{PROCESSED_BUCKET}/processed/market=KOSPI"

# boto3 클라이언트 생성 (S3 폴더 목록 조회용)
s3 = boto3.client(
    "s3",
    aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
    region_name=os.environ.get("AWS_DEFAULT_REGION"),
)

# ── 2. 수집된 날짜 목록 조회 (raw)
# pandas: os.listdir(path)로 폴더 목록 조회하는 것에 해당
logger.info("수집된 날짜 목록 조회 중...")
response = s3.list_objects_v2(
    Bucket=RAW_BUCKET,
    Prefix="market=KOSPI/",
    Delimiter="/"
)
raw_dates = set()
for prefix in response.get("CommonPrefixes", []):
    folder = prefix["Prefix"].split("/")[1]  # "date=2026-01-02" 형식
    if folder.startswith("date="):
        raw_dates.add(folder.replace("date=", ""))

# ── 3. 이미 처리된 날짜 목록 조회 (processed)
# pandas: set(df['date'].unique())에 해당
logger.info("처리된 날짜 목록 조회 중...")
processed_dates = set()
try:
    df_processed_existing = spark.read.parquet(PROCESSED_BASE)
    processed_dates = set(
        row["date"] for row in df_processed_existing.select("date").distinct().collect()
    )
except Exception:
    logger.info("처리된 데이터 없음 — 전체 처리 시작")

# ── 4. 미처리 날짜만 추출
# pandas: set(raw_dates) - set(processed_dates)에 해당
raw_dates_normalized = [d.replace("-", "") for d in raw_dates]  # YYYY-MM-DD → YYYYMMDD 변환
pending_dates = sorted(raw_dates_normalized - processed_dates)
logger.info(f"미처리 날짜 {len(pending_dates)}개: {pending_dates}")

if not pending_dates:
    logger.info("처리할 날짜 없음 — 종료")
    spark.stop()
    exit(0)

# ── 5. 미처리 날짜 데이터만 읽기
# pandas: pd.concat([pd.read_csv(f"{path}/date={d}/ohlcv.csv") for d in pending_dates])에 해당
input_paths = [f"{RAW_BASE}/date={d}/ohlcv.csv" for d in pending_dates]
df_raw = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(input_paths)

logger.info(f"읽기 완료 — {df_raw.count()}행")

# ── 6. 타입 정리 및 컬럼 정제
# pandas: df.astype(int)에 해당
df = df_raw.select(
    col("ticker").cast(StringType()),
    col("date").cast(StringType()),
    col("open").cast(LongType()),
    col("high").cast(LongType()),
    col("low").cast(LongType()),
    col("close").cast(LongType()),
    col("volume").cast(LongType()),
    col("trade_value").cast(LongType()),
)

# ── 7. Window 정의
# pandas: df.groupby('ticker').apply(lambda x: x.sort_values('date'))에 해당
window_ticker = Window.partitionBy("ticker").orderBy("date")

# 5일 이동평균 Window
# pandas: df.groupby('ticker')['close'].rolling(5).mean()에 해당
window_ma5  = window_ticker.rowsBetween(-4, 0)

# 20일 이동평균 Window
# pandas: df.groupby('ticker')['close'].rolling(20).mean()에 해당
window_ma20 = window_ticker.rowsBetween(-19, 0)

# ── 8. 이동평균, 등락률 계산
logger.info("이동평균 및 등락률 계산 중...")
df_result = df \
    .withColumn(
        "ma5",
        # pandas: df.groupby('ticker')['close'].rolling(5).mean()에 해당
        round(avg("close").over(window_ma5), 2)
    ) \
    .withColumn(
        "ma20",
        # pandas: df.groupby('ticker')['close'].rolling(20).mean()에 해당
        round(avg("close").over(window_ma20), 2)
    ) \
    .withColumn(
        "prev_close",
        # pandas: df.groupby('ticker')['close'].shift(1)에 해당
        lag("close", 1).over(window_ticker)
    ) \
    .withColumn(
        "change_pct",
        # pandas: (df['close'] - df['close'].shift(1)) / df['close'].shift(1) * 100에 해당
        when(
            col("prev_close").isNotNull(),
            round((col("close") - col("prev_close")) / col("prev_close") * 100, 2)
        ).otherwise(None)
    ) \
    .drop("prev_close")  # 중간 계산용 컬럼 제거

# ── 9. 미처리 날짜만 필터링 후 S3 Parquet 저장
# pandas: df[df['date'].isin(pending_dates)].to_parquet(path)에 해당
# partitionBy로 ticker별 폴더 분리 저장
logger.info("S3에 Parquet 저장 중...")

df_to_save = df_result.filter(col("date").isin(pending_dates))
logger.info(f"저장할 행 수: {df_to_save.count()}")

df_to_save \
    .write \
    .mode("overwrite") \
    .partitionBy("ticker") \
    .parquet(PROCESSED_BASE)

logger.info(f"저장 완료 — {len(pending_dates)}개 날짜 처리")
spark.stop()
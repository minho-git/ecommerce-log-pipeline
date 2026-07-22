"""
Kafka Consumer -> Bronze 레이어 적재 (Spark Structured Streaming)

목적:
- Kafka topic("web-logs")을 Structured Streaming으로 읽어서 Bronze 레이어에
  Parquet로 저장한다. Bronze는 정제 없이 원본을 보존하는 레이어이므로 여기서는
  파싱만 하고 dedup/정제는 하지 않는다 (그건 Silver의 역할).

trigger(availableNow=True):
- 무한정 떠있는 스트리밍 잡이 아니라, 지금까지 쌓인 데이터를 전부 처리하고
  종료하는 방식. Airflow가 이 스크립트를 주기적으로(예: 매일) 실행시키는
  구조에 맞는 실행 방식이다.

체크포인트(checkpointLocation):
- 어디까지 처리했는지(Kafka 오프셋)를 저장해둔다. 다음 실행에서는 체크포인트
  이후의 새 메시지만 처리하므로, 같은 로그를 중복으로 다시 읽지 않는다.

파티셔닝 기준:
- 로그의 이벤트 시간이 아니라 "컨슘(수집)한 날짜" 기준으로 파티션을 나눈다.
  실무에서 Bronze는 append-only로 쌓이는 원본 기록이라, 나중에 도착한 데이터
  때문에 예전 파티션을 다시 여는 상황을 피하기 위해 수집 시간 기준으로 나누는
  것이 일반적이다.
"""

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date, from_json
from pyspark.sql.types import StringType, StructField, StructType

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "web-logs"

PROJECT_ROOT = Path(__file__).parent
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"
CHECKPOINT_DIR = PROJECT_ROOT / "data" / "_checkpoints" / "bronze"

LOG_SCHEMA = StructType(
    [
        StructField("log_id", StringType(), nullable=False),
        StructField("user_id", StringType(), nullable=True),
        StructField("timestamp", StringType(), nullable=False),
        StructField("page", StringType(), nullable=True),
        StructField("action", StringType(), nullable=True),
        StructField("session_id", StringType(), nullable=True),
    ]
)


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("bronze-ingest")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        )
        .getOrCreate()
    )


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed = raw.select(
        from_json(col("value").cast("string"), LOG_SCHEMA).alias("log"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp"),
    ).select("log.*", "kafka_partition", "kafka_offset", "kafka_timestamp")

    bronze = parsed.withColumn("ingestion_date", current_date())

    query = (
        bronze.writeStream.format("parquet")
        .option("path", str(BRONZE_DIR))
        .option("checkpointLocation", str(CHECKPOINT_DIR))
        .partitionBy("ingestion_date")
        .trigger(availableNow=True)
        .start()
    )

    query.awaitTermination()
    print("Bronze 적재 완료")


if __name__ == "__main__":
    main()

import os

import psycopg2
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, max as spark_max
from pyspark.sql.functions import min as spark_min
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.functions import to_timestamp, window
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType


load_dotenv()


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "stock_prices")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "stock_market")
POSTGRES_USER = os.getenv("POSTGRES_USER", "marketpulse")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "marketpulse123")

CHECKPOINT_PATH = os.getenv(
    "SPARK_CHECKPOINT_PATH", "checkpoints/marketpulse_stock_prices_curated"
)


def create_database_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


def ensure_curated_objects() -> None:
    connection = create_database_connection()
    cursor = connection.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS stock_prices_curated (
            id SERIAL PRIMARY KEY,
            batch_time TIMESTAMP,
            symbol VARCHAR(20),
            avg_price NUMERIC,
            min_price NUMERIC,
            max_price NUMERIC,
            total_volume BIGINT,
            event_count INTEGER,
            latest_event_time TIMESTAMP,
            inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    connection.commit()
    cursor.close()
    connection.close()


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("marketpulse-spark-processor")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,org.postgresql:postgresql:42.7.4",
        )
        .getOrCreate()
    )


def create_stock_schema() -> StructType:
    return StructType(
        [
            StructField("symbol", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("open", DoubleType(), True),
            StructField("high", DoubleType(), True),
            StructField("low", DoubleType(), True),
            StructField("volume", LongType(), True),
            StructField("event_time", StringType(), True),
        ]
    )


def write_batch_to_postgres(batch_df, batch_id) -> None:
    if batch_df.rdd.isEmpty():
        return

    print(f"Writing curated micro-batch: {batch_id}")
    (
        batch_df.write.format("jdbc")
        .option("url", f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
        .option("dbtable", "stock_prices_curated")
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )


def run_spark_processor() -> None:
    ensure_curated_objects()
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    stock_schema = create_stock_schema()

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed_stream = raw_stream.selectExpr("CAST(value AS STRING) AS json_payload")

    stock_stream = (
        parsed_stream.selectExpr(
            "from_json(json_payload, 'symbol STRING, price DOUBLE, open DOUBLE, high DOUBLE, low DOUBLE, volume BIGINT, event_time STRING') AS stock"
        )
        .select("stock.*")
        .withColumn("event_time", to_timestamp(col("event_time")))
    )

    curated_stream = (
        stock_stream.withWatermark("event_time", "2 minutes")
        .groupBy(window(col("event_time"), "1 minute"), col("symbol"))
        .agg(
            spark_sum("volume").alias("total_volume"),
            count("*").alias("event_count"),
            spark_min("price").alias("min_price"),
            spark_max("price").alias("max_price"),
            spark_sum("price").alias("price_sum"),
            spark_max("event_time").alias("latest_event_time"),
        )
        .withColumn("avg_price", col("price_sum") / col("event_count"))
        .select(
            col("window.end").alias("batch_time"),
            col("symbol"),
            col("avg_price"),
            col("min_price"),
            col("max_price"),
            col("total_volume"),
            col("event_count"),
            col("latest_event_time"),
        )
    )

    query = (
        curated_stream.writeStream.outputMode("update")
        .foreachBatch(write_batch_to_postgres)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .start()
    )

    print("Spark processor started.")
    print(f"Reading from Kafka topic: {KAFKA_TOPIC}")
    print("Writing curated data to PostgreSQL table: stock_prices_curated")

    query.awaitTermination()


if __name__ == "__main__":
    run_spark_processor()
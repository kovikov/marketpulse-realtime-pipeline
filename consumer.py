import json
import os

import psycopg2
from psycopg2.extras import Json
from dotenv import load_dotenv
from kafka import KafkaConsumer

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "stock_prices")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "stock_market")
POSTGRES_USER = os.getenv("POSTGRES_USER", "marketpulse")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "marketpulse123")

REQUIRED_EVENT_FIELDS = (
    "symbol",
    "price",
    "open",
    "high",
    "low",
    "volume",
    "event_time",
)
REPORT_EVERY_N_EVENTS = 50


def create_database_connection():
    connection = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )
    return connection


def create_support_tables(connection):
    cursor = connection.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS stock_prices (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(20),
            price NUMERIC,
            open_price NUMERIC,
            high_price NUMERIC,
            low_price NUMERIC,
            volume BIGINT,
            event_time TIMESTAMP,
            inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS dead_letter_events (
            id SERIAL PRIMARY KEY,
            source_topic VARCHAR(255),
            source_partition INTEGER,
            source_offset BIGINT,
            error_message TEXT,
            payload JSONB,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS pipeline_metrics (
            id SERIAL PRIMARY KEY,
            metric_name VARCHAR(100),
            metric_value DOUBLE PRECISION,
            extra_data JSONB,
            recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    connection.commit()
    cursor.close()


def insert_stock_event(connection, event):
    cursor = connection.cursor()

    cursor.execute(
        """
        INSERT INTO stock_prices (
            symbol,
            price,
            open_price,
            high_price,
            low_price,
            volume,
            event_time
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s);
        """,
        (
            event["symbol"],
            event["price"],
            event["open"],
            event["high"],
            event["low"],
            event["volume"],
            event["event_time"],
        ),
    )

    connection.commit()
    cursor.close()


def insert_dead_letter_event(connection, message, payload, error_message):
    cursor = connection.cursor()

    cursor.execute(
        """
        INSERT INTO dead_letter_events (
            source_topic,
            source_partition,
            source_offset,
            error_message,
            payload
        )
        VALUES (%s, %s, %s, %s, %s);
        """,
        (
            message.topic,
            message.partition,
            message.offset,
            error_message,
            Json(payload),
        ),
    )

    connection.commit()
    cursor.close()


def insert_pipeline_metric(connection, metric_name, metric_value, extra_data):
    cursor = connection.cursor()

    cursor.execute(
        """
        INSERT INTO pipeline_metrics (
            metric_name,
            metric_value,
            extra_data
        )
        VALUES (%s, %s, %s);
        """,
        (metric_name, float(metric_value), Json(extra_data)),
    )

    connection.commit()
    cursor.close()


def validate_event(event):
    if not isinstance(event, dict):
        return False, "Event payload is not a JSON object"

    missing_fields = [field for field in REQUIRED_EVENT_FIELDS if field not in event]
    if missing_fields:
        return False, f"Missing required fields: {', '.join(missing_fields)}"

    try:
        float(event["price"])
        float(event["open"])
        float(event["high"])
        float(event["low"])
        int(event["volume"])
    except (TypeError, ValueError):
        return False, "Numeric fields contain invalid values"

    if not str(event["symbol"]).strip():
        return False, "Symbol is empty"

    return True, ""


def create_kafka_consumer():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="marketpulse-stock-consumer",
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )

    return consumer


def run_consumer():
    connection = create_database_connection()
    create_support_tables(connection)

    consumer = create_kafka_consumer()

    print("Stock consumer started.")
    print("Reading from Kafka topic:", KAFKA_TOPIC)

    processed_count = 0
    inserted_count = 0
    failed_count = 0

    try:
        for message in consumer:
            event = message.value
            processed_count += 1

            is_valid, validation_error = validate_event(event)
            if not is_valid:
                failed_count += 1
                insert_dead_letter_event(connection, message, event, validation_error)
                print("Invalid event moved to dead letter table:", validation_error)
                continue

            try:
                insert_stock_event(connection, event)
                inserted_count += 1
                print("Inserted:", event)

            except Exception as error:
                failed_count += 1
                error_message = str(error)
                insert_dead_letter_event(connection, message, event, error_message)
                print("Error inserting event. Moved to dead letter table:")
                print(event)
                print(error_message)

            if processed_count % REPORT_EVERY_N_EVENTS == 0:
                summary = {
                    "processed": processed_count,
                    "inserted": inserted_count,
                    "failed": failed_count,
                    "topic": KAFKA_TOPIC,
                }
                insert_pipeline_metric(
                    connection,
                    "consumer_success_ratio",
                    inserted_count / processed_count if processed_count else 0,
                    summary,
                )
                print("Consumer health summary:", summary)

    finally:
        connection.close()
        consumer.close()


if __name__ == "__main__":
    run_consumer()

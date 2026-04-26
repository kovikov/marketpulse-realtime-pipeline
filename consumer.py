import json
import os

import psycopg2
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


def create_database_connection():
    connection = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )
    return connection


def create_stock_table(connection):
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
    create_stock_table(connection)

    consumer = create_kafka_consumer()

    print("Stock consumer started.")
    print("Reading from Kafka topic:", KAFKA_TOPIC)

    for message in consumer:
        event = message.value

        try:
            insert_stock_event(connection, event)
            print("Inserted:", event)

        except Exception as error:
            print("Error inserting event:")
            print(event)
            print(error)


if __name__ == "__main__":
    run_consumer()

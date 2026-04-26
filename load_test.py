import argparse
import json
import os
import random
import time
from datetime import datetime, timezone

import psycopg2
from dotenv import load_dotenv
from kafka import KafkaProducer


load_dotenv()


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "stock_prices")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "stock_market")
POSTGRES_USER = os.getenv("POSTGRES_USER", "marketpulse")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "marketpulse123")


def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )


def create_db_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


def get_stock_row_count() -> int:
    connection = create_db_connection()
    cursor = connection.cursor()
    cursor.execute("SELECT COUNT(1) FROM stock_prices;")
    row_count = int(cursor.fetchone()[0])
    cursor.close()
    connection.close()
    return row_count


def build_event(symbol):
    price = round(random.uniform(50, 500), 2)
    return {
        "symbol": symbol,
        "price": price,
        "open": round(price - random.uniform(0, 2), 2),
        "high": round(price + random.uniform(0, 2), 2),
        "low": round(max(0.01, price - random.uniform(0, 2)), 2),
        "volume": random.randint(1000, 5000000),
        "event_time": datetime.now(timezone.utc).isoformat(),
    }


def parse_args():
    parser = argparse.ArgumentParser(description="MarketPulse pipeline load test")
    parser.add_argument("--count", type=int, default=1000, help="Number of events to publish")
    parser.add_argument(
        "--symbols",
        type=str,
        default="AAPL,MSFT,GOOGL,TSLA,NVDA",
        help="Comma-separated symbol list",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Producer flush interval",
    )
    parser.add_argument(
        "--wait-seconds",
        type=int,
        default=30,
        help="Seconds to wait for consumer inserts",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    symbols = [item.strip().upper() for item in args.symbols.split(",") if item.strip()]

    start_rows = get_stock_row_count()
    producer = create_producer()

    print("Load test started")
    print("Kafka topic:", KAFKA_TOPIC)
    print("Events to publish:", args.count)
    print("Starting row count:", start_rows)

    start_time = time.time()
    for index in range(args.count):
        symbol = symbols[index % len(symbols)]
        producer.send(KAFKA_TOPIC, build_event(symbol))

        if (index + 1) % args.batch_size == 0:
            producer.flush()

    producer.flush()
    publish_elapsed = time.time() - start_time
    publish_rate = args.count / publish_elapsed if publish_elapsed else 0

    print(f"Publish completed in {publish_elapsed:.2f}s ({publish_rate:.2f} events/s)")
    print("Waiting for consumer to persist events...")

    deadline = time.time() + args.wait_seconds
    latest_rows = start_rows
    while time.time() < deadline:
        latest_rows = get_stock_row_count()
        if latest_rows >= start_rows + args.count:
            break
        time.sleep(2)

    inserted_delta = latest_rows - start_rows
    print("Ending row count:", latest_rows)
    print("Rows inserted during test:", inserted_delta)
    print("Estimated end-to-end completion:", "PASS" if inserted_delta > 0 else "CHECK")


if __name__ == "__main__":
    main()
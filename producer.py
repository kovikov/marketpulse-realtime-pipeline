import json
import os
import time
from datetime import datetime, timezone

import yfinance as yf
from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "stock_prices")
STOCK_SYMBOLS = os.getenv("STOCK_SYMBOLS", "AAPL,MSFT,GOOGL").split(",")
FETCH_INTERVAL_SECONDS = int(os.getenv("FETCH_INTERVAL_SECONDS", "10"))


def create_kafka_producer():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )
    return producer


def fetch_stock_price(symbol):
    ticker = yf.Ticker(symbol)
    data = ticker.history(period="1d", interval="1m")

    if data.empty:
        return None

    latest_row = data.tail(1).iloc[0]

    stock_event = {
        "symbol": symbol,
        "price": round(float(latest_row["Close"]), 2),
        "open": round(float(latest_row["Open"]), 2),
        "high": round(float(latest_row["High"]), 2),
        "low": round(float(latest_row["Low"]), 2),
        "volume": int(latest_row["Volume"]),
        "event_time": datetime.now(timezone.utc).isoformat(),
    }

    return stock_event


def run_producer():
    producer = create_kafka_producer()

    print("Stock producer started.")
    print("Sending data to Kafka topic:", KAFKA_TOPIC)

    while True:
        for symbol in STOCK_SYMBOLS:
            symbol = symbol.strip().upper()

            try:
                stock_event = fetch_stock_price(symbol)

                if stock_event is not None:
                    producer.send(KAFKA_TOPIC, stock_event)
                    producer.flush()
                    print("Sent:", stock_event)
                else:
                    print("No data found for:", symbol)

            except Exception as error:
                print("Error fetching data for", symbol)
                print(error)

        time.sleep(FETCH_INTERVAL_SECONDS)


if __name__ == "__main__":
    run_producer()

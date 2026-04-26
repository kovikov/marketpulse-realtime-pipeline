# MarketPulse Real-Time Stock Market Insights

This project builds a real-time stock market data pipeline using Python, Kafka, PostgreSQL, Docker, and Streamlit.

## Project Flow

1. Python producer collects stock prices.
2. Producer sends stock events to Kafka.
3. Python consumer reads events from Kafka.
4. Consumer saves processed events into PostgreSQL.
5. Streamlit dashboard visualizes latest stock trends.

## Tech Stack

- Python
- Apache Kafka
- PostgreSQL
- Docker
- Streamlit
- Plotly

## How to Run

Start infrastructure:

```bash
docker compose up -d
```

Activate the virtual environment:

```bash
venv\Scripts\activate
```

Install dependencies:

```bash
pip install -r requirements.txt
```

Run the Kafka producer:

```bash
python producer.py
```

Run the Kafka consumer:

```bash
python consumer.py
```

Start the Streamlit dashboard:

```bash
streamlit run dashboard.py
```

Open the dashboard in your browser:

```text
http://localhost:8501
```

## Environment Variables

The project uses [.env](.env) for configuration, including:

- Kafka bootstrap server
- Kafka topic name
- PostgreSQL host, port, database, username, and password
- Stock symbols to fetch
- Fetch interval in seconds
- RapidAPI key

## Notes

- Make sure Docker Desktop is running before starting the containers.
- Kafka runs on port `9092` and PostgreSQL runs on port `5432`.
- The consumer automatically creates the `stock_prices` table if it does not exist.
# marketpulse-realtime-pipeline

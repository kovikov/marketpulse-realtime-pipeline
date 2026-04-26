# MarketPulse Real-Time Stock Market Insights

This project builds a real-time stock market data pipeline using Python, Kafka, PostgreSQL, Docker, and Streamlit.

It now includes a Spark structured-streaming processor for curated analytics output that can be consumed by Power BI.

## Project Flow

1. Python producer collects stock prices.
2. Producer sends stock events to Kafka.
3. Python consumer reads events from Kafka.
4. Consumer saves processed events into PostgreSQL.
5. Streamlit dashboard visualizes latest stock trends.

## Tech Stack

- Python
- Apache Kafka
- Apache Spark (Structured Streaming)
- PostgreSQL
- Docker
- Streamlit
- Plotly
- Power BI (views provided via SQL)

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

Run the Spark processor (curated aggregates):

```bash
python spark_processor.py
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
- Kafka runs on port `9092` and Docker PostgreSQL is exposed on host port `55432`.
- The consumer automatically creates the `stock_prices` table if it does not exist.
- The Spark processor automatically creates the `stock_prices_curated` table if it does not exist.

## Monitoring and Dead Letter Handling

The consumer now includes baseline operational controls:

- Event validation before database writes
- Dead letter persistence for invalid or failed events (`dead_letter_events`)
- Periodic consumer health metrics (`pipeline_metrics`)

Check dead letter events:

```bash
docker exec marketpulse-postgres psql -U marketpulse -d stock_market -c "SELECT id, source_topic, error_message, created_at FROM dead_letter_events ORDER BY created_at DESC LIMIT 10;"
```

Check consumer metrics:

```bash
docker exec marketpulse-postgres psql -U marketpulse -d stock_market -c "SELECT metric_name, metric_value, recorded_at FROM pipeline_metrics ORDER BY recorded_at DESC LIMIT 10;"
```

## Power BI Setup

Run the view script:

```bash
docker exec -i marketpulse-postgres psql -U marketpulse -d stock_market < powerbi_views.sql
```

In Power BI Desktop, connect to PostgreSQL using:

- Server: `localhost`
- Port: `55432`
- Database: `stock_market`
- Username: `marketpulse`
- Password: `marketpulse123`

Recommended reporting objects:

- `stock_prices` (raw events)
- `stock_prices_curated` (Spark aggregations)
- `vw_stock_latest`
- `vw_stock_curated_recent`
- `vw_stock_latency`

## Assignment Coverage Snapshot

Implemented:

- Kafka streaming ingest
- Real-time consumer to PostgreSQL
- Spark structured-streaming processor to curated table
- Interactive real-time dashboard (Streamlit)
- Power BI-ready SQL views
- Baseline monitoring tables and dead-letter handling

Not yet implemented:

- Automated alerting integrations (email/Slack/PagerDuty)
- Multi-source feeds (news and sentiment)
- Fault-injection/load tests and SLA benchmarks

# MarketPulse Submission Checklist

## Completed Core Deliverables

- [x] Kafka producer publishes stock events in real time
- [x] Kafka consumer persists events to PostgreSQL (`stock_prices`)
- [x] Streamlit dashboard shows live trend, volume, and recent records
- [x] Dockerized infra for Kafka, ZooKeeper, and PostgreSQL
- [x] Spark structured streaming processor writes curated aggregates (`stock_prices_curated`)
- [x] Power BI reporting views created (`vw_stock_latest`, `vw_stock_curated_recent`, `vw_stock_latency`)
- [x] Dead-letter handling for invalid/failed events (`dead_letter_events`)
- [x] Consumer baseline metrics table (`pipeline_metrics`)

## Verification Commands

### 1) Start infrastructure

```bash
docker compose up -d
```

### 2) Start pipeline services (three terminals)

```bash
python producer.py
```

```bash
python consumer.py
```

```bash
streamlit run dashboard.py
```

### 3) Start Spark processor (fourth terminal)

```bash
python spark_processor.py
```

### 4) Confirm data persisted

```bash
docker exec marketpulse-postgres psql -U marketpulse -d stock_market -c "SELECT * FROM stock_prices ORDER BY inserted_at DESC LIMIT 10;"
```

### 5) Confirm curated output

```bash
docker exec marketpulse-postgres psql -U marketpulse -d stock_market -c "SELECT * FROM stock_prices_curated ORDER BY inserted_at DESC LIMIT 10;"
```

### 6) Confirm dead letter and metrics

```bash
docker exec marketpulse-postgres psql -U marketpulse -d stock_market -c "SELECT id, error_message, created_at FROM dead_letter_events ORDER BY created_at DESC LIMIT 10;"
```

```bash
docker exec marketpulse-postgres psql -U marketpulse -d stock_market -c "SELECT metric_name, metric_value, recorded_at FROM pipeline_metrics ORDER BY recorded_at DESC LIMIT 10;"
```

### 7) Optional load benchmark

```bash
python load_test.py --count 2000 --batch-size 200 --wait-seconds 45
```

## Remaining Stretch Goals

- [ ] Alerting integration to Slack/Teams/PagerDuty using `ALERT_WEBHOOK_URL`
- [ ] Multi-source feeds (news/sentiment)
- [ ] Formal performance benchmark report (latency percentiles, throughput at peak)
CREATE OR REPLACE VIEW vw_stock_latest AS
SELECT DISTINCT ON (symbol)
    symbol,
    price,
    open_price,
    high_price,
    low_price,
    volume,
    event_time,
    inserted_at
FROM stock_prices
ORDER BY symbol, event_time DESC;

CREATE OR REPLACE VIEW vw_stock_curated_recent AS
SELECT
    symbol,
    batch_time,
    avg_price,
    min_price,
    max_price,
    total_volume,
    event_count,
    latest_event_time,
    inserted_at
FROM stock_prices_curated
ORDER BY batch_time DESC;

CREATE OR REPLACE VIEW vw_stock_latency AS
SELECT
    symbol,
    batch_time,
    latest_event_time,
    EXTRACT(EPOCH FROM (batch_time - latest_event_time)) AS latency_seconds,
    inserted_at
FROM stock_prices_curated
ORDER BY batch_time DESC;
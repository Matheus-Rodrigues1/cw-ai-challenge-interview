-- Drop existing tables to recreate with the correct schema
DROP TABLE IF EXISTS checkout, transactions_db, transactions, transactions_auth_codes CASCADE;

-- Create tables
CREATE TABLE IF NOT EXISTS checkout (
    time VARCHAR(10),
    today INTEGER,
    yesterday INTEGER,
    same_day_last_week INTEGER,
    avg_last_week DECIMAL(10,2),
    avg_last_month DECIMAL(10,2)
);

CREATE TABLE IF NOT EXISTS transactions_db (
    day DATE,
    entity VARCHAR(10),
    product VARCHAR(50),
    price_tier VARCHAR(50),
    anticipation_method VARCHAR(50),
    nitro_or_d0 VARCHAR(50),
    payment_method VARCHAR(50),
    installments INTEGER,
    amount_transacted DECIMAL(15,2),
    quantity_transactions INTEGER,
    quantitu_of_merchants INTEGER
);

CREATE TABLE IF NOT EXISTS transactions (
    timestamp TIMESTAMP,
    status VARCHAR(20),
    count INTEGER
);

CREATE TABLE IF NOT EXISTS transactions_auth_codes (
    timestamp TIMESTAMP,
    auth_code VARCHAR(20),
    count INTEGER
);

CREATE TABLE IF NOT EXISTS anomaly_results (
    id SERIAL PRIMARY KEY,
    evaluated_at TIMESTAMP DEFAULT NOW(),
    timestamp TIMESTAMP NOT NULL,
    is_anomaly BOOLEAN NOT NULL,
    anomaly_score DECIMAL(6,3) NOT NULL,
    alert_level VARCHAR(10) NOT NULL,
    triggered_rules TEXT,
    z_scores JSONB,
    iso_score DECIMAL(6,3),
    counts JSONB,
    notified_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_anomaly_results_timestamp ON anomaly_results(timestamp);
CREATE INDEX IF NOT EXISTS idx_anomaly_results_alert_level ON anomaly_results(alert_level);
CREATE INDEX IF NOT EXISTS idx_anomaly_results_notified ON anomaly_results(notified_at) WHERE notified_at IS NULL;

CREATE TABLE IF NOT EXISTS notification_log (
    id SERIAL PRIMARY KEY,
    anomaly_id INTEGER REFERENCES anomaly_results(id),
    channel VARCHAR(20) NOT NULL,
    status VARCHAR(20) NOT NULL,
    sent_at TIMESTAMP DEFAULT NOW(),
    error_message TEXT
);

CREATE INDEX IF NOT EXISTS idx_notification_log_anomaly ON notification_log(anomaly_id);

-- AI/ML detection results (populated by worker-ai-ml — independent of rule-based anomaly_results)
CREATE TABLE IF NOT EXISTS ai_anomaly_results (
    id                   SERIAL PRIMARY KEY,
    detected_at          TIMESTAMP DEFAULT NOW(),
    data_timestamp       TIMESTAMP,
    source               VARCHAR(50) DEFAULT 'transactions',
    feature_snapshot     JSONB,
    if_score             DOUBLE PRECISION,
    lof_score            DOUBLE PRECISION,
    ensemble_score       DOUBLE PRECISION,
    is_anomaly           BOOLEAN     NOT NULL,
    alert_level          VARCHAR(20) NOT NULL,
    model_version        VARCHAR(50),
    num_training_samples INTEGER
);

CREATE INDEX IF NOT EXISTS idx_ai_ts    ON ai_anomaly_results(data_timestamp);
CREATE INDEX IF NOT EXISTS idx_ai_level ON ai_anomaly_results(alert_level);
CREATE INDEX IF NOT EXISTS idx_ai_det   ON ai_anomaly_results(detected_at);

-- Truncate load tables before import
TRUNCATE checkout, transactions_db, transactions, transactions_auth_codes;

-- Load CSV data
COPY checkout FROM '/csv_data/checkout_1.csv' DELIMITER ',' CSV HEADER;
COPY checkout FROM '/csv_data/checkout_2.csv' DELIMITER ',' CSV HEADER;
COPY transactions_db FROM '/csv_data/operational_intelligence_transactions_db.csv' DELIMITER ',' CSV HEADER;
COPY transactions FROM '/csv_data/transactions.csv' DELIMITER ',' CSV HEADER;
COPY transactions_auth_codes FROM '/csv_data/transactions_auth_codes.csv' DELIMITER ',' CSV HEADER;

-- Monitoring views (denied / failed / reversals per minute — used by dashboards & SQL analytics)
CREATE OR REPLACE VIEW monitoring_minute_pivot AS
SELECT
    date_trunc('minute', t."timestamp") AS minute_ts,
    SUM(CASE WHEN t.status = 'approved' THEN t.count ELSE 0 END)          AS approved,
    SUM(CASE WHEN t.status = 'denied' THEN t.count ELSE 0 END)            AS denied,
    SUM(CASE WHEN t.status = 'failed' THEN t.count ELSE 0 END)            AS failed,
    SUM(CASE WHEN t.status = 'reversed' THEN t.count ELSE 0 END)          AS reversed,
    SUM(CASE WHEN t.status = 'backend_reversed' THEN t.count ELSE 0 END) AS backend_reversed,
    SUM(CASE WHEN t.status = 'refunded' THEN t.count ELSE 0 END)          AS refunded,
    SUM(t.count)                                                          AS total_count
FROM transactions t
GROUP BY 1;

CREATE OR REPLACE VIEW monitoring_minute_with_rollups AS
SELECT
    minute_ts,
    denied,
    failed,
    reversed,
    backend_reversed,
    AVG(denied::numeric) OVER (ORDER BY minute_ts ROWS BETWEEN 59 PRECEDING AND CURRENT ROW)  AS denied_rolling_avg_60m,
    AVG(failed::numeric) OVER (ORDER BY minute_ts ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS failed_rolling_avg_60m,
    AVG(reversed::numeric) OVER (ORDER BY minute_ts ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS reversed_rolling_avg_60m
FROM monitoring_minute_pivot;

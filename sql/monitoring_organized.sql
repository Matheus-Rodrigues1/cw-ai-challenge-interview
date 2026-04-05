-- Organized monitoring view: per-minute pivoted counts for denied, failed, and reversals
-- (challenge: alert when these are above normal — baselines are computed in the app/model).
-- Compatible with PostgreSQL (same schema as CSV-loaded tables).

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

-- Rolling companion: optional window stats (requires PostgreSQL window functions)
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

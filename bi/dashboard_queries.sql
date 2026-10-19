-- =====================================================================
-- BI dashboard cards (Metabase / Superset / Snowsight / Power BI).
-- Local: Metabase at http://localhost:3000 -> add PostgreSQL "fraud_dw".
-- Production: point the BI tool at Snowflake with role FRAUD_BI_READER
-- (PII is masked automatically). All cards read the MART layer only.
-- =====================================================================

-- [Card 1] KPI tiles - today (WIB)
SELECT SUM(txn_count) AS transaksi, SUM(total_amount_idr) AS nominal_idr,
       SUM(flagged_count) AS ditandai, SUM(hold_count) AS ditahan,
       SUM(flagged_amount_idr) AS nominal_berisiko
FROM mart.v_fraud_kpi_hourly
WHERE event_hour >= date_trunc('day', (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Jakarta')) - INTERVAL '7 hours';

-- [Card 2] Trend per hour & channel (stacked bar), last 48h
SELECT event_hour, channel, txn_count, flagged_count
FROM mart.v_fraud_kpi_hourly
WHERE event_hour >= (CURRENT_TIMESTAMP AT TIME ZONE 'UTC') - INTERVAL '48 hours'
ORDER BY event_hour;

-- [Card 3] Flag rate per channel (bar)
SELECT channel, SUM(flagged_count) * 1.0 / NULLIF(SUM(txn_count), 0) AS flag_rate
FROM mart.v_fraud_kpi_hourly
WHERE event_hour >= (CURRENT_TIMESTAMP AT TIME ZONE 'UTC') - INTERVAL '24 hours'
GROUP BY channel ORDER BY flag_rate DESC;

-- [Card 4] Open alert queue (table) - highest risk first
SELECT alert_id, event_ts, channel, account_id, amount_idr, risk_score, decision, reason_codes, explanation
FROM mart.v_alert_investigation
WHERE disposition = 'PENDING'
ORDER BY risk_score DESC, event_ts DESC
LIMIT 200;

-- [Card 5] Reason-code mix (donut)
SELECT rule_code, COUNT(*) AS hits
FROM core.transaction_rule_hits
WHERE event_ts >= (CURRENT_TIMESTAMP AT TIME ZONE 'UTC') - INTERVAL '24 hours'
GROUP BY rule_code ORDER BY hits DESC;

-- [Card 6] Rule precision from analyst feedback (table)
SELECT rule_code, rules_version, hits, confirmed_fraud, false_positive, pending_review, precision_ratio
FROM mart.v_rule_performance ORDER BY hits DESC;

-- [Card 7] Daily summary per channel (accurate: duplicates excluded)
SELECT * FROM mart.v_channel_daily_summary ORDER BY event_date DESC, channel;

-- [Card 8] Data quality trend
SELECT batch_date, input_rows, valid_rows, exact_duplicates, cross_duplicates, quarantined_rows, quarantine_ratio
FROM mart.v_data_quality_daily ORDER BY batch_date DESC;

-- [Card 9] Pipeline freshness (seconds since last batch)
SELECT pipeline, last_batch_ts, last_event_ts,
       EXTRACT(EPOCH FROM ((CURRENT_TIMESTAMP AT TIME ZONE 'UTC') - last_batch_ts)) AS seconds_since_last_batch
FROM mart.v_pipeline_health;

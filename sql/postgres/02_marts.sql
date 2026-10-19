-- =====================================================================
-- Gold layer: BI-facing views (PostgreSQL / DuckDB compatible).
-- Duplicates (is_duplicate = TRUE) are ALWAYS excluded from business KPIs.
-- =====================================================================

-- Near-real-time KPI per hour & channel
CREATE OR REPLACE VIEW mart.v_fraud_kpi_hourly AS
SELECT
    date_trunc('hour', event_ts)                                         AS event_hour,
    channel,
    COUNT(*)                                                             AS txn_count,
    SUM(amount_idr)                                                      AS total_amount_idr,
    COUNT(*) FILTER (WHERE decision IN ('REVIEW', 'HOLD'))               AS flagged_count,
    COUNT(*) FILTER (WHERE decision = 'HOLD')                            AS hold_count,
    SUM(amount_idr) FILTER (WHERE decision IN ('REVIEW', 'HOLD'))        AS flagged_amount_idr,
    AVG(risk_score)                                                      AS avg_risk_score,
    COUNT(DISTINCT account_id)                                           AS active_accounts
FROM core.fct_transactions
WHERE NOT is_duplicate
GROUP BY 1, 2;

-- Daily channel summary (accurate aggregates: duplicates removed)
CREATE OR REPLACE VIEW mart.v_channel_daily_summary AS
SELECT
    CAST(event_ts AS DATE)                                               AS event_date,
    channel,
    COUNT(*)                                                             AS txn_count,
    COUNT(*) FILTER (WHERE status = 'SUCCESS')                           AS success_count,
    COUNT(*) FILTER (WHERE status = 'DECLINED')                          AS declined_count,
    SUM(amount_idr) FILTER (WHERE status = 'SUCCESS')                    AS success_amount_idr,
    COUNT(DISTINCT account_id)                                           AS active_accounts,
    COUNT(*) FILTER (WHERE decision IN ('REVIEW', 'HOLD'))               AS flagged_count
FROM core.fct_transactions
WHERE NOT is_duplicate
GROUP BY 1, 2;

-- Alert queue for investigators, with every reason in one place
CREATE OR REPLACE VIEW mart.v_alert_investigation AS
SELECT
    a.alert_id,
    a.txn_id,
    a.created_at,
    a.event_ts,
    a.channel,
    a.account_id,
    a.customer_id,
    a.amount_idr,
    a.risk_score,
    a.risk_level,
    a.decision,
    a.reason_codes,
    a.explanation,
    t.txn_type,
    t.status,
    t.merchant_name,
    t.mcc,
    t.counterparty_account,
    t.city,
    t.country_code,
    t.device_id,
    t.rules_version,
    t.features,
    COALESCE(f.disposition, 'PENDING')                                   AS disposition,
    f.analyst
FROM core.fraud_alerts a
JOIN core.fct_transactions t ON t.txn_id = a.txn_id
LEFT JOIN core.fraud_case_feedback f ON f.txn_id = a.txn_id;

-- Rule effectiveness (feedback loop): precision per rule
CREATE OR REPLACE VIEW mart.v_rule_performance AS
SELECT
    h.rule_code,
    h.rules_version,
    COUNT(*)                                                             AS hits,
    COUNT(*) FILTER (WHERE f.disposition = 'CONFIRMED_FRAUD')            AS confirmed_fraud,
    COUNT(*) FILTER (WHERE f.disposition = 'FALSE_POSITIVE')             AS false_positive,
    COUNT(*) FILTER (WHERE f.disposition IS NULL)                        AS pending_review,
    CAST(COUNT(*) FILTER (WHERE f.disposition = 'CONFIRMED_FRAUD') AS DOUBLE PRECISION)
        / NULLIF(COUNT(*) FILTER (WHERE f.disposition IS NOT NULL), 0)   AS precision_ratio
FROM core.transaction_rule_hits h
LEFT JOIN core.fraud_case_feedback f ON f.txn_id = h.txn_id
GROUP BY 1, 2;

-- Data quality per day: valid vs quarantined vs duplicates
CREATE OR REPLACE VIEW mart.v_data_quality_daily AS
SELECT
    CAST(batch_ts AS DATE)                                               AS batch_date,
    SUM(input_rows)                                                      AS input_rows,
    SUM(valid_rows)                                                      AS valid_rows,
    SUM(exact_duplicates)                                                AS exact_duplicates,
    SUM(cross_duplicates)                                                AS cross_duplicates,
    SUM(quarantined_rows)                                                AS quarantined_rows,
    CAST(SUM(quarantined_rows) AS DOUBLE PRECISION) / NULLIF(SUM(input_rows), 0) AS quarantine_ratio
FROM ops.pipeline_batch_metrics
GROUP BY 1;

-- Quarantine breakdown by error code(s)
CREATE OR REPLACE VIEW mart.v_quarantine_by_error AS
SELECT
    CAST(ingest_ts AS DATE) AS ingest_date,
    source_topic,
    dq_errors,
    COUNT(*)                AS events
FROM core.quarantine_events
GROUP BY 1, 2, 3;

-- Pipeline health: how stale is each pipeline?
CREATE OR REPLACE VIEW mart.v_pipeline_health AS
SELECT
    pipeline,
    last_batch_id,
    last_batch_ts,
    last_event_ts,
    status
FROM ops.pipeline_heartbeat;

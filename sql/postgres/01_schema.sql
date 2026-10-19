-- =====================================================================
-- Local warehouse schema (PostgreSQL / DuckDB compatible subset).
-- Production uses Snowflake: see sql/snowflake/. Columns must stay in sync
-- with src/fraud_platform/sinks/tables.py.
-- All timestamps are UTC (naive).
-- =====================================================================
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS ops;
CREATE SCHEMA IF NOT EXISTS mart;

-- Silver: canonical, de-duplicated, scored transactions (1 row per txn_id)
CREATE TABLE IF NOT EXISTS core.fct_transactions (
    txn_id               TEXT PRIMARY KEY,
    source_txn_id        TEXT NOT NULL,
    idempotency_key      TEXT NOT NULL,
    content_fingerprint  TEXT,
    channel              TEXT NOT NULL,
    customer_id          TEXT,
    account_id           TEXT NOT NULL,
    txn_type             TEXT NOT NULL,
    status               TEXT NOT NULL,
    amount               DOUBLE PRECISION NOT NULL,
    currency             TEXT NOT NULL,
    amount_idr           DOUBLE PRECISION NOT NULL,
    event_ts             TIMESTAMP NOT NULL,
    device_id            TEXT,
    ip_address           TEXT,
    merchant_name        TEXT,
    mcc                  TEXT,
    counterparty_account TEXT,
    counterparty_bank    TEXT,
    city                 TEXT,
    country_code         TEXT,
    latitude             DOUBLE PRECISION,
    longitude            DOUBLE PRECISION,
    source_topic         TEXT,
    source_partition     BIGINT,
    source_offset        BIGINT,
    ingest_ts            TIMESTAMP,
    is_duplicate         BOOLEAN NOT NULL DEFAULT FALSE,   -- TRUE = cross-channel copy, excluded from KPIs
    duplicate_type       TEXT,
    duplicate_of         TEXT,
    risk_score           DOUBLE PRECISION,
    risk_level           TEXT,
    decision             TEXT,
    reason_codes         TEXT,
    explanation          TEXT,
    rule_hits            TEXT,           -- JSON array
    features             TEXT,           -- JSON object
    rules_version        TEXT,
    rules_hash           TEXT,
    scored_at            TIMESTAMP,
    loaded_at            TIMESTAMP
);

-- Explainability: 1 row per rule that fired on a transaction
CREATE TABLE IF NOT EXISTS core.transaction_rule_hits (
    txn_id        TEXT NOT NULL,
    rule_code     TEXT NOT NULL,
    weight        DOUBLE PRECISION,
    explanation   TEXT,
    evidence      TEXT,                  -- JSON object
    account_id    TEXT,
    channel       TEXT,
    event_ts      TIMESTAMP,
    rules_version TEXT,
    scored_at     TIMESTAMP,
    PRIMARY KEY (txn_id, rule_code)
);

-- Alert queue for the fraud operations team (REVIEW / HOLD)
CREATE TABLE IF NOT EXISTS core.fraud_alerts (
    alert_id      TEXT PRIMARY KEY,
    txn_id        TEXT NOT NULL,
    account_id    TEXT,
    customer_id   TEXT,
    channel       TEXT,
    event_ts      TIMESTAMP,
    amount_idr    DOUBLE PRECISION,
    risk_score    DOUBLE PRECISION,
    risk_level    TEXT,
    decision      TEXT,
    reason_codes  TEXT,
    explanation   TEXT,
    alert_status  TEXT,                  -- OPEN | IN_REVIEW | CLOSED
    created_at    TIMESTAMP
);

-- Analyst disposition (from case management) -> feedback loop for rule tuning
CREATE TABLE IF NOT EXISTS core.fraud_case_feedback (
    txn_id       TEXT PRIMARY KEY,
    disposition  TEXT NOT NULL,          -- CONFIRMED_FRAUD | FALSE_POSITIVE
    analyst      TEXT,
    notes        TEXT,
    decided_at   TIMESTAMP
);

-- Audit of every duplicate message that was NOT counted
CREATE TABLE IF NOT EXISTS core.duplicate_events (
    pipeline         TEXT NOT NULL,
    batch_id         BIGINT NOT NULL,
    txn_id           TEXT,
    duplicate_type   TEXT,               -- EXACT | CROSS_CHANNEL
    duplicate_of     TEXT,
    channel          TEXT,
    account_id       TEXT,
    amount_idr       DOUBLE PRECISION,
    event_ts         TIMESTAMP,
    source_topic     TEXT,
    source_partition BIGINT,
    source_offset    BIGINT,
    detected_at      TIMESTAMP
);

-- Dead-letter / quarantine: records that failed parsing or data-quality checks
CREATE TABLE IF NOT EXISTS core.quarantine_events (
    pipeline         TEXT NOT NULL,
    batch_id         BIGINT NOT NULL,
    source_topic     TEXT,
    source_partition BIGINT,
    source_offset    BIGINT,
    channel          TEXT,
    txn_id           TEXT,
    account_id       TEXT,
    dq_errors        TEXT,               -- JSON array of DQ_* codes
    raw_payload      TEXT,
    ingest_ts        TIMESTAMP
);

-- Observability
CREATE TABLE IF NOT EXISTS ops.pipeline_batch_metrics (
    pipeline            TEXT NOT NULL,
    batch_id            BIGINT NOT NULL,
    batch_ts            TIMESTAMP NOT NULL,
    input_rows          BIGINT,
    valid_rows          BIGINT,
    exact_duplicates    BIGINT,
    cross_duplicates    BIGINT,
    quarantined_rows    BIGINT,
    alerts              BIGINT,
    hold_count          BIGINT,
    min_event_ts        TIMESTAMP,
    max_event_ts        TIMESTAMP,
    processing_seconds  DOUBLE PRECISION,
    PRIMARY KEY (pipeline, batch_id)
);

CREATE TABLE IF NOT EXISTS ops.pipeline_heartbeat (
    pipeline       TEXT PRIMARY KEY,
    last_batch_id  BIGINT,
    last_batch_ts  TIMESTAMP,
    last_event_ts  TIMESTAMP,
    status         TEXT
);

CREATE TABLE IF NOT EXISTS ops.dq_check_results (
    check_name   TEXT NOT NULL,
    run_ts       TIMESTAMP NOT NULL,
    status       TEXT NOT NULL,          -- PASS | WARN | FAIL
    observed     DOUBLE PRECISION,
    threshold    DOUBLE PRECISION,
    details      TEXT,
    PRIMARY KEY (check_name, run_ts)
);

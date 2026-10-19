"""Warehouse table contracts (column order + keys). Must match sql/*/01_schema.sql."""
from __future__ import annotations

from ..core.schema import CANONICAL_FIELDS, SCORING_FIELDS, field_names

FCT_TRANSACTIONS = (
    field_names(CANONICAL_FIELDS)
    + ["source_topic", "source_partition", "source_offset", "ingest_ts"]
    + field_names(SCORING_FIELDS)
    + ["loaded_at"]
)

TABLES: dict[str, dict] = {
    "core.fct_transactions": {"columns": FCT_TRANSACTIONS, "keys": ["txn_id"]},
    "core.transaction_rule_hits": {
        "columns": ["txn_id", "rule_code", "weight", "explanation", "evidence", "account_id", "channel",
                    "event_ts", "rules_version", "scored_at"],
        "keys": ["txn_id", "rule_code"],
    },
    "core.fraud_alerts": {
        "columns": ["alert_id", "txn_id", "account_id", "customer_id", "channel", "event_ts", "amount_idr",
                    "risk_score", "risk_level", "decision", "reason_codes", "explanation", "alert_status",
                    "created_at"],
        "keys": ["alert_id"],
    },
    "core.fraud_case_feedback": {
        "columns": ["txn_id", "disposition", "analyst", "notes", "decided_at"],
        "keys": ["txn_id"],
    },
    "core.duplicate_events": {
        "columns": ["pipeline", "batch_id", "txn_id", "duplicate_type", "duplicate_of", "channel", "account_id",
                    "amount_idr", "event_ts", "source_topic", "source_partition", "source_offset", "detected_at"],
        "keys": None,   # append-only, replaced per (pipeline, batch_id)
    },
    "core.quarantine_events": {
        "columns": ["pipeline", "batch_id", "source_topic", "source_partition", "source_offset", "channel",
                    "txn_id", "account_id", "dq_errors", "raw_payload", "ingest_ts"],
        "keys": None,
    },
    "ops.pipeline_batch_metrics": {
        "columns": ["pipeline", "batch_id", "batch_ts", "input_rows", "valid_rows", "exact_duplicates",
                    "cross_duplicates", "quarantined_rows", "alerts", "hold_count", "min_event_ts",
                    "max_event_ts", "processing_seconds"],
        "keys": ["pipeline", "batch_id"],
    },
    "ops.pipeline_heartbeat": {
        "columns": ["pipeline", "last_batch_id", "last_batch_ts", "last_event_ts", "status"],
        "keys": ["pipeline"],
    },
    "ops.dq_check_results": {
        "columns": ["check_name", "run_ts", "status", "observed", "threshold", "details"],
        "keys": ["check_name", "run_ts"],
    },
}

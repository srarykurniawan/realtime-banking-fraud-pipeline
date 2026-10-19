"""Canonical data contracts.

Every channel publishes its own payload format. All of them are mapped into ONE
canonical transaction record (``CANONICAL_FIELDS``) before validation, de-duplication
and scoring. The same contract is used by the Spark job, the lite pipeline and the
warehouse DDL (``sql/``), so a field added here must be added there too.
"""
from __future__ import annotations

# (name, logical type) - logical types: string | double | long | timestamp | boolean
CANONICAL_FIELDS: list[tuple[str, str]] = [
    ("txn_id", "string"),               # global id: <CHANNEL>:<source_txn_id>
    ("source_txn_id", "string"),        # id from the source system
    ("idempotency_key", "string"),      # sha256(channel|source_txn_id) -> exact-duplicate key
    ("content_fingerprint", "string"),  # sha256(account|amount|counterparty|type) -> cross-channel duplicate key
    ("channel", "string"),              # MOBILE | ATM | CARD | IBANKING | PARTNER
    ("customer_id", "string"),
    ("account_id", "string"),
    ("txn_type", "string"),             # TRANSFER | WITHDRAWAL | PURCHASE | BILL_PAYMENT | TOPUP
    ("status", "string"),               # SUCCESS | DECLINED | PENDING | REVERSED
    ("amount", "double"),
    ("currency", "string"),
    ("amount_idr", "double"),
    ("event_ts", "timestamp"),          # event time, UTC (naive)
    ("device_id", "string"),
    ("ip_address", "string"),
    ("merchant_name", "string"),
    ("mcc", "string"),
    ("counterparty_account", "string"),
    ("counterparty_bank", "string"),
    ("city", "string"),
    ("country_code", "string"),
    ("latitude", "double"),
    ("longitude", "double"),
]

# Lineage + data-quality columns added by the normaliser.
INGEST_FIELDS: list[tuple[str, str]] = [
    ("source_topic", "string"),
    ("source_partition", "long"),
    ("source_offset", "long"),
    ("ingest_ts", "timestamp"),
    ("dq_status", "string"),            # VALID | INVALID
    ("dq_errors", "string"),            # JSON array of error codes
    ("raw_payload", "string"),
]

NORMALIZED_FIELDS = CANONICAL_FIELDS + INGEST_FIELDS

# Columns produced by the stateful scoring step.
SCORING_FIELDS: list[tuple[str, str]] = [
    ("is_duplicate", "boolean"),
    ("duplicate_type", "string"),       # EXACT | CROSS_CHANNEL | null
    ("duplicate_of", "string"),         # txn_id of the original record
    ("risk_score", "double"),           # 0-100
    ("risk_level", "string"),           # LOW | MEDIUM | HIGH | CRITICAL
    ("decision", "string"),             # ALLOW | REVIEW | HOLD | DUPLICATE
    ("reason_codes", "string"),         # comma separated rule codes
    ("explanation", "string"),          # human readable (Bahasa Indonesia)
    ("rule_hits", "string"),            # JSON array: [{code, weight, explanation, evidence}]
    ("features", "string"),             # JSON object of computed features (audit)
    ("rules_version", "string"),
    ("rules_hash", "string"),
    ("scored_at", "timestamp"),
]

SCORED_FIELDS = CANONICAL_FIELDS + [f for f in INGEST_FIELDS if f[0] != "raw_payload"] + SCORING_FIELDS

CHANNELS = ("MOBILE", "ATM", "CARD", "IBANKING", "PARTNER")
TXN_TYPES = ("TRANSFER", "WITHDRAWAL", "PURCHASE", "BILL_PAYMENT", "TOPUP")
STATUSES = ("SUCCESS", "DECLINED", "PENDING", "REVERSED")

REQUIRED_FIELDS = (
    "source_txn_id", "channel", "account_id", "txn_type", "status", "amount", "currency", "event_ts",
)


def field_names(fields: list[tuple[str, str]]) -> list[str]:
    return [name for name, _ in fields]


def to_spark_schema(fields: list[tuple[str, str]]):
    """Build a pyspark StructType lazily so the core package has no Spark dependency."""
    from pyspark.sql import types as T

    mapping = {
        "string": T.StringType(),
        "double": T.DoubleType(),
        "long": T.LongType(),
        "timestamp": T.TimestampType(),
        "boolean": T.BooleanType(),
    }
    return T.StructType([T.StructField(name, mapping[kind], True) for name, kind in fields])

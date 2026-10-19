"""Data quality checks + the single entry point used to ingest one raw message.

``ingest_message`` = parse -> normalise -> enrich -> validate. It ALWAYS returns a
record (never raises): invalid input is returned with ``dq_status='INVALID'`` and a
JSON list of error codes so it can be routed to quarantine and counted in monitoring.

Error codes
-----------
DQ_MALFORMED_JSON, DQ_EMPTY_PAYLOAD, DQ_UNKNOWN_CHANNEL, DQ_MISSING_<FIELD>,
DQ_INVALID_AMOUNT, DQ_AMOUNT_OUT_OF_RANGE, DQ_UNSUPPORTED_CURRENCY, DQ_INVALID_TIMESTAMP,
DQ_FUTURE_TIMESTAMP, DQ_STALE_TIMESTAMP, DQ_INVALID_TXN_TYPE, DQ_INVALID_STATUS,
DQ_INVALID_GEO, DQ_NORMALIZATION_FAILED
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any

from .normalizers import NORMALIZERS, NormalizationError, channel_from_topic, enrich_keys, parse_payload
from .schema import CANONICAL_FIELDS, REQUIRED_FIELDS, STATUSES, TXN_TYPES, field_names

_CANONICAL = field_names(CANONICAL_FIELDS)


def utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def validate_record(rec: dict, settings: dict, now: datetime | None = None) -> list[str]:
    """Return a list of DQ error codes (empty list = valid)."""
    now = now or utcnow()
    vcfg = settings.get("validation", {})
    errors: list[str] = []

    for f in REQUIRED_FIELDS:
        if rec.get(f) in (None, ""):
            errors.append(f"DQ_MISSING_{f.upper()}")

    amount = rec.get("amount")
    if amount is not None and amount <= 0:
        errors.append("DQ_INVALID_AMOUNT")
    amount_idr = rec.get("amount_idr")
    if amount_idr is not None and amount_idr > float(vcfg.get("max_amount_idr", 1e10)):
        errors.append("DQ_AMOUNT_OUT_OF_RANGE")

    currency = rec.get("currency")
    supported = set(vcfg.get("supported_currencies", ["IDR"]))
    if currency and (currency not in supported or rec.get("amount_idr") is None):
        errors.append("DQ_UNSUPPORTED_CURRENCY")

    ts = rec.get("event_ts")
    if isinstance(ts, datetime):
        if ts > now + timedelta(minutes=int(vcfg.get("max_future_skew_minutes", 5))):
            errors.append("DQ_FUTURE_TIMESTAMP")
        if ts < now - timedelta(days=int(vcfg.get("max_event_age_days", 7))):
            errors.append("DQ_STALE_TIMESTAMP")

    if rec.get("txn_type") is not None and rec["txn_type"] not in TXN_TYPES:
        errors.append("DQ_INVALID_TXN_TYPE")
    if rec.get("status") is not None and rec["status"] not in STATUSES:
        errors.append("DQ_INVALID_STATUS")

    lat, lon = rec.get("latitude"), rec.get("longitude")
    if (lat is None) != (lon is None) or (
        lat is not None and not (-90 <= lat <= 90 and -180 <= lon <= 180)
    ):
        errors.append("DQ_INVALID_GEO")
    return errors


def ingest_message(
    topic: str | None,
    value: str | bytes | None,
    settings: dict,
    *,
    partition: int | None = None,
    offset: int | None = None,
    ingest_ts: datetime | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    ingest_ts = ingest_ts or utcnow()
    raw = value.decode("utf-8", errors="replace") if isinstance(value, bytes) else value
    rec: dict[str, Any] = {name: None for name in _CANONICAL}
    rec.update(source_topic=topic, source_partition=partition, source_offset=offset,
               ingest_ts=ingest_ts, raw_payload=raw)

    errors: list[str] = []
    channel_key = channel_from_topic(topic)
    try:
        if channel_key is None:
            raise NormalizationError("UNKNOWN_CHANNEL", str(topic))
        payload = parse_payload(raw)
        rec.update(NORMALIZERS[channel_key](payload))
        enrich_keys(rec, settings.get("fx_rates_to_idr", {"IDR": 1}))
        errors = validate_record(rec, settings, now=now)
    except NormalizationError as exc:
        errors = [f"DQ_{exc.code}"]
        if channel_key:
            rec["channel"] = channel_key.upper()
    except Exception as exc:  # defensive: a bug in a mapper must not kill the stream
        errors = [f"DQ_NORMALIZATION_FAILED:{type(exc).__name__}"]

    if rec.get("amount") is not None and not isinstance(rec["amount"], float):
        rec["amount"] = float(rec["amount"])
    rec["dq_status"] = "INVALID" if errors else "VALID"
    rec["dq_errors"] = json.dumps(errors)
    return rec

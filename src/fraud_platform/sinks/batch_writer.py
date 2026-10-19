"""Turn one scored / quarantined micro-batch (pandas) into warehouse writes + metrics.

Shared by the Spark ``foreachBatch`` handlers and the lite (non-Spark) pipeline, so the
warehouse contents are identical whichever engine produced them.
Write order matters: facts first, heartbeat/metrics LAST - a heartbeat therefore
guarantees that the data of that batch is committed.
"""
from __future__ import annotations

import json
import time
from datetime import datetime, timezone

import pandas as pd

from ..monitoring.metrics import MetricsEmitter
from .alerts import AlertPublisher
from .warehouse import WarehouseSink

SCORED_PIPELINE = "fraud_scoring"
QUARANTINE_PIPELINE = "quarantine"


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _ts(value) -> datetime | None:
    if value is None or pd.isna(value):
        return None
    return pd.Timestamp(value).to_pydatetime()


def explode_rule_hits(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for rec in df[["txn_id", "rule_hits", "account_id", "channel", "event_ts", "rules_version",
                   "scored_at"]].itertuples(index=False):
        for hit in json.loads(rec.rule_hits or "[]"):
            rows.append({"txn_id": rec.txn_id, "rule_code": hit["code"], "weight": hit["weight"],
                         "explanation": hit["explanation"],
                         "evidence": json.dumps(hit.get("evidence", {}), default=str),
                         "account_id": rec.account_id, "channel": rec.channel, "event_ts": rec.event_ts,
                         "rules_version": rec.rules_version, "scored_at": rec.scored_at})
    return pd.DataFrame(rows)


def write_scored_batch(pdf: pd.DataFrame, batch_id: int, sink: WarehouseSink, emitter: MetricsEmitter,
                       publisher: AlertPublisher, pipeline: str = SCORED_PIPELINE) -> dict:
    started = time.monotonic()
    now = _now()
    if pdf.empty:
        metrics = {"input_rows": 0}
    else:
        pdf = pdf.copy()
        pdf["loaded_at"] = now
        exact = pdf[pdf["duplicate_type"] == "EXACT"]
        facts = pdf[pdf["duplicate_type"] != "EXACT"]          # uniques + cross-channel copies (flagged)
        dups = pdf[pdf["is_duplicate"].fillna(False).astype(bool)]
        scored = facts[~facts["is_duplicate"].fillna(False).astype(bool)]

        sink.upsert("core.fct_transactions", facts)
        sink.upsert("core.transaction_rule_hits", explode_rule_hits(scored))

        alerts = scored[scored["decision"].isin(["REVIEW", "HOLD"])].copy()
        if not alerts.empty:
            alerts["alert_id"] = "ALR-" + alerts["txn_id"]
            alerts["alert_status"] = "OPEN"
            alerts["created_at"] = now
            sink.upsert("core.fraud_alerts", alerts)
            publisher.publish(alerts[["alert_id", "txn_id", "account_id", "customer_id", "channel", "event_ts",
                                      "amount_idr", "risk_score", "risk_level", "decision", "reason_codes",
                                      "explanation"]].to_dict("records"))

        dup_df = dups.assign(pipeline=pipeline, batch_id=int(batch_id), detected_at=now)
        sink.replace_batch("core.duplicate_events", dup_df, pipeline, batch_id)

        metrics = {
            "input_rows": len(pdf), "valid_rows": len(scored), "exact_duplicates": len(exact),
            "cross_duplicates": int((dups["duplicate_type"] == "CROSS_CHANNEL").sum()),
            "quarantined_rows": 0, "alerts": len(alerts),
            "hold_count": int((scored["decision"] == "HOLD").sum()),
            "min_event_ts": _ts(pdf["event_ts"].min()), "max_event_ts": _ts(pdf["event_ts"].max()),
        }
    return _finish(sink, emitter, pipeline, batch_id, now, started, metrics)


def write_quarantine_batch(pdf: pd.DataFrame, batch_id: int, sink: WarehouseSink, emitter: MetricsEmitter,
                           pipeline: str = QUARANTINE_PIPELINE) -> dict:
    started = time.monotonic()
    now = _now()
    q = pdf.assign(pipeline=pipeline, batch_id=int(batch_id)) if not pdf.empty else pdf
    sink.replace_batch("core.quarantine_events", q, pipeline, batch_id)
    metrics = {"input_rows": len(pdf), "valid_rows": 0, "exact_duplicates": 0, "cross_duplicates": 0,
               "quarantined_rows": len(pdf), "alerts": 0, "hold_count": 0,
               # event_ts of rejected records is untrusted (may be future/garbage) -> not used for freshness
               "min_event_ts": None, "max_event_ts": None}
    return _finish(sink, emitter, pipeline, batch_id, now, started, metrics)


def _finish(sink, emitter, pipeline, batch_id, now, started, metrics) -> dict:
    metrics["processing_seconds"] = round(time.monotonic() - started, 3)
    row = {"pipeline": pipeline, "batch_id": int(batch_id), "batch_ts": now, **metrics}
    sink.upsert("ops.pipeline_batch_metrics", pd.DataFrame([row]), update=True)
    sink.upsert("ops.pipeline_heartbeat", pd.DataFrame([{
        "pipeline": pipeline, "last_batch_id": int(batch_id), "last_batch_ts": now,
        "last_event_ts": metrics.get("max_event_ts"), "status": "RUNNING"}]), update=True)

    lag = (now - metrics["max_event_ts"]).total_seconds() if metrics.get("max_event_ts") else None
    inp = metrics.get("input_rows") or 0
    emitter.emit({
        "BatchCompleted": 1, "InputRows": inp, "QuarantinedRows": metrics.get("quarantined_rows", 0),
        "DuplicateRows": metrics.get("exact_duplicates", 0) + metrics.get("cross_duplicates", 0),
        "FraudAlerts": metrics.get("alerts", 0), "HoldDecisions": metrics.get("hold_count", 0),
        "EventLagSeconds": lag, "BatchDurationSeconds": metrics["processing_seconds"],
    }, {"Pipeline": pipeline})
    return row

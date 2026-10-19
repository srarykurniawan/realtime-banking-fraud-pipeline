"""Pipeline health & data-quality checks.

Pure functions: each takes ``query(sql, params) -> list[tuple]`` and returns a
:class:`CheckResult`. They have NO Airflow dependency so they are unit-tested against
DuckDB (tests/test_airflow_checks.py) and reused by the DAGs against Postgres/Snowflake.
SQL uses ``?`` placeholders and only portable syntax; time windows are computed in Python
(all warehouse timestamps are naive UTC).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

Query = Callable[[str, list], list]

PASS, WARN, FAIL = "PASS", "WARN", "FAIL"


@dataclass
class CheckResult:
    check_name: str
    status: str
    observed: float | None
    threshold: float | None
    details: str
    run_ts: datetime = field(default_factory=lambda: datetime.now(timezone.utc).replace(tzinfo=None))

    def as_row(self) -> dict[str, Any]:
        return {"check_name": self.check_name, "run_ts": self.run_ts, "status": self.status,
                "observed": self.observed, "threshold": self.threshold, "details": self.details}


def _scalar(query: Query, sql: str, params: list | None = None) -> Any:
    rows = query(sql, params or [])
    value = rows[0][0] if rows and rows[0] else None
    # some drivers return NaN / NaT instead of NULL for empty aggregates (NaN != NaN)
    return None if value is not None and value != value else value


def utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


# --------------------------------------------------------------------------- freshness
def check_heartbeat(query: Query, now: datetime | None = None, sla_seconds: int = 300,
                    pipeline: str = "fraud_scoring") -> CheckResult:
    """Is the streaming job alive AND receiving data? (heartbeat = last committed batch)."""
    now = now or utcnow()
    last = _scalar(query, "SELECT MAX(last_batch_ts) FROM ops.pipeline_heartbeat WHERE pipeline = ?", [pipeline])
    if last is None:
        return CheckResult("heartbeat_freshness", FAIL, None, sla_seconds, f"no heartbeat for {pipeline}")
    age = (now - last).total_seconds()
    status = FAIL if age > sla_seconds else WARN if age > sla_seconds / 2 else PASS
    return CheckResult("heartbeat_freshness", status, round(age, 1), sla_seconds,
                       f"{pipeline} last batch {age:.0f}s ago (SLA {sla_seconds}s)")


def check_event_lag(query: Query, now: datetime | None = None, max_lag_seconds: int = 180) -> CheckResult:
    """How far behind real time is the newest scored transaction?"""
    now = now or utcnow()
    last_event = _scalar(query, "SELECT MAX(last_event_ts) FROM ops.pipeline_heartbeat WHERE pipeline = ?",
                         ["fraud_scoring"])
    if last_event is None:
        return CheckResult("event_lag", WARN, None, max_lag_seconds, "no events scored yet")
    lag = (now - last_event).total_seconds()
    return CheckResult("event_lag", FAIL if lag > max_lag_seconds else PASS, round(lag, 1), max_lag_seconds,
                       f"newest scored event is {lag:.0f}s old")


# --------------------------------------------------------------------------- data quality
def check_quarantine_ratio(query: Query, now: datetime | None = None, window_minutes: int = 15,
                           max_ratio: float = 0.05) -> CheckResult:
    now = now or utcnow()
    since = now - timedelta(minutes=window_minutes)
    rows = query("SELECT COALESCE(SUM(input_rows), 0), COALESCE(SUM(quarantined_rows), 0) "
                 "FROM ops.pipeline_batch_metrics WHERE batch_ts >= ?", [since])
    total, bad = (rows[0] if rows else (0, 0))
    total, bad = float(total or 0), float(bad or 0)
    if total == 0:
        return CheckResult("quarantine_ratio", WARN, None, max_ratio, f"no input in last {window_minutes} min")
    ratio = bad / total
    status = FAIL if ratio > max_ratio else WARN if ratio > max_ratio / 2 else PASS
    top = query("SELECT source_topic, dq_errors, COUNT(*) FROM core.quarantine_events WHERE ingest_ts >= ? "
                "GROUP BY source_topic, dq_errors ORDER BY 3 DESC LIMIT 3", [since])
    return CheckResult("quarantine_ratio", status, round(ratio, 4), max_ratio,
                       f"{bad:.0f}/{total:.0f} rejected in {window_minutes} min; top: {top}")


def check_volume_anomaly(query: Query, now: datetime | None = None, low: float = 0.5,
                         high: float = 3.0, min_baseline: float = 20) -> CheckResult:
    """Last hour vs the same hour on the previous 7 days (detects silent source outages)."""
    now = now or utcnow()
    sql = "SELECT COALESCE(SUM(input_rows), 0) FROM ops.pipeline_batch_metrics WHERE batch_ts >= ? AND batch_ts < ?"
    current = float(_scalar(query, sql, [now - timedelta(hours=1), now]) or 0)
    history = [float(_scalar(query, sql, [now - timedelta(days=d, hours=1), now - timedelta(days=d)]) or 0)
               for d in range(1, 8)]
    history = [h for h in history if h > 0]
    if not history or sum(history) / len(history) < min_baseline:
        return CheckResult("volume_anomaly", PASS, current, None, "not enough history for a baseline yet")
    baseline = sum(history) / len(history)
    ratio = current / baseline
    status = FAIL if ratio < low else WARN if ratio > high else PASS
    return CheckResult("volume_anomaly", status, round(ratio, 3), low,
                       f"last hour {current:.0f} rows vs baseline {baseline:.0f} ({ratio:.0%})")


def check_duplicate_integrity(query: Query) -> CheckResult:
    """Snowflake does not enforce primary keys -> verify uniqueness explicitly."""
    dup_txn = _scalar(query, "SELECT COUNT(*) FROM (SELECT txn_id FROM core.fct_transactions "
                             "GROUP BY txn_id HAVING COUNT(*) > 1) d") or 0
    dup_key = _scalar(query, "SELECT COUNT(*) FROM (SELECT idempotency_key FROM core.fct_transactions "
                             "GROUP BY idempotency_key HAVING COUNT(*) > 1) d") or 0
    bad = int(dup_txn) + int(dup_key)
    return CheckResult("duplicate_integrity", FAIL if bad else PASS, bad, 0,
                       f"{dup_txn} duplicated txn_id, {dup_key} duplicated idempotency_key in fct_transactions")


def check_reconciliation(query: Query, start: datetime, end: datetime) -> CheckResult:
    """Conservation law per day: input = scored + duplicates, and warehouse rows = scored rows."""
    rows = query(
        "SELECT COALESCE(SUM(input_rows), 0), COALESCE(SUM(valid_rows), 0), "
        "COALESCE(SUM(exact_duplicates), 0), COALESCE(SUM(cross_duplicates), 0) "
        "FROM ops.pipeline_batch_metrics WHERE pipeline = ? AND batch_ts >= ? AND batch_ts < ?",
        ["fraud_scoring", start, end])
    inp, valid, exact, cross = (float(x or 0) for x in rows[0])
    loaded = float(_scalar(query, "SELECT COUNT(*) FROM core.fct_transactions WHERE NOT is_duplicate "
                                  "AND loaded_at >= ? AND loaded_at < ?", [start, end]) or 0)
    gap_flow = inp - (valid + exact + cross)
    gap_load = valid - loaded
    status = PASS if gap_flow == 0 and gap_load == 0 else FAIL
    return CheckResult("daily_reconciliation", status, gap_flow + gap_load, 0,
                       f"[{start:%Y-%m-%d}] input={inp:.0f} valid={valid:.0f} exact_dup={exact:.0f} "
                       f"cross_dup={cross:.0f} loaded={loaded:.0f} flow_gap={gap_flow:.0f} load_gap={gap_load:.0f}")


def check_alert_backlog(query: Query, now: datetime | None = None, max_age_minutes: int = 30) -> CheckResult:
    """HOLD alerts must be handled quickly (customer money is on hold)."""
    now = now or utcnow()
    n = _scalar(query, "SELECT COUNT(*) FROM core.fraud_alerts WHERE decision = 'HOLD' "
                       "AND alert_status = 'OPEN' AND created_at < ?",
                [now - timedelta(minutes=max_age_minutes)]) or 0
    return CheckResult("hold_alert_backlog", WARN if n else PASS, int(n), 0,
                       f"{n} HOLD alerts open > {max_age_minutes} min")


def check_rule_precision(query: Query, min_precision: float = 0.2, min_dispositions: int = 20) -> CheckResult:
    rows = query(
        "SELECT h.rule_code, "
        "SUM(CASE WHEN f.disposition = 'CONFIRMED_FRAUD' THEN 1 ELSE 0 END), "
        "SUM(CASE WHEN f.disposition IS NOT NULL THEN 1 ELSE 0 END) "
        "FROM core.transaction_rule_hits h LEFT JOIN core.fraud_case_feedback f ON f.txn_id = h.txn_id "
        "GROUP BY h.rule_code", [])
    noisy = [(code, round(tp / n, 3)) for code, tp, n in rows
             if n and n >= min_dispositions and tp / n < min_precision]
    return CheckResult("rule_precision", WARN if noisy else PASS, len(noisy), min_precision,
                       f"rules below precision {min_precision}: {noisy}" if noisy else "all rules OK")

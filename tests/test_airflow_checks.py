"""Problem 6: monitoring checks used by the Airflow DAGs, tested against a real warehouse."""
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest

from fraud_common import checks
from fraud_platform.lite_pipeline import run
from fraud_platform.simulator.generator import FileSink, Simulator
from fraud_platform.sinks.warehouse import build_sink

DAG_DIR = Path(__file__).resolve().parents[1] / "airflow" / "dags"


@pytest.fixture
def wh(settings, rules, tmp_path):
    sim = Simulator(n_customers=50, seed=3)
    end = datetime.now(timezone.utc).replace(microsecond=0)
    FileSink(settings["source"]["files_path"]).send(sim.generate_window(end - timedelta(minutes=180), 180, 4))
    run(settings["source"]["files_path"], str(tmp_path / "ck"), settings, rules)
    sink = build_sink(settings)

    def query(sql, params):
        return [tuple(r) for r in sink.query(sql, params).itertuples(index=False)]

    return sink, query


def test_heartbeat_fresh_then_stale(wh):
    _, query = wh
    now = checks.utcnow()
    assert checks.check_heartbeat(query, now=now).status == checks.PASS
    assert checks.check_heartbeat(query, now=now + timedelta(minutes=10)).status == checks.FAIL
    assert checks.check_heartbeat(query, pipeline="missing").status == checks.FAIL


def test_quarantine_ratio_detects_bad_feed(wh):
    sink, query = wh
    ok = checks.check_quarantine_ratio(query, window_minutes=60)
    assert ok.status in (checks.PASS, checks.WARN) and ok.observed < 0.05
    sink.upsert("ops.pipeline_batch_metrics", pd.DataFrame([{
        "pipeline": "quarantine", "batch_id": 999, "batch_ts": checks.utcnow(), "input_rows": 5000,
        "quarantined_rows": 5000}]))
    assert checks.check_quarantine_ratio(query, window_minutes=60).status == checks.FAIL


def test_duplicate_integrity_passes(wh):
    _, query = wh
    assert checks.check_duplicate_integrity(query).status == checks.PASS


def test_reconciliation_balances(wh):
    _, query = wh
    now = checks.utcnow()
    res = checks.check_reconciliation(query, now - timedelta(days=1), now + timedelta(minutes=1))
    assert res.status == checks.PASS, res.details


def test_reconciliation_detects_lost_rows(wh):
    sink, query = wh
    sink.query("DELETE FROM core.fct_transactions WHERE txn_id IN "
               "(SELECT txn_id FROM core.fct_transactions WHERE NOT is_duplicate LIMIT 3)")
    now = checks.utcnow()
    res = checks.check_reconciliation(query, now - timedelta(days=1), now + timedelta(minutes=1))
    assert res.status == checks.FAIL and "load_gap=3" in res.details


def test_alert_backlog_and_event_lag(wh):
    _, query = wh
    later = checks.utcnow() + timedelta(hours=2)
    assert checks.check_alert_backlog(query, now=later).status == checks.WARN
    assert checks.check_event_lag(query, now=later).status == checks.FAIL


def test_volume_anomaly_without_history_passes(wh):
    _, query = wh
    assert checks.check_volume_anomaly(query).status == checks.PASS


def test_rule_precision_flags_noisy_rule(wh):
    sink, query = wh
    hits = sink.query("SELECT txn_id FROM core.transaction_rule_hits WHERE rule_code = 'AMOUNT_SPIKE'")
    fb = pd.DataFrame({"txn_id": hits["txn_id"], "disposition": "FALSE_POSITIVE", "analyst": "t",
                       "notes": None, "decided_at": checks.utcnow()})
    sink.upsert("core.fraud_case_feedback", fb)
    res = checks.check_rule_precision(query, min_dispositions=1)
    assert res.status == checks.WARN and "AMOUNT_SPIKE" in res.details


def test_dag_files_are_valid_python():
    for f in DAG_DIR.glob("*.py"):
        compile(f.read_text(), str(f), "exec")

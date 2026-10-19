"""End-to-end (no Spark): simulator -> lite pipeline -> DuckDB warehouse -> evaluation.
Also proves idempotency (re-run adds nothing) and incremental processing."""
from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from fraud_platform.evaluate import evaluate
from fraud_platform.lite_pipeline import run
from fraud_platform.simulator.generator import FileSink, Simulator, write_labels
from fraud_platform.sinks.warehouse import build_sink


def simulate(settings, tmp_path, minutes=240, seed=7, start=None):
    sim = Simulator(n_customers=80, seed=seed)
    end = start or datetime.now(timezone.utc).replace(microsecond=0)
    msgs = sim.generate_window(end - timedelta(minutes=minutes), minutes, rate_per_min=5)
    FileSink(settings["source"]["files_path"], chunk_size=400).send(msgs)
    write_labels(str(tmp_path / "labels.jsonl"), msgs)
    return msgs


@pytest.fixture
def processed(settings, rules, tmp_path):
    simulate(settings, tmp_path)
    run(settings["source"]["files_path"], str(tmp_path / "lite_ckpt"), settings, rules)
    return build_sink(settings)


def test_detection_quality(processed, tmp_path):
    s = evaluate(str(tmp_path / "labels.jsonl"), processed)["summary"]
    assert s["missing_in_warehouse"] == 0
    assert s["fraud_attack_detection_rate"] >= 0.95
    assert s["false_positive_rate"] <= 0.02
    assert s["exact_duplicates_caught"] == s["exact_duplicates_expected"] > 0
    assert s["cross_duplicates_caught"] == s["cross_duplicates_expected"] > 0
    assert s["invalid_quarantined"] == s["invalid_expected"] > 0


def test_aggregates_exclude_duplicates(processed):
    """Problem 3: report totals must not double count."""
    view = processed.query("SELECT SUM(txn_count) AS n FROM mart.v_channel_daily_summary")["n"].iloc[0]
    unique = processed.query("SELECT COUNT(*) AS n FROM core.fct_transactions WHERE NOT is_duplicate")["n"].iloc[0]
    total = processed.query("SELECT COUNT(*) AS n FROM core.fct_transactions")["n"].iloc[0]
    assert view == unique < total


def test_every_alert_is_explainable(processed):
    """Problem 4: each REVIEW/HOLD has reason codes, text and per-rule rows."""
    alerts = processed.query("SELECT * FROM mart.v_alert_investigation")
    assert len(alerts) > 0
    assert alerts["reason_codes"].str.len().min() > 0
    assert alerts["explanation"].str.len().min() > 10
    orphan = processed.query(
        "SELECT COUNT(*) AS n FROM core.fraud_alerts a WHERE NOT EXISTS "
        "(SELECT 1 FROM core.transaction_rule_hits h WHERE h.txn_id = a.txn_id)")["n"].iloc[0]
    assert orphan == 0


def test_conservation_of_records(processed):
    m = processed.query("SELECT SUM(input_rows) i, SUM(valid_rows) v, SUM(exact_duplicates) e, "
                        "SUM(cross_duplicates) c FROM ops.pipeline_batch_metrics WHERE pipeline='fraud_scoring'")
    i, v, e, c = m.iloc[0]
    assert i == v + e + c


def test_rerun_is_idempotent(processed, settings, rules, tmp_path):
    before = processed.query("SELECT COUNT(*) AS n FROM core.fct_transactions")["n"].iloc[0]
    results = run(settings["source"]["files_path"], str(tmp_path / "lite_ckpt"), settings, rules)
    after = processed.query("SELECT COUNT(*) AS n FROM core.fct_transactions")["n"].iloc[0]
    assert results == [] and before == after


def test_replayed_batch_does_not_double_write(processed):
    """Simulates Spark replaying a batch after a crash (same batch_id)."""
    from fraud_platform.monitoring.metrics import NoopEmitter
    from fraud_platform.sinks.alerts import NoopAlertPublisher
    from fraud_platform.sinks.batch_writer import write_scored_batch

    fct = processed.query("SELECT * FROM core.fct_transactions LIMIT 50")
    counts = lambda: processed.query(  # noqa: E731
        "SELECT (SELECT COUNT(*) FROM core.fct_transactions) a, (SELECT COUNT(*) FROM core.duplicate_events) b, "
        "(SELECT COUNT(*) FROM ops.pipeline_batch_metrics) c").iloc[0].tolist()
    before = counts()
    batch = fct.drop(columns=["loaded_at"]).assign(duplicate_type="EXACT", is_duplicate=True)
    write_scored_batch(batch, 10_000, processed, NoopEmitter(), NoopAlertPublisher())
    after_first = counts()
    write_scored_batch(batch, 10_000, processed, NoopEmitter(), NoopAlertPublisher())   # replay
    assert counts() == after_first
    assert after_first == [before[0], before[1] + len(batch), before[2] + 1]


def test_incremental_new_files_continue_account_state(processed, settings, rules, tmp_path):
    before = processed.query("SELECT COUNT(*) AS n FROM core.fct_transactions")["n"].iloc[0]
    simulate(settings, tmp_path, minutes=30, seed=99)
    results = run(settings["source"]["files_path"], str(tmp_path / "lite_ckpt"), settings, rules)
    after = processed.query("SELECT COUNT(*) AS n FROM core.fct_transactions")["n"].iloc[0]
    assert results and after > before


def test_heartbeat_written_last(processed):
    hb = processed.query("SELECT * FROM ops.pipeline_heartbeat ORDER BY pipeline")
    assert set(hb["pipeline"]) == {"fraud_scoring", "quarantine"}
    assert pd.notna(hb.loc[hb.pipeline == "fraud_scoring", "last_event_ts"]).all()


def test_console_queries_run(processed):
    """Every SQL used by the investigation console must work against the warehouse."""
    import re
    from pathlib import Path

    src = (Path(__file__).resolve().parents[1] / "apps" / "investigation_console" / "app.py").read_text()
    since = datetime(2000, 1, 1)
    for sql in re.findall(r'q\(\s*((?:"[^"]*"\s*)+)', src):
        stmt = "".join(re.findall(r'"([^"]*)"', sql))
        parts = stmt.split("?")[:-1]
        params = [0 if "risk_score" in part[-25:] else since for part in parts]
        if "txn_id = ?" in stmt or "account_id = ?" in stmt:
            continue                     # parameterised drill-downs need a concrete id
        processed.query(stmt, params)

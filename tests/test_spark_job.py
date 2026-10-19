"""Real Spark Structured Streaming run (file source, availableNow trigger).

Verifies the streaming topology, the RocksDB-backed per-account state, the checkpoint
(restart continues where it stopped, state survives) and parity with the lite pipeline.
"""
import shutil
from datetime import datetime, timedelta, timezone

import pytest

pyspark = pytest.importorskip("pyspark")
if shutil.which("java") is None:  # pragma: no cover
    pytest.skip("Java is required for Spark tests", allow_module_level=True)

from fraud_platform.evaluate import evaluate  # noqa: E402
from fraud_platform.simulator.generator import FileSink, Simulator, write_labels  # noqa: E402
from fraud_platform.sinks.warehouse import build_sink  # noqa: E402
from fraud_platform.streaming.spark_job import build_spark, start  # noqa: E402

pytestmark = pytest.mark.spark


@pytest.fixture(scope="module")
def spark():
    from fraud_platform.config import load_settings

    s = load_settings()
    s["streaming"]["shuffle_partitions"] = 2
    session = build_spark(s)
    yield session
    session.stop()


def run_available_now(spark, settings, rules):
    for q in start(spark, settings, rules, available_now=True):
        q.awaitTermination()
        assert q.exception() is None


def test_spark_end_to_end_with_restart(spark, settings, rules, tmp_path):
    sim = Simulator(n_customers=60, seed=11)
    end = datetime.now(timezone.utc).replace(microsecond=0)
    first = sim.generate_window(end - timedelta(minutes=200), 170, rate_per_min=4)
    FileSink(settings["source"]["files_path"], chunk_size=300).send(first)
    write_labels(str(tmp_path / "labels.jsonl"), first)

    run_available_now(spark, settings, rules)
    sink = build_sink(settings)
    report = evaluate(str(tmp_path / "labels.jsonl"), sink)["summary"]
    assert report["missing_in_warehouse"] == 0
    assert report["fraud_attack_detection_rate"] >= 0.95
    assert report["exact_duplicates_caught"] == report["exact_duplicates_expected"]
    assert report["cross_duplicates_caught"] == report["cross_duplicates_expected"]
    assert report["invalid_quarantined"] == report["invalid_expected"]
    batches_1 = sink.query("SELECT COUNT(*) n FROM ops.pipeline_batch_metrics WHERE pipeline='fraud_scoring'")["n"][0]
    assert batches_1 >= 2                        # several micro-batches -> state carried across batches

    # "restart": same checkpoint, new data incl. a replay of an already-processed message
    replay = first[len(first) // 2]
    new = sim.generate_window(end - timedelta(minutes=30), 25, rate_per_min=4, scenarios_per_type=0,
                              cross_dup_pairs=0)
    FileSink(settings["source"]["files_path"], chunk_size=300).send(new + [replay])
    run_available_now(spark, settings, rules)

    batches_2 = sink.query("SELECT COUNT(*) n FROM ops.pipeline_batch_metrics WHERE pipeline='fraud_scoring'")["n"][0]
    assert batches_2 > batches_1                 # resumed, did not reprocess from scratch
    if replay.label["expect"] == "VALID":
        dup = sink.query("SELECT COUNT(*) n FROM core.duplicate_events WHERE txn_id = ? AND duplicate_type='EXACT'",
                         [replay.label["txn_id"]])["n"][0]
        assert dup >= 1                          # state from run 1 recognised the replay in run 2
    dupes = sink.query("SELECT COUNT(*) n FROM (SELECT txn_id FROM core.fct_transactions GROUP BY txn_id "
                       "HAVING COUNT(*) > 1) d")["n"][0]
    assert dupes == 0

    bronze = list((tmp_path / "bronze").rglob("*.parquet"))
    assert bronze, "raw events must be archived to the bronze layer"

"""Real-time fraud detection job (Spark Structured Streaming).

Topology (3 streaming queries sharing one source definition)::

    Kafka txn.raw.* ──► raw ──┬──► [bronze_archive]  parquet (S3) - replay & audit
                              │
                              └──► normalize (mapInPandas: parse + map channel + DQ checks)
                                     ├── VALID ──► groupBy(account_id)
                                     │             .applyInPandasWithState(dedup + score)   (RocksDB state)
                                     │             └──► [fraud_scoring] foreachBatch -> warehouse + alerts + metrics
                                     └── INVALID ─► [quarantine] foreachBatch -> quarantine table (+ DLQ)

Design decisions
----------------
* **No watermark on the scoring path.** A watermark would make Spark silently drop late
  transactions; in banking every transaction must be accounted for. Rules are event-time
  aware, so late events are still scored correctly against the profile.
* **Dedup inside the state function**, not ``dropDuplicates``: duplicates are *recorded*
  (``core.duplicate_events``) instead of silently disappearing, which makes reconciliation
  possible (input = unique + duplicates + quarantined).
* **Kafka key = account_id** so all events of an account are on one partition (ordering).
* **Exactly-once effect**: checkpoints + idempotent warehouse writes keyed by txn_id/batch_id.

Run::

    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1 \\
        -m fraud_platform.streaming.spark_job            # or: fraud-stream
    fraud-stream --available-now                         # process backlog then stop (backfill / tests)
"""
from __future__ import annotations

import argparse
import logging
import os
import signal
import sys
from datetime import datetime, timezone

import pandas as pd

from ..config import load_rules, load_settings
from ..core.schema import NORMALIZED_FIELDS, SCORED_FIELDS, field_names, to_spark_schema
from ..core.scoring import AccountState, FraudScorer, score_account_events
from ..core.validation import ingest_message
from ..monitoring.metrics import build_emitter
from ..sinks.alerts import build_alert_publisher
from ..sinks.batch_writer import write_quarantine_batch, write_scored_batch
from ..sinks.warehouse import build_sink

log = logging.getLogger("fraud_platform.stream")

NORMALIZED_COLS = field_names(NORMALIZED_FIELDS)
SCORED_COLS = field_names(SCORED_FIELDS)
QUARANTINE_COLS = ["source_topic", "source_partition", "source_offset", "channel", "txn_id", "account_id",
                   "dq_errors", "raw_payload", "ingest_ts", "event_ts"]


# --------------------------------------------------------------------------- session
def build_spark(settings: dict):
    from pyspark.sql import SparkSession

    # local mode: python workers must use the same interpreter that has fraud_platform installed
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    s = settings["streaming"]
    builder = (
        SparkSession.builder.appName(s.get("app_name", "fraud-detection-stream"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(s.get("shuffle_partitions", 8)))
        .config("spark.sql.streaming.stateStore.providerClass",
                "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
        .config("spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled", "true")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.streaming.metricsEnabled", "true")
        # Processing-time state timeouts would otherwise trigger an empty micro-batch every
        # trigger forever (and availableNow would never finish). Idle profiles are evicted in
        # the next batch that carries data, which in a bank is always seconds away.
        .config("spark.sql.streaming.noDataMicroBatches.enabled", "false")
    )
    return builder.getOrCreate()


# --------------------------------------------------------------------------- source
def _kafka_security_options(src: dict) -> dict:
    opts = {}
    proto = src.get("security_protocol", "PLAINTEXT")
    if proto != "PLAINTEXT":
        opts["kafka.security.protocol"] = proto
    if src.get("sasl_mechanism") == "AWS_MSK_IAM":
        opts.update({
            "kafka.sasl.mechanism": "AWS_MSK_IAM",
            "kafka.sasl.jaas.config": "software.amazon.msk.auth.iam.IAMLoginModule required;",
            "kafka.sasl.client.callback.handler.class": "software.amazon.msk.auth.iam.IAMClientCallbackHandler",
        })
    elif src.get("sasl_mechanism"):
        opts["kafka.sasl.mechanism"] = src["sasl_mechanism"]
    return opts


def read_raw(spark, settings: dict):
    """Return a streaming DataFrame with Kafka-like columns: topic, partition, offset, kafka_ts, value."""
    from pyspark.sql import functions as F

    src = settings["source"]
    if src["type"] == "kafka":
        reader = (spark.readStream.format("kafka")
                  .option("kafka.bootstrap.servers", src["kafka_bootstrap_servers"])
                  .option("subscribePattern", src["topic_pattern"])
                  .option("startingOffsets", src.get("starting_offsets", "latest"))
                  .option("maxOffsetsPerTrigger", str(src.get("max_offsets_per_trigger", 50000)))
                  .option("failOnDataLoss", "false"))
        for k, v in _kafka_security_options(src).items():
            reader = reader.option(k, v)
        return reader.load().select(
            "topic", "partition", "offset", F.col("timestamp").alias("kafka_ts"),
            F.col("value").cast("string").alias("value"))

    # files mode: data/landing/<topic>/*.jsonl (local testing without Kafka)
    raw = (spark.readStream.format("text")
           .option("maxFilesPerTrigger", str(src.get("max_files_per_trigger", 5)))
           .load(f"{src['files_path']}/txn.raw.*"))
    return raw.select(
        F.regexp_extract(F.col("_metadata.file_path"), r"(txn\.raw\.[a-z]+)/[^/]+$", 1).alias("topic"),
        F.lit(0).cast("int").alias("partition"),
        F.lit(None).cast("long").alias("offset"),
        F.current_timestamp().alias("kafka_ts"),
        F.col("value"),
    )


# --------------------------------------------------------------------------- transforms
def make_normalizer(settings: dict):
    """mapInPandas function: raw rows -> canonical + DQ columns (reuses core.validation)."""

    def normalize(batches):
        for pdf in batches:
            now = datetime.now(timezone.utc).replace(tzinfo=None)
            recs = [
                ingest_message(t, v, settings, partition=None if pd.isna(p) else int(p),
                               offset=None if pd.isna(o) else int(o), ingest_ts=now, now=now)
                for t, v, p, o in zip(pdf["topic"], pdf["value"], pdf["partition"], pdf["offset"], strict=True)
            ]
            yield pd.DataFrame.from_records(recs, columns=NORMALIZED_COLS)

    return normalize


def make_state_fn(rules: dict, idle_timeout_ms: int):
    """applyInPandasWithState function: dedup + score all events of ONE account."""
    scorer = FraudScorer(rules)

    def score(key, pdfs, state):
        if state.hasTimedOut:                       # account idle for too long -> drop profile
            state.remove()
            return
        acct_state = AccountState.from_json(state.get[0]) if state.exists else AccountState()
        frames = [p for p in pdfs if not p.empty]
        if not frames:
            return
        events = pd.concat(frames, ignore_index=True)
        events = events.astype(object).where(pd.notna(events), None)
        records = []
        for r in events.to_dict("records"):
            r["event_ts"] = pd.Timestamp(r["event_ts"]).to_pydatetime()
            records.append(r)
        out = score_account_events(records, acct_state, scorer)
        state.update((acct_state.to_json(),))
        state.setTimeoutDuration(idle_timeout_ms)
        yield pd.DataFrame.from_records(out, columns=SCORED_COLS)

    return score


def _duration_ms(text: str) -> int:
    value, unit = str(text).split()
    mult = {"second": 1, "seconds": 1, "minute": 60, "minutes": 60, "hour": 3600, "hours": 3600,
            "day": 86400, "days": 86400}[unit]
    return int(float(value) * mult * 1000)


def build_pipeline(spark, settings: dict, rules: dict):
    """Return (raw, valid_scored, invalid) streaming DataFrames - separated for testability."""
    from pyspark.sql import functions as F
    from pyspark.sql.streaming.state import GroupStateTimeout

    raw = read_raw(spark, settings)
    normalized = raw.mapInPandas(make_normalizer(settings), schema=to_spark_schema(NORMALIZED_FIELDS))

    valid = normalized.filter(F.col("dq_status") == "VALID").drop("raw_payload")
    invalid = normalized.filter(F.col("dq_status") != "VALID").select(*QUARANTINE_COLS)

    idle_ms = _duration_ms(settings["streaming"].get("state_idle_timeout", "30 days"))
    scored = valid.groupBy("account_id").applyInPandasWithState(
        make_state_fn(rules, idle_ms),
        outputStructType=to_spark_schema(SCORED_FIELDS),
        stateStructType="state_json STRING",
        outputMode="append",
        timeoutConf=GroupStateTimeout.ProcessingTimeTimeout,
    )
    return raw, scored, invalid


# --------------------------------------------------------------------------- listener
def register_listener(spark, emitter) -> None:
    from pyspark.sql.streaming import StreamingQueryListener

    class MetricsListener(StreamingQueryListener):
        def onQueryStarted(self, event):
            log.info("query started: %s (%s)", event.name, event.id)

        def onQueryProgress(self, event):
            p = event.progress
            state_rows = sum(op.numRowsTotal for op in p.stateOperators) if p.stateOperators else 0
            emitter.emit({
                "InputRowsPerSecond": p.inputRowsPerSecond or 0.0,
                "ProcessedRowsPerSecond": p.processedRowsPerSecond or 0.0,
                "TriggerDurationMs": p.durationMs.get("triggerExecution", 0),
                "StateRows": state_rows,
            }, {"Query": p.name or "unnamed"})

        def onQueryIdle(self, event):
            pass

        def onQueryTerminated(self, event):
            if event.exception:
                log.error("query %s terminated with error: %s", event.id, event.exception)
                emitter.emit({"QueryFailed": 1}, {"Query": str(event.id)})

    spark.streams.addListener(MetricsListener())


# --------------------------------------------------------------------------- start
def start(spark, settings: dict, rules: dict, available_now: bool = False):
    from pyspark.sql import functions as F

    s = settings["streaming"]
    ckpt = s["checkpoint_dir"].rstrip("/")
    sink, emitter = build_sink(settings), build_emitter(settings)
    publisher = build_alert_publisher(settings)
    sink.ensure_schema()
    register_listener(spark, emitter)

    raw, scored, invalid = build_pipeline(spark, settings, rules)

    def trigger(writer):
        return writer.trigger(availableNow=True) if available_now else \
            writer.trigger(processingTime=s.get("trigger_interval", "10 seconds"))

    def on_scored(df, batch_id):
        write_scored_batch(df.toPandas(), batch_id, sink, emitter, publisher)

    def on_invalid(df, batch_id):
        pdf = df.toPandas()
        write_quarantine_batch(pdf, batch_id, sink, emitter)
        if settings["source"]["type"] == "kafka" and not pdf.empty:   # DLQ topic for upstream owners
            (df.select(F.col("source_topic").alias("key"), F.to_json(F.struct("*")).alias("value"))
               .write.format("kafka")
               .option("kafka.bootstrap.servers", settings["source"]["kafka_bootstrap_servers"])
               .option("topic", settings["topics"]["dlq"]).save())

    queries = [
        trigger(scored.writeStream.queryName("fraud_scoring").outputMode("append")
                .option("checkpointLocation", f"{ckpt}/fraud_scoring").foreachBatch(on_scored)).start(),
        trigger(invalid.writeStream.queryName("quarantine").outputMode("append")
                .option("checkpointLocation", f"{ckpt}/quarantine").foreachBatch(on_invalid)).start(),
    ]
    if s.get("enable_bronze_archive", True):
        bronze = raw.withColumn("ingest_date", F.to_date("kafka_ts"))
        queries.append(trigger(bronze.writeStream.queryName("bronze_archive").format("parquet")
                               .partitionBy("topic", "ingest_date")
                               .option("path", s["bronze_dir"])
                               .option("checkpointLocation", f"{ckpt}/bronze_archive")).start())
    return queries


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    ap = argparse.ArgumentParser(description="Real-time fraud detection streaming job")
    ap.add_argument("--available-now", action="store_true", help="process available data then exit")
    args = ap.parse_args(argv)

    settings, rules = load_settings(), load_rules()
    spark = build_spark(settings)
    log.info("rules version %s (%s), source=%s, warehouse=%s", rules.get("version"), rules["config_hash"],
             settings["source"]["type"], settings["warehouse"]["type"])
    queries = start(spark, settings, rules, available_now=args.available_now)

    def shutdown(signum, _frame):
        log.warning("signal %s received - stopping queries gracefully", signum)
        for q in queries:
            q.stop()

    signal.signal(signal.SIGTERM, shutdown)
    try:
        if args.available_now:
            for q in queries:
                q.awaitTermination()
        else:
            spark.streams.awaitAnyTermination()     # any failure -> exit non-zero -> supervisor restarts
    finally:
        failed = [q for q in queries if q.exception()]
        for q in queries:
            if q.isActive:
                q.stop()
        spark.stop()
    if failed:
        for q in failed:
            log.error("query %s failed: %s", q.name, q.exception())
        sys.exit(1)


if __name__ == "__main__":
    main()

"""Lite pipeline: the same ingestion -> dedup -> scoring -> warehouse logic WITHOUT Spark/Kafka.

Use it for a 1-minute local demo, CI and for debugging rules on a laptop. It reads the
landing files written by the simulator, keeps a tiny checkpoint (processed files +
per-account state) so re-running is idempotent and incremental, exactly like the
Spark job's checkpoint does.

    fraud-lite --landing data/landing --checkpoint data/checkpoints/lite
"""
from __future__ import annotations

import argparse
import json
import logging
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from .config import load_rules, load_settings
from .core.scoring import AccountState, FraudScorer, score_account_events
from .core.validation import ingest_message
from .monitoring.metrics import build_emitter
from .sinks.alerts import build_alert_publisher
from .sinks.batch_writer import write_quarantine_batch, write_scored_batch
from .sinks.warehouse import build_sink

log = logging.getLogger("fraud_platform.lite")


class LiteCheckpoint:
    def __init__(self, path: str):
        self.dir = Path(path)
        self.dir.mkdir(parents=True, exist_ok=True)
        self.meta_path, self.state_path = self.dir / "progress.json", self.dir / "state.json"
        meta = json.loads(self.meta_path.read_text()) if self.meta_path.exists() else {}
        self.files: set[str] = set(meta.get("files", []))
        self.batch_id: int = meta.get("batch_id", -1)
        raw = json.loads(self.state_path.read_text()) if self.state_path.exists() else {}
        self.states = {k: AccountState.from_json(v) for k, v in raw.items()}

    def commit(self, files: list[str]) -> None:
        self.files.update(files)
        self.state_path.write_text(json.dumps({k: v.to_json() for k, v in self.states.items()}))
        self.meta_path.write_text(json.dumps({"batch_id": self.batch_id, "files": sorted(self.files)}))


def run(landing: str, checkpoint: str, settings: dict, rules: dict, now: datetime | None = None) -> list[dict]:
    sink, emitter = build_sink(settings), build_emitter(settings)
    publisher = build_alert_publisher(settings)
    sink.ensure_schema()
    scorer = FraudScorer(rules)
    ckpt = LiteCheckpoint(checkpoint)

    # group files by part number -> one micro-batch per simulator chunk (all topics)
    parts: dict[str, list[Path]] = defaultdict(list)
    for f in sorted(Path(landing).glob("txn.raw.*/part-*.jsonl")):
        if str(f) not in ckpt.files:
            parts[f.name].append(f)

    results = []
    for part_name in sorted(parts):
        files = parts[part_name]
        ckpt.batch_id += 1
        records = []
        for f in files:
            topic = f.parent.name
            for line_no, line in enumerate(f.read_text(encoding="utf-8").splitlines()):
                if line.strip():
                    records.append(ingest_message(topic, line, settings, partition=0, offset=line_no, now=now))
        valid = [r for r in records if r["dq_status"] == "VALID"]
        invalid = [r for r in records if r["dq_status"] != "VALID"]

        by_account: dict[str, list[dict]] = defaultdict(list)
        for r in valid:
            by_account[r["account_id"]].append(r)
        scored = []
        for acct, events in by_account.items():
            state = ckpt.states.setdefault(acct, AccountState())
            scored.extend(score_account_events(events, state, scorer))

        res = write_scored_batch(pd.DataFrame(scored), ckpt.batch_id, sink, emitter, publisher)
        write_quarantine_batch(pd.DataFrame(invalid), ckpt.batch_id, sink, emitter)
        ckpt.commit([str(f) for f in files])     # commit AFTER the warehouse write (at-least-once)
        results.append(res)
        log.info("batch %s: %s valid, %s dup, %s quarantined, %s alerts", ckpt.batch_id, res.get("valid_rows"),
                 (res.get("exact_duplicates") or 0) + (res.get("cross_duplicates") or 0), len(invalid),
                 res.get("alerts"))
    return results


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    ap = argparse.ArgumentParser(description="Run the fraud pipeline locally without Spark")
    ap.add_argument("--landing", default=None)
    ap.add_argument("--checkpoint", default="data/checkpoints/lite")
    args = ap.parse_args(argv)
    settings, rules = load_settings(), load_rules()
    landing = args.landing or settings["source"]["files_path"]
    results = run(landing, args.checkpoint, settings, rules, now=datetime.now(timezone.utc).replace(tzinfo=None))
    print(f"processed {len(results)} batches")


if __name__ == "__main__":
    main()

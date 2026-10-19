"""Compare warehouse output with the simulator's ground-truth labels.

    fraud-evaluate --labels data/labels/labels.jsonl

Reports fraud detection rate per scenario, false-positive rate on normal traffic and
whether every duplicate / malformed record was caught - a regression test for rules.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from .config import load_settings
from .sinks.warehouse import build_sink


def evaluate(labels_path: str, sink) -> dict:
    labels = pd.DataFrame([json.loads(x) for x in Path(labels_path).read_text().splitlines() if x.strip()])
    fct = sink.query("SELECT txn_id, decision, risk_score, is_duplicate FROM core.fct_transactions")
    dups = sink.query("SELECT duplicate_type, COUNT(*) AS n FROM core.duplicate_events GROUP BY 1")
    quarantined = int(sink.query("SELECT COUNT(*) AS n FROM core.quarantine_events")["n"].iloc[0])

    valid = labels[labels["expect"] == "VALID"].merge(fct, on="txn_id", how="left")
    valid["flagged"] = valid["decision"].isin(["REVIEW", "HOLD"])
    fraud = valid[valid["is_fraud"]]
    normal = valid[~valid["is_fraud"]]

    # an attack is "caught" if at least one of its events is flagged
    attacks = fraud.groupby(["scenario", "attack_id"])["flagged"].any().reset_index()
    per_scenario = (fraud.groupby("scenario")
                    .agg(events=("txn_id", "count"), events_flagged=("flagged", "sum"),
                         max_score=("risk_score", "max"))
                    .join(attacks.groupby("scenario").agg(attacks=("attack_id", "count"),
                                                          attacks_caught=("flagged", "sum"))))
    dup_counts = dict(zip(dups["duplicate_type"], dups["n"], strict=True)) if not dups.empty else {}
    # cross-channel pair: whichever copy arrives first is kept ("first seen wins"), the other must be
    # flagged -> correct when EXACTLY one member of every pair is a duplicate (counted once in reports)
    pairs = labels[labels["expect"] == "CROSS_DUP"]
    pair_of = pairs["pair_of"] if "pair_of" in pairs else pd.Series(dtype=object)
    is_dup = dict(zip(fct["txn_id"], fct["is_duplicate"].astype(bool), strict=True))
    cross_ids = pairs["txn_id"]
    cross_flagged = [a for a, b in zip(pairs["txn_id"], pair_of, strict=True)
                     if is_dup.get(a, False) != is_dup.get(b, False)]

    report = {
        "messages": len(labels),
        "fraud_events": len(fraud),
        "fraud_attacks": int(len(attacks)),
        "fraud_attack_detection_rate": round(float(attacks["flagged"].mean()), 3) if len(attacks) else None,
        "fraud_event_detection_rate": round(float(fraud["flagged"].mean()), 3) if len(fraud) else None,
        "normal_events": len(normal),
        "false_positive_rate": round(float(normal["flagged"].mean()), 4) if len(normal) else None,
        "missing_in_warehouse": int(valid["decision"].isna().sum()),
        "exact_duplicates_expected": int((labels["expect"] == "EXACT_DUP").sum()),
        "exact_duplicates_caught": int(dup_counts.get("EXACT", 0)),
        "cross_duplicates_expected": int(len(cross_ids)),
        "cross_duplicates_caught": int(len(cross_flagged)),
        "invalid_expected": int((labels["expect"] == "INVALID").sum()),
        "invalid_quarantined": quarantined,
    }
    return {"summary": report, "per_scenario": per_scenario.reset_index().to_dict("records")}


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--labels", default="data/labels/labels.jsonl")
    args = ap.parse_args(argv)
    result = evaluate(args.labels, build_sink(load_settings()))
    print(json.dumps(result["summary"], indent=2))
    print(pd.DataFrame(result["per_scenario"]).to_string(index=False))


if __name__ == "__main__":
    main()

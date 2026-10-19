"""Weekly feedback loop: which rules generate noise for the fraud team?

Uses analyst dispositions (core.fraud_case_feedback) to compute precision per rule and
warns when a rule falls below the target, so thresholds in config/rules.yaml get tuned.
Also trims quarantine data older than the retention period (PII minimisation).
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models import Variable

from fraud_common import checks
from fraud_common.runtime import enforce, get_query, on_failure_callback


@dag(
    dag_id="fraud_rule_review_weekly",
    schedule="0 2 * * 1",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"owner": "fraud-data-platform", "retries": 1, "on_failure_callback": on_failure_callback},
    tags=["fraud", "rules", "governance"],
    doc_md=__doc__,
)
def fraud_rule_review_weekly():
    @task
    def rule_precision() -> dict:
        query, _ = get_query()
        return enforce(checks.check_rule_precision(
            query, min_precision=float(Variable.get("fraud_min_rule_precision", default_var=0.2))))

    @task
    def purge_quarantine() -> None:
        _, hook = get_query()
        days = int(Variable.get("fraud_quarantine_retention_days", default_var=30))
        cutoff = datetime.utcnow() - timedelta(days=days)
        hook.run("DELETE FROM core.quarantine_events WHERE ingest_ts < %s", parameters=(cutoff,))

    rule_precision()
    purge_quarantine()


fraud_rule_review_weekly()

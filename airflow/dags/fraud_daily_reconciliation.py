"""Daily (01:00 WIB): prove the numbers are right before the business day starts.

1. reconcile         input = scored + duplicates ; scored rows == rows in warehouse
2. duplicate_integrity  no duplicated txn_id / idempotency_key (Snowflake has no PK enforcement)
3. refresh_marts     force-refresh dynamic tables (Snowflake) - no-op on Postgres views
4. daily_summary     post yesterday's fraud & DQ summary to the team channel
"""
from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.decorators import dag, task

from fraud_common import checks
from fraud_common.runtime import enforce, get_query, notify, on_failure_callback

WIB = pendulum.timezone("Asia/Jakarta")


@dag(
    dag_id="fraud_daily_reconciliation",
    schedule="0 1 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz=WIB),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "fraud-data-platform", "retries": 2, "retry_delay": timedelta(minutes=5),
                  "on_failure_callback": on_failure_callback},
    tags=["fraud", "reconciliation", "data-quality"],
    doc_md=__doc__,
)
def fraud_daily_reconciliation():
    @task
    def reconcile(data_interval_start=None, data_interval_end=None) -> dict:
        query, _ = get_query()
        start = data_interval_start.in_timezone("UTC").naive()
        end = data_interval_end.in_timezone("UTC").naive()
        return enforce(checks.check_reconciliation(query, start, end))

    @task
    def duplicate_integrity() -> dict:
        query, _ = get_query()
        return enforce(checks.check_duplicate_integrity(query))

    @task
    def refresh_marts() -> None:
        _, hook = get_query()
        if hook.conn_type == "snowflake":
            for dt in ("FRAUD_DB.MART.DT_FRAUD_KPI_HOURLY", "FRAUD_DB.MART.DT_CHANNEL_DAILY_SUMMARY"):
                hook.run(f"ALTER DYNAMIC TABLE {dt} REFRESH")

    @task
    def daily_summary(data_interval_start=None, data_interval_end=None) -> None:
        query, _ = get_query()
        start = data_interval_start.in_timezone("UTC").naive()
        end = data_interval_end.in_timezone("UTC").naive()
        txn, amount, flagged, held = query(
            "SELECT COUNT(*), COALESCE(SUM(amount_idr), 0), "
            "SUM(CASE WHEN decision IN ('REVIEW','HOLD') THEN 1 ELSE 0 END), "
            "SUM(CASE WHEN decision = 'HOLD' THEN 1 ELSE 0 END) "
            "FROM core.fct_transactions WHERE NOT is_duplicate AND event_ts >= ? AND event_ts < ?",
            [start, end])[0]
        dq = query("SELECT COALESCE(SUM(quarantined_rows),0), COALESCE(SUM(exact_duplicates + cross_duplicates),0) "
                   "FROM ops.pipeline_batch_metrics WHERE batch_ts >= ? AND batch_ts < ?", [start, end])[0]
        top = query("SELECT rule_code, COUNT(*) FROM core.transaction_rule_hits WHERE event_ts >= ? "
                    "AND event_ts < ? GROUP BY rule_code ORDER BY 2 DESC LIMIT 5", [start, end])
        notify(f"Fraud daily summary {start:%Y-%m-%d}",
               f"txn={txn} amount=Rp{float(amount):,.0f} flagged={flagged} hold={held} "
               f"quarantined={dq[0]} duplicates_removed={dq[1]} top_rules={top}", "INFO")

    [reconcile(), duplicate_integrity()] >> refresh_marts() >> daily_summary()


fraud_daily_reconciliation()

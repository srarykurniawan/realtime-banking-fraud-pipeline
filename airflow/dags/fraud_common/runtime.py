"""Airflow glue: warehouse access, result persistence and notifications."""
from __future__ import annotations

import json
import logging
import urllib.request

from .checks import FAIL, CheckResult

log = logging.getLogger(__name__)

DEFAULT_CONN_ID = "fraud_dw"   # Postgres locally, Snowflake in production (same SQL)


def _variable(name: str, default: str | None = None) -> str | None:
    from airflow.models import Variable

    return Variable.get(name, default_var=default)


def get_query():
    """Return query(sql, params) bound to the warehouse connection."""
    from airflow.hooks.base import BaseHook

    hook = BaseHook.get_connection(_variable("fraud_warehouse_conn_id", DEFAULT_CONN_ID)).get_hook()

    def query(sql: str, params: list | None = None) -> list:
        return hook.get_records(sql.replace("?", "%s"), parameters=params or None)

    return query, hook


def record(result: CheckResult) -> None:
    query, hook = get_query()
    hook.run(
        "INSERT INTO ops.dq_check_results (check_name, run_ts, status, observed, threshold, details) "
        "VALUES (%s, %s, %s, %s, %s, %s)",
        parameters=(result.check_name, result.run_ts, result.status, result.observed, result.threshold,
                    result.details[:4000]),
    )


def notify(title: str, message: str, severity: str = "WARN") -> None:
    """Slack webhook and/or SNS (-> email, PagerDuty). Never raises."""
    text = f"[{severity}] {title}\n{message}"
    webhook = _variable("fraud_slack_webhook_url")
    if webhook:
        try:
            req = urllib.request.Request(webhook, data=json.dumps({"text": text}).encode(),
                                         headers={"Content-Type": "application/json"})
            urllib.request.urlopen(req, timeout=10)
        except Exception:
            log.exception("slack notification failed")
    topic = _variable("fraud_sns_topic_arn")
    if topic:
        try:
            import boto3

            boto3.client("sns").publish(TopicArn=topic, Subject=title[:100], Message=text)
        except Exception:
            log.exception("sns notification failed")
    log.warning(text)


def enforce(result: CheckResult) -> dict:
    """Persist the result, notify on WARN/FAIL and fail the task on FAIL."""
    from airflow.exceptions import AirflowException

    record(result)
    if result.status != "PASS":
        notify(f"Fraud platform check '{result.check_name}' = {result.status}", result.details, result.status)
    if result.status == FAIL:
        raise AirflowException(f"{result.check_name} FAILED: {result.details}")
    return result.as_row()


def on_failure_callback(context) -> None:
    ti = context.get("task_instance")
    notify(f"Airflow task failed: {ti.dag_id}.{ti.task_id}",
           f"run={context.get('run_id')} log={getattr(ti, 'log_url', '')}", "FAIL")

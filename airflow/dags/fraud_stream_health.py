"""Every 5 minutes: is the real-time fraud pipeline alive, fresh and fed with good data?

heartbeat ──(stale)──► restart_streaming_job ──► (alert)
event_lag
quarantine_ratio
volume_anomaly
hold_alert_backlog

A FAIL raises -> task fails -> on_failure_callback -> Slack / SNS (PagerDuty).
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.bash import BashOperator

from fraud_common import checks
from fraud_common.runtime import enforce, get_query, on_failure_callback, record

DEFAULT_ARGS = {
    "owner": "fraud-data-platform",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": on_failure_callback,
}


@dag(
    dag_id="fraud_stream_health",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(minutes=4),
    tags=["fraud", "monitoring", "sla"],
    doc_md=__doc__,
)
def fraud_stream_health():
    @task.short_circuit(ignore_downstream_trigger_rules=False)
    def heartbeat_is_stale() -> bool:
        """Returns True (-> run restart) when the stream has not committed a batch within SLA."""
        query, _ = get_query()
        sla = int(Variable.get("fraud_heartbeat_sla_seconds", default_var=300))
        result = checks.check_heartbeat(query, sla_seconds=sla)
        record(result)
        return result.status == checks.FAIL

    # Restart command is environment specific, e.g.
    #   kubectl -n fraud rollout restart deployment/fraud-stream
    #   aws emr-containers start-job-run ... (EMR on EKS)
    restart = BashOperator(
        task_id="restart_streaming_job",
        bash_command="{{ var.value.get('fraud_stream_restart_cmd', 'echo \"no restart command configured\"') }}",
    )

    @task
    def alert_pipeline_down() -> None:
        query, _ = get_query()
        sla = int(Variable.get("fraud_heartbeat_sla_seconds", default_var=300))
        enforce(checks.check_heartbeat(query, sla_seconds=sla))   # notifies + fails the run

    @task
    def event_lag() -> dict:
        query, _ = get_query()
        return enforce(checks.check_event_lag(query, max_lag_seconds=int(
            Variable.get("fraud_max_event_lag_seconds", default_var=180))))

    @task
    def quarantine_ratio() -> dict:
        query, _ = get_query()
        return enforce(checks.check_quarantine_ratio(query, max_ratio=float(
            Variable.get("fraud_max_quarantine_ratio", default_var=0.05))))

    @task
    def volume_anomaly() -> dict:
        query, _ = get_query()
        return enforce(checks.check_volume_anomaly(query))

    @task
    def hold_alert_backlog() -> dict:
        query, _ = get_query()
        return enforce(checks.check_alert_backlog(query))

    heartbeat_is_stale() >> restart >> alert_pipeline_down()
    event_lag()
    quarantine_ratio()
    volume_anomaly()
    hold_alert_backlog()


fraud_stream_health()

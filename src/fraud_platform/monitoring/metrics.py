"""Pipeline metrics emitters.

Metrics are emitted once per micro-batch (and from the Spark StreamingQueryListener).
CloudWatch alarms in ``infra/terraform/cloudwatch.tf`` are built on these names:

=====================  ======  =============================================================
Metric                 Unit    Meaning
=====================  ======  =============================================================
BatchCompleted         Count   1 per successful batch -> "missing data" alarm = pipeline down
InputRows              Count   records read in the batch
QuarantinedRows        Count   records rejected by data-quality checks
QuarantineRatio        None    quarantined / input (alarm > 5%)
DuplicateRows          Count   exact + cross-channel duplicates removed
FraudAlerts            Count   REVIEW + HOLD decisions
HoldDecisions          Count   HOLD decisions (critical business alarm)
EventLagSeconds        Seconds now - max(event_ts) in batch (freshness)
BatchDurationSeconds   Seconds processing time of the batch
=====================  ======  =============================================================
"""
from __future__ import annotations

import json
import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone

log = logging.getLogger("fraud_platform.metrics")

UNITS = {"EventLagSeconds": "Seconds", "BatchDurationSeconds": "Seconds", "QuarantineRatio": "None",
         "ProcessedRowsPerSecond": "Count/Second", "InputRowsPerSecond": "Count/Second"}


class MetricsEmitter(ABC):
    @abstractmethod
    def emit(self, metrics: dict[str, float], dimensions: dict[str, str]) -> None: ...


class NoopEmitter(MetricsEmitter):
    def emit(self, metrics, dimensions) -> None:
        pass


class LoggingEmitter(MetricsEmitter):
    """Structured JSON log line - picked up by CloudWatch Logs / Loki / Datadog agents."""

    def emit(self, metrics, dimensions) -> None:
        log.info(json.dumps({"type": "metrics", "ts": datetime.now(timezone.utc).isoformat(),
                             "dimensions": dimensions, "metrics": metrics}, default=str))


class CloudWatchEmitter(MetricsEmitter):
    def __init__(self, namespace: str, region: str):
        import boto3

        self.namespace = namespace
        self.client = boto3.client("cloudwatch", region_name=region)

    def emit(self, metrics, dimensions) -> None:
        dims = [{"Name": k, "Value": str(v)} for k, v in dimensions.items()]
        data = [{"MetricName": name, "Dimensions": dims, "Value": float(value),
                 "Unit": UNITS.get(name, "Count")}
                for name, value in metrics.items() if value is not None]
        for i in range(0, len(data), 20):        # PutMetricData limit per call
            try:
                self.client.put_metric_data(Namespace=self.namespace, MetricData=data[i:i + 20])
            except Exception:  # monitoring must never break the pipeline
                log.exception("failed to push metrics to CloudWatch")


def build_emitter(settings: dict) -> MetricsEmitter:
    m = settings.get("monitoring", {})
    kind = m.get("emitter", "logging")
    if kind == "cloudwatch":
        return CloudWatchEmitter(m.get("cloudwatch_namespace", "FraudPlatform"), m.get("aws_region", "ap-southeast-3"))
    if kind == "logging":
        return LoggingEmitter()
    return NoopEmitter()

"""Publish REVIEW/HOLD alerts to the ``fraud.alerts`` Kafka topic for case management
and real-time actions (e.g. hold the transaction, push notification to the customer)."""
from __future__ import annotations

import json
import logging

log = logging.getLogger(__name__)


class AlertPublisher:
    def publish(self, alerts: list[dict]) -> None:  # pragma: no cover - interface
        raise NotImplementedError


class NoopAlertPublisher(AlertPublisher):
    def publish(self, alerts: list[dict]) -> None:
        if alerts:
            log.debug("alert publishing disabled: %d alerts", len(alerts))


class KafkaAlertPublisher(AlertPublisher):
    def __init__(self, bootstrap: str, topic: str, extra: dict | None = None):
        from confluent_kafka import Producer

        self.topic = topic
        self.producer = Producer({"bootstrap.servers": bootstrap, "enable.idempotence": True,
                                  "acks": "all", **(extra or {})})

    def publish(self, alerts: list[dict]) -> None:
        for a in alerts:
            self.producer.produce(self.topic, key=str(a["alert_id"]).encode(),
                                  value=json.dumps(a, default=str).encode())
        self.producer.flush(30)


def build_alert_publisher(settings: dict) -> AlertPublisher:
    s = settings.get("streaming", {})
    if s.get("publish_alerts_to_kafka"):
        return KafkaAlertPublisher(settings["source"]["kafka_bootstrap_servers"], settings["topics"]["alerts"])
    return NoopAlertPublisher()

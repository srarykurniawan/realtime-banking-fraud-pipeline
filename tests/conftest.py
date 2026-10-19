import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "airflow" / "dags"))

from fraud_platform.config import load_rules, load_settings  # noqa: E402


@pytest.fixture
def settings(tmp_path):
    s = load_settings(ROOT / "config" / "settings.yaml")
    s["warehouse"]["type"] = "duckdb"
    s["warehouse"]["duckdb_path"] = str(tmp_path / "wh.duckdb")
    s["monitoring"]["emitter"] = "none"
    s["source"]["files_path"] = str(tmp_path / "landing")
    s["streaming"]["checkpoint_dir"] = str(tmp_path / "ckpt")
    s["streaming"]["bronze_dir"] = str(tmp_path / "bronze")
    return s


@pytest.fixture
def rules():
    return load_rules(ROOT / "config" / "rules.yaml")


@pytest.fixture
def now():
    return datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)


def canonical(**overrides):
    """A valid canonical event (as produced by ingest_message) for scoring tests."""
    base = {
        "txn_id": "MOBILE:T1", "source_txn_id": "T1", "idempotency_key": "k-T1", "content_fingerprint": "fp-T1",
        "channel": "MOBILE", "customer_id": "CIF1", "account_id": "1234567890", "txn_type": "TRANSFER",
        "status": "SUCCESS", "amount": 500_000.0, "currency": "IDR", "amount_idr": 500_000.0,
        "event_ts": datetime(2026, 10, 19, 3, 0, 0), "device_id": "DEV-1", "ip_address": None,
        "merchant_name": None, "mcc": None, "counterparty_account": "1111111111", "counterparty_bank": "BCA",
        "city": "Jakarta", "country_code": "ID", "latitude": -6.2, "longitude": 106.8,
    }
    base.update(overrides)
    return base


@pytest.fixture
def make_event():
    counter = {"n": 0}

    def _make(minutes: float = 0, **overrides):
        counter["n"] += 1
        n = counter["n"]
        ev = canonical(txn_id=f"MOBILE:T{n}", source_txn_id=f"T{n}", idempotency_key=f"k-T{n}",
                       content_fingerprint=f"fp-T{n}",
                       event_ts=datetime(2026, 10, 19, 3, 0, 0) + timedelta(minutes=minutes))
        ev.update(overrides)
        return ev

    return _make

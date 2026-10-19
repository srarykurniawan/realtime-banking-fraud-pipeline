"""Problem 6 (incomplete data): every bad record is quarantined with an explicit reason."""
import json
from datetime import timedelta

from fraud_platform.core.validation import ingest_message


def mobile(now, **overrides):
    p = {"trxId": "MB1", "custId": "C", "accountNo": "123", "trxType": "TRANSFER", "status": "SUCCESS",
         "amt": "100000", "ccy": "IDR", "trxTime": (now - timedelta(minutes=1)).isoformat() + "+00:00"}
    p.update(overrides)
    return json.dumps({k: v for k, v in p.items() if v is not None})


def errors(settings, now, **overrides):
    rec = ingest_message("txn.raw.mobile", mobile(now, **overrides), settings, now=now)
    return rec["dq_status"], json.loads(rec["dq_errors"])


def test_valid_record(settings, now):
    assert errors(settings, now) == ("VALID", [])


def test_missing_account(settings, now):
    status, errs = errors(settings, now, accountNo=None)
    assert status == "INVALID" and "DQ_MISSING_ACCOUNT_ID" in errs


def test_negative_amount(settings, now):
    assert "DQ_INVALID_AMOUNT" in errors(settings, now, amt="-5")[1]


def test_non_numeric_amount(settings, now):
    assert errors(settings, now, amt="abc")[1] == ["DQ_INVALID_AMOUNT"]


def test_unsupported_currency(settings, now):
    assert "DQ_UNSUPPORTED_CURRENCY" in errors(settings, now, ccy="XYZ")[1]


def test_amount_out_of_range(settings, now):
    assert "DQ_AMOUNT_OUT_OF_RANGE" in errors(settings, now, amt="99999999999")[1]


def test_future_and_stale_timestamps(settings, now):
    assert "DQ_FUTURE_TIMESTAMP" in errors(settings, now, trxTime=(now + timedelta(hours=1)).isoformat() + "+00:00")[1]
    assert "DQ_STALE_TIMESTAMP" in errors(settings, now, trxTime=(now - timedelta(days=30)).isoformat() + "+00:00")[1]


def test_unknown_txn_type_reported_as_missing(settings, now):
    assert "DQ_MISSING_TXN_TYPE" in errors(settings, now, trxType="TELEPORT")[1]


def test_invalid_geo(settings, now):
    assert "DQ_INVALID_GEO" in errors(settings, now, geo={"lat": 123.0, "lon": 500.0})[1]


def test_multiple_errors_are_all_reported(settings, now):
    _, errs = errors(settings, now, accountNo=None, ccy="XYZ")
    assert {"DQ_MISSING_ACCOUNT_ID", "DQ_UNSUPPORTED_CURRENCY"} <= set(errs)


def test_lineage_is_kept_for_quarantine(settings, now):
    rec = ingest_message("txn.raw.mobile", "{oops", settings, partition=3, offset=42, now=now)
    assert (rec["source_topic"], rec["source_partition"], rec["source_offset"]) == ("txn.raw.mobile", 3, 42)
    assert rec["raw_payload"] == "{oops" and rec["channel"] == "MOBILE"

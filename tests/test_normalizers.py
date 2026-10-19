"""Problem 2: each channel has a different format -> all map to ONE canonical contract."""
import json
from datetime import datetime, timedelta, timezone

import pytest

from fraud_platform.core.normalizers import NormalizationError, channel_from_topic, normalize_card
from fraud_platform.core.validation import ingest_message


def ingest(topic, payload, settings, now):
    value = payload if isinstance(payload, str) else json.dumps(payload)
    return ingest_message(topic, value, settings, now=now)


def test_mobile_iso_time_with_offset_is_converted_to_utc(settings, now):
    local = (now - timedelta(minutes=5)).replace(tzinfo=timezone.utc).astimezone(
        timezone(timedelta(hours=7)))
    rec = ingest("txn.raw.mobile", {
        "trxId": "MB1", "custId": "CIF1", "accountNo": "123", "trxType": "QRIS", "status": "SUCCESS",
        "amt": "150000.00", "ccy": "IDR", "trxTime": local.isoformat(), "deviceId": "D1",
        "geo": {"lat": -6.2, "lon": 106.8, "city": "Jakarta"}, "merchant": {"name": "Indomaret", "mcc": "5411"},
    }, settings, now)
    assert rec["dq_status"] == "VALID", rec["dq_errors"]
    assert rec["txn_id"] == "MOBILE:MB1"
    assert rec["txn_type"] == "PURCHASE"
    assert rec["event_ts"] == now - timedelta(minutes=5)
    assert rec["amount_idr"] == 150000.0 and rec["country_code"] == "ID"


def test_atm_epoch_millis_and_response_codes(settings, now):
    ts = now - timedelta(minutes=1)
    rec = ingest("txn.raw.atm", {
        "atm_txn_ref": "A1", "cif": "CIF1", "account_number": "123", "card_hash": "C1", "txn_code": "WD",
        "response_code": "51", "amount_idr": 500000,
        "ts_epoch_ms": int(ts.replace(tzinfo=timezone.utc).timestamp() * 1000),
        "terminal_city": "Medan", "terminal_lat": 3.59, "terminal_lon": 98.67,
    }, settings, now)
    assert rec["dq_status"] == "VALID"
    assert rec["status"] == "DECLINED" and rec["txn_type"] == "WITHDRAWAL" and rec["mcc"] == "6011"
    assert rec["event_ts"] == ts


def test_card_iso8583_minor_units_numeric_currency_and_wib_time(settings, now):
    wib = (now - timedelta(minutes=2)).replace(tzinfo=timezone.utc).astimezone(timezone(timedelta(hours=7)))
    rec = ingest("txn.raw.card", {
        "rrn": "R1", "cif": "CIF1", "acct": "123", "pan_hash": "P1", "de4_amount": "000000150050",
        "de49_currency": "840", "de39_response": "00", "txn_datetime_local": wib.strftime("%Y%m%d%H%M%S"),
        "mcc": "7995", "merchant_name": "Casino", "merchant_country": "MO",
    }, settings, now)
    assert rec["dq_status"] == "VALID", rec["dq_errors"]
    assert rec["amount"] == 1500.50 and rec["currency"] == "USD"
    assert rec["amount_idr"] == pytest.approx(1500.50 * settings["fx_rates_to_idr"]["USD"])
    assert rec["event_ts"] == now - timedelta(minutes=2)


def test_ibanking_nested_payload(settings, now):
    rec = ingest("txn.raw.ibanking", {
        "reference": "IB-1", "customer": {"cif": "CIF1", "account": "123"},
        "transaction": {"type": "FUND_TRANSFER", "amount": {"value": 1e6, "currency": "IDR"},
                        "timestamp": (now - timedelta(minutes=3)).strftime("%Y-%m-%d %H:%M:%S"),
                        "beneficiary": {"account": "999", "bank_code": "BNI"}},
        "session": {"ip": "1.2.3.4", "device_fp": "FP1"},
    }, settings, now)
    assert rec["dq_status"] == "VALID"
    assert rec["txn_type"] == "TRANSFER" and rec["counterparty_account"] == "999" and rec["ip_address"] == "1.2.3.4"


@pytest.mark.parametrize("event_time_kind", ["iso", "epoch"])
def test_partner_feed_accepts_iso_or_epoch(settings, now, event_time_kind):
    ts = now - timedelta(minutes=4)
    event_time = ts.strftime("%Y-%m-%dT%H:%M:%SZ") if event_time_kind == "iso" else \
        int(ts.replace(tzinfo=timezone.utc).timestamp())
    rec = ingest("txn.raw.partner", {
        "partner_code": "EW", "partner_txn_id": "X1", "account_ref": "123", "category": "TOPUP",
        "status": "SETTLED", "nominal": "250000", "currency": "IDR", "event_time": event_time,
    }, settings, now)
    assert rec["dq_status"] == "VALID" and rec["txn_id"] == "PARTNER:EW-X1" and rec["event_ts"] == ts


def test_same_money_from_two_channels_shares_content_fingerprint(settings, now):
    t = now - timedelta(minutes=1)
    mobile = ingest("txn.raw.mobile", {"trxId": "M", "accountNo": "123", "trxType": "TOPUP", "amt": "100000",
                                       "ccy": "IDR", "trxTime": t.replace(tzinfo=timezone.utc).isoformat(),
                                       "dest": {"account": "EW081", "bank": "EWALLET"}}, settings, now)
    partner = ingest("txn.raw.partner", {"partner_code": "EW", "partner_txn_id": "P", "account_ref": "123",
                                         "category": "TOPUP", "status": "SETTLED", "nominal": "100000",
                                         "currency": "IDR", "event_time": t.strftime("%Y-%m-%dT%H:%M:%SZ"),
                                         "beneficiary_account": "EW081"}, settings, now)
    assert mobile["content_fingerprint"] == partner["content_fingerprint"]
    assert mobile["idempotency_key"] != partner["idempotency_key"]


def test_card_unknown_numeric_currency_raises():
    with pytest.raises(NormalizationError) as exc:
        normalize_card({"de49_currency": "999", "de4_amount": "000000001000"})
    assert exc.value.code == "UNSUPPORTED_CURRENCY"


def test_channel_from_topic():
    assert channel_from_topic("txn.raw.mobile") == "mobile"
    assert channel_from_topic("txn.raw.unknown") is None
    assert channel_from_topic(None) is None


def test_ingest_never_raises_on_garbage(settings, now):
    for topic, value in [("txn.raw.mobile", "{not json"), ("txn.raw.mobile", None), ("txn.raw.zzz", "{}"),
                         ("txn.raw.card", json.dumps([1, 2])), ("txn.raw.atm", json.dumps({"ts_epoch_ms": "x"}))]:
        rec = ingest_message(topic, value, settings, now=now)
        assert rec["dq_status"] == "INVALID"
        assert json.loads(rec["dq_errors"])


def test_timestamps_are_naive_utc(settings, now):
    rec = ingest("txn.raw.ibanking", {"reference": "IB-2", "customer": {"account": "1"}, "transaction": {
        "type": "BILLPAY", "amount": {"value": 1, "currency": "IDR"},
        "timestamp": now.strftime("%Y-%m-%d %H:%M:%S")}}, settings, now)
    assert isinstance(rec["event_ts"], datetime) and rec["event_ts"].tzinfo is None

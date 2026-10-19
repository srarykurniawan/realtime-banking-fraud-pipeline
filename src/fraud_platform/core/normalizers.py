"""Channel-specific payload -> canonical transaction mapping.

Each source channel has a different payload format (field names, timestamp format,
amount representation, currency coding). Normalisers are small pure functions that
map one raw payload into the canonical record defined in :mod:`schema`. Unknown or
unparseable payloads raise :class:`NormalizationError`, which the caller turns into a
quarantine (DLQ) record - never a silent drop.

Supported formats (see docs/data_contracts.md for full examples):

* MOBILE   - camelCase JSON, ISO-8601 time with offset, amount as decimal string
* ATM      - snake_case flat JSON, epoch millis, integer rupiah
* CARD     - ISO 8583-like: amount in minor units (12 digits), numeric currency, local WIB time
* IBANKING - nested JSON, "YYYY-MM-DD HH:MM:SS" UTC, amount object {value, currency}
* PARTNER  - 3rd-party feed (e-wallet/PG), ISO-8601 'Z' or epoch seconds
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Callable
from zoneinfo import ZoneInfo

WIB = ZoneInfo("Asia/Jakarta")

ISO4217_NUMERIC = {"360": "IDR", "840": "USD", "702": "SGD", "978": "EUR", "458": "MYR"}
MINOR_UNIT_EXPONENT = {"IDR": 2, "USD": 2, "SGD": 2, "EUR": 2, "MYR": 2}


class NormalizationError(ValueError):
    """Raised when a payload cannot be mapped to the canonical contract."""

    def __init__(self, code: str, detail: str = ""):
        super().__init__(f"{code}: {detail}" if detail else code)
        self.code = code
        self.detail = detail


# --------------------------------------------------------------------------- helpers
def _to_utc_naive(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        raise NormalizationError("TIMESTAMP_WITHOUT_TZ", str(dt))
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


def parse_iso8601(value: Any) -> datetime:
    if not isinstance(value, str) or not value:
        raise NormalizationError("INVALID_TIMESTAMP", repr(value))
    try:
        return _to_utc_naive(datetime.fromisoformat(value.replace("Z", "+00:00")))
    except ValueError as exc:
        raise NormalizationError("INVALID_TIMESTAMP", value) from exc


def parse_epoch(value: Any, unit: str = "ms") -> datetime:
    try:
        seconds = float(value) / (1000.0 if unit == "ms" else 1.0)
        return datetime.fromtimestamp(seconds, tz=timezone.utc).replace(tzinfo=None)
    except (TypeError, ValueError, OverflowError, OSError) as exc:
        raise NormalizationError("INVALID_TIMESTAMP", repr(value)) from exc


def parse_decimal(value: Any, field: str = "amount") -> Decimal:
    if value is None or isinstance(value, bool):
        raise NormalizationError("INVALID_AMOUNT", f"{field}={value!r}")
    try:
        return Decimal(str(value).strip().replace(",", ""))
    except (InvalidOperation, ValueError) as exc:
        raise NormalizationError("INVALID_AMOUNT", f"{field}={value!r}") from exc


def _opt_str(value: Any) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    return s or None


def _opt_float(value: Any) -> float | None:
    try:
        return None if value is None or value == "" else float(value)
    except (TypeError, ValueError):
        return None


def sha256(*parts: Any) -> str:
    return hashlib.sha256("|".join("" if p is None else str(p) for p in parts).encode()).hexdigest()


# --------------------------------------------------------------------------- channel mappers
_MOBILE_TYPES = {"TRANSFER": "TRANSFER", "QRIS": "PURCHASE", "BILLPAY": "BILL_PAYMENT",
                 "TOPUP": "TOPUP", "PAYMENT": "PURCHASE"}
_MOBILE_STATUS = {"SUCCESS": "SUCCESS", "OK": "SUCCESS", "FAILED": "DECLINED",
                  "REJECTED": "DECLINED", "PENDING": "PENDING"}


def normalize_mobile(p: dict) -> dict:
    geo = p.get("geo") or {}
    merchant = p.get("merchant") or {}
    dest = p.get("dest") or {}
    return {
        "source_txn_id": _opt_str(p.get("trxId")),
        "channel": "MOBILE",
        "customer_id": _opt_str(p.get("custId")),
        "account_id": _opt_str(p.get("accountNo")),
        "txn_type": _MOBILE_TYPES.get(str(p.get("trxType", "")).upper()),
        "status": _MOBILE_STATUS.get(str(p.get("status", "SUCCESS")).upper()),
        "amount": parse_decimal(p.get("amt")),
        "currency": _opt_str(p.get("ccy")),
        "event_ts": parse_iso8601(p.get("trxTime")),
        "device_id": _opt_str(p.get("deviceId")),
        "ip_address": _opt_str(p.get("ipAddr")),
        "merchant_name": _opt_str(merchant.get("name")),
        "mcc": _opt_str(merchant.get("mcc")),
        "counterparty_account": _opt_str(dest.get("account")),
        "counterparty_bank": _opt_str(dest.get("bank")),
        "city": _opt_str(geo.get("city")),
        "country_code": _opt_str(geo.get("country")) or "ID",
        "latitude": _opt_float(geo.get("lat")),
        "longitude": _opt_float(geo.get("lon")),
    }


_ATM_TYPES = {"WD": "WITHDRAWAL", "TRF": "TRANSFER", "BP": "BILL_PAYMENT", "BAL": None}
_ATM_STATUS = {"00": "SUCCESS", "51": "DECLINED", "55": "DECLINED", "61": "DECLINED", "91": "DECLINED"}


def normalize_atm(p: dict) -> dict:
    return {
        "source_txn_id": _opt_str(p.get("atm_txn_ref")),
        "channel": "ATM",
        "customer_id": _opt_str(p.get("cif")),
        "account_id": _opt_str(p.get("account_number")),
        "txn_type": _ATM_TYPES.get(str(p.get("txn_code", "")).upper()),
        "status": _ATM_STATUS.get(str(p.get("response_code", "00")), "DECLINED"),
        "amount": parse_decimal(p.get("amount_idr")),
        "currency": "IDR",
        "event_ts": parse_epoch(p.get("ts_epoch_ms"), "ms"),
        "device_id": _opt_str(p.get("card_hash")),     # the card acts as the "device" at an ATM
        "ip_address": None,
        "merchant_name": _opt_str(p.get("terminal_id")),
        "mcc": "6011",
        "counterparty_account": _opt_str(p.get("dest_account")),
        "counterparty_bank": _opt_str(p.get("dest_bank")),
        "city": _opt_str(p.get("terminal_city")),
        "country_code": _opt_str(p.get("terminal_country")) or "ID",
        "latitude": _opt_float(p.get("terminal_lat")),
        "longitude": _opt_float(p.get("terminal_lon")),
    }


_CARD_STATUS = {"00": "SUCCESS", "05": "DECLINED", "14": "DECLINED", "51": "DECLINED",
                "54": "DECLINED", "57": "DECLINED", "59": "DECLINED"}


def normalize_card(p: dict) -> dict:
    currency = ISO4217_NUMERIC.get(str(p.get("de49_currency", "")).zfill(3))
    if currency is None:
        raise NormalizationError("UNSUPPORTED_CURRENCY", f"numeric={p.get('de49_currency')!r}")
    raw_amount = str(p.get("de4_amount", ""))
    if not raw_amount.isdigit():
        raise NormalizationError("INVALID_AMOUNT", f"de4_amount={raw_amount!r}")
    amount = Decimal(raw_amount) / (Decimal(10) ** MINOR_UNIT_EXPONENT[currency])
    local = p.get("txn_datetime_local")
    try:
        ts = datetime.strptime(str(local), "%Y%m%d%H%M%S").replace(tzinfo=WIB)
    except ValueError as exc:
        raise NormalizationError("INVALID_TIMESTAMP", repr(local)) from exc
    return {
        "source_txn_id": _opt_str(p.get("rrn")),
        "channel": "CARD",
        "customer_id": _opt_str(p.get("cif")),
        "account_id": _opt_str(p.get("acct")),
        "txn_type": "PURCHASE",
        "status": _CARD_STATUS.get(str(p.get("de39_response", "00")), "DECLINED"),
        "amount": amount,
        "currency": currency,
        "event_ts": _to_utc_naive(ts),
        "device_id": _opt_str(p.get("pan_hash")),
        "ip_address": None,
        "merchant_name": _opt_str(p.get("merchant_name")),
        "mcc": _opt_str(p.get("mcc")),
        "counterparty_account": None,
        "counterparty_bank": None,
        "city": _opt_str(p.get("merchant_city")),
        "country_code": _opt_str(p.get("merchant_country")),
        "latitude": _opt_float(p.get("merchant_lat")),
        "longitude": _opt_float(p.get("merchant_lon")),
    }


_IB_TYPES = {"FUND_TRANSFER": "TRANSFER", "BILLPAY": "BILL_PAYMENT", "TOPUP": "TOPUP",
             "ONLINE_PURCHASE": "PURCHASE"}


def normalize_ibanking(p: dict) -> dict:
    cust = p.get("customer") or {}
    trx = p.get("transaction") or {}
    amount_obj = trx.get("amount") or {}
    session = p.get("session") or {}
    ben = trx.get("beneficiary") or {}
    ts_raw = trx.get("timestamp")
    try:
        ts = datetime.strptime(str(ts_raw), "%Y-%m-%d %H:%M:%S")  # documented as UTC
    except ValueError as exc:
        raise NormalizationError("INVALID_TIMESTAMP", repr(ts_raw)) from exc
    return {
        "source_txn_id": _opt_str(p.get("reference")),
        "channel": "IBANKING",
        "customer_id": _opt_str(cust.get("cif")),
        "account_id": _opt_str(cust.get("account")),
        "txn_type": _IB_TYPES.get(str(trx.get("type", "")).upper()),
        "status": "SUCCESS" if str(trx.get("result", "COMPLETED")).upper() == "COMPLETED" else "DECLINED",
        "amount": parse_decimal(amount_obj.get("value")),
        "currency": _opt_str(amount_obj.get("currency")),
        "event_ts": ts,
        "device_id": _opt_str(session.get("device_fp")),
        "ip_address": _opt_str(session.get("ip")),
        "merchant_name": _opt_str(trx.get("biller")),
        "mcc": None,
        "counterparty_account": _opt_str(ben.get("account")),
        "counterparty_bank": _opt_str(ben.get("bank_code")),
        "city": _opt_str(session.get("geo_city")),
        "country_code": _opt_str(session.get("geo_country")) or "ID",
        "latitude": _opt_float(session.get("geo_lat")),
        "longitude": _opt_float(session.get("geo_lon")),
    }


_PARTNER_TYPES = {"TOPUP": "TOPUP", "P2P": "TRANSFER", "PAYMENT": "PURCHASE", "CASHOUT": "WITHDRAWAL"}
_PARTNER_STATUS = {"SETTLED": "SUCCESS", "SUCCESS": "SUCCESS", "FAILED": "DECLINED",
                   "PENDING": "PENDING", "REFUNDED": "REVERSED"}


def normalize_partner(p: dict) -> dict:
    event_time = p.get("event_time")
    ts = parse_epoch(event_time, "s") if isinstance(event_time, (int, float)) else parse_iso8601(event_time)
    partner = _opt_str(p.get("partner_code")) or "UNKNOWN"
    ptx = _opt_str(p.get("partner_txn_id"))
    return {
        "source_txn_id": f"{partner}-{ptx}" if ptx else None,
        "channel": "PARTNER",
        "customer_id": _opt_str(p.get("customer_ref")),
        "account_id": _opt_str(p.get("account_ref")),
        "txn_type": _PARTNER_TYPES.get(str(p.get("category", "")).upper()),
        "status": _PARTNER_STATUS.get(str(p.get("status", "")).upper()),
        "amount": parse_decimal(p.get("nominal")),
        "currency": _opt_str(p.get("currency")),
        "event_ts": ts,
        "device_id": _opt_str(p.get("device_id")),
        "ip_address": None,
        "merchant_name": _opt_str(p.get("merchant")),
        "mcc": _opt_str(p.get("mcc")),
        "counterparty_account": _opt_str(p.get("beneficiary_account")),
        "counterparty_bank": _opt_str(p.get("beneficiary_bank")),
        "city": None,
        "country_code": "ID",
        "latitude": None,
        "longitude": None,
    }


NORMALIZERS: dict[str, Callable[[dict], dict]] = {
    "mobile": normalize_mobile,
    "atm": normalize_atm,
    "card": normalize_card,
    "ibanking": normalize_ibanking,
    "partner": normalize_partner,
}


def channel_from_topic(topic: str | None) -> str | None:
    """'txn.raw.mobile' -> 'mobile'."""
    if not topic:
        return None
    key = topic.rsplit(".", 1)[-1].lower()
    return key if key in NORMALIZERS else None


def parse_payload(value: str | bytes | None) -> dict:
    if value is None:
        raise NormalizationError("EMPTY_PAYLOAD")
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="replace")
    try:
        payload = json.loads(value)
    except json.JSONDecodeError as exc:
        raise NormalizationError("MALFORMED_JSON", str(exc)[:120]) from exc
    if not isinstance(payload, dict):
        raise NormalizationError("MALFORMED_JSON", "payload is not an object")
    return payload


def enrich_keys(rec: dict, fx_rates: dict[str, float]) -> dict:
    """Derive ids, IDR amount and duplicate keys on a canonical record (in place)."""
    channel, src = rec.get("channel"), rec.get("source_txn_id")
    amount = rec.get("amount")
    rec["amount"] = float(amount) if amount is not None else None
    rate = fx_rates.get(rec.get("currency") or "")
    rec["amount_idr"] = round(float(amount) * float(rate), 2) if amount is not None and rate else None
    rec["txn_id"] = f"{channel}:{src}" if channel and src else None
    rec["idempotency_key"] = sha256(channel, src) if channel and src else None
    counterparty = rec.get("counterparty_account") or rec.get("merchant_name") or ""
    rec["content_fingerprint"] = sha256(
        rec.get("account_id"), f"{rec['amount_idr']:.2f}" if rec["amount_idr"] is not None else None,
        counterparty, rec.get("txn_type"),
    )
    return rec

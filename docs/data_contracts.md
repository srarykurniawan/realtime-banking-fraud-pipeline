# Kontrak Data

## 1. Format sumber per kanal

Semua topik membawa JSON UTF-8, key Kafka = nomor rekening.

### `txn.raw.mobile` — Mobile Banking
```json
{"trxId": "MB7F3A...", "custId": "CIF100023", "accountNo": "4821937465", "trxType": "TRANSFER",
 "status": "SUCCESS", "amt": "1500000.00", "ccy": "IDR", "trxTime": "2026-10-19T08:37:12+07:00",
 "deviceId": "MD-9f2c...", "ipAddr": "10.1.2.7",
 "geo": {"lat": -6.2088, "lon": 106.8456, "city": "Jakarta", "country": "ID"},
 "merchant": null, "dest": {"bank": "BCA", "account": "1234567890"}}
```
Waktu ISO-8601 dengan offset · nominal string desimal · `trxType`: TRANSFER/QRIS/BILLPAY/TOPUP.

### `txn.raw.atm` — ATM switch
```json
{"atm_txn_ref": "ATM1A2B...", "cif": "CIF100023", "account_number": "4821937465", "card_hash": "CH-...",
 "txn_code": "WD", "response_code": "00", "amount_idr": 500000, "ts_epoch_ms": 1792373832000,
 "terminal_id": "ATM-JKT-07", "terminal_city": "Jakarta", "terminal_lat": -6.2, "terminal_lon": 106.8}
```
Epoch milidetik · rupiah bulat · `response_code` 00 = sukses, lainnya = ditolak.

### `txn.raw.card` — Kartu / POS (gaya ISO 8583)
```json
{"rrn": "7F3A9C2D1E0B", "stan": "004211", "cif": "CIF100023", "acct": "4821937465", "pan_hash": "PH-...",
 "de4_amount": "000000150050", "de49_currency": "840", "de39_response": "00",
 "txn_datetime_local": "20261019083712", "mcc": "7995", "merchant_name": "Casino Lisboa",
 "merchant_city": "Macau", "merchant_country": "MO", "merchant_lat": 22.19, "merchant_lon": 113.54}
```
Nominal dalam *minor units* 12 digit · mata uang numerik ISO-4217 (360=IDR, 840=USD, 702=SGD) ·
waktu lokal WIB `yyyyMMddHHmmss` · PAN tidak pernah dikirim, hanya hash.

### `txn.raw.ibanking` — Internet Banking
```json
{"reference": "IB-...", "customer": {"cif": "CIF100023", "account": "4821937465"},
 "transaction": {"type": "FUND_TRANSFER", "amount": {"value": 85000000.0, "currency": "IDR"},
                 "timestamp": "2026-10-19 01:37:12", "result": "COMPLETED",
                 "beneficiary": {"account": "9990001111", "bank_code": "BNI"}, "biller": null},
 "session": {"ip": "36.80.1.12", "device_fp": "FP-...", "geo_city": null}}
```
Timestamp UTC tanpa offset (didokumentasikan sebagai UTC).

### `txn.raw.partner` — feed pihak ketiga (e-wallet / payment gateway)
```json
{"partner_code": "EWALLET_A", "partner_txn_id": "F86F...", "customer_ref": "CIF100023",
 "account_ref": "4821937465", "category": "TOPUP", "status": "SETTLED", "nominal": "250000",
 "currency": "IDR", "event_time": "2026-10-19T01:36:09Z", "beneficiary_account": "EW0812..."}
```
`event_time` bisa ISO-8601 `Z` **atau** epoch detik.

## 2. Skema kanonik (`core.fct_transactions`)

| Kolom | Arti |
|---|---|
| `txn_id` | `<CHANNEL>:<source_txn_id>`, unik global (PK) |
| `idempotency_key` | sha256(channel, source_txn_id): kunci duplikat exact |
| `content_fingerprint` | sha256(account, amount_idr, counterparty/merchant, txn_type): kunci duplikat lintas kanal |
| `channel` | MOBILE, ATM, CARD, IBANKING, PARTNER |
| `txn_type` | TRANSFER, WITHDRAWAL, PURCHASE, BILL_PAYMENT, TOPUP |
| `status` | SUCCESS, DECLINED, PENDING, REVERSED |
| `amount`, `currency`, `amount_idr` | nominal asli + konversi IDR |
| `event_ts` | waktu kejadian, UTC |
| `device_id`, `ip_address`, `city`, `country_code`, `latitude`, `longitude` | konteks perangkat & lokasi |
| `merchant_name`, `mcc`, `counterparty_account`, `counterparty_bank` | lawan transaksi |
| `source_topic`, `source_partition`, `source_offset`, `ingest_ts` | lineage ke Kafka |
| `is_duplicate`, `duplicate_type`, `duplicate_of` | hasil dedup |
| `risk_score` (0–100), `risk_level`, `decision` | LOW/MEDIUM/HIGH/CRITICAL · ALLOW/REVIEW/HOLD/DUPLICATE |
| `reason_codes`, `explanation`, `rule_hits` (JSON), `features` (JSON) | explainability |
| `rules_version`, `rules_hash`, `scored_at` | audit konfigurasi rule |

## 3. Kode data quality (karantina)

| Kode | Arti |
|---|---|
| `DQ_MALFORMED_JSON` / `DQ_EMPTY_PAYLOAD` | payload tidak bisa di-parse |
| `DQ_UNKNOWN_CHANNEL` | topik tidak dikenal |
| `DQ_MISSING_<FIELD>` | field wajib kosong (mis. `DQ_MISSING_ACCOUNT_ID`, juga jenis transaksi tak dikenal) |
| `DQ_INVALID_AMOUNT` / `DQ_AMOUNT_OUT_OF_RANGE` | nominal ≤ 0, bukan angka, atau > Rp10 miliar |
| `DQ_UNSUPPORTED_CURRENCY` | mata uang tidak didukung / tidak ada kurs |
| `DQ_INVALID_TIMESTAMP` / `DQ_FUTURE_TIMESTAMP` / `DQ_STALE_TIMESTAMP` | format waktu salah, > 5 menit di masa depan, > 7 hari lalu |
| `DQ_INVALID_GEO` | koordinat tidak valid |
| `DQ_NORMALIZATION_FAILED:<Error>` | bug mapper (tertangkap, stream tetap jalan) |

## 4. Reason code fraud

| Kode | Bobot | Kondisi (default `config/rules.yaml`) |
|---|---|---|
| `WATCHLIST_COUNTERPARTY` | 0.80 | rekening tujuan ada di watchlist (rekening penampung) |
| `IMPOSSIBLE_TRAVEL` | 0.60 | ≥ 100 km dengan kecepatan tersirat > 800 km/jam |
| `CARD_TESTING` | 0.50 | ≥ 3 transaksi ditolak dalam 15 menit |
| `AMOUNT_SPIKE` | 0.45 | ≥ 5× rata-rata historis (min. 5 riwayat, ≥ Rp5 jt) |
| `STRUCTURING` | 0.45 | ≥ 3 transaksi 80–100% dari Rp100 jt dalam 24 jam |
| `VELOCITY` | 0.40 | > 5 transaksi dalam 10 menit |
| `NEW_DEVICE_HIGH_VALUE` | 0.35 | perangkat/kartu baru untuk nominal ≥ Rp10 jt |
| `LARGE_AMOUNT` | 0.30 | ≥ Rp100 jt |
| `HIGH_RISK_MCC` | 0.30 | judi, quasi-cash/crypto, wire transfer, sekuritas |
| `FOREIGN_COUNTRY` | 0.25 | negara asing yang belum pernah dipakai akun |
| `ODD_HOUR` | 0.15 | 00:00–04:59 WIB dengan nominal ≥ Rp5 jt |

Keputusan: skor ≥ 70 → `HOLD`, ≥ 40 → `REVIEW`, selain itu `ALLOW`.

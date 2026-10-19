"""Multi-channel banking transaction simulator.

Produces realistic raw payloads for 5 channels (each in its own format), plus injected
fraud scenarios, exact & cross-channel duplicates and malformed records. A ground-truth
label file is written next to the data so detection quality can be measured
(``fraud-evaluate``).

Modes
-----
* ``batch``    : generate a historical window (default 6h ending now) -> files or Kafka
* ``realtime`` : endless live stream with event time = now -> Kafka (or files)

Examples::

    fraud-simulate --mode batch --sink files --out data/landing --labels data/labels/labels.jsonl
    fraud-simulate --mode realtime --sink kafka --bootstrap localhost:9092 --rate 20
"""
from __future__ import annotations

import argparse
import json
import math
import random
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

WIB = ZoneInfo("Asia/Jakarta")

CITIES = {
    "Jakarta": (-6.2088, 106.8456), "Surabaya": (-7.2575, 112.7521), "Bandung": (-6.9175, 107.6191),
    "Medan": (3.5952, 98.6722), "Pekanbaru": (0.5071, 101.4478), "Makassar": (-5.1477, 119.4327),
    "Denpasar": (-8.6705, 115.2126), "Yogyakarta": (-7.7956, 110.3695), "Semarang": (-6.9667, 110.4167),
}
FOREIGN = {"Macau": (22.1987, 113.5439, "MO"), "Singapore": (1.2903, 103.8519, "SG")}
BANKS = ["BCA", "BRI", "BNI", "MDR", "BSI", "CIMB"]
MERCHANTS = [("Indomaret", "5411"), ("Alfamart", "5411"), ("Tokopedia", "5399"), ("Shopee", "5399"),
             ("Starbucks", "5814"), ("Pertamina", "5541"), ("Erafone", "5732"), ("Hotel Santika", "7011"),
             ("Garuda Indonesia", "4511"), ("Kimia Farma", "5912"), ("Grab", "4121")]
WATCHLIST = ["9990001111", "9990002222", "9990003333"]
TOPICS = {"MOBILE": "txn.raw.mobile", "ATM": "txn.raw.atm", "CARD": "txn.raw.card",
          "IBANKING": "txn.raw.ibanking", "PARTNER": "txn.raw.partner"}
CHANNEL_MIX = [("MOBILE", 0.45), ("CARD", 0.25), ("ATM", 0.12), ("IBANKING", 0.10), ("PARTNER", 0.08)]


@dataclass
class Customer:
    cif: str
    account: str
    city: str
    mobile_device: str
    card_hash: str
    pan_hash: str
    ib_device: str
    wallet_id: str
    typical_amount: float


@dataclass
class Txn:
    """Channel-agnostic logical transaction; rendered into a channel payload later."""
    channel: str
    cust: Customer
    ts: datetime                      # aware, UTC
    txn_type: str
    amount: float
    status: str = "SUCCESS"
    currency: str = "IDR"
    city: str | None = None
    country: str = "ID"
    lat: float | None = None
    lon: float | None = None
    device: str | None = None
    merchant: str | None = None
    mcc: str | None = None
    counterparty: str | None = None
    bank: str | None = None
    src_id: str = field(default_factory=lambda: uuid.uuid4().hex[:16].upper())
    scenario: str = "NORMAL"
    is_fraud: bool = False
    attack_id: str | None = None      # groups the events of one injected attack


@dataclass
class Message:
    ts: datetime
    topic: str
    key: str
    value: str
    label: dict


class Simulator:
    def __init__(self, n_customers: int = 150, seed: int = 42):
        self.rng = random.Random(seed)
        self.customers = [self._new_customer(i) for i in range(n_customers)]
        self.seq = 0

    # ------------------------------------------------------------------ entities
    def _new_customer(self, i: int) -> Customer:
        r = self.rng
        city = r.choices(list(CITIES), weights=[30, 12, 10, 8, 6, 6, 5, 5, 5])[0]
        return Customer(
            cif=f"CIF{100000 + i}", account=f"{r.randint(10**9, 9 * 10**9 - 1)}", city=city,
            mobile_device=f"MD-{uuid.UUID(int=r.getrandbits(128)).hex[:12]}",
            card_hash=f"CH-{uuid.UUID(int=r.getrandbits(128)).hex[:12]}",
            pan_hash=f"PH-{uuid.UUID(int=r.getrandbits(128)).hex[:16]}",
            ib_device=f"FP-{uuid.UUID(int=r.getrandbits(128)).hex[:12]}",
            wallet_id=f"EW08{r.randint(10**9, 10**10 - 1)}",
            typical_amount=round(math.exp(r.uniform(math.log(100_000), math.log(2_500_000))), -3),
        )

    def _geo(self, city: str, jitter: float = 0.03) -> tuple[float, float]:
        lat, lon = CITIES[city]
        return round(lat + self.rng.uniform(-jitter, jitter), 5), round(lon + self.rng.uniform(-jitter, jitter), 5)

    def _amount(self, c: Customer) -> float:
        return max(10_000.0, round(c.typical_amount * self.rng.lognormvariate(0, 0.5), -3))

    # ------------------------------------------------------------------ normal traffic
    def normal_txn(self, ts: datetime, cust: Customer | None = None) -> Txn:
        r = self.rng
        c = cust or r.choice(self.customers)
        channel = r.choices([ch for ch, _ in CHANNEL_MIX], weights=[w for _, w in CHANNEL_MIX])[0]
        lat, lon = self._geo(c.city)
        t = Txn(channel=channel, cust=c, ts=ts, txn_type="TRANSFER", amount=self._amount(c),
                city=c.city, lat=lat, lon=lon)
        if channel == "MOBILE":
            t.txn_type = r.choice(["TRANSFER", "PURCHASE", "BILL_PAYMENT", "TOPUP"])
            t.device = c.mobile_device
            if t.txn_type == "PURCHASE":
                t.merchant, t.mcc = r.choice(MERCHANTS)
            elif t.txn_type == "TOPUP":
                t.counterparty, t.bank = c.wallet_id, "EWALLET"
                t.amount = float(r.choice([50_000, 100_000, 200_000, 500_000]))
            elif t.txn_type == "BILL_PAYMENT":
                t.merchant = r.choice(["PLN", "BPJS", "Telkomsel", "PDAM"])
            else:
                t.counterparty, t.bank = f"{r.randint(10**9, 9 * 10**9)}", r.choice(BANKS)
        elif channel == "CARD":
            t.txn_type, t.device = "PURCHASE", c.pan_hash
            t.merchant, t.mcc = r.choice(MERCHANTS)
        elif channel == "ATM":
            t.txn_type, t.device = "WITHDRAWAL", c.card_hash
            t.amount = float(r.choice([100_000, 200_000, 500_000, 1_000_000, 1_500_000]))
        elif channel == "IBANKING":
            t.txn_type = r.choice(["TRANSFER", "BILL_PAYMENT"])
            t.device = c.ib_device
            if t.txn_type == "TRANSFER":
                t.counterparty, t.bank = f"{r.randint(10**9, 9 * 10**9)}", r.choice(BANKS)
            else:
                t.merchant = r.choice(["PLN", "BPJS", "PBB"])
        else:  # PARTNER (e-wallet payments funded from the bank account)
            t.txn_type, t.device = "PURCHASE", None
            t.merchant, t.mcc = r.choice(MERCHANTS)
            t.lat = t.lon = None
            t.city = None
        return t

    # ------------------------------------------------------------------ fraud scenarios
    def scenario(self, name: str, ts: datetime) -> list[Txn]:
        r, c = self.rng, self.rng.choice(self.customers)
        out: list[Txn] = []
        if name == "ACCOUNT_TAKEOVER":       # new device + mule account + big transfer
            lat, lon = self._geo("Jakarta")
            out.append(Txn("MOBILE", c, ts, "TRANSFER", float(r.randint(60, 95) * 1_000_000),
                           device=f"MD-NEW-{uuid.uuid4().hex[:8]}", counterparty=r.choice(WATCHLIST),
                           bank="BRI", city="Jakarta", lat=lat, lon=lon))
        elif name == "IMPOSSIBLE_TRAVEL":
            far = r.choice([x for x in ("Medan", "Makassar", "Denpasar") if x != c.city] or ["Medan"])
            home = c.city if c.city != far else "Jakarta"
            out.append(Txn("ATM", c, ts, "WITHDRAWAL", 2_000_000.0, device=c.card_hash, city=home,
                           lat=CITIES[home][0], lon=CITIES[home][1], merchant="ATM-JKT-01"))
            out.append(Txn("CARD", c, ts + timedelta(minutes=r.randint(8, 20)), "PURCHASE",
                           float(r.randint(5, 15) * 1_000_000), device=c.pan_hash, city=far,
                           lat=CITIES[far][0], lon=CITIES[far][1], merchant="Erafone", mcc="5732"))
        elif name == "CARD_TESTING":
            for i in range(4):
                out.append(Txn("CARD", c, ts + timedelta(minutes=i), "PURCHASE",
                               float(r.randint(1, 5) * 10_000), status="DECLINED", device=c.pan_hash,
                               merchant="ONLINE-STORE-X", mcc="5999", city="Jakarta"))
            out.append(Txn("CARD", c, ts + timedelta(minutes=6), "PURCHASE", 8_500_000.0,
                           device=c.pan_hash, merchant="ONLINE-STORE-X", mcc="5732", city="Jakarta"))
        elif name == "VELOCITY":
            for i in range(8):
                out.append(Txn("MOBILE", c, ts + timedelta(seconds=40 * i), "TRANSFER",
                               float(r.randint(2, 5) * 1_000_000), device=c.mobile_device,
                               counterparty=f"{r.randint(10**9, 9 * 10**9)}", bank=r.choice(BANKS),
                               city=c.city, lat=CITIES[c.city][0], lon=CITIES[c.city][1]))
        elif name == "STRUCTURING":
            for i in range(4):
                out.append(Txn("IBANKING", c, ts + timedelta(minutes=45 * i), "TRANSFER",
                               float(r.randint(85, 99) * 1_000_000), device=c.ib_device,
                               counterparty=f"{r.randint(10**9, 9 * 10**9)}", bank=r.choice(BANKS)))
        elif name == "FOREIGN_GAMBLING":
            lat, lon, cc = FOREIGN["Macau"]
            out.append(Txn("CARD", c, ts, "PURCHASE", float(r.randint(1500, 3000)), currency="USD",
                           device=c.pan_hash, merchant="Casino Lisboa", mcc="7995", city="Macau",
                           country=cc, lat=lat, lon=lon))
        elif name == "MULE_TRANSFER":
            out.append(Txn("IBANKING", c, ts, "TRANSFER", float(r.randint(10, 40) * 1_000_000),
                           device=c.ib_device, counterparty=r.choice(WATCHLIST), bank="BNI"))
        attack_id = f"ATK-{uuid.UUID(int=r.getrandbits(128)).hex[:8]}"
        for t in out:
            t.scenario, t.is_fraud, t.attack_id = name, True, attack_id
        return out

    # ------------------------------------------------------------------ rendering
    def render(self, t: Txn) -> tuple[str, str, dict]:
        wib, utc = t.ts.astimezone(WIB), t.ts.astimezone(timezone.utc)
        c = t.cust
        if t.channel == "MOBILE":
            payload = {
                "trxId": f"MB{t.src_id}", "custId": c.cif, "accountNo": c.account,
                "trxType": {"TRANSFER": "TRANSFER", "PURCHASE": "QRIS", "BILL_PAYMENT": "BILLPAY",
                            "TOPUP": "TOPUP"}[t.txn_type],
                "status": "SUCCESS" if t.status == "SUCCESS" else "FAILED",
                "amt": f"{t.amount:.2f}", "ccy": t.currency, "trxTime": wib.isoformat(timespec="seconds"),
                "deviceId": t.device, "ipAddr": f"10.{self.rng.randint(0, 255)}.{self.rng.randint(0, 255)}.7",
                "geo": {"lat": t.lat, "lon": t.lon, "city": t.city, "country": t.country},
                "merchant": {"name": t.merchant, "mcc": t.mcc} if t.merchant else None,
                "dest": {"bank": t.bank, "account": t.counterparty} if t.counterparty else None,
                "appVersion": "5.12.0",
            }
            src = payload["trxId"]
        elif t.channel == "ATM":
            payload = {
                "atm_txn_ref": f"ATM{t.src_id}", "cif": c.cif, "account_number": c.account,
                "card_hash": t.device, "txn_code": "WD" if t.txn_type == "WITHDRAWAL" else "TRF",
                "response_code": "00" if t.status == "SUCCESS" else "51", "amount_idr": int(t.amount),
                "ts_epoch_ms": int(utc.timestamp() * 1000), "terminal_id": t.merchant or f"ATM-{t.city[:3].upper()}-07",
                "terminal_city": t.city, "terminal_country": "ID", "terminal_lat": t.lat, "terminal_lon": t.lon,
            }
            src = payload["atm_txn_ref"]
        elif t.channel == "CARD":
            numeric = {"IDR": "360", "USD": "840", "SGD": "702"}.get(t.currency, "999")
            payload = {
                "rrn": t.src_id[:12], "stan": f"{self.rng.randint(0, 999999):06d}", "cif": c.cif, "acct": c.account,
                "pan_hash": t.device, "de4_amount": f"{int(round(t.amount * 100)):012d}", "de49_currency": numeric,
                "de39_response": "00" if t.status == "SUCCESS" else "51",
                "txn_datetime_local": wib.strftime("%Y%m%d%H%M%S"), "mcc": t.mcc, "merchant_name": t.merchant,
                "merchant_city": t.city, "merchant_country": t.country, "merchant_lat": t.lat,
                "merchant_lon": t.lon, "pos_entry_mode": "051" if t.lat else "812",
            }
            src = payload["rrn"]
        elif t.channel == "IBANKING":
            payload = {
                "reference": f"IB-{t.src_id}",
                "customer": {"cif": c.cif, "account": c.account},
                "transaction": {
                    "type": {"TRANSFER": "FUND_TRANSFER", "BILL_PAYMENT": "BILLPAY", "TOPUP": "TOPUP",
                             "PURCHASE": "ONLINE_PURCHASE"}[t.txn_type],
                    "amount": {"value": t.amount, "currency": t.currency},
                    "timestamp": utc.strftime("%Y-%m-%d %H:%M:%S"),
                    "result": "COMPLETED" if t.status == "SUCCESS" else "REJECTED",
                    "beneficiary": {"account": t.counterparty, "bank_code": t.bank} if t.counterparty else None,
                    "biller": t.merchant,
                },
                "session": {"ip": f"36.{self.rng.randint(64, 95)}.{self.rng.randint(0, 255)}.12",
                            "device_fp": t.device, "geo_city": t.city, "geo_country": t.country,
                            "geo_lat": t.lat, "geo_lon": t.lon},
            }
            src = payload["reference"]
        else:  # PARTNER
            category = {"TOPUP": "TOPUP", "TRANSFER": "P2P", "PURCHASE": "PAYMENT", "WITHDRAWAL": "CASHOUT"}
            use_epoch = self.rng.random() < 0.5
            payload = {
                "partner_code": "EWALLET_A", "partner_txn_id": t.src_id, "customer_ref": c.cif,
                "account_ref": c.account, "category": category[t.txn_type],
                "status": "SETTLED" if t.status == "SUCCESS" else "FAILED",
                "nominal": str(int(t.amount)), "currency": t.currency,
                "event_time": int(utc.timestamp()) if use_epoch else utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "merchant": t.merchant, "mcc": t.mcc, "beneficiary_account": t.counterparty,
                "beneficiary_bank": t.bank, "device_id": t.device,
            }
            src = f"EWALLET_A-{t.src_id}"
        return TOPICS[t.channel], f"{t.channel}:{src}", payload

    def _msg(self, ts: datetime, topic: str, key: str, value: str, txn_id: str | None,
             scenario: str, is_fraud: bool, expect: str, attack_id: str | None = None) -> Message:
        self.seq += 1
        return Message(ts, topic, key, value, {"seq": self.seq, "txn_id": txn_id, "scenario": scenario,
                                               "is_fraud": is_fraud, "expect": expect, "attack_id": attack_id})

    def to_messages(self, t: Txn) -> list[Message]:
        topic, txn_id, payload = self.render(t)
        return [self._msg(t.ts, topic, t.cust.account, json.dumps(payload), txn_id, t.scenario, t.is_fraud,
                          "VALID", t.attack_id)]

    # ------------------------------------------------------------------ data problems
    def exact_duplicate(self, m: Message) -> Message:
        """Producer retry / replay: byte-identical payload arrives again."""
        return self._msg(m.ts + timedelta(seconds=self.rng.randint(1, 30)), m.topic, m.key, m.value,
                         m.label["txn_id"], m.label["scenario"], m.label["is_fraud"], "EXACT_DUP",
                         m.label.get("attack_id"))

    def cross_channel_pair(self, ts: datetime) -> list[Message]:
        """Mobile top-up also reported by the e-wallet partner feed (different id, same money)."""
        c = self.rng.choice(self.customers)
        amount = float(self.rng.choice([100_000, 250_000, 500_000]))
        mob = Txn("MOBILE", c, ts, "TOPUP", amount, device=c.mobile_device, counterparty=c.wallet_id,
                  bank="EWALLET", city=c.city, lat=CITIES[c.city][0], lon=CITIES[c.city][1])
        par = Txn("PARTNER", c, ts + timedelta(seconds=self.rng.randint(20, 90)), "TOPUP", amount,
                  counterparty=c.wallet_id, bank="EWALLET")
        m1 = self.to_messages(mob)[0]
        topic, txn_id, payload = self.render(par)
        m2 = self._msg(par.ts, topic, c.account, json.dumps(payload), txn_id, "NORMAL", False, "CROSS_DUP")
        m2.label["pair_of"] = m1.label["txn_id"]
        return [m1, m2]

    def malformed(self, ts: datetime) -> Message:
        kind = self.rng.choice(["bad_json", "negative", "missing_account", "bad_currency", "future", "bad_card_ccy"])
        t = self.normal_txn(ts)
        t.channel = "MOBILE" if kind not in ("bad_card_ccy",) else "CARD"
        if t.channel == "MOBILE":
            t.txn_type, t.device, t.merchant, t.mcc = "TRANSFER", t.cust.mobile_device, None, None
            t.counterparty, t.bank = "1234567890", "BCA"
        else:
            t.txn_type, t.device, t.merchant, t.mcc = "PURCHASE", t.cust.pan_hash, "Indomaret", "5411"
        topic, txn_id, payload = self.render(t)
        if kind == "bad_json":
            value = json.dumps(payload)[:-15]
        else:
            if kind == "negative":
                payload["amt"] = "-150000.00"
            elif kind == "missing_account":
                payload.pop("accountNo")
            elif kind == "bad_currency":
                payload["ccy"] = "XYZ"
            elif kind == "future":
                payload["trxTime"] = (ts + timedelta(days=2)).astimezone(WIB).isoformat(timespec="seconds")
            elif kind == "bad_card_ccy":
                payload["de49_currency"] = "999"
            value = json.dumps(payload)
        return self._msg(ts, topic, t.cust.account, value, txn_id, f"MALFORMED_{kind.upper()}", False, "INVALID")

    # ------------------------------------------------------------------ window generation
    def generate_window(self, start: datetime, minutes: int, rate_per_min: float = 6.0,
                        dup_ratio: float = 0.02, malformed_ratio: float = 0.01,
                        scenario_start_frac: float = 0.4, scenarios_per_type: int = 3,
                        cross_dup_pairs: int = 10) -> list[Message]:
        r = self.rng
        msgs: list[Message] = []
        for minute in range(minutes):
            n = self._poisson(rate_per_min)
            for _ in range(n):
                ts = start + timedelta(minutes=minute, seconds=r.uniform(0, 60))
                for m in self.to_messages(self.normal_txn(ts)):
                    msgs.append(m)
                    if r.random() < dup_ratio:
                        msgs.append(self.exact_duplicate(m))
            if r.random() < malformed_ratio * rate_per_min:
                msgs.append(self.malformed(start + timedelta(minutes=minute, seconds=r.uniform(0, 60))))

        first = start + timedelta(minutes=int(minutes * scenario_start_frac))
        span = max(1, int(minutes * (1 - scenario_start_frac)) - 150)  # leave room for multi-step scenarios
        for name in ("ACCOUNT_TAKEOVER", "IMPOSSIBLE_TRAVEL", "CARD_TESTING", "VELOCITY", "STRUCTURING",
                     "FOREIGN_GAMBLING", "MULE_TRANSFER"):
            for _ in range(scenarios_per_type):
                ts = first + timedelta(minutes=r.uniform(0, span))
                for t in self.scenario(name, ts):
                    msgs.extend(self.to_messages(t))
        for _ in range(cross_dup_pairs):
            msgs.extend(self.cross_channel_pair(first + timedelta(minutes=r.uniform(0, span))))
        msgs.sort(key=lambda m: m.ts)
        return msgs

    def _poisson(self, lam: float) -> int:
        limit, k, p = math.exp(-lam), 0, 1.0
        while True:
            p *= self.rng.random()
            if p <= limit:
                return k
            k += 1


# ---------------------------------------------------------------------- sinks
class FileSink:
    """Writes data/landing/<topic>/part-<n>.jsonl (one file per chunk -> one Spark micro-batch)."""

    def __init__(self, out_dir: str, chunk_size: int = 500):
        self.out = Path(out_dir)
        self.chunk_size = chunk_size

    def send(self, msgs: list[Message]) -> None:
        base = time.time_ns()     # unique & sortable across runs -> files never overwrite each other
        for n, i in enumerate(range(0, len(msgs), self.chunk_size)):
            chunk = msgs[i:i + self.chunk_size]
            part = f"{base}-{n:05d}"
            by_topic: dict[str, list[str]] = {}
            for m in chunk:
                by_topic.setdefault(m.topic, []).append(m.value.replace("\n", " "))
            for topic, lines in by_topic.items():
                d = self.out / topic
                d.mkdir(parents=True, exist_ok=True)
                tmp = d / f".part-{part}.jsonl.tmp"
                tmp.write_text("\n".join(lines) + "\n", encoding="utf-8")
                tmp.rename(d / f"part-{part}.jsonl")   # atomic publish for the file source

    def close(self) -> None:
        pass


class KafkaSink:
    def __init__(self, bootstrap: str, extra: dict | None = None):
        from confluent_kafka import Producer  # lazy import

        conf = {"bootstrap.servers": bootstrap, "enable.idempotence": True, "acks": "all",
                "linger.ms": 20, "compression.type": "lz4", **(extra or {})}
        self.p = Producer(conf)

    def send(self, msgs: list[Message]) -> None:
        for m in msgs:
            # key = account -> all events of an account land on one partition (ordering)
            self.p.produce(m.topic, key=m.key.encode(), value=m.value.encode())
            self.p.poll(0)
        self.p.flush(30)

    def close(self) -> None:
        self.p.flush(30)


def write_labels(path: str, msgs: list[Message]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("a", encoding="utf-8") as fh:
        for m in msgs:
            fh.write(json.dumps(m.label) + "\n")


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description="Banking transaction simulator")
    ap.add_argument("--mode", choices=["batch", "realtime"], default="batch")
    ap.add_argument("--sink", choices=["files", "kafka"], default="files")
    ap.add_argument("--out", default="data/landing")
    ap.add_argument("--labels", default="data/labels/labels.jsonl")
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--customers", type=int, default=150)
    ap.add_argument("--minutes", type=int, default=360, help="batch window length")
    ap.add_argument("--rate", type=float, default=6.0, help="normal transactions per minute")
    ap.add_argument("--chunk-size", type=int, default=500)
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args(argv)

    sim = Simulator(args.customers, args.seed)
    sink = FileSink(args.out, args.chunk_size) if args.sink == "files" else KafkaSink(args.bootstrap)

    if args.mode == "batch":
        end = datetime.now(timezone.utc).replace(microsecond=0)
        msgs = sim.generate_window(end - timedelta(minutes=args.minutes), args.minutes, args.rate)
        sink.send(msgs)
        write_labels(args.labels, msgs)
        frauds = sum(1 for m in msgs if m.label["is_fraud"])
        print(f"generated {len(msgs)} messages ({frauds} fraud, "
              f"{sum(m.label['expect'] != 'VALID' for m in msgs)} dup/invalid) -> {args.sink}")
    else:
        print(f"streaming ~{args.rate}/min to {args.sink} (Ctrl+C to stop)")
        try:
            while True:
                start = datetime.now(timezone.utc)
                msgs = sim.generate_window(start, 1, args.rate, scenario_start_frac=0, scenarios_per_type=0,
                                           cross_dup_pairs=0)
                if sim.rng.random() < 0.3:   # inject a fraud scenario ~ every 3 minutes
                    name = sim.rng.choice(["ACCOUNT_TAKEOVER", "IMPOSSIBLE_TRAVEL", "CARD_TESTING", "VELOCITY",
                                           "FOREIGN_GAMBLING", "MULE_TRANSFER"])
                    for t in sim.scenario(name, start):
                        msgs.extend(sim.to_messages(t))
                if sim.rng.random() < 0.3:
                    msgs.extend(sim.cross_channel_pair(start))
                msgs.sort(key=lambda m: m.ts)
                if isinstance(sink, FileSink):   # files: one file per simulated minute
                    time.sleep(max(0.0, 60 - (datetime.now(timezone.utc) - start).total_seconds()))
                    sink.send(msgs)
                    write_labels(args.labels, msgs)
                    continue
                for m in msgs:                # emit each message at its event time
                    delay = (m.ts - datetime.now(timezone.utc)).total_seconds()
                    if delay > 0:
                        time.sleep(min(delay, 60))
                    sink.send([m])
                write_labels(args.labels, msgs)
        except KeyboardInterrupt:
            pass
        finally:
            sink.close()


if __name__ == "__main__":
    main()

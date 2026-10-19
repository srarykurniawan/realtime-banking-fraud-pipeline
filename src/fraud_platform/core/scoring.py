"""Stateful, explainable fraud scoring.

Design
------
* Scoring is keyed by ``account_id``. Each account has a compact behavioural profile
  (:class:`AccountState`) that Spark keeps in its fault-tolerant state store
  (``applyInPandasWithState``). The same code runs in the lite pipeline and tests.
* Every rule returns a :class:`RuleHit` with a reason code, weight, a human readable
  explanation (Bahasa Indonesia) and the raw evidence numbers. These are persisted so
  an analyst can always see *why* a transaction was flagged (``core.transaction_rule_hits``).
* Rule scores are combined with a noisy-OR: ``score = 100 * (1 - Π(1 - w_i))`` so many
  weak signals add up, but the score never exceeds 100.
* Duplicates are detected BEFORE scoring and never update the profile, so they cannot
  inflate aggregates, velocity counters or the amount baseline.
"""
from __future__ import annotations

import json
import math
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

WIB_OFFSET = timedelta(hours=7)


# --------------------------------------------------------------------------- formatting
def fmt_idr(value: float | None) -> str:
    if value is None:
        return "-"
    return "Rp" + f"{value:,.0f}".replace(",", ".")


def fmt_num(value: float, digits: int = 1) -> str:
    s = f"{value:,.{digits}f}"
    return s.replace(",", "_").replace(".", ",").replace("_", ".")


def epoch(ts: datetime) -> float:
    return ts.replace(tzinfo=timezone.utc).timestamp()


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


# --------------------------------------------------------------------------- state
@dataclass
class AccountState:
    amounts: list[float] = field(default_factory=list)            # baseline (non-declined, not held)
    txn_times: list[float] = field(default_factory=list)          # velocity
    decline_times: list[float] = field(default_factory=list)      # card testing
    near_threshold: list[list[float]] = field(default_factory=list)  # [ts, amount_idr] structuring
    geo_points: list[list[Any]] = field(default_factory=list)     # [ts, lat, lon, city]
    devices: dict[str, float] = field(default_factory=dict)       # device -> last seen
    countries: list[str] = field(default_factory=list)
    seen_keys: dict[str, list[Any]] = field(default_factory=dict)     # idem_key -> [ts, txn_id]
    # fp -> recent occurrences [[ts, channel, txn_id, matched], ...] (one-to-one cross-channel matching)
    fingerprints: dict[str, list[list[Any]]] = field(default_factory=dict)
    txn_count: int = 0
    max_ts: float = 0.0

    def to_json(self) -> str:
        return json.dumps(asdict(self), separators=(",", ":"))

    @classmethod
    def from_json(cls, raw: str | None) -> "AccountState":
        if not raw:
            return cls()
        data = json.loads(raw)
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


@dataclass
class RuleHit:
    code: str
    weight: float
    explanation: str
    evidence: dict[str, Any]


# --------------------------------------------------------------------------- engine
class FraudScorer:
    def __init__(self, rules_cfg: dict):
        self.cfg = rules_cfg
        self.rules = rules_cfg.get("rules", {})
        self.version = str(rules_cfg.get("version", "unknown"))
        self.hash = rules_cfg.get("config_hash", "")
        self.levels = sorted(rules_cfg.get("risk_levels", {"LOW": 0}).items(), key=lambda kv: kv[1])
        dec = rules_cfg.get("decisions", {})
        self.review_threshold = float(dec.get("review_threshold", 40))
        self.hold_threshold = float(dec.get("hold_threshold", 70))
        dup = rules_cfg.get("duplicates", {})
        self.dup_window_s = float(dup.get("cross_channel_window_minutes", 3)) * 60
        self.key_retention_s = float(dup.get("exact_key_retention_hours", 24)) * 3600
        prof = rules_cfg.get("profile", {})
        self.history_size = int(prof.get("amount_history_size", 50))
        self.max_devices = int(prof.get("max_known_devices", 20))
        self.max_geo = int(prof.get("max_geo_points", 10))
        # sliding-window history is kept at least this long so that events from a lagging topic/feed
        # (arriving after newer events of the same account) are still evaluated against the right window
        self.late_tolerance_s = float(prof.get("late_event_tolerance_hours", 6)) * 3600

    def _rule(self, code: str) -> dict | None:
        r = self.rules.get(code)
        return r if r and r.get("enabled", True) else None

    # ---------------------------------------------------------------- public API
    def process(self, event: dict, state: AccountState, scored_at: datetime | None = None) -> dict:
        """Score one canonical event and update ``state`` in place. Returns the output record."""
        scored_at = scored_at or datetime.now(timezone.utc).replace(tzinfo=None)
        ts = epoch(event["event_ts"])
        out = dict(event)
        out.update(is_duplicate=False, duplicate_type=None, duplicate_of=None, rules_version=self.version,
                   rules_hash=self.hash, scored_at=scored_at)

        dup = self._check_duplicate(event, state, ts)
        if dup:
            dup_type, original = dup
            out.update(is_duplicate=True, duplicate_type=dup_type, duplicate_of=original,
                       risk_score=None, risk_level=None, decision="DUPLICATE", reason_codes="",
                       explanation=f"Duplikat ({dup_type}) dari transaksi {original}; tidak dihitung ulang.",
                       rule_hits="[]", features="{}")
            if dup_type == "CROSS_CHANNEL":   # remember the key so a replay of this copy is EXACT
                state.seen_keys[event["idempotency_key"]] = [ts, event["txn_id"]]
            return out

        hits, features = self._evaluate(event, state, ts)
        score = round(100.0 * (1.0 - math.prod(1.0 - h.weight for h in hits)), 2) if hits else 0.0
        level = self._level(score)
        decision = "HOLD" if score >= self.hold_threshold else "REVIEW" if score >= self.review_threshold else "ALLOW"
        hits.sort(key=lambda h: -h.weight)
        out.update(
            risk_score=score, risk_level=level, decision=decision,
            reason_codes=",".join(h.code for h in hits),
            explanation=" | ".join(h.explanation for h in hits) if hits else "Tidak ada indikator risiko.",
            rule_hits=json.dumps([asdict(h) for h in hits], ensure_ascii=False, default=str),
            features=json.dumps(features, default=str),
        )
        self._update_state(event, state, ts, decision)
        return out

    def _level(self, score: float) -> str:
        level = self.levels[0][0]
        for name, lower in self.levels:
            if score >= lower:
                level = name
        return level

    # ---------------------------------------------------------------- duplicates
    def _check_duplicate(self, event: dict, state: AccountState, ts: float) -> tuple[str, str] | None:
        key = event.get("idempotency_key")
        if key and key in state.seen_keys:
            return "EXACT", state.seen_keys[key][1]
        fp = event.get("content_fingerprint")
        if not fp:
            return None
        # nearest-in-time unmatched copy from ANOTHER channel; robust to out-of-order arrival and to a
        # customer legitimately repeating the same amount later in the day
        candidates = [o for o in state.fingerprints.get(fp, [])
                      if o[1] != event.get("channel") and not o[3] and abs(ts - o[0]) <= self.dup_window_s]
        if candidates:
            original = min(candidates, key=lambda o: abs(ts - o[0]))
            original[3] = True
            return "CROSS_CHANNEL", original[2]
        return None

    # ---------------------------------------------------------------- rules
    def _evaluate(self, e: dict, s: AccountState, ts: float) -> tuple[list[RuleHit], dict]:
        hits: list[RuleHit] = []
        amt = float(e.get("amount_idr") or 0.0)
        local_hour = (e["event_ts"] + WIB_OFFSET).hour
        avg = sum(s.amounts) / len(s.amounts) if s.amounts else None
        features: dict[str, Any] = {"amount_idr": amt, "avg_amount_idr": round(avg, 2) if avg else None,
                                    "history_size": len(s.amounts), "local_hour": local_hour,
                                    "known_devices": len(s.devices), "lifetime_txn": s.txn_count}

        if r := self._rule("AMOUNT_SPIKE"):
            if avg and len(s.amounts) >= r["min_history"] and amt >= r["min_amount_idr"] \
                    and amt >= r["multiplier"] * avg:
                ratio = amt / avg
                hits.append(RuleHit("AMOUNT_SPIKE", r["weight"],
                                    f"Nominal {fmt_idr(amt)} = {fmt_num(ratio)}x rata-rata "
                                    f"{len(s.amounts)} transaksi sebelumnya ({fmt_idr(avg)})",
                                    {"ratio": round(ratio, 2), "avg_amount_idr": round(avg, 2)}))

        if r := self._rule("LARGE_AMOUNT"):
            if amt >= r["amount_idr"]:
                hits.append(RuleHit("LARGE_AMOUNT", r["weight"],
                                    f"Nominal {fmt_idr(amt)} melebihi batas {fmt_idr(r['amount_idr'])}",
                                    {"threshold_idr": r["amount_idr"]}))

        if r := self._rule("VELOCITY"):
            window = r["window_minutes"] * 60
            count = sum(1 for t in s.txn_times if ts - window < t <= ts) + 1
            features["velocity_count"] = count
            if count > r["max_txn"]:
                hits.append(RuleHit("VELOCITY", r["weight"],
                                    f"{count} transaksi dalam {r['window_minutes']} menit "
                                    f"(batas {r['max_txn']})", {"count": count}))

        if (r := self._rule("IMPOSSIBLE_TRAVEL")) and e.get("latitude") is not None and s.geo_points:
            # compare with the previous AND the next known location in event time, so the rule still
            # works when events from different channels/topics arrive out of order
            before = [g for g in s.geo_points if g[0] <= ts]
            after = [g for g in s.geo_points if g[0] > ts]
            neighbours = ([max(before, key=lambda g: g[0])] if before else []) + \
                         ([min(after, key=lambda g: g[0])] if after else [])
            worst = None
            for p in neighbours:
                dist = haversine_km(p[1], p[2], e["latitude"], e["longitude"])
                minutes = max(abs(ts - p[0]) / 60.0, 1.0)
                speed = dist / (minutes / 60.0)
                if worst is None or speed > worst[2]:
                    worst = (p, dist, speed, minutes)
            p, dist, speed, minutes = worst
            features.update(distance_km=round(dist, 1), travel_minutes=round(minutes, 1),
                            speed_kmh=round(speed, 1))
            if dist >= r["min_distance_km"] and speed > r["max_speed_kmh"]:
                src, dst = (p[3], e.get("city")) if p[0] <= ts else (e.get("city"), p[3])
                hits.append(RuleHit("IMPOSSIBLE_TRAVEL", r["weight"],
                                    f"Lokasi berpindah {src or '?'} -> {dst or '?'} sejauh "
                                    f"{fmt_num(dist, 0)} km dalam {fmt_num(minutes, 0)} menit "
                                    f"(~{fmt_num(speed, 0)} km/jam)",
                                    {"distance_km": round(dist, 1), "minutes": round(minutes, 1),
                                     "speed_kmh": round(speed, 1), "other_city": p[3]}))

        if (r := self._rule("NEW_DEVICE_HIGH_VALUE")) and e.get("device_id"):
            if e["device_id"] not in s.devices and len(s.devices) >= r["min_known_devices"] \
                    and amt >= r["min_amount_idr"]:
                hits.append(RuleHit("NEW_DEVICE_HIGH_VALUE", r["weight"],
                                    f"Perangkat/kartu baru belum pernah dipakai ({len(s.devices)} perangkat "
                                    f"dikenal) untuk nominal {fmt_idr(amt)}",
                                    {"device_id": e["device_id"], "known_devices": len(s.devices)}))

        if r := self._rule("ODD_HOUR"):
            if r["start_hour"] <= local_hour <= r["end_hour"] and amt >= r["min_amount_idr"]:
                hits.append(RuleHit("ODD_HOUR", r["weight"],
                                    f"Transaksi {fmt_idr(amt)} pada jam {local_hour:02d}:xx WIB "
                                    f"(jam rawan {r['start_hour']:02d}-{r['end_hour']:02d})",
                                    {"local_hour": local_hour}))

        if (r := self._rule("HIGH_RISK_MCC")) and e.get("mcc") in set(map(str, r["mcc"])):
            hits.append(RuleHit("HIGH_RISK_MCC", r["weight"],
                                f"Merchant berisiko tinggi (MCC {e['mcc']} - {e.get('merchant_name') or '-'})",
                                {"mcc": e["mcc"]}))

        if (r := self._rule("FOREIGN_COUNTRY")) and e.get("country_code"):
            cc = e["country_code"]
            if cc != r["home_country"] and cc not in s.countries:
                hits.append(RuleHit("FOREIGN_COUNTRY", r["weight"],
                                    f"Transaksi pertama dari luar negeri ({cc})", {"country_code": cc}))

        if (r := self._rule("WATCHLIST_COUNTERPARTY")) and e.get("counterparty_account"):
            if e["counterparty_account"] in set(map(str, r["accounts"])):
                hits.append(RuleHit("WATCHLIST_COUNTERPARTY", r["weight"],
                                    f"Rekening tujuan {e['counterparty_account']} ada di watchlist "
                                    f"(rekening penampung terindikasi)",
                                    {"counterparty_account": e["counterparty_account"]}))

        if r := self._rule("CARD_TESTING"):
            window = r["window_minutes"] * 60
            declines = sum(1 for t in s.decline_times if ts - window < t <= ts)
            declines += 1 if e.get("status") == "DECLINED" else 0
            features["recent_declines"] = declines
            if declines >= r["min_declines"]:
                tail = " lalu transaksi berhasil" if e.get("status") == "SUCCESS" else ""
                hits.append(RuleHit("CARD_TESTING", r["weight"],
                                    f"{declines} transaksi ditolak dalam {r['window_minutes']} menit{tail}",
                                    {"declines": declines}))

        if r := self._rule("STRUCTURING"):
            lo, hi = r["lower_ratio"] * r["threshold_idr"], r["threshold_idr"]
            if lo <= amt < hi:
                window = r["window_hours"] * 3600
                count = sum(1 for t, _ in s.near_threshold if ts - window < t <= ts) + 1
                features["near_threshold_count"] = count
                if count >= r["min_count"]:
                    hits.append(RuleHit("STRUCTURING", r["weight"],
                                        f"{count} transaksi di kisaran {fmt_idr(lo)}-{fmt_idr(hi)} dalam "
                                        f"{r['window_hours']} jam (indikasi pemecahan transaksi)",
                                        {"count": count}))
        return hits, features

    # ---------------------------------------------------------------- state update
    def _update_state(self, e: dict, s: AccountState, ts: float, decision: str) -> None:
        s.max_ts = max(s.max_ts, ts)
        s.txn_count += 1
        s.seen_keys[e["idempotency_key"]] = [ts, e["txn_id"]]
        if e.get("content_fingerprint"):
            occ = s.fingerprints.setdefault(e["content_fingerprint"], [])
            occ.append([ts, e.get("channel"), e["txn_id"], False])
            del occ[:-20]
        s.txn_times.append(ts)
        amt = float(e.get("amount_idr") or 0.0)
        if e.get("status") == "DECLINED":
            s.decline_times.append(ts)
        elif decision != "HOLD":          # held transactions must not poison the baseline
            s.amounts.append(amt)
            s.amounts = s.amounts[-self.history_size:]
            r = self.rules.get("STRUCTURING") or {}
            if r and r.get("lower_ratio", 0) * r.get("threshold_idr", 0) <= amt < r.get("threshold_idr", 0):
                s.near_threshold.append([ts, amt])
        if e.get("latitude") is not None and e.get("longitude") is not None:
            s.geo_points.append([ts, e["latitude"], e["longitude"], e.get("city")])
            s.geo_points = sorted(s.geo_points, key=lambda g: g[0])[-self.max_geo:]
        if e.get("device_id") and decision != "HOLD":   # unconfirmed device is not trusted yet
            s.devices[e["device_id"]] = ts
            if len(s.devices) > self.max_devices:
                for dev, _ in sorted(s.devices.items(), key=lambda kv: kv[1])[: len(s.devices) - self.max_devices]:
                    del s.devices[dev]
        cc = e.get("country_code")
        if cc and cc not in s.countries and decision != "HOLD":
            s.countries.append(cc)
        self._prune(s)

    def _prune(self, s: AccountState) -> None:
        now = s.max_ts

        def win(code: str, key: str, mult: float) -> float:
            return float((self.rules.get(code) or {}).get(key, 0)) * mult

        late = self.late_tolerance_s
        s.txn_times = [t for t in s.txn_times if t > now - max(win("VELOCITY", "window_minutes", 60), late)]
        s.decline_times = [t for t in s.decline_times if t > now - max(win("CARD_TESTING", "window_minutes", 60), late)]
        s.near_threshold = [x for x in s.near_threshold
                            if x[0] > now - max(win("STRUCTURING", "window_hours", 3600), late)]
        s.seen_keys = {k: v for k, v in s.seen_keys.items() if v[0] > now - self.key_retention_s}
        # Keep fingerprints as long as exact keys: a lagging feed (e.g. the partner topic consumed
        # minutes/hours behind the mobile topic) must still be matched against the original.
        cutoff = now - self.key_retention_s
        s.fingerprints = {k: kept for k, v in s.fingerprints.items() if (kept := [o for o in v if o[0] > cutoff])}


def score_account_events(events: list[dict], state: AccountState, scorer: FraudScorer,
                         scored_at: datetime | None = None) -> list[dict]:
    """Score a batch of events for ONE account in event-time order."""
    ordered = sorted(events, key=lambda ev: (ev["event_ts"], ev.get("txn_id") or ""))
    return [scorer.process(ev, state, scored_at) for ev in ordered]

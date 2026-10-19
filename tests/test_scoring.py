"""Problems 1, 3, 4: early detection, duplicates, explainability."""
import json

import pytest

from fraud_platform.core.scoring import AccountState, FraudScorer, haversine_km, score_account_events


@pytest.fixture
def scorer(rules):
    return FraudScorer(rules)


def warm_up(scorer, state, make_event, n=10, amount=500_000.0):
    """Give the account a normal history: n transactions, 1 per hour, same device/city."""
    for i in range(n):
        scorer.process(make_event(minutes=-60 * (n - i), amount_idr=amount, amount=amount), state)


def codes(out):
    return set(filter(None, out["reason_codes"].split(",")))


def test_normal_transaction_is_allowed_with_explanation(scorer, make_event):
    state = AccountState()
    warm_up(scorer, state, make_event)
    out = scorer.process(make_event(), state)
    assert out["decision"] == "ALLOW" and out["risk_score"] == 0.0
    assert out["explanation"] == "Tidak ada indikator risiko."


def test_amount_spike_and_explanation_contains_numbers(scorer, make_event):
    state = AccountState()
    warm_up(scorer, state, make_event)
    out = scorer.process(make_event(amount_idr=8_000_000.0), state)
    assert "AMOUNT_SPIKE" in codes(out)
    hit = json.loads(out["rule_hits"])[0]
    assert hit["code"] == "AMOUNT_SPIKE" and hit["evidence"]["ratio"] == 16.0
    assert "Rp8.000.000" in hit["explanation"] and "16,0x" in hit["explanation"]


def test_account_takeover_pattern_is_held(scorer, make_event):
    state = AccountState()
    warm_up(scorer, state, make_event)
    out = scorer.process(make_event(amount_idr=75_000_000.0, device_id="NEW-DEVICE",
                                    counterparty_account="9990001111"), state)
    assert {"WATCHLIST_COUNTERPARTY", "NEW_DEVICE_HIGH_VALUE", "AMOUNT_SPIKE"} <= codes(out)
    assert out["decision"] == "HOLD" and out["risk_level"] == "CRITICAL"
    # held txn must not poison the profile
    assert "NEW-DEVICE" not in state.devices and 75_000_000.0 not in state.amounts


def test_velocity(scorer, make_event):
    state = AccountState()
    outs = [scorer.process(make_event(minutes=i), state) for i in range(7)]
    assert "VELOCITY" not in codes(outs[4])
    assert "VELOCITY" in codes(outs[5]) and "VELOCITY" in codes(outs[6])


def test_impossible_travel(scorer, make_event):
    state = AccountState()
    scorer.process(make_event(minutes=0, latitude=-6.2088, longitude=106.8456, city="Jakarta"), state)
    out = scorer.process(make_event(minutes=10, latitude=3.5952, longitude=98.6722, city="Medan"), state)
    assert "IMPOSSIBLE_TRAVEL" in codes(out)
    assert "Jakarta -> Medan" in out["explanation"]


def test_travel_with_enough_time_is_fine(scorer, make_event):
    state = AccountState()
    scorer.process(make_event(minutes=0, latitude=-6.2088, longitude=106.8456, city="Jakarta"), state)
    out = scorer.process(make_event(minutes=300, latitude=3.5952, longitude=98.6722, city="Medan"), state)
    assert "IMPOSSIBLE_TRAVEL" not in codes(out)


def test_card_testing(scorer, make_event):
    state = AccountState()
    for i in range(3):
        scorer.process(make_event(minutes=i, status="DECLINED", amount_idr=20_000.0, channel="CARD"), state)
    out = scorer.process(make_event(minutes=4, amount_idr=8_000_000.0, channel="CARD"), state)
    assert "CARD_TESTING" in codes(out) and "lalu transaksi berhasil" in out["explanation"]


def test_structuring(scorer, make_event):
    state = AccountState()
    outs = [scorer.process(make_event(minutes=60 * i, amount_idr=95_000_000.0), state) for i in range(3)]
    assert "STRUCTURING" not in codes(outs[1]) and "STRUCTURING" in codes(outs[2])


def test_foreign_high_risk_mcc_and_odd_hour(scorer, make_event):
    state = AccountState()
    warm_up(scorer, state, make_event)
    # 19:30 UTC = 02:30 WIB
    ev = make_event(country_code="MO", mcc="7995", merchant_name="Casino", amount_idr=30_000_000.0)
    ev["event_ts"] = ev["event_ts"].replace(hour=19, minute=30)
    out = scorer.process(ev, state)
    assert {"FOREIGN_COUNTRY", "HIGH_RISK_MCC", "ODD_HOUR"} <= codes(out)


def test_risk_levels(scorer):
    assert scorer._level(100.0) == "CRITICAL" and scorer._level(0.0) == "LOW"
    assert scorer._level(59.99) == "MEDIUM" and scorer._level(60) == "HIGH"


def test_noisy_or_score_never_exceeds_100(scorer, make_event):
    state = AccountState()
    warm_up(scorer, state, make_event)
    ev = make_event(amount_idr=500_000_000.0, device_id="X", counterparty_account="9990001111",
                    country_code="MO", mcc="7995")
    out = scorer.process(ev, state)
    assert len(codes(out)) >= 5 and 95 < out["risk_score"] <= 100


# ------------------------------------------------------------------ duplicates (problem 3)
def test_exact_duplicate_is_flagged_and_not_scored_twice(scorer, make_event):
    state = AccountState()
    ev = make_event()
    first = scorer.process(dict(ev), state)
    second = scorer.process(dict(ev), state)
    assert not first["is_duplicate"]
    assert second["is_duplicate"] and second["duplicate_type"] == "EXACT" and second["decision"] == "DUPLICATE"
    assert state.txn_count == 1          # profile (velocity, baseline) counted it once


def test_cross_channel_duplicate(scorer, make_event):
    state = AccountState()
    mobile = make_event(content_fingerprint="same-money", channel="MOBILE")
    partner = make_event(minutes=1, content_fingerprint="same-money", channel="PARTNER", txn_id="PARTNER:X")
    scorer.process(mobile, state)
    out = scorer.process(partner, state)
    assert out["duplicate_type"] == "CROSS_CHANNEL" and out["duplicate_of"] == mobile["txn_id"]


def test_same_channel_repeat_is_not_a_duplicate(scorer, make_event):
    """Two identical coffees in a row at the same merchant are legitimate."""
    state = AccountState()
    scorer.process(make_event(content_fingerprint="coffee"), state)
    out = scorer.process(make_event(minutes=1, content_fingerprint="coffee"), state)
    assert not out["is_duplicate"]


def test_cross_channel_outside_window_is_not_duplicate(scorer, make_event):
    state = AccountState()
    scorer.process(make_event(content_fingerprint="fp", channel="MOBILE"), state)
    out = scorer.process(make_event(minutes=30, content_fingerprint="fp", channel="PARTNER"), state)
    assert not out["is_duplicate"]


# ------------------------------------------------------------------ state & ordering
def test_state_roundtrip_json(scorer, make_event):
    state = AccountState()
    warm_up(scorer, state, make_event, n=3)
    restored = AccountState.from_json(state.to_json())
    assert restored == state


def test_out_of_order_batch_is_scored_in_event_time_order(scorer, make_event):
    state = AccountState()
    events = [make_event(minutes=m) for m in (5, 1, 3)]
    out = score_account_events(events, state, scorer)
    assert [o["event_ts"].minute for o in out] == [1, 3, 5]


def test_state_is_bounded(scorer, make_event, rules):
    state = AccountState()
    for i in range(500):
        scorer.process(make_event(minutes=i * 30, device_id=f"D{i}"), state)
    assert len(state.amounts) <= rules["profile"]["amount_history_size"]
    assert len(state.devices) <= rules["profile"]["max_known_devices"]
    assert len(state.seen_keys) < 60                 # 24h retention at 1 txn / 30 min


def test_disabled_rule_does_not_fire(rules, make_event):
    rules["rules"]["VELOCITY"]["enabled"] = False
    scorer, state = FraudScorer(rules), AccountState()
    outs = [scorer.process(make_event(minutes=i * 0.1), state) for i in range(10)]
    assert all("VELOCITY" not in codes(o) for o in outs)


def test_haversine():
    assert haversine_km(-6.2088, 106.8456, 3.5952, 98.6722) == pytest.approx(1420, rel=0.02)


def test_scores_carry_rule_version_for_audit(scorer, make_event, rules):
    out = scorer.process(make_event(), AccountState())
    assert out["rules_version"] == rules["version"] and out["rules_hash"] == rules["config_hash"]
    assert json.loads(out["features"])["amount_idr"] == 500_000.0


def test_late_cross_channel_copy_after_profile_moved_on(scorer, make_event):
    """A partner feed lagging >1h behind must still be recognised as a duplicate."""
    state = AccountState()
    scorer.process(make_event(minutes=0, content_fingerprint="topup", channel="MOBILE"), state)
    for i in range(1, 6):                        # account keeps transacting for 2+ hours
        scorer.process(make_event(minutes=30 * i), state)
    late = scorer.process(make_event(minutes=1, content_fingerprint="topup", channel="PARTNER"), state)
    assert late["duplicate_type"] == "CROSS_CHANNEL"


def test_cross_channel_matching_is_one_to_one_and_nearest(scorer, make_event):
    """Customer tops up the same amount twice; feeds arrive out of order."""
    state = AccountState()
    m1 = make_event(minutes=0, content_fingerprint="tp", channel="MOBILE")
    m2 = make_event(minutes=60, content_fingerprint="tp", channel="MOBILE")
    p2 = make_event(minutes=61, content_fingerprint="tp", channel="PARTNER")
    p1 = make_event(minutes=1, content_fingerprint="tp", channel="PARTNER")
    outs = {o["txn_id"]: o for o in (scorer.process(e, state) for e in (m1, m2, p2, p1))}
    assert outs[p2["txn_id"]]["duplicate_of"] == m2["txn_id"]
    assert outs[p1["txn_id"]]["duplicate_of"] == m1["txn_id"]
    third = scorer.process(make_event(minutes=62, content_fingerprint="tp", channel="PARTNER"), state)
    assert not third["is_duplicate"]            # m2 already matched -> no double matching


def test_impossible_travel_when_events_arrive_out_of_order(scorer, make_event):
    state = AccountState()
    scorer.process(make_event(minutes=-3, latitude=-6.21, longitude=106.85, city="Jakarta"), state)
    scorer.process(make_event(minutes=12, latitude=3.5952, longitude=98.6722, city="Medan"), state)  # arrives first
    late_atm = scorer.process(make_event(minutes=0, latitude=-6.2088, longitude=106.8456, city="Jakarta"), state)
    assert "IMPOSSIBLE_TRAVEL" in codes(late_atm) and "Jakarta -> Medan" in late_atm["explanation"]


def test_velocity_burst_arriving_late_is_still_detected(scorer, make_event):
    """Mobile topic lags 2h behind: the account's newer events were processed first."""
    state = AccountState()
    scorer.process(make_event(minutes=120), state)           # newer event already seen
    outs = [scorer.process(make_event(minutes=i * 0.5), state) for i in range(8)]
    assert "VELOCITY" in codes(outs[-1])

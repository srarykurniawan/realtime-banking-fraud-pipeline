"""Fraud Investigation Console (Streamlit).

For the fraud operations team: near-real-time KPIs, the alert queue with the full
"why was this flagged?" explanation, the account's recent activity, and a disposition
form that feeds back into rule tuning (core.fraud_case_feedback).

    streamlit run apps/investigation_console/app.py
Reads the same warehouse as the pipeline (FRAUD__WAREHOUSE__TYPE=duckdb|postgres|snowflake).
"""
from __future__ import annotations

import json
import time
from datetime import datetime, timedelta, timezone

import pandas as pd
import streamlit as st

from fraud_platform.config import load_settings
from fraud_platform.sinks.warehouse import build_sink

st.set_page_config(page_title="Fraud Investigation Console", page_icon="🛡️", layout="wide")


@st.cache_resource
def sink():
    return build_sink(load_settings())


def q(sql: str, params=None, retries: int = 5) -> pd.DataFrame:
    for attempt in range(retries):          # DuckDB file may be briefly locked by the writer
        try:
            return sink().query(sql, params)
        except Exception as exc:  # noqa: BLE001
            if attempt == retries - 1:
                st.error(f"Query gagal: {exc}")
                return pd.DataFrame()
            time.sleep(0.5)


def rp(x) -> str:
    return "Rp" + f"{float(x or 0):,.0f}".replace(",", ".")


now = datetime.now(timezone.utc).replace(tzinfo=None)
st.sidebar.title("🛡️ Fraud Console")
page = st.sidebar.radio("Halaman", ["Ringkasan", "Antrean Alert", "Kualitas Data & Pipeline"])
hours = st.sidebar.selectbox("Rentang waktu", [1, 6, 24, 72, 168], index=2, format_func=lambda h: f"{h} jam terakhir")
since = now - timedelta(hours=hours)
if st.sidebar.button("🔄 Refresh"):
    st.rerun()

# --------------------------------------------------------------------------- overview
if page == "Ringkasan":
    st.header("Ringkasan transaksi & fraud")
    k = q("SELECT COUNT(*) AS txn, COALESCE(SUM(amount_idr),0) AS amount, "
          "SUM(CASE WHEN decision IN ('REVIEW','HOLD') THEN 1 ELSE 0 END) AS flagged, "
          "SUM(CASE WHEN decision='HOLD' THEN 1 ELSE 0 END) AS hold, "
          "COALESCE(SUM(CASE WHEN decision IN ('REVIEW','HOLD') THEN amount_idr END),0) AS flagged_amount "
          "FROM core.fct_transactions WHERE NOT is_duplicate AND event_ts >= ?", [since])
    if not k.empty:
        r = k.iloc[0]
        c = st.columns(5)
        c[0].metric("Transaksi", f"{int(r.txn or 0):,}".replace(",", "."))
        c[1].metric("Nominal", rp(r.amount))
        c[2].metric("Ditandai (review+hold)", int(r.flagged or 0))
        c[3].metric("HOLD", int(r.hold or 0))
        c[4].metric("Nominal berisiko", rp(r.flagged_amount))

    hourly = q("SELECT event_hour, channel, txn_count, flagged_count FROM mart.v_fraud_kpi_hourly "
               "WHERE event_hour >= ? ORDER BY event_hour", [since])
    if not hourly.empty:
        left, right = st.columns(2)
        left.subheader("Transaksi per jam per kanal")
        left.bar_chart(hourly.pivot_table(index="event_hour", columns="channel", values="txn_count", aggfunc="sum"))
        right.subheader("Transaksi ditandai per jam")
        right.line_chart(hourly.groupby("event_hour")["flagged_count"].sum())

    st.subheader("Rule paling sering terpicu")
    rules = q("SELECT rule_code, COUNT(*) AS hits, AVG(weight) AS weight FROM core.transaction_rule_hits "
              "WHERE event_ts >= ? GROUP BY rule_code ORDER BY hits DESC", [since])
    st.dataframe(rules, use_container_width=True, hide_index=True)

# --------------------------------------------------------------------------- alerts
elif page == "Antrean Alert":
    st.header("Antrean alert")
    f1, f2, f3 = st.columns(3)
    decisions = f1.multiselect("Keputusan", ["HOLD", "REVIEW"], default=["HOLD", "REVIEW"])
    disp = f2.selectbox("Status disposisi", ["PENDING", "Semua", "CONFIRMED_FRAUD", "FALSE_POSITIVE"])
    min_score = f3.slider("Skor minimum", 0, 100, 40)

    alerts = q("SELECT alert_id, txn_id, event_ts, channel, account_id, amount_idr, risk_score, risk_level, "
               "decision, reason_codes, explanation, disposition FROM mart.v_alert_investigation "
               "WHERE event_ts >= ? AND risk_score >= ? ORDER BY risk_score DESC, event_ts DESC",
               [since, min_score])
    if not alerts.empty:
        alerts = alerts[alerts["decision"].isin(decisions)]
        if disp != "Semua":
            alerts = alerts[alerts["disposition"] == disp]
    st.caption(f"{len(alerts)} alert")
    st.dataframe(alerts.drop(columns=["explanation"]) if not alerts.empty else alerts,
                 use_container_width=True, hide_index=True)

    if not alerts.empty:
        sel = st.selectbox("Investigasi transaksi", alerts["txn_id"].tolist())
        txn = q("SELECT * FROM core.fct_transactions WHERE txn_id = ?", [sel]).iloc[0]
        st.subheader(f"{sel} — skor {txn.risk_score:.1f} ({txn.risk_level}) → {txn.decision}")
        a, b = st.columns([3, 2])
        with a:
            st.markdown("**Mengapa transaksi ini ditandai?**")
            for hit in json.loads(txn.rule_hits or "[]"):
                st.warning(f"**{hit['code']}** (bobot {hit['weight']:.2f}) — {hit['explanation']}")
            st.markdown("**Fitur yang dihitung saat scoring**")
            st.json(json.loads(txn.features or "{}"))
            st.caption(f"Rules version {txn.rules_version} · hash {txn.rules_hash} · scored {txn.scored_at}")
        with b:
            st.markdown("**Detail transaksi**")
            st.table(pd.DataFrame({"nilai": [txn.channel, txn.txn_type, txn.status, rp(txn.amount_idr),
                                             f"{txn.amount:,.2f} {txn.currency}", txn.merchant_name,
                                             txn.mcc, txn.counterparty_account, txn.city, txn.country_code,
                                             txn.device_id, str(txn.event_ts)]},
                                  index=["Kanal", "Jenis", "Status", "Nominal IDR", "Nominal asli", "Merchant",
                                         "MCC", "Rek. tujuan", "Kota", "Negara", "Perangkat", "Waktu (UTC)"]))

        st.markdown("**Aktivitas akun 24 jam di sekitar transaksi**")
        hist = q("SELECT event_ts, channel, txn_type, status, amount_idr, city, device_id, risk_score, decision, "
                 "reason_codes, is_duplicate FROM core.fct_transactions WHERE account_id = ? "
                 "AND event_ts BETWEEN ? AND ? ORDER BY event_ts",
                 [txn.account_id, pd.Timestamp(txn.event_ts) - timedelta(hours=24),
                  pd.Timestamp(txn.event_ts) + timedelta(hours=2)])
        st.dataframe(hist, use_container_width=True, hide_index=True)

        with st.form("disposition"):
            st.markdown("**Disposisi analis** (umpan balik untuk tuning rule)")
            d = st.radio("Hasil investigasi", ["CONFIRMED_FRAUD", "FALSE_POSITIVE"], horizontal=True)
            analyst = st.text_input("Nama analis")
            notes = st.text_area("Catatan")
            if st.form_submit_button("Simpan disposisi"):
                sink().upsert("core.fraud_case_feedback", pd.DataFrame([{
                    "txn_id": sel, "disposition": d, "analyst": analyst, "notes": notes,
                    "decided_at": datetime.now(timezone.utc).replace(tzinfo=None)}]), update=True)
                st.success("Disposisi tersimpan")

# --------------------------------------------------------------------------- DQ & health
else:
    st.header("Kualitas data & kesehatan pipeline")
    health = q("SELECT * FROM mart.v_pipeline_health")
    if not health.empty:
        health["detik_sejak_batch_terakhir"] = (now - pd.to_datetime(health["last_batch_ts"])).dt.total_seconds()
        st.dataframe(health, use_container_width=True, hide_index=True)
        stale = health[health["detik_sejak_batch_terakhir"] > 300]
        if not stale.empty:
            st.error(f"Pipeline tanpa batch baru > 5 menit: {', '.join(stale['pipeline'])}")
    st.subheader("Kualitas data harian")
    st.dataframe(q("SELECT * FROM mart.v_data_quality_daily ORDER BY batch_date DESC"),
                 use_container_width=True, hide_index=True)
    st.subheader("Data dikarantina per jenis error")
    st.dataframe(q("SELECT * FROM mart.v_quarantine_by_error ORDER BY ingest_date DESC, events DESC"),
                 use_container_width=True, hide_index=True)
    st.subheader("Contoh payload yang ditolak")
    st.dataframe(q("SELECT ingest_ts, source_topic, dq_errors, raw_payload FROM core.quarantine_events "
                   "ORDER BY ingest_ts DESC LIMIT 50"), use_container_width=True, hide_index=True)
    st.subheader("Hasil pengecekan Airflow terakhir")
    st.dataframe(q("SELECT * FROM ops.dq_check_results ORDER BY run_ts DESC LIMIT 50"),
                 use_container_width=True, hide_index=True)

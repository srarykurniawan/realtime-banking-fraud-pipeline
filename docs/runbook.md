# Runbook Operasional

Kanal notifikasi: SNS `fraud-<env>-pipeline-alerts` (email/PagerDuty) dan Slack (Airflow).
Semua waktu di tabel warehouse = UTC; tampilan konsol & Airflow = WIB.

## Pipeline stale
**Pemicu:** alarm `pipeline-no-batches`, Snowflake alert `ALERT_STREAM_STALE`, atau DAG `fraud_stream_health` gagal di `alert_pipeline_down`.

1. Cek `mart.v_pipeline_health`: apakah hanya `fraud_scoring` atau semua pipeline yang berhenti?
2. Cek pod/log Spark (`/fraud/<env>/stream` di CloudWatch Logs). Cari `terminated with error`.
3. Jika job mati: DAG sudah menjalankan `fraud_stream_restart_cmd`. Job melanjutkan dari checkpoint; tidak ada data hilang karena offset Kafka belum di-commit.
4. Jika job hidup tetapi tidak ada batch: kemungkinan **sumber diam**. Cek lag & throughput topik `txn.raw.*` di MSK/Kafka UI dan hubungi pemilik kanal.
5. Setelah pulih, pantau `EventLagSeconds` turun ke < 60 detik (backlog terkejar, dibatasi `maxOffsetsPerTrigger`).

## Rasio karantina tinggi
**Pemicu:** alarm `quarantine-ratio` atau cek `quarantine_ratio` FAIL (> 5%).

1. `SELECT * FROM mart.v_quarantine_by_error ORDER BY events DESC` → kode error & topik dominan.
2. Ambil contoh `raw_payload` dari `core.quarantine_events` (hanya role admin/pipeline).
3. Penyebab umum: rilis baru aplikasi kanal mengubah format (`DQ_MISSING_*`), mata uang baru (`DQ_UNSUPPORTED_CURRENCY`), jam server kanal salah (`DQ_FUTURE_TIMESTAMP`).
4. Perbaiki normaliser/konfigurasi → deploy. **Replay**: payload asli ada di bronze S3 dan topik `txn.dlq`; kirim ulang ke topik `txn.raw.<kanal>`. Dedup menjamin tidak ada penghitungan ganda.

## Event lag tinggi
**Pemicu:** alarm `event-lag` (> 180 detik).
- `BatchDurationSeconds` naik → kapasitas kurang: tambah executor / partisi, naikkan ukuran warehouse Snowflake `FRAUD_STREAM_WH`.
- `BatchDurationSeconds` normal tetapi lag tinggi → produsen terlambat mengirim (cek di sisi kanal).

## Anomali volume
**Pemicu:** alarm `input-volume-anomaly` atau cek `volume_anomaly` FAIL (< 50% baseline jam yang sama 7 hari terakhir).
Hampir selalu berarti satu kanal berhenti mengirim. Bandingkan `v_fraud_kpi_hourly` per `channel` untuk menemukan kanalnya.

## Lonjakan HOLD
**Pemicu:** alarm `hold-decisions-spike` (> 50 HOLD / 10 menit).
1. Konsol → Antrean Alert, filter HOLD: apakah satu rule mendominasi?
2. Satu rekening tujuan/merchant berulang → kemungkinan serangan terkoordinasi: eskalasi ke Fraud Ops, tambahkan ke watchlist.
3. Rule baru saja diubah (`rules_version` berganti) → kemungkinan threshold salah: rollback `config/rules.yaml`.

## Rekonsiliasi harian gagal
**Pemicu:** DAG `fraud_daily_reconciliation` task `reconcile` atau `duplicate_integrity` FAIL.
- `flow_gap ≠ 0`: ada baris yang tidak masuk ke unik/duplikat. Periksa batch di `ops.pipeline_batch_metrics` hari itu.
- `load_gap ≠ 0`: jumlah baris di `fct_transactions` tidak sama dengan yang diproses (mis. penghapusan manual / gagal MERGE). Jalankan ulang job untuk rentang offset terkait dari bronze; penulisan idempoten.
- `duplicate_integrity` FAIL (Snowflake tidak menegakkan PK): dedup dengan `QUALIFY ROW_NUMBER() OVER (PARTITION BY txn_id ORDER BY loaded_at) = 1` lalu investigasi penyebab.

## Operasional rutin
- **Tuning rule:** DAG mingguan `fraud_rule_review_weekly` melaporkan rule dengan presisi < 20% berdasarkan disposisi analis. Ubah `config/rules.yaml`, naikkan `version`, `make test` (gate deteksi ≥ 95%, FP ≤ 2%), deploy.
- **Watchlist:** tambahkan rekening penampung yang terkonfirmasi ke `WATCHLIST_COUNTERPARTY.accounts`.
- **Retensi:** karantina dihapus setelah 30 hari (minimisasi PII); bronze mengikuti lifecycle S3 (default 10 tahun).
- **Reset state (jarang):** menghapus checkpoint `fraud_scoring` mereset profil semua akun. Lakukan hanya bersama backfill dari bronze.

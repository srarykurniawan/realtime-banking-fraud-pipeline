-- =====================================================================
-- Defence in depth: Snowflake-native alert in case BOTH the streaming job
-- and Airflow are down (Airflow health DAG is the primary detector).
-- =====================================================================
USE ROLE ACCOUNTADMIN;
CREATE NOTIFICATION INTEGRATION IF NOT EXISTS FRAUD_EMAIL_INT
  TYPE = EMAIL ENABLED = TRUE ALLOWED_RECIPIENTS = ('fraud-oncall@bank.co.id');
GRANT USAGE ON INTEGRATION FRAUD_EMAIL_INT TO ROLE FRAUD_ADMIN;
GRANT EXECUTE ALERT ON ACCOUNT TO ROLE FRAUD_ADMIN;

USE ROLE FRAUD_ADMIN;
USE SCHEMA FRAUD_DB.OPS;

CREATE OR REPLACE ALERT ALERT_STREAM_STALE
  WAREHOUSE = FRAUD_TRANSFORM_WH
  SCHEDULE = '5 MINUTE'
  IF (EXISTS (
      SELECT 1 FROM FRAUD_DB.OPS.PIPELINE_HEARTBEAT
      WHERE PIPELINE = 'fraud_scoring' AND LAST_BATCH_TS < DATEADD('minute', -10, SYSDATE())
  ))
  THEN CALL SYSTEM$SEND_EMAIL(
      'FRAUD_EMAIL_INT', 'fraud-oncall@bank.co.id',
      '[P1] Fraud scoring pipeline stale > 10 minutes',
      'Tidak ada micro-batch baru dari fraud_scoring selama >10 menit. Cek runbook docs/runbook.md#pipeline-stale');

ALTER ALERT ALERT_STREAM_STALE RESUME;

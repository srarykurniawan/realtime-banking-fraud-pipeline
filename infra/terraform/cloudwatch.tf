# =====================================================================
# Monitoring: custom metrics emitted by the Spark job (namespace FraudPlatform,
# see src/fraud_platform/monitoring/metrics.py) -> alarms -> SNS -> email/PagerDuty.
# =====================================================================
resource "aws_sns_topic" "alerts" {
  name              = "${local.name}-pipeline-alerts"
  kms_master_key_id = aws_kms_key.fraud.id
}

resource "aws_sns_topic_subscription" "email" {
  for_each  = toset(var.alert_emails)
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = each.value
}

resource "aws_cloudwatch_log_group" "stream" {
  name              = "/fraud/${var.environment}/stream"
  retention_in_days = 90
  kms_key_id        = aws_kms_key.fraud.arn
}

locals {
  scoring_dim = { Pipeline = "fraud_scoring" }
}

# 1. Pipeline down / stuck: no committed batch for 5 minutes (missing data = breaching)
resource "aws_cloudwatch_metric_alarm" "pipeline_down" {
  alarm_name          = "${local.name}-pipeline-no-batches"
  alarm_description   = "P1: fraud_scoring committed no micro-batch for 5 minutes. Runbook: docs/runbook.md#pipeline-stale"
  namespace           = "FraudPlatform"
  metric_name         = "BatchCompleted"
  dimensions          = local.scoring_dim
  statistic           = "Sum"
  period              = 60
  evaluation_periods  = 5
  threshold           = 1
  comparison_operator = "LessThanThreshold"
  treat_missing_data  = "breaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
  ok_actions          = [aws_sns_topic.alerts.arn]
}

# 2. Data freshness: newest event in the batch is too old (upstream delay or backlog)
resource "aws_cloudwatch_metric_alarm" "event_lag" {
  alarm_name          = "${local.name}-event-lag"
  alarm_description   = "P2: transactions are scored > ${var.max_event_lag_seconds}s after they happen"
  namespace           = "FraudPlatform"
  metric_name         = "EventLagSeconds"
  dimensions          = local.scoring_dim
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 3
  threshold           = var.max_event_lag_seconds
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
}

# 3. Incomplete / broken input: quarantine ratio across both pipelines (metric math)
resource "aws_cloudwatch_metric_alarm" "quarantine_ratio" {
  alarm_name          = "${local.name}-quarantine-ratio"
  alarm_description   = "P2: > ${var.max_quarantine_ratio * 100}% of incoming records fail data-quality checks"
  evaluation_periods  = 3
  threshold           = var.max_quarantine_ratio
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]

  metric_query {
    id          = "ratio"
    expression  = "IF((good + bad) > 0, bad / (good + bad), 0)"
    label       = "Quarantine ratio"
    return_data = true
  }
  metric_query {
    id = "bad"
    metric {
      namespace   = "FraudPlatform"
      metric_name = "QuarantinedRows"
      dimensions  = { Pipeline = "quarantine" }
      stat        = "Sum"
      period      = 300
    }
  }
  metric_query {
    id = "good"
    metric {
      namespace   = "FraudPlatform"
      metric_name = "InputRows"
      dimensions  = local.scoring_dim
      stat        = "Sum"
      period      = 300
    }
  }
}

# 4. Silent source: input volume collapses vs. its own recent behaviour (anomaly band)
resource "aws_cloudwatch_metric_alarm" "volume_anomaly" {
  alarm_name          = "${local.name}-input-volume-anomaly"
  alarm_description   = "P2: incoming transaction volume is far below the expected band (source outage?)"
  evaluation_periods  = 3
  comparison_operator = "LessThanLowerThreshold"
  threshold_metric_id = "band"
  treat_missing_data  = "breaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]

  metric_query {
    id          = "band"
    expression  = "ANOMALY_DETECTION_BAND(m1, 3)"
    return_data = true
  }
  metric_query {
    id          = "m1"
    return_data = true
    metric {
      namespace   = "FraudPlatform"
      metric_name = "InputRows"
      dimensions  = local.scoring_dim
      stat        = "Sum"
      period      = 300
    }
  }
}

# 5. Business alarm: spike of HOLD decisions (possible coordinated attack)
resource "aws_cloudwatch_metric_alarm" "hold_spike" {
  alarm_name          = "${local.name}-hold-decisions-spike"
  alarm_description   = "Fraud: unusually many HOLD decisions in 10 minutes - possible coordinated attack"
  namespace           = "FraudPlatform"
  metric_name         = "HoldDecisions"
  dimensions          = local.scoring_dim
  statistic           = "Sum"
  period              = 600
  evaluation_periods  = 1
  threshold           = 50
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
}

# 6. Spark query crashed (listener emits QueryFailed)
resource "aws_cloudwatch_log_metric_filter" "query_failed" {
  name           = "${local.name}-query-failed"
  log_group_name = aws_cloudwatch_log_group.stream.name
  pattern        = "\"terminated with error\""
  metric_transformation {
    name      = "StreamingQueryErrors"
    namespace = "FraudPlatform"
    value     = "1"
  }
}

resource "aws_cloudwatch_metric_alarm" "query_failed" {
  alarm_name          = "${local.name}-streaming-query-error"
  alarm_description   = "P1: a Spark streaming query terminated with an error"
  namespace           = "FraudPlatform"
  metric_name         = "StreamingQueryErrors"
  statistic           = "Sum"
  period              = 60
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = [aws_sns_topic.alerts.arn]
}

resource "aws_cloudwatch_dashboard" "fraud" {
  dashboard_name = "${local.name}-pipeline"
  dashboard_body = jsonencode({
    widgets = [
      { type = "metric", x = 0, y = 0, width = 12, height = 6, properties = {
        title = "Throughput (rows / batch)", region = var.region, stat = "Sum", period = 60,
        metrics = [
          ["FraudPlatform", "InputRows", "Pipeline", "fraud_scoring"],
          ["FraudPlatform", "QuarantinedRows", "Pipeline", "quarantine"],
          ["FraudPlatform", "DuplicateRows", "Pipeline", "fraud_scoring"],
      ] } },
      { type = "metric", x = 12, y = 0, width = 12, height = 6, properties = {
        title = "Freshness (seconds)", region = var.region, stat = "Maximum", period = 60,
        metrics = [
          ["FraudPlatform", "EventLagSeconds", "Pipeline", "fraud_scoring"],
          ["FraudPlatform", "BatchDurationSeconds", "Pipeline", "fraud_scoring"],
      ] } },
      { type = "metric", x = 0, y = 6, width = 12, height = 6, properties = {
        title = "Fraud decisions", region = var.region, stat = "Sum", period = 300,
        metrics = [
          ["FraudPlatform", "FraudAlerts", "Pipeline", "fraud_scoring"],
          ["FraudPlatform", "HoldDecisions", "Pipeline", "fraud_scoring"],
      ] } },
      { type = "alarm", x = 12, y = 6, width = 12, height = 6, properties = {
        title = "Alarms",
        alarms = [
          aws_cloudwatch_metric_alarm.pipeline_down.arn, aws_cloudwatch_metric_alarm.event_lag.arn,
          aws_cloudwatch_metric_alarm.quarantine_ratio.arn, aws_cloudwatch_metric_alarm.volume_anomaly.arn,
          aws_cloudwatch_metric_alarm.hold_spike.arn, aws_cloudwatch_metric_alarm.query_failed.arn,
      ] } },
    ]
  })
}

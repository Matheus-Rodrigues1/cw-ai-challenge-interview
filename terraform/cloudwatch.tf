# -----------------------------------------------------------------------------
# CloudWatch Log Groups
# -----------------------------------------------------------------------------

resource "aws_cloudwatch_log_group" "api" {
  name              = "/ecs/${local.name_prefix}/api"
  retention_in_days = 30
}

resource "aws_cloudwatch_log_group" "anomaly_worker" {
  name              = "/ecs/${local.name_prefix}/anomaly-worker"
  retention_in_days = 30
}

resource "aws_cloudwatch_log_group" "monitoring_worker" {
  name              = "/ecs/${local.name_prefix}/monitoring-worker"
  retention_in_days = 30
}

resource "aws_cloudwatch_log_group" "metabase" {
  name              = "/ecs/${local.name_prefix}/metabase"
  retention_in_days = 30
}

resource "aws_cloudwatch_log_group" "db_seed" {
  name              = "/ecs/${local.name_prefix}/db-seed"
  retention_in_days = 7
}

# -----------------------------------------------------------------------------
# Secrets Manager — Database Password
# -----------------------------------------------------------------------------

resource "aws_secretsmanager_secret" "db_password" {
  name                    = "${local.name_prefix}/db-password"
  description             = "RDS PostgreSQL master password"
  recovery_window_in_days = var.environment == "prod" ? 30 : 0
}

resource "aws_secretsmanager_secret_version" "db_password" {
  secret_id     = aws_secretsmanager_secret.db_password.id
  secret_string = var.db_password
}

# -----------------------------------------------------------------------------
# Secrets Manager — SMTP Credentials
# -----------------------------------------------------------------------------

resource "aws_secretsmanager_secret" "smtp_credentials" {
  name                    = "${local.name_prefix}/smtp-credentials"
  description             = "SMTP credentials for email alerts"
  recovery_window_in_days = var.environment == "prod" ? 30 : 0
}

resource "aws_secretsmanager_secret_version" "smtp_credentials" {
  secret_id = aws_secretsmanager_secret.smtp_credentials.id

  secret_string = jsonencode({
    SMTP_HOST = var.smtp_host
    SMTP_PORT = var.smtp_port
    SMTP_SSL  = var.smtp_ssl
    SMTP_USER = var.smtp_user
    SMTP_PASS = var.smtp_pass
  })
}

# -----------------------------------------------------------------------------
# Secrets Manager — Slack Webhook
# -----------------------------------------------------------------------------

resource "aws_secretsmanager_secret" "slack_webhook" {
  name                    = "${local.name_prefix}/slack-webhook"
  description             = "Slack webhook URL for alert notifications"
  recovery_window_in_days = var.environment == "prod" ? 30 : 0
}

resource "aws_secretsmanager_secret_version" "slack_webhook" {
  secret_id     = aws_secretsmanager_secret.slack_webhook.id
  secret_string = var.slack_webhook_url
}

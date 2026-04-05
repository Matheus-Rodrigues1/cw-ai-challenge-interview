# -----------------------------------------------------------------------------
# General
# -----------------------------------------------------------------------------

variable "aws_region" {
  description = "AWS region to deploy resources"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Project name used as prefix for all resources"
  type        = string
  default     = "cloudwalk-monitoring"
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "prod"
}

# -----------------------------------------------------------------------------
# Networking
# -----------------------------------------------------------------------------

variable "vpc_cidr" {
  description = "CIDR block for the VPC"
  type        = string
  default     = "10.0.0.0/16"
}

# -----------------------------------------------------------------------------
# Database (RDS)
# -----------------------------------------------------------------------------

variable "db_name" {
  description = "Name of the primary PostgreSQL database"
  type        = string
  default     = "cloudwalk_transactions"
}

variable "db_username" {
  description = "Master username for the RDS instance"
  type        = string
  default     = "admin"
}

variable "db_password" {
  description = "Master password for the RDS instance"
  type        = string
  sensitive   = true
}

variable "db_instance_class" {
  description = "RDS instance class"
  type        = string
  default     = "db.t3.micro"
}

variable "db_allocated_storage" {
  description = "Allocated storage in GB for RDS"
  type        = number
  default     = 20
}

variable "db_multi_az" {
  description = "Enable Multi-AZ deployment for RDS"
  type        = bool
  default     = false
}

# -----------------------------------------------------------------------------
# ECS
# -----------------------------------------------------------------------------

variable "api_cpu" {
  description = "CPU units for the monitoring API task (1 vCPU = 1024)"
  type        = number
  default     = 512
}

variable "api_memory" {
  description = "Memory (MiB) for the monitoring API task"
  type        = number
  default     = 1024
}

variable "api_desired_count" {
  description = "Desired number of monitoring API tasks"
  type        = number
  default     = 2
}

variable "worker_cpu" {
  description = "CPU units for worker tasks"
  type        = number
  default     = 512
}

variable "worker_memory" {
  description = "Memory (MiB) for worker tasks"
  type        = number
  default     = 1024
}

variable "metabase_cpu" {
  description = "CPU units for Metabase task"
  type        = number
  default     = 1024
}

variable "metabase_memory" {
  description = "Memory (MiB) for Metabase task"
  type        = number
  default     = 2048
}

# -----------------------------------------------------------------------------
# Alerting / Notification (optional)
# -----------------------------------------------------------------------------

variable "smtp_host" {
  description = "SMTP host for email alerts"
  type        = string
  default     = ""
}

variable "smtp_port" {
  description = "SMTP port"
  type        = string
  default     = "587"
}

variable "smtp_ssl" {
  description = "Use implicit SSL for SMTP (true for port 465)"
  type        = string
  default     = ""
}

variable "smtp_user" {
  description = "SMTP username"
  type        = string
  default     = ""
}

variable "smtp_pass" {
  description = "SMTP password"
  type        = string
  default     = ""
  sensitive   = true
}

variable "alert_email_to" {
  description = "Email address to receive alerts"
  type        = string
  default     = ""
}

variable "alert_email_from" {
  description = "Email address for alert sender"
  type        = string
  default     = "monitoring@cloudwalk.io"
}

variable "slack_webhook_url" {
  description = "Slack webhook URL for notifications"
  type        = string
  default     = ""
  sensitive   = true
}

# -----------------------------------------------------------------------------
# Worker tuning
# -----------------------------------------------------------------------------

variable "anomaly_run_interval" {
  description = "Anomaly worker run interval in seconds"
  type        = number
  default     = 60
}

variable "monitoring_poll_interval" {
  description = "Monitoring worker poll interval in seconds"
  type        = number
  default     = 30
}

variable "min_score_to_notify" {
  description = "Minimum anomaly score to trigger notifications"
  type        = string
  default     = "0.5"
}

variable "critical_channels" {
  description = "Notification channels for critical alerts"
  type        = string
  default     = "console,email"
}

variable "warning_channels" {
  description = "Notification channels for warning alerts"
  type        = string
  default     = "console,email"
}

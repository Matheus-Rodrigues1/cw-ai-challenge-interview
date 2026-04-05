# -----------------------------------------------------------------------------
# ECS Cluster
# -----------------------------------------------------------------------------

resource "aws_ecs_cluster" "main" {
  name = "${local.name_prefix}-cluster"

  setting {
    name  = "containerInsights"
    value = "enabled"
  }
}

resource "aws_ecs_cluster_capacity_providers" "main" {
  cluster_name       = aws_ecs_cluster.main.name
  capacity_providers = ["FARGATE", "FARGATE_SPOT"]

  default_capacity_provider_strategy {
    capacity_provider = "FARGATE"
    weight            = 1
    base              = 1
  }
}

# =============================================================================
# TASK DEFINITIONS
# =============================================================================

# -----------------------------------------------------------------------------
# Monitoring API
# -----------------------------------------------------------------------------

resource "aws_ecs_task_definition" "api" {
  family                   = "${local.name_prefix}-api"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.api_cpu
  memory                   = var.api_memory
  execution_role_arn       = aws_iam_role.ecs_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn

  container_definitions = jsonencode([{
    name      = "monitoring-api"
    image     = local.api_image
    essential = true

    portMappings = [{
      containerPort = 8000
      protocol      = "tcp"
    }]

    environment = [
      { name = "DB_HOST", value = local.db_host },
      { name = "DB_PORT", value = local.db_port },
      { name = "DB_NAME", value = local.db_name },
      { name = "DB_USER", value = local.db_user },
      { name = "ALERT_EMAIL_TO", value = var.alert_email_to },
      { name = "ALERT_EMAIL_FROM", value = var.alert_email_from },
      { name = "ALERT_ON_EVALUATE", value = "false" },
    ]

    secrets = [
      {
        name      = "DB_PASSWORD"
        valueFrom = aws_secretsmanager_secret.db_password.arn
      },
      {
        name      = "SMTP_HOST"
        valueFrom = "${aws_secretsmanager_secret.smtp_credentials.arn}:SMTP_HOST::"
      },
      {
        name      = "SMTP_PORT"
        valueFrom = "${aws_secretsmanager_secret.smtp_credentials.arn}:SMTP_PORT::"
      },
      {
        name      = "SMTP_SSL"
        valueFrom = "${aws_secretsmanager_secret.smtp_credentials.arn}:SMTP_SSL::"
      },
      {
        name      = "SMTP_USER"
        valueFrom = "${aws_secretsmanager_secret.smtp_credentials.arn}:SMTP_USER::"
      },
      {
        name      = "SMTP_PASS"
        valueFrom = "${aws_secretsmanager_secret.smtp_credentials.arn}:SMTP_PASS::"
      },
      {
        name      = "SLACK_WEBHOOK_URL"
        valueFrom = aws_secretsmanager_secret.slack_webhook.arn
      },
    ]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.api.name
        "awslogs-region"        = var.aws_region
        "awslogs-stream-prefix" = "api"
      }
    }
  }])
}

# -----------------------------------------------------------------------------
# Anomaly Worker
# -----------------------------------------------------------------------------

resource "aws_ecs_task_definition" "anomaly_worker" {
  family                   = "${local.name_prefix}-anomaly-worker"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.worker_cpu
  memory                   = var.worker_memory
  execution_role_arn       = aws_iam_role.ecs_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn

  container_definitions = jsonencode([{
    name      = "anomaly-worker"
    image     = local.worker_image
    essential = true

    environment = [
      { name = "DB_HOST", value = local.db_host },
      { name = "DB_PORT", value = local.db_port },
      { name = "DB_NAME", value = local.db_name },
      { name = "DB_USER", value = local.db_user },
      { name = "RUN_INTERVAL_SECONDS", value = tostring(var.anomaly_run_interval) },
    ]

    secrets = [{
      name      = "DB_PASSWORD"
      valueFrom = aws_secretsmanager_secret.db_password.arn
    }]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.anomaly_worker.name
        "awslogs-region"        = var.aws_region
        "awslogs-stream-prefix" = "anomaly"
      }
    }
  }])
}

# -----------------------------------------------------------------------------
# Monitoring Worker
# -----------------------------------------------------------------------------

resource "aws_ecs_task_definition" "monitoring_worker" {
  family                   = "${local.name_prefix}-monitoring-worker"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.worker_cpu
  memory                   = var.worker_memory
  execution_role_arn       = aws_iam_role.ecs_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn

  container_definitions = jsonencode([{
    name      = "monitoring-worker"
    image     = local.worker_image
    essential = true

    command = ["python", "src/workers/monitoring_worker.py"]

    environment = [
      { name = "API_BASE_URL", value = local.api_internal_url },
      { name = "POLL_INTERVAL_SECONDS", value = tostring(var.monitoring_poll_interval) },
      { name = "MIN_SCORE_TO_NOTIFY", value = var.min_score_to_notify },
      { name = "CRITICAL_CHANNELS", value = var.critical_channels },
      { name = "WARNING_CHANNELS", value = var.warning_channels },
    ]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.monitoring_worker.name
        "awslogs-region"        = var.aws_region
        "awslogs-stream-prefix" = "monitoring"
      }
    }
  }])
}

# -----------------------------------------------------------------------------
# Metabase
# -----------------------------------------------------------------------------

resource "aws_ecs_task_definition" "metabase" {
  family                   = "${local.name_prefix}-metabase"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.metabase_cpu
  memory                   = var.metabase_memory
  execution_role_arn       = aws_iam_role.ecs_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn

  container_definitions = jsonencode([{
    name      = "metabase"
    image     = local.metabase_image
    essential = true

    portMappings = [{
      containerPort = 3000
      protocol      = "tcp"
    }]

    environment = [
      { name = "MB_DB_TYPE", value = "postgres" },
      { name = "MB_DB_DBNAME", value = "metabase_appdb" },
      { name = "MB_DB_PORT", value = local.db_port },
      { name = "MB_DB_USER", value = local.db_user },
      { name = "MB_DB_HOST", value = local.db_host },
    ]

    secrets = [{
      name      = "MB_DB_PASS"
      valueFrom = aws_secretsmanager_secret.db_password.arn
    }]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.metabase.name
        "awslogs-region"        = var.aws_region
        "awslogs-stream-prefix" = "metabase"
      }
    }
  }])
}

# -----------------------------------------------------------------------------
# Database Seed Task (run once to initialize schema + load CSVs)
# The worker image already has psycopg2; use a postgres image instead.
# Run manually: aws ecs run-task --cluster ... --task-definition ...
# -----------------------------------------------------------------------------

resource "aws_ecs_task_definition" "db_seed" {
  family                   = "${local.name_prefix}-db-seed"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 256
  memory                   = 512
  execution_role_arn       = aws_iam_role.ecs_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn

  container_definitions = jsonencode([{
    name      = "db-seed"
    image     = "postgres:15"
    essential = true

    command = [
      "bash", "-c",
      join(" && ", [
        "PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -U $DB_USER -d postgres -c \"SELECT 'CREATE DATABASE metabase_appdb' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'metabase_appdb')\\gexec\"",
        "echo 'Database metabase_appdb ensured.'",
        "echo 'Run sql/init.sql manually against RDS to load schema and CSV data.'",
      ])
    ]

    environment = [
      { name = "DB_HOST", value = local.db_host },
      { name = "DB_USER", value = local.db_user },
    ]

    secrets = [{
      name      = "DB_PASSWORD"
      valueFrom = aws_secretsmanager_secret.db_password.arn
    }]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.db_seed.name
        "awslogs-region"        = var.aws_region
        "awslogs-stream-prefix" = "seed"
      }
    }
  }])
}

# =============================================================================
# ECS SERVICES
# =============================================================================

# -----------------------------------------------------------------------------
# Monitoring API Service
# -----------------------------------------------------------------------------

resource "aws_ecs_service" "api" {
  name            = "${local.name_prefix}-api"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.api.arn
  desired_count   = var.api_desired_count
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = aws_subnet.private[*].id
    security_groups  = [aws_security_group.ecs_tasks.id]
    assign_public_ip = false
  }

  load_balancer {
    target_group_arn = aws_lb_target_group.api.arn
    container_name   = "monitoring-api"
    container_port   = 8000
  }

  service_registries {
    registry_arn = aws_service_discovery_service.api.arn
  }

  depends_on = [aws_lb_listener.api]
}

# -----------------------------------------------------------------------------
# Anomaly Worker Service
# -----------------------------------------------------------------------------

resource "aws_ecs_service" "anomaly_worker" {
  name            = "${local.name_prefix}-anomaly-worker"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.anomaly_worker.arn
  desired_count   = 1
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = aws_subnet.private[*].id
    security_groups  = [aws_security_group.ecs_tasks.id]
    assign_public_ip = false
  }
}

# -----------------------------------------------------------------------------
# Monitoring Worker Service
# -----------------------------------------------------------------------------

resource "aws_ecs_service" "monitoring_worker" {
  name            = "${local.name_prefix}-monitoring-worker"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.monitoring_worker.arn
  desired_count   = 1
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = aws_subnet.private[*].id
    security_groups  = [aws_security_group.ecs_tasks.id]
    assign_public_ip = false
  }

  depends_on = [aws_ecs_service.api, aws_ecs_service.anomaly_worker]
}

# -----------------------------------------------------------------------------
# Metabase Service
# -----------------------------------------------------------------------------

resource "aws_ecs_service" "metabase" {
  name            = "${local.name_prefix}-metabase"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.metabase.arn
  desired_count   = 1
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = aws_subnet.private[*].id
    security_groups  = [aws_security_group.ecs_tasks.id]
    assign_public_ip = false
  }

  load_balancer {
    target_group_arn = aws_lb_target_group.metabase.arn
    container_name   = "metabase"
    container_port   = 3000
  }

  depends_on = [aws_lb_listener.metabase]
}

# =============================================================================
# AUTO SCALING — Monitoring API
# =============================================================================

resource "aws_appautoscaling_target" "api" {
  max_capacity       = 6
  min_capacity       = var.api_desired_count
  resource_id        = "service/${aws_ecs_cluster.main.name}/${aws_ecs_service.api.name}"
  scalable_dimension = "ecs:service:DesiredCount"
  service_namespace  = "ecs"
}

resource "aws_appautoscaling_policy" "api_cpu" {
  name               = "${local.name_prefix}-api-cpu-scaling"
  policy_type        = "TargetTrackingScaling"
  resource_id        = aws_appautoscaling_target.api.resource_id
  scalable_dimension = aws_appautoscaling_target.api.scalable_dimension
  service_namespace  = aws_appautoscaling_target.api.service_namespace

  target_tracking_scaling_policy_configuration {
    predefined_metric_specification {
      predefined_metric_type = "ECSServiceAverageCPUUtilization"
    }
    target_value       = 70.0
    scale_in_cooldown  = 300
    scale_out_cooldown = 60
  }
}

resource "aws_appautoscaling_policy" "api_memory" {
  name               = "${local.name_prefix}-api-memory-scaling"
  policy_type        = "TargetTrackingScaling"
  resource_id        = aws_appautoscaling_target.api.resource_id
  scalable_dimension = aws_appautoscaling_target.api.scalable_dimension
  service_namespace  = aws_appautoscaling_target.api.service_namespace

  target_tracking_scaling_policy_configuration {
    predefined_metric_specification {
      predefined_metric_type = "ECSServiceAverageMemoryUtilization"
    }
    target_value       = 80.0
    scale_in_cooldown  = 300
    scale_out_cooldown = 60
  }
}

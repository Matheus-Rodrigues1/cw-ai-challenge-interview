# -----------------------------------------------------------------------------
# Networking
# -----------------------------------------------------------------------------

output "vpc_id" {
  description = "ID of the VPC"
  value       = aws_vpc.main.id
}

# -----------------------------------------------------------------------------
# Load Balancer
# -----------------------------------------------------------------------------

output "alb_dns_name" {
  description = "DNS name of the Application Load Balancer"
  value       = aws_lb.main.dns_name
}

output "api_url" {
  description = "Monitoring API URL"
  value       = "http://${aws_lb.main.dns_name}:8000/api/v1"
}

output "metabase_url" {
  description = "Metabase dashboard URL"
  value       = "http://${aws_lb.main.dns_name}"
}

# -----------------------------------------------------------------------------
# Database
# -----------------------------------------------------------------------------

output "rds_endpoint" {
  description = "RDS PostgreSQL endpoint"
  value       = aws_db_instance.postgres.endpoint
}

output "rds_address" {
  description = "RDS PostgreSQL hostname"
  value       = aws_db_instance.postgres.address
}

# -----------------------------------------------------------------------------
# ECR Repositories
# -----------------------------------------------------------------------------

output "ecr_api_url" {
  description = "ECR repository URL for the API image"
  value       = aws_ecr_repository.api.repository_url
}

output "ecr_worker_url" {
  description = "ECR repository URL for the Worker image"
  value       = aws_ecr_repository.worker.repository_url
}

# -----------------------------------------------------------------------------
# ECS
# -----------------------------------------------------------------------------

output "ecs_cluster_name" {
  description = "Name of the ECS cluster"
  value       = aws_ecs_cluster.main.name
}

output "db_seed_task_definition" {
  description = "ARN of the DB seed task definition (run once after first deploy)"
  value       = aws_ecs_task_definition.db_seed.arn
}

# -----------------------------------------------------------------------------
# Convenience — deploy commands
# -----------------------------------------------------------------------------

output "ecr_login_command" {
  description = "Command to authenticate Docker with ECR"
  value       = "aws ecr get-login-password --region ${var.aws_region} | docker login --username AWS --password-stdin ${data.aws_caller_identity.current.account_id}.dkr.ecr.${var.aws_region}.amazonaws.com"
}

output "push_api_image_commands" {
  description = "Commands to build and push the API image"
  value       = <<-EOT
    docker build -f Dockerfile.api -t ${aws_ecr_repository.api.repository_url}:latest .
    docker push ${aws_ecr_repository.api.repository_url}:latest
  EOT
}

output "push_worker_image_commands" {
  description = "Commands to build and push the Worker image"
  value       = <<-EOT
    docker build -f Dockerfile.worker -t ${aws_ecr_repository.worker.repository_url}:latest .
    docker push ${aws_ecr_repository.worker.repository_url}:latest
  EOT
}

output "run_db_seed_command" {
  description = "Command to run the DB seed task (creates metabase_appdb)"
  value       = <<-EOT
    aws ecs run-task \
      --cluster ${aws_ecs_cluster.main.name} \
      --task-definition ${aws_ecs_task_definition.db_seed.family} \
      --launch-type FARGATE \
      --network-configuration "awsvpcConfiguration={subnets=[${join(",", aws_subnet.private[*].id)}],securityGroups=[${aws_security_group.ecs_tasks.id}]}"
  EOT
}

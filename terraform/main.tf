terraform {
  required_version = ">= 1.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # Uncomment and configure for remote state
  # backend "s3" {
  #   bucket         = "your-terraform-state-bucket"
  #   key            = "cloudwalk-monitoring/terraform.tfstate"
  #   region         = "us-east-1"
  #   dynamodb_table = "terraform-locks"
  #   encrypt        = true
  # }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = var.project_name
      Environment = var.environment
      ManagedBy   = "terraform"
    }
  }
}

data "aws_availability_zones" "available" {
  state = "available"
}

data "aws_caller_identity" "current" {}

locals {
  name_prefix = "${var.project_name}-${var.environment}"
  azs         = slice(data.aws_availability_zones.available.names, 0, 2)

  db_host = aws_db_instance.postgres.address
  db_port = tostring(aws_db_instance.postgres.port)
  db_name = var.db_name
  db_user = var.db_username

  api_image            = "${aws_ecr_repository.api.repository_url}:latest"
  worker_image         = "${aws_ecr_repository.worker.repository_url}:latest"
  metabase_image       = "metabase/metabase:latest"
  api_internal_url     = "http://${aws_service_discovery_service.api.name}.${aws_service_discovery_private_dns_namespace.main.name}:8000"
}

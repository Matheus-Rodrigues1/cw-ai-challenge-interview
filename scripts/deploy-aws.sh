#!/usr/bin/env bash
# =============================================================================
# CloudWalk Monitoring — AWS Deployment Script
#
# Usage:
#   chmod +x scripts/deploy-aws.sh
#   ./scripts/deploy-aws.sh          # full deploy (infra + images + seed)
#   ./scripts/deploy-aws.sh images   # rebuild and push images only
#   ./scripts/deploy-aws.sh infra    # terraform apply only
#   ./scripts/deploy-aws.sh seed     # run DB seed task only
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
TF_DIR="$PROJECT_DIR/terraform"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log()  { echo -e "${GREEN}[deploy]${NC} $*"; }
warn() { echo -e "${YELLOW}[warn]${NC} $*"; }
err()  { echo -e "${RED}[error]${NC} $*" >&2; exit 1; }

# ---------------------------------------------------------------------------
# Pre-flight checks
# ---------------------------------------------------------------------------

command -v aws       >/dev/null 2>&1 || err "aws CLI not found. Install: https://aws.amazon.com/cli/"
command -v terraform >/dev/null 2>&1 || err "terraform not found. Install: https://developer.hashicorp.com/terraform/install"
command -v docker    >/dev/null 2>&1 || err "docker not found."

AWS_REGION=$(cd "$TF_DIR" && terraform output -raw aws_region 2>/dev/null || grep -oP 'aws_region\s*=\s*"\K[^"]+' "$TF_DIR/terraform.tfvars" 2>/dev/null || echo "us-east-1")
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

# ---------------------------------------------------------------------------
# Functions
# ---------------------------------------------------------------------------

run_infra() {
  log "Initializing Terraform..."
  cd "$TF_DIR"
  terraform init -upgrade

  log "Planning infrastructure changes..."
  terraform plan -out=tfplan

  log "Applying infrastructure..."
  terraform apply tfplan
  rm -f tfplan

  log "Infrastructure deployed successfully."
}

push_images() {
  log "Authenticating Docker with ECR..."
  aws ecr get-login-password --region "$AWS_REGION" | \
    docker login --username AWS --password-stdin "$AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com"

  cd "$TF_DIR"
  ECR_API=$(terraform output -raw ecr_api_url)
  ECR_WORKER=$(terraform output -raw ecr_worker_url)

  cd "$PROJECT_DIR"

  log "Building API image..."
  docker build -f Dockerfile.api -t "$ECR_API:latest" .
  docker push "$ECR_API:latest"

  log "Building Worker image..."
  docker build -f Dockerfile.worker -t "$ECR_WORKER:latest" .
  docker push "$ECR_WORKER:latest"

  log "Images pushed to ECR."

  log "Forcing ECS service redeployments..."
  CLUSTER=$(cd "$TF_DIR" && terraform output -raw ecs_cluster_name)
  for svc in api anomaly-worker monitoring-worker; do
    aws ecs update-service \
      --cluster "$CLUSTER" \
      --service "cloudwalk-monitoring-prod-$svc" \
      --force-new-deployment \
      --region "$AWS_REGION" \
      --no-cli-pager > /dev/null
    log "  ↳ Redeployed $svc"
  done
}

run_seed() {
  cd "$TF_DIR"
  CLUSTER=$(terraform output -raw ecs_cluster_name)
  TASK_DEF=$(terraform output -raw db_seed_task_definition)
  SUBNETS=$(terraform output -json | python3 -c "
import sys, json
data = json.load(sys.stdin)
cmd = data.get('run_db_seed_command', {}).get('value', '')
print(cmd)
" 2>/dev/null || true)

  log "Running DB seed task..."
  eval "$(terraform output -raw run_db_seed_command)"
  log "Seed task started. Check CloudWatch logs for status."
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

ACTION="${1:-all}"

case "$ACTION" in
  infra)
    run_infra
    ;;
  images)
    push_images
    ;;
  seed)
    run_seed
    ;;
  all)
    run_infra
    push_images
    run_seed
    log "Full deployment complete!"
    echo ""
    cd "$TF_DIR"
    echo -e "  API URL:      ${GREEN}$(terraform output -raw api_url)${NC}"
    echo -e "  Metabase URL: ${GREEN}$(terraform output -raw metabase_url)${NC}"
    echo -e "  RDS Endpoint: $(terraform output -raw rds_endpoint)"
    ;;
  *)
    echo "Usage: $0 {all|infra|images|seed}"
    exit 1
    ;;
esac

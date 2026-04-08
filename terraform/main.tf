# ── Provider ──────────────────────────────────────────────────────────────────
terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}


# ── S3 Bucket (existing — not managed by Terraform) ───────────────────────────
# The bucket already exists and contains data being uploaded.
# We reference it here only to pass its name to other resources.
# terraform destroy will NOT touch this bucket or its contents.
data "aws_s3_bucket" "ndvi" {
  bucket = var.s3_bucket_name
}


# ── ECR Repository ────────────────────────────────────────────────────────────
# Stores the Docker image that Batch pulls when starting a job
resource "aws_ecr_repository" "ndvi" {
  name                 = "${var.project}-job"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = false
  }
}


# ── IAM: Batch service role ───────────────────────────────────────────────────
# Allows AWS Batch itself to manage EC2 instances on your behalf
resource "aws_iam_role" "batch_service" {
  name = "${var.project}-batch-service-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "batch.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "batch_service" {
  role       = aws_iam_role.batch_service.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSBatchServiceRole"
}


# ── IAM: EC2 instance role ────────────────────────────────────────────────────
# Permissions the container itself has when running (S3 read/write, CloudWatch logs)
resource "aws_iam_role" "batch_instance" {
  name = "${var.project}-batch-instance-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "batch_instance_ecs" {
  role       = aws_iam_role.batch_instance.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonEC2ContainerServiceforEC2Role"
}

resource "aws_iam_role_policy_attachment" "batch_instance_s3" {
  role       = aws_iam_role.batch_instance.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}

resource "aws_iam_role_policy_attachment" "batch_instance_cloudwatch" {
  role       = aws_iam_role.batch_instance.name
  policy_arn = "arn:aws:iam::aws:policy/CloudWatchLogsFullAccess"
}

resource "aws_iam_instance_profile" "batch_instance" {
  name = "${var.project}-batch-instance-profile"
  role = aws_iam_role.batch_instance.name
}


# ── IAM: Job execution role ───────────────────────────────────────────────────
# Used by ECS to pull the image from ECR and send logs to CloudWatch
resource "aws_iam_role" "job_execution" {
  name = "${var.project}-job-execution-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ecs-tasks.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "job_execution" {
  role       = aws_iam_role.job_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role_policy_attachment" "job_execution_s3" {
  role       = aws_iam_role.job_execution.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}


# ── CloudWatch Log Group ──────────────────────────────────────────────────────
# Captures stdout from each container (everything the scripts print)
resource "aws_cloudwatch_log_group" "ndvi" {
  name              = "/aws/batch/${var.project}"
  retention_in_days = 7
}


# ── Networking: use default VPC ───────────────────────────────────────────────
data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

resource "aws_security_group" "batch" {
  name   = "${var.project}-batch-sg"
  vpc_id = data.aws_vpc.default.id

  # Allow all outbound traffic (needed to reach STAC endpoint and S3)
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}


# ── Batch Compute Environment ─────────────────────────────────────────────────
# EC2 managed pool — scales to 0 when idle (no cost when no jobs running)
resource "aws_batch_compute_environment" "ndvi" {
  compute_environment_name = "${var.project}-compute"
  type                     = "MANAGED"
  service_role             = aws_iam_role.batch_service.arn

  compute_resources {
    type               = "EC2"
    instance_role      = aws_iam_instance_profile.batch_instance.arn
    instance_type      = [var.instance_type]
    min_vcpus          = 0
    max_vcpus          = var.max_vcpus
    subnets            = data.aws_subnets.default.ids
    security_group_ids = [aws_security_group.batch.id]
  }

  depends_on = [aws_iam_role_policy_attachment.batch_service]
}


# ── Batch Job Queue ───────────────────────────────────────────────────────────
resource "aws_batch_job_queue" "ndvi" {
  name     = "${var.project}-queue"
  state    = "ENABLED"
  priority = 1

  compute_environment_order {
    order               = 1
    compute_environment = aws_batch_compute_environment.ndvi.arn
  }
}


# ── Batch Job Definition ──────────────────────────────────────────────────────
# Single definition for both pipelines — SCRIPT_TYPE overridden at submit time
resource "aws_batch_job_definition" "ndvi" {
  name = "${var.project}-job"
  type = "container"

  container_properties = jsonencode({
    image      = "${aws_ecr_repository.ndvi.repository_url}:latest"
    jobRoleArn = aws_iam_role.job_execution.arn

    resourceRequirements = [
      { type = "VCPU",   value = "2" },
      { type = "MEMORY", value = "14336" }  # 14 GB — leaves headroom on r6i.large (16 GB)
    ]

    environment = [
      { name = "S3_BUCKET", value = var.s3_bucket_name }
      # SCRIPT_TYPE, SHP_PATH, YEAR, S3_PREFIX are set per-job at submission time
    ]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.ndvi.name
        "awslogs-region"        = var.aws_region
        "awslogs-stream-prefix" = "job"
      }
    }
  })
}

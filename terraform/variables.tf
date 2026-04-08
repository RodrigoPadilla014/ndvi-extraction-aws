variable "aws_region" {
  description = "AWS region"
  default     = "us-east-1"
}

variable "project" {
  description = "Project name — used as prefix for all resource names"
  default     = "ndvi-extraction"
}

variable "s3_bucket_name" {
  description = "Existing S3 bucket for inputs and outputs (not managed by Terraform)"
  default     = "ndvi-extraction"
}

variable "instance_type" {
  description = "EC2 instance type for Batch compute environment"
  default     = "r6i.large"
}

variable "max_vcpus" {
  description = "Maximum vCPUs across all running Batch jobs"
  default     = 96
}

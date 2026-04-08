# Printed after terraform apply — use these values in the next steps

output "ecr_repository_url" {
  description = "ECR URL — tag and push the Docker image here"
  value       = aws_ecr_repository.ndvi.repository_url
}

output "batch_job_queue" {
  description = "Batch job queue name — used by submit scripts"
  value       = aws_batch_job_queue.ndvi.name
}

output "batch_job_definition" {
  description = "Batch job definition name — used by submit scripts"
  value       = aws_batch_job_definition.ndvi.name
}

output "cloudwatch_log_group" {
  description = "CloudWatch log group — where container logs appear"
  value       = aws_cloudwatch_log_group.ndvi.name
}

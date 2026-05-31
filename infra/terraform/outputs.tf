output "lakehouse_bucket_name" {
  description = "S3 bucket used for Bronze, Silver, Gold, checkpoints, and quarantine data."
  value       = aws_s3_bucket.lakehouse.bucket
}


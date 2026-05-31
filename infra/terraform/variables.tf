variable "aws_region" {
  description = "AWS region for the portfolio deployment."
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Project prefix used for cloud resources."
  type        = string
  default     = "fraudstream-lakehouse"
}

variable "environment" {
  description = "Deployment environment."
  type        = string
  default     = "dev"
}


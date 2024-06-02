variable "project_id" {
  description = "The ID of the GCP project"
  type        = string
}

variable "region" {
  description = "The GCP region"
  type        = string
  default     = "europe-west1"
}

variable "bucket_name" {
  description = "The name of the GCS bucket"
  type        = string
}

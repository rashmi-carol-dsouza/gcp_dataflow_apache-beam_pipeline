provider "google" {
  project = var.project_id
  region  = var.region
}


# Enable necessary APIs

resource "google_project_service" "dataflow" {
  project = var.project_id
  service = "dataflow.googleapis.com"
  disable_on_destroy = false
  disable_dependent_services = false
}

resource "google_project_service" "bigquery" {
  project = var.project_id
  service = "bigquery.googleapis.com"
  disable_on_destroy = false
  disable_dependent_services = false
}

resource "google_project_service" "storage" {
  project = var.project_id
  service = "storage.googleapis.com"
  disable_on_destroy = false
  disable_dependent_services = false
}

# Create a Google Cloud Storage bucket
resource "google_storage_bucket" "dataflow_bucket" {
  name          = var.bucket_name
  location      = var.region
  force_destroy = true
}


# Create a service account for Dataflow
resource "google_service_account" "dataflow_service_account" {
  account_id   = "dataflow-service-account"
  display_name = "Dataflow Service Account"
}

# Set IAM roles for the Dataflow service account
resource "google_project_iam_member" "dataflow_admin" {
  project = var.project_id
  role    = "roles/dataflow.admin"
  member  = "serviceAccount:ml6-coding-challenge-evaluator@recruiting-220608.iam.gserviceaccount.com"
}

resource "google_project_iam_member" "storage_admin" {
  project = var.project_id
  role    = "roles/storage.admin"
  member  = "serviceAccount:ml6-coding-challenge-evaluator@recruiting-220608.iam.gserviceaccount.com"
}

resource "google_project_iam_member" "bigquery_admin" {
  project = var.project_id
  role    = "roles/bigquery.admin"
  member  = "serviceAccount:ml6-coding-challenge-evaluator@recruiting-220608.iam.gserviceaccount.com"
}

# Add the evaluator as project owner
resource "google_project_iam_member" "evaluator_owner" {
  project = var.project_id
  role    = "roles/owner"
  member  = "serviceAccount:ml6-coding-challenge-evaluator@recruiting-220608.iam.gserviceaccount.com"
}

# Output the service account email
output "service_account_email" {
  value = google_service_account.dataflow_service_account.email
}

provider "google" {
  project = var.project_id
  region  = var.region
}

resource "google_storage_bucket" "dataflow_bucket" {
  name          = var.bucket_name
  location      = var.region
  force_destroy = true
}

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

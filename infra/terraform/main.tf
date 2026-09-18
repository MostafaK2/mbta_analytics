terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "8.0.0"
    }
  }
}

provider "google" {
    project = var.project
    region  = var.region
    zone    = var.zone
    credentials = file(var.credentials_file)
}



resource "google_storage_bucket" "auto-expire" {
  name          = var.gcs_bucket_name
  location      = var.location
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 3
    }
    action {
      type = "Delete"
    }
  }

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

resource "google_bigquery_dataset" "demo_dataset" {
  dataset_id = var.bq_dataset_name
  location = var.location
  delete_contents_on_destroy = true
}
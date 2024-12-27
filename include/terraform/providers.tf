terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.9.0"
    }
  }
}

provider "google" {
    project = "testproject-bq-02"
    region  = "us-central1"
    zone    = "us-central1-a"
    credentials = file("/usr/local/airflow/include/gcp/service_account.json")
}
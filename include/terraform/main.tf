resource "google_storage_bucket" "gcs1" {
  name          = "testproject-bq-02-tfbucket"
  location      = "us-central1"
  force_destroy = true              # Allow the bucket to be destroyed even if it contains objects
}

resource "google_bigquery_dataset" "bq_dataset" {
  dataset_id  = "youtube_tf"        # Unique ID for the dataset in BigQuery
  location    = "US"                # Location for BigQuery datasets; use "US" or "EU" for multi-region
  project     = "testproject-bq-02" # Your GCP project ID
  
  labels = {
    env  = "dev"                     # Optional: add labels to organize resources
  }
}

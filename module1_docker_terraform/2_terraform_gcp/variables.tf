variable "credentials" {
  description = "My Credentials"
  default     = "/Users/xiaozhuxin/.google/credentials/google_credentials.json"
}

variable "project" {
  description = "Project"
  default     = "terraform-introduction-449023"
}

variable "region" {
  description = "Region"
  default     = "us-central1"
}

variable "location" {
  description = "Project Location"
  default     = "US"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  default     = "ny_taxi"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default     = "terraform-introduction-449023-terra-bucket"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}
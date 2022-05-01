locals {
  data_lake_bucket = "data_lake"
  cluster_bucket = "spark_cluster_bucket"
}

variable "project" {
  description = "GCP Project ID"
  default = "airpollution-348907"
  type = string
}

variable "credentials" {
  description = "path to the correct google service account"
  default = "/home/tim/.google/credentials/airpollution-348907-9b63fa676541.json"
  type = string
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default = "europe-west6"
  type = string
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default = "STANDARD"
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type = string
  default = "airpollution_data_all"
}

variable "DATAPROC_CLUSTER" {
  description = "Name of the dataproc cluster"
  type = string
  default = "airpollution-spark-cluster"
}
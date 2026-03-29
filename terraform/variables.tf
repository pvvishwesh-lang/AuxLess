variable "project_id" {
    description = "GCS PROJECT ID- CHANGE IF MOVING TO NEW ACCOUNT"
    type=string
}

variable "region" {
  description = "PRIMARY REGION FOR CLOUD RUN AND OTHER RESOURCES"
  type=string
  default = "europe-west1"
}

variable "dataflow_region" {
  description = "DATAFLOW REGION FOR JOBS"
  type=string
  default = "us-central1"
}

variable "firestore_location" {
  description = "FIRESTORE DATABASE LOCATION"
  type=string
  default = "nam5"
}

variable "firestore_database" {
    description = "FIRESTORE DATABASE"
    type=string
    default = "auxless"
}

variable "github_owner" {
  description = "GITHUB REPO OWNER"
  type=string
  default = "pvvishwesh-lang"
}

variable "github_repo" {
  description = "GITHUB REPO NAME"
  type=string
  default = "AuxLess"
}

variable "secret_names" {
  description = "List of Secrets to create"
  type=list(string)
  default = [ 
    "CLIENT_ID",
    "CLIENT_SECRET",
    "TOKEN_URI",
    "AUTH_URI",
    "auth_provider_x509_cert_url",
    "REDIRECT_URIS",
    "PROJECT_ID",
    "FIRESTORE_DATABASE",
    "BUCKET",
    "SERVICE_ACCOUNT_EMAIL",
    "SLACK_WEBHOOK_URL",
    "SESSION_READY_TOPIC",
    ]
}

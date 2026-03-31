terraform {
  required_version = ">=1.5"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~>5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

data "google_project" "current" {
  project_id = var.project_id
}

locals {
  project_number = data.google_project.current.number
  tf_secrets = [
    "PROJECT_ID",
    "FIRESTORE_DATABASE",
    "BUCKET",
    "SESSION_READY_TOPIC",
    "SERVICE_ACCOUNT_EMAIL"
  ]
  external_secrets = [
    "CLIENT_ID",
    "CLIENT_SECRET",
    "SLACK_WEBHOOK_URL",
    "TOKEN_URI",
    "AUTH_URI",
    "auth_provider_x509_cert_url",
    "REDIRECT_URIS"
  ]
  all_secrets = concat(local.tf_secrets, local.external_secrets)
}

# ── APIs ──────────────────────────────────────────────────────────────────────

resource "google_project_service" "apis" {
  for_each = toset([
    "cloudbuild.googleapis.com",
    "run.googleapis.com",
    "firestore.googleapis.com",
    "pubsub.googleapis.com",
    "dataflow.googleapis.com",
    "secretmanager.googleapis.com",
    "eventarc.googleapis.com",
    "eventarcpublishing.googleapis.com",
    "artifactregistry.googleapis.com",
    "bigquery.googleapis.com",
    "youtube.googleapis.com",
    "cloudfunctions.googleapis.com",
    "compute.googleapis.com",
  ])
  service                    = each.key
  disable_on_destroy         = false
  disable_dependent_services = false
}

# ── Firestore ─────────────────────────────────────────────────────────────────

resource "google_firestore_database" "auxless" {
  project     = var.project_id
  name        = var.firestore_database
  location_id = var.firestore_location
  type        = "FIRESTORE_NATIVE"
  depends_on  = [google_project_service.apis]
}

# ── GCS Buckets ───────────────────────────────────────────────────────────────

resource "google_storage_bucket" "pipeline" {
  name                        = "${var.project_id}-pipeline"
  location                    = "US"
  uniform_bucket_level_access = true
  force_destroy               = true
  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type = "Delete"
    }
  }
  soft_delete_policy {
    retention_duration_seconds = 0
  }
}

# ML model storage bucket — stores gru_model.pt and ml outputs
resource "google_storage_bucket" "ml_models" {
  name                        = "${var.project_id}-ml-models"
  location                    = "US"
  uniform_bucket_level_access = true
  force_destroy               = true
  versioning {
    enabled = true
  }
  soft_delete_policy {
    retention_duration_seconds = 0
  }
}

# ── Artifact Registry ─────────────────────────────────────────────────────────

resource "google_artifact_registry_repository" "cloud_run" {
  location      = var.region
  repository_id = "cloud-run-source-deploy"
  format        = "DOCKER"
  depends_on    = [google_project_service.apis]
}

# ── Pub/Sub Topics ────────────────────────────────────────────────────────────

resource "google_pubsub_topic" "cloud_function" {
  name = "CloudFunctionPUBSUB"
}

resource "google_pubsub_topic" "session_ready" {
  name = "SESSION_READY_TOPIC"
}

resource "google_pubsub_topic" "feedback" {
  name = "FeedbackTopic"
}

# ── Pub/Sub Subscriptions ─────────────────────────────────────────────────────

# Batch pipeline trigger — CloudFunctionPUBSUB -> Cloud Run /
resource "google_pubsub_subscription" "batch_trigger" {
  name  = "CloudFunctionPUBSUB-sub"
  topic = google_pubsub_topic.cloud_function.id
  push_config {
    push_endpoint = "${google_cloud_run_v2_service.auxless_api.uri}/"
  }
  ack_deadline_seconds = 600
  depends_on           = [google_cloud_run_v2_service.auxless_api]
}

# ML pipeline trigger — SESSION_READY_TOPIC -> Cloud Run /ml
resource "google_pubsub_subscription" "ml_trigger" {
  name  = "session-ready-ml-sub"
  topic = google_pubsub_topic.session_ready.id
  push_config {
    push_endpoint = "${google_cloud_run_v2_service.auxless_api.uri}/ml"
  }
  ack_deadline_seconds = 600
  depends_on           = [google_cloud_run_v2_service.auxless_api]
}

# Feedback subscription — FeedbackTopic -> feedback-events-sub
resource "google_pubsub_subscription" "feedback_event" {
  name                 = "feedback-events-sub"
  topic                = google_pubsub_topic.feedback.id
  ack_deadline_seconds = 600
}

# ── Secret Manager ────────────────────────────────────────────────────────────

resource "google_secret_manager_secret" "secrets" {
  for_each  = toset(local.tf_secrets)
  secret_id = each.key

  replication {
    auto {}
  }

  depends_on = [google_project_service.apis]
}

resource "google_secret_manager_secret_version" "project_id" {
  secret      = google_secret_manager_secret.secrets["PROJECT_ID"].id
  secret_data = var.project_id
}

resource "google_secret_manager_secret_version" "firestore_db" {
  secret      = google_secret_manager_secret.secrets["FIRESTORE_DATABASE"].id
  secret_data = var.firestore_database
}

resource "google_secret_manager_secret_version" "bucket" {
  secret      = google_secret_manager_secret.secrets["BUCKET"].id
  secret_data = google_storage_bucket.pipeline.name
}

resource "google_secret_manager_secret_version" "token_uri" {
  secret      = google_secret_manager_secret.secrets["TOKEN_URI"].id
  secret_data = "https://oauth2.googleapis.com/token"
}

resource "google_secret_manager_secret_version" "auth_uri" {
  secret      = google_secret_manager_secret.secrets["AUTH_URI"].id
  secret_data = "https://accounts.google.com/o/oauth2/auth"
}

resource "google_secret_manager_secret_version" "auth_cert" {
  secret      = google_secret_manager_secret.secrets["auth_provider_x509_cert_url"].id
  secret_data = "https://www.googleapis.com/oauth2/v1/certs"
}

resource "google_secret_manager_secret_version" "session_ready_topic" {
  secret      = google_secret_manager_secret.secrets["SESSION_READY_TOPIC"].id
  secret_data = "SESSION_READY_TOPIC"
}

resource "google_secret_manager_secret_version" "service_account" {
  secret      = google_secret_manager_secret.secrets["SERVICE_ACCOUNT_EMAIL"].id
  secret_data = "${local.project_number}-compute@developer.gserviceaccount.com"
}

# ── BigQuery ──────────────────────────────────────────────────────────────────

resource "google_bigquery_dataset" "song_recommendations" {
  dataset_id = "song_recommendations"
  location   = "US"
  delete_contents_on_destroy = true
}

# Song embeddings table — ML pipeline catalog (81K songs)
resource "google_bigquery_table" "song_embeddings" {
  dataset_id          = google_bigquery_dataset.song_recommendations.dataset_id
  table_id            = "song_embeddings"
  deletion_protection = false
  schema = jsonencode([
    { name = "video_id",         type = "STRING",  mode = "REQUIRED" },
    { name = "track_title",      type = "STRING",  mode = "NULLABLE" },
    { name = "artist_name",      type = "STRING",  mode = "NULLABLE" },
    { name = "genre",            type = "STRING",  mode = "NULLABLE" },
    { name = "duration_sec",     type = "FLOAT64", mode = "NULLABLE" },
    { name = "popularity_score", type = "FLOAT64", mode = "NULLABLE" },
    { name = "embedding",        type = "FLOAT64", mode = "REPEATED" },
  ])
}

# Users table — liked/disliked/skipped/replayed songs per user
resource "google_bigquery_table" "users" {
  dataset_id          = google_bigquery_dataset.song_recommendations.dataset_id
  table_id            = "users"
  deletion_protection = false
  schema = jsonencode([
    { name = "user_id",        type = "STRING",    mode = "REQUIRED" },
    { name = "liked_songs",    type = "STRING",    mode = "REPEATED" },
    { name = "disliked_songs", type = "STRING",    mode = "REPEATED" },
    { name = "skipped_songs",  type = "STRING",    mode = "REPEATED" },
    { name = "replayed_songs", type = "STRING",    mode = "REPEATED" },
    { name = "last_updated",   type = "TIMESTAMP", mode = "NULLABLE" },
  ])
}

# Session history table — feedback events per session
resource "google_bigquery_table" "session_history" {
  dataset_id          = google_bigquery_dataset.song_recommendations.dataset_id
  table_id            = "session_history"
  deletion_protection = false
  schema = jsonencode([
    { name = "session_id",  type = "STRING",    mode = "REQUIRED" },
    { name = "user_id",     type = "STRING",    mode = "REQUIRED" },
    { name = "video_id",    type = "STRING",    mode = "REQUIRED" },
    { name = "action",      type = "STRING",    mode = "NULLABLE" },
    { name = "score_delta", type = "FLOAT64",   mode = "NULLABLE" },
    { name = "timestamp",   type = "TIMESTAMP", mode = "NULLABLE" },
  ])
}

# ── IAM ───────────────────────────────────────────────────────────────────────

resource "google_project_iam_member" "cloudbuild_roles" {
  for_each = toset([
    "roles/run.admin",
    "roles/secretmanager.secretAccessor",
    "roles/iam.serviceAccountUser",
  ])
  project = var.project_id
  role    = each.key
  member  = "serviceAccount:${local.project_number}@cloudbuild.gserviceaccount.com"
}

resource "google_project_iam_member" "compute_roles" {
  for_each = toset([
    "roles/dataflow.worker",
    "roles/storage.objectAdmin",
    "roles/datastore.user",
    "roles/eventarc.eventReceiver",
    "roles/pubsub.editor",
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser",
    "roles/secretmanager.secretAccessor",
  ])
  project = var.project_id
  role    = each.key
  member  = "serviceAccount:${local.project_number}-compute@developer.gserviceaccount.com"
}

# NOTE: Commented out until eventarc service account is provisioned.
# Re-enable after running: gcloud services enable eventarc.googleapis.com
# and waiting ~5 minutes for the service account to be created.
# resource "google_project_iam_member" "eventarc_invoker" {
#   project = var.project_id
#   role    = "roles/run.invoker"
#   member  = "serviceAccount:service-${local.project_number}@gcp-sa-eventarc.iam.gserviceaccount.com"
#   depends_on = [google_project_service.apis]
# }

# ── Cloud Run ─────────────────────────────────────────────────────────────────

resource "google_cloud_run_v2_service" "auxless_api" {
  name     = "auxless-api"
  location = var.region
  template {
    scaling {
      min_instance_count = 1
      max_instance_count = 6
    }
    containers {
      image = "${var.region}-docker.pkg.dev/${var.project_id}/cloud-run-source-deploy/auxless/auxless-git:latest"
      ports {
        container_port = 8080
      }
      dynamic "env" {
        for_each = local.all_secrets
        content {
          name = env.value
          value_source {
            secret_key_ref {
              secret  = env.value 
              version = "latest"
            }
          }
        }
      }
      env {
        name  = "PYTHONUNBUFFERED"
        value = "1"
      }
      env {
        name  = "GRU_MODEL_PATH"
        value = "gs://${google_storage_bucket.ml_models.name}/models/gru_model.pt"
      }
    }
    annotations = {
      "run.googleapis.com/cpu-throttling" = "false"
    }
  }
  depends_on = [
    google_project_service.apis,
    google_secret_manager_secret.secrets,
    google_artifact_registry_repository.cloud_run,
    google_storage_bucket.ml_models,
  ]
}

resource "google_cloud_run_v2_service_iam_member" "public" {
  project  = var.project_id
  location = var.region
  name     = google_cloud_run_v2_service.auxless_api.name
  role     = "roles/run.invoker"
  member   = "allUsers"
}

# NOTE: Commented out until eventarc service account is provisioned.
# resource "google_eventarc_trigger" "firestore_session" {
#   name     = "auxless-session-trigger"
#   location = var.firestore_location
#   matching_criteria {
#     attribute = "type"
#     value     = "google.cloud.firestore.document.v1.created"
#   }
#   matching_criteria {
#     attribute = "database"
#     value     = var.firestore_database
#   }
#   matching_criteria {
#     attribute = "document"
#     value     = "sessions/{sessionId}"
#     operator  = "match-path-pattern"
#   }
#   destination {
#     cloud_run_service {
#       service = "auxlessfunction"
#       region  = var.region
#       path    = "/"
#     }
#   }
#   service_account         = "${local.project_number}-compute@developer.gserviceaccount.com"
#   event_data_content_type = "application/protobuf"
#   depends_on = [
#     google_project_service.apis,
#     google_firestore_database.auxless,
#     google_project_iam_member.eventarc_invoker,
#   ]
# }

# NOTE: Cloud Build trigger is managed manually via GCP Console due to
# GitHub App connection incompatibility with terraform google provider.
# Trigger ID: 72f46029-0b2c-426d-be92-30cf6915f032
# resource "google_cloudbuild_trigger" "deploy" {
#   name     = "auxless-deploy"
#   location = "global"
#   github {
#     owner = var.github_owner
#     name  = var.github_repo
#     push {
#       branch = "^main$"
#     }
#   }
#   filename   = "cloudbuild.yaml"
#   depends_on = [google_project_service.apis]
# }

# ── Outputs ───────────────────────────────────────────────────────────────────

output "cloud_run_url" {
  value = google_cloud_run_v2_service.auxless_api.uri
}

output "pipeline_bucket" {
  value = google_storage_bucket.pipeline.name
}

output "ml_models_bucket" {
  value = google_storage_bucket.ml_models.name
}

output "project_number" {
  value = local.project_number
}

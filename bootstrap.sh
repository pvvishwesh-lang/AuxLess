#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
TFVARS="$SCRIPT_DIR/terraform/terraform.tfvars"
PROJECT_ID=$(grep 'project_id' "$TFVARS" | sed 's/.*= *"\(.*\)"/\1/')
REGION=$(grep '^region ' "$TFVARS" | sed 's/.*= *"\(.*\)"/\1/' || echo "europe-west1")

echo ""
echo "Setting gcloud project..."
gcloud config set project "$PROJECT_ID"

echo ""
echo "Enabling APIs..."
gcloud services enable \
  cloudbuild.googleapis.com \
  run.googleapis.com \
  artifactregistry.googleapis.com \
  secretmanager.googleapis.com \
  firestore.googleapis.com \
  pubsub.googleapis.com \
  dataflow.googleapis.com \
  eventarc.googleapis.com \
  eventarcpublishing.googleapis.com \
  bigquery.googleapis.com \
  youtube.googleapis.com \
  compute.googleapis.com \
  cloudfunctions.googleapis.com \
  storage.googleapis.com

echo "Waiting 30s for APIs to propagate..."
sleep 30

echo ""
echo "Creating Artifact Registry..."
gcloud artifacts repositories create cloud-run-source-deploy \
  --repository-format=docker \
  --location="$REGION" \
  --quiet 2>/dev/null || echo "  (already exists)"

echo ""
echo "Creating ML models bucket..."
ML_MODELS_BUCKET="${PROJECT_ID}-ml-models"
gcloud storage buckets create "gs://${ML_MODELS_BUCKET}" --location=US --quiet 2>/dev/null || echo "  (bucket already exists)"
gcloud storage buckets update "gs://${ML_MODELS_BUCKET}" --versioning --quiet 2>/dev/null || true

echo ""
echo "Building Docker images..."

echo "  Building auxless-api image..."
gcloud builds submit \
  --config=cloudbuild.yaml \
  "$SCRIPT_DIR" \
  --quiet

echo "  Building feedback-streaming image..."
gcloud builds submit \
  --config=/dev/stdin "$SCRIPT_DIR" <<EOF
steps:
  - name: gcr.io/cloud-builders/docker
    args:
      - build
      - -f
      - Dockerfile.streaming
      - -t
      - ${REGION}-docker.pkg.dev/${PROJECT_ID}/cloud-run-source-deploy/feedback-streaming:latest
      - .
images:
  - ${REGION}-docker.pkg.dev/${PROJECT_ID}/cloud-run-source-deploy/feedback-streaming:latest
EOF

echo ""
echo "Building Dataflow Flex Template..."

BUCKET_NAME="${PROJECT_ID}-pipeline"
gcloud storage buckets create "gs://${BUCKET_NAME}" --location=US --quiet 2>/dev/null || echo "  (bucket already exists)"
gcloud storage buckets update "gs://${BUCKET_NAME}" --no-soft-delete --quiet 2>/dev/null || true

gcloud dataflow flex-template build \
  "gs://${BUCKET_NAME}/templates/feedback-streaming.json" \
  --image "${REGION}-docker.pkg.dev/${PROJECT_ID}/cloud-run-source-deploy/feedback-streaming:latest" \
  --sdk-language PYTHON

echo ""
echo "Running Terraform..."
cd "$SCRIPT_DIR/terraform"
terraform init -input=false
terraform apply -auto-approve

CLOUD_RUN_URL=$(terraform output -raw cloud_run_url)
ML_MODELS_BUCKET=$(terraform output -raw ml_models_bucket)

echo ""
echo "Deploying Cloud Functions..."

if [ -d "$SCRIPT_DIR/backend/functions" ]; then
  echo "  Deploying auxlessfunction..."
  cd "$SCRIPT_DIR/backend/functions"
  gcloud functions deploy auxlessfunction \
    --gen2 \
    --region="$REGION" \
    --runtime=python312 \
    --entry-point=firestore_session_trigger \
    --source=. \
    --trigger-location=nam5 \
    --trigger-event-filters="type=google.cloud.firestore.document.v1.created" \
    --trigger-event-filters="database=auxless" \
    --trigger-event-filters-path-pattern="document=sessions/{sessionId}" \
    --quiet 2>/dev/null || echo "  (deploy via console if this fails)"

  echo "  Deploying auxlessstreamfunction..."
  gcloud functions deploy auxlessstreamfunction \
    --gen2 \
    --region="$REGION" \
    --runtime=python312 \
    --entry-point=trigger_streaming_pipeline \
    --source=. \
    --trigger-topic=SESSION_READY_TOPIC \
    --set-env-vars="PROJECT_ID=${PROJECT_ID},BUCKET=${BUCKET_NAME},FIRESTORE_DATABASE=auxless,REGION=us-central1,STREAMING_TEMPLATE_PATH=gs://${BUCKET_NAME}/templates/feedback-streaming.json" \
    --quiet 2>/dev/null || echo "  (deploy via console if this fails)"
fi

echo ""
echo "============================================"
echo "  Deployment Complete!"
echo "============================================"
echo "  Cloud Run URL:      $CLOUD_RUN_URL"
echo "  Pipeline Bucket:    gs://${BUCKET_NAME}"
echo "  ML Models Bucket:   gs://${ML_MODELS_BUCKET}"
echo "  GRU Model Path:     gs://${ML_MODELS_BUCKET}/models/gru_model.pt"
echo "============================================"
echo ""
echo "  NOTE: Upload your trained GRU model to:"
echo "  gs://${ML_MODELS_BUCKET}/models/gru_model.pt"
echo ""
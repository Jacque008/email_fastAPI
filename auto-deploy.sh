#!/bin/bash
REGION="europe-west4"
PROJECT_ID="drp-system"   
REPO_NAME="ml"
IMAGE_NAME="classify_emails" 
IMAGE_TAG="${2:-latest}"
IMAGE_URI=${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO_NAME}/${IMAGE_NAME}:${IMAGE_TAG}

cp app/main.py data/backup/main.py
cp requirements.txt data/backup/requirements.txt

echo "🚀 Building Image from Dockerfile ...\n"
echo "IMAGE_URI: $IMAGE_URI"
docker build -f Dockerfile -t $IMAGE_URI ./ --platform=linux/amd64
echo "🚀 Pushing Image to Google Cloud Artifact Registry" 
docker push $IMAGE_URI

if [ "$1" = "prod" ]
then
    SERVICE_NAME="classify-emails"
    echo "Deploying new revision of $SERVICE_NAME to Production Environment ..."
else
    SERVICE_NAME="classify-emails-wisentic"
    echo "Deploying new revision of $SERVICE_NAME to Test Environment ..."
fi

gcloud run deploy $SERVICE_NAME \
    --image $IMAGE_URI \
    --region $REGION \
    --platform managed \
    --allow-unauthenticated \
    --memory 2Gi \
    --cpu 2 \
    --timeout 3600 \
    --set-env-vars ENV_MODE=test

# ✅ Display Cloud Run URL
SERVICE_URL=$(gcloud run services describe $SERVICE_NAME --region $REGION --format 'value(status.url)')

echo "✅ Deployment successful! Your service is live at: $SERVICE_URL"
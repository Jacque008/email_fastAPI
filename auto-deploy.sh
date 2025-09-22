#!/bin/bash
REGION="europe-west4"
PROJECT_ID="drp-system"   
REPO_NAME="ml"
IMAGE_NAME="classify_emails" 
IMAGE_TAG="$2"
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
    --memory 1Gi \
    --cpu 1 \
    --timeout 3600 \
    --set-env-vars ENV_MODE=test,DB_USER=drp,DB_NAME=drp-production,INSTANCE_CONNECTION_NAME=drp-system:europe-west4:drp,GOOGLE_REDIRECT_URI=https://classify-emails-wisentic-596633500987.europe-west4.run.app/auth/google/callback \
    --set-secrets DB_PASSWORD=DRP_DB_PASSWORD:latest,GROQ_API_KEY=GROQ_API_KEY:latest,SECRET_KEY=FLASK_SECRET_KEY:latest,OAUTH2_CLIENT_SECRET=OAUTH2_CLIENT_SECRET_JIE:latest,SECRET_KEY_FASTAPI=SECRET_KEY_FASTAPI:latest,JWT_SECRET_FASTAPI=JWT_SECRET_FASTAPI:latest,GOOGLE_CLIENT_ID=GOOGLE_CLIENT_ID:latest,GOOGLE_CLIENT_SECRET=GOOGLE_CLIENT_SECRET:latest \
    --set-secrets /SERVICE_ACCOUNT_JIE/SERVICE_ACCOUNT_JIE=SERVICE_ACCOUNT_JIE:latest

# ✅ Display Cloud Run URL
SERVICE_URL=$(gcloud run services describe $SERVICE_NAME --region $REGION --format 'value(status.url)')

echo "✅ Deployment successful! Your service is live at: $SERVICE_URL"
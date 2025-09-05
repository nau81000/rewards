#!/bin/sh
set -e

echo -n "Waiting for Dremio API..."
until curl -s http://dremio:9047/apiv2/server_status | grep OK >/dev/null; do
  sleep 5
done
echo ""

echo "Creating first admin..."
curl -s -X PUT http://dremio:9047/apiv2/bootstrap/firstuser \
  -H "Content-Type: application/json" \
  -d @- <<EOF
  {
    "userName": "${DREMIO_USERNAME}",
    "password": "${DREMIO_PASSWORD}",
    "firstName": "Admin",
    "lastName": "User",
    "email": "admin@example.com"
  }
EOF
echo ""

echo "Getting token..."
RESPONSE=$(curl -s -X POST http://dremio:9047/apiv2/login \
  -H "Content-Type: application/json" \
  -d @- <<EOF
  {
    "userName": "${DREMIO_USERNAME}",
    "password": "${DREMIO_PASSWORD}"
  }
EOF
)
TOKEN=$(echo "$RESPONSE" | sed -E 's/.*"token":"([^"]+)".*/\1/')

echo "Adding AWS S3 source..."
curl -s -X POST "http://dremio:9047/api/v3/catalog" \
  -H "Authorization: _dremio${TOKEN}" \
  -H "Content-Type: application/json" \
  -d @- <<EOF
  {
  	"entityType": "source",
    "name":"${DREMIO_SOURCE_NAME}",
    "type":"S3",
    "config": {
      "credentialType": "ACCESS_KEY",
      "accessKey": "${AWS_ACCESS_KEY_ID}",
      "accessSecret": "${AWS_SECRET_ACCESS_KEY}",
      "secure":true
    }
  }
EOF
echo ""

for folder in activities rewards
do
  echo "Promoting Delta Lake folder '${AWS_S3_BUCKET_NAME}/delta/${folder}'..."
  FOLDER_ID="dremio%3A%2F${DREMIO_SOURCE_NAME}%2F${AWS_S3_BUCKET_NAME}%2Fdelta%2F${folder}"
  curl -s -X POST "http://dremio:9047/api/v3/catalog/${FOLDER_ID}" \
    -H "Authorization: _dremio${TOKEN}" \
    -H "Content-Type: application/json" \
    -d @- <<EOF
  {
    "entityType": "dataset",
    "type": "PHYSICAL_DATASET",
    "path": ["${DREMIO_SOURCE_NAME}", "${AWS_S3_BUCKET_NAME}", "delta", "${folder}"],
    "format": { 
      "type": "Delta",
      "fullPath": ["${DREMIO_SOURCE_NAME}", "${AWS_S3_BUCKET_NAME}", "delta", "${folder}"],
      "isFolder": true
    }
  }
EOF
  echo ""
done

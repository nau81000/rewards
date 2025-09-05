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

echo "Creating Tableau activities and rewards tds files..."
for folder in activities rewards
do
  cat > /tmp/${folder}.tds <<EOF
  <?xml version='1.0' encoding='utf-8'?>
  <datasource inline="true" version="18.1">
  <connection class="dremio" dbname="DREMIO" schema="&quot;${DREMIO_SOURCE_NAME}&quot;.&quot;${AWS_S3_BUCKET_NAME}&quot;.delta" port="31010" server="127.0.0.1" username="dremio" v-dremio-product="v-software" sslmode="" v-disable-cert-verification="" v-use-flight-driver="false" authentication="basic" v-routing-queue="" v-routing-tag="" v-engine="">
  <relation name="${folder}" type="table" table="[${DREMIO_SOURCE_NAME}.${AWS_S3_BUCKET_NAME}.delta].[${folder}]"/>
  </connection>
  <aliases enabled="yes"/>
  </datasource>
EOF
done


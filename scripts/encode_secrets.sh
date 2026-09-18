#!/bin/bash

ENV_FILENAME=.env_encoded

while IFS='=' read -r key value; do
  echo "SECRET_$key=$(echo -n "$value" | base64)";
done < ../.env > $ENV_FILENAME

## Encodes the service account file without line wrapping to make sure the whole JSON value is intact.
echo "SECRET_GCP_SERVICE_ACCOUNT=$(cat ../keys/my_creds.json | base64 -w 0)" >> $ENV_FILENAME

# Move encoded env file into kestra folder
mv "$ENV_FILENAME" ../kestra/.env_encoded

#!/bin/bash

# Check if .env file exists
if [ ! -f .env ]; then
  echo ".env file not found!"
  exit 1
fi

# Load the secret scope from the .env file
SECRET_SCOPE=$(grep -w 'SECRET_SCOPE' .env | cut -d '=' -f 2 | xargs)

# Check if the SECRET_SCOPE is defined
if [ -z "$SECRET_SCOPE" ]; then
  echo "SECRET_SCOPE is not defined in the .env file!"
  exit 1
fi

echo "Using secret scope: $SECRET_SCOPE"

# Check if the secret scope exists
SCOPE_EXISTS=$(databricks secrets list-scopes | grep -w "$SECRET_SCOPE")

# Create the secret scope if it doesn't exist
if [ -z "$SCOPE_EXISTS" ]; then
  echo "Secret scope $SECRET_SCOPE does not exist. Creating it..."
  databricks secrets create-scope "$SECRET_SCOPE"
  
  if [ $? -eq 0 ]; then
    echo "Successfully created secret scope: $SECRET_SCOPE"
  else
    echo "Failed to create secret scope: $SECRET_SCOPE"
    exit 1
  fi
else
  echo "Secret scope $SECRET_SCOPE already exists."
fi

# Load environment variables from .env file and store them as secrets in Databricks using JSON
while IFS='=' read -r key value; do
  # Trim leading and trailing whitespace
  key=$(echo "$key" | xargs)
  value=$(echo "$value" | xargs)

  # Skip empty lines, comments, and the SECRET_SCOPE line itself
  if [[ -z "$key" || "$key" == "SECRET_SCOPE" || "$key" == \#* ]]; then
    continue
  fi

  # Create JSON payload for each secret
  JSON_PAYLOAD=$(cat <<EOF
{
  "scope": "$SECRET_SCOPE",
  "key": "$key",
  "string_value": "$value"
}
EOF
)

  # Store the secret using the JSON payload
  databricks secrets put-secret --json "$JSON_PAYLOAD"

  # Check if the secret was successfully uploaded
  if [ $? -eq 0 ]; then
    echo "Successfully stored secret for key: $key"
  else
    echo "Failed to store secret for key: $key"
  fi

done < .env


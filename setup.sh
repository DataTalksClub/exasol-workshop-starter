#!/bin/bash
# Run this in your Codespace to configure AWS access.
# Usage: source setup.sh <endpoint_url> <token>

if [ $# -lt 2 ]; then
    echo "Usage: source setup.sh <endpoint_url> <token>"
    return 1 2>/dev/null || exit 1
fi

export AWS_CONTAINER_CREDENTIALS_FULL_URI="$1"
export AWS_CONTAINER_AUTHORIZATION_TOKEN="$2"
unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN AWS_PROFILE

echo "Testing AWS access..."
aws sts get-caller-identity 2>/dev/null && echo "Success!" || echo "Failed - check your endpoint URL and token."

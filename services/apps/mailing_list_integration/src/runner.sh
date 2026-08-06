#!/bin/bash

# Exit on any error
set -e

# Get environment from first argument, default to production
ENV=${1:-"prod"}

# Default values (following project patterns)
HOST=${HOST:-"0.0.0.0"}
PORT=${PORT:-"8085"}

# Change to the script directory
cd "$(dirname "$0")"

# Check if we're in development mode (following project patterns)
if [ "$ENV" = "local" ]; then
    # Development mode with reload
    exec uvicorn crowdmail.server:app --host "$HOST" --port "$PORT" --reload
else
    # Production mode with proxy headers and workers
    exec uvicorn crowdmail.server:app --proxy-headers --host "$HOST" --port "$PORT" --workers 1
fi

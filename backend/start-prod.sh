#!/bin/bash

# Production backend startup script
set -e

# Configuration
BACKEND_HOST=${BACKEND_HOST:-0.0.0.0}
BACKEND_PORT=${BACKEND_PORT:-9998}
WORKERS=${WORKERS:-1}

echo "Starting STICAL Data Backend..."
echo "Host: $BACKEND_HOST"
echo "Port: $BACKEND_PORT"
echo "Workers: $WORKERS"

# Set production environment
export ENVIRONMENT=production

# Install requirements if needed
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

echo "Activating virtual environment..."
source venv/bin/activate

echo "Installing/updating requirements..."
pip install -r requirements.txt

echo "Starting FastAPI server with Gunicorn..."
gunicorn main:app \
    --bind $BACKEND_HOST:$BACKEND_PORT \
    --workers $WORKERS \
    --worker-class uvicorn.workers.UvicornWorker \
    --access-logfile - \
    --error-logfile - \
    --log-level info

echo "Backend stopped."

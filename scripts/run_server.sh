#!/bin/bash
set -euo pipefail

PYBIN="/opt/homebrew/bin/python3.11"
if [ ! -x "$PYBIN" ]; then
  echo "Python 3.11 not found at $PYBIN. Please install with: brew install python@3.11" >&2
  exit 1
fi

echo "Using Python interpreter: $PYBIN"

# Recreate venv to ensure correct Python version
rm -rf venv
"$PYBIN" -m venv venv
source venv/bin/activate

# Upgrade packaging tools
python -m pip install --upgrade pip setuptools wheel

# Install deps
pip install -r requirements.txt

# Load env if present
if [ -f .env ]; then
  set -a
  # shellcheck disable=SC2046
  export $(grep -v '^#' .env | xargs)
  set +a
fi

# Fallbacks
export DATABENTO_API_KEY=${DATABENTO_API_KEY:-"your_databento_api_key_here"}
export DATABASE_URL=${DATABASE_URL:-"postgresql://postgres:postgres@localhost:5432/aitrader"}
export DISCORD_WEBHOOK_URL=${DISCORD_WEBHOOK_URL:-""}

# Run API
echo "Starting FastAPI server on http://localhost:8000"
python app.py


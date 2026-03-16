#!/usr/bin/env bash

# Simple deploy helper for the OilData app on the VPS.
# Usage on VPS:
#   cd /root/OilData
#   bash deploy.sh

set -e

echo "== Pulling latest code from Git =="
git pull

echo "== Activating virtualenv =="
if [ -d "venv" ]; then
  # shellcheck disable=SC1091
  source venv/bin/activate
else
  python3 -m venv venv
  # shellcheck disable=SC1091
  source venv/bin/activate
fi

echo "== Installing dependencies =="
pip install -r requirements.txt

echo "== Using NEON_DATABASE_URL from environment =="
if [ -z "${NEON_DATABASE_URL:-}" ]; then
  echo "WARNING: NEON_DATABASE_URL is not set. Neon writes will be disabled."
else
  echo "NEON_DATABASE_URL is set."
fi

echo "== Starting Streamlit app on port 8501 =="
echo "   Visit: http://31.220.48.107:8501"
echo "   (Press Ctrl+C to stop, or run this script under nohup if you want it detached.)"

streamlit run app.py --server.port 8501 --server.address 0.0.0.0


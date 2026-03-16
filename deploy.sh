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

echo "== Setting Neon connection string =="
export NEON_DATABASE_URL="postgresql://neondb_owner:npg_tzx7mJyPqE0H@ep-patient-firefly-a82i7jrf-pooler.eastus2.azure.neon.tech/neondb?sslmode=require&channel_binding=require"

echo "== Starting Streamlit app on port 8501 =="
echo "   Visit: http://31.220.48.107:8501"
echo "   (Press Ctrl+C to stop, or run this script under nohup if you want it detached.)"

streamlit run app.py --server.port 8501 --server.address 0.0.0.0


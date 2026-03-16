#!/usr/bin/env bash

# Simple helper to run all scrapers for Neon.
# Usage on VPS:
#   cd /root/OilData
#   bash run_scrapers.sh

set -e

cd "$(dirname "$0")"

echo "== Activating virtualenv =="
if [ -d "venv" ]; then
  # shellcheck disable=SC1091
  source venv/bin/activate
else
  python3 -m venv venv
  # shellcheck disable=SC1091
  source venv/bin/activate
fi

if [ -z "${NEON_DATABASE_URL:-}" ]; then
  echo "ERROR: NEON_DATABASE_URL is not set."
  echo "Set it once in this terminal, e.g.:"
  echo "  export NEON_DATABASE_URL=\"postgresql://neondb_owner:...@.../neondb?sslmode=require&channel_binding=require\""
  exit 1
fi

echo "== Running spot price scraper =="
python spot_price_scraper.py

echo "All scrapers finished."


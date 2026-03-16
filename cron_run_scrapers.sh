#!/usr/bin/env bash
#
# Run scrapers from cron (no terminal). Cron has no env, so we load NEON URL
# from a file you create once on the VPS.
#
# One-time setup on VPS:
#   1. Create file:  /root/OilData/.env.neon
#      with one line:  export NEON_DATABASE_URL="postgresql://..."
#   2. chmod +x /root/OilData/cron_run_scrapers.sh
#   3. crontab -e  and add (daily at 6:00 AM server time):
#      0 6 * * * /root/OilData/cron_run_scrapers.sh >> /root/OilData/cron_scraper.log 2>&1
#
set -e

cd /root/OilData

# Load Neon URL (create this file once on the VPS; do not commit it)
if [ -f .env.neon ]; then
  # shellcheck disable=SC1091
  source .env.neon
fi

if [ -z "${NEON_DATABASE_URL:-}" ]; then
  echo "$(date -Iseconds) ERROR: NEON_DATABASE_URL not set. Create /root/OilData/.env.neon with: export NEON_DATABASE_URL=\"...\"" >> /root/OilData/cron_scraper.log
  exit 1
fi

# shellcheck disable=SC1091
source venv/bin/activate

echo "$(date -Iseconds) Running scrapers..."
python spot_price_scraper.py
echo "$(date -Iseconds) Done."

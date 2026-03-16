# Oil Data Scraper

Streamlit app for oil price data (WTI, Brent, etc.) via Yahoo Finance.

## Run locally

```bash
pip install -r requirements.txt
streamlit run app.py
```

## Deploy on VPS (Hostinger terminal)

After pushing this repo to GitHub, on the VPS run:

```bash
cd /root
git clone https://github.com/kapitalized/OilData.git
cd OilData
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
streamlit run app.py --server.port 8501 --server.address 0.0.0.0
```

Then open **http://31.220.48.107:8501** in your browser.

To run in background: `nohup streamlit run app.py --server.port 8501 --server.address 0.0.0.0 &`

## Run scrapers on a schedule (no terminal)

So the scraper runs automatically (e.g. daily) without keeping a terminal open:

1. **One-time on VPS:** create a file with your Neon URL (never commit this file):

   ```bash
   cd /root/OilData
   echo 'export NEON_DATABASE_URL="postgresql://neondb_owner:YOUR_PASSWORD@YOUR_HOST/neondb?sslmode=require&channel_binding=require"' > .env.neon
   chmod +x cron_run_scrapers.sh
   ```

2. **Install the cron job** (e.g. daily at 6:00 AM server time):

   ```bash
   crontab -e
   ```

   Add this line (then save and exit):

   ```
   0 6 * * * /root/OilData/cron_run_scrapers.sh >> /root/OilData/cron_scraper.log 2>&1
   ```

3. **Check logs:** `tail -f /root/OilData/cron_scraper.log`

To run at a different time, change `0 6` (6:00 AM): e.g. `0 0` = midnight, `30 8` = 8:30 AM.

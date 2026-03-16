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

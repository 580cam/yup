# Realtor Lead Scraper - Python Version

Simple HTTP-based scraper (no browser needed!) for finding and enriching realtor leads.

## Deploy to Railway

1. Go to [railway.app](https://railway.app)
2. Click "New Project" > "Deploy from GitHub repo"
3. Connect your GitHub and select this repo
4. Railway will auto-detect Python and deploy
5. Done! Your app will be live in ~2 minutes

## Deploy to Render

1. Go to [render.com](https://render.com)
2. Click "New+" > "Web Service"
3. Connect your GitHub repo
4. Settings:
   - Build Command: `pip install -r requirements.txt`
   - Start Command: `gunicorn app:app`
5. Click "Create Web Service"

## Local Testing

```bash
pip install -r requirements.txt
python app.py
```

Open http://localhost:5000

## How It Works

- **No browser needed!** Uses simple HTTP requests + BeautifulSoup
- Scrapes realtor.com for agents in your target areas
- Enriches with: sales stats, contact info, social media
- Exports to CSV

## Features

- Find realtors by zip code or city
- Upload CSV to enrich existing leads
- Get: email, phone, years experience, total sales, 12-month sales, social media links
- Download results as CSV

## Rate Limiting

- 1 second delay between each scrape
- Respectful to servers
- For faster scraping, adjust `time.sleep(1)` in app.py
# Redeploy Tue Dec 16 02:24:39 CST 2025

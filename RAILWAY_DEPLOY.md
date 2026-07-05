# 🚀 Railway Deployment Guide

Deploy the Reddit Brand Monitor to Railway. This is a **single service**: the
Flask backend also serves the dashboard (frontend), so you only deploy one
thing — there is no separate frontend deployment.

## 📋 Prerequisites

1. A [Railway](https://railway.app) account
2. Reddit API credentials (script app)
3. (Optional) A [Groq](https://console.groq.com) API key for sentiment analysis

## 🔑 1. Get Reddit API credentials

1. Log into Reddit, go to https://www.reddit.com/prefs/apps
2. Click "create another app…"
3. Type: **script**. Name/description: anything. Redirect URI: `http://localhost:8080`
4. Copy the **Client ID** (string under the app name) and **Client Secret**

> If you created credentials over a year ago, verify the app still exists on
> that page — old unused apps keep working, but double-check.

## 🔑 2. (Optional) Get a Groq API key

1. Go to https://console.groq.com/keys
2. Create an API key (free tier is enough — sentiment is only run on actual
   brand mentions, which are rare)

## 🚂 3. Deploy on Railway

1. Push this repository to GitHub (main branch)
2. In Railway: **New Project → Deploy from GitHub repo** → select this repo
3. Railway auto-detects Python, installs `requirements.txt`, and runs the
   `Procfile` (`web: python all_in_one_reddit_monitor.py`)

## 💾 4. Add a persistent volume (IMPORTANT)

Without a volume the SQLite database is wiped on every redeploy.

1. In your Railway project, right-click the service → **Attach Volume**
   (or service → Settings → Volumes)
2. Mount path: `/app/data`

The app stores its database at `/app/data/reddit_monitor.db` by default
(override with the `DATABASE_PATH` variable if needed).

## ⚙️ 5. Set environment variables

Service → **Variables** tab:

Required:
```
REDDIT_CLIENT_ID=your_reddit_client_id
REDDIT_CLIENT_SECRET=your_reddit_client_secret
```

Optional:
```
GROQ_API_TOKEN=your_groq_api_key      # enables sentiment analysis
REDDIT_USER_AGENT=python:brand-mention-monitor:v3.0 (by /u/YOUR_REDDIT_USERNAME)
```

Do **not** set `PORT` — Railway sets it automatically.

Setting `REDDIT_USER_AGENT` with your real Reddit username is recommended:
Reddit's API rules ask for an identifying user agent.

## 🌐 6. Expose the app

Service → Settings → **Networking → Generate Domain**. Visit the URL — the
dashboard should load. Health check: `https://<your-domain>/health`
(Railway also polls this automatically, configured in `railway.toml`).

## ✅ 7. Verify it's working

- `/health` → `"monitoring": true`
- `/system-health` → live worker status: comments/posts streamed counters
  should climb within a minute or two, `threads_alive` all true
- `/test-groq` → checks the sentiment API (if configured)
- Railway logs → look for `🎯 comment stream connected (r/all)` and the
  periodic `💓 Heartbeat` lines

## 🧠 How coverage works (why mentions aren't missed)

1. Streams `r/all` comments **and** posts in real time via the official API
2. Reddit IDs are sequential, so any ID the stream skips (rate limits,
   restarts, errors, subreddits excluded from r/all) is detected as a gap
   and fetched explicitly via `/api/info` in batches of 100
3. The last processed IDs persist in the database, so downtime is backfilled
   on restart (up to ~1 hour of all-of-Reddit comments by default; tune with
   `GAP_LIMIT_COMMENTS` / `GAP_LIMIT_POSTS`)
4. Every 30 minutes a Reddit search sweep per brand catches any post that
   still slipped through (`SWEEP_INTERVAL_SECONDS` to tune)

## 🔧 Customization

Edit `CONFIG` in `all_in_one_reddit_monitor.py`:

```python
'brands': {
    'badinka': r'(?<![a-z0-9])[@#]?badinka(?:\.com)?',
    'candy catz': r'(?<![a-z0-9])[@#]?candy\s*catz(?:\.com)?',
},
'search_terms': {
    'badinka': ['badinka'],
    'candy catz': ['"candy catz"', 'candycatz'],
},
```

Push to GitHub and Railway redeploys automatically.

## 🆘 Troubleshooting

- **401 in logs** → wrong/revoked Reddit credentials; recreate the script app
- **`database is locked`** → shouldn't happen anymore (WAL mode), but check
  that only one instance/replica of the service is running
- **No mentions appearing** → mentions of niche brands are genuinely rare;
  check `/system-health` counters are climbing, and try
  `/backfill/<subreddit>` to scan a specific subreddit on demand
- **Data lost after redeploy** → the volume isn't attached at `/app/data`

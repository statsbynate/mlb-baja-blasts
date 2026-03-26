# MLB HR Tracker — 420 ft or More, 2026

Live tracker pulling directly from Baseball Savant (Statcast) via CSV export.

---

## How it works

- **Backend** (`app.py`): A Python/Flask server that fetches Baseball Savant's CSV export, filters for 420+ ft home runs, and serves the data as JSON. Results are cached for 5 minutes so you're not hammering Savant on every click.
- **Frontend** (`index.html`): A standalone HTML file you can host anywhere (GitHub Pages, Netlify, etc.) that calls your backend and displays the data.

---

## Step 1 — Deploy the backend to Render (free)

1. Push this folder to a GitHub repository (public or private)
2. Go to [render.com](https://render.com) and sign up with your GitHub account
3. Click **New → Web Service**
4. Connect your GitHub repo
5. Render will auto-detect `render.yaml` — just click **Deploy**
6. Wait ~2 minutes for the first deploy to finish
7. Copy your Render URL — it looks like `https://mlb-hr-tracker.onrender.com`

> **Free tier note**: Render's free tier spins down after 15 minutes of inactivity. The first request after a sleep takes ~30 seconds to wake up. Upgrade to the $7/month Starter plan for always-on.

---

## Step 2 — Host the frontend on GitHub Pages (free)

1. In your GitHub repo, go to **Settings → Pages**
2. Set source to **main branch / root**
3. Your tracker will be live at `https://yourusername.github.io/mlb-hr-tracker`

Or just open `index.html` locally in any browser — it works the same way.

---

## Step 3 — Connect frontend to backend

When you first open the tracker, paste your Render URL into the setup field and click **Save & load data**. The URL is saved in your browser so you only do this once.

---

## For the 2027 season

In `app.py`, find this line and update the year:

```python
"&hfSea=2026%7C"
```

Change `2026` to `2027`. Redeploy to Render (just push to GitHub — it auto-deploys).

In `index.html`, update the heading and stat card text from `2026` to `2027`.

---

## Files

| File | Purpose |
|------|---------|
| `app.py` | Python/Flask backend — fetches Savant CSV, serves JSON API |
| `requirements.txt` | Python dependencies |
| `render.yaml` | Render deployment config |
| `index.html` | Frontend tracker — host on GitHub Pages or anywhere |

---

## API endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /api/homeruns` | Returns all 420+ ft HRs as JSON |
| `GET /health` | Health check — returns `{"status": "ok"}` |

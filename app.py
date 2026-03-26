import os
import csv
import io
import time
import logging
from flask import Flask, jsonify
from flask_cors import CORS
import requests

app = Flask(__name__)
CORS(app)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Simple in-memory cache: (data, timestamp)
_cache = {"data": None, "ts": 0}
CACHE_TTL = 300  # seconds (5 minutes)

SAVANT_URL = (
    "https://baseballsavant.mlb.com/statcast_search/csv"
    "?type=batter"
    "&hfAB=home__run%7C"
    "&hfGT=R%7C"
    "&hfSea=2026%7C"
    "&player_type=batter"
    "&min_pitches=0"
    "&min_results=0"
    "&group_by=name-event"
    "&sort_col=hit_distance_sc"
    "&sort_order=desc"
    "&min_abs=0"
    "&type=details"
)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer": "https://baseballsavant.mlb.com/",
}

MIN_DISTANCE = 420


def fetch_savant_data():
    """Fetch and parse the Baseball Savant CSV, filtering for 420+ ft HRs."""
    logger.info("Fetching data from Baseball Savant...")
    resp = requests.get(SAVANT_URL, headers=HEADERS, timeout=30)
    resp.raise_for_status()

    reader = csv.DictReader(io.StringIO(resp.text))
    results = []

    for row in reader:
        try:
            dist_raw = row.get("hit_distance_sc", "").strip()
            if not dist_raw:
                continue
            dist = float(dist_raw)
            if dist < MIN_DISTANCE:
                continue

            ev_raw = row.get("launch_speed", "").strip()
            ev = round(float(ev_raw), 1) if ev_raw else None

            la_raw = row.get("launch_angle", "").strip()
            la = round(float(la_raw), 1) if la_raw else None

            results.append({
                "player": row.get("player_name", "Unknown").strip(),
                "team": row.get("home_team", row.get("away_team", "—")).strip(),
                "opponent": row.get("away_team", row.get("home_team", "—")).strip(),
                "distance": int(dist),
                "exit_velocity": ev,
                "launch_angle": la,
                "date": row.get("game_date", "").strip(),
                "inning": row.get("inning", "").strip(),
                "pitch_type": row.get("pitch_type", "").strip(),
                "pitcher": row.get("pitcher_name", row.get("player_name", "")).strip(),
            })
        except (ValueError, KeyError):
            continue

    # Sort longest first
    results.sort(key=lambda x: x["distance"], reverse=True)
    logger.info(f"Found {len(results)} HRs of {MIN_DISTANCE}+ ft")
    return results


@app.route("/api/homeruns")
def homeruns():
    global _cache
    now = time.time()

    if _cache["data"] is not None and (now - _cache["ts"]) < CACHE_TTL:
        logger.info("Serving cached data")
        return jsonify({
            "homeruns": _cache["data"],
            "count": len(_cache["data"]),
            "cached": True,
            "cache_age_seconds": int(now - _cache["ts"]),
        })

    try:
        data = fetch_savant_data()
        _cache = {"data": data, "ts": now}
        return jsonify({
            "homeruns": data,
            "count": len(data),
            "cached": False,
            "cache_age_seconds": 0,
        })
    except Exception as e:
        logger.error(f"Error fetching data: {e}")
        # Return stale cache if available
        if _cache["data"] is not None:
            return jsonify({
                "homeruns": _cache["data"],
                "count": len(_cache["data"]),
                "cached": True,
                "error": str(e),
                "cache_age_seconds": int(now - _cache["ts"]),
            })
        return jsonify({"error": str(e), "homeruns": [], "count": 0}), 500


@app.route("/health")
def health():
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)

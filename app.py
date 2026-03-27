import os
import csv
import io
import time
import logging
from flask import Flask, jsonify, request
from flask_cors import CORS
import requests

app = Flask(__name__)
CORS(app)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

_cache = {"data": None, "ts": 0}
CACHE_TTL = 300

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


def build_savant_url(season="2026"):
    return (
        "https://baseballsavant.mlb.com/statcast_search/csv"
        "?type=batter"
        "&hfAB=home__run%7C"
        "&hfGT=R%7C"
        f"&hfSea={season}%7C"
        "&player_type=batter"
        "&min_pitches=0"
        "&min_results=0"
        "&group_by=name-event"
        "&sort_col=hit_distance_sc"
        "&sort_order=desc"
        "&min_abs=0"
        "&type=details"
    )


def fetch_savant_data(season="2026"):
    url = build_savant_url(season)
    logger.info(f"Fetching: {url}")
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()

    text = resp.text
    logger.info(f"Response length: {len(text)} chars")
    logger.info(f"First 200 chars: {text[:200]}")

    reader = csv.DictReader(io.StringIO(text))
    all_rows = []
    filtered = []

    for row in reader:
        all_rows.append(row)
        try:
            dist_raw = row.get("hit_distance_sc", "").strip()
            if not dist_raw:
                continue
            dist = float(dist_raw)

            ev_raw = row.get("launch_speed", "").strip()
            ev = round(float(ev_raw), 1) if ev_raw else None

            la_raw = row.get("launch_angle", "").strip()
            la = round(float(la_raw), 1) if la_raw else None

            entry = {
                "player": row.get("player_name", "Unknown").strip(),
                "team": row.get("home_team", row.get("away_team", "—")).strip(),
                "opponent": row.get("away_team", row.get("home_team", "—")).strip(),
                "distance": int(dist),
                "exit_velocity": ev,
                "launch_angle": la,
                "date": row.get("game_date", "").strip(),
                "inning": row.get("inning", "").strip(),
                "pitch_type": row.get("pitch_type", "").strip(),
            }

            if dist >= MIN_DISTANCE:
                filtered.append(entry)

        except (ValueError, KeyError):
            continue

    logger.info(f"Total rows: {len(all_rows)}, 420+ ft: {len(filtered)}")
    filtered.sort(key=lambda x: x["distance"], reverse=True)
    return filtered, len(all_rows)


@app.route("/api/homeruns")
def homeruns():
    global _cache
    now = time.time()
    season = request.args.get("season", "2026")

    if _cache["data"] is not None and (now - _cache["ts"]) < CACHE_TTL:
        logger.info("Serving cached data")
        return jsonify({
            "homeruns": _cache["data"],
            "count": len(_cache["data"]),
            "cached": True,
            "cache_age_seconds": int(now - _cache["ts"]),
        })

    try:
        data, total_rows = fetch_savant_data(season)
        _cache = {"data": data, "ts": now}
        return jsonify({
            "homeruns": data,
            "count": len(data),
            "total_hrs_from_savant": total_rows,
            "cached": False,
            "cache_age_seconds": 0,
        })
    except Exception as e:
        logger.error(f"Error: {e}")
        if _cache["data"] is not None:
            return jsonify({
                "homeruns": _cache["data"],
                "count": len(_cache["data"]),
                "cached": True,
                "error": str(e),
                "cache_age_seconds": int(now - _cache["ts"]),
            })
        return jsonify({"error": str(e), "homeruns": [], "count": 0}), 500


@app.route("/api/debug")
def debug():
    """Shows what Baseball Savant is actually returning — useful for troubleshooting."""
    try:
        url = build_savant_url("2026")
        resp = requests.get(url, headers=HEADERS, timeout=30)
        text = resp.text
        lines = text.strip().split("\n")
        return jsonify({
            "status_code": resp.status_code,
            "response_length": len(text),
            "line_count": len(lines),
            "first_line": lines[0] if lines else "",
            "second_line": lines[1] if len(lines) > 1 else "",
            "url_fetched": url,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/health")
def health():
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)

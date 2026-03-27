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

_cache_mlb = {"data": None, "ts": 0}
CACHE_TTL = 300

SEASON = "2026"
MIN_DISTANCE = 420

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.5",
}


def fetch_mlb_homeruns(season=SEASON):
    schedule_url = (
        f"https://statsapi.mlb.com/api/v1/schedule"
        f"?sportId=1&season={season}&gameType=R"
        f"&fields=dates,games,gamePk,gameDate,teams,home,away,team,name,abbreviation,status,abstractGameState"
    )
    logger.info(f"Fetching schedule: {schedule_url}")
    sched_resp = requests.get(schedule_url, headers=HEADERS, timeout=30)
    sched_resp.raise_for_status()
    sched_data = sched_resp.json()

    game_pks = []
    for date_entry in sched_data.get("dates", []):
        game_date = date_entry.get("date", "")
        for game in date_entry.get("games", []):
            state = game.get("status", {}).get("abstractGameState", "")
            if state != "Final":
                continue
            game_pks.append({
                "gamePk": game["gamePk"],
                "gameDate": game_date,
                "home": game["teams"]["home"]["team"]["abbreviation"],
                "away": game["teams"]["away"]["team"]["abbreviation"],
            })

    logger.info(f"Found {len(game_pks)} final games in {season}")
    if not game_pks:
        return []

    all_hrs = []
    for game in game_pks:
        try:
            feed_url = (
                f"https://statsapi.mlb.com/api/v1.1/game/{game['gamePk']}/feed/live"
            )
            resp = requests.get(feed_url, headers=HEADERS, timeout=20)
            if resp.status_code != 200:
                continue
            feed = resp.json()

            plays = feed.get("liveData", {}).get("plays", {}).get("allPlays", [])
            for play in plays:
                result = play.get("result", {})
                if result.get("event", "").lower() != "home run":
                    continue

                hit_data = play.get("hitData", {})
                distance = hit_data.get("totalDistance")
                launch_speed = hit_data.get("launchSpeed")
                launch_angle = hit_data.get("launchAngle")

                matchup = play.get("matchup", {})
                batter_name = matchup.get("batter", {}).get("fullName", "Unknown")

                about = play.get("about", {})
                inning = about.get("inning", "")
                half = about.get("halfInning", "")

                if half == "top":
                    team = game["away"]
                    opponent = game["home"]
                else:
                    team = game["home"]
                    opponent = game["away"]

                all_hrs.append({
                    "player": batter_name,
                    "team": team,
                    "opponent": opponent,
                    "distance": int(distance) if distance else None,
                    "exit_velocity": round(float(launch_speed), 1) if launch_speed else None,
                    "launch_angle": round(float(launch_angle), 1) if launch_angle else None,
                    "date": game.get("gameDate", ""),
                    "inning": str(inning),
                    "game_pk": str(game["gamePk"]),
                    "source": "MLB Stats API",
                })

        except Exception as e:
            logger.warning(f"Error fetching game {game['gamePk']}: {e}")
            continue

    logger.info(f"Total HRs from MLB API: {len(all_hrs)}")
    return all_hrs


def fetch_savant_distances(season=SEASON):
    url = (
        "https://baseballsavant.mlb.com/statcast_search/csv"
        "?type=batter"
        "&hfAB=home__run%7C"
        "&hfGT=R%7C"
        f"&hfSea={season}%7C"
        "&player_type=batter"
        "&min_pitches=0&min_results=0"
        "&group_by=name-event"
        "&sort_col=hit_distance_sc"
        "&sort_order=desc"
        "&min_abs=0&type=details"
    )
    savant_headers = {**HEADERS, "Referer": "https://baseballsavant.mlb.com/"}
    resp = requests.get(url, headers=savant_headers, timeout=30)
    resp.raise_for_status()

    reader = csv.DictReader(io.StringIO(resp.text))
    lookup = {}
    for row in reader:
        try:
            dist_raw = row.get("hit_distance_sc", "").strip()
            if not dist_raw:
                continue
            dist = int(float(dist_raw))
            name = row.get("player_name", "").strip()
            date = row.get("game_date", "").strip()
            ev_raw = row.get("launch_speed", "").strip()
            ev = round(float(ev_raw), 1) if ev_raw else None
            la_raw = row.get("launch_angle", "").strip()
            la = round(float(la_raw), 1) if la_raw else None
            key = (name, date)
            if key not in lookup:
                lookup[key] = {"distance": dist, "exit_velocity": ev, "launch_angle": la}
        except (ValueError, KeyError):
            continue

    logger.info(f"Savant lookup entries: {len(lookup)}")
    return lookup


def fetch_all_homeruns(season=SEASON):
    mlb_hrs = fetch_mlb_homeruns(season)

    savant_lookup = {}
    try:
        savant_lookup = fetch_savant_distances(season)
    except Exception as e:
        logger.warning(f"Savant enrichment failed (non-fatal): {e}")

    results = []
    for hr in mlb_hrs:
        key = (hr["player"], hr.get("date", ""))
        savant = savant_lookup.get(key)
        if savant:
            hr["distance"] = savant["distance"]
            hr["exit_velocity"] = savant["exit_velocity"] or hr["exit_velocity"]
            hr["launch_angle"] = savant["launch_angle"] or hr["launch_angle"]
            hr["source"] = "Baseball Savant"

        dist = hr.get("distance")
        if dist and dist >= MIN_DISTANCE:
            results.append(hr)
        elif not dist:
            hr["source"] = "MLB Stats API (distance pending)"
            results.append(hr)

    known = [h for h in results if h.get("distance") and h["distance"] >= MIN_DISTANCE]
    unknown = [h for h in results if not h.get("distance")]
    known.sort(key=lambda x: x["distance"], reverse=True)

    return known + unknown


@app.route("/api/homeruns")
def homeruns():
    global _cache_mlb
    now = time.time()

    if _cache_mlb["data"] is not None and (now - _cache_mlb["ts"]) < CACHE_TTL:
        return jsonify({
            "homeruns": _cache_mlb["data"],
            "count": len(_cache_mlb["data"]),
            "cached": True,
            "cache_age_seconds": int(now - _cache_mlb["ts"]),
        })

    try:
        data = fetch_all_homeruns()
        _cache_mlb = {"data": data, "ts": now}
        return jsonify({
            "homeruns": data,
            "count": len(data),
            "cached": False,
            "cache_age_seconds": 0,
        })
    except Exception as e:
        logger.error(f"Error: {e}")
        if _cache_mlb["data"] is not None:
            return jsonify({
                "homeruns": _cache_mlb["data"],
                "count": len(_cache_mlb["data"]),
                "cached": True,
                "error": str(e),
                "cache_age_seconds": int(now - _cache_mlb["ts"]),
            })
        return jsonify({"error": str(e), "homeruns": [], "count": 0}), 500


@app.route("/api/debug")
def debug():
    result = {}

    try:
        url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&season={SEASON}&gameType=R&fields=dates,games,gamePk,gameDate,status,abstractGameState,teams,home,away,team,abbreviation"
        resp = requests.get(url, headers=HEADERS, timeout=15)
        data = resp.json()
        total_games = sum(len(d.get("games", [])) for d in data.get("dates", []))
        final_games = sum(
            1 for d in data.get("dates", [])
            for g in d.get("games", [])
            if g.get("status", {}).get("abstractGameState") == "Final"
        )
        result["mlb_api"] = {
            "status": resp.status_code,
            "total_games_in_schedule": total_games,
            "final_games": final_games,
        }
    except Exception as e:
        result["mlb_api"] = {"error": str(e)}

    try:
        savant_url = (
            "https://baseballsavant.mlb.com/statcast_search/csv"
            f"?type=batter&hfAB=home__run%7C&hfGT=R%7C&hfSea={SEASON}%7C"
            "&player_type=batter&min_pitches=0&min_results=0"
            "&group_by=name-event&sort_col=hit_distance_sc"
            "&sort_order=desc&min_abs=0&type=details"
        )
        savant_headers = {**HEADERS, "Referer": "https://baseballsavant.mlb.com/"}
        resp = requests.get(savant_url, headers=savant_headers, timeout=15)
        lines = resp.text.strip().split("\n")
        result["savant"] = {
            "status": resp.status_code,
            "line_count": len(lines),
            "has_data": len(lines) > 1,
        }
    except Exception as e:
        result["savant"] = {"error": str(e)}

    return jsonify(result)


@app.route("/health")
def health():
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)

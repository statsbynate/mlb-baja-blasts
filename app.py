import os
import csv
import io
import time
import logging
from datetime import datetime
from flask import Flask, jsonify
from flask_cors import CORS
import requests

app = Flask(__name__)
CORS(app)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

_cache_mlb = {"data": None, "ts": 0}
CACHE_TTL = 300

SEASON = "2026"
SEASON_START = "2026-03-26"
MIN_DISTANCE = 420

MLB_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}

SAVANT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/javascript, */*",
    "Referer": "https://baseballsavant.mlb.com/",
}

TEAM_ABBREVS = {
    "Arizona Diamondbacks": "ARI", "Atlanta Braves": "ATL",
    "Baltimore Orioles": "BAL", "Boston Red Sox": "BOS",
    "Chicago Cubs": "CHC", "Chicago White Sox": "CWS",
    "Cincinnati Reds": "CIN", "Cleveland Guardians": "CLE",
    "Colorado Rockies": "COL", "Detroit Tigers": "DET",
    "Houston Astros": "HOU", "Kansas City Royals": "KC",
    "Los Angeles Angels": "LAA", "Los Angeles Dodgers": "LAD",
    "Miami Marlins": "MIA", "Milwaukee Brewers": "MIL",
    "Minnesota Twins": "MIN", "New York Mets": "NYM",
    "New York Yankees": "NYY", "Oakland Athletics": "OAK",
    "Philadelphia Phillies": "PHI", "Pittsburgh Pirates": "PIT",
    "San Diego Padres": "SD", "San Francisco Giants": "SF",
    "Seattle Mariners": "SEA", "St. Louis Cardinals": "STL",
    "Tampa Bay Rays": "TB", "Texas Rangers": "TEX",
    "Toronto Blue Jays": "TOR", "Washington Nationals": "WSH",
    "Athletics": "OAK",
}


def safe_get(d, *keys, default=""):
    for key in keys:
        if not isinstance(d, dict):
            return default
        d = d.get(key, default)
    return d if d != "" else default


def team_abbrev(team_dict):
    if not isinstance(team_dict, dict):
        return "—"
    abbr = team_dict.get("abbreviation", "")
    if abbr:
        return abbr
    name = team_dict.get("name", "")
    return TEAM_ABBREVS.get(name, name[:3].upper() if name else "—")


# ---------------------------------------------------------------------------
# Source 1: MLB Stats API — schedule + play-by-play
# ---------------------------------------------------------------------------

def fetch_final_games(season=SEASON):
    """Return list of final game dicts with gamePk, gameDate, home, away."""
    url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&season={season}&gameType=R"
    resp = requests.get(url, headers=MLB_HEADERS, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    games = []
    for date_entry in data.get("dates", []):
        game_date = date_entry.get("date", "")
        for game in date_entry.get("games", []):
            state = safe_get(game, "status", "abstractGameState")
            if state != "Final":
                continue
            home_dict = safe_get(game, "teams", "home", "team", default={})
            away_dict = safe_get(game, "teams", "away", "team", default={})
            games.append({
                "gamePk": str(game["gamePk"]),
                "gameDate": game_date,
                "home": team_abbrev(home_dict),
                "away": team_abbrev(away_dict),
            })

    logger.info(f"Found {len(games)} final games")
    return games


def fetch_mlb_homeruns_for_game(game):
    """Pull home runs from MLB live feed for one game."""
    url = f"https://statsapi.mlb.com/api/v1.1/game/{game['gamePk']}/feed/live"
    resp = requests.get(url, headers=MLB_HEADERS, timeout=20)
    if resp.status_code != 200:
        return []

    feed = resp.json()
    plays = feed.get("liveData", {}).get("plays", {}).get("allPlays", [])
    hrs = []
    for play in plays:
        event = safe_get(play, "result", "event")
        if event.lower() != "home run":
            continue

        hit_data = play.get("hitData", {})
        distance = hit_data.get("totalDistance")
        launch_speed = hit_data.get("launchSpeed")
        launch_angle = hit_data.get("launchAngle")
        batter_name = safe_get(play, "matchup", "batter", "fullName") or "Unknown"
        batter_id = safe_get(play, "matchup", "batter", "id")
        inning = safe_get(play, "about", "inning")
        half = safe_get(play, "about", "halfInning")

        team = game["away"] if half == "top" else game["home"]
        opponent = game["home"] if half == "top" else game["away"]

        hrs.append({
            "player": batter_name,
            "batter_id": str(batter_id),
            "team": team,
            "opponent": opponent,
            "distance": int(distance) if distance is not None else None,
            "exit_velocity": round(float(launch_speed), 1) if launch_speed is not None else None,
            "launch_angle": round(float(launch_angle), 1) if launch_angle is not None else None,
            "date": game["gameDate"],
            "inning": str(inning),
            "game_pk": game["gamePk"],
            "source": "MLB Stats API",
        })
    return hrs


# ---------------------------------------------------------------------------
# Source 2: Savant Game Feed — distance enrichment per game
# ---------------------------------------------------------------------------

def fetch_savant_game_feed(game_pk):
    """
    Pull Statcast data from Savant's game feed endpoint.
    Returns dict keyed by (player_name, inning) -> {distance, exit_velocity, launch_angle}
    """
    url = f"https://baseballsavant.mlb.com/gf?game_pk={game_pk}"
    try:
        resp = requests.get(url, headers=SAVANT_HEADERS, timeout=15)
        if resp.status_code != 200:
            logger.warning(f"Savant game feed {game_pk}: status {resp.status_code}")
            return {}

        data = resp.json()
        lookup = {}

        # Game feed has home/away team data
        for side in ["home", "away"]:
            team_data = data.get(f"team_{side}", {})
            for player_id, events in team_data.items():
                if not isinstance(events, list):
                    continue
                for event in events:
                    if not isinstance(event, dict):
                        continue
                    result = event.get("result", "")
                    if "home_run" not in str(result).lower() and "home run" not in str(result).lower():
                        # also check play_id or type fields
                        if event.get("type") != "X" and "home_run" not in str(event.get("pitch_type", "")):
                            if str(event.get("events", "")).lower() != "home_run":
                                continue

                    dist = event.get("hit_distance_sc") or event.get("hit_distance")
                    ev = event.get("launch_speed")
                    la = event.get("launch_angle")
                    name = event.get("batter_name", "")
                    inning = str(event.get("inning", ""))

                    if dist:
                        key = (name, inning)
                        lookup[key] = {
                            "distance": int(float(dist)),
                            "exit_velocity": round(float(ev), 1) if ev else None,
                            "launch_angle": round(float(la), 1) if la else None,
                        }

        logger.info(f"Savant game feed {game_pk}: {len(lookup)} HR entries")
        return lookup

    except Exception as e:
        logger.warning(f"Savant game feed {game_pk} error: {e}")
        return {}


# ---------------------------------------------------------------------------
# Combine everything
# ---------------------------------------------------------------------------

def fetch_all_homeruns(season=SEASON):
    games = fetch_final_games(season)
    if not games:
        return []

    all_hrs = []
    for game in games:
        try:
            # Get HR list from MLB API
            hrs = fetch_mlb_homeruns_for_game(game)
            if not hrs:
                continue

            # Try to enrich with Savant game feed distance
            savant_lookup = fetch_savant_game_feed(game["gamePk"])

            for hr in hrs:
                if savant_lookup:
                    key = (hr["player"], hr["inning"])
                    enriched = savant_lookup.get(key)
                    if enriched and enriched.get("distance"):
                        hr["distance"] = enriched["distance"]
                        hr["exit_velocity"] = enriched.get("exit_velocity") or hr["exit_velocity"]
                        hr["launch_angle"] = enriched.get("launch_angle") or hr["launch_angle"]
                        hr["source"] = "Statcast (game feed)"

                all_hrs.append(hr)

        except Exception as e:
            logger.warning(f"Error processing game {game['gamePk']}: {e}")
            continue

    logger.info(f"Total HRs collected: {len(all_hrs)}")

    # Split into known distance (420+) and pending
    known = [h for h in all_hrs if h.get("distance") and h["distance"] >= MIN_DISTANCE]
    unknown = [h for h in all_hrs if not h.get("distance")]

    known.sort(key=lambda x: x["distance"], reverse=True)
    return known + unknown


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

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

    # MLB API check
    try:
        url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&season={SEASON}&gameType=R"
        resp = requests.get(url, headers=MLB_HEADERS, timeout=15)
        data = resp.json()
        total = sum(len(d.get("games", [])) for d in data.get("dates", []))
        final = sum(
            1 for d in data.get("dates", [])
            for g in d.get("games", [])
            if safe_get(g, "status", "abstractGameState") == "Final"
        )
        # Get first final game pk for savant test
        first_pk = None
        for d in data.get("dates", []):
            for g in d.get("games", []):
                if safe_get(g, "status", "abstractGameState") == "Final":
                    first_pk = str(g["gamePk"])
                    break
            if first_pk:
                break

        result["mlb_api"] = {
            "status": resp.status_code,
            "total_games": total,
            "final_games": final,
            "sample_game_pk": first_pk,
        }
    except Exception as e:
        result["mlb_api"] = {"error": str(e)}

    # Savant game feed check using first final game
    first_pk = result.get("mlb_api", {}).get("sample_game_pk")
    if first_pk:
        try:
            url = f"https://baseballsavant.mlb.com/gf?game_pk={first_pk}"
            resp = requests.get(url, headers=SAVANT_HEADERS, timeout=15)
            body = resp.text[:500] if resp.status_code == 200 else ""
            keys = list(resp.json().keys()) if resp.status_code == 200 else []
            result["savant_game_feed"] = {
                "status": resp.status_code,
                "game_pk": first_pk,
                "top_level_keys": keys,
                "preview": body[:300],
            }
        except Exception as e:
            result["savant_game_feed"] = {"error": str(e)}

    # Savant CSV check
    try:
        csv_url = (
            "https://baseballsavant.mlb.com/statcast_search/csv"
            f"?type=batter&hfAB=home__run%7C&hfGT=R%7C&hfSea={SEASON}%7C"
            "&player_type=batter&min_pitches=0&min_results=0"
            "&group_by=name-event&sort_col=hit_distance_sc"
            "&sort_order=desc&min_abs=0&type=details"
        )
        resp = requests.get(csv_url, headers={**SAVANT_HEADERS, "Accept": "text/csv"}, timeout=15)
        raw = resp.content.decode("utf-8-sig", errors="replace")
        lines = raw.strip().split("\n")
        result["savant_csv"] = {
            "status": resp.status_code,
            "line_count": len(lines),
            "has_data": len(lines) > 1,
        }
    except Exception as e:
        result["savant_csv"] = {"error": str(e)}

    return jsonify(result)


@app.route("/health")
def health():
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)

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

_cache = {"data": None, "ts": 0}
CACHE_TTL = 300

SEASON = "2026"
MIN_DISTANCE = 420

MLB_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/123.0.0.0 Safari/537.36",
    "Accept": "application/json",
}

SAVANT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/123.0.0.0 Safari/537.36",
    "Accept": "text/csv, application/json, */*",
    "Accept-Encoding": "identity",
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
# MLB Stats API — schedule + play-by-play
# ---------------------------------------------------------------------------

def fetch_final_games(season=SEASON):
    url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&season={season}&gameType=R"
    resp = requests.get(url, headers=MLB_HEADERS, timeout=30)
    resp.raise_for_status()
    games = []
    for date_entry in resp.json().get("dates", []):
        game_date = date_entry.get("date", "")
        for game in date_entry.get("games", []):
            if safe_get(game, "status", "abstractGameState") != "Final":
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


def fetch_homeruns_for_game(game):
    url = f"https://statsapi.mlb.com/api/v1.1/game/{game['gamePk']}/feed/live"
    resp = requests.get(url, headers=MLB_HEADERS, timeout=20)
    if resp.status_code != 200:
        return []
    hrs = []
    plays = resp.json().get("liveData", {}).get("plays", {}).get("allPlays", [])
    for play in plays:
        if safe_get(play, "result", "event").lower() != "home run":
            continue
        hit = play.get("hitData", {})
        distance = hit.get("totalDistance")
        ev = hit.get("launchSpeed")
        la = hit.get("launchAngle")
        batter = safe_get(play, "matchup", "batter", "fullName") or "Unknown"
        inning = str(safe_get(play, "about", "inning"))
        half = safe_get(play, "about", "halfInning")
        team = game["away"] if half == "top" else game["home"]
        opponent = game["home"] if half == "top" else game["away"]
        hrs.append({
            "player": batter,
            "team": team,
            "opponent": opponent,
            "distance": int(distance) if distance is not None else None,
            "exit_velocity": round(float(ev), 1) if ev is not None else None,
            "launch_angle": round(float(la), 1) if la is not None else None,
            "date": game["gameDate"],
            "inning": inning,
            "game_pk": game["gamePk"],
            "source": "MLB Stats API",
        })
    return hrs


# ---------------------------------------------------------------------------
# Baseball Savant CSV — distance enrichment (when available)
# ---------------------------------------------------------------------------

def fetch_savant_distances(season=SEASON):
    url = (
        "https://baseballsavant.mlb.com/statcast_search/csv"
        "?type=batter&hfAB=home__run%7C&hfGT=R%7C"
        f"&hfSea={season}%7C&player_type=batter"
        "&min_pitches=0&min_results=0&group_by=name-event"
        "&sort_col=hit_distance_sc&sort_order=desc&min_abs=0&type=details"
    )
    resp = requests.get(url, headers=SAVANT_HEADERS, timeout=30)
    resp.raise_for_status()
    raw = resp.content.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(raw))
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
            la_raw = row.get("launch_angle", "").strip()
            key = (name, date)
            if key not in lookup:
                lookup[key] = {
                    "distance": dist,
                    "exit_velocity": round(float(ev_raw), 1) if ev_raw else None,
                    "launch_angle": round(float(la_raw), 1) if la_raw else None,
                }
        except (ValueError, KeyError):
            continue
    logger.info(f"Savant: {len(lookup)} HR entries")
    return lookup


# ---------------------------------------------------------------------------
# Combined fetch
# ---------------------------------------------------------------------------

def fetch_all_homeruns(season=SEASON):
    games = fetch_final_games(season)
    if not games:
        return []

    all_hrs = []
    for game in games:
        try:
            hrs = fetch_homeruns_for_game(game)
            all_hrs.extend(hrs)
        except Exception as e:
            logger.warning(f"Game {game['gamePk']} error: {e}")

    logger.info(f"Total HRs from MLB API: {len(all_hrs)}")

    # Try to enrich with Savant distance data
    savant_lookup = {}
    try:
        savant_lookup = fetch_savant_distances(season)
    except Exception as e:
        logger.warning(f"Savant fetch failed (non-fatal): {e}")

    results = []
    for hr in all_hrs:
        key = (hr["player"], hr.get("date", ""))
        enriched = savant_lookup.get(key)
        if enriched:
            hr["distance"] = enriched["distance"]
            hr["exit_velocity"] = enriched.get("exit_velocity") or hr["exit_velocity"]
            hr["launch_angle"] = enriched.get("launch_angle") or hr["launch_angle"]
            hr["source"] = "Statcast"

        dist = hr.get("distance")
        if dist is not None and dist >= MIN_DISTANCE:
            results.append(hr)
        elif dist is None:
            hr["source"] = "MLB Stats API (distance pending)"
            results.append(hr)

    known = [h for h in results if h.get("distance") and h["distance"] >= MIN_DISTANCE]
    unknown = [h for h in results if not h.get("distance")]
    known.sort(key=lambda x: x["distance"], reverse=True)
    return known + unknown


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/api/homeruns")
def homeruns():
    global _cache
    now = time.time()
    if _cache["data"] is not None and (now - _cache["ts"]) < CACHE_TTL:
        return jsonify({
            "homeruns": _cache["data"],
            "count": len(_cache["data"]),
            "cached": True,
            "cache_age_seconds": int(now - _cache["ts"]),
        })
    try:
        data = fetch_all_homeruns()
        _cache = {"data": data, "ts": now}
        return jsonify({"homeruns": data, "count": len(data), "cached": False, "cache_age_seconds": 0})
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
    result = {}
    try:
        url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&season={SEASON}&gameType=R"
        resp = requests.get(url, headers=MLB_HEADERS, timeout=15)
        data = resp.json()
        total = sum(len(d.get("games", [])) for d in data.get("dates", []))
        final = sum(1 for d in data.get("dates", []) for g in d.get("games", []) if safe_get(g, "status", "abstractGameState") == "Final")
        result["mlb_api"] = {"status": resp.status_code, "total_games": total, "final_games": final}
    except Exception as e:
        result["mlb_api"] = {"error": str(e)}

    try:
        savant_url = (
            "https://baseballsavant.mlb.com/statcast_search/csv"
            f"?type=batter&hfAB=home__run%7C&hfGT=R%7C&hfSea={SEASON}%7C"
            "&player_type=batter&min_pitches=0&min_results=0"
            "&group_by=name-event&sort_col=hit_distance_sc&sort_order=desc&min_abs=0&type=details"
        )
        resp = requests.get(savant_url, headers=SAVANT_HEADERS, timeout=15)
        raw = resp.content.decode("utf-8-sig", errors="replace")
        lines = raw.strip().split("\n")
        result["savant"] = {
            "status": resp.status_code,
            "line_count": len(lines),
            "has_data": len(lines) > 1,
            "first_data_row": lines[1][:200] if len(lines) > 1 else "",
        }
    except Exception as e:
        result["savant"] = {"error": str(e)}

    return jsonify(result)



@app.route("/api/inspect")
def inspect():
    """Temporary: inspect the exit_velocity key and HR pitch fields in Savant game feed."""
    try:
        url = "https://baseballsavant.mlb.com/gf?game_pk=823649"
        resp = requests.get(url, headers=SAVANT_HEADERS, timeout=15)
        data = resp.json()

        # Check top-level exit_velocity key
        ev_top = data.get("exit_velocity")

        # Find first HR pitch and show ALL its fields
        hr_pitch = None
        for side in ["home_batters", "away_batters"]:
            batters = data.get(side, {})
            if not isinstance(batters, dict):
                continue
            for pid, pitches in batters.items():
                if not isinstance(pitches, list):
                    continue
                for pitch in pitches:
                    if isinstance(pitch, dict) and str(pitch.get("events", "")).lower() == "home run":
                        hr_pitch = pitch
                        break
                if hr_pitch:
                    break
            if hr_pitch:
                break

        return jsonify({
            "exit_velocity_top_level": ev_top,
            "hr_pitch_found": hr_pitch is not None,
            "hr_pitch_all_fields": hr_pitch if hr_pitch else {},
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/health")
def health():
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)

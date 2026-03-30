import os
import time
import logging
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
MIN_DISTANCE = 420

MLB_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/123.0.0.0 Safari/537.36",
    "Accept": "application/json",
}

SAVANT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/123.0.0.0 Safari/537.36",
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


def fetch_final_games(season=SEASON):
    url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&season={season}&gameType=R"
    resp = requests.get(url, headers=MLB_HEADERS, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    games = []
    for date_entry in data.get("dates", []):
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


def fetch_mlb_homeruns_for_game(game):
    url = f"https://statsapi.mlb.com/api/v1.1/game/{game['gamePk']}/feed/live"
    resp = requests.get(url, headers=MLB_HEADERS, timeout=20)
    if resp.status_code != 200:
        return []
    feed = resp.json()
    plays = feed.get("liveData", {}).get("plays", {}).get("allPlays", [])
    hrs = []
    for play in plays:
        if safe_get(play, "result", "event").lower() != "home run":
            continue
        hit_data = play.get("hitData", {})
        distance = hit_data.get("totalDistance")
        launch_speed = hit_data.get("launchSpeed")
        launch_angle = hit_data.get("launchAngle")
        batter_name = safe_get(play, "matchup", "batter", "fullName") or "Unknown"
        batter_id = str(safe_get(play, "matchup", "batter", "id"))
        inning = str(safe_get(play, "about", "inning"))
        half = safe_get(play, "about", "halfInning")
        team = game["away"] if half == "top" else game["home"]
        opponent = game["home"] if half == "top" else game["away"]
        hrs.append({
            "player": batter_name,
            "batter_id": batter_id,
            "team": team,
            "opponent": opponent,
            "distance": int(distance) if distance is not None else None,
            "exit_velocity": round(float(launch_speed), 1) if launch_speed is not None else None,
            "launch_angle": round(float(launch_angle), 1) if launch_angle is not None else None,
            "date": game["gameDate"],
            "inning": inning,
            "game_pk": game["gamePk"],
            "source": "MLB Stats API",
        })
    return hrs


def fetch_savant_game_feed(game_pk):
    """
    Parse Savant game feed pitch-by-pitch data.
    Each batter has a list of pitch dicts. The HOME RUN pitch is the one
    where events == 'home_run'. That pitch contains hit_distance_sc,
    launch_speed, launch_angle.
    Returns: {batter_id: [{distance, exit_velocity, launch_angle, inning}]}
    """
    url = f"https://baseballsavant.mlb.com/gf?game_pk={game_pk}"
    try:
        resp = requests.get(url, headers=SAVANT_HEADERS, timeout=15)
        if resp.status_code != 200:
            return {}
        data = resp.json()

        lookup = {}  # str(batter_id) -> list of HR pitch dicts

        for side in ["home_batters", "away_batters"]:
            batters = data.get(side, {})
            if not isinstance(batters, dict):
                continue
            for player_id, pitches in batters.items():
                if not isinstance(pitches, list):
                    continue
                for pitch in pitches:
                    if not isinstance(pitch, dict):
                        continue
                    # Home run pitch has events == "home_run"
                    events = str(pitch.get("events", "")).lower()
                    if events != "home_run":
                        continue

                    # Try all possible distance field names
                    dist = None
                    for field in ["hit_distance_sc", "hit_distance", "total_distance", "batted_distance"]:
                        val = pitch.get(field)
                        if val is not None and str(val) not in ("", "null", "nan"):
                            try:
                                dist = int(float(val))
                                break
                            except (ValueError, TypeError):
                                continue

                    ev = None
                    for field in ["launch_speed", "exit_velocity", "hit_speed"]:
                        val = pitch.get(field)
                        if val is not None and str(val) not in ("", "null", "nan"):
                            try:
                                ev = round(float(val), 1)
                                break
                            except (ValueError, TypeError):
                                continue

                    la = None
                    val = pitch.get("launch_angle")
                    if val is not None and str(val) not in ("", "null", "nan"):
                        try:
                            la = round(float(val), 1)
                        except (ValueError, TypeError):
                            pass

                    inning = str(pitch.get("inning", ""))

                    if player_id not in lookup:
                        lookup[player_id] = []
                    lookup[player_id].append({
                        "distance": dist,
                        "exit_velocity": ev,
                        "launch_angle": la,
                        "inning": inning,
                        "all_keys": list(pitch.keys()),  # for debugging
                    })

        hr_count = sum(len(v) for v in lookup.values())
        logger.info(f"Savant game feed {game_pk}: {hr_count} HR pitches for {len(lookup)} batters")
        return lookup

    except Exception as e:
        logger.warning(f"Savant game feed {game_pk} error: {e}")
        return {}


def fetch_all_homeruns(season=SEASON):
    games = fetch_final_games(season)
    if not games:
        return []

    all_hrs = []
    for game in games:
        try:
            hrs = fetch_mlb_homeruns_for_game(game)
            if not hrs:
                continue

            savant_lookup = fetch_savant_game_feed(game["gamePk"])

            for hr in hrs:
                bid = hr["batter_id"]
                if savant_lookup and bid in savant_lookup:
                    pas = savant_lookup[bid]
                    matched = next(
                        (p for p in pas if p["inning"] == hr["inning"]),
                        pas[0] if pas else None
                    )
                    if matched:
                        if matched.get("distance"):
                            hr["distance"] = matched["distance"]
                            hr["source"] = "Statcast (game feed)"
                        if matched.get("exit_velocity"):
                            hr["exit_velocity"] = matched["exit_velocity"]
                        if matched.get("launch_angle"):
                            hr["launch_angle"] = matched["launch_angle"]

                all_hrs.append(hr)

        except Exception as e:
            logger.warning(f"Error processing game {game['gamePk']}: {e}")
            continue

    logger.info(f"Total HRs: {len(all_hrs)}")
    known = [h for h in all_hrs if h.get("distance") and h["distance"] >= MIN_DISTANCE]
    unknown = [h for h in all_hrs if not h.get("distance")]
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
        return jsonify({"homeruns": data, "count": len(data), "cached": False, "cache_age_seconds": 0})
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

    # MLB API
    try:
        url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&season={SEASON}&gameType=R"
        resp = requests.get(url, headers=MLB_HEADERS, timeout=15)
        data = resp.json()
        total = sum(len(d.get("games", [])) for d in data.get("dates", []))
        final = sum(1 for d in data.get("dates", []) for g in d.get("games", []) if safe_get(g, "status", "abstractGameState") == "Final")
        # Get ALL final game pks so we can find one with HRs
        all_final_pks = [str(g["gamePk"]) for d in data.get("dates", []) for g in d.get("games", []) if safe_get(g, "status", "abstractGameState") == "Final"]
        first_pk = all_final_pks[0] if all_final_pks else None
        # Prefer games we know had HRs for better debug info
        known_hr_games = ["823649", "823812", "823163", "823486", "823081"]
        for pk in known_hr_games:
            if pk in all_final_pks:
                first_pk = pk
                break
        result["mlb_api"] = {"status": resp.status_code, "total_games": total, "final_games": final, "sample_game_pk": first_pk}
    except Exception as e:
        result["mlb_api"] = {"error": str(e)}

    # Savant game feed — find a home_run pitch and show its keys
    first_pk = result.get("mlb_api", {}).get("sample_game_pk")
    if first_pk:
        try:
            url = f"https://baseballsavant.mlb.com/gf?game_pk={first_pk}"
            resp = requests.get(url, headers=SAVANT_HEADERS, timeout=15)
            data = resp.json()

            # Find the first home_run pitch across all batters
            hr_pitch = None
            hr_player_id = None
            for side in ["home_batters", "away_batters"]:
                batters = data.get(side, {})
                if not isinstance(batters, dict):
                    continue
                for pid, pitches in batters.items():
                    if not isinstance(pitches, list):
                        continue
                    for pitch in pitches:
                        if isinstance(pitch, dict) and str(pitch.get("events", "")).lower() == "home_run":
                            hr_pitch = pitch
                            hr_player_id = pid
                            break
                    if hr_pitch:
                        break
                if hr_pitch:
                    break

            result["savant_game_feed"] = {
                "status": resp.status_code,
                "game_pk": first_pk,
                "hr_pitch_found": hr_pitch is not None,
                "hr_player_id": hr_player_id,
                "hr_pitch_keys": list(hr_pitch.keys()) if hr_pitch else [],
                "hr_pitch_data": hr_pitch if hr_pitch else {},
            }
        except Exception as e:
            result["savant_game_feed"] = {"error": str(e)}

    return jsonify(result)


@app.route("/health")
def health():
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)

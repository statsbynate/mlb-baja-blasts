import os
import csv
import io
import time
import logging
import traceback
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
    feed = resp.json()
    plays = feed.get("liveData", {}).get("plays", {}).get("allPlays", [])

    # Find the index of the last play to detect walk-offs
    all_play_indices = {p.get("about", {}).get("atBatIndex", -1): i for i, p in enumerate(plays)}
    last_idx = len(plays) - 1

    hrs = []
    for i, play in enumerate(plays):
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

        # Walk-off: HR is in bottom half, inning >= 9, and it's the last play of the game
        try:
            is_walkoff = (
                half == "bottom"
                and int(inning) >= 9
                and i == last_idx
            )
        except (ValueError, TypeError):
            is_walkoff = False

        # RBI count from description
        desc = safe_get(play, "result", "description", default="")
        rbi = play.get("result", {}).get("rbi", 0)

        hrs.append({
            "player": batter,
            "team": team,
            "opponent": opponent,
            "distance": int(distance) if distance is not None else None,
            "exit_velocity": round(float(ev), 1) if ev is not None else None,
            "launch_angle": round(float(la), 1) if la is not None else None,
            "date": game["gameDate"],
            "inning": inning,
            "inning_half": half,
            "rbi": rbi,
            "is_walkoff": is_walkoff,
            "game_pk": game["gamePk"],
            "play_id": "",
            "source": "MLB Stats API",
        })
    return hrs


# ---------------------------------------------------------------------------
# Baseball Savant CSV — distance enrichment (when available)
# ---------------------------------------------------------------------------

def fetch_savant_game_distances(game_pk):
    """
    Pull distance data from Savant game feed exit_velocity array.
    This array contains every batted ball with hit_distance, launch_speed,
    launch_angle, batter_name, and inning — available even when the bulk CSV is empty.
    Returns: {(batter_name, inning): {distance, exit_velocity, launch_angle}}
    """
    url = f"https://baseballsavant.mlb.com/gf?game_pk={game_pk}"
    try:
        resp = requests.get(url, headers=SAVANT_HEADERS, timeout=15)
        if resp.status_code != 200:
            return {}
        try:
            data = resp.json()
        except Exception:
            return {}

        # exit_velocity is top-level array of ALL batted balls with Statcast data
        ev_array = data.get("exit_velocity", [])
        if not isinstance(ev_array, list):
            return {}

        lookup = {}
        for play in ev_array:
            if not isinstance(play, dict):
                continue
            # Only home runs
            if str(play.get("events", "")).lower() != "home run":
                continue
            dist_raw = play.get("hit_distance")
            if not dist_raw:
                continue
            try:
                dist = int(float(str(dist_raw)))
                name = str(play.get("batter_name", "")).strip()
                inning = str(play.get("inning", ""))
                ev_raw = play.get("hit_speed") or play.get("launch_speed")
                la_raw = play.get("launch_angle") or play.get("hit_angle")
                play_id = str(play.get("play_id", "")).strip()
                key = (name, inning)
                if key not in lookup:
                    lookup[key] = {
                        "distance": dist,
                        "exit_velocity": round(float(str(ev_raw)), 1) if ev_raw else None,
                        "launch_angle": round(float(str(la_raw)), 1) if la_raw else None,
                        "play_id": play_id,
                    }
            except (ValueError, TypeError):
                continue

        logger.info(f"Savant game feed {game_pk}: {len(lookup)} HR distance entries")
        return lookup

    except Exception as e:
        logger.warning(f"Savant game feed {game_pk} error: {e}")
        return {}


def fetch_savant_distances(season=SEASON):
    """Bulk CSV fallback — used when game feed is unavailable."""
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
    logger.info(f"Savant CSV: {len(lookup)} HR entries")
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
            logger.warning(f"Game {game['gamePk']} MLB fetch error: {e}")

    logger.info(f"Total HRs from MLB API: {len(all_hrs)}")

    # Enrich with Savant game feed distances, with per-game error isolation
    game_feed_cache = {}
    for hr in all_hrs:
        gk = hr["game_pk"]
        if gk not in game_feed_cache:
            try:
                game_feed_cache[gk] = fetch_savant_game_distances(gk)
            except Exception as e:
                logger.warning(f"Savant feed {gk} error (skipping): {e}")
                game_feed_cache[gk] = {}

    results = []
    for hr in all_hrs:
        gk = hr["game_pk"]
        if gk not in game_feed_cache:
            game_feed_cache[gk] = {}

        game_lookup = game_feed_cache[gk]
        key = (hr["player"], hr["inning"])
        enriched = game_lookup.get(key)

        if enriched and enriched.get("distance"):
            hr["distance"] = enriched["distance"]
            hr["exit_velocity"] = enriched.get("exit_velocity") or hr["exit_velocity"]
            hr["launch_angle"] = enriched.get("launch_angle") or hr["launch_angle"]
            hr["play_id"] = enriched.get("play_id") or hr.get("play_id", "")
            hr["source"] = "Statcast (game feed)"
        else:
            hr["source"] = "MLB Stats API (distance pending)"

        # Always include every HR regardless of distance
        results.append(hr)

    # Sort: Baja Blasts (420+ ft) first, then sub-420 with distance, then pending
    baja = [h for h in results if h.get("distance") and h["distance"] >= MIN_DISTANCE]
    sub = [h for h in results if h.get("distance") and h["distance"] < MIN_DISTANCE]
    pending = [h for h in results if not h.get("distance")]
    baja.sort(key=lambda x: x["distance"], reverse=True)
    sub.sort(key=lambda x: x["distance"], reverse=True)
    return baja + sub + pending


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
        logger.error(traceback.format_exc())
        if _cache["data"] is not None:
            return jsonify({
                "homeruns": _cache["data"],
                "count": len(_cache["data"]),
                "cached": True,
                "error": str(e),
                "cache_age_seconds": int(now - _cache["ts"]),
            })
        return jsonify({"error": str(e), "homeruns": [], "count": 0}), 500

@app.route("/api/test")
def test():
    try:
        games = fetch_final_games()
        all_hrs = []
        failed_games = []
        for game in games:
            try:
                hrs = fetch_homeruns_for_game(game)
                all_hrs.extend(hrs)
            except Exception as e:
                failed_games.append({"gamePk": game["gamePk"], "error": str(e)})
        
        game_feed_cache = {}
        failed_savant = []
        results = []
        for hr in all_hrs:
            gk = hr["game_pk"]
            if gk not in game_feed_cache:
                try:
                    game_feed_cache[gk] = fetch_savant_game_distances(gk)
                except Exception as e:
                    failed_savant.append({"gamePk": gk, "error": str(e)})
                    game_feed_cache[gk] = {}
            game_lookup = game_feed_cache[gk]
            key = (hr["player"], hr["inning"])
            enriched = game_lookup.get(key)
            if enriched and enriched.get("distance"):
                hr["distance"] = enriched["distance"]
                hr["play_id"] = enriched.get("play_id", "")
                hr["source"] = "Statcast (game feed)"
            else:
                hr["source"] = "MLB Stats API (distance pending)"
            results.append(hr)

        return jsonify({
            "step": "5_full_ok",
            "total_games": len(games),
            "total_hrs": len(results),
            "failed_mlb_games": failed_games,
            "failed_savant_games": failed_savant,
        })
    except Exception as e:
        return jsonify({"step": "5_full_failed", "error": str(e), "trace": traceback.format_exc()}), 500
        
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

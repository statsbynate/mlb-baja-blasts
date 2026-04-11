import os
import csv
import io
import time
import logging
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from flask import Flask, jsonify
from flask_cors import CORS
import requests

app = Flask(__name__)
CORS(app)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

_cache = {"data": None, "ts": 0}
_game_cache = {}  # game_pk -> list of HRs, permanently cached once fetched
_savant_cache = {}  # game_pk -> savant lookup, permanently cached once fetched
_notified_blasts = set()
_fetch_in_progress = False
CACHE_TTL = 1800

SEASON = "2026"
MIN_DISTANCE = 420
NTFY_CHANNEL = "baja-blast-tracker-2026"

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
        pitcher = safe_get(play, "matchup", "pitcher", "fullName") or "Unknown"
        inning = str(safe_get(play, "about", "inning"))
        half = safe_get(play, "about", "halfInning")
        team = game["away"] if half == "top" else game["home"]
        opponent = game["home"] if half == "top" else game["away"]
        try:
            is_walkoff = (half == "bottom" and int(inning) >= 9 and i == last_idx)
        except (ValueError, TypeError):
            is_walkoff = False
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
            "pitcher": pitcher,
            "play_id": "",
            "hc_x": None,
            "hc_y": None,
            "source": "MLB Stats API",
        })
    return hrs


def fetch_savant_game_distances(game_pk):
    url = f"https://baseballsavant.mlb.com/gf?game_pk={game_pk}"
    try:
        resp = requests.get(url, headers=SAVANT_HEADERS, timeout=15)
        if resp.status_code != 200:
            return {}
        try:
            data = resp.json()
        except Exception:
            return {}

        ev_array = data.get("exit_velocity", [])
        if not isinstance(ev_array, list):
            return {}

        lookup = {}
        for play in ev_array:
            if not isinstance(play, dict):
                continue
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
                hc_x = play.get("hc_x")
                hc_y = play.get("hc_y")
                key = (name, inning)
                if key not in lookup:
                    lookup[key] = {
                        "distance": dist,
                        "exit_velocity": round(float(str(ev_raw)), 1) if ev_raw else None,
                        "launch_angle": round(float(str(la_raw)), 1) if la_raw else None,
                        "play_id": play_id,
                        "hc_x": round(float(str(hc_x)), 2) if hc_x else None,
                        "hc_y": round(float(str(hc_y)), 2) if hc_y else None,
                    }
            except (ValueError, TypeError):
                continue

        logger.info(f"Savant game feed {game_pk}: {len(lookup)} HR distance entries")
        return lookup

    except Exception as e:
        logger.warning(f"Savant game feed {game_pk} error: {e}")
        return {}


def fetch_all_homeruns(season=SEASON):
    games = fetch_final_games(season)
    if not games:
        return []

    all_hrs = []
    games_to_fetch = [g for g in games if g["gamePk"] not in _game_cache]
    logger.info(f"Fetching {len(games_to_fetch)} new games, {len(games) - len(games_to_fetch)} from cache")

    def fetch_game(game):
        try:
            return game["gamePk"], fetch_homeruns_for_game(game)
        except Exception as e:
            logger.warning(f"Game {game['gamePk']} error: {e}")
            return game["gamePk"], []

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(fetch_game, game): game for game in games_to_fetch}
        for future in as_completed(futures):
            gk, hrs = future.result()
            _game_cache[gk] = hrs

    for game in games:
        all_hrs.extend(_game_cache.get(game["gamePk"], []))

    logger.info(f"Total HRs from MLB API: {len(all_hrs)}")

    # Fetch Savant game feeds in parallel, skip already cached
    unique_pks = list({hr["game_pk"] for hr in all_hrs})
    pks_to_fetch = [gk for gk in unique_pks if gk not in _savant_cache]
    logger.info(f"Fetching {len(pks_to_fetch)} new Savant feeds, {len(unique_pks) - len(pks_to_fetch)} from cache")

    def fetch_one(gk):
        try:
            return gk, fetch_savant_game_distances(gk)
        except Exception as e:
            logger.warning(f"Savant feed {gk} error: {e}")
            return gk, {}

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(fetch_one, gk): gk for gk in pks_to_fetch}
        for future in as_completed(futures):
            gk, data = future.result()
            _savant_cache[gk] = data

    game_feed_cache = {gk: _savant_cache.get(gk, {}) for gk in unique_pks}

    results = []
    for hr in all_hrs:
        gk = hr["game_pk"]
        game_lookup = game_feed_cache.get(gk, {})
        key = (hr["player"], hr["inning"])
        enriched = game_lookup.get(key)

        if enriched and enriched.get("distance"):
            hr["distance"] = enriched["distance"]
            hr["exit_velocity"] = enriched.get("exit_velocity") or hr["exit_velocity"]
            hr["launch_angle"] = enriched.get("launch_angle") or hr["launch_angle"]
            hr["play_id"] = enriched.get("play_id", "")
            hr["hc_x"] = enriched.get("hc_x")
            hr["hc_y"] = enriched.get("hc_y")
            hr["source"] = "Statcast (game feed)"
        else:
            hr["source"] = "MLB Stats API (distance pending)"

        results.append(hr)

    baja = [h for h in results if h.get("distance") and h["distance"] >= MIN_DISTANCE]
    sub = [h for h in results if h.get("distance") and h["distance"] < MIN_DISTANCE]
    pending = [h for h in results if not h.get("distance")]
    baja.sort(key=lambda x: x["distance"], reverse=True)
    sub.sort(key=lambda x: x["distance"], reverse=True)
    return baja + sub + pending



def send_ntfy_notification(hr):
    try:
        dist = hr.get("distance", "")
        player = hr.get("player", "Unknown")
        team = hr.get("team", "")
        opponent = hr.get("opponent", "")
        ev = hr.get("exit_velocity")
        inning = hr.get("inning", "")
        title = f"Baja Blast! {player} ({team})"
        parts = [f"{dist} ft"]
        if ev: parts.append(f"{ev} mph exit velo")
        if opponent: parts.append(f"vs {opponent}")
        if inning: parts.append(f"Inn. {inning}")
        body = " · ".join(parts)
        requests.post(
            f"https://ntfy.sh/{NTFY_CHANNEL}",
            data=body.encode("utf-8"),
            headers={
                "Title": title.encode("utf-8"),
                "Priority": "high",
                "Tags": "baseball,tada",
                "Click": "https://statsbynate.github.io",
                "Content-Type": "text/plain; charset=utf-8",
            },
            timeout=5,
        )
        logger.info(f"Sent ntfy notification for {player} {dist} ft")
    except Exception as e:
        logger.warning(f"ntfy notification failed: {e}")


def check_and_notify(new_data, first_run=False):
    global _notified_blasts
    for hr in new_data:
        if not hr.get("distance") or hr["distance"] < MIN_DISTANCE:
            continue
        key = (hr["game_pk"], hr["player"])
        if key not in _notified_blasts:
            _notified_blasts.add(key)
            if not first_run:
                send_ntfy_notification(hr)
    if first_run:
        logger.info(f"First run: pre-populated {len(_notified_blasts)} known Baja Blasts, no notifications sent")


@app.route("/api/ntfy-channel")
def ntfy_channel():
    return jsonify({"channel": NTFY_CHANNEL})


def background_fetch():
    global _cache, _fetch_in_progress
    if _fetch_in_progress:
        return
    _fetch_in_progress = True
    try:
        now = time.time()
        data = fetch_all_homeruns()
        first_run = _cache["data"] is None
        check_and_notify(data, first_run=first_run)
        _cache = {"data": data, "ts": now}
        logger.info(f"Background fetch complete: {len(data)} HRs")
    except Exception as e:
        logger.error(f"Background fetch error: {e}")
        logger.error(traceback.format_exc())
    finally:
        _fetch_in_progress = False


@app.route("/api/homeruns")
def homeruns():
    global _cache
    now = time.time()
    cache_fresh = _cache["data"] is not None and (now - _cache["ts"]) < CACHE_TTL
    cache_exists = _cache["data"] is not None

    # If cache is fresh, return it immediately
    if cache_fresh:
        return jsonify({
            "homeruns": _cache["data"],
            "count": len(_cache["data"]),
            "cached": True,
            "cache_age_seconds": int(now - _cache["ts"]),
        })

    # If cache is stale but exists, return stale data and refresh in background
    if cache_exists:
        if not _fetch_in_progress:
            t = threading.Thread(target=background_fetch, daemon=True)
            t.start()
        return jsonify({
            "homeruns": _cache["data"],
            "count": len(_cache["data"]),
            "cached": True,
            "cache_age_seconds": int(now - _cache["ts"]),
            "refreshing": True,
        })

    # No cache at all — if fetch in progress return empty with status
    if _fetch_in_progress:
        return jsonify({
            "homeruns": [],
            "count": 0,
            "cached": False,
            "loading": True,
            "message": "Data is loading for the first time, please refresh in 60 seconds.",
        })

    # No cache, no fetch in progress — kick off background fetch and return loading state
    t = threading.Thread(target=background_fetch, daemon=True)
    t.start()
    return jsonify({
        "homeruns": [],
        "count": 0,
        "cached": False,
        "loading": True,
        "message": "Data is loading for the first time, please refresh in 60 seconds.",
    })


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
        }
    except Exception as e:
        result["savant"] = {"error": str(e)}
    return jsonify(result)


@app.route("/health")
def health():
    return jsonify({"status": "ok"})


# Pre-warm cache on startup so first request is never slow
_startup_thread = threading.Thread(target=background_fetch, daemon=True)
_startup_thread.start()
logger.info("Started background cache pre-warm on startup")

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)

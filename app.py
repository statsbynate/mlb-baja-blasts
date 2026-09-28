import os
import time
import logging
import traceback
import json
import threading
import socket
from collections import defaultdict
from datetime import date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from flask import Flask, jsonify
from flask_cors import CORS
import requests

# Global socket timeout — kills any connection that hangs at the TCP level
# requests timeouts only cover connect+read, not hung sockets
socket.setdefaulttimeout(25)

app = Flask(__name__)
CORS(app)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

_game_cache = {}    # game_pk -> list of HRs (only successful fetches are cached)
_game_meta = {}     # game_pk -> {"state": codedGameState at fetch time, "ts": fetch time}
_savant_cache = {}  # game_pk -> savant lookup
_notified_blasts = set()
_fetch_in_progress = False
_last_reconcile = 0
CACHE_TTL = 600

SEASON = "2026"
MIN_DISTANCE = 420
NTFY_CHANNEL = "baja-blast-tracker-2026"

# codedGameState values that mean the game was actually played to completion.
# NOTE: Postponed ("D"), Cancelled ("C") and Suspended games ALSO report
# abstractGameState == "Final", which is why we can't filter on that field.
PLAYED_STATES = {"F", "O"}          # F = Final / Completed Early, O = Game Over
RECHECK_DAYS = 2                    # always re-pull games from the last N days (scoring changes)
RECONCILE_INTERVAL = 6 * 3600       # compare against official team game logs every 6h
MAX_GAMES_PER_RUN = 400             # keeps a cold start inside the 5-min watchdog
NOTIFY_MAX_AGE_DAYS = 1             # never push notifications for old HRs found in a backfill

# Use persistent disk if available, fall back to /tmp
_PERSISTENT_PATH = "/data/mlb_hr_cache.json"
_TMP_PATH = "/tmp/mlb_hr_cache.json"
CACHE_FILE = _PERSISTENT_PATH if os.path.isdir("/data") else _TMP_PATH

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
    """Return one entry per completed game, keyed on gamePk.

    A postponed game shows up in the schedule twice with the SAME gamePk:
    once on the original date (status Postponed, abstractGameState "Final")
    and again on the makeup date (often a doubleheader). The old code
    treated the postponed entry as a finished game, cached it as 0 HRs,
    and never re-fetched it when the makeup was actually played.
    """
    url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&season={season}&gameType=R"
    resp = requests.get(url, headers=MLB_HEADERS, timeout=30)
    resp.raise_for_status()
    by_pk = {}
    for date_entry in resp.json().get("dates", []):
        for game in date_entry.get("games", []):
            state = safe_get(game, "status", "codedGameState")
            if state not in PLAYED_STATES:
                continue
            home_dict = safe_get(game, "teams", "home", "team", default={})
            away_dict = safe_get(game, "teams", "away", "team", default={})
            pk = str(game["gamePk"])
            # Later entries win, so a suspended/resumed game gets its completion date
            by_pk[pk] = {
                "gamePk": pk,
                "gameDate": game.get("officialDate") or date_entry.get("date", ""),
                "home": team_abbrev(home_dict),
                "away": team_abbrev(away_dict),
                "state": state,
            }
    games = list(by_pk.values())
    logger.info(f"Found {len(games)} completed games")
    return games


def fetch_homeruns_for_game(game):
    """Return list of HRs, or None if the feed couldn't be fetched (so it gets retried)."""
    url = f"https://statsapi.mlb.com/api/v1.1/game/{game['gamePk']}/feed/live"
    resp = requests.get(url, headers=MLB_HEADERS, timeout=20)
    if resp.status_code != 200:
        logger.warning(f"Game {game['gamePk']} feed returned {resp.status_code}")
        return None
    feed = resp.json()
    plays = feed.get("liveData", {}).get("plays", {}).get("allPlays", [])
    last_idx = len(plays) - 1
    hrs = []
    for i, play in enumerate(plays):
        if safe_get(play, "result", "event").lower() != "home run":
            continue
        # hitData and playId live on the pitch event, not on the play itself
        events = play.get("playEvents") or []
        pitch = next((e for e in reversed(events) if e.get("hitData")), events[-1] if events else {})
        hit = pitch.get("hitData", {}) or {}
        mlb_play_id = str(pitch.get("playId", "") or "").strip()
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
            "play_id": mlb_play_id,
            "at_bat_index": safe_get(play, "about", "atBatIndex", default=i),
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
                entry = {
                    "distance": dist,
                    "exit_velocity": round(float(str(ev_raw)), 1) if ev_raw else None,
                    "launch_angle": round(float(str(la_raw)), 1) if la_raw else None,
                    "play_id": play_id,
                    "hc_x": round(float(str(hc_x)), 2) if hc_x else None,
                    "hc_y": round(float(str(hc_y)), 2) if hc_y else None,
                }
                # Primary key: the play UUID (same ID the MLB feed uses).
                if play_id:
                    lookup["id:" + play_id] = entry
                # Fallback key: (name, inning). A player can homer twice in one
                # inning, so keep a list and only use it when it's unambiguous.
                lookup.setdefault((name, inning), []).append(entry)
            except (ValueError, TypeError):
                continue

        logger.info(f"Savant game feed {game_pk}: {sum(1 for k in lookup if isinstance(k, str))} HR entries")
        return lookup

    except Exception as e:
        logger.warning(f"Savant game feed {game_pk} error: {e}")
        return {}


def _invalidate(pk):
    _game_cache.pop(pk, None)
    _game_meta.pop(pk, None)
    _savant_cache.pop(pk, None)


def reconcile_with_game_logs(season=SEASON):
    """Compare cached HR counts to MLB's official per-team game logs and
    evict any game that doesn't match, so it gets re-fetched."""
    teams_url = f"https://statsapi.mlb.com/api/v1/teams?sportId=1&season={season}"
    teams = requests.get(teams_url, headers=MLB_HEADERS, timeout=20).json().get("teams", [])

    def team_log(team_id):
        url = (f"https://statsapi.mlb.com/api/v1/teams/{team_id}/stats"
               f"?season={season}&group=hitting&stats=gameLog&gameType=R")
        data = requests.get(url, headers=MLB_HEADERS, timeout=20).json()
        return data.get("stats", [{}])[0].get("splits", [])

    official = defaultdict(int)
    with ThreadPoolExecutor(max_workers=6) as ex:
        for splits in ex.map(team_log, [t["id"] for t in teams]):
            for sp in splits:
                pk = str(safe_get(sp, "game", "gamePk"))
                official[pk] += int(safe_get(sp, "stat", "homeRuns", default=0) or 0)

    if len(teams) < 30 or not official:
        logger.warning("Reconcile skipped: incomplete game log data")
        return 0

    mismatched = [pk for pk, hrs in list(_game_cache.items())
                  if pk in official and len(hrs) != official[pk]]
    for pk in mismatched:
        logger.info(f"Reconcile: game {pk} cached {len(_game_cache[pk])} HRs, official {official[pk]} — re-fetching")
        _invalidate(pk)
    total = sum(official.values())
    logger.info(f"Reconcile done: official season HRs={total}, {len(mismatched)} games evicted")
    return len(mismatched)


def _needs_fetch(game, recheck_after):
    pk = game["gamePk"]
    if pk not in _game_cache:
        return True
    meta = _game_meta.get(pk, {})
    if meta.get("state") != "F":          # first fetched as "Game Over" — pull the final version
        return True
    if game["gameDate"] >= recheck_after:  # recent game — catch scoring changes
        return time.time() - meta.get("ts", 0) > 3600
    return False


def _savant_match(lookup, hr):
    """Match a HR to its Statcast entry by play ID, falling back to a unique (name, inning)."""
    if hr.get("play_id") and ("id:" + hr["play_id"]) in lookup:
        return lookup["id:" + hr["play_id"]]
    candidates = lookup.get((hr["player"], hr["inning"])) or []
    return candidates[0] if len(candidates) == 1 else None


def _needs_savant(pk):
    if pk not in _savant_cache:
        return True
    lookup = _savant_cache[pk]
    # Retry if any HR in this game still lacks a Statcast match
    return any(_savant_match(lookup, hr) is None for hr in _game_cache.get(pk, []))


def _build_result(all_hrs):
    """Deduplicate and sort a flat list of HR dicts."""
    seen = set()
    deduped = []
    for hr in all_hrs:
        pid = hr.get("play_id", "").strip()
        key = pid or f"{hr['game_pk']}|{hr.get('at_bat_index')}|{hr['player']}|{hr['inning']}|{hr['inning_half']}"
        if key not in seen:
            seen.add(key)
            deduped.append(hr)
    baja = [h for h in deduped if h.get("distance") and h["distance"] >= MIN_DISTANCE]
    sub = [h for h in deduped if h.get("distance") and h["distance"] < MIN_DISTANCE]
    pending = [h for h in deduped if not h.get("distance")]
    baja.sort(key=lambda x: x["distance"], reverse=True)
    sub.sort(key=lambda x: x["distance"], reverse=True)
    return baja + sub + pending


def fetch_all_homeruns(season=SEASON):
    """Returns (results, games_completed, is_complete)."""
    global _last_reconcile
    games = fetch_final_games(season)
    if not games:
        return [], 0, True

    if time.time() - _last_reconcile > RECONCILE_INTERVAL and _game_cache:
        try:
            reconcile_with_game_logs(season)
            _last_reconcile = time.time()
        except Exception as e:
            logger.warning(f"Reconcile failed: {e}")

    recheck_after = (date.today() - timedelta(days=RECHECK_DAYS)).isoformat()
    games_to_fetch = [g for g in games if _needs_fetch(g, recheck_after)]
    backlog = max(0, len(games_to_fetch) - MAX_GAMES_PER_RUN)
    games_to_fetch = games_to_fetch[:MAX_GAMES_PER_RUN]
    logger.info(f"Fetching {len(games_to_fetch)} games ({backlog} queued for next run)")

    def fetch_game(game):
        try:
            return game, fetch_homeruns_for_game(game)
        except Exception as e:
            logger.warning(f"Game {game['gamePk']} error: {e}")
            return game, None

    with ThreadPoolExecutor(max_workers=6) as executor:
        for future in as_completed([executor.submit(fetch_game, g) for g in games_to_fetch]):
            game, hrs = future.result()
            if hrs is None:
                continue  # don't cache failures — retry next run
            pk = game["gamePk"]
            _game_cache[pk] = hrs
            _game_meta[pk] = {"state": game["state"], "ts": time.time()}
            _savant_cache.pop(pk, None)  # fresh play data -> re-enrich

    all_hrs = []
    for game in games:
        all_hrs.extend(_game_cache.get(game["gamePk"], []))
    missing = sum(1 for g in games if g["gamePk"] not in _game_cache)
    logger.info(f"Total HRs from MLB API: {len(all_hrs)} ({missing} games not yet loaded)")

    # Savant enrichment
    unique_pks = list({hr["game_pk"] for hr in all_hrs})
    pks_to_fetch = [gk for gk in unique_pks if _needs_savant(gk)][:MAX_GAMES_PER_RUN]
    logger.info(f"Fetching {len(pks_to_fetch)} Savant feeds")

    def fetch_one(gk):
        try:
            return gk, fetch_savant_game_distances(gk)
        except Exception as e:
            logger.warning(f"Savant feed {gk} error: {e}")
            return gk, {}

    with ThreadPoolExecutor(max_workers=4) as executor:
        for future in as_completed([executor.submit(fetch_one, gk) for gk in pks_to_fetch]):
            gk, data = future.result()
            _savant_cache[gk] = data

    for hr in all_hrs:
        enriched = _savant_match(_savant_cache.get(hr["game_pk"], {}), hr)
        if enriched and enriched.get("distance"):
            hr["distance"] = enriched["distance"]
            hr["exit_velocity"] = enriched.get("exit_velocity") or hr["exit_velocity"]
            hr["launch_angle"] = enriched.get("launch_angle") or hr["launch_angle"]
            hr["play_id"] = enriched.get("play_id") or hr.get("play_id", "")
            hr["hc_x"] = enriched.get("hc_x")
            hr["hc_y"] = enriched.get("hc_y")
            hr["source"] = "Statcast (game feed)"
        elif hr.get("distance"):
            hr["source"] = "MLB Stats API"
        else:
            hr["source"] = "MLB Stats API (distance pending)"

    return _build_result(all_hrs), len(games), missing == 0


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


def _blast_key(hr):
    return (hr["game_pk"], hr["player"], hr.get("inning", ""))


def check_and_notify(new_data, first_run=False):
    cutoff = (date.today() - timedelta(days=NOTIFY_MAX_AGE_DAYS)).isoformat()
    for hr in new_data:
        if not hr.get("distance") or hr["distance"] < MIN_DISTANCE:
            continue
        key = _blast_key(hr)
        if key in _notified_blasts:
            continue
        _notified_blasts.add(key)
        # Only push for recent HRs — backfilled or corrected old games stay silent
        if not first_run and hr.get("date", "") >= cutoff:
            send_ntfy_notification(hr)
    if first_run:
        logger.info(f"First run: pre-populated {len(_notified_blasts)} known Baja Blasts, no notifications sent")


@app.route("/api/ntfy-channel")
def ntfy_channel():
    return jsonify({"channel": NTFY_CHANNEL})


def load_file_cache():
    """Load cache from file — shared across threads/processes."""
    try:
        if os.path.exists(CACHE_FILE):
            with open(CACHE_FILE, 'r') as f:
                return json.load(f)
    except Exception as e:
        logger.warning(f"Cache file read error: {e}")
    return None


def save_file_cache(data, games_completed):
    """Save cache to file — visible to all threads/processes."""
    try:
        tmp = CACHE_FILE + ".tmp"
        with open(tmp, 'w') as f:
            json.dump({"data": data, "games_completed": games_completed, "ts": time.time()}, f)
        os.replace(tmp, CACHE_FILE)  # atomic — readers never see a half-written file
    except Exception as e:
        logger.warning(f"Cache file write error: {e}")


# Max time a background fetch is allowed to run before being force-reset
FETCH_TIMEOUT = 300  # 5 minutes

_fetch_started_at = None
_fetch_thread = None


def check_stuck_fetch():
    """Reset fetch_in_progress if the fetching thread died or ran too long. Call from any route."""
    global _fetch_in_progress, _fetch_started_at
    # Under gunicorn --preload the startup thread runs in the master process; forked
    # workers inherit fetch_in_progress=True with no thread behind it. Clear that now.
    if _fetch_in_progress and (_fetch_thread is None or not _fetch_thread.is_alive()):
        logger.warning("Watchdog: fetch flag set but no live fetch thread, resetting")
        _fetch_in_progress = False
        _fetch_started_at = None
        return
    if _fetch_in_progress and _fetch_started_at and (time.time() - _fetch_started_at) > FETCH_TIMEOUT:
        logger.warning(f"Watchdog: fetch stuck for >{FETCH_TIMEOUT}s, force-resetting")
        _fetch_in_progress = False
        _fetch_started_at = None


def background_fetch():
    global _fetch_in_progress, _fetch_started_at, _fetch_thread
    check_stuck_fetch()
    if _fetch_in_progress:
        return
    _fetch_thread = threading.current_thread()
    _fetch_in_progress = True
    _fetch_started_at = time.time()
    run_again = False
    try:
        cached = load_file_cache()
        # Seed notification memory from the saved file so a restart doesn't re-announce old blasts
        if not _notified_blasts and cached:
            for hr in cached.get("data", []):
                if hr.get("distance") and hr["distance"] >= MIN_DISTANCE:
                    _notified_blasts.add(_blast_key(hr))
        first_run = cached is None
        data, games_completed, complete = fetch_all_homeruns()
        check_and_notify(data, first_run=first_run)
        # During a cold-start backfill, keep serving the previous full dataset
        # instead of overwriting it with a partial one.
        if complete or cached is None:
            save_file_cache(data, games_completed)
            logger.info(f"Background fetch complete: {len(data)} HRs across {games_completed} games")
        else:
            logger.info(f"Backfill in progress: {len(data)} HRs so far, continuing")
        run_again = not complete
    except Exception as e:
        logger.error(f"Background fetch error: {e}")
        logger.error(traceback.format_exc())
    finally:
        _fetch_in_progress = False
        _fetch_started_at = None
    if run_again:
        threading.Timer(5, background_fetch).start()


@app.route("/api/homeruns")
def homeruns():
    check_stuck_fetch()
    now = time.time()
    cached = load_file_cache()

    # Trigger background refresh if cache is stale or missing
    if not _fetch_in_progress:
        if cached is None or (now - cached["ts"]) > CACHE_TTL:
            t = threading.Thread(target=background_fetch, daemon=True)
            t.start()

    # Return cached data immediately if available
    if cached is not None:
        age = int(now - cached["ts"])
        return jsonify({
            "homeruns": cached["data"],
            "count": len(cached["data"]),
            "games_completed": cached.get("games_completed"),
            "cached": True,
            "cache_age_seconds": age,
            "refreshing": _fetch_in_progress,
        })

    # No cache yet — tell frontend to retry
    return jsonify({
        "homeruns": [],
        "count": 0,
        "cached": False,
        "loading": True,
        "message": "Data is loading, please wait 60 seconds and refresh.",
    })


@app.route("/api/debug")
def debug():
    result = {}
    try:
        url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&season={SEASON}&gameType=R"
        resp = requests.get(url, headers=MLB_HEADERS, timeout=15)
        data = resp.json()
        all_games = [g for d in data.get("dates", []) for g in d.get("games", [])]
        played = {g["gamePk"] for g in all_games if safe_get(g, "status", "codedGameState") in PLAYED_STATES}
        result["mlb_api"] = {
            "status": resp.status_code,
            "schedule_entries": len(all_games),
            "unique_games": len({g["gamePk"] for g in all_games}),
            "completed_games": len(played),
        }
    except Exception as e:
        result["mlb_api"] = {"error": str(e)}
    result["cache"] = {
        "games_cached": len(_game_cache),
        "hrs_cached": sum(len(v) for v in _game_cache.values()),
        "savant_cached": len(_savant_cache),
        "last_reconcile_age_seconds": int(time.time() - _last_reconcile) if _last_reconcile else None,
    }
    return jsonify(result)


@app.route("/api/status")
def status():
    """Fast status check - no external calls."""
    cached = load_file_cache()
    return jsonify({
        "cache_exists": cached is not None,
        "cache_age_seconds": int(time.time() - cached["ts"]) if cached else None,
        "hr_count": len(cached["data"]) if cached else 0,
        "games_completed": cached.get("games_completed") if cached else None,
        "games_in_memory": len(_game_cache),
        "fetch_in_progress": _fetch_in_progress,
        "cache_file_exists": os.path.exists(CACHE_FILE),
    })


@app.route("/health")
def health():
    # Watchdog: reset stuck fetch and trigger refresh if cache is stale
    check_stuck_fetch()
    now = time.time()
    cached = load_file_cache()
    if not _fetch_in_progress and (cached is None or (now - cached["ts"]) > CACHE_TTL):
        logger.info("Health check triggered background refresh (cache stale)")
        t = threading.Thread(target=background_fetch, daemon=True)
        t.start()
    return jsonify({"status": "ok"})


# Pre-warm on startup
_startup_thread = threading.Thread(target=background_fetch, daemon=True)
_startup_thread.start()
logger.info("Started background cache pre-warm on startup")

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)

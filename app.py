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

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "identity",
}

TEAM_ABBREVS = {
    "Arizona Diamondbacks": "ARI",
    "Atlanta Braves": "ATL",
    "Baltimore Orioles": "BAL",
    "Boston Red Sox": "BOS",
    "Chicago Cubs": "CHC",
    "Chicago White Sox": "CWS",
    "Cincinnati Reds": "CIN",
    "Cleveland Guardians": "CLE",
    "Colorado Rockies": "COL",
    "Detroit Tigers": "DET",
    "Houston Astros": "HOU",
    "Kansas City Royals": "KC",
    "Los Angeles Angels": "LAA",
    "Los Angeles Dodgers": "LAD",
    "Miami Marlins": "MIA",
    "Milwaukee Brewers": "MIL",
    "Minnesota Twins": "MIN",
    "New York Mets": "NYM",
    "New York Yankees": "NYY",
    "Oakland Athletics": "OAK",
    "Philadelphia Phillies": "PHI",
    "Pittsburgh Pirates": "PIT",
    "San Diego Padres": "SD",
    "San Francisco Giants": "SF",
    "Seattle Mariners": "SEA",
    "St. Louis Cardinals": "STL",
    "Tampa Bay Rays": "TB",
    "Texas Rangers": "TEX",
    "Toronto Blue Jays": "TOR",
    "Washington Nationals": "WSH",
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


def get_distance(row):
    """Try multiple field names for HR distance."""
    for field in ["hit_distance_sc", "hit_distance", "total_distance"]:
        val = row.get(field, "").strip()
        if val and val != "null":
            try:
                return int(float(val))
            except ValueError:
                continue
    return None


# ---------------------------------------------------------------------------
# Source 1: MLB Stats API
# ---------------------------------------------------------------------------

def fetch_mlb_homeruns(season=SEASON):
    schedule_url = (
        f"https://statsapi.mlb.com/api/v1/schedule"
        f"?sportId=1&season={season}&gameType=R"
    )
    logger.info(f"Fetching schedule: {schedule_url}")
    sched_resp = requests.get(schedule_url, headers={"User-Agent": HEADERS["User-Agent"]}, timeout=30)
    sched_resp.raise_for_status()
    sched_data = sched_resp.json()

    game_pks = []
    for date_entry in sched_data.get("dates", []):
        game_date = date_entry.get("date", "")
        for game in date_entry.get("games", []):
            state = safe_get(game, "status", "abstractGameState")
            if state != "Final":
                continue
            home_team_dict = safe_get(game, "teams", "home", "team", default={})
            away_team_dict = safe_get(game, "teams", "away", "team", default={})
            game_pks.append({
                "gamePk": game["gamePk"],
                "gameDate": game_date,
                "home": team_abbrev(home_team_dict),
                "away": team_abbrev(away_team_dict),
            })

    logger.info(f"Found {len(game_pks)} final games in {season}")
    if not game_pks:
        return []

    all_hrs = []
    for game in game_pks:
        try:
            feed_url = f"https://statsapi.mlb.com/api/v1.1/game/{game['gamePk']}/feed/live"
            resp = requests.get(feed_url, headers={"User-Agent": HEADERS["User-Agent"]}, timeout=20)
            if resp.status_code != 200:
                continue
            feed = resp.json()

            plays = feed.get("liveData", {}).get("plays", {}).get("allPlays", [])
            for play in plays:
                event = safe_get(play, "result", "event")
                if event.lower() != "home run":
                    continue

                hit_data = play.get("hitData", {})
                distance = hit_data.get("totalDistance")
                launch_speed = hit_data.get("launchSpeed")
                launch_angle = hit_data.get("launchAngle")

                batter_name = safe_get(play, "matchup", "batter", "fullName") or "Unknown"
                inning = safe_get(play, "about", "inning")
                half = safe_get(play, "about", "halfInning")

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
                    "distance": int(distance) if distance is not None else None,
                    "exit_velocity": round(float(launch_speed), 1) if launch_speed is not None else None,
                    "launch_angle": round(float(launch_angle), 1) if launch_angle is not None else None,
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


# ---------------------------------------------------------------------------
# Source 2: Baseball Savant CSV (with fixed encoding)
# ---------------------------------------------------------------------------

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
    savant_headers = {
        **HEADERS,
        "Referer": "https://baseballsavant.mlb.com/",
        "Accept": "text/csv,text/html,*/*",
    }
    resp = requests.get(url, headers=savant_headers, timeout=30)
    resp.raise_for_status()

    # Handle BOM and encoding explicitly
    raw = resp.content.decode("utf-8-sig", errors="replace")
    logger.info(f"Savant CSV length: {len(raw)}, first 100: {raw[:100]}")

    reader = csv.DictReader(io.StringIO(raw))
    lookup = {}
    row_count = 0
    for row in reader:
        row_count += 1
        try:
            dist = get_distance(row)
            if dist is None:
                continue

            name = row.get("player_name", "").strip()
            date = row.get("game_date", "").strip()

            ev_raw = row.get("launch_speed", "").strip()
            ev = round(float(ev_raw), 1) if ev_raw and ev_raw != "null" else None

            la_raw = row.get("launch_angle", "").strip()
            la = round(float(la_raw), 1) if la_raw and la_raw != "null" else None

            key = (name, date)
            if key not in lookup:
                lookup[key] = {"distance": dist, "exit_velocity": ev, "launch_angle": la}

        except (ValueError, KeyError) as e:
            logger.debug(f"Row parse error: {e}")
            continue

    logger.info(f"Savant: {row_count} rows parsed, {len(lookup)} HR entries found")
    return lookup


# ---------------------------------------------------------------------------
# Source 3: pybaseball
# ---------------------------------------------------------------------------

def fetch_pybaseball_distances(start_date=SEASON_START):
    try:
        import pybaseball
        pybaseball.cache.enable()
        today = datetime.today().strftime("%Y-%m-%d")
        logger.info(f"Fetching pybaseball statcast {start_date} to {today}")
        data = pybaseball.statcast(start_dt=start_date, end_dt=today)
        if data is None or data.empty:
            logger.info("pybaseball returned no data")
            return {}

        hrs = data[data["events"] == "home_run"]
        lookup = {}
        for _, row in hrs.iterrows():
            try:
                dist = None
                for field in ["hit_distance_sc", "hit_distance"]:
                    val = row.get(field)
                    if val is not None and str(val) != "nan":
                        dist = int(float(val))
                        break
                if dist is None:
                    continue

                name = str(row.get("player_name", "")).strip()
                date = str(row.get("game_date", ""))[:10]
                ev = row.get("launch_speed")
                la = row.get("launch_angle")
                key = (name, date)
                if key not in lookup:
                    lookup[key] = {
                        "distance": dist,
                        "exit_velocity": round(float(ev), 1) if ev and str(ev) != "nan" else None,
                        "launch_angle": round(float(la), 1) if la and str(la) != "nan" else None,
                    }
            except Exception:
                continue

        logger.info(f"pybaseball HR entries: {len(lookup)}")
        return lookup

    except Exception as e:
        logger.warning(f"pybaseball fetch failed (non-fatal): {e}")
        return {}


# ---------------------------------------------------------------------------
# Combined
# ---------------------------------------------------------------------------

def fetch_all_homeruns(season=SEASON):
    mlb_hrs = fetch_mlb_homeruns(season)

    distance_lookup = {}

    # Try pybaseball first
    try:
        distance_lookup = fetch_pybaseball_distances()
        logger.info(f"pybaseball returned {len(distance_lookup)} entries")
    except Exception as e:
        logger.warning(f"pybaseball failed: {e}")

    # Fall back to Savant if pybaseball empty
    if not distance_lookup:
        try:
            distance_lookup = fetch_savant_distances(season)
            logger.info(f"Savant returned {len(distance_lookup)} entries")
        except Exception as e:
            logger.warning(f"Savant fallback failed: {e}")

    results = []
    for hr in mlb_hrs:
        key = (hr["player"], hr.get("date", ""))
        enriched = distance_lookup.get(key)
        if enriched:
            hr["distance"] = enriched["distance"]
            hr["exit_velocity"] = enriched["exit_velocity"] or hr["exit_velocity"]
            hr["launch_angle"] = enriched["launch_angle"] or hr["launch_angle"]
            hr["source"] = "Statcast"

        dist = hr.get("distance")
        if dist is not None and dist >= MIN_DISTANCE:
            results.append(hr)
        elif dist is None:
            hr["source"] = "MLB Stats API (distance pending)"
            results.append(hr)

    known = [h for h in results if h.get("distance") is not None and h["distance"] >= MIN_DISTANCE]
    unknown = [h for h in results if h.get("distance") is None]
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

    # MLB API
    try:
        url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&season={SEASON}&gameType=R"
        resp = requests.get(url, headers={"User-Agent": HEADERS["User-Agent"]}, timeout=15)
        data = resp.json()
        total_games = sum(len(d.get("games", [])) for d in data.get("dates", []))
        final_games = sum(
            1 for d in data.get("dates", [])
            for g in d.get("games", [])
            if safe_get(g, "status", "abstractGameState") == "Final"
        )
        result["mlb_api"] = {"status": resp.status_code, "total_games": total_games, "final_games": final_games}
    except Exception as e:
        result["mlb_api"] = {"error": str(e)}

    # Savant — show first data row so we can see field names
    try:
        savant_url = (
            "https://baseballsavant.mlb.com/statcast_search/csv"
            f"?type=batter&hfAB=home__run%7C&hfGT=R%7C&hfSea={SEASON}%7C"
            "&player_type=batter&min_pitches=0&min_results=0"
            "&group_by=name-event&sort_col=hit_distance_sc"
            "&sort_order=desc&min_abs=0&type=details"
        )
        savant_headers = {**HEADERS, "Referer": "https://baseballsavant.mlb.com/", "Accept": "text/csv,*/*"}
        resp = requests.get(savant_url, headers=savant_headers, timeout=15)
        raw = resp.content.decode("utf-8-sig", errors="replace")
        lines = raw.strip().split("\n")
        result["savant"] = {
            "status": resp.status_code,
            "line_count": len(lines),
            "has_data": len(lines) > 1,
            "first_data_row": lines[1][:300] if len(lines) > 1 else "",
            "headers": lines[0][:300] if lines else "",
        }
    except Exception as e:
        result["savant"] = {"error": str(e)}

    # pybaseball
    try:
        import pybaseball
        result["pybaseball"] = {"available": True}
    except ImportError:
        result["pybaseball"] = {"available": False}

    return jsonify(result)


@app.route("/health")
def health():
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)

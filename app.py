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


def fetch_mlb_homeruns(season=SEASON):
    schedule_url = (
        f"https://statsapi.mlb.com/api/v1/schedule"
        f"?sportId=1&season={season}&gameType=R"
    )
    logger.info(f"Fetching schedule: {schedule_url}")
    sched_resp = requests.get(schedule_url, headers=HEADERS, timeout=30)
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
                "home": team_abbrev(home_team

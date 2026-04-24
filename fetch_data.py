#!/usr/bin/env python3
"""
Barry Ballstein's Boys — Yahoo Fantasy Data Fetcher
Runs nightly via GitHub Actions to regenerate the dashboard HTML.

Setup:
  - Set YAHOO_COOKIE as a GitHub Actions secret
  - Script fetches live batter + pitcher stats from Yahoo
  - Generates barry_ballstein.html with fresh data
"""

import os
import re
import json
import math
import requests
from bs4 import BeautifulSoup
from datetime import datetime

# ----------------------------------------------------------------
# CONFIG
# ----------------------------------------------------------------
LEAGUE_ID   = "96087"
LEAGUE_ID2  = "100507"  # individual team logs
CURRENT_WEEK = 4        # update this each week

COOKIE = os.environ.get("YAHOO_COOKIE", "")
if not COOKIE:
    raise SystemExit("ERROR: YAHOO_COOKIE environment variable not set.")

HEADERS = {
    "Cookie": COOKIE.strip(),
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://baseball.fantasysports.yahoo.com/",
}

# ----------------------------------------------------------------
# SCORING MULTIPLIERS (from your spreadsheet)
# ----------------------------------------------------------------
BAT_MULT = {
    "R": 1, "1B": 1, "2B": 2, "3B": 3, "HR": 3,
    "RBI": 0.5, "SB": 3, "BB": 0.5, "IBB": 0.5, "HBP": 0.5
}
PIT_MULT = {
    "IP": 1, "W": 12, "L": -6, "SV": 7, "ER": -0.5,
    "K": 1.5, "HLD": 2, "NH": 10, "PG": 15, "BSV": -3.5
}

# ----------------------------------------------------------------
# TEAM METADATA (update W/L each week manually or add scoreboard fetch)
# ----------------------------------------------------------------
TEAMS = {
    "Chicks Dig The Holds": {"owner": "Pat",     "w": 2, "l": 2},
    "Deez \U0001f95c":      {"owner": "Sal",     "w": 3, "l": 1},
    "Dirty Meat Swing":     {"owner": "Charlie", "w": 4, "l": 0},
    "Frank Latera":         {"owner": "Fur",     "w": 1, "l": 3},
    "Santolos":             {"owner": "Oded",    "w": 1, "l": 3},
    "Team Mush":            {"owner": "Jimmy",   "w": 2, "l": 2},
    "Tony's Calling":       {"owner": "Phil",    "w": 2, "l": 2},
    "YOU ALWAYS DO THIS!":  {"owner": "Marz",    "w": 1, "l": 3},
}

# Weekly scores - update each week by adding new column
# Format: [week1, week2, week3, week4, ...]
WEEKLY_SCORES = {
    "Pat":     [216, 383, 309, 323],
    "Sal":     [161, 273, 336, 338],
    "Charlie": [214, 290, 263, 375],
    "Fur":     [140, 376, 374, 258],
    "Oded":    [177, 284, 336, 369],
    "Jimmy":   [132, 342, 367, 333],
    "Phil":    [208, 257, 304, 325],
    "Marz":    [173, 308, 255, 369],
}

MATCHUPS = [
    {"week":1,"home":"Pat","hpts":216,"away":"Jimmy","apts":132},
    {"week":1,"home":"Charlie","hpts":214,"away":"Oded","apts":177},
    {"week":1,"home":"Phil","hpts":208,"away":"Marz","apts":173},
    {"week":1,"home":"Sal","hpts":161,"away":"Fur","apts":140},
    {"week":2,"home":"Pat","hpts":383,"away":"Fur","apts":376},
    {"week":2,"home":"Jimmy","hpts":342,"away":"Marz","apts":308},
    {"week":2,"home":"Oded","hpts":284,"away":"Sal","apts":273},
    {"week":2,"home":"Charlie","hpts":290,"away":"Phil","apts":257},
    {"week":3,"home":"Jimmy","hpts":367,"away":"Oded","apts":336},
    {"week":3,"home":"Fur","hpts":374,"away":"Phil","apts":304},
    {"week":3,"home":"Sal","hpts":336,"away":"Pat","apts":309},
    {"week":3,"home":"Charlie","hpts":263,"away":"Marz","apts":255},
    {"week":4,"home":"Marz","hpts":369,"away":"Fur","apts":258},
    {"week":4,"home":"Sal","hpts":338,"away":"Jimmy","apts":333},
    {"week":4,"home":"Oded","hpts":369,"away":"Charlie","apts":375},
    {"week":4,"home":"Pat","hpts":323,"away":"Phil","apts":325},
]

# ----------------------------------------------------------------
# FETCH HELPERS
# ----------------------------------------------------------------
def fetch_soup(url):
    print(f"  Fetching: {url[-80:]}")
    r = requests.get(url, headers=HEADERS, timeout=25)
    print(f"  Status: {r.status_code}")
    if r.status_code != 200:
        print(f"  WARNING: Non-200 response")
        return None
    return BeautifulSoup(r.text, "lxml")

def sf(v):
    try: return float(str(v).replace(",", "").strip() or 0)
    except: return 0.0

def parse_table(soup, idx=0):
    """Parse the idx-th table in a soup, return (headers, rows)"""
    tables = soup.find_all("table") if soup else []
    if not tables or idx >= len(tables):
        return [], []
    rows = tables[idx].find_all("tr")
    headers, data = [], []
    for row in rows:
        cells = [c.get_text(strip=True) for c in row.find_all(["th", "td"])]
        if not headers and cells:
            headers = cells
        elif cells and len(cells) == len(headers):
            data.append(dict(zip(headers, cells)))
    return headers, data

# ----------------------------------------------------------------
# FETCH LEAGUE-WIDE BATTER + PITCHER STATS
# ----------------------------------------------------------------
def fetch_batter_stats():
    print("\n[Batters] Fetching cumulative batter stats...")
    url = f"https://baseball.fantasysports.yahoo.com/b1/{LEAGUE_ID}/headtoheadstats?pt=B&type=stats"
    soup = fetch_soup(url)
    if not soup:
        return {}
    _, rows = parse_table(soup)
    result = {}
    for row in rows:
        team = list(row.values())[0]
        if not team or team.lower() in ["team name", "totals", ""]:
            continue
        # Yahoo headtoheadstats provides 1B directly (not H)
        pts = (sf(row.get("R",   0)) * BAT_MULT["R"]   +
               sf(row.get("1B",  0)) * BAT_MULT["1B"]  +
               sf(row.get("2B",  0)) * BAT_MULT["2B"]  +
               sf(row.get("3B",  0)) * BAT_MULT["3B"]  +
               sf(row.get("HR",  0)) * BAT_MULT["HR"]  +
               sf(row.get("RBI", 0)) * BAT_MULT["RBI"] +
               sf(row.get("SB",  0)) * BAT_MULT["SB"]  +
               sf(row.get("BB",  0)) * BAT_MULT["BB"]  +
               sf(row.get("IBB", 0)) * BAT_MULT["IBB"] +
               sf(row.get("HBP", 0)) * BAT_MULT["HBP"])
        result[team] = {"pts": round(pts, 1), "raw": row}
        print(f"    {team}: {round(pts,1)} pts")
    return result

def fetch_pitcher_stats():
    print("\n[Pitchers] Fetching cumulative pitcher stats...")
    url = f"https://baseball.fantasysports.yahoo.com/b1/{LEAGUE_ID}/headtoheadstats?pt=P&type=stats"
    soup = fetch_soup(url)
    if not soup:
        return {}
    _, rows = parse_table(soup)
    result = {}
    for row in rows:
        team = list(row.values())[0]
        if not team or team.lower() in ["team name", "totals", ""]:
            continue
        pts = sum(sf(row.get(s, 0)) * m for s, m in PIT_MULT.items())
        result[team] = {"pts": round(pts, 1), "raw": row}
        print(f"    {team}: {round(pts,1)} pts")
    return result

# ----------------------------------------------------------------
# MATCH LIVE DATA TO OWNER NAMES
# ----------------------------------------------------------------
def match_team(live_name, team_dict):
    """Fuzzy match a live Yahoo team name to our TEAMS dict."""
    live_lower = live_name.lower().strip()
    for display_name, meta in team_dict.items():
        if (display_name.lower()[:10] in live_lower or
            live_lower[:10] in display_name.lower()):
            return display_name
    return None

# ----------------------------------------------------------------
# COMPUTE LEAGUE DATA
# ----------------------------------------------------------------
def build_league_data(bat_stats, pit_stats):
    print("\n[League] Building league data...")
    league = {}

    for display_name, meta in TEAMS.items():
        owner = meta["owner"]
        batters = 0
        pitchers = 0

        # Match batter stats
        for live_name, data in bat_stats.items():
            if match_team(live_name, {display_name: meta}):
                batters = data["pts"]
                break

        # Match pitcher stats
        for live_name, data in pit_stats.items():
            if match_team(live_name, {display_name: meta}):
                pitchers = data["pts"]
                break

        pf = round(batters + pitchers, 0)
        pa = 0  # PA requires matchup data — using snapshot for now

        # Pythagorean wins
        pyth = 0
        weeks = meta["w"] + meta["l"]
        if pf > 0 and pa > 0:
            pyth_pct = (pf**2) / (pf**2 + pa**2)
            pyth = round(pyth_pct * weeks, 3)

        league[owner] = {
            "display":  display_name,
            "w":        meta["w"],
            "l":        meta["l"],
            "pf":       int(pf),
            "pa":       0,           # populated from snapshot below
            "batters":  int(batters),
            "pitchers": int(pitchers),
            "luck":     0,           # populated from snapshot
            "pyth":     0,           # populated from snapshot
            "owner":    owner,
            "perfBat":  0,           # computed below
            "perfPit":  0,
            "perfTot":  0,
        }
        print(f"  {owner}: PF={int(pf)} Bat={int(batters)} Pit={int(pitchers)}")

    # Fill in PA, luck, pyth from snapshot (these need full matchup history to compute live)
    SNAPSHOT_PA   = {"Pat":1169,"Sal":1065,"Charlie":1058,"Fur":1217,"Oded":1229,"Jimmy":1197,"Phil":1159,"Marz":1071}
    SNAPSHOT_LUCK = {"Pat":0.2105,"Sal":0.8947,"Charlie":1.0,"Fur":0.0526,"Oded":0.0,"Jimmy":0.4211,"Phil":0.6842,"Marz":0.1053}
    SNAPSHOT_PYTH = {"Pat":2.571,"Sal":1.714,"Charlie":2.429,"Fur":2.0,"Oded":2.143,"Jimmy":2.0,"Phil":1.286,"Marz":1.857}

    for owner in league:
        league[owner]["pa"]   = SNAPSHOT_PA.get(owner, 0)
        league[owner]["luck"] = SNAPSHOT_LUCK.get(owner, 0)
        league[owner]["pyth"] = SNAPSHOT_PYTH.get(owner, 0)

    # Compute percentile ratings
    owners = list(league.keys())
    def pct_rank(key):
        vals = sorted([league[o][key] for o in owners])
        for o in owners:
            v = league[o][key]
            below = sum(1 for x in vals if x < v)
            league[o]["perf" + key.capitalize()[:3]] = round(below / (len(vals)-1) * 100) if len(vals) > 1 else 50

    # Manual percentile fields
    bat_vals = sorted([league[o]["batters"] for o in owners])
    pit_vals = sorted([league[o]["pitchers"] for o in owners])
    for o in owners:
        b_below = sum(1 for x in bat_vals if x < league[o]["batters"])
        p_below = sum(1 for x in pit_vals if x < league[o]["pitchers"])
        pb = round(b_below / (len(owners)-1) * 100)
        pp = round(p_below / (len(owners)-1) * 100)
        league[o]["perfBat"] = pb
        league[o]["perfPit"] = pp
        league[o]["perfTot"] = round((pb + pp) / 2)

    return league

# ----------------------------------------------------------------
# READ EXISTING HTML AND INJECT NEW DATA
# ----------------------------------------------------------------
def update_html(league_data):
    print("\n[HTML] Updating dashboard...")

    # Support both filename conventions
    import glob
    candidates = ["barry_ballstein.html", "barry_ballstein_VGoLive.html"] + glob.glob("barry_ballstein*.html")
    html_path = next((f for f in candidates if os.path.exists(f)), None)
    if not html_path:
        print(f"  ERROR: No barry_ballstein*.html found. Files present: {os.listdir()}")
        return False
    if not os.path.exists(html_path):
        print(f"  ERROR: {html_path} not found. Run from repo root.")
        return False

    with open(html_path, "r", encoding="utf-8") as f:
        html = f.read()

    # Replace the data constants
    import re

    def replace_const(html, name, new_value):
        pattern = name + r'=\{.*?\};'
        replacement = name + "=" + json.dumps(new_value, ensure_ascii=False) + ";"
        result = re.sub(pattern, replacement, html, flags=re.DOTALL)
        if result == html:
            print(f"  WARNING: Could not replace {name}")
        else:
            print(f"  Updated {name}")
        return result

    def replace_array_const(html, name, new_value):
        pattern = name + r'=\[.*?\];'
        replacement = name + "=" + json.dumps(new_value, ensure_ascii=False) + ";"
        result = re.sub(pattern, replacement, html, flags=re.DOTALL)
        if result == html:
            print(f"  WARNING: Could not replace {name}")
        else:
            print(f"  Updated {name}")
        return result

    html = replace_const(html, "const LD", league_data)
    html = replace_const(html, "const WD", WEEKLY_SCORES)
    html = replace_array_const(html, "const MT", MATCHUPS)

    # Update the week badge
    html = re.sub(
        r'2026 &middot; Week \d+',
        f'2026 &middot; Week {CURRENT_WEEK}',
        html
    )

    # Update last-refreshed timestamp in subtitle
    ts = datetime.utcnow().strftime("%b %d %Y %H:%M UTC")
    html = re.sub(
        r'Live from Yahoo Fantasy',
        f'Live from Yahoo Fantasy &middot; Updated {ts}',
        html
    )

    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"  Saved {html_path} ({len(html):,} chars)")
    return True

# ----------------------------------------------------------------
# MAIN
# ----------------------------------------------------------------
def main():
    print("=" * 60)
    print("Barry Ballstein's Boys — Nightly Data Fetch")
    print(f"  {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 60)

    bat_stats = fetch_batter_stats()
    pit_stats = fetch_pitcher_stats()

    if not bat_stats and not pit_stats:
        print("\nERROR: No data fetched. Cookie may be expired.")
        print("Action needed: refresh YAHOO_COOKIE in GitHub Secrets.")
        raise SystemExit(1)

    league_data = build_league_data(bat_stats, pit_stats)
    success = update_html(league_data)

    if success:
        print("\nDone! Dashboard updated successfully.")
    else:
        raise SystemExit(1)

if __name__ == "__main__":
    main()

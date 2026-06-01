#!/usr/bin/env python3
"""
MLB Wins Pool 2026 — Data Fetcher
Runs nightly via GitHub Actions (or locally).
Writes: data.json, monthly_snapshots.json
"""

import json
import os
import datetime
import urllib.request
import csv
import io

# ─────────────────────────────────────────────
# CONFIG — update draft each season
# ─────────────────────────────────────────────

PROJECTIONS_CSV_URL = (
    "https://docs.google.com/spreadsheets/d/e/"
    "2PACX-1vT7QIsrI-na6em_whpQCyuwpsJDHfxzuQWVqUmBPZxlNjicA7tW-416Yj06h8TuJarR4_wR47lJjtSx"
    "/pub?gid=1619309557&single=true&output=csv"
)

DRAFT = [
    {"pick": 1,  "owner": "Jimmy",  "team": "LAD", "name": "Los Angeles Dodgers",    "w2025": 93,  "proj_w": 96},
    {"pick": 2,  "owner": "Lon",    "team": "NYM", "name": "New York Mets",           "w2025": 83,  "proj_w": 88},
    {"pick": 3,  "owner": "Ant",    "team": "NYY", "name": "New York Yankees",        "w2025": 94,  "proj_w": 87},
    {"pick": 4,  "owner": "Marz",   "team": "PHI", "name": "Philadelphia Phillies",   "w2025": 96,  "proj_w": 87},
    {"pick": 5,  "owner": "Phil",   "team": "SEA", "name": "Seattle Mariners",        "w2025": 90,  "proj_w": 88},
    {"pick": 6,  "owner": "Dom",    "team": "TOR", "name": "Toronto Blue Jays",       "w2025": 94,  "proj_w": 86},
    {"pick": 7,  "owner": "Fur",    "team": "CHC", "name": "Chicago Cubs",            "w2025": 92,  "proj_w": 84},
    {"pick": 8,  "owner": "Adam B", "team": "ATL", "name": "Atlanta Braves",          "w2025": 76,  "proj_w": 88},
    {"pick": 9,  "owner": "Fig",    "team": "BOS", "name": "Boston Red Sox",          "w2025": 89,  "proj_w": 85},
    {"pick": 10, "owner": "Kev",    "team": "DET", "name": "Detroit Tigers",          "w2025": 87,  "proj_w": 86},
    {"pick": 11, "owner": "Kev",    "team": "HOU", "name": "Houston Astros",          "w2025": 87,  "proj_w": 81},
    {"pick": 12, "owner": "Fig",    "team": "MIL", "name": "Milwaukee Brewers",       "w2025": 97,  "proj_w": 81},
    {"pick": 13, "owner": "Adam B", "team": "BAL", "name": "Baltimore Orioles",       "w2025": 75,  "proj_w": 84},
    {"pick": 14, "owner": "Fur",    "team": "TEX", "name": "Texas Rangers",           "w2025": 81,  "proj_w": 81},
    {"pick": 15, "owner": "Dom",    "team": "KCR", "name": "Kansas City Royals",      "w2025": 82,  "proj_w": 81},
    {"pick": 16, "owner": "Phil",   "team": "SFG", "name": "San Francisco Giants",    "w2025": 81,  "proj_w": 82},
    {"pick": 17, "owner": "Marz",   "team": "ARI", "name": "Arizona Diamondbacks",    "w2025": 80,  "proj_w": 81},
    {"pick": 18, "owner": "Ant",    "team": "CLE", "name": "Cleveland Guardians",     "w2025": 88,  "proj_w": 76},
    {"pick": 19, "owner": "Lon",    "team": "SDP", "name": "San Diego Padres",        "w2025": 90,  "proj_w": 79},
    {"pick": 20, "owner": "Jimmy",  "team": "TBR", "name": "Tampa Bay Rays",          "w2025": 77,  "proj_w": 81},
    {"pick": 21, "owner": "Jimmy",  "team": "STL", "name": "St. Louis Cardinals",     "w2025": 78,  "proj_w": 75},
    {"pick": 22, "owner": "Lon",    "team": "CIN", "name": "Cincinnati Reds",         "w2025": 83,  "proj_w": 78},
    {"pick": 23, "owner": "Ant",    "team": "PIT", "name": "Pittsburgh Pirates",      "w2025": 71,  "proj_w": 82},
    {"pick": 24, "owner": "Marz",   "team": "PIT", "name": "Pittsburgh Pirates",      "w2025": 71,  "proj_w": 82},
    {"pick": 25, "owner": "Phil",   "team": "CIN", "name": "Cincinnati Reds",         "w2025": 83,  "proj_w": 78},
    {"pick": 26, "owner": "Dom",    "team": "ATH", "name": "Oakland A's",             "w2025": 76,  "proj_w": 80},
    {"pick": 27, "owner": "Fur",    "team": "ATH", "name": "Oakland A's",             "w2025": 76,  "proj_w": 80},
    {"pick": 28, "owner": "Adam B", "team": "STL", "name": "St. Louis Cardinals",     "w2025": 78,  "proj_w": 75},
    {"pick": 29, "owner": "Fig",    "team": "MIA", "name": "Miami Marlins",           "w2025": 79,  "proj_w": 75},
    {"pick": 30, "owner": "Kev",    "team": "MIN", "name": "Minnesota Twins",         "w2025": 70,  "proj_w": 79},
]

NOT_PICKED = [
    {"team": "CHW", "name": "Chicago White Sox",    "w2025": 41,  "proj_w": 71},
    {"team": "COL", "name": "Colorado Rockies",     "w2025": 61,  "proj_w": 62},
    {"team": "LAA", "name": "Los Angeles Angels",   "w2025": 69,  "proj_w": 66},
    {"team": "WSN", "name": "Washington Nationals", "w2025": 72,  "proj_w": 67},
]

# MLB Stats API team ID map
TEAM_ID_MAP = {
    "LAD": 119, "NYM": 121, "NYY": 147, "PHI": 143, "SEA": 136,
    "TOR": 141, "CHC": 112, "ATL": 144, "BOS": 111, "DET": 116,
    "HOU": 117, "MIL": 158, "BAL": 110, "TEX": 140, "KCR": 118,
    "SFG": 137, "ARI": 109, "CLE": 114, "SDP": 135, "TBR": 139,
    "STL": 138, "CIN": 113, "PIT": 134, "ATH": 133, "MIA": 146,
    "MIN": 142, "CHW": 145, "COL": 115, "LAA": 108, "WSN": 120,
}

# Month grouping rules
# Mar/Apr combined, Sep/Oct combined
MONTH_GROUPS = {
    3: "apr", 4: "apr",
    5: "may", 6: "jun", 7: "jul", 8: "aug",
    9: "sep", 10: "sep",
}
MONTH_LABELS = {
    "apr": "March/April", "may": "May", "jun": "June",
    "jul": "July", "aug": "August", "sep": "Sept/Oct",
}
MONTH_ORDER = ["apr", "may", "jun", "jul", "aug", "sep"]

SNAPSHOTS_FILE = "monthly_snapshots.json"
DATA_FILE = "data.json"

# ─────────────────────────────────────────────
# SEEDED APRIL SNAPSHOT (from 2026 Excel file)
# ─────────────────────────────────────────────
APRIL_SEED = {
    "ATL": {"w": 22, "l": 10, "rd": 66},
    "BAL": {"w": 15, "l": 16, "rd": -12},
    "STL": {"w": 18, "l": 13, "rd": -3},
    "NYY": {"w": 20, "l": 11, "rd": 47},
    "CLE": {"w": 16, "l": 16, "rd": -8},
    "PIT": {"w": 16, "l": 16, "rd": 12},
    "TOR": {"w": 14, "l": 17, "rd": -24},
    "KCR": {"w": 12, "l": 19, "rd": -22},
    "ATH": {"w": 17, "l": 14, "rd": -5},
    "BOS": {"w": 12, "l": 19, "rd": -16},
    "MIL": {"w": 16, "l": 14, "rd": 38},
    "MIA": {"w": 15, "l": 16, "rd": 1},
    "CHC": {"w": 19, "l": 12, "rd": 35},
    "TEX": {"w": 15, "l": 16, "rd": 8},
    "LAD": {"w": 20, "l": 11, "rd": 66},
    "TBR": {"w": 18, "l": 12, "rd": -1},
    "DET": {"w": 16, "l": 16, "rd": 9},
    "HOU": {"w": 12, "l": 20, "rd": -26},
    "MIN": {"w": 14, "l": 18, "rd": 5},
    "NYM": {"w": 10, "l": 21, "rd": -35},
    "SDP": {"w": 19, "l": 11, "rd": 8},
    "CIN": {"w": 20, "l": 11, "rd": -3},
    "PHI": {"w": 12, "l": 19, "rd": -45},
    "ARI": {"w": 16, "l": 14, "rd": -24},
    "SEA": {"w": 16, "l": 16, "rd": 7},
    "SFG": {"w": 13, "l": 18, "rd": -26},
}


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def fetch_url(url, timeout=15):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8")


def load_snapshots():
    if os.path.exists(SNAPSHOTS_FILE):
        with open(SNAPSHOTS_FILE) as f:
            return json.load(f)
    # First run — seed April
    snapshots = {"apr": APRIL_SEED}
    save_snapshots(snapshots)
    return snapshots


def save_snapshots(snapshots):
    with open(SNAPSHOTS_FILE, "w") as f:
        json.dump(snapshots, f, indent=2)


def get_current_period(today):
    """Return the month key for the current in-progress period."""
    return MONTH_GROUPS.get(today.month)


def get_completed_periods(today):
    """Return list of month keys that are fully completed before today's period."""
    current = get_current_period(today)
    completed = []
    for key in MONTH_ORDER:
        if key == current:
            break
        completed.append(key)
    return completed


def should_snapshot_today(today, snapshots):
    """
    Returns the period key to snapshot if today is the 1st of a new period
    and the previous period hasn't been snapshotted yet.
    """
    if today.day != 1:
        return None
    # What period just ended?
    yesterday = today - datetime.timedelta(days=1)
    ended_period = MONTH_GROUPS.get(yesterday.month)
    if ended_period and ended_period not in snapshots:
        return ended_period
    return None


# ─────────────────────────────────────────────
# MLB STATS API
# ─────────────────────────────────────────────

def fetch_mlb_standings():
    """Fetch current MLB standings from statsapi.mlb.com"""
    url = "https://statsapi.mlb.com/api/v1/standings?leagueId=103,104&season=2026&standingsTypes=regularSeason"
    try:
        raw = fetch_url(url)
        data = json.loads(raw)
    except Exception as e:
        print(f"ERROR fetching MLB standings: {e}")
        return {}

    teams = {}
    for division in data.get("records", []):
        for entry in division.get("teamRecords", []):
            tid = entry["team"]["id"]
            # Find abbreviation
            abbr = None
            for k, v in TEAM_ID_MAP.items():
                if v == tid:
                    abbr = k
                    break
            if not abbr:
                continue
            w = entry["wins"]
            l = entry["losses"]
            rd = entry.get("runDifferential", 0)
            pct = round(w / (w + l), 6) if (w + l) > 0 else 0
            teams[abbr] = {"w": w, "l": l, "rd": rd, "pct": pct}
    return teams


# ─────────────────────────────────────────────
# PROJECTIONS CSV
# ─────────────────────────────────────────────

def fetch_projections():
    """
    Fetch projections from published Google Sheet CSV.
    Expected columns: team abbr, full name, SimW, SimL, SimW%, DC RS, DC RA,
                      Div%, WC%, Playoff%, PAdj%, WS%, D1%, D7%
    Rows with division headers are skipped (no numeric SimW).
    """
    proj = {}
    try:
        raw = fetch_url(PROJECTIONS_CSV_URL)
        reader = csv.reader(io.StringIO(raw))
        rows = list(reader)
        for row in rows:
            if len(row) < 5:
                continue
            abbr = row[0].strip()
            if not abbr or len(abbr) > 4:
                continue
            try:
                sim_w = float(row[2])
                sim_l = float(row[3])
                sim_pct = float(row[4])
                playoff_pct = float(row[9]) if len(row) > 9 else 0
                ws_pct = float(row[11]) if len(row) > 11 else 0
            except (ValueError, IndexError):
                continue
            proj[abbr] = {
                "sim_w": sim_w,
                "sim_l": sim_l,
                "sim_pct": sim_pct,
                "playoff_pct": playoff_pct,
                "ws_pct": ws_pct,
            }
    except Exception as e:
        print(f"ERROR fetching projections: {e}")
    return proj


# ─────────────────────────────────────────────
# MONTHLY CALCULATIONS
# ─────────────────────────────────────────────

def calc_monthly_stats(live_teams, snapshots, today):
    """
    For each team, compute stats per period:
    - Completed periods: from snapshots
    - Current period: cumulative minus sum of completed frozen months
    Returns dict keyed by team abbr -> {period_key: {w, l, rd, pct}}
    """
    current_period = get_current_period(today)
    completed = get_completed_periods(today)

    monthly = {}
    all_teams = set(t["team"] for t in DRAFT)

    for abbr in all_teams:
        monthly[abbr] = {}
        # Fill completed periods from snapshots
        for period in completed:
            snap = snapshots.get(period, {}).get(abbr, {"w": 0, "l": 0, "rd": 0})
            g = snap["w"] + snap["l"]
            monthly[abbr][period] = {
                "w": snap["w"], "l": snap["l"], "rd": snap["rd"],
                "pct": round(snap["w"] / g, 4) if g > 0 else 0,
            }

        # Current period = cumulative - completed
        if current_period and abbr in live_teams:
            live = live_teams[abbr]
            cum_w = live["w"]
            cum_l = live["l"]
            cum_rd = live["rd"]

            frozen_w = sum(snapshots.get(p, {}).get(abbr, {}).get("w", 0) for p in completed)
            frozen_l = sum(snapshots.get(p, {}).get(abbr, {}).get("l", 0) for p in completed)
            frozen_rd = sum(snapshots.get(p, {}).get(abbr, {}).get("rd", 0) for p in completed)

            cur_w = max(0, cum_w - frozen_w)
            cur_l = max(0, cum_l - frozen_l)
            cur_rd = cum_rd - frozen_rd
            cur_g = cur_w + cur_l
            monthly[abbr][current_period] = {
                "w": cur_w, "l": cur_l, "rd": cur_rd,
                "pct": round(cur_w / cur_g, 4) if cur_g > 0 else 0,
            }

    return monthly


def calc_owner_monthly(monthly_by_team):
    """Aggregate per-team monthly stats up to owner level."""
    owner_monthly = {}
    for pick in DRAFT:
        owner = pick["owner"]
        team = pick["team"]
        if owner not in owner_monthly:
            owner_monthly[owner] = {}
        team_data = monthly_by_team.get(team, {})
        for period, stats in team_data.items():
            if period not in owner_monthly[owner]:
                owner_monthly[owner][period] = {"w": 0, "l": 0, "rd": 0}
            owner_monthly[owner][period]["w"] += stats["w"]
            owner_monthly[owner][period]["l"] += stats["l"]
            owner_monthly[owner][period]["rd"] += stats["rd"]

    # Compute pct for each owner period
    for owner in owner_monthly:
        for period in owner_monthly[owner]:
            d = owner_monthly[owner][period]
            g = d["w"] + d["l"]
            d["pct"] = round(d["w"] / g, 4) if g > 0 else 0

    return owner_monthly


# ─────────────────────────────────────────────
# OWNER STANDINGS
# ─────────────────────────────────────────────

def build_owner_standings(live_teams, projections):
    owners = {}
    for pick in DRAFT:
        owner = pick["owner"]
        team = pick["team"]
        if owner not in owners:
            owners[owner] = {
                "owner": owner,
                "teams": [],
                "w": 0, "l": 0, "rd": 0,
                "proj_sim_pct": 0.0,
                "g_played": 0, "g_rem": 0,
            }
        owners[owner]["teams"].append(team)

        if team in live_teams:
            g = live_teams[team]["w"] + live_teams[team]["l"]
            owners[owner]["w"] += live_teams[team]["w"]
            owners[owner]["l"] += live_teams[team]["l"]
            owners[owner]["rd"] += live_teams[team]["rd"]
            owners[owner]["g_played"] += g
            owners[owner]["g_rem"] += max(0, 162 - g)

        if team in projections:
            owners[owner]["proj_sim_pct"] += projections[team]["sim_pct"]

    result = []
    for owner, d in owners.items():
        g = d["w"] + d["l"]
        pct = round(d["w"] / g, 6) if g > 0 else 0
        avg_proj_pct = round(d["proj_sim_pct"] / 3, 6)
        proj_wins = round(avg_proj_pct * (d["g_played"] + d["g_rem"]), 1)
        result.append({
            "owner": owner,
            "teams": d["teams"],
            "w": d["w"], "l": d["l"], "rd": d["rd"],
            "pct": pct,
            "proj_pct": avg_proj_pct,
            "proj_wins": proj_wins,
            "g_rem": d["g_rem"],
        })

    result.sort(key=lambda x: (x["pct"], x["rd"]), reverse=True)

    if result:
        leader_pw = result[0]["proj_wins"]
        for i, row in enumerate(result):
            row["rank"] = i + 1
            row["gb"] = round(
                ((result[0]["w"] - result[0]["l"]) - (row["w"] - row["l"])) / 2, 1
            ) if i > 0 else 0
            row["proj_wins_behind"] = round(leader_pw - row["proj_wins"], 1)

    return result


# ─────────────────────────────────────────────
# DRAFT VALUE (Best/Worst Pick)
# ─────────────────────────────────────────────

def calc_draft_value(live_teams):
    """
    For each pick slot N, compare the owner's actual team win%
    vs the Nth-best team's win% in the entire league.
    """
    # Get all 30 drafted teams sorted by current pct desc
    all_team_pcts = []
    seen = set()
    for pick in DRAFT:
        team = pick["team"]
        key = team  # Teams picked twice will have same pct — that's fine
        if team in live_teams:
            pct = live_teams[team]["pct"]
        else:
            pct = 0
        all_team_pcts.append(pct)

    # Sort descending — Nth-best is index N-1
    ranked_pcts = sorted(all_team_pcts, reverse=True)

    results = []
    for pick in DRAFT:
        team = pick["team"]
        actual_pct = live_teams[team]["pct"] if team in live_teams else 0
        optimal_pct = ranked_pcts[pick["pick"] - 1]
        value = round(actual_pct - optimal_pct, 6)
        results.append({
            "pick": pick["pick"],
            "owner": pick["owner"],
            "team": team,
            "name": pick["name"],
            "actual_pct": round(actual_pct, 6),
            "optimal_pct": round(optimal_pct, 6),
            "value": value,
        })

    return results


# ─────────────────────────────────────────────
# TEAM-LEVEL DATA
# ─────────────────────────────────────────────

def build_team_rows(live_teams, projections):
    rows = []
    for pick in DRAFT:
        team = pick["team"]
        live = live_teams.get(team, {"w": 0, "l": 0, "rd": 0, "pct": 0})
        proj = projections.get(team, {"sim_w": 0, "sim_l": 0, "sim_pct": 0, "playoff_pct": 0, "ws_pct": 0})
        rows.append({
            "pick": pick["pick"],
            "owner": pick["owner"],
            "team": team,
            "name": pick["name"],
            "w2025": pick["w2025"],
            "proj_w": pick["proj_w"],
            "w": live["w"],
            "l": live["l"],
            "rd": live["rd"],
            "pct": live["pct"],
            "sim_w": proj["sim_w"],
            "sim_pct": proj["sim_pct"],
            "playoff_pct": proj["playoff_pct"],
            "ws_pct": proj["ws_pct"],
            "proj_change": round(live["pct"] - proj["sim_pct"], 6),
        })
    return rows


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    today = datetime.date.today()
    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"Running fetch_data.py at {now_str}")

    # Load or initialise snapshots (seeds April on first run)
    snapshots = load_snapshots()

    # Fetch live data
    print("Fetching MLB standings...")
    live_teams = fetch_mlb_standings()
    if not live_teams:
        print("WARNING: No live team data retrieved. Aborting to avoid overwriting good data.")
        return

    print(f"  Got {len(live_teams)} teams")

    # Snapshot check: if today is the 1st of a new period, freeze yesterday's standings
    period_to_snap = should_snapshot_today(today, snapshots)
    if period_to_snap:
        print(f"  Snapshotting completed period: {period_to_snap}")
        # Get all prior completed periods so we can store period-ONLY deltas
        prior_periods = get_completed_periods(today)  # excludes the period being snapped
        snap = {}
        for abbr, data in live_teams.items():
            # Subtract all prior frozen periods to get this period only
            prior_w  = sum(snapshots.get(p, {}).get(abbr, {}).get("w",  0) for p in prior_periods)
            prior_l  = sum(snapshots.get(p, {}).get(abbr, {}).get("l",  0) for p in prior_periods)
            prior_rd = sum(snapshots.get(p, {}).get(abbr, {}).get("rd", 0) for p in prior_periods)
            snap[abbr] = {
                "w":  max(0, data["w"]  - prior_w),
                "l":  max(0, data["l"]  - prior_l),
                "rd": data["rd"] - prior_rd,
            }
        snapshots[period_to_snap] = snap
        save_snapshots(snapshots)
        print(f"  Snapshot saved for {period_to_snap} (period-only deltas)")

    print("Fetching projections...")
    projections = fetch_projections()
    print(f"  Got {len(projections)} teams with projections")

    # Build data structures
    monthly_by_team = calc_monthly_stats(live_teams, snapshots, today)
    owner_monthly = calc_owner_monthly(monthly_by_team)
    standings = build_owner_standings(live_teams, projections)
    team_rows = build_team_rows(live_teams, projections)
    draft_value = calc_draft_value(live_teams)

    # Not-picked team rows
    not_picked_rows = []
    for np in NOT_PICKED:
        t = np["team"]
        live = live_teams.get(t, {"w": 0, "l": 0, "rd": 0, "pct": 0})
        proj = projections.get(t, {"sim_w": 0, "sim_pct": 0})
        not_picked_rows.append({
            "team": t, "name": np["name"],
            "w2025": np["w2025"], "proj_w": np["proj_w"],
            "w": live["w"], "l": live["l"], "rd": live["rd"], "pct": live["pct"],
            "sim_pct": proj["sim_pct"],
        })

    # Assemble output
    output = {
        "updated": now_str,
        "season": 2026,
        "standings": standings,
        "team_rows": team_rows,
        "not_picked": not_picked_rows,
        "draft_value": draft_value,
        "owner_monthly": owner_monthly,
        "monthly_by_team": {
            abbr: {p: v for p, v in data.items()}
            for abbr, data in monthly_by_team.items()
        },
        "month_order": MONTH_ORDER,
        "month_labels": MONTH_LABELS,
        "snapshots_frozen": list(snapshots.keys()),
        "draft": DRAFT,
        "not_picked_config": NOT_PICKED,
    }

    with open(DATA_FILE, "w") as f:
        json.dump(output, f, indent=2)

    print(f"Wrote {DATA_FILE}")
    print(f"Standings leader: {standings[0]['owner']} ({standings[0]['pct']:.3f})")
    print("Done.")


if __name__ == "__main__":
    main()

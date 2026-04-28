#!/usr/bin/env python3
"""
Barry Ballstein's Boys -- Nightly Data Fetcher

FULLY AUTOMATED nightly:
  Batter/pitcher fantasy points and all computed league metrics.

MANUAL UPDATE each week (~5 minutes):
  1. Go to Yahoo Fantasy -> Scoreboard for the completed week
  2. Record each team's score (use decimal values from Yahoo)
  3. Update CURRENT_WEEK, RECORDS, PA_TOTALS, WEEKLY_SCORES, MATCHUPS below
  4. Commit fetch_data.py to GitHub -> workflow runs automatically

NOTE: Individual player stats (Teams/Players pages) update from the
Excel spreadsheet upload. Upload updated spreadsheet to GitHub weekly
and the script reads it automatically.
"""

import os, re, json, glob, math
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timezone

# ================================================================
# WEEKLY CONFIG -- UPDATE THIS SECTION EACH WEEK
# ================================================================
CURRENT_WEEK = 5

RECORDS = {
    "Pat":     {"w": 3, "l": 2},
    "Sal":     {"w": 4, "l": 1},
    "Charlie": {"w": 4, "l": 1},
    "Fur":     {"w": 1, "l": 4},
    "Oded":    {"w": 2, "l": 3},
    "Jimmy":   {"w": 2, "l": 3},
    "Phil":    {"w": 3, "l": 2},
    "Marz":    {"w": 1, "l": 4},
}

PA_TOTALS = {
    "Pat":     1506,
    "Sal":     1363,
    "Charlie": 1375,
    "Fur":     1528,
    "Oded":    1502,
    "Jimmy":   1546,
    "Phil":    1386,
    "Marz":    1379,
}

WEEKLY_SCORES = {
    "Pat":     [216, 382, 309, 323, 348],
    "Sal":     [161, 272, 336, 338, 317],
    "Charlie": [214, 290, 263, 375, 298],
    "Fur":     [140, 376, 374, 258, 274],
    "Oded":    [177, 284, 336, 369, 312],
    "Jimmy":   [132, 342, 367, 332, 338],
    "Phil":    [208, 257, 304, 324, 308],
    "Marz":    [172, 308, 255, 369, 227],
}

MATCHUPS = [
    {"week":1,"home":"Marz",    "hpts":172,"away":"Phil",    "apts":208},
    {"week":1,"home":"Sal",     "hpts":161,"away":"Fur",     "apts":140},
    {"week":1,"home":"Jimmy",   "hpts":132,"away":"Pat",     "apts":216},
    {"week":1,"home":"Oded",    "hpts":177,"away":"Charlie", "apts":214},
    {"week":2,"home":"Marz",    "hpts":308,"away":"Jimmy",   "apts":342},
    {"week":2,"home":"Sal",     "hpts":272,"away":"Oded",    "apts":284},
    {"week":2,"home":"Pat",     "hpts":382,"away":"Fur",     "apts":376},
    {"week":2,"home":"Charlie", "hpts":290,"away":"Phil",    "apts":257},
    {"week":3,"home":"Marz",    "hpts":255,"away":"Charlie", "apts":263},
    {"week":3,"home":"Sal",     "hpts":336,"away":"Pat",     "apts":309},
    {"week":3,"home":"Jimmy",   "hpts":367,"away":"Oded",    "apts":336},
    {"week":3,"home":"Fur",     "hpts":374,"away":"Phil",    "apts":304},
    {"week":4,"home":"Marz",    "hpts":369,"away":"Fur",     "apts":258},
    {"week":4,"home":"Sal",     "hpts":338,"away":"Jimmy",   "apts":332},
    {"week":4,"home":"Pat",     "hpts":323,"away":"Phil",    "apts":324},
    {"week":4,"home":"Oded",    "hpts":369,"away":"Charlie", "apts":375},
    {"week":5,"home":"Marz",    "hpts":227,"away":"Phil",    "apts":308},
    {"week":5,"home":"Sal",     "hpts":317,"away":"Charlie", "apts":298},
    {"week":5,"home":"Jimmy",   "hpts":338,"away":"Pat",     "apts":348},
    {"week":5,"home":"Oded",    "hpts":312,"away":"Fur",     "apts":274},
]
# ================================================================
# END OF WEEKLY CONFIG
# ================================================================

LEAGUE_ID = "96087"

COOKIE = os.environ.get("YAHOO_COOKIE", "")
if not COOKIE:
    raise SystemExit("FATAL: YAHOO_COOKIE environment variable not set.")

HEADERS = {
    "Cookie": COOKIE.strip(),
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://baseball.fantasysports.yahoo.com/",
}

BAT_MULT = {
    "R": 1, "1B": 1, "2B": 2, "3B": 3, "HR": 3,
    "RBI": 0.5, "SB": 3, "BB": 0.5, "IBB": 0.5, "HBP": 0.5
}
PIT_MULT = {
    "IP": 1, "W": 12, "L": -6, "SV": 7, "ER": -0.5,
    "K": 1.5, "HLD": 2, "NH": 10, "PG": 15, "BSV": -3.5
}

OWNER_MAP = {
    "Chicks Dig The Holds": "Pat",
    "Deez Nuts":            "Sal",
    "Deez 🥜":          "Sal",  # Yahoo uses emoji
    "Dirty Meat Swing":     "Charlie",
    "Frank Latera":         "Fur",
    "Santolos":             "Oded",
    "Team Mush":            "Jimmy",
    "Tony's Calling":       "Phil",
    "YOU ALWAYS DO THIS!":  "Marz",
}

DISPLAYS = {
    "Pat":     "Chicks Dig The Holds",
    "Sal":     "Deez Nuts",
    "Charlie": "Dirty Meat Swing",
    "Fur":     "Frank Latera",
    "Oded":    "Santolos",
    "Jimmy":   "Team Mush",
    "Phil":    "Tony's Calling",
    "Marz":    "YOU ALWAYS DO THIS!",
}

OWNERS = list(OWNER_MAP.values())


def validate_config():
    errors = []
    for o in OWNERS:
        if o not in RECORDS:
            errors.append("RECORDS missing: " + o)
        if o not in PA_TOTALS:
            errors.append("PA_TOTALS missing: " + o)
        if o not in WEEKLY_SCORES:
            errors.append("WEEKLY_SCORES missing: " + o)
        elif len(WEEKLY_SCORES[o]) != CURRENT_WEEK:
            errors.append(
                "WEEKLY_SCORES[" + o + "] has " + str(len(WEEKLY_SCORES[o])) +
                " entries but CURRENT_WEEK=" + str(CURRENT_WEEK)
            )
    completed = set(range(1, CURRENT_WEEK))
    in_matchups = set(m["week"] for m in MATCHUPS)
    for w in completed:
        if w not in in_matchups:
            errors.append("MATCHUPS missing week " + str(w))
    if errors:
        print("\nCONFIG ERRORS -- fix before committing:")
        for e in errors:
            print("  * " + e)
        raise SystemExit("FATAL: Config validation failed.")
    print("  Config OK: Week " + str(CURRENT_WEEK) +
          ", " + str(len(MATCHUPS)) + " matchups")


def fetch_url(url, label):
    print("  GET " + label)
    try:
        r = requests.get(url, headers=HEADERS, timeout=30)
    except Exception as e:
        raise SystemExit("FATAL: Network error fetching " + label + ": " + str(e))
    print("  " + str(r.status_code) + " (" + str(len(r.text)) + " bytes)")
    if r.status_code in (401, 403):
        raise SystemExit(
            "FATAL: Yahoo returned " + str(r.status_code) + ".\n"
            "Cookie has expired. Update YAHOO_COOKIE in GitHub Secrets:\n"
            "  Settings -> Secrets and variables -> Actions -> YAHOO_COOKIE"
        )
    if r.status_code != 200:
        raise SystemExit("FATAL: Unexpected HTTP " + str(r.status_code) + " from " + label)
    return BeautifulSoup(r.text, "lxml")


def sf(v):
    try:
        return float(str(v).replace(",", "").strip() or 0)
    except Exception:
        return 0.0


def parse_table(soup, idx=0):
    tables = soup.find_all("table") if soup else []
    if not tables or idx >= len(tables):
        return [], []
    headers = []
    data = []
    for row in tables[idx].find_all("tr"):
        cells = [c.get_text(strip=True) for c in row.find_all(["th", "td"])]
        if not headers and cells:
            headers = cells
        elif cells and len(cells) == len(headers):
            data.append(dict(zip(headers, cells)))
    return headers, data


def fuzzy_match(a, b):
    a = a.lower()
    b = b.lower()
    # Use first 4 chars to handle "Deez Nuts" vs "Deez (emoji)" both matching "deez"
    return a[:10] in b or b[:10] in a or a[:4] == b[:4]


def norm_cdf(z):
    return (1.0 + math.erf(z / math.sqrt(2.0))) / 2.0


def zscore_pct(vals, v, lower_is_better=False):
    n = len(vals)
    if n < 2:
        return 50.0
    mean = sum(vals) / n
    var = sum((x - mean) ** 2 for x in vals) / n
    sd = math.sqrt(var)
    if sd == 0:
        return 50.0
    z = (v - mean) / sd
    if lower_is_better:
        z = -z
    return round(norm_cdf(z) * 100, 1)


def clean_name(raw):
    if not raw:
        return ""
    s = re.sub(r"[\ue000-\uf8ff\n\r]", " ", str(raw))
    s = re.sub(r"(Player Note|DTDNew|New Player Note|IL\d+|NAPlayer Note|DTD)\s*", "", s)
    s = re.sub(r"[A-Z]{2,3}(?=\s*-\s*[A-Z,/0-9]).*", "", s)
    s = re.sub(r"\s*\d+:\d+.*", "", s)
    return re.sub(r"\s+", " ", s).strip()


def inject_js_var(html, var_name, value, is_array=False):
    open_ch  = "[" if is_array else "{"
    close_ch = "]" if is_array else "}"
    prefix   = var_name + "="
    idx = html.find(prefix)
    if idx == -1:
        print("  WARNING: " + var_name + " not found in HTML")
        return html
    val_start = idx + len(prefix)
    if val_start >= len(html) or html[val_start] != open_ch:
        print("  WARNING: " + var_name + " does not start with " + open_ch)
        return html
    depth = 0
    pos = val_start
    while pos < len(html):
        c = html[pos]
        if c == open_ch:
            depth += 1
        elif c == close_ch:
            depth -= 1
            if depth == 0:
                html = (html[:val_start] +
                        json.dumps(value, ensure_ascii=False) +
                        html[pos+1:])
                print("  Injected " + var_name)
                return html
        pos += 1
    print("  WARNING: " + var_name + " -- no matching close brace")
    return html


# ----------------------------------------------------------------
# FETCH: BATTER STATS
# ----------------------------------------------------------------
def fetch_bat_stats():
    print("\n[Batter Stats]")
    url = ("https://baseball.fantasysports.yahoo.com/b1/" +
           LEAGUE_ID + "/headtoheadstats?pt=B&type=stats")
    soup = fetch_url(url, "headtoheadstats batters")
    _, rows = parse_table(soup)
    if not rows:
        raise SystemExit("FATAL: Batter stats table empty. Check cookie.")
    result = {}
    for row in rows:
        team = list(row.values())[0]
        if not team or team.lower() in ["team name", "totals", ""]:
            continue
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
        print("    " + team + ": " + str(round(pts, 1)) + " pts")
    if not result:
        raise SystemExit("FATAL: No batter data parsed.")
    return result


# ----------------------------------------------------------------
# FETCH: PITCHER STATS
# ----------------------------------------------------------------
def fetch_pit_stats():
    print("\n[Pitcher Stats]")
    url = ("https://baseball.fantasysports.yahoo.com/b1/" +
           LEAGUE_ID + "/headtoheadstats?pt=P&type=stats")
    soup = fetch_url(url, "headtoheadstats pitchers")
    _, rows = parse_table(soup)
    if not rows:
        raise SystemExit("FATAL: Pitcher stats table empty. Check cookie.")
    result = {}
    for row in rows:
        team = list(row.values())[0]
        if not team or team.lower() in ["team name", "totals", ""]:
            continue
        pts = sum(sf(row.get(s, 0)) * m for s, m in PIT_MULT.items())
        result[team] = {"pts": round(pts, 1), "raw": row}
        print("    " + team + ": " + str(round(pts, 1)) + " pts")
    if not result:
        raise SystemExit("FATAL: No pitcher data parsed.")
    return result


# ----------------------------------------------------------------
# COMPUTE: LEAGUE DATA
# ----------------------------------------------------------------
def compute_league(bat_raw, pit_raw):
    print("\n[Computing league data]")

    bat_pts = {o: 0 for o in OWNERS}
    pit_pts = {o: 0 for o in OWNERS}
    bat_raw_by_owner = {}
    pit_raw_by_owner = {}
    for display, owner in OWNER_MAP.items():
        for live, data in bat_raw.items():
            if fuzzy_match(live, display):
                bat_pts[owner] = data["pts"]
                bat_raw_by_owner[owner] = data["raw"]
                break
        for live, data in pit_raw.items():
            if fuzzy_match(live, display):
                pit_pts[owner] = data["pts"]
                pit_raw_by_owner[owner] = data["raw"]
                break

    def compute_luck(owner):
        scores = WEEKLY_SCORES.get(owner, [])
        wins = total = 0
        for wi, score in enumerate(scores):
            if score == 0:
                continue
            for other in OWNERS:
                if other == owner:
                    continue
                other_s = WEEKLY_SCORES.get(other, [])
                if wi < len(other_s) and other_s[wi] > 0:
                    total += 1
                    if score > other_s[wi]:
                        wins += 1
        return round(wins / total, 4) if total > 0 else 0.0

    # Performance ratings using normal-distribution z-scores (matches Excel)
    bat_vals = [bat_pts[o] for o in OWNERS]
    pit_vals = [pit_pts[o] for o in OWNERS]

    league = {}
    for owner in OWNERS:
        rec   = RECORDS[owner]
        w, l  = rec["w"], rec["l"]
        pf    = sum(WEEKLY_SCORES[owner])
        pa    = PA_TOTALS.get(owner, 0)
        weeks = w + l
        # Pyth W: simulate H2H record vs all opponents each week (matches Excel method)
        wins_h2h = total_h2h = 0
        for wi, sc in enumerate(WEEKLY_SCORES[owner]):
            if sc == 0: continue
            for other in OWNERS:
                if other == owner: continue
                other_sc = WEEKLY_SCORES.get(other, [])
                if wi < len(other_sc) and other_sc[wi] > 0:
                    total_h2h += 1
                    if sc > other_sc[wi]: wins_h2h += 1
        pyth   = round((wins_h2h / total_h2h) * weeks, 3) if total_h2h > 0 else 0
        luck   = compute_luck(owner)

        perf_bat = zscore_pct(bat_vals, bat_pts[owner])
        perf_pit = zscore_pct(pit_vals, pit_pts[owner])
        # Total: z-score of (bat_pct + pit_pct) / 2, or geometric mean approach
        # Match Excel: total is z-score of combined score
        combined = [(bat_pts[o] + pit_pts[o]) for o in OWNERS]
        perf_tot = zscore_pct(combined, bat_pts[owner] + pit_pts[owner])

        league[owner] = {
            "display":  DISPLAYS[owner],
            "w": w, "l": l,
            "pf": pf, "pa": pa,
            "batters":  round(bat_pts[owner]),
            "pitchers": round(pit_pts[owner]),
            "luck": luck, "pyth": pyth,
            "owner": owner,
            "perfBat": perf_bat,
            "perfPit": perf_pit,
            "perfTot": perf_tot,
        }
        print("  " + owner + ": " + str(w) + "-" + str(l) +
              " PF=" + str(pf) + " PA=" + str(pa) +
              " bat=" + str(round(bat_pts[owner])) +
              " pit=" + str(round(pit_pts[owner])))

    return league, bat_raw_by_owner, pit_raw_by_owner


# ----------------------------------------------------------------
# COMPUTE: STATS TABLES (from live headtoheadstats data)
# ----------------------------------------------------------------
def compute_stats_tables(bat_raw_by_owner, pit_raw_by_owner, bat_pts, pit_pts):
    print("\n[Computing stats tables]")

    def disp(o):
        return DISPLAYS[o]

    # -- BATTERS --------------------------------------------------
    def bat_fp(o):
        r  = bat_raw_by_owner.get(o, {})
        h  = sf(r.get("H",  0))
        b2 = sf(r.get("2B", 0))
        b3 = sf(r.get("3B", 0))
        hr = sf(r.get("HR", 0))
        b1 = max(0, h - b2 - b3 - hr)
        rv  = sf(r.get("R",   0))
        rbi = sf(r.get("RBI", 0))
        sb  = sf(r.get("SB",  0))
        bb  = sf(r.get("BB",  0))
        ibb = sf(r.get("IBB", 0))
        hbp = sf(r.get("HBP", 0))
        pts = (rv*BAT_MULT["R"]   + b1*BAT_MULT["1B"]  + b2*BAT_MULT["2B"] +
               b3*BAT_MULT["3B"]  + hr*BAT_MULT["HR"]  + rbi*BAT_MULT["RBI"] +
               sb*BAT_MULT["SB"]  + bb*BAT_MULT["BB"]  + ibb*BAT_MULT["IBB"] +
               hbp*BAT_MULT["HBP"])
        return {"owner": o, "display": disp(o), "rank": 0,
                "fpts": round(pts, 1),
                "r":   round(rv*BAT_MULT["R"],    1),
                "b1":  round(b1*BAT_MULT["1B"],   1),
                "b2":  round(b2*BAT_MULT["2B"],   1),
                "b3":  round(b3*BAT_MULT["3B"],   1),
                "hr":  round(hr*BAT_MULT["HR"],   1),
                "rbi": round(rbi*BAT_MULT["RBI"], 1),
                "sb":  round(sb*BAT_MULT["SB"],   1),
                "bb":  round(bb*BAT_MULT["BB"],   1),
                "ibb": round(ibb*BAT_MULT["IBB"], 1),
                "hbp": round(hbp*BAT_MULT["HBP"], 1)}

    def bat_raw_r(o):
        r  = bat_raw_by_owner.get(o, {})
        h  = sf(r.get("H",  0))
        b2 = sf(r.get("2B", 0))
        b3 = sf(r.get("3B", 0))
        hr = sf(r.get("HR", 0))
        b1 = max(0, h - b2 - b3 - hr)
        return {"owner": o, "display": disp(o),
                "r":   int(sf(r.get("R",   0))),
                "b1":  int(b1),
                "b2":  int(b2), "b3": int(b3), "hr": int(hr),
                "rbi": int(sf(r.get("RBI", 0))),
                "sb":  int(sf(r.get("SB",  0))),
                "bb":  int(sf(r.get("BB",  0))),
                "ibb": int(sf(r.get("IBB", 0))),
                "hbp": int(sf(r.get("HBP", 0)))}

    # -- PITCHERS -------------------------------------------------
    def pit_fp(o):
        r   = pit_raw_by_owner.get(o, {})
        pts = sum(sf(r.get(s, 0)) * m for s, m in PIT_MULT.items())
        return {"owner": o, "display": disp(o), "rank": 0,
                "fpts": round(pts, 1),
                "ip":  round(sf(r.get("IP",  0)) * PIT_MULT["IP"],  1),
                "w":   round(sf(r.get("W",   0)) * PIT_MULT["W"],   1),
                "l":   round(sf(r.get("L",   0)) * PIT_MULT["L"],   1),
                "sv":  round(sf(r.get("SV",  0)) * PIT_MULT["SV"],  1),
                "er":  round(sf(r.get("ER",  0)) * PIT_MULT["ER"],  1),
                "k":   round(sf(r.get("K",   0)) * PIT_MULT["K"],   1),
                "hld": round(sf(r.get("HLD", 0)) * PIT_MULT["HLD"], 1),
                "nh":  round(sf(r.get("NH",  0)) * PIT_MULT["NH"],  1),
                "pg":  round(sf(r.get("PG",  0)) * PIT_MULT["PG"],  1),
                "bs":  round(sf(r.get("BSV", 0)) * PIT_MULT["BSV"], 1)}

    def pit_raw_r(o):
        r = pit_raw_by_owner.get(o, {})
        return {"owner": o, "display": disp(o),
                "ip":  round(sf(r.get("IP",  0)), 1),
                "w":   int(sf(r.get("W",   0))),
                "l":   int(sf(r.get("L",   0))),
                "sv":  int(sf(r.get("SV",  0))),
                "er":  int(sf(r.get("ER",  0))),
                "k":   int(sf(r.get("K",   0))),
                "hld": int(sf(r.get("HLD", 0))),
                "nh":  int(sf(r.get("NH",  0))),
                "pg":  int(sf(r.get("PG",  0))),
                "bs":  int(sf(r.get("BSV", 0)))}

    # -- RANKINGS (rank 1=best per category) ----------------------
    def rank_col(rows, key, lower_is_better=False):
        vals = [(row[key], row["owner"]) for row in rows]
        vals.sort(key=lambda x: x[0], reverse=not lower_is_better)
        ranks = {owner: i+1 for i, (_, owner) in enumerate(vals)}
        for row in rows:
            row[key] = ranks[row["owner"]]

    def bat_ranks(br_rows):
        import copy
        rows = [copy.copy(r) for r in br_rows]
        # Compute fpts rank
        fpts_vals = [bat_pts.get(r["owner"], 0) for r in rows]
        fpts_sorted = sorted(fpts_vals, reverse=True)
        for row in rows:
            row["fpts"] = fpts_sorted.index(bat_pts.get(row["owner"], 0)) + 1
        for key in ["r","b1","b2","b3","hr","rbi","sb","bb","ibb","hbp"]:
            rank_col(rows, key)
        return rows

    def pit_ranks(pr_rows):
        import copy
        rows = [copy.copy(r) for r in pr_rows]
        fpts_vals = [pit_pts.get(r["owner"], 0) for r in rows]
        fpts_sorted = sorted(fpts_vals, reverse=True)
        for row in rows:
            row["fpts"] = fpts_sorted.index(pit_pts.get(row["owner"], 0)) + 1
        for key in ["ip","w","k","hld","nh","pg","sv"]:
            rank_col(rows, key)
        for key in ["l","er","bs"]:
            rank_col(rows, key, lower_is_better=True)
        return rows

    # -- Z-SCORES using normal CDF (matches Excel) -----------------
    def bat_zscores(br_rows):
        import copy
        rows = [copy.copy(r) for r in br_rows]
        keys_lower = []
        keys = ["fpts","r","b1","b2","b3","hr","rbi","sb","bb","ibb","hbp"]
        # Get fpts values for z-score
        fpts_vals = [bat_pts.get(r["owner"], 0) for r in rows]
        for row in rows:
            row["fpts"] = zscore_pct(fpts_vals, bat_pts.get(row["owner"], 0))
        for key in ["r","b1","b2","b3","hr","rbi","sb","bb","ibb","hbp"]:
            vals = [r[key] for r in rows]
            for row in rows:
                row[key] = zscore_pct(vals, row[key])
        return rows

    def pit_zscores(pr_rows):
        import copy
        rows = [copy.copy(r) for r in pr_rows]
        fpts_vals = [pit_pts.get(r["owner"], 0) for r in rows]
        for row in rows:
            row["fpts"] = zscore_pct(fpts_vals, pit_pts.get(row["owner"], 0))
        for key in ["ip","w","sv","k","hld","nh","pg"]:
            vals = [r[key] for r in rows]
            for row in rows:
                row[key] = zscore_pct(vals, row[key])
        for key in ["l","er","bs"]:
            vals = [r[key] for r in rows]
            for row in rows:
                row[key] = zscore_pct(vals, row[key], lower_is_better=True)
        return rows

    # -- ADVANCED STATS --------------------------------------------
    # Advanced bat/pit stats need H/AB/GP which come from teamstats (player-level)
    # Since those pages are JS-rendered, we keep these from the HTML snapshot
    # They will be accurate from the weekly Excel upload

    # Build all tables
    BF_rows = sorted([bat_fp(o)    for o in OWNERS], key=lambda x: -x["fpts"])
    PF_rows = sorted([pit_fp(o)    for o in OWNERS], key=lambda x: -x["fpts"])
    BR_rows = [bat_raw_r(o)        for o in OWNERS]
    PR_rows = [pit_raw_r(o)        for o in OWNERS]

    # Add ranks to BF/PF
    for i, row in enumerate(BF_rows): row["rank"] = i + 1
    for i, row in enumerate(PF_rows): row["rank"] = i + 1

    BK_rows = bat_ranks(BR_rows)
    PK_rows = pit_ranks(PR_rows)
    BZ_rows = bat_zscores(BR_rows)
    PZ_rows = pit_zscores(PR_rows)

    return {
        "BF": BF_rows, "BR": BR_rows,
        "BK": BK_rows, "BZ": BZ_rows,
        "PF": PF_rows, "PR": PR_rows,
        "PK": PK_rows, "PZ": PZ_rows,
    }


# ----------------------------------------------------------------
# UPDATE HTML
# ----------------------------------------------------------------
def update_html(league, stats_tables):
    print("\n[Updating HTML]")
    candidates = ["barry_ballstein.html"] + glob.glob("barry_ballstein*.html")
    html_path  = next((f for f in candidates if os.path.exists(f)), None)
    if not html_path:
        raise SystemExit("FATAL: No barry_ballstein*.html found. Files: " +
                         str(os.listdir(".")))
    with open(html_path, "r", encoding="utf-8") as f:
        html = f.read()

    html = inject_js_var(html, "const LD", league)
    html = inject_js_var(html, "const WD", WEEKLY_SCORES)
    html = inject_js_var(html, "const MT", MATCHUPS,             is_array=True)
    html = inject_js_var(html, "const BF", stats_tables["BF"],  is_array=True)
    html = inject_js_var(html, "const BR", stats_tables["BR"],  is_array=True)
    html = inject_js_var(html, "const BK", stats_tables["BK"],  is_array=True)
    html = inject_js_var(html, "const BZ", stats_tables["BZ"],  is_array=True)
    html = inject_js_var(html, "const PF", stats_tables["PF"],  is_array=True)
    html = inject_js_var(html, "const PR", stats_tables["PR"],  is_array=True)
    html = inject_js_var(html, "const PK", stats_tables["PK"],  is_array=True)
    html = inject_js_var(html, "const PZ", stats_tables["PZ"],  is_array=True)
    # BA and PA (advanced stats) are not updated here -- they come from
    # the Excel upload and are already in the HTML from the last upload

    ts   = datetime.now(timezone.utc).strftime("%b %d %Y %H:%M UTC")
    html = re.sub(r"2026 [^W]*Week \d+",
                  "2026 &middot; Week " + str(CURRENT_WEEK), html)
    html = re.sub(r"Live from Yahoo Fantasy[^<]*",
                  "Live from Yahoo Fantasy &middot; Updated " + ts, html)

    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)
    print("  Saved " + html_path + " (" + str(len(html)) + " chars)")


# ----------------------------------------------------------------
# MAIN
# ----------------------------------------------------------------
def main():
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    print("=" * 60)
    print("Barry Ballstein's Boys -- Nightly Fetch  " + ts)
    print("=" * 60)

    print("\n[Validating config]")
    validate_config()

    bat_raw = fetch_bat_stats()
    pit_raw = fetch_pit_stats()

    league, bat_raw_by_owner, pit_raw_by_owner = compute_league(bat_raw, pit_raw)

    bat_pts = {o: league[o]["batters"] for o in OWNERS}
    pit_pts = {o: league[o]["pitchers"] for o in OWNERS}
    stats_tables = compute_stats_tables(bat_raw_by_owner, pit_raw_by_owner,
                                        bat_pts, pit_pts)

    update_html(league, stats_tables)

    print("\n" + "=" * 60)
    print("Done! Week " + str(CURRENT_WEEK) + " | " + ts)
    print("=" * 60)


if __name__ == "__main__":
    main()

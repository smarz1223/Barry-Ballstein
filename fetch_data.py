#!/usr/bin/env python3
"""
Barry Ballstein's Boys -- Nightly Data Fetcher

FULLY AUTOMATED nightly:
  Batter/pitcher fantasy points, player stats, advanced metrics,
  performance ratings, all stats tables.

MANUAL UPDATE each week (5 min):
  Update the WEEKLY CONFIG section below after each week completes.
"""

import os, re, json, glob
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
    "Charlie": {"w": 5, "l": 0},
    "Fur":     {"w": 1, "l": 4},
    "Oded":    {"w": 2, "l": 3},
    "Jimmy":   {"w": 2, "l": 3},
    "Phil":    {"w": 2, "l": 3},
    "Marz":    {"w": 1, "l": 4},
}

PA_TOTALS = {
    "Pat":     1169,
    "Sal":     1065,
    "Charlie": 1058,
    "Fur":     1217,
    "Oded":    1229,
    "Jimmy":   1197,
    "Phil":    1159,
    "Marz":    1071,
}

WEEKLY_SCORES = {
    "Pat":     [216, 383, 309, 323, 0],
    "Sal":     [161, 273, 336, 338, 0],
    "Charlie": [214, 290, 263, 375, 0],
    "Fur":     [140, 376, 374, 258, 0],
    "Oded":    [177, 284, 336, 369, 0],
    "Jimmy":   [132, 342, 367, 333, 0],
    "Phil":    [208, 257, 304, 325, 0],
    "Marz":    [173, 308, 255, 369, 0],
}

MATCHUPS = [
    {"week":1,"home":"Pat",     "hpts":216,"away":"Jimmy",   "apts":132},
    {"week":1,"home":"Charlie", "hpts":214,"away":"Oded",    "apts":177},
    {"week":1,"home":"Phil",    "hpts":208,"away":"Marz",    "apts":173},
    {"week":1,"home":"Sal",     "hpts":161,"away":"Fur",     "apts":140},
    {"week":2,"home":"Pat",     "hpts":383,"away":"Fur",     "apts":376},
    {"week":2,"home":"Jimmy",   "hpts":342,"away":"Marz",    "apts":308},
    {"week":2,"home":"Oded",    "hpts":284,"away":"Sal",     "apts":273},
    {"week":2,"home":"Charlie", "hpts":290,"away":"Phil",    "apts":257},
    {"week":3,"home":"Jimmy",   "hpts":367,"away":"Oded",    "apts":336},
    {"week":3,"home":"Fur",     "hpts":374,"away":"Phil",    "apts":304},
    {"week":3,"home":"Sal",     "hpts":336,"away":"Pat",     "apts":309},
    {"week":3,"home":"Charlie", "hpts":263,"away":"Marz",    "apts":255},
    {"week":4,"home":"Marz",    "hpts":369,"away":"Fur",     "apts":258},
    {"week":4,"home":"Sal",     "hpts":338,"away":"Jimmy",   "apts":333},
    {"week":4,"home":"Oded",    "hpts":369,"away":"Charlie", "apts":375},
    {"week":4,"home":"Pat",     "hpts":323,"away":"Phil",    "apts":325},
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
    "Deez \U0001f95c":      "Sal",
    "Dirty Meat Swing":     "Charlie",
    "Frank Latera":         "Fur",
    "Santolos":             "Oded",
    "Team Mush":            "Jimmy",
    "Tony's Calling":       "Phil",
    "YOU ALWAYS DO THIS!":  "Marz",
}


def validate_config():
    owners = list(OWNER_MAP.values())
    errors = []
    for o in owners:
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
        print("\nCONFIG ERRORS:")
        for e in errors:
            print("  * " + e)
        raise SystemExit("FATAL: Config validation failed.")
    print("  Config OK: Week " + str(CURRENT_WEEK) + ", " + str(len(MATCHUPS)) + " matchups")


def fetch_url(url, label):
    print("  GET " + label)
    try:
        r = requests.get(url, headers=HEADERS, timeout=30)
    except Exception as e:
        raise SystemExit("FATAL: Network error: " + str(e))
    print("  " + str(r.status_code) + " (" + str(len(r.text)) + " bytes)")
    if r.status_code in (401, 403):
        raise SystemExit(
            "FATAL: Yahoo returned " + str(r.status_code) + ".\n"
            "Cookie has expired. Update YAHOO_COOKIE in GitHub Secrets.\n"
            "Settings -> Secrets and variables -> Actions -> YAHOO_COOKIE"
        )
    if r.status_code != 200:
        raise SystemExit("FATAL: Unexpected status " + str(r.status_code))
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
    return a[:10] in b or b[:10] in a


def pct_rank(vals, v):
    below = sum(1 for x in vals if x < v)
    return round(below / (len(vals) - 1) * 100) if len(vals) > 1 else 50


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
        print("  WARNING: " + var_name + " value does not start with " + open_ch)
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
                html = html[:val_start] + json.dumps(value, ensure_ascii=False) + html[pos+1:]
                print("  Injected " + var_name)
                return html
        pos += 1
    print("  WARNING: " + var_name + " no matching close brace")
    return html


def fetch_bat_stats():
    print("\n[Batter Stats]")
    url = "https://baseball.fantasysports.yahoo.com/b1/" + LEAGUE_ID + "/headtoheadstats?pt=B&type=stats"
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


def fetch_pit_stats():
    print("\n[Pitcher Stats]")
    url = "https://baseball.fantasysports.yahoo.com/b1/" + LEAGUE_ID + "/headtoheadstats?pt=P&type=stats"
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


def fetch_player_stats(bat_raw, pit_raw):
    """
    Fetch player stats by trying team IDs 1-10.
    Matches each team to an owner using HR+SB fingerprint from headtoheadstats.
    No JS-rendered pages needed.
    """
    print("\n[Player Stats]")

    # Build reference fingerprints from headtoheadstats
    bat_ref = {}
    for live_name, data in bat_raw.items():
        r = data["raw"]
        hr = int(sf(r.get("HR", 0)))
        sb = int(sf(r.get("SB", 0)))
        for display, owner in OWNER_MAP.items():
            if fuzzy_match(live_name, display):
                bat_ref[(hr, sb)] = owner
                break

    print("  Bat fingerprints (HR,SB): " + str(sorted(bat_ref.keys())))

    batters      = {}
    pitchers     = {}
    team_bat_agg = {}
    team_pit_agg = {}

    for num in range(1, 11):
        if len(batters) == len(OWNER_MAP):
            break

        # --- BATTERS ---
        url = ("https://baseball.fantasysports.yahoo.com/b1/" + LEAGUE_ID +
               "/" + str(num) + "/teamstats?pt=B&type=stats")
        try:
            r = requests.get(url, headers=HEADERS, timeout=20)
        except Exception:
            continue
        if r.status_code == 404:
            continue
        if r.status_code in (401, 403):
            raise SystemExit("FATAL: Cookie expired. Update YAHOO_COOKIE in GitHub Secrets.")
        if r.status_code != 200:
            continue

        soup = BeautifulSoup(r.text, "lxml")
        _, rows = parse_table(soup)
        if not rows:
            continue

        bat_list = []
        bat_agg  = {"h":0,"ab":0,"gp":0,"r":0,"b1":0,"b2":0,"b3":0,
                    "hr":0,"rbi":0,"sb":0,"bb":0,"ibb":0,"hbp":0}

        for row in rows:
            name = clean_name(list(row.values())[0] if row else "")
            if not name or name.lower() in ["name", "player", "totals", ""]:
                continue
            gp  = sf(row.get("GP", row.get("G", 0)))
            ab  = sf(row.get("AB", 0))
            h   = sf(row.get("H",  0))
            if gp == 0 and ab == 0:
                continue
            b2  = sf(row.get("2B",  0))
            b3  = sf(row.get("3B",  0))
            hr  = sf(row.get("HR",  0))
            rv  = sf(row.get("R",   0))
            rbi = sf(row.get("RBI", 0))
            sb  = sf(row.get("SB",  0))
            bb  = sf(row.get("BB",  0))
            ibb = sf(row.get("IBB", 0))
            hbp = sf(row.get("HBP", 0))
            b1  = max(0.0, h - b2 - b3 - hr)
            avg  = round(h / ab, 3)                  if ab > 0 else 0.0
            slug = round((h + b2 + b3*2 + hr*3)/ab, 3) if ab > 0 else 0.0
            pts  = (rv*BAT_MULT["R"]   + b1*BAT_MULT["1B"]  + b2*BAT_MULT["2B"] +
                    b3*BAT_MULT["3B"]  + hr*BAT_MULT["HR"]  + rbi*BAT_MULT["RBI"] +
                    sb*BAT_MULT["SB"]  + bb*BAT_MULT["BB"]  + ibb*BAT_MULT["IBB"] +
                    hbp*BAT_MULT["HBP"])
            bat_list.append({
                "name": name, "pos": row.get("Pos", ""),
                "gp": int(gp), "hits": int(h), "ab": int(ab),
                "avg": avg, "slug": slug,
                "r": int(rv), "b1": int(b1), "b2": int(b2), "b3": int(b3), "hr": int(hr),
                "rbi": int(rbi), "sb": int(sb), "bb": int(bb), "ibb": int(ibb), "hbp": int(hbp),
                "fpts": round(pts, 1), "fppg": round(pts/gp, 2) if gp > 0 else 0
            })
            bat_agg["h"]   += int(h)
            bat_agg["ab"]  += int(ab)
            bat_agg["gp"]  += int(gp)
            bat_agg["r"]   += int(rv)
            bat_agg["b1"]  += int(b1)
            bat_agg["b2"]  += int(b2)
            bat_agg["b3"]  += int(b3)
            bat_agg["hr"]  += int(hr)
            bat_agg["rbi"] += int(rbi)
            bat_agg["sb"]  += int(sb)
            bat_agg["bb"]  += int(bb)
            bat_agg["ibb"] += int(ibb)
            bat_agg["hbp"] += int(hbp)

        if not bat_list:
            continue

        # Match to owner via HR+SB fingerprint
        key = (bat_agg["hr"], bat_agg["sb"])
        owner = bat_ref.get(key)
        if not owner:
            # Fuzzy: find closest match
            best_dist = 9999
            for (ref_hr, ref_sb), ref_owner in bat_ref.items():
                dist = abs(bat_agg["hr"] - ref_hr) + abs(bat_agg["sb"] - ref_sb)
                if dist < best_dist:
                    best_dist = dist
                    owner = ref_owner
            print("  Team " + str(num) + ": fuzzy match dist=" + str(best_dist) + " -> " + str(owner))

        if owner and owner not in batters:
            batters[owner]      = sorted(bat_list, key=lambda x: -x["fpts"])
            team_bat_agg[owner] = bat_agg
            print("  Team " + str(num) + " " + str(key) + " -> " + owner +
                  ": " + str(len(bat_list)) + " batters")

        # --- PITCHERS ---
        url = ("https://baseball.fantasysports.yahoo.com/b1/" + LEAGUE_ID +
               "/" + str(num) + "/teamstats?pt=P&type=stats")
        try:
            r = requests.get(url, headers=HEADERS, timeout=20)
        except Exception:
            continue
        if r.status_code != 200:
            continue

        soup = BeautifulSoup(r.text, "lxml")
        _, rows = parse_table(soup)
        pit_list = []
        pit_agg  = {"ip": 0.0, "gp": 0, "w": 0, "l": 0, "sv": 0, "er": 0,
                    "k": 0, "hld": 0, "nh": 0, "pg": 0, "bsv": 0}

        for row in rows:
            name = clean_name(list(row.values())[0] if row else "")
            if not name or name.lower() in ["name", "player", "totals", ""]:
                continue
            gp  = sf(row.get("GP", row.get("G", 0)))
            ip  = round(sf(row.get("IP", 0)), 1)
            if gp == 0 and ip == 0:
                continue
            w   = int(sf(row.get("W",   0)))
            l   = int(sf(row.get("L",   0)))
            sv  = int(sf(row.get("SV",  0)))
            er  = sf(row.get("ER",  0))
            k   = sf(row.get("K",   0))
            hld = int(sf(row.get("HLD", 0)))
            nh  = int(sf(row.get("NH",  0)))
            pg  = int(sf(row.get("PG",  0)))
            bsv = int(sf(row.get("BSV", 0)))
            era    = round(er*9/ip, 2)  if ip > 0 else 0.0
            k9     = round(k *9/ip, 2)  if ip > 0 else 0.0
            wl_pct = round(w/(w+l), 3)  if (w+l) > 0 else 0.0
            pts    = (ip*PIT_MULT["IP"]   + w*PIT_MULT["W"]   + l*PIT_MULT["L"] +
                      sv*PIT_MULT["SV"]   + er*PIT_MULT["ER"] + k*PIT_MULT["K"] +
                      hld*PIT_MULT["HLD"] + nh*PIT_MULT["NH"] + pg*PIT_MULT["PG"] +
                      bsv*PIT_MULT["BSV"])
            pit_list.append({
                "name": name, "pos": row.get("Pos", "SP"),
                "gp": int(gp), "ip": ip, "w": w, "l": l, "sv": sv,
                "er": int(er), "k": int(k), "hld": hld, "nh": nh, "pg": pg, "bsv": bsv,
                "era": era, "k9": k9, "wl_pct": wl_pct,
                "fpts": round(pts, 1), "fppg": round(pts/gp, 2) if gp > 0 else 0
            })
            pit_agg["ip"]  += ip
            pit_agg["gp"]  += int(gp)
            pit_agg["w"]   += w
            pit_agg["l"]   += l
            pit_agg["sv"]  += sv
            pit_agg["er"]  += int(er)
            pit_agg["k"]   += int(k)
            pit_agg["hld"] += hld
            pit_agg["nh"]  += nh
            pit_agg["pg"]  += pg
            pit_agg["bsv"] += bsv

        if pit_list and owner and owner not in pitchers:
            pitchers[owner]     = sorted(pit_list, key=lambda x: -x["fpts"])
            team_pit_agg[owner] = pit_agg
            print("  Team " + str(num) + " pitchers -> " + owner +
                  ": " + str(len(pit_list)) + " pitchers")

    print("  Found: " + str(sorted(batters.keys())))
    if not batters:
        print("  WARNING: teamstats returned no table data (JS-rendered). Keeping existing player data.")
        return None, None, None, None
    if len(batters) < len(OWNER_MAP):
        print("  WARNING: Only got " + str(len(batters)) + "/" + str(len(OWNER_MAP)) + " teams.")
    return batters, pitchers, team_bat_agg, team_pit_agg


def compute_league(bat_raw, pit_raw):
    print("\n[Computing league data]")
    owners = list(OWNER_MAP.values())
    bat_pts = {o: 0 for o in owners}
    pit_pts = {o: 0 for o in owners}
    for display, owner in OWNER_MAP.items():
        for live, data in bat_raw.items():
            if fuzzy_match(live, display):
                bat_pts[owner] = data["pts"]
                break
        for live, data in pit_raw.items():
            if fuzzy_match(live, display):
                pit_pts[owner] = data["pts"]
                break

    def compute_luck(owner):
        scores = WEEKLY_SCORES.get(owner, [])
        wins = total = 0
        for wi, score in enumerate(scores):
            if score == 0:
                continue
            for other in owners:
                if other == owner:
                    continue
                other_s = WEEKLY_SCORES.get(other, [])
                if wi < len(other_s) and other_s[wi] > 0:
                    total += 1
                    if score > other_s[wi]:
                        wins += 1
        return round(wins/total, 4) if total > 0 else 0.0

    league = {}
    for owner in owners:
        rec   = RECORDS[owner]
        w, l  = rec["w"], rec["l"]
        pf    = round(bat_pts[owner] + pit_pts[owner])
        pa    = PA_TOTALS.get(owner, 0)
        weeks = w + l
        pyth_p = (pf**2) / (pf**2 + pa**2) if pa > 0 and pf > 0 else 0
        pyth   = round(pyth_p * weeks, 3)
        luck   = compute_luck(owner)
        display = next(d for d, o in OWNER_MAP.items() if o == owner)
        league[owner] = {
            "display": display, "w": w, "l": l,
            "pf": pf, "pa": pa,
            "batters":  round(bat_pts[owner]),
            "pitchers": round(pit_pts[owner]),
            "luck": luck, "pyth": pyth,
            "owner": owner,
            "perfBat": 0, "perfPit": 0, "perfTot": 0,
        }
        print("  " + owner + ": " + str(w) + "-" + str(l) +
              " PF=" + str(pf) + " PA=" + str(pa) + " Luck=" + str(luck))

    bat_vals = [league[o]["batters"]  for o in owners]
    pit_vals = [league[o]["pitchers"] for o in owners]
    for o in owners:
        pb = pct_rank(bat_vals, league[o]["batters"])
        pp = pct_rank(pit_vals, league[o]["pitchers"])
        league[o]["perfBat"] = pb
        league[o]["perfPit"] = pp
        league[o]["perfTot"] = round((pb + pp) / 2)
    return league


def compute_stats_tables(bat_raw, pit_raw, team_bat_agg, team_pit_agg):
    print("\n[Computing stats tables]")
    owners = list(OWNER_MAP.values())
    bat_by = {}
    pit_by = {}
    for display, owner in OWNER_MAP.items():
        for live, data in bat_raw.items():
            if fuzzy_match(live, display):
                bat_by[owner] = data["raw"]
                break
        for live, data in pit_raw.items():
            if fuzzy_match(live, display):
                pit_by[owner] = data["raw"]
                break

    def disp(o):
        return next(d for d, x in OWNER_MAP.items() if x == o)

    def bat_fp(o):
        r  = bat_by.get(o, {})
        h  = sf(r.get("H", 0))
        b2 = sf(r.get("2B", 0))
        b3 = sf(r.get("3B", 0))
        hr = sf(r.get("HR", 0))
        b1 = max(0, h - b2 - b3 - hr)
        pts = (sf(r.get("R",   0)) * BAT_MULT["R"]   + b1 * BAT_MULT["1B"] +
               b2 * BAT_MULT["2B"]  + b3 * BAT_MULT["3B"]  + hr * BAT_MULT["HR"] +
               sf(r.get("RBI", 0)) * BAT_MULT["RBI"] + sf(r.get("SB",  0)) * BAT_MULT["SB"] +
               sf(r.get("BB",  0)) * BAT_MULT["BB"]  + sf(r.get("IBB", 0)) * BAT_MULT["IBB"] +
               sf(r.get("HBP", 0)) * BAT_MULT["HBP"])
        return {"owner": o, "display": disp(o), "rank": 0, "fpts": round(pts, 1),
                "r":   round(sf(r.get("R",   0)) * BAT_MULT["R"],   1),
                "b1":  round(b1 * BAT_MULT["1B"], 1),
                "b2":  round(b2 * BAT_MULT["2B"], 1),
                "b3":  round(b3 * BAT_MULT["3B"], 1),
                "hr":  round(hr * BAT_MULT["HR"], 1),
                "rbi": round(sf(r.get("RBI", 0)) * BAT_MULT["RBI"], 1),
                "sb":  round(sf(r.get("SB",  0)) * BAT_MULT["SB"],  1),
                "bb":  round(sf(r.get("BB",  0)) * BAT_MULT["BB"],  1),
                "ibb": round(sf(r.get("IBB", 0)) * BAT_MULT["IBB"], 1),
                "hbp": round(sf(r.get("HBP", 0)) * BAT_MULT["HBP"], 1)}

    def bat_raw_r(o):
        r  = bat_by.get(o, {})
        h  = sf(r.get("H", 0))
        b2 = sf(r.get("2B", 0))
        b3 = sf(r.get("3B", 0))
        hr = sf(r.get("HR", 0))
        return {"owner": o, "display": disp(o),
                "r":   int(sf(r.get("R",   0))), "b1": int(max(0, h - b2 - b3 - hr)),
                "b2":  int(b2), "b3": int(b3), "hr": int(hr),
                "rbi": int(sf(r.get("RBI", 0))), "sb":  int(sf(r.get("SB",  0))),
                "bb":  int(sf(r.get("BB",  0))), "ibb": int(sf(r.get("IBB", 0))),
                "hbp": int(sf(r.get("HBP", 0)))}

    def pit_fp(o):
        r   = pit_by.get(o, {})
        pts = sum(sf(r.get(s, 0)) * m for s, m in PIT_MULT.items())
        return {"owner": o, "display": disp(o), "rank": 0, "fpts": round(pts, 1),
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
        r = pit_by.get(o, {})
        return {"owner": o, "display": disp(o),
                "ip":  round(sf(r.get("IP",  0)), 1), "w":  int(sf(r.get("W",   0))),
                "l":   int(sf(r.get("L",   0))),      "sv": int(sf(r.get("SV",  0))),
                "er":  int(sf(r.get("ER",  0))),      "k":  int(sf(r.get("K",   0))),
                "hld": int(sf(r.get("HLD", 0))),      "nh": int(sf(r.get("NH",  0))),
                "pg":  int(sf(r.get("PG",  0))),      "bs": int(sf(r.get("BSV", 0)))}

    def adv_bat(o):
        agg = team_bat_agg.get(o, {})
        h   = agg.get("h",   0)
        ab  = agg.get("ab",  0)
        gp  = agg.get("gp",  1)
        b1  = agg.get("b1",  0)
        b2  = agg.get("b2",  0)
        b3  = agg.get("b3",  0)
        hr  = agg.get("hr",  0)
        rbi = agg.get("rbi", 0)
        sb  = agg.get("sb",  0)
        bb  = agg.get("bb",  0)
        ibb = agg.get("ibb", 0)
        hbp = agg.get("hbp", 0)
        ra  = agg.get("r",   0)
        pts = (ra*BAT_MULT["R"]   + b1*BAT_MULT["1B"]  + b2*BAT_MULT["2B"] +
               b3*BAT_MULT["3B"]  + hr*BAT_MULT["HR"]  + rbi*BAT_MULT["RBI"] +
               sb*BAT_MULT["SB"]  + bb*BAT_MULT["BB"]  + ibb*BAT_MULT["IBB"] +
               hbp*BAT_MULT["HBP"])
        avg  = round(h/ab, 3)                  if ab > 0 else 0
        slug = round((h + b2 + b3*2 + hr*3)/ab, 3) if ab > 0 else 0
        return {"owner": o, "display": disp(o),
                "hits": h, "ab": ab, "avg": avg, "gp": gp,
                "ptsGP": round(pts/gp, 2)  if gp > 0 else 0,
                "slug":  slug,
                "ptsAB": round(pts/ab, 3)  if ab > 0 else 0,
                "total_bases": int(h + b2 + b3*2 + hr*3),
                "abGP":  round(ab/gp, 2)   if gp > 0 else 0}

    def adv_pit(o):
        agg = team_pit_agg.get(o, {})
        r   = pit_by.get(o, {})
        ip  = agg.get("ip",  0.0)
        gp  = agg.get("gp",  1)
        w   = agg.get("w",   0)
        l   = agg.get("l",   0)
        er  = agg.get("er",  0)
        k   = agg.get("k",   0)
        sv  = agg.get("sv",  0)
        hld = agg.get("hld", 0)
        bsv = agg.get("bsv", 0)
        pts = sum(sf(r.get(s, 0)) * m for s, m in PIT_MULT.items())
        return {"owner": o, "display": disp(o),
                "ptsIP":    round(pts/ip, 2)   if ip > 0 else 0,
                "gp":       gp,
                "ptsGP":    round(pts/gp, 2)   if gp > 0 else 0,
                "ipGP":     round(ip/gp, 2)    if gp > 0 else 0,
                "era":      round(er*9/ip, 2)  if ip > 0 else 0,
                "wl_pct":   round(w/(w+l), 3)  if (w+l) > 0 else 0,
                "k9":       round(k*9/ip, 2)   if ip > 0 else 0,
                "sv_hld_bs": round(sv + hld - bsv, 1),
                "decisions": w + l,
                "dec_rate":  round((w+l)/gp, 3) if gp > 0 else 0}

    def zscore(rows, keys, lower=None):
        lower = lower or []
        result = []
        for row in rows:
            z = {"owner": row["owner"], "display": row["display"]}
            for key in keys:
                vals  = [rx[key] for rx in rows]
                v     = row[key]
                below = sum(1 for x in vals if x < v)
                pct   = round(below/(len(vals)-1)*100) if len(vals) > 1 else 50
                if key in lower:
                    pct = 100 - pct
                z[key] = pct
            result.append(z)
        return result

    bf = sorted([bat_fp(o)    for o in owners], key=lambda x: -x["fpts"])
    pf = sorted([pit_fp(o)    for o in owners], key=lambda x: -x["fpts"])
    br = [bat_raw_r(o)        for o in owners]
    pr = [pit_raw_r(o)        for o in owners]
    ba = [adv_bat(o)          for o in owners]
    pa = [adv_pit(o)          for o in owners]
    bk = ["r","b1","b2","b3","hr","rbi","sb","bb","ibb","hbp"]
    pk = ["ip","w","l","sv","er","k","hld","nh","pg","bs"]
    return {
        "BF": bf, "BR": br,
        "BK": zscore(br, bk),
        "BZ": zscore(br, bk),
        "BA": ba,
        "PF": pf, "PR": pr,
        "PK": zscore(pr, pk, ["l","er","bs"]),
        "PZ": zscore(pr, pk, ["l","er","bs"]),
        "PA": pa,
    }


def update_html(league, player_bat, player_pit, stats_tables):
    print("\n[Updating HTML]")
    candidates = ["barry_ballstein.html"] + glob.glob("barry_ballstein*.html")
    html_path  = next((f for f in candidates if os.path.exists(f)), None)
    if not html_path:
        raise SystemExit("FATAL: No barry_ballstein*.html found. Files: " + str(os.listdir(".")))
    with open(html_path, "r", encoding="utf-8") as f:
        html = f.read()

    html = inject_js_var(html, "const LD", league)
    html = inject_js_var(html, "const WD", WEEKLY_SCORES)
    html = inject_js_var(html, "const MT", MATCHUPS,                         is_array=True)
    if player_bat is not None and player_pit is not None:
        html = inject_js_var(html, "var PLR", {"batters": player_bat, "pitchers": player_pit})
    else:
        print("  Skipping PLR -- keeping existing player data in HTML")
    html = inject_js_var(html, "const BF", stats_tables["BF"],               is_array=True)
    html = inject_js_var(html, "const BR", stats_tables["BR"],               is_array=True)
    html = inject_js_var(html, "const BK", stats_tables["BK"],               is_array=True)
    html = inject_js_var(html, "const BZ", stats_tables["BZ"],               is_array=True)
    html = inject_js_var(html, "const BA", stats_tables["BA"],               is_array=True)
    html = inject_js_var(html, "const PF", stats_tables["PF"],               is_array=True)
    html = inject_js_var(html, "const PR", stats_tables["PR"],               is_array=True)
    html = inject_js_var(html, "const PK", stats_tables["PK"],               is_array=True)
    html = inject_js_var(html, "const PZ", stats_tables["PZ"],               is_array=True)
    html = inject_js_var(html, "const PA", stats_tables["PA"],               is_array=True)

    ts   = datetime.now(timezone.utc).strftime("%b %d %Y %H:%M UTC")
    html = re.sub(r"2026 [^W]*Week \d+",
                  "2026 &middot; Week " + str(CURRENT_WEEK), html)
    html = re.sub(r"Live from Yahoo Fantasy[^<]*",
                  "Live from Yahoo Fantasy &middot; Updated " + ts, html)

    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)
    print("  Saved " + html_path + " (" + str(len(html)) + " chars)")


def main():
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    print("=" * 60)
    print("Barry Ballstein's Boys -- Nightly Fetch  " + ts)
    print("=" * 60)

    print("\n[Validating config]")
    validate_config()

    bat_raw = fetch_bat_stats()
    pit_raw = fetch_pit_stats()
    player_bat, player_pit, team_bat_agg, team_pit_agg = fetch_player_stats(bat_raw, pit_raw)
    league       = compute_league(bat_raw, pit_raw)
    # Use empty dicts for stats tables if player data unavailable
    tba = team_bat_agg or {}
    tpa = team_pit_agg or {}
    stats_tables = compute_stats_tables(bat_raw, pit_raw, tba, tpa)
    update_html(league, player_bat, player_pit, stats_tables)

    print("\n" + "=" * 60)
    print("Done! Week " + str(CURRENT_WEEK) + " | " + ts)
    print("=" * 60)


if __name__ == "__main__":
    main()

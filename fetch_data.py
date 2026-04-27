#!/usr/bin/env python3
"""
Barry Ballstein's Boys -- Full Nightly Data Fetcher
Fetches all data from Yahoo Fantasy and regenerates the dashboard.
Fails loudly on critical errors so GitHub Actions shows a red X.
"""

import os, re, json, glob, sys
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timezone

# ----------------------------------------------------------------
# CONFIG
# ----------------------------------------------------------------
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
    "R":1, "1B":1, "2B":2, "3B":3, "HR":3,
    "RBI":0.5, "SB":3, "BB":0.5, "IBB":0.5, "HBP":0.5
}
PIT_MULT = {
    "IP":1, "W":12, "L":-6, "SV":7, "ER":-0.5,
    "K":1.5, "HLD":2, "NH":10, "PG":15, "BSV":-3.5
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

# ----------------------------------------------------------------
# CORE HELPERS
# ----------------------------------------------------------------
def fetch(url, label=""):
    tag = label or url[-60:]
    print(f"  GET {tag}")
    try:
        r = requests.get(url, headers=HEADERS, timeout=30)
    except Exception as e:
        print(f"  ERROR: {e}")
        return None
    print(f"  {r.status_code} ({len(r.text):,} bytes)")
    if r.status_code == 401:
        raise SystemExit("FATAL: Yahoo returned 401. Cookie has expired -- update YAHOO_COOKIE secret.")
    if r.status_code == 403:
        raise SystemExit("FATAL: Yahoo returned 403. Cookie has expired -- update YAHOO_COOKIE secret.")
    return BeautifulSoup(r.text, "lxml") if r.status_code == 200 else None

def sf(v):
    try: return float(str(v).replace(",","").strip() or 0)
    except: return 0.0

def parse_table(soup, idx=0):
    """Return (headers, list-of-row-dicts) from the idx-th table."""
    tables = soup.find_all("table") if soup else []
    if not tables or idx >= len(tables): return [], []
    headers, data = [], []
    for row in tables[idx].find_all("tr"):
        cells = [c.get_text(strip=True) for c in row.find_all(["th","td"])]
        if not headers and cells:
            headers = cells
        elif cells and len(cells) == len(headers):
            data.append(dict(zip(headers, cells)))
    return headers, data

def fuzzy_match(a, b):
    a, b = a.lower(), b.lower()
    return a[:10] in b or b[:10] in a

def pct_rank(vals, v):
    below = sum(1 for x in vals if x < v)
    return round(below / (len(vals) - 1) * 100) if len(vals) > 1 else 50

def clean_name(raw):
    if not raw: return ""
    s = re.sub(r'[\ue000-\uf8ff\n\r]', ' ', str(raw))
    s = re.sub(r'(Player Note|DTDNew|New Player Note|IL\d+|NAPlayer Note|DTD)\s*', '', s)
    s = re.sub(r'[A-Z]{2,3}(?=\s*-\s*[A-Z,/0-9]).*', '', s)
    s = re.sub(r'\s*\d+:\d+.*', '', s)
    return re.sub(r'\s+', ' ', s).strip()

def inject_js_var(html, var_name, value, is_array=False):
    """Replace a JS variable in HTML using brace-depth matching."""
    open_ch  = '[' if is_array else '{'
    close_ch = ']' if is_array else '}'
    prefix   = var_name + '='
    idx = html.find(prefix)
    if idx == -1:
        print(f"  WARNING: {var_name} not found in HTML")
        return html
    val_start = idx + len(prefix)
    if val_start >= len(html) or html[val_start] != open_ch:
        print(f"  WARNING: {var_name} value doesn't start with {open_ch}")
        return html
    depth = 0
    pos   = val_start
    while pos < len(html):
        c = html[pos]
        if   c == open_ch:  depth += 1
        elif c == close_ch:
            depth -= 1
            if depth == 0:
                html = html[:val_start] + json.dumps(value, ensure_ascii=False) + html[pos+1:]
                print(f"  Injected {var_name}")
                return html
        pos += 1
    print(f"  WARNING: {var_name} -- no matching close brace found")
    return html

# ----------------------------------------------------------------
# FETCH 1: STANDINGS (W, L, PF, PA, current week)
# ----------------------------------------------------------------
def fetch_standings():
    """
    Parse /b1/{lid}/standings for W/L/PF/PA per team.
    Also infers CURRENT_WEEK from games played.
    Returns: dict owner -> {w, l, pf, pa}, current_week int
    """
    print("\n[Standings]")
    soup = fetch(f"https://baseball.fantasysports.yahoo.com/b1/{LEAGUE_ID}/standings",
                 "standings page")
    if not soup:
        raise SystemExit("FATAL: Could not fetch standings page.")

    standings = {}

    # Try all tables
    tables = soup.find_all("table")
    print(f"  Tables found: {len(tables)}")

    for table in tables:
        rows = table.find_all("tr")
        if not rows: continue
        header_cells = [th.get_text(strip=True) for th in rows[0].find_all(["th","td"])]
        print(f"  Table headers: {header_cells[:10]}")

        # Look for W, L columns
        if "W" not in header_cells and "Win" not in str(header_cells): continue

        for row in rows[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all(["td","th"])]
            if not cells or len(cells) < 4: continue

            # Find team name -> owner
            team_name = cells[0]
            owner = None
            for display, own in OWNER_MAP.items():
                if fuzzy_match(team_name, display):
                    owner = own
                    break
            if not owner: continue

            # Extract W, L, PF, PA from known column positions
            row_dict = dict(zip(header_cells, cells))
            w  = int(sf(row_dict.get("W",  row_dict.get("Win", 0))))
            l  = int(sf(row_dict.get("L",  row_dict.get("Loss",0))))
            pf = round(sf(row_dict.get("PF", row_dict.get("Pts For", 0))))
            pa = round(sf(row_dict.get("PA", row_dict.get("Pts Agn", 0))))

            standings[owner] = {"w": w, "l": l, "pf": pf, "pa": pa}
            print(f"  {owner}: {w}-{l} PF={pf} PA={pa}")

    if not standings:
        # Fallback: look for W-L pattern anywhere in page
        print("  Table parse failed, trying text scan...")
        text = soup.get_text(separator="\n")
        for display, owner in OWNER_MAP.items():
            # Find the team name in text and look for W-L nearby
            idx = text.lower().find(display.lower()[:8])
            if idx > -1:
                chunk = text[idx:idx+200]
                wl = re.findall(r'(\d+)-(\d+)', chunk)
                if wl:
                    w, l = int(wl[0][0]), int(wl[0][1])
                    standings[owner] = {"w": w, "l": l, "pf": 0, "pa": 0}

    if not standings:
        raise SystemExit("FATAL: Could not parse any standings data from Yahoo.")

    # Infer current week from max games played
    max_games = max((v["w"] + v["l"]) for v in standings.values()) if standings else 0
    current_week = max_games
    print(f"  Current week inferred: {current_week}")

    return standings, current_week

# ----------------------------------------------------------------
# FETCH 2: CUMULATIVE BATTER/PITCHER STATS
# ----------------------------------------------------------------
def fetch_bat_stats():
    print("\n[Batter Stats]")
    soup = fetch(
        f"https://baseball.fantasysports.yahoo.com/b1/{LEAGUE_ID}/headtoheadstats?pt=B&type=stats",
        "headtoheadstats batters"
    )
    if not soup:
        raise SystemExit("FATAL: Could not fetch batter stats.")

    _, rows = parse_table(soup)
    result = {}
    for row in rows:
        team = list(row.values())[0]
        if not team or team.lower() in ["team name", "totals", ""]: continue
        pts = (sf(row.get("R",0))   * BAT_MULT["R"]   +
               sf(row.get("1B",0))  * BAT_MULT["1B"]  +
               sf(row.get("2B",0))  * BAT_MULT["2B"]  +
               sf(row.get("3B",0))  * BAT_MULT["3B"]  +
               sf(row.get("HR",0))  * BAT_MULT["HR"]  +
               sf(row.get("RBI",0)) * BAT_MULT["RBI"] +
               sf(row.get("SB",0))  * BAT_MULT["SB"]  +
               sf(row.get("BB",0))  * BAT_MULT["BB"]  +
               sf(row.get("IBB",0)) * BAT_MULT["IBB"] +
               sf(row.get("HBP",0)) * BAT_MULT["HBP"])
        result[team] = {"pts": round(pts, 1), "raw": row}
        print(f"  {team}: {round(pts,1)} bat pts")

    if not result:
        raise SystemExit("FATAL: Batter stats table was empty.")
    return result

def fetch_pit_stats():
    print("\n[Pitcher Stats]")
    soup = fetch(
        f"https://baseball.fantasysports.yahoo.com/b1/{LEAGUE_ID}/headtoheadstats?pt=P&type=stats",
        "headtoheadstats pitchers"
    )
    if not soup:
        raise SystemExit("FATAL: Could not fetch pitcher stats.")

    _, rows = parse_table(soup)
    result = {}
    for row in rows:
        team = list(row.values())[0]
        if not team or team.lower() in ["team name", "totals", ""]: continue
        pts = sum(sf(row.get(s, 0)) * m for s, m in PIT_MULT.items())
        result[team] = {"pts": round(pts, 1), "raw": row}
        print(f"  {team}: {round(pts,1)} pit pts")

    if not result:
        raise SystemExit("FATAL: Pitcher stats table was empty.")
    return result

# ----------------------------------------------------------------
# FETCH 3: WEEKLY SCORES + MATCHUPS
# ----------------------------------------------------------------
def fetch_weekly_and_matchups(current_week):
    """
    Fetch weekly scores and matchup history.
    Tries multiple Yahoo URLs and parsing strategies.
    Fails loudly if nothing works.
    """
    print("\n[Weekly Scores & Matchups]")
    owners = list(OWNER_MAP.values())
    weekly   = {o: [] for o in owners}
    matchups = []
    pa_totals = {o: 0 for o in owners}

    def parse_scores_from_soup(soup, week):
        """Try every strategy to extract team scores from a page."""
        scores = {}
        if not soup: return scores

        page_text = soup.get_text(separator="\n")

        # Strategy 1: look for score pattern "TeamName\n123.45" in raw text
        for display, owner in OWNER_MAP.items():
            # Find the team name in text, then look for a score within next 200 chars
            pattern = re.escape(display[:8])
            for m in re.finditer(pattern, page_text, re.IGNORECASE):
                chunk = page_text[m.start():m.start()+300]
                nums = re.findall(r'\b(\d{2,3}\.\d{1,2})\b', chunk)
                for n in nums:
                    v = float(n)
                    if 50 < v < 700:
                        scores[owner] = v
                        break
                if owner in scores: break

        # Strategy 2: scan all tables
        if len(scores) < 4:
            for table in soup.find_all("table"):
                for row in table.find_all("tr"):
                    cells = [td.get_text(strip=True) for td in row.find_all(["td","th"])]
                    for i in range(len(cells)-1):
                        try:
                            v = float(cells[i+1].replace(",",""))
                            if 50 < v < 700 and len(cells[i]) > 3:
                                for display, owner in OWNER_MAP.items():
                                    if fuzzy_match(cells[i], display) and owner not in scores:
                                        scores[owner] = v
                        except: pass

        # Strategy 3: look for data-* attributes with scores
        if len(scores) < 4:
            for el in soup.find_all(attrs={"data-score": True}):
                try:
                    v = float(el["data-score"])
                    if 50 < v < 700:
                        parent_text = el.parent.get_text(" ", strip=True) if el.parent else ""
                        for display, owner in OWNER_MAP.items():
                            if fuzzy_match(parent_text, display) and owner not in scores:
                                scores[owner] = v
                except: pass

        return scores

    weeks_with_data = 0
    for week in range(1, current_week + 1):
        week_scores = {}

        # Try scoreboard URL
        soup = fetch(
            f"https://baseball.fantasysports.yahoo.com/b1/{LEAGUE_ID}/scoreboard?week={week}",
            f"scoreboard week {week}"
        )
        week_scores = parse_scores_from_soup(soup, week)
        print(f"  Week {week} scoreboard: {len(week_scores)} scores")

        # Try matchups URL if scoreboard didn't get enough
        if len(week_scores) < 6:
            soup2 = fetch(
                f"https://baseball.fantasysports.yahoo.com/b1/{LEAGUE_ID}/matchups?week={week}",
                f"matchups week {week}"
            )
            scores2 = parse_scores_from_soup(soup2, week)
            print(f"  Week {week} matchups: {len(scores2)} scores")
            week_scores.update(scores2)

        print(f"  Week {week} final: {week_scores}")

        if len(week_scores) >= 6:
            weeks_with_data += 1

        # Build matchups from pairs
        items = list(week_scores.items())
        for j in range(0, len(items)-1, 2):
            if j+1 < len(items):
                o1, s1 = items[j]
                o2, s2 = items[j+1]
                matchups.append({
                    "week": week,
                    "home": o1, "hpts": round(s1, 2),
                    "away": o2, "apts": round(s2, 2)
                })
                pa_totals[o1] = pa_totals[o1] + s2
                pa_totals[o2] = pa_totals[o2] + s1

        for owner in owners:
            weekly[owner].append(round(week_scores.get(owner, 0), 2))

    if weeks_with_data == 0:
        raise SystemExit(
            "FATAL: Could not extract any weekly scores from Yahoo.\n"
            "  The scoreboard may be JS-rendered. Check the URL manually.\n"
            "  URL tried: https://baseball.fantasysports.yahoo.com/b1/"
            f"{LEAGUE_ID}/scoreboard?week=1"
        )

    if weeks_with_data < current_week:
        print(f"  WARNING: Only got scores for {weeks_with_data}/{current_week} weeks.")

    return weekly, matchups, pa_totals

# ----------------------------------------------------------------
# FETCH 4: INDIVIDUAL PLAYER STATS (per team)
# Returns player data AND team-level aggregates (H, AB, GP)
# ----------------------------------------------------------------
def fetch_player_stats():
    print("\n[Player Stats]")
    batters   = {}
    pitchers  = {}
    team_bat_agg = {}   # owner -> {h, ab, gp, ...} aggregated from players
    team_pit_agg = {}   # owner -> {ip, gp, ...} aggregated from players

    # Get team IDs from standings page
    soup = fetch(
        f"https://baseball.fantasysports.yahoo.com/b1/{LEAGUE_ID}",
        "league home (team IDs)"
    )
    team_links = {}
    if soup:
        for a in soup.find_all("a", href=True):
            m = re.match(rf"/b1/{LEAGUE_ID}/(\d+)$", a["href"])
            if m:
                num  = m.group(1)
                name = a.get_text(strip=True)
                if len(name) > 2:
                    for display, owner in OWNER_MAP.items():
                        if fuzzy_match(name, display):
                            team_links[owner] = num
                            break

    print(f"  Team IDs found: {team_links}")
    if not team_links:
        raise SystemExit("FATAL: Could not find team IDs from league home page.")

    for owner, team_num in team_links.items():
        bat_list, pit_list = [], []
        bat_agg = {"h":0,"ab":0,"gp":0,"r":0,"b1":0,"b2":0,"b3":0,"hr":0,
                   "rbi":0,"sb":0,"bb":0,"ibb":0,"hbp":0}
        pit_agg = {"ip":0,"gp":0,"w":0,"l":0,"sv":0,"er":0,"k":0,
                   "hld":0,"nh":0,"pg":0,"bsv":0}

        # --- BATTERS ---
        url = f"https://baseball.fantasysports.yahoo.com/b1/{LEAGUE_ID}/{team_num}/teamstats?pt=B&type=stats"
        soup = fetch(url, f"{owner} batter teamstats")
        if soup:
            _, rows = parse_table(soup)
            for row in rows:
                name = clean_name(list(row.values())[0] if row else "")
                if not name or name.lower() in ["name","player","totals",""]: continue
                gp = sf(row.get("GP", row.get("G", 0)))
                ab = sf(row.get("AB", 0))
                h  = sf(row.get("H",  0))
                if gp == 0 and ab == 0: continue
                b2 = sf(row.get("2B", 0)); b3 = sf(row.get("3B", 0)); hr = sf(row.get("HR", 0))
                b1 = max(0, h - b2 - b3 - hr)
                avg  = round(h/ab, 3)  if ab > 0 else 0.0
                slug = round((h + b2 + b3*2 + hr*3)/ab, 3) if ab > 0 else 0.0
                r    = sf(row.get("R",   0))
                rbi  = sf(row.get("RBI", 0)); sb  = sf(row.get("SB",  0))
                bb   = sf(row.get("BB",  0)); ibb = sf(row.get("IBB", 0))
                hbp  = sf(row.get("HBP", 0))
                pts  = (r*BAT_MULT["R"] + b1*BAT_MULT["1B"] + b2*BAT_MULT["2B"] +
                        b3*BAT_MULT["3B"] + hr*BAT_MULT["HR"] + rbi*BAT_MULT["RBI"] +
                        sb*BAT_MULT["SB"] + bb*BAT_MULT["BB"] + ibb*BAT_MULT["IBB"] +
                        hbp*BAT_MULT["HBP"])
                bat_list.append({
                    "name":name, "pos":row.get("Pos",""), "gp":int(gp),
                    "hits":int(h), "ab":int(ab), "avg":avg, "slug":slug,
                    "r":int(r), "b1":int(b1), "b2":int(b2), "b3":int(b3), "hr":int(hr),
                    "rbi":int(rbi), "sb":int(sb), "bb":int(bb), "ibb":int(ibb), "hbp":int(hbp),
                    "fpts":round(pts,1), "fppg":round(pts/gp,2) if gp > 0 else 0
                })
                # Aggregate for team-level advanced stats
                bat_agg["h"]   += int(h);  bat_agg["ab"]  += int(ab)
                bat_agg["gp"]  += int(gp); bat_agg["r"]   += int(r)
                bat_agg["b1"]  += int(b1); bat_agg["b2"]  += int(b2)
                bat_agg["b3"]  += int(b3); bat_agg["hr"]  += int(hr)
                bat_agg["rbi"] += int(rbi); bat_agg["sb"]  += int(sb)
                bat_agg["bb"]  += int(bb);  bat_agg["ibb"] += int(ibb)
                bat_agg["hbp"] += int(hbp)

        # --- PITCHERS ---
        url = f"https://baseball.fantasysports.yahoo.com/b1/{LEAGUE_ID}/{team_num}/teamstats?pt=P&type=stats"
        soup = fetch(url, f"{owner} pitcher teamstats")
        if soup:
            _, rows = parse_table(soup)
            for row in rows:
                name = clean_name(list(row.values())[0] if row else "")
                if not name or name.lower() in ["name","player","totals",""]: continue
                gp = sf(row.get("GP", row.get("G", 0)))
                ip = round(sf(row.get("IP", 0)), 1)
                if gp == 0 and ip == 0: continue
                w   = int(sf(row.get("W",   0))); l   = int(sf(row.get("L",   0)))
                sv  = int(sf(row.get("SV",  0))); er  = sf(row.get("ER",  0))
                k   = sf(row.get("K",   0));  hld = int(sf(row.get("HLD", 0)))
                nh  = int(sf(row.get("NH",  0))); pg  = int(sf(row.get("PG",  0)))
                bsv = int(sf(row.get("BSV", 0)))
                era    = round(er*9/ip,  2) if ip > 0 else 0.0
                k9     = round(k *9/ip,  2) if ip > 0 else 0.0
                wl_pct = round(w/(w+l),  3) if (w+l) > 0 else 0.0
                pts = (ip*PIT_MULT["IP"] + w*PIT_MULT["W"] + l*PIT_MULT["L"] +
                       sv*PIT_MULT["SV"] + er*PIT_MULT["ER"] + k*PIT_MULT["K"] +
                       hld*PIT_MULT["HLD"] + nh*PIT_MULT["NH"] + pg*PIT_MULT["PG"] +
                       bsv*PIT_MULT["BSV"])
                pit_list.append({
                    "name":name, "pos":row.get("Pos","SP"), "gp":int(gp), "ip":ip,
                    "w":w, "l":l, "sv":sv, "er":int(er), "k":int(k),
                    "hld":hld, "nh":nh, "pg":pg, "bsv":bsv,
                    "era":era, "k9":k9, "wl_pct":wl_pct,
                    "fpts":round(pts,1), "fppg":round(pts/gp,2) if gp > 0 else 0
                })
                pit_agg["ip"]  += ip;   pit_agg["gp"]  += int(gp)
                pit_agg["w"]   += w;    pit_agg["l"]   += l
                pit_agg["sv"]  += sv;   pit_agg["er"]  += int(er)
                pit_agg["k"]   += int(k); pit_agg["hld"] += hld
                pit_agg["nh"]  += nh;   pit_agg["pg"]  += pg
                pit_agg["bsv"] += bsv

        if bat_list: batters[owner]      = sorted(bat_list, key=lambda x:-x["fpts"])
        if pit_list: pitchers[owner]     = sorted(pit_list, key=lambda x:-x["fpts"])
        if bat_agg["ab"] > 0:
            team_bat_agg[owner] = bat_agg
        if pit_agg["ip"] > 0:
            team_pit_agg[owner] = pit_agg
        print(f"  {owner}: {len(bat_list)} batters, {len(pit_list)} pitchers")

    if not batters:
        raise SystemExit("FATAL: Could not fetch any player stats.")

    return batters, pitchers, team_bat_agg, team_pit_agg

# ----------------------------------------------------------------
# COMPUTE: LEAGUE DATA
# ----------------------------------------------------------------
def compute_league(bat_raw, pit_raw, standings_data, weekly, pa_totals):
    print("\n[Computing league data]")
    owners = list(OWNER_MAP.values())
    league = {}

    bat_pts = {o: 0 for o in owners}
    pit_pts = {o: 0 for o in owners}
    for display, owner in OWNER_MAP.items():
        for live, data in bat_raw.items():
            if fuzzy_match(live, display):
                bat_pts[owner] = data["pts"]; break
        for live, data in pit_raw.items():
            if fuzzy_match(live, display):
                pit_pts[owner] = data["pts"]; break

    def compute_luck(owner):
        scores = weekly.get(owner, [])
        if not scores: return 0.0
        wins = total = 0
        for wi, score in enumerate(scores):
            if score == 0: continue
            for other in owners:
                if other == owner: continue
                other_s = weekly.get(other, [])
                if wi < len(other_s) and other_s[wi] > 0:
                    total += 1
                    if score > other_s[wi]: wins += 1
        return round(wins/total, 4) if total > 0 else 0.0

    for owner in owners:
        sd  = standings_data.get(owner, {})
        w   = sd.get("w", 0)
        l   = sd.get("l", 0)
        # PF: prefer live from bat+pit calc (most accurate)
        pf_live = round(bat_pts[owner] + pit_pts[owner])
        pf      = pf_live if pf_live > 0 else sd.get("pf", 0)
        # PA: prefer from matchup data, fallback to standings
        pa_match = round(pa_totals.get(owner, 0))
        pa       = pa_match if pa_match > 0 else sd.get("pa", 0)
        weeks    = w + l
        pyth_pct = (pf**2) / (pf**2 + pa**2) if pa > 0 and pf > 0 else 0
        pyth     = round(pyth_pct * weeks, 3)
        luck     = compute_luck(owner)
        display  = next(d for d, o in OWNER_MAP.items() if o == owner)

        league[owner] = {
            "display":  display,
            "w":        w,  "l": l,
            "pf":       pf, "pa": pa,
            "batters":  round(bat_pts[owner]),
            "pitchers": round(pit_pts[owner]),
            "luck":     luck,
            "pyth":     pyth,
            "owner":    owner,
            "perfBat":  0, "perfPit": 0, "perfTot": 0,
        }
        print(f"  {owner}: {w}-{l} PF={pf} PA={pa} Luck={luck} Pyth={pyth}")

    bat_vals = [league[o]["batters"] for o in owners]
    pit_vals = [league[o]["pitchers"] for o in owners]
    for o in owners:
        pb = pct_rank(bat_vals, league[o]["batters"])
        pp = pct_rank(pit_vals, league[o]["pitchers"])
        league[o]["perfBat"] = pb
        league[o]["perfPit"] = pp
        league[o]["perfTot"] = round((pb + pp) / 2)

    return league

# ----------------------------------------------------------------
# COMPUTE: STATS TABLES
# Uses team-level aggregates from player stats for H/AB/GP
# ----------------------------------------------------------------
def compute_stats_tables(bat_raw, pit_raw, team_bat_agg, team_pit_agg):
    print("\n[Computing stats tables]")
    owners = list(OWNER_MAP.values())

    bat_by = {}; pit_by = {}
    for display, owner in OWNER_MAP.items():
        for live, data in bat_raw.items():
            if fuzzy_match(live, display): bat_by[owner] = data["raw"]; break
        for live, data in pit_raw.items():
            if fuzzy_match(live, display): pit_by[owner] = data["raw"]; break

    def disp(o): return next(d for d,x in OWNER_MAP.items() if x==o)

    def bat_fp(owner):
        r = bat_by.get(owner, {})
        h  = sf(r.get("H",0)); b2=sf(r.get("2B",0)); b3=sf(r.get("3B",0)); hr=sf(r.get("HR",0))
        b1 = max(0, h-b2-b3-hr)
        pts = (sf(r.get("R",0))*BAT_MULT["R"] + b1*BAT_MULT["1B"] +
               b2*BAT_MULT["2B"] + b3*BAT_MULT["3B"] + hr*BAT_MULT["HR"] +
               sf(r.get("RBI",0))*BAT_MULT["RBI"] + sf(r.get("SB",0))*BAT_MULT["SB"] +
               sf(r.get("BB",0))*BAT_MULT["BB"]   + sf(r.get("IBB",0))*BAT_MULT["IBB"] +
               sf(r.get("HBP",0))*BAT_MULT["HBP"])
        return {"owner":owner,"display":disp(owner),"rank":0,
                "fpts":round(pts,1),
                "r":round(sf(r.get("R",0))*BAT_MULT["R"],1),
                "b1":round(b1*BAT_MULT["1B"],1), "b2":round(b2*BAT_MULT["2B"],1),
                "b3":round(b3*BAT_MULT["3B"],1), "hr":round(hr*BAT_MULT["HR"],1),
                "rbi":round(sf(r.get("RBI",0))*BAT_MULT["RBI"],1),
                "sb":round(sf(r.get("SB",0))*BAT_MULT["SB"],1),
                "bb":round(sf(r.get("BB",0))*BAT_MULT["BB"],1),
                "ibb":round(sf(r.get("IBB",0))*BAT_MULT["IBB"],1),
                "hbp":round(sf(r.get("HBP",0))*BAT_MULT["HBP"],1)}

    def bat_raw_row(owner):
        r = bat_by.get(owner, {})
        h=sf(r.get("H",0)); b2=sf(r.get("2B",0)); b3=sf(r.get("3B",0)); hr=sf(r.get("HR",0))
        return {"owner":owner,"display":disp(owner),
                "r":int(sf(r.get("R",0))), "b1":int(max(0,h-b2-b3-hr)),
                "b2":int(b2), "b3":int(b3), "hr":int(hr),
                "rbi":int(sf(r.get("RBI",0))), "sb":int(sf(r.get("SB",0))),
                "bb":int(sf(r.get("BB",0))),  "ibb":int(sf(r.get("IBB",0))),
                "hbp":int(sf(r.get("HBP",0)))}

    def pit_fp(owner):
        r = pit_by.get(owner, {})
        pts = sum(sf(r.get(s,0))*m for s,m in PIT_MULT.items())
        return {"owner":owner,"display":disp(owner),"rank":0,
                "fpts":round(pts,1),
                "ip":round(sf(r.get("IP",0))*PIT_MULT["IP"],1),
                "w":round(sf(r.get("W",0))*PIT_MULT["W"],1),
                "l":round(sf(r.get("L",0))*PIT_MULT["L"],1),
                "sv":round(sf(r.get("SV",0))*PIT_MULT["SV"],1),
                "er":round(sf(r.get("ER",0))*PIT_MULT["ER"],1),
                "k":round(sf(r.get("K",0))*PIT_MULT["K"],1),
                "hld":round(sf(r.get("HLD",0))*PIT_MULT["HLD"],1),
                "nh":round(sf(r.get("NH",0))*PIT_MULT["NH"],1),
                "pg":round(sf(r.get("PG",0))*PIT_MULT["PG"],1),
                "bs":round(sf(r.get("BSV",0))*PIT_MULT["BSV"],1)}

    def pit_raw_row(owner):
        r = pit_by.get(owner, {})
        return {"owner":owner,"display":disp(owner),
                "ip":round(sf(r.get("IP",0)),1), "w":int(sf(r.get("W",0))),
                "l":int(sf(r.get("L",0))),  "sv":int(sf(r.get("SV",0))),
                "er":int(sf(r.get("ER",0))), "k":int(sf(r.get("K",0))),
                "hld":int(sf(r.get("HLD",0))), "nh":int(sf(r.get("NH",0))),
                "pg":int(sf(r.get("PG",0))),   "bs":int(sf(r.get("BSV",0)))}

    def adv_bat(owner):
        # Use aggregated player stats for H, AB, GP
        agg = team_bat_agg.get(owner, {})
        r   = bat_by.get(owner, {})
        h   = agg.get("h",  int(sf(r.get("H",0))))
        ab  = agg.get("ab", 0)
        gp  = agg.get("gp", int(sf(r.get("GP",1))))
        b2  = agg.get("b2", int(sf(r.get("2B",0))))
        b3  = agg.get("b3", int(sf(r.get("3B",0))))
        hr  = agg.get("hr", int(sf(r.get("HR",0))))
        b1_r= agg.get("b1", int(max(0, h-b2-b3-hr)))
        rbi = agg.get("rbi",int(sf(r.get("RBI",0))))
        sb  = agg.get("sb", int(sf(r.get("SB",0))))
        bb  = agg.get("bb", int(sf(r.get("BB",0))))
        ibb = agg.get("ibb",int(sf(r.get("IBB",0))))
        hbp = agg.get("hbp",int(sf(r.get("HBP",0))))
        pts = (agg.get("r",int(sf(r.get("R",0))))*BAT_MULT["R"] + b1_r*BAT_MULT["1B"] +
               b2*BAT_MULT["2B"] + b3*BAT_MULT["3B"] + hr*BAT_MULT["HR"] +
               rbi*BAT_MULT["RBI"] + sb*BAT_MULT["SB"] + bb*BAT_MULT["BB"] +
               ibb*BAT_MULT["IBB"] + hbp*BAT_MULT["HBP"])
        avg  = round(h/ab,3)           if ab > 0 else 0
        slug = round((h+b2+b3*2+hr*3)/ab,3) if ab > 0 else 0
        tb   = h + b2 + b3*2 + hr*3
        return {"owner":owner,"display":disp(owner),
                "hits":h, "ab":ab, "avg":avg, "gp":gp,
                "ptsGP":round(pts/gp,2)   if gp > 0 else 0,
                "slug":slug,
                "ptsAB":round(pts/ab,3)   if ab > 0 else 0,
                "total_bases":int(tb),
                "abGP":round(ab/gp,2)     if gp > 0 else 0}

    def adv_pit(owner):
        agg = team_pit_agg.get(owner, {})
        r   = pit_by.get(owner, {})
        ip  = agg.get("ip",  sf(r.get("IP",0)))
        gp  = agg.get("gp",  int(sf(r.get("GP",1))))
        w   = agg.get("w",   int(sf(r.get("W",0))))
        l   = agg.get("l",   int(sf(r.get("L",0))))
        er  = agg.get("er",  sf(r.get("ER",0)))
        k   = agg.get("k",   sf(r.get("K",0)))
        sv  = agg.get("sv",  int(sf(r.get("SV",0))))
        hld = agg.get("hld", int(sf(r.get("HLD",0))))
        bsv = agg.get("bsv", int(sf(r.get("BSV",0))))
        pts = sum(sf(r.get(s,0))*m for s,m in PIT_MULT.items())
        return {"owner":owner,"display":disp(owner),
                "ptsIP":  round(pts/ip,2)     if ip > 0 else 0,
                "gp":     gp,
                "ptsGP":  round(pts/gp,2)     if gp > 0 else 0,
                "ipGP":   round(ip/gp,2)      if gp > 0 else 0,
                "era":    round(er*9/ip,2)    if ip > 0 else 0,
                "wl_pct": round(w/(w+l),3)   if (w+l)>0 else 0,
                "k9":     round(k*9/ip,2)    if ip > 0 else 0,
                "sv_hld_bs": round(sv+hld-bsv,1),
                "decisions": w+l,
                "dec_rate":  round((w+l)/gp,3) if gp > 0 else 0}

    def zscore(rows, keys, lower_is_better=None):
        lower_is_better = lower_is_better or []
        result = []
        for row in rows:
            z = {"owner":row["owner"],"display":row["display"]}
            for key in keys:
                vals = [r[key] for r in rows]
                v    = row[key]
                below = sum(1 for x in vals if x < v)
                pct   = round(below/(len(vals)-1)*100) if len(vals) > 1 else 50
                if key in lower_is_better: pct = 100 - pct
                z[key] = pct
            result.append(z)
        return result

    bf  = sorted([bat_fp(o)     for o in owners], key=lambda x: -x["fpts"])
    pf  = sorted([pit_fp(o)     for o in owners], key=lambda x: -x["fpts"])
    br  = [bat_raw_row(o)       for o in owners]
    pr  = [pit_raw_row(o)       for o in owners]
    ba  = [adv_bat(o)           for o in owners]
    pa  = [adv_pit(o)           for o in owners]

    bat_keys = ["r","b1","b2","b3","hr","rbi","sb","bb","ibb","hbp"]
    pit_keys = ["ip","w","l","sv","er","k","hld","nh","pg","bs"]

    return {
        "BF": bf, "BR": br,
        "BK": zscore(br, bat_keys),
        "BZ": zscore(br, bat_keys),
        "BA": ba,
        "PF": pf, "PR": pr,
        "PK": zscore(pr, pit_keys, ["l","er","bs"]),
        "PZ": zscore(pr, pit_keys, ["l","er","bs"]),
        "PA": pa,
    }

# ----------------------------------------------------------------
# UPDATE HTML
# ----------------------------------------------------------------
def update_html(league, weekly, matchups, player_bat, player_pit, stats_tables, current_week):
    print("\n[Updating HTML]")

    candidates = ["barry_ballstein.html"] + glob.glob("barry_ballstein*.html")
    html_path  = next((f for f in candidates if os.path.exists(f)), None)
    if not html_path:
        raise SystemExit(f"FATAL: No barry_ballstein*.html found. Files: {os.listdir('.')}")

    with open(html_path, "r", encoding="utf-8") as f:
        html = f.read()

    html = inject_js_var(html, "const LD", league)
    html = inject_js_var(html, "const WD", weekly)
    html = inject_js_var(html, "const MT", matchups,             is_array=True)
    html = inject_js_var(html, "var PLR",  {"batters": player_bat, "pitchers": player_pit})
    html = inject_js_var(html, "const BF", stats_tables["BF"],  is_array=True)
    html = inject_js_var(html, "const BR", stats_tables["BR"],  is_array=True)
    html = inject_js_var(html, "const BK", stats_tables["BK"],  is_array=True)
    html = inject_js_var(html, "const BZ", stats_tables["BZ"],  is_array=True)
    html = inject_js_var(html, "const BA", stats_tables["BA"],  is_array=True)
    html = inject_js_var(html, "const PF", stats_tables["PF"],  is_array=True)
    html = inject_js_var(html, "const PR", stats_tables["PR"],  is_array=True)
    html = inject_js_var(html, "const PK", stats_tables["PK"],  is_array=True)
    html = inject_js_var(html, "const PZ", stats_tables["PZ"],  is_array=True)
    html = inject_js_var(html, "const PA", stats_tables["PA"],  is_array=True)

    # Update week badge and timestamp
    ts = datetime.now(timezone.utc).strftime("%b %d %Y %H:%M UTC")
    html = re.sub(r"2026 [^W]*Week \d+",
                  f"2026 &middot; Week {current_week}", html)
    html = re.sub(r"Live from Yahoo Fantasy[^<]*",
                  f"Live from Yahoo Fantasy &middot; Updated {ts}", html)

    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"  Saved {html_path} ({len(html):,} chars)")

# ----------------------------------------------------------------
# MAIN
# ----------------------------------------------------------------
def main():
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    print("=" * 60)
    print(f"Barry Ballstein's Boys -- Nightly Fetch  {ts}")
    print("=" * 60)

    # All fetches fail loudly
    standings_data, current_week = fetch_standings()
    bat_raw = fetch_bat_stats()
    pit_raw = fetch_pit_stats()

    weekly, matchups, pa_totals = fetch_weekly_and_matchups(current_week)

    player_bat, player_pit, team_bat_agg, team_pit_agg = fetch_player_stats()

    league = compute_league(bat_raw, pit_raw, standings_data, weekly, pa_totals)

    stats_tables = compute_stats_tables(bat_raw, pit_raw, team_bat_agg, team_pit_agg)

    update_html(league, weekly, matchups, player_bat, player_pit, stats_tables, current_week)

    print("\n" + "=" * 60)
    print("Done! All data updated successfully.")
    print("=" * 60)

if __name__ == "__main__":
    main()

#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Worlds 2026 Predictor (League of Legends World Championship)
- Winner-only picks on best-of series (no draws)
- Match feed behind fetch_matches() (requests on Linux/Render, PowerShell subprocess on Windows)
- MongoDB Atlas for persistent storage
- Pure stdlib HTTP server (no Flask needed)
"""
import http.server
import socketserver
import urllib.parse
import json
import hashlib
import os
import sys
import time
import uuid
import re
import html
import threading
import subprocess

# ── Optional imports ──────────────────────────────────────────────────────────
try:
    import requests as _requests
    _HAS_REQUESTS = True
except ImportError:
    _HAS_REQUESTS = False

try:
    import pymongo as _pymongo
    _HAS_PYMONGO = True
except ImportError:
    _HAS_PYMONGO = False

# ── Config ────────────────────────────────────────────────────────────────────
INITIAL_TOKENS = 3000
BET_COST       = 200
WIN_REWARD     = 500

MONGO_URI = os.environ.get("MONGO_URI", "")       # set in Render env vars
ADMIN_PW  = os.environ.get("ADMIN_PASSWORD", "admin1234")
PORT      = int(os.environ.get("PORT", 8080))
IS_WIN    = sys.platform == "win32"

STAGE_LABELS = {
    "play-in":       "Play-In",
    "swiss":         "Swiss Stage",
    "quarterfinals": "Quarterfinals",
    "semifinals":    "Semifinals",
    "final":         "Final",
}

# ── Storage layer (MongoDB or JSON fallback) ──────────────────────────────────
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
os.makedirs(DATA_DIR, exist_ok=True)

_db = None
_db_lock = threading.Lock()

def get_db():
    global _db
    if _db is not None:
        return _db
    with _db_lock:
        if _db is not None:
            return _db
        if _HAS_PYMONGO and MONGO_URI:
            try:
                client = _pymongo.MongoClient(
                    MONGO_URI,
                    serverSelectionTimeoutMS=5000,
                    connectTimeoutMS=5000,
                    socketTimeoutMS=10000,
                    maxPoolSize=10,
                    retryWrites=True,
                )
                client.admin.command("ping")
                _db = client["wc2026"]
                print("[DB] Connected to MongoDB Atlas")
                return _db
            except Exception as e:
                print("[DB] MongoDB connection failed:", e)
        print("[DB] Using local JSON files")
        return None

def _safe_db(fn, fallback):
    """Run a DB operation; on any error reconnect once and retry, else return fallback."""
    global _db
    try:
        return fn()
    except Exception as e:
        print("[DB] error, reconnecting:", e)
        _db = None          # force reconnect on next get_db()
        get_db()
        try:
            return fn()
        except Exception as e2:
            print("[DB] retry failed:", e2)
            return fallback

# JSON file helpers (local fallback)
def _jpath(name):
    return os.path.join(DATA_DIR, name + ".json")

def _jload(name, default):
    p = _jpath(name)
    if os.path.exists(p):
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    return default

def _jsave(name, data):
    with open(_jpath(name), "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

# Unified storage API
def db_get_user(username):
    db = get_db()
    if db is not None:
        return _safe_db(lambda: db.users.find_one({"username": username}, {"_id": 0}), None)
    return _jload("users", {}).get(username)

def db_set_user(username, data):
    db = get_db()
    if db is not None:
        _safe_db(lambda: db.users.replace_one(
            {"username": username}, {"username": username, **data}, upsert=True), None)
        return
    users = _jload("users", {})
    users[username] = data
    _jsave("users", users)

def db_all_users():
    db = get_db()
    if db is not None:
        return _safe_db(lambda: list(db.users.find({}, {"_id": 0})), [])
    users = _jload("users", {})
    return [{"username": u, **d} for u, d in users.items()]

def db_get_session(token):
    db = get_db()
    if db is not None:
        s = _safe_db(lambda: db.sessions.find_one({"token": token}, {"_id": 0}), None)
        if s and s["expires"] > time.time():
            return s
        return None
    sessions = _jload("sessions", {})
    s = sessions.get(token)
    if s and s["expires"] > time.time():
        return s
    return None

def db_set_session(token, username):
    expires = time.time() + 86400 * 7
    db = get_db()
    if db is not None:
        _safe_db(lambda: db.sessions.replace_one(
            {"token": token},
            {"token": token, "username": username, "expires": expires},
            upsert=True), None)
        return
    sessions = _jload("sessions", {})
    sessions[token] = {"username": username, "expires": expires}
    _jsave("sessions", sessions)

def db_get_predictions(username):
    db = get_db()
    if db is not None:
        doc = _safe_db(lambda: db.predictions.find_one({"username": username}, {"_id": 0}), None)
        return doc.get("picks", {}) if doc else {}
    return _jload("predictions", {}).get(username, {})

def db_set_prediction(username, match_id, prediction):
    db = get_db()
    if db is not None:
        _safe_db(lambda: db.predictions.update_one(
            {"username": username},
            {"$set": {"picks." + match_id: {"prediction": prediction, "ts": time.time()}}},
            upsert=True), None)
        return
    preds = _jload("predictions", {})
    if username not in preds:
        preds[username] = {}
    preds[username][match_id] = {"prediction": prediction, "ts": time.time()}
    _jsave("predictions", preds)

def db_all_predictions():
    db = get_db()
    if db is not None:
        docs = _safe_db(lambda: list(db.predictions.find({}, {"_id": 0})), [])
        return {doc["username"]: doc.get("picks", {}) for doc in docs}
    return _jload("predictions", {})

# ── Season reset ──────────────────────────────────────────────────────────────
_RESET_COLLECTIONS = ("predictions", "overrides", "ou_predictions", "ou_overrides")

def db_reset_season():
    """Fresh tournament: keep accounts, reset tokens/stats, wipe picks and results.

    Also clears the legacy ou_* collections left over from the World Cup version.
    Returns the number of accounts reset, or None if the database was unreachable.
    """
    db = get_db()
    if db is not None:
        def _wipe():
            for coll in _RESET_COLLECTIONS:
                db[coll].delete_many({})
            r = db.users.update_many({}, {"$set": {
                "tokens": INITIAL_TOKENS, "correct": 0, "predictions": 0}})
            return r.matched_count
        return _safe_db(_wipe, None)
    for name in _RESET_COLLECTIONS:
        _jsave(name, {})
    users = _jload("users", {})
    for u in users.values():
        u.update({"tokens": INITIAL_TOKENS, "correct": 0, "predictions": 0})
    _jsave("users", users)
    return len(users)


def db_get_override(match_id):
    db = get_db()
    if db is not None:
        doc = _safe_db(lambda: db.overrides.find_one({"match_id": match_id}, {"_id": 0}), None)
        return doc or {}
    return _jload("overrides", {}).get(match_id, {})

def db_set_override(match_id, data):
    db = get_db()
    if db is not None:
        _safe_db(lambda: db.overrides.replace_one(
            {"match_id": match_id},
            {"match_id": match_id, **data}, upsert=True), None)
        return
    overrides = _jload("overrides", {})
    overrides[match_id] = data
    _jsave("overrides", overrides)

def db_all_overrides():
    db = get_db()
    if db is not None:
        docs = _safe_db(lambda: list(db.overrides.find({}, {"_id": 0})), [])
        return {doc["match_id"]: doc for doc in docs}
    return _jload("overrides", {})

# ── Match feed ────────────────────────────────────────────────────────────────
# Every source returns a list of match dicts with these fields (see _normalise_match):
#   id, home, away, homeAbbr, awayAbbr, homeLogo, awayLogo,
#   homeScore, awayScore   series wins ("" until the match starts),
#   time                   ISO-8601 UTC start time,
#   stage                  key of STAGE_LABELS, bestOf (int), statusState "pre"|"in"|"post"
_match_cache = {"data": None, "ts": 0}
_match_lock  = threading.Lock()

# PowerShell script template (Windows fallback — Anaconda SSL is broken and the
# corporate proxy needs the logged-in user's credentials)
_PS_TPL = (
    "[System.Net.WebRequest]::DefaultWebProxy = [System.Net.WebRequest]::GetSystemWebProxy()\n"
    "[System.Net.WebRequest]::DefaultWebProxy.Credentials = [System.Net.CredentialCache]::DefaultCredentials\n"
    "$wc = New-Object System.Net.WebClient\n"
    "$wc.Proxy = [System.Net.WebRequest]::GetSystemWebProxy()\n"
    "$wc.Proxy.Credentials = [System.Net.CredentialCache]::DefaultCredentials\n"
    "$wc.Headers.Add('User-Agent','Mozilla/5.0')\n"
    "Write-Output $wc.DownloadString('{url}')\n"
)

def _http_get(url):
    """Return the response body for `url`, or None on failure."""
    if _HAS_REQUESTS and not IS_WIN:
        # Linux / Render: requests works natively
        try:
            r = _requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=20)
            r.raise_for_status()
            return r.text
        except Exception as e:
            print("[FEED] requests error:", e)
            return None
    try:
        script = _PS_TPL.format(url=url)
        proc = subprocess.run(
            ["powershell.exe", "-NoProfile", "-NonInteractive", "-Command", script],
            capture_output=True, text=True, timeout=25
        )
        if proc.returncode != 0 or not proc.stdout.strip():
            print("[FEED] PowerShell error:", proc.stderr[:200])
            return None
        return proc.stdout
    except Exception as e:
        print("[FEED] subprocess error:", e)
        return None

# ── Leaguepedia provider (lol.fandom.com MediaWiki Cargo API) ─────────────────
# Table MatchSchedule. NOT yet verified against live data: every anonymous
# cargoquery was rate-limited while this was written. To get a usable quota set
# LEAGUEPEDIA_USER / LEAGUEPEDIA_PASS to a Special:BotPasswords login (used on
# Linux/Render only — the Windows PowerShell path is anonymous GET). Set
# WORLDS_PAGE_PREFIX to the real OverviewPage prefix once it is confirmed.
LP_API    = "https://lol.fandom.com/api.php"
LP_UA     = "worlds2026-predictor/1.0 (internal prediction game)"
LP_PREFIX = os.environ.get("WORLDS_PAGE_PREFIX", "2026 Season World Championship")
LP_USER   = os.environ.get("LEAGUEPEDIA_USER", "")
LP_PASS   = os.environ.get("LEAGUEPEDIA_PASS", "")
LP_TTL     = 600    # seconds between real API calls
LP_BACKOFF = 900    # seconds to stay away after a rate-limit response
LP_PAGE    = 500    # rows per request (anonymous maximum)
LP_FIELDS  = ("MatchId,Team1,Team2,Team1Score,Team2Score,Winner,BestOf,"
              "OverviewPage,Tab,Phase,Round,IsNullified,DateTime_UTC=StartUTC")
_LP_PREFIX_OK = re.compile(r"^[A-Za-z0-9 /_.\-]+$")
_lp = {"data": None, "ts": 0, "next_try": 0, "session": None, "login_ts": 0, "last_error": ""}

def _lp_login():
    """Bot-password login. Returns a requests.Session, or None to go anonymous."""
    if not (LP_USER and LP_PASS and _HAS_REQUESTS and not IS_WIN):
        return None
    if _lp["session"] and time.time() - _lp["login_ts"] < 12 * 3600:
        return _lp["session"]
    try:
        s = _requests.Session()
        s.headers["User-Agent"] = LP_UA
        tok = s.get(LP_API, params={"action": "query", "meta": "tokens", "type": "login",
                                    "format": "json"}, timeout=20).json()["query"]["tokens"]["logintoken"]
        res = s.post(LP_API, data={"action": "login", "lgname": LP_USER, "lgpassword": LP_PASS,
                                   "lgtoken": tok, "format": "json"}, timeout=20).json().get("login", {})
        if res.get("result") != "Success":
            print("[LP] login failed:", res.get("result"), res.get("reason", ""))
            return None
        _lp["session"], _lp["login_ts"] = s, time.time()
        return s
    except Exception as e:
        print("[LP] login error:", type(e).__name__)   # type only — never echo request details
        return None

def _lp_get(params):
    """GET the API with `params`; return the parsed JSON dict, or None on failure."""
    q = dict(params, format="json")
    try:
        if _HAS_REQUESTS and not IS_WIN:
            getter = _lp_login() or _requests
            return getter.get(LP_API, params=q, headers={"User-Agent": LP_UA}, timeout=25).json()
        body = _http_get(LP_API + "?" + urllib.parse.urlencode(q))
        return json.loads(body) if body else None
    except Exception as e:
        print("[LP] request error:", type(e).__name__)
        return None

def _lp_clean(v):
    return html.unescape(v).strip() if isinstance(v, str) else ""

def _lp_stage(row):
    """Map Leaguepedia's page/tab/round naming onto STAGE_LABELS keys."""
    text = " ".join(_lp_clean(row.get(k)) for k in ("OverviewPage", "Tab", "Phase", "Round")).lower()
    if "play-in" in text or "play in" in text or "playin" in text: return "play-in"
    if "swiss" in text:    return "swiss"
    if "quarter" in text:  return "quarterfinals"    # before "final": "quarterfinals" contains it
    if "semi" in text:     return "semifinals"
    if "final" in text:    return "final"
    other = _lp_clean(row.get("Tab")) or _lp_clean(row.get("Round"))
    return re.sub(r"[^a-z0-9]+", "-", other.lower()).strip("-")

def _lp_rows_to_matches(rows, now_iso=None):
    """Convert cargoquery rows to the raw match dicts that _normalise_match expects."""
    now_iso = now_iso or time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    out, skipped = [], 0
    for r in rows:
        row = r.get("title", r) if isinstance(r, dict) else {}
        # Cargo may return the alias, or the field name with underscores shown as spaces
        start = _lp_clean(row.get("StartUTC") or row.get("DateTime UTC") or row.get("DateTime_UTC"))
        tm    = re.match(r"(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2})(?::(\d{2}))?", start)
        mid   = _lp_clean(row.get("MatchId"))
        if _lp_clean(row.get("IsNullified")) == "1" or not tm or not mid:
            skipped += 1
            continue
        iso    = "%sT%s:%sZ" % (tm.group(1), tm.group(2), tm.group(3) or "00")
        winner = {"1": "home", "2": "away"}.get(_lp_clean(row.get("Winner")))
        bo     = _lp_clean(row.get("BestOf"))
        out.append({
            "id":          mid,
            "home":        _lp_clean(row.get("Team1")),
            "away":        _lp_clean(row.get("Team2")),
            "homeScore":   _lp_clean(row.get("Team1Score")),
            "awayScore":   _lp_clean(row.get("Team2Score")),
            "time":        iso,
            "stage":       _lp_stage(row),
            "bestOf":      int(bo) if bo.isdigit() else None,
            "winner":      winner,
            "statusState": "post" if winner else ("in" if iso <= now_iso else "pre"),
        })
    if skipped:
        print("[LP] skipped {} row(s) without MatchId/start time or nullified".format(skipped))
    return out

def _provider_remote():
    """Worlds 2026 schedule/results from Leaguepedia's Cargo API.

    Own 10-min cache plus a 15-min backoff after a rate-limit, so the app's 120 s
    refresh does not turn into 120 s API calls. Returns the last good data on any
    failure, or None if there never was any (the caller then tries data/matches.json).
    """
    now = time.time()
    if _lp["data"] is not None and now - _lp["ts"] < LP_TTL:
        return _lp["data"]
    if now < _lp["next_try"]:
        return _lp["data"]
    if not _LP_PREFIX_OK.match(LP_PREFIX):
        print("[LP] WORLDS_PAGE_PREFIX has unsupported characters — ignoring remote feed")
        _lp["next_try"] = now + LP_TTL
        return _lp["data"]

    rows = []
    for page in range(5):
        data = _lp_get({
            "action": "cargoquery", "tables": "MatchSchedule", "fields": LP_FIELDS,
            "where": "OverviewPage LIKE '{}%'".format(LP_PREFIX),
            "order_by": "DateTime_UTC", "limit": LP_PAGE, "offset": page * LP_PAGE,
        })
        err = (data or {}).get("error")
        if data is None or err:
            code = err.get("code") if isinstance(err, dict) else "no response"
            print("[LP] fetch failed:", code)
            _lp["last_error"] = str(code)
            _lp["next_try"] = now + (LP_BACKOFF if code == "ratelimited" else 120)
            return _lp["data"]                       # discard partial pages, keep last good
        batch = data.get("cargoquery") or []
        rows.extend(batch)
        if len(batch) < LP_PAGE:
            break

    matches = _lp_rows_to_matches(rows)
    if not matches:
        print("[LP] no usable matches for prefix {!r} — check WORLDS_PAGE_PREFIX".format(LP_PREFIX))
        _lp["last_error"] = "no matches found for page prefix '{}'".format(LP_PREFIX)
        _lp["next_try"] = now + 300
        return _lp["data"]
    _lp["data"], _lp["ts"], _lp["last_error"] = matches, now, ""
    return matches

# ── Riot LoL Esports provider (esports-api.lolesports.com) ────────────────────
# The API lolesports.com itself uses. Undocumented; the x-api-key below is the
# public key shipped in the lolesports.com web client (not a secret). Written from
# the known response format and NOT yet verified against a live response — it is
# blocked on the Bosch proxy, so it only runs where requests works (Render).
RIOT_API    = "https://esports-api.lolesports.com/persisted/gw/getSchedule"
RIOT_KEY    = os.environ.get("RIOT_API_KEY", "0TvQnueqKa5mxJntVWt0w4LpLfEkrV1Ta8rQBb9Z")
RIOT_LEAGUE = os.environ.get("RIOT_LEAGUE_ID", "98767975604431411")   # "Worlds"
RIOT_FROM   = os.environ.get("RIOT_FROM", "2026-09-01")               # ignore older seasons
RIOT_TTL    = 300
RIOT_PAGES  = 12                                                       # max pages walked each way
_riot = {"data": None, "ts": 0, "next_try": 0}

_LOGO_HOST = "static.lolesports.com"
_LOGO_URL  = re.compile(r"^https?://" + re.escape(_LOGO_HOST) + r"/teams/([A-Za-z0-9._\-]{1,200}\.(?:png|jpg|jpeg|webp|gif|svg))$", re.I)
_LOGO_NAME = re.compile(r"^[A-Za-z0-9._\-]{1,200}\.(?:png|jpg|jpeg|webp|gif|svg)$", re.I)
_LOGO_MAX  = 2 * 1024 * 1024
_logo_cache = {}                     # filename -> (expires_at, content_type, bytes) or (expires_at, None, None)
_logo_lock  = threading.Lock()

def _logo_url(url):
    """Point team logos at our own /logo/<file> route.

    Riot's image host serves http:// links (blocked as mixed content on the https site) and
    can be unreachable from some networks, so the server fetches and caches the images.
    URLs that are not on the expected host are upgraded to https and passed through.
    """
    url = url or ""
    m = _LOGO_URL.match(url)
    if m:
        return "/logo/" + m.group(1)
    return "https://" + url[len("http://"):] if url.startswith("http://") else url

def _fetch_logo_upstream(name):
    """Return (content_type, bytes) for a logo from Riot's image host, or None."""
    if not _HAS_REQUESTS:
        return None
    try:
        r = _requests.get("https://{}/teams/{}".format(_LOGO_HOST, name),
                          headers={"User-Agent": LP_UA}, timeout=15, stream=True)
        ctype = (r.headers.get("Content-Type") or "").split(";")[0].strip().lower()
        if r.status_code != 200 or not ctype.startswith("image/"):
            print("[LOGO] upstream {} for {} ({})".format(r.status_code, name, ctype or "no type"))
            return None
        data = r.raw.read(_LOGO_MAX + 1, decode_content=True)
        if len(data) > _LOGO_MAX:
            print("[LOGO] {} too large".format(name))
            return None
        return ctype, data
    except Exception as e:
        print("[LOGO] fetch error:", type(e).__name__)
        return None

def get_logo(name):
    """Cached logo lookup. Returns (content_type, bytes) or None. Successes are kept 24 h, failures 5 min."""
    if not _LOGO_NAME.match(name):
        return None
    now = time.time()
    with _logo_lock:
        hit = _logo_cache.get(name)
        if hit and hit[0] > now:
            return (hit[1], hit[2]) if hit[1] else None
    got = _fetch_logo_upstream(name)
    with _logo_lock:
        if len(_logo_cache) >= 300:
            _logo_cache.clear()
        _logo_cache[name] = (now + (86400 if got else 300),) + (got if got else (None, None))
    return got

def _riot_events_to_matches(events):
    """Convert getSchedule events to the raw match dicts _normalise_match expects."""
    out = []
    for ev in events:
        try:
            if ev.get("type") != "match":
                continue
            match = ev.get("match") or {}
            teams = match.get("teams") or []
            start = str(ev.get("startTime") or "")
            if len(teams) != 2 or not match.get("id") or start[:10] < RIOT_FROM:
                continue
            t1, t2 = teams
            state = {"completed": "post", "inProgress": "in"}.get(ev.get("state"), "pre")
            r1, r2 = (t1.get("result") or {}), (t2.get("result") or {})
            winner = "home" if r1.get("outcome") == "win" else "away" if r2.get("outcome") == "win" else None
            out.append({
                "id":          str(match["id"]),
                "home":        t1.get("name") or "TBD",   "away":      t2.get("name") or "TBD",
                "homeAbbr":    t1.get("code") or "",      "awayAbbr":  t2.get("code") or "",
                "homeLogo":    _logo_url(t1.get("image")), "awayLogo": _logo_url(t2.get("image")),
                "homeScore":   r1.get("gameWins", ""),    "awayScore": r2.get("gameWins", ""),
                "time":        start,
                "stage":       _lp_stage({"Tab": ev.get("blockName") or ""}),
                "bestOf":      (match.get("strategy") or {}).get("count"),
                "winner":      winner if state == "post" else None,
                "statusState": state,
            })
        except (AttributeError, TypeError, KeyError):
            continue
    return out

def _riot_get(token=None):
    params = {"hl": "en-US", "leagueId": RIOT_LEAGUE}
    if token:
        params["pageToken"] = token
    r = _requests.get(RIOT_API, params=params, headers={"x-api-key": RIOT_KEY, "User-Agent": LP_UA}, timeout=20)
    r.raise_for_status()
    return (r.json().get("data") or {}).get("schedule") or {}

def _provider_riot():
    """Worlds schedule/results from the Riot LoL Esports API (cached, backs off on failure)."""
    now = time.time()
    if _riot["data"] is not None and now - _riot["ts"] < RIOT_TTL:
        return _riot["data"]
    if now < _riot["next_try"]:
        return _riot["data"]
    if not (_HAS_REQUESTS and not IS_WIN):
        return None                                # local Windows: proxy blocks it, skip quietly
    try:
        first = _riot_get()
        events = list(first.get("events") or [])
        pages  = first.get("pages") or {}
        for direction in ("newer", "older"):       # the API returns a window around "now"
            token, cur = pages.get(direction), first
            for _ in range(RIOT_PAGES):
                if not token:
                    break
                cur = _riot_get(token)
                batch = cur.get("events") or []
                events.extend(batch)
                token = (cur.get("pages") or {}).get(direction)
                if direction == "older" and batch and min(str(e.get("startTime", "")) for e in batch)[:10] < RIOT_FROM:
                    break
        matches = _riot_events_to_matches(events)
        if not matches:
            raise ValueError("no matches since {} for league {}".format(RIOT_FROM, RIOT_LEAGUE))
        seen = {}
        for m in matches:
            seen[m["id"]] = m                      # de-dupe overlapping pages
        _riot["data"], _riot["ts"] = list(seen.values()), now
        _lp["last_error"] = ""
        return _riot["data"]
    except Exception as e:
        print("[RIOT] fetch failed:", type(e).__name__, str(e)[:120])
        _riot["next_try"] = now + 120
        _lp["riot_error"] = "{}: {}".format(type(e).__name__, str(e)[:80])
        return _riot["data"]

def _provider_local_file():
    """Dev/test feed: data/matches.json holds a list of match dicts."""
    data = _jload("matches", None)
    return data if isinstance(data, list) else None

def _score_to_result(home_score, away_score):
    try:
        h, a = int(home_score), int(away_score)
    except (ValueError, TypeError):
        return None
    if h > a: return "home"
    if a > h: return "away"
    return None   # a best-of series cannot finish level

def _s(v):
    return "" if v is None else str(v)

def _normalise_match(m):
    state = m.get("statusState", "pre")
    stage = m.get("stage", "")
    out = {
        "id":          str(m["id"]),
        "home":        m.get("home") or "TBD",
        "away":        m.get("away") or "TBD",
        "homeAbbr":    m.get("homeAbbr", ""),
        "awayAbbr":    m.get("awayAbbr", ""),
        "homeLogo":    m.get("homeLogo", ""),
        "awayLogo":    m.get("awayLogo", ""),
        "homeScore":   _s(m.get("homeScore")),
        "awayScore":   _s(m.get("awayScore")),
        "time":        m["time"],
        "stage":       stage,
        "stageLabel":  STAGE_LABELS.get(stage, stage.replace("-", " ").title()),
        "bestOf":      m.get("bestOf"),
        "statusState": state,
        "statusDesc":  m.get("statusDesc") or {"pre": "Scheduled", "in": "Live", "post": "Final"}.get(state, state),
    }
    out["sourceResult"] = None
    if state == "post":
        if m.get("winner") in ("home", "away"):      # explicit winner (covers forfeits with no scores)
            out["sourceResult"] = m["winner"]
        elif out["homeScore"] != "" and out["awayScore"] != "":
            out["sourceResult"] = _score_to_result(out["homeScore"], out["awayScore"])
    return out

def fetch_matches():
    """Return list of normalised match dicts, soonest first (cached 120 s)."""
    with _match_lock:
        now = time.time()
        if _match_cache["data"] and now - _match_cache["ts"] < 120:
            return _match_cache["data"]

        raw = _provider_riot()
        if raw is None:
            raw = _provider_remote()
        if raw is None:
            raw = _provider_local_file()
        if raw is None:
            return _match_cache["data"]  # stale on failure

        matches = []
        for m in raw:
            try:
                matches.append(_normalise_match(m))
            except (KeyError, TypeError, AttributeError) as e:
                print("[FEED] skipping malformed match:", e)
        matches.sort(key=lambda m: m["time"])

        _match_cache["data"] = matches
        _match_cache["ts"]   = now

        # Auto-award runs in background — never blocks the HTTP response
        threading.Thread(target=_auto_award, args=(matches,), daemon=True).start()

        return matches


def _auto_award(matches):
    """Auto-award tokens for finished matches from the source result."""
    try:
        # Load all predictions once — avoids repeated DB calls per match
        all_preds = db_all_predictions()

        for m in matches:
            if m["statusState"] != "post" or not m.get("sourceResult"):
                continue
            try:
                ov = db_get_override(m["id"])
                if ov.get("result"):
                    continue
                result = m["sourceResult"]
                print("[AUTO] Winner {} for {} vs {}".format(result, m["home"], m["away"]))
                db_set_override(m["id"], {"result": result, "locked": True, "auto": True})
                for uname, picks in all_preds.items():
                    p = picks.get(m["id"])
                    if p and p.get("prediction") == result:
                        u = db_get_user(uname)
                        if u:
                            u["tokens"]  = u.get("tokens", 0) + WIN_REWARD
                            u["correct"] = u.get("correct", 0) + 1
                            db_set_user(uname, u)
            except Exception as e:
                print("[AUTO] Award error for {}: {}".format(m["id"], e))

    except Exception as e:
        print("[AUTO] _auto_award failed:", e)

# ── Helpers ───────────────────────────────────────────────────────────────────
def hash_pw(pw):
    return hashlib.sha256(pw.encode()).hexdigest()

def json_resp(handler, code, data):
    body = json.dumps(data, ensure_ascii=False).encode("utf-8")
    handler.send_response(code)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", len(body))
    handler.send_header("Access-Control-Allow-Origin", "*")
    handler.end_headers()
    handler.wfile.write(body)

def read_body(handler):
    length = int(handler.headers.get("Content-Length", 0))
    if length:
        return json.loads(handler.rfile.read(length))
    return {}

def auth_user(handler):
    token = handler.headers.get("Authorization", "").replace("Bearer ", "").strip()
    s = db_get_session(token)
    if not s:
        return None, None
    u = db_get_user(s["username"])
    return u, s["username"]

def check_admin(body):
    return hash_pw(body.get("admin_password", "")) == hash_pw(ADMIN_PW)

# ── Request handler ───────────────────────────────────────────────────────────
class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization")
        self.end_headers()

    def handle_one_request(self):
        """Override to catch all unhandled exceptions and return 500."""
        try:
            super().handle_one_request()
        except Exception as e:
            print("[SERVER] Unhandled exception:", e)
            try:
                json_resp(self, 500, {"error": "Internal server error"})
            except Exception:
                pass

    def do_GET(self):
        path = urllib.parse.urlparse(self.path).path.rstrip("/")
        if path == "/healthz":
            # Lightweight health check for Render — never blocks
            body = b"ok"
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", len(body))
            self.end_headers()
            self.wfile.write(body)
        elif path in ("", "/"):
            self._serve_file("index.html", "text/html; charset=utf-8")
        elif path == "/admin":
            self._serve_file("admin.html", "text/html; charset=utf-8")
        elif path.startswith("/logo/"):
            got = get_logo(path[len("/logo/"):])
            if not got:
                self.send_response(404); self.send_header("Content-Length", "0"); self.end_headers(); return
            ctype, data = got
            self.send_response(200)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", len(data))
            self.send_header("Cache-Control", "public, max-age=86400")
            self.send_header("X-Content-Type-Options", "nosniff")
            if ctype == "image/svg+xml":
                self.send_header("Content-Security-Policy", "default-src 'none'; style-src 'unsafe-inline'")
            self.end_headers()
            self.wfile.write(data)
        elif path.startswith("/api"):
            self._get(path)
        else:
            self.send_response(404); self.end_headers()

    def do_POST(self):
        path = urllib.parse.urlparse(self.path).path.rstrip("/")
        if path.startswith("/api"):
            self._post(path)
        else:
            self.send_response(404); self.end_headers()

    def _serve_file(self, filename, ctype):
        fpath = os.path.join(os.path.dirname(os.path.abspath(__file__)), filename)
        if not os.path.exists(fpath):
            self.send_response(404); self.end_headers(); return
        with open(fpath, "rb") as f:
            data = f.read()
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", len(data))
        self.end_headers()
        self.wfile.write(data)

    # ── GET routes ────────────────────────────────────────────────────────────
    def _get(self, path):
        if path == "/api/matches":
            matches = fetch_matches()
            if matches is None:
                why = "Riot: {}; Leaguepedia: {}".format(_lp.get("riot_error") or "not fetched yet",
                                                         _lp["last_error"] or "not fetched yet")
                json_resp(self, 502, {"error": "No Worlds 2026 match data available yet. ({})".format(why)}); return
            overrides = db_all_overrides()
            result = []
            for m in matches:
                ov    = overrides.get(m["id"], {})
                entry = dict(m)
                entry["locked"]      = ov.get("locked", False) or m["statusState"] in ("in", "post")
                entry["adminResult"] = ov.get("result")
                result.append(entry)
            json_resp(self, 200, result)

        elif path == "/api/leaderboard":
            users = db_all_users()

            # Build set of finished match IDs (statusState == "post")
            matches   = fetch_matches() or []
            overrides = db_all_overrides()
            finished_ids = {
                m["id"] for m in matches
                if m["statusState"] == "post"
                or overrides.get(m["id"], {}).get("result")
            }

            all_preds = db_all_predictions()

            board = []
            for u in users:
                tokens   = u.get("tokens", 0)
                correct  = u.get("correct", 0)
                net_gain = tokens - INITIAL_TOKENS

                # Count predictions on finished matches only (for accuracy denominator)
                uname          = u["username"]
                finished_count = sum(1 for mid in all_preds.get(uname, {}) if mid in finished_ids)

                board.append({
                    "username":       uname,
                    "tokens":         tokens,
                    "netGain":        net_gain,
                    "correct":        correct,
                    "predictions":    u.get("predictions", 0),  # all bets placed
                    "finishedBets":   finished_count,           # bets on finished matches (accuracy denominator)
                })
            board.sort(key=lambda x: (-x["netGain"], -x["correct"], -x["finishedBets"]))
            json_resp(self, 200, board)

        elif path == "/api/me":
            user, uname = auth_user(self)
            if not user:
                json_resp(self, 401, {"error": "Unauthorized"}); return
            json_resp(self, 200, {
                "username":         uname,
                "tokens":           user.get("tokens", 0),
                "correct":          user.get("correct", 0),
                "predictions":      db_get_predictions(uname),    # dict of winner picks
                "predictionCount":  user.get("predictions", 0),   # total count
            })

        elif path == "/api/predictions":
            user, uname = auth_user(self)
            if not user:
                json_resp(self, 401, {"error": "Unauthorized"}); return
            json_resp(self, 200, db_get_predictions(uname))

        else:
            json_resp(self, 404, {"error": "Not found"})

    # ── POST routes ───────────────────────────────────────────────────────────
    def _post(self, path):
        body = read_body(self)

        if path == "/api/register":
            username = body.get("username", "").strip()
            password = body.get("password", "")
            if not username or not password:
                json_resp(self, 400, {"error": "username and password required"}); return
            if not re.match(r'^[A-Za-z0-9_]{3,20}$', username):
                json_resp(self, 400, {"error": "Username: 3-20 chars, letters/digits/_"}); return
            if db_get_user(username):
                json_resp(self, 409, {"error": "Username already taken"}); return
            db_set_user(username, {
                "password": hash_pw(password),
                "tokens": INITIAL_TOKENS,
                "correct": 0, "predictions": 0
            })
            json_resp(self, 201, {"message": "Account created", "tokens": INITIAL_TOKENS})

        elif path == "/api/login":
            username = body.get("username", "").strip()
            password = body.get("password", "")
            u = db_get_user(username)
            if not u or u.get("password") != hash_pw(password):
                json_resp(self, 401, {"error": "Invalid credentials"}); return
            tok = str(uuid.uuid4())
            db_set_session(tok, username)
            json_resp(self, 200, {"token": tok, "username": username, "tokens": u.get("tokens", 0)})

        elif path == "/api/predict":
            user, uname = auth_user(self)
            if not user:
                json_resp(self, 401, {"error": "Unauthorized"}); return
            match_id   = str(body.get("match_id", "")).strip()
            prediction = body.get("prediction")
            if prediction not in ("home", "away"):
                json_resp(self, 400, {"error": "prediction must be home or away"}); return
            if not match_id:
                json_resp(self, 400, {"error": "match_id required"}); return

            # Always use fresh/cached match data — fetch now if cache is empty
            cached = _match_cache.get("data") or fetch_matches() or []
            m_src = next((m for m in cached if m["id"] == match_id), None)

            # Reject unknown match IDs first
            if m_src is None:
                json_resp(self, 404, {"error": "Match not found"}); return

            # Reject if match has started or finished
            if m_src["statusState"] in ("in", "post"):
                json_resp(self, 400, {"error": "Match has already started"}); return

            # Reject if admin-locked or result already set
            ov = db_get_override(match_id)
            if ov.get("locked") or ov.get("result"):
                json_resp(self, 400, {"error": "Match is locked or result already set"}); return

            existing = db_get_predictions(uname).get(match_id)
            if existing:
                db_set_prediction(uname, match_id, prediction)
                json_resp(self, 200, {"message": "Prediction updated", "tokens": user.get("tokens", 0)}); return

            if user.get("tokens", 0) < BET_COST:
                json_resp(self, 400, {"error": "Not enough tokens"}); return

            user["tokens"]     -= BET_COST
            user["predictions"] = user.get("predictions", 0) + 1
            db_set_user(uname, user)
            db_set_prediction(uname, match_id, prediction)
            json_resp(self, 200, {"message": "Prediction placed", "tokens": user["tokens"]})

        elif path == "/api/admin/result":
            if not check_admin(body):
                json_resp(self, 403, {"error": "Forbidden"}); return
            match_id = str(body.get("match_id", "")).strip()
            result   = body.get("result")
            if result not in ("home", "away"):
                json_resp(self, 400, {"error": "result must be home or away"}); return
            ov = db_get_override(match_id)
            if ov.get("result"):
                json_resp(self, 400, {"error": "Result already set"}); return
            db_set_override(match_id, {"result": result, "locked": True})

            # Award winners
            all_preds = db_all_predictions()
            winners = []
            for uname, picks in all_preds.items():
                p = picks.get(match_id)
                if p and p.get("prediction") == result:
                    u = db_get_user(uname)
                    if u:
                        u["tokens"]  = u.get("tokens", 0) + WIN_REWARD
                        u["correct"] = u.get("correct", 0) + 1
                        db_set_user(uname, u)
                        winners.append(uname)
            json_resp(self, 200, {"message": "Result set", "winners": winners,
                                   "winner_count": len(winners)})

        elif path == "/api/admin/clear_result":
            if not check_admin(body):
                json_resp(self, 403, {"error": "Forbidden"}); return
            match_id = str(body.get("match_id", "")).strip()
            if not match_id:
                json_resp(self, 400, {"error": "match_id required"}); return
            ov = db_get_override(match_id)
            old_result = ov.get("result")
            if not old_result:
                json_resp(self, 400, {"error": "No result to clear"}); return
            all_preds = db_all_predictions()
            reverted = []
            for uname, picks in all_preds.items():
                p = picks.get(match_id)
                if p and p.get("prediction") == old_result:
                    u = db_get_user(uname)
                    if u:
                        u["tokens"]  = u.get("tokens", 0) - WIN_REWARD
                        u["correct"] = max(u.get("correct", 0) - 1, 0)
                        db_set_user(uname, u)
                        reverted.append(uname)
            db_set_override(match_id, {})
            json_resp(self, 200, {"message": "Result cleared", "old_result": old_result,
                                   "reverted_users": reverted})

        elif path == "/api/admin/reset_season":
            # Start a fresh tournament: keep accounts, reset tokens, wipe picks + results
            if not check_admin(body):
                json_resp(self, 403, {"error": "Forbidden"}); return
            if body.get("confirm") != "RESET":
                json_resp(self, 400, {"error": 'confirm must be "RESET"'}); return
            n = db_reset_season()
            if n is None:
                json_resp(self, 500, {"error": "Reset failed — database unavailable, nothing was changed"}); return
            json_resp(self, 200, {"message": "Season reset", "users_reset": n,
                                   "tokens": INITIAL_TOKENS})

        elif path == "/api/admin/reset_password":
            if not check_admin(body):
                json_resp(self, 403, {"error": "Forbidden"}); return
            username = body.get("username", "").strip()
            new_pw   = body.get("new_password", "").strip()
            if not username or not new_pw:
                json_resp(self, 400, {"error": "username and new_password required"}); return
            if len(new_pw) < 4:
                json_resp(self, 400, {"error": "Password must be at least 4 characters"}); return
            u = db_get_user(username)
            if not u:
                json_resp(self, 404, {"error": "User not found"}); return
            u["password"] = hash_pw(new_pw)
            db_set_user(username, u)
            json_resp(self, 200, {"message": "Password reset for {}".format(username)})

        elif path == "/api/admin/lock":
            if not check_admin(body):
                json_resp(self, 403, {"error": "Forbidden"}); return
            match_id = str(body.get("match_id", "")).strip()
            locked   = bool(body.get("locked", True))
            ov = db_get_override(match_id)
            ov["locked"] = locked
            db_set_override(match_id, ov)
            json_resp(self, 200, {"message": "Lock updated"})

        else:
            json_resp(self, 404, {"error": "Not found"})


class ThreadedHTTPServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    """Handle each request in its own thread — prevents the match-feed fetch from blocking health checks."""
    daemon_threads = True


if __name__ == "__main__":
    # Warm-up DB connection and match cache in background
    threading.Thread(target=get_db, daemon=True).start()
    threading.Thread(target=fetch_matches, daemon=True).start()

    server = ThreadedHTTPServer(("0.0.0.0", PORT), Handler)
    print("=" * 52)
    print("  Worlds 2026 Predictor")
    print("  http://localhost:{}".format(PORT))
    print("  Admin: http://localhost:{}/admin".format(PORT))
    print("  Storage: {}".format("MongoDB Atlas" if (MONGO_URI and _HAS_PYMONGO) else "Local JSON files"))
    print("=" * 52)
    server.serve_forever()

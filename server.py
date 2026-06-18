#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
World Cup 2026 Predictor
- ESPN live data (requests on Linux/Render, PowerShell subprocess on Windows)
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

ESPN_URL = (
    "https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world"
    "/scoreboard?dates=20260611-20260720&limit=200"
)

STAGE_LABELS = {
    "group-stage":     "Group Stage",
    "round-of-32":     "Round of 32",
    "round-of-16":     "Round of 16",
    "quarterfinals":   "Quarter-Finals",
    "semifinals":      "Semi-Finals",
    "3rd-place-match": "3rd Place Match",
    "final":           "Final",
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

# ── O/U predictions ───────────────────────────────────────────────────────────
def db_get_ou_predictions(username):
    db = get_db()
    if db is not None:
        doc = _safe_db(lambda: db.ou_predictions.find_one({"username": username}, {"_id": 0}), None)
        return doc.get("picks", {}) if doc else {}
    return _jload("ou_predictions", {}).get(username, {})

def db_set_ou_prediction(username, match_id, prediction):
    db = get_db()
    if db is not None:
        _safe_db(lambda: db.ou_predictions.update_one(
            {"username": username},
            {"$set": {"picks." + match_id: {"prediction": prediction, "ts": time.time()}}},
            upsert=True), None)
        return
    preds = _jload("ou_predictions", {})
    if username not in preds:
        preds[username] = {}
    preds[username][match_id] = {"prediction": prediction, "ts": time.time()}
    _jsave("ou_predictions", preds)

def db_all_ou_predictions():
    db = get_db()
    if db is not None:
        docs = _safe_db(lambda: list(db.ou_predictions.find({}, {"_id": 0})), [])
        return {doc["username"]: doc.get("picks", {}) for doc in docs}
    return _jload("ou_predictions", {})

def db_get_ou_override(match_id):
    db = get_db()
    if db is not None:
        doc = _safe_db(lambda: db.ou_overrides.find_one({"match_id": match_id}, {"_id": 0}), None)
        return doc or {}
    return _jload("ou_overrides", {}).get(match_id, {})

def db_set_ou_override(match_id, data):
    db = get_db()
    if db is not None:
        _safe_db(lambda: db.ou_overrides.replace_one(
            {"match_id": match_id},
            {"match_id": match_id, **data}, upsert=True), None)
        return
    overrides = _jload("ou_overrides", {})
    overrides[match_id] = data
    _jsave("ou_overrides", overrides)

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

# ── ESPN fetch ────────────────────────────────────────────────────────────────
_espn_cache = {"data": None, "ts": 0}
_espn_lock  = threading.Lock()

# PowerShell script template (Windows fallback)
_PS_TPL = (
    "[System.Net.WebRequest]::DefaultWebProxy = [System.Net.WebRequest]::GetSystemWebProxy()\n"
    "[System.Net.WebRequest]::DefaultWebProxy.Credentials = [System.Net.CredentialCache]::DefaultCredentials\n"
    "$wc = New-Object System.Net.WebClient\n"
    "$wc.Proxy = [System.Net.WebRequest]::GetSystemWebProxy()\n"
    "$wc.Proxy.Credentials = [System.Net.CredentialCache]::DefaultCredentials\n"
    "$wc.Headers.Add('User-Agent','Mozilla/5.0')\n"
    "Write-Output $wc.DownloadString('{url}')\n"
)

def _fetch_espn_raw():
    """Return raw JSON string from ESPN, or None on failure."""
    if _HAS_REQUESTS and not IS_WIN:
        # Linux / Render: requests works natively
        try:
            r = _requests.get(ESPN_URL,
                headers={"User-Agent": "Mozilla/5.0"}, timeout=20)
            return r.text
        except Exception as e:
            print("[ESPN] requests error:", e)
            return None
    else:
        # Windows: SSL broken in Anaconda, use PowerShell
        try:
            script = _PS_TPL.format(url=ESPN_URL)
            proc = subprocess.run(
                ["powershell.exe", "-NoProfile", "-NonInteractive", "-Command", script],
                capture_output=True, text=True, timeout=25
            )
            if proc.returncode != 0 or not proc.stdout.strip():
                print("[ESPN] PowerShell error:", proc.stderr[:200])
                return None
            return proc.stdout
        except Exception as e:
            print("[ESPN] subprocess error:", e)
            return None

def _score_to_result(home_score, away_score):
    try:
        h, a = int(home_score), int(away_score)
        if h > a: return "home"
        if a > h: return "away"
        return "draw"
    except (ValueError, TypeError):
        return None

def fetch_espn():
    """Return list of normalised match dicts (cached 120 s)."""
    with _espn_lock:
        now = time.time()
        if _espn_cache["data"] and now - _espn_cache["ts"] < 120:
            return _espn_cache["data"]

        raw = _fetch_espn_raw()
        if not raw:
            return _espn_cache["data"]  # stale on failure

        try:
            data = json.loads(raw, strict=False)
        except Exception as e:
            print("[ESPN] JSON parse error:", e)
            return _espn_cache["data"]

        matches = []
        for event in data.get("events", []):
            comp   = event["competitions"][0]
            status = comp["status"]["type"]
            state  = status.get("state", "pre")
            desc   = status.get("description", "Scheduled")
            slug   = event.get("season", {}).get("slug", "")

            comps   = comp.get("competitors", [])
            home_c  = next((c for c in comps if c.get("homeAway") == "home"), comps[0] if comps else {})
            away_c  = next((c for c in comps if c.get("homeAway") == "away"), comps[1] if len(comps) > 1 else {})
            home_t  = home_c.get("team", {})
            away_t  = away_c.get("team", {})
            eid     = str(event["id"])

            home_score = home_c.get("score", "")
            away_score = away_c.get("score", "")
            espn_result = None
            if state == "post" and home_score != "" and away_score != "":
                espn_result = _score_to_result(home_score, away_score)

            # O/U odds — ESPN returns odds as a LIST, first item is the primary provider
            odds_raw   = comp.get("odds") or []
            odds       = odds_raw[0] if isinstance(odds_raw, list) and odds_raw else (odds_raw if isinstance(odds_raw, dict) else {})
            ou_line    = odds.get("overUnder") or None
            over_odds  = odds.get("overOdds")  or None
            under_odds = odds.get("underOdds") or None

            # For finished matches derive O/U result from final score
            ou_result = None
            if state == "post" and home_score != "" and away_score != "":
                try:
                    total = int(home_score) + int(away_score)
                    if ou_line is not None:
                        if total > ou_line:   ou_result = "over"
                        elif total < ou_line: ou_result = "under"
                        # exact line = push (no result, no award)
                except (ValueError, TypeError):
                    pass

            matches.append({
                "id":          eid,
                "home":        home_t.get("displayName", "TBD"),
                "away":        away_t.get("displayName", "TBD"),
                "homeAbbr":    home_t.get("abbreviation", ""),
                "awayAbbr":    away_t.get("abbreviation", ""),
                "homeLogo":    home_t.get("logo", ""),
                "awayLogo":    away_t.get("logo", ""),
                "homeScore":   home_score,
                "awayScore":   away_score,
                "time":        event.get("date", ""),
                "stage":       slug,
                "stageLabel":  STAGE_LABELS.get(slug, slug.replace("-", " ").title()),
                "venue":       comp.get("venue", {}).get("fullName", ""),
                "statusState": state,
                "statusDesc":  desc,
                "espnResult":  espn_result,
                "ouLine":      ou_line,
                "overOdds":    over_odds,
                "underOdds":   under_odds,
                "ouResult":    ou_result,
            })

        _espn_cache["data"] = matches
        _espn_cache["ts"]   = now

        # Auto-award runs in background — never blocks the HTTP response
        threading.Thread(target=_auto_award, args=(matches,), daemon=True).start()

        return matches


def _auto_award(matches):
    """Auto-award tokens for finished matches (win/draw and O/U) from ESPN scores."""
    try:
        # Load all predictions once — avoids repeated DB calls per match
        all_preds = db_all_predictions()
        all_ou    = db_all_ou_predictions()

        for m in matches:
            if m["statusState"] != "post":
                continue

            # ── Win/Draw/Loss ─────────────────────────────────────────────────
            try:
                if m.get("espnResult"):
                    ov = db_get_override(m["id"])
                    if not ov.get("result"):
                        result = m["espnResult"]
                        print("[AUTO] Win result {} for {} vs {}".format(result, m["home"], m["away"]))
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
                print("[AUTO] Win award error for {}: {}".format(m["id"], e))

            # ── Over/Under ────────────────────────────────────────────────────
            try:
                if m.get("ouResult"):
                    ou_ov = db_get_ou_override(m["id"])
                    if not ou_ov.get("result"):
                        ou_result = m["ouResult"]
                        print("[AUTO] O/U result {} for {} vs {}".format(ou_result, m["home"], m["away"]))
                        db_set_ou_override(m["id"], {"result": ou_result, "locked": True, "auto": True})
                        for uname, picks in all_ou.items():
                            p = picks.get(m["id"])
                            if p and p.get("prediction") == ou_result:
                                u = db_get_user(uname)
                                if u:
                                    u["tokens"]  = u.get("tokens", 0) + WIN_REWARD
                                    u["correct"] = u.get("correct", 0) + 1
                                    db_set_user(uname, u)
            except Exception as e:
                print("[AUTO] O/U award error for {}: {}".format(m["id"], e))

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
        if path == "/api/espn/matches":
            matches = fetch_espn()
            if matches is None:
                json_resp(self, 502, {"error": "Could not reach ESPN. Check internet/proxy."}); return
            overrides = db_all_overrides()
            result = []
            for m in matches:
                ov = overrides.get(m["id"], {})
                entry = dict(m)
                entry["locked"]      = ov.get("locked", False) or m["statusState"] in ("in", "post")
                entry["adminResult"] = ov.get("result")
                result.append(entry)
            json_resp(self, 200, result)

        elif path == "/api/leaderboard":
            users = db_all_users()
            board = []
            for u in users:
                tokens      = u.get("tokens", 0)
                predictions = u.get("predictions", 0)
                correct     = u.get("correct", 0)
                net_gain    = tokens - INITIAL_TOKENS   # positive = profit, negative = loss
                board.append({
                    "username":    u["username"],
                    "tokens":      tokens,
                    "netGain":     net_gain,
                    "correct":     correct,
                    "predictions": predictions,
                })
            # Sort by: net gain desc → correct desc → predictions desc
            # Inactive accounts (0 predictions) all sit at net_gain=0 at the bottom
            board.sort(key=lambda x: (-x["netGain"], -x["correct"], -x["predictions"]))
            json_resp(self, 200, board)

        elif path == "/api/me":
            user, uname = auth_user(self)
            if not user:
                json_resp(self, 401, {"error": "Unauthorized"}); return
            json_resp(self, 200, {
                "username":       uname,
                "tokens":         user.get("tokens", 0),
                "correct":        user.get("correct", 0),
                "predictions":    db_get_predictions(uname),
                "ouPredictions":  db_get_ou_predictions(uname),
            })

        elif path == "/api/predictions":
            user, uname = auth_user(self)
            if not user:
                json_resp(self, 401, {"error": "Unauthorized"}); return
            json_resp(self, 200, db_get_predictions(uname))

        elif path == "/api/ou_predictions":
            user, uname = auth_user(self)
            if not user:
                json_resp(self, 401, {"error": "Unauthorized"}); return
            json_resp(self, 200, db_get_ou_predictions(uname))

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
            if prediction not in ("home", "away", "draw"):
                json_resp(self, 400, {"error": "prediction must be home, away, or draw"}); return
            if not match_id:
                json_resp(self, 400, {"error": "match_id required"}); return

            # Always use fresh/cached ESPN data — fetch now if cache is empty
            cached = _espn_cache.get("data") or fetch_espn() or []
            m_espn = next((m for m in cached if m["id"] == match_id), None)

            # Reject unknown match IDs first
            if m_espn is None:
                json_resp(self, 404, {"error": "Match not found"}); return

            # Reject if match has started or finished
            if m_espn["statusState"] in ("in", "post"):
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

        elif path == "/api/predict/ou":
            user, uname = auth_user(self)
            if not user:
                json_resp(self, 401, {"error": "Unauthorized"}); return
            match_id   = str(body.get("match_id", "")).strip()
            prediction = body.get("prediction")
            if prediction not in ("over", "under"):
                json_resp(self, 400, {"error": "prediction must be over or under"}); return
            if not match_id:
                json_resp(self, 400, {"error": "match_id required"}); return

            cached = _espn_cache.get("data") or fetch_espn() or []
            m_espn = next((m for m in cached if m["id"] == match_id), None)
            if m_espn is None:
                json_resp(self, 404, {"error": "Match not found"}); return
            if m_espn.get("ouLine") is None:
                json_resp(self, 400, {"error": "No O/U line available for this match"}); return
            if m_espn["statusState"] in ("in", "post"):
                json_resp(self, 400, {"error": "Match has already started"}); return

            ou_ov = db_get_ou_override(match_id)
            if ou_ov.get("locked") or ou_ov.get("result"):
                json_resp(self, 400, {"error": "O/U is locked for this match"}); return

            existing = db_get_ou_predictions(uname).get(match_id)
            if existing:
                db_set_ou_prediction(uname, match_id, prediction)
                json_resp(self, 200, {"message": "O/U prediction updated", "tokens": user.get("tokens", 0)}); return

            if user.get("tokens", 0) < BET_COST:
                json_resp(self, 400, {"error": "Not enough tokens"}); return

            user["tokens"]     -= BET_COST
            user["predictions"] = user.get("predictions", 0) + 1
            db_set_user(uname, user)
            db_set_ou_prediction(uname, match_id, prediction)
            json_resp(self, 200, {"message": "O/U prediction placed", "tokens": user["tokens"]})

        elif path == "/api/admin/result":
            if not check_admin(body):
                json_resp(self, 403, {"error": "Forbidden"}); return
            match_id = str(body.get("match_id", "")).strip()
            result   = body.get("result")
            if result not in ("home", "away", "draw"):
                json_resp(self, 400, {"error": "result must be home, away, or draw"}); return
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
    """Handle each request in its own thread — prevents ESPN fetch from blocking health checks."""
    daemon_threads = True


if __name__ == "__main__":
    # Warm-up DB connection and ESPN cache in background
    threading.Thread(target=get_db, daemon=True).start()
    threading.Thread(target=fetch_espn, daemon=True).start()

    server = ThreadedHTTPServer(("0.0.0.0", PORT), Handler)
    print("=" * 52)
    print("  World Cup 2026 Predictor")
    print("  http://localhost:{}".format(PORT))
    print("  Admin: http://localhost:{}/admin".format(PORT))
    print("  Storage: {}".format("MongoDB Atlas" if (MONGO_URI and _HAS_PYMONGO) else "Local JSON files"))
    print("=" * 52)
    server.serve_forever()

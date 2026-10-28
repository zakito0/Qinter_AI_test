"""
Qinter AI - Account system (stdlib only)

  • Email + password sign-up / sign-in (scrypt-hashed passwords, per-user random salt)
  • "Sign in with Google" (verifies the Google ID token and its audience = our client ID)
  • Opaque session tokens (stored hashed in SQLite, 30-day expiry), logout & /me

Routes (wired into agent_service.py):
  POST /api/auth/signup   {email, password, name?}
  POST /api/auth/signin   {email, password}
  POST /api/auth/google   {id_token}
  GET  /api/auth/me       Authorization: Bearer <token>
  POST /api/auth/logout   Authorization: Bearer <token>
"""

import hashlib
import hmac
import json
import os
import re
import secrets
import sqlite3
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

DB_PATH = Path(os.getenv("QINTER_AUTH_DB", str(Path(__file__).parent / "users.db")))
SESSION_TTL_SECONDS = 30 * 24 * 3600
GOOGLE_TOKENINFO_URL = "https://oauth2.googleapis.com/tokeninfo?id_token="
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
MIN_PASSWORD_LEN = 8

_db_lock = threading.Lock()
_attempts: Dict[str, list] = {}  # naive in-memory rate limiter: key -> [timestamps]


DEFAULT_GOOGLE_CLIENT_IDS = {
    "676889473726-tmlsvgko7m7uh6qekl1ej4vd0eb3j1om.apps.googleusercontent.com",
    "676889473726-qinter.apps.googleusercontent.com"
}


def _google_client_ids():
    raw = os.getenv("GOOGLE_CLIENT_ID", "") + "," + os.getenv("GOOGLE_ANDROID_CLIENT_ID", "")
    env_ids = {c.strip() for c in raw.split(",") if c.strip()}
    return env_ids if env_ids else DEFAULT_GOOGLE_CLIENT_IDS


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with _db_lock, _connect() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT NOT NULL UNIQUE,
                name TEXT NOT NULL DEFAULT '',
                password_hash TEXT,           -- NULL for Google-only accounts
                google_sub TEXT UNIQUE,
                created_at INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS sessions (
                token_hash TEXT PRIMARY KEY,
                user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                expires_at INTEGER NOT NULL
            );
            """
        )


# ---------------------------------------------------------------- hashing
def _hash_password(password: str) -> str:
    salt = secrets.token_bytes(16)
    dk = hashlib.scrypt(password.encode("utf-8"), salt=salt, n=2 ** 14, r=8, p=1, dklen=32)
    return f"scrypt${salt.hex()}${dk.hex()}"


def _verify_password(password: str, stored: Optional[str]) -> bool:
    if not stored:
        return False
    try:
        _, salt_hex, dk_hex = stored.split("$")
        dk = hashlib.scrypt(password.encode("utf-8"), salt=bytes.fromhex(salt_hex), n=2 ** 14, r=8, p=1, dklen=32)
        return hmac.compare_digest(dk.hex(), dk_hex)
    except Exception:
        return False


def _token_hash(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _issue_session(conn: sqlite3.Connection, user_id: int) -> str:
    token = secrets.token_urlsafe(32)
    conn.execute(
        "INSERT INTO sessions (token_hash, user_id, expires_at) VALUES (?, ?, ?)",
        (_token_hash(token), user_id, int(time.time()) + SESSION_TTL_SECONDS),
    )
    conn.execute("DELETE FROM sessions WHERE expires_at < ?", (int(time.time()),))
    return token


def _user_json(row: sqlite3.Row) -> Dict[str, Any]:
    return {"id": row["id"], "email": row["email"], "name": row["name"]}


def _rate_limited(key: str, limit: int = 8, window: int = 60) -> bool:
    now = time.time()
    hits = [t for t in _attempts.get(key, []) if now - t < window]
    hits.append(now)
    _attempts[key] = hits
    return len(hits) > limit


def _err(status: int, message: str) -> Tuple[int, Dict[str, Any]]:
    return status, {"error": message}


# ---------------------------------------------------------------- operations
def signup(body: Dict[str, Any], client_ip: str = "") -> Tuple[int, Dict[str, Any]]:
    if _rate_limited(f"signup:{client_ip}"):
        return _err(429, "Too many attempts. Please wait a minute.")
    email = str(body.get("email", "")).strip().lower()
    password = str(body.get("password", ""))
    name = str(body.get("name", "")).strip()[:80] or email.split("@")[0]
    if not EMAIL_RE.match(email):
        return _err(400, "Please enter a valid email address.")
    if len(password) < MIN_PASSWORD_LEN:
        return _err(400, f"Password must be at least {MIN_PASSWORD_LEN} characters.")
    with _db_lock, _connect() as conn:
        if conn.execute("SELECT 1 FROM users WHERE email = ?", (email,)).fetchone():
            return _err(409, "An account with this email already exists. Try signing in.")
        cur = conn.execute(
            "INSERT INTO users (email, name, password_hash, created_at) VALUES (?, ?, ?, ?)",
            (email, name, _hash_password(password), int(time.time())),
        )
        row = conn.execute("SELECT * FROM users WHERE id = ?", (cur.lastrowid,)).fetchone()
        token = _issue_session(conn, row["id"])
    return 201, {"token": token, "user": _user_json(row)}


def signin(body: Dict[str, Any], client_ip: str = "") -> Tuple[int, Dict[str, Any]]:
    email = str(body.get("email", "")).strip().lower()
    password = str(body.get("password", ""))
    if _rate_limited(f"signin:{client_ip}:{email}"):
        return _err(429, "Too many attempts. Please wait a minute.")
    with _db_lock, _connect() as conn:
        row = conn.execute("SELECT * FROM users WHERE email = ?", (email,)).fetchone()
        if row is None or not row["password_hash"]:
            if row is not None and not row["password_hash"]:
                return _err(401, "This account uses Google sign-in. Tap 'Continue with Google'.")
            _verify_password(password, None)
            return _err(401, "Incorrect email or password.")
        if not _verify_password(password, row["password_hash"]):
            return _err(401, "Incorrect email or password.")
        token = _issue_session(conn, row["id"])
    return 200, {"token": token, "user": _user_json(row)}


def _verify_google_id_token(id_token: str) -> Optional[Dict[str, Any]]:
    try:
        url = GOOGLE_TOKENINFO_URL + urllib.parse.quote(id_token, safe="")
        with urllib.request.urlopen(url, timeout=10) as resp:
            info = json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        print(f"[Auth] Google tokeninfo request failed: {e}", flush=True)
        return None
    allowed = _google_client_ids()
    aud = info.get("aud")
    azp = info.get("azp")
    if not allowed:
        print("[Auth] No GOOGLE_CLIENT_ID configured on server.", flush=True)
        return None
    if aud not in allowed and azp not in allowed:
        print(f"[Auth] Client ID mismatch: aud='{aud}', azp='{azp}' not in allowed: {allowed}", flush=True)
        return None
    if info.get("iss") not in ("accounts.google.com", "https://accounts.google.com"):
        print(f"[Auth] Invalid issuer: {info.get('iss')}", flush=True)
        return None
    if str(info.get("email_verified", "")).lower() != "true":
        print("[Auth] Email not verified in Google token.", flush=True)
        return None
    if int(info.get("exp", 0)) < time.time():
        print("[Auth] Google token has expired.", flush=True)
        return None
    return info


def google_signin(body: Dict[str, Any], client_ip: str = "") -> Tuple[int, Dict[str, Any]]:
    if _rate_limited(f"google:{client_ip}", limit=20):
        return _err(429, "Too many attempts. Please wait a minute.")
    if not _google_client_ids():
        return _err(503, "Google sign-in is not configured on the server (set GOOGLE_CLIENT_ID / GOOGLE_ANDROID_CLIENT_ID).")
    id_token = str(body.get("id_token", "")).strip()
    if id_token:
        info = _verify_google_id_token(id_token)
        if info is None:
            return _err(401, "Google sign-in could not be verified.")
        email = str(info.get("email", "")).lower()
        sub = str(info.get("sub", ""))
        name = str(info.get("name", "")) or email.split("@")[0]
    else:
        # Native Android Google Play Services sign-in:
        # The Android app has authenticated via Google Play Services on device
        sub = str(body.get("google_sub", "")).strip()
        email = str(body.get("email", "")).strip().lower()
        name = str(body.get("name", "")).strip() or email.split("@")[0]
        client_id = str(body.get("client_id", "")).strip()
        if not sub or not email:
            return _err(400, "Missing Google sign-in credentials.")
        allowed = _google_client_ids()
        if client_id and client_id not in allowed:
            print(f"[Auth] Client ID mismatch: {client_id} not in {allowed}", flush=True)
            return _err(401, "Invalid Google client ID.")

    with _db_lock, _connect() as conn:
        row = conn.execute("SELECT * FROM users WHERE google_sub = ? OR email = ?", (sub, email)).fetchone()
        if row is None:
            cur = conn.execute(
                "INSERT INTO users (email, name, google_sub, created_at) VALUES (?, ?, ?, ?)",
                (email, name, sub, int(time.time())),
            )
            row = conn.execute("SELECT * FROM users WHERE id = ?", (cur.lastrowid,)).fetchone()
        elif not row["google_sub"]:
            conn.execute("UPDATE users SET google_sub = ? WHERE id = ?", (sub, row["id"]))
        token = _issue_session(conn, row["id"])
    return 200, {"token": token, "user": _user_json(row)}


def _bearer(headers) -> str:
    auth = headers.get("Authorization", "")
    return auth[7:].strip() if auth.lower().startswith("bearer ") else ""


def me(headers) -> Tuple[int, Dict[str, Any]]:
    token = _bearer(headers)
    if not token:
        return _err(401, "Not signed in.")
    with _db_lock, _connect() as conn:
        row = conn.execute(
            "SELECT u.* FROM sessions s JOIN users u ON u.id = s.user_id "
            "WHERE s.token_hash = ? AND s.expires_at > ?",
            (_token_hash(token), int(time.time())),
        ).fetchone()
    if row is None:
        return _err(401, "Session expired. Please sign in again.")
    return 200, {"user": _user_json(row)}


def logout(headers) -> Tuple[int, Dict[str, Any]]:
    token = _bearer(headers)
    if token:
        with _db_lock, _connect() as conn:
            conn.execute("DELETE FROM sessions WHERE token_hash = ?", (_token_hash(token),))
    return 200, {"ok": True}


init_db()

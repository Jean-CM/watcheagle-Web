import os
import json
import requests
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict, Any

DATABASE_URL = os.environ.get("DATABASE_URL")
LASTFM_API_KEY = os.environ.get("LASTFM_API_KEY")

WARN_MIN = int(os.environ.get("WARN_MIN", "15"))
INCIDENT_MIN = int(os.environ.get("INCIDENT_MIN", "20"))

COLLECTOR_LIMIT = int(os.environ.get("COLLECTOR_LIMIT", "100"))
COLLECTOR_MAX_PAGES = int(os.environ.get("COLLECTOR_MAX_PAGES", "4"))

BACKFILL_LIMIT = int(os.environ.get("BACKFILL_LIMIT", "200"))
BACKFILL_MAX_PAGES = int(os.environ.get("BACKFILL_MAX_PAGES", "10"))

REFRESH_LIMIT = int(os.environ.get("REFRESH_LIMIT", "100"))
REFRESH_MAX_PAGES = int(os.environ.get("REFRESH_MAX_PAGES", "3"))


def get_conn():
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)


def utc_now():
    return datetime.now(timezone.utc)


def to_naive_utc(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


def parse_unix_to_utc_naive(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromtimestamp(int(value), tz=timezone.utc).replace(tzinfo=None)
    except Exception:
        return None


def init_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS teams (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        app_name VARCHAR(50) NOT NULL,
        lastfm_user VARCHAR(100) NOT NULL UNIQUE,
        country_code VARCHAR(10),
        status VARCHAR(20) DEFAULT 'PENDING',
        last_scrobble_at TIMESTAMP NULL,
        last_check_at TIMESTAMP NULL,
        idle_minutes INTEGER DEFAULT 0,
        active BOOLEAN DEFAULT TRUE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS scrobbles (
        id SERIAL PRIMARY KEY,
        team_id INTEGER REFERENCES teams(id) ON DELETE CASCADE,
        team_name VARCHAR(100),
        lastfm_user VARCHAR(100) NOT NULL,
        app_name VARCHAR(50),
        country_code VARCHAR(10),
        artist VARCHAR(255) NOT NULL,
        track VARCHAR(255) NOT NULL,
        album VARCHAR(255),
        scrobbled_at TIMESTAMP NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS job_runs (
        id SERIAL PRIMARY KEY,
        job_name VARCHAR(100) NOT NULL,
        status VARCHAR(20) DEFAULT 'RUNNING',
        message TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        finished_at TIMESTAMP NULL
    );
    """)

    cur.execute("ALTER TABLE teams ADD COLUMN IF NOT EXISTS country_code VARCHAR(10);")
    cur.execute("ALTER TABLE scrobbles ADD COLUMN IF NOT EXISTS country_code VARCHAR(10);")
    cur.execute("ALTER TABLE teams ADD COLUMN IF NOT EXISTS active BOOLEAN DEFAULT TRUE;")
    cur.execute("ALTER TABLE teams ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;")

    cur.execute("""
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_indexes WHERE indexname = 'idx_scrobbles_unique'
        ) THEN
            CREATE UNIQUE INDEX idx_scrobbles_unique
            ON scrobbles (lastfm_user, artist, track, scrobbled_at);
        END IF;
    END $$;
    """)

    conn.commit()
    cur.close()
    conn.close()


def start_job(job_name: str, message: str = "") -> int:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO job_runs (job_name, status, message)
        VALUES (%s, 'RUNNING', %s)
        RETURNING id
    """, (job_name, message))
    job_id = cur.fetchone()["id"]
    conn.commit()
    cur.close()
    conn.close()
    return job_id


def finish_job(job_id: int, status: str, message: str = ""):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        UPDATE job_runs
        SET status = %s,
            message = %s,
            finished_at = CURRENT_TIMESTAMP
        WHERE id = %s
    """, (status, message[:20000], job_id))
    conn.commit()
    cur.close()
    conn.close()


def log_job(job_name: str, status: str, message: str = ""):
    job_id = start_job(job_name, "")
    finish_job(job_id, status, message)


def get_last_job(job_name: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT *
        FROM job_runs
        WHERE job_name = %s
        ORDER BY created_at DESC
        LIMIT 1
    """, (job_name,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row


def lastfm_user_exists(username: str) -> bool:
    if not username or not LASTFM_API_KEY:
        return False

    url = "https://ws.audioscrobbler.com/2.0/"

    try:
        r = requests.get(
            url,
            params={
                "method": "user.getinfo",
                "user": username,
                "api_key": LASTFM_API_KEY,
                "format": "json"
            },
            timeout=15
        )
        data = r.json()
        if "user" in data:
            return True
    except Exception:
        pass

    try:
        r = requests.get(
            url,
            params={
                "method": "user.getrecenttracks",
                "user": username,
                "api_key": LASTFM_API_KEY,
                "format": "json",
                "limit": 1
            },
            timeout=15
        )
        data = r.json()
        if "recenttracks" in data:
            return True
    except Exception:
        pass

    return False


def fetch_recent_tracks(user: str, limit: int = 50, page: int = 1) -> Dict[str, Any]:
    url = "https://ws.audioscrobbler.com/2.0/"
    r = requests.get(
        url,
        params={
            "method": "user.getrecenttracks",
            "user": user,
            "api_key": LASTFM_API_KEY,
            "format": "json",
            "limit": limit,
            "page": page
        },
        timeout=30
    )
    data = r.json()
    return data


def normalize_tracks_payload(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    if not data or "recenttracks" not in data:
        return []

    rt = data.get("recenttracks", {})
    tracks = rt.get("track", [])

    if isinstance(tracks, dict):
        tracks = [tracks]

    result = []
    for t in tracks:
        artist = ""
        track = ""
        album = ""

        if isinstance(t.get("artist"), dict):
            artist = t["artist"].get("#text", "") or ""
        else:
            artist = str(t.get("artist", "") or "")

        track = str(t.get("name", "") or "")

        if isinstance(t.get("album"), dict):
            album = t["album"].get("#text", "") or ""
        else:
            album = str(t.get("album", "") or "")

        date_info = t.get("date")
        scrobbled_at = None
        if isinstance(date_info, dict):
            scrobbled_at = parse_unix_to_utc_naive(date_info.get("uts"))

        now_playing = False
        attrs = t.get("@attr", {})
        if isinstance(attrs, dict) and attrs.get("nowplaying") == "true":
            now_playing = True

        result.append({
            "artist": artist.strip(),
            "track": track.strip(),
            "album": album.strip(),
            "scrobbled_at": scrobbled_at,
            "now_playing": now_playing
        })

    return result


def insert_scrobble(team: Dict[str, Any], item: Dict[str, Any]) -> bool:
    if not item.get("artist") or not item.get("track") or not item.get("scrobbled_at"):
        return False

    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO scrobbles (
                team_id, team_name, lastfm_user, app_name, country_code,
                artist, track, album, scrobbled_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
            RETURNING id
        """, (
            team["id"],
            team["name"],
            team["lastfm_user"],
            team["app_name"],
            team.get("country_code"),
            item["artist"],
            item["track"],
            item.get("album"),
            item["scrobbled_at"]
        ))
        row = cur.fetchone()
        conn.commit()
        inserted = bool(row)
    finally:
        cur.close()
        conn.close()
    return inserted


def get_active_teams() -> List[Dict[str, Any]]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT *
        FROM teams
        WHERE active = TRUE
        ORDER BY id ASC
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def get_new_user_teams() -> List[Dict[str, Any]]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT t.*
        FROM teams t
        LEFT JOIN scrobbles s ON s.team_id = t.id
        WHERE t.active = TRUE
        GROUP BY t.id
        HAVING COUNT(s.id) = 0
        ORDER BY t.id ASC
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def get_latest_scrobble_for_user(lastfm_user: str) -> Optional[datetime]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT MAX(scrobbled_at) AS max_scrobble
        FROM scrobbles
        WHERE lastfm_user = %s
    """, (lastfm_user,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row["max_scrobble"] if row else None


def update_team_status(team_id: int, last_scrobble_at: Optional[datetime], idle_minutes: int, status: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        UPDATE teams
        SET last_scrobble_at = %s,
            last_check_at = CURRENT_TIMESTAMP,
            idle_minutes = %s,
            status = %s,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = %s
    """, (last_scrobble_at, idle_minutes, status, team_id))
    conn.commit()
    cur.close()
    conn.close()


def get_status_from_idle(idle_minutes: Optional[int]) -> str:
    if idle_minutes is None:
        return "PENDING"
    if idle_minutes >= INCIDENT_MIN:
        return "INCIDENT"
    if idle_minutes >= WARN_MIN:
        return "WARN"
    return "OK"


def app_rate(app_name: str) -> float:
    name = (app_name or "").strip().lower()
    if name == "spotify":
        return 0.0035
    if name == "tidal":
        return 0.006
    return 0.0


def get_runtime_file_snapshot() -> Dict[str, Any]:
    return {
        "cwd": os.getcwd(),
        "files": sorted(os.listdir(".")),
        "backfill_new_users_exists": os.path.exists("backfill_new_users.py"),
        "refresh_last_24h_exists": os.path.exists("refresh_last_24h.py"),
        "backfill_scrobbles_exists": os.path.exists("backfill_scrobbles.py"),
    }

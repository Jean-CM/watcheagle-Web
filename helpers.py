import os
from datetime import datetime, timezone
import requests
import psycopg2
from psycopg2.extras import RealDictCursor

DATABASE_URL = os.environ.get("DATABASE_URL")
LASTFM_API_KEY = os.environ.get("LASTFM_API_KEY")

WARN_MIN = int(os.environ.get("WARN_MIN", 15))
INCIDENT_MIN = int(os.environ.get("INCIDENT_MIN", 20))

COLLECTOR_LIMIT = int(os.environ.get("COLLECTOR_LIMIT", 100))
COLLECTOR_MAX_PAGES = int(os.environ.get("COLLECTOR_MAX_PAGES", 4))

BACKFILL_LIMIT = int(os.environ.get("BACKFILL_LIMIT", 200))
BACKFILL_MAX_PAGES = int(os.environ.get("BACKFILL_MAX_PAGES", 10))

REFRESH_LIMIT = int(os.environ.get("REFRESH_LIMIT", 100))
REFRESH_MAX_PAGES = int(os.environ.get("REFRESH_MAX_PAGES", 3))


def get_conn():
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)


def utc_now():
    return datetime.now(timezone.utc)


def init_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS teams (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        app_name VARCHAR(50) NOT NULL,
        lastfm_user VARCHAR(100) NOT NULL UNIQUE,
        status VARCHAR(20) DEFAULT 'PENDING',
        last_scrobble_at TIMESTAMP NULL,
        last_check_at TIMESTAMP NULL,
        idle_minutes INTEGER DEFAULT 0,
        last_alert_at TIMESTAMP NULL,
        active BOOLEAN DEFAULT TRUE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS scrobbles (
        id SERIAL PRIMARY KEY,
        team_id INTEGER REFERENCES teams(id) ON DELETE CASCADE,
        team_name VARCHAR(100),
        lastfm_user VARCHAR(100),
        app_name VARCHAR(50),
        artist_name TEXT,
        track_name TEXT,
        album_name TEXT,
        scrobble_time TIMESTAMP,
        collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)

    cur.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS uq_scrobble_unique
    ON scrobbles(team_id, track_name, artist_name, scrobble_time);
    """)

    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_scrobbles_team_time
    ON scrobbles(team_id, scrobble_time DESC);
    """)

    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_scrobbles_artist_time
    ON scrobbles(artist_name, scrobble_time DESC);
    """)

    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_scrobbles_user_time
    ON scrobbles(lastfm_user, scrobble_time DESC);
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS job_runs (
        id SERIAL PRIMARY KEY,
        job_name TEXT,
        status TEXT,
        output TEXT,
        started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        finished_at TIMESTAMP NULL
    );
    """)

    conn.commit()
    cur.close()
    conn.close()


def get_active_teams():
    init_db()
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            id,
            name,
            app_name,
            lastfm_user,
            status,
            last_scrobble_at,
            last_check_at,
            idle_minutes,
            last_alert_at,
            active,
            created_at
        FROM teams
        WHERE active = TRUE
        ORDER BY id ASC
    """)

    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def get_new_user_teams():
    init_db()
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT t.*
        FROM teams t
        LEFT JOIN scrobbles s
            ON s.team_id = t.id
        WHERE t.active = TRUE
        GROUP BY
            t.id, t.name, t.app_name, t.lastfm_user, t.status,
            t.last_scrobble_at, t.last_check_at, t.idle_minutes,
            t.last_alert_at, t.active, t.created_at
        HAVING COUNT(s.id) = 0
        ORDER BY t.id ASC
    """)

    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def start_job(job_name: str, output: str = ""):
    init_db()
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO job_runs (job_name, status, output)
        VALUES (%s, %s, %s)
        RETURNING id
    """, (job_name, "RUNNING", output))

    row = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()

    return row["id"] if row else None


def finish_job(job_id: int, status: str, output: str):
    init_db()
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        UPDATE job_runs
        SET
            status = %s,
            output = %s,
            finished_at = CURRENT_TIMESTAMP
        WHERE id = %s
    """, (status, output, job_id))

    conn.commit()
    cur.close()
    conn.close()


def fetch_recent_tracks(lastfm_user: str, limit: int = 1, page: int = 1):
    url = "https://ws.audioscrobbler.com/2.0/"
    params = {
        "method": "user.getrecenttracks",
        "user": lastfm_user,
        "api_key": LASTFM_API_KEY,
        "format": "json",
        "limit": limit,
        "page": page,
    }

    response = requests.get(url, params=params, timeout=30)

    try:
        data = response.json()
    except Exception:
        return {
            "error": -1,
            "message": f"Respuesta no JSON. HTTP {response.status_code}: {response.text[:300]}"
        }

    if "error" in data:
        return data

    if response.status_code != 200:
        return {
            "error": response.status_code,
            "message": str(data)
        }

    return data


def normalize_tracks_payload(data):
    recenttracks = data.get("recenttracks", {})
    track = recenttracks.get("track", [])

    if isinstance(track, dict):
        track = [track]

    normalized = []

    for item in track:
        now_playing = item.get("@attr", {}).get("nowplaying") == "true"

        artist_name = ""
        artist_field = item.get("artist")
        if isinstance(artist_field, dict):
            artist_name = (artist_field.get("#text") or "").strip()
        elif isinstance(artist_field, str):
            artist_name = artist_field.strip()

        album_name = ""
        album_field = item.get("album")
        if isinstance(album_field, dict):
            album_name = (album_field.get("#text") or "").strip()
        elif isinstance(album_field, str):
            album_name = album_field.strip()

        track_name = (item.get("name") or "").strip()

        scrobbled_at = None
        if not now_playing:
            date_info = item.get("date", {})
            uts = date_info.get("uts")
            if uts:
                scrobbled_at = datetime.fromtimestamp(int(uts))

        normalized.append({
            "artist_name": artist_name,
            "track_name": track_name,
            "album_name": album_name,
            "now_playing": now_playing,
            "scrobbled_at": scrobbled_at,
        })

    return normalized


def get_status_from_idle(idle_minutes: int):
    if idle_minutes is None:
        return "PENDING"
    if idle_minutes >= INCIDENT_MIN:
        return "INCIDENT"
    if idle_minutes >= WARN_MIN:
        return "WARN"
    return "OK"


def update_team_status(team_id: int, last_scrobbled_at, idle_minutes: int, status: str):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        UPDATE teams
        SET
            status = %s,
            last_scrobble_at = %s,
            idle_minutes = %s,
            last_check_at = CURRENT_TIMESTAMP
        WHERE id = %s
    """, (status, last_scrobbled_at, idle_minutes, team_id))

    conn.commit()
    cur.close()
    conn.close()


def get_latest_scrobble_for_user(lastfm_user: str):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT scrobble_time
        FROM scrobbles
        WHERE lastfm_user = %s
        ORDER BY scrobble_time DESC
        LIMIT 1
    """, (lastfm_user,))

    row = cur.fetchone()
    cur.close()
    conn.close()

    return row["scrobble_time"] if row else None


def insert_scrobble(team, item):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO scrobbles (
            team_id,
            team_name,
            lastfm_user,
            app_name,
            artist_name,
            track_name,
            album_name,
            scrobble_time
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
        RETURNING id
    """, (
        team["id"],
        team["name"],
        team["lastfm_user"],
        team["app_name"],
        item["artist_name"],
        item["track_name"],
        item["album_name"],
        item["scrobbled_at"],
    ))

    row = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()

    return row is not None

import os
import psycopg2
from psycopg2.extras import RealDictCursor

DATABASE_URL = os.environ.get("DATABASE_URL")


def get_conn():
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)


def init_db():
    conn = get_conn()
    cur = conn.cursor()

    # =========================
    # TEAMS
    # =========================
    cur.execute("""
    CREATE TABLE IF NOT EXISTS teams (
        id SERIAL PRIMARY KEY,
        name VARCHAR(50) NOT NULL,
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

    # =========================
    # SCROBBLES
    # =========================
    cur.execute("""
    CREATE TABLE IF NOT EXISTS scrobbles (
        id SERIAL PRIMARY KEY,
        team_id INTEGER REFERENCES teams(id) ON DELETE CASCADE,
        team_name VARCHAR(50),
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

    # =========================
    # JOB RUNS
    # =========================
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

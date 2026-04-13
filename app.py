from flask import Flask, request, jsonify
import os
import psycopg2
from psycopg2.extras import RealDictCursor
import subprocess
import requests

app = Flask(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL")


# =========================
# CONEXIÓN DB
# =========================
def get_conn():
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)


# =========================
# INIT DB
# =========================
def init_db():
    conn = get_conn()
    cur = conn.cursor()

    # TEAMS
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

    # SCROBBLES
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

    # JOB RUNS (FIX ERROR finished_at)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS job_runs (
        id SERIAL PRIMARY KEY,
        job_name TEXT,
        status TEXT,
        output TEXT,
        started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)

    cur.execute("""
    ALTER TABLE job_runs
    ADD COLUMN IF NOT EXISTS finished_at TIMESTAMP NULL;
    """)

    conn.commit()
    cur.close()
    conn.close()


# =========================
# DASHBOARD
# =========================
@app.route("/")
def home():
    init_db()

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT id,name,app_name,lastfm_user,status,idle_minutes,last_scrobble_at,last_check_at
        FROM teams
        WHERE active = TRUE
        ORDER BY id ASC
    """)

    teams = cur.fetchall()

    cur.close()
    conn.close()

    rows = ""

    for t in teams:
        estado = t["status"] or "PENDING"

        color = ""
        if estado == "OK":
            color = "green"
        elif estado == "WARN":
            color = "orange"
        elif estado == "INCIDENT":
            color = "red"

        rows += f"""
        <tr>
            <td>{t['id']}</td>
            <td>{t['name']}</td>
            <td>{t['app_name']}</td>
            <td>{t['lastfm_user']}</td>
            <td style="color:{color}">{estado}</td>
            <td>{t['last_scrobble_at'] or '-'}</td>
            <td>{t['idle_minutes']}</td>
            <td>{t['last_check_at'] or '-'}</td>
        </tr>
        """

    return f"""
    <html>
    <body style="background:#071226;color:white;font-family:Arial;padding:20px">

    <h2>WatchEagle</h2>

    <p>Monitores: {len(teams)}</p>

    <p>
    <a href="/run-check">run-check</a> |
    <a href="/collect-now">collect</a> |
    <a href="/scrobbles-count">count</a> |
    <a href="/fix-job-runs">fix jobs</a>
    </p>

    <table border="1" width="100%" cellpadding="8">
    <tr>
    <th>ID</th><th>Equipo</th><th>App</th><th>User</th>
    <th>Status</th><th>Last</th><th>Idle</th><th>Check</th>
    </tr>
    {rows}
    </table>

    </body>
    </html>
    """


# =========================
# FIX JOB_RUNS
# =========================
@app.route("/fix-job-runs")
def fix_job_runs():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    ALTER TABLE job_runs
    ADD COLUMN IF NOT EXISTS finished_at TIMESTAMP NULL;
    """)

    conn.commit()
    cur.close()
    conn.close()

    return {"ok": True}


# =========================
# TEST
# =========================
@app.route("/ping")
def ping():
    return {"ok": True}


# =========================
# HEALTH
# =========================
@app.route("/health")
def health():
    return {"ok": True}


# =========================
# TEAMS
# =========================
@app.route("/seed-team")
def seed_team():
    name = request.args.get("name")
    app_name = request.args.get("app")
    user = request.args.get("user")

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    INSERT INTO teams(name,app_name,lastfm_user)
    VALUES(%s,%s,%s)
    ON CONFLICT DO NOTHING
    RETURNING id
    """, (name, app_name, user))

    row = cur.fetchone()

    conn.commit()
    cur.close()
    conn.close()

    return {"created": row}


# =========================
# MONITOR
# =========================
@app.route("/run-check")
def run_check():
    result = subprocess.run(
        ["python", "watch_scrobbles.py"],
        capture_output=True,
        text=True
    )
    return f"<pre>{result.stdout}</pre>"


# =========================
# COLLECTOR
# =========================
@app.route("/collect-now")
def collect_now():
    result = subprocess.run(
        ["python", "collect_scrobbles.py"],
        capture_output=True,
        text=True
    )
    return f"<pre>{result.stdout}</pre>"


# =========================
# COUNT
# =========================
@app.route("/scrobbles-count")
def scrobbles_count():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) total FROM scrobbles")
    row = cur.fetchone()

    cur.close()
    conn.close()

    return row


# =========================
# MAIN
# =========================
if __name__ == "__main__":
    init_db()
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)

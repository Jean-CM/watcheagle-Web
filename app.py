from flask import Flask, request, jsonify
import os
import psycopg2
from psycopg2.extras import RealDictCursor
import subprocess

app = Flask(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL")


def get_conn():
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)


def init_db():
    conn = get_conn()
    cur = conn.cursor()

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


@app.route("/")
def home():
    try:
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
                idle_minutes,
                last_scrobble_at,
                last_check_at
            FROM teams
            WHERE active = TRUE
            ORDER BY id ASC
        """)

        teams = cur.fetchall()
        cur.close()
        conn.close()

        rows = ""

        for t in teams:
            estado = t.get("status") or "PENDING"
            color = "white"

            if estado == "OK":
                color = "#22c55e"
            elif estado == "WARN":
                color = "#f59e0b"
            elif estado == "INCIDENT":
                color = "#ef4444"

            rows += f"""
            <tr>
                <td>{t.get('id', '-')}</td>
                <td>{t.get('name', '-')}</td>
                <td>{t.get('app_name', '-')}</td>
                <td>{t.get('lastfm_user', '-')}</td>
                <td style="color:{color};font-weight:bold">{estado}</td>
                <td>{t.get('last_scrobble_at') or '-'}</td>
                <td>{t.get('idle_minutes', 0)}</td>
                <td>{t.get('last_check_at') or '-'}</td>
            </tr>
            """

        return f"""
        <html>
        <body style="background:#071226;color:white;font-family:Arial;padding:20px">
            <h2>WatchEagle</h2>
            <p>Monitores: {len(teams)}</p>

            <p>
                <a href="/ping">ping</a> |
                <a href="/healthz">healthz</a> |
                <a href="/run-check">run-check</a> |
                <a href="/collect-now">collect</a> |
                <a href="/scrobbles-count">count</a> |
                <a href="/fix-job-runs">fix jobs</a>
            </p>

            <table border="1" width="100%" cellpadding="8" cellspacing="0">
                <tr>
                    <th>ID</th>
                    <th>Equipo</th>
                    <th>App</th>
                    <th>User</th>
                    <th>Status</th>
                    <th>Last scrobble</th>
                    <th>Idle</th>
                    <th>Last check</th>
                </tr>
                {rows}
            </table>
        </body>
        </html>
        """

    except Exception as e:
        return f"<pre>ERROR EN HOME:\\n{str(e)}</pre>", 500


@app.route("/ping")
def ping():
    return jsonify({"ok": True, "msg": "pong"})


@app.route("/health")
def health():
    return jsonify({"ok": True})


@app.route("/healthz")
def healthz():
    try:
        init_db()
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) AS total FROM teams")
        row = cur.fetchone()
        cur.close()
        conn.close()

        return jsonify({
            "ok": True,
            "database": "connected",
            "teams": row["total"]
        })
    except Exception as e:
        return jsonify({
            "ok": False,
            "error": str(e)
        }), 500


@app.route("/fix-job-runs")
def fix_job_runs():
    try:
        conn = get_conn()
        cur = conn.cursor()

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

        return jsonify({"ok": True, "message": "job_runs corregida"})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/seed-team")
def seed_team():
    try:
        init_db()

        name = request.args.get("name")
        app_name = request.args.get("app")
        user = request.args.get("user")

        if not name or not app_name or not user:
            return jsonify({
                "ok": False,
                "error": "Usa /seed-team?name=Equipo%2001&app=spotify&user=JeanCMP"
            }), 400

        conn = get_conn()
        cur = conn.cursor()

        cur.execute("""
        INSERT INTO teams(name, app_name, lastfm_user)
        VALUES(%s, %s, %s)
        ON CONFLICT (lastfm_user) DO NOTHING
        RETURNING id
        """, (name, app_name, user))

        row = cur.fetchone()

        conn.commit()
        cur.close()
        conn.close()

        return jsonify({"ok": True, "created": row})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/run-check")
def run_check():
    try:
        result = subprocess.run(
            ["python", "watch_scrobbles.py"],
            capture_output=True,
            text=True
        )
        return f"<pre>{result.stdout}\n{result.stderr}</pre>"
    except Exception as e:
        return f"<pre>{str(e)}</pre>", 500


@app.route("/collect-now")
def collect_now():
    try:
        result = subprocess.run(
            ["python", "collect_scrobbles.py"],
            capture_output=True,
            text=True
        )
        return f"<pre>{result.stdout}\n{result.stderr}</pre>"
    except Exception as e:
        return f"<pre>{str(e)}</pre>", 500


@app.route("/scrobbles-count")
def scrobbles_count():
    try:
        init_db()
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) AS total FROM scrobbles")
        row = cur.fetchone()
        cur.close()
        conn.close()

        return jsonify({"ok": True, "total": row["total"]})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


if __name__ == "__main__":
    init_db()
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)

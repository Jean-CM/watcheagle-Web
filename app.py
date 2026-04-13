from flask import Flask, request, jsonify
import os
import psycopg2
from psycopg2.extras import RealDictCursor
import subprocess
import requests

app = Flask(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL")


def get_conn():
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)


def init_db():
    conn = get_conn()
    cur = conn.cursor()

    # =========================
    # Tabla teams
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
    # Tabla scrobbles
    # =========================
    cur.execute("""
    CREATE TABLE IF NOT EXISTS scrobbles (
        id SERIAL PRIMARY KEY,
        team_id INTEGER REFERENCES teams(id) ON DELETE CASCADE,
        team_name VARCHAR(50) NOT NULL,
        lastfm_user VARCHAR(100) NOT NULL,
        app_name VARCHAR(50) NOT NULL,
        artist_name TEXT NOT NULL,
        track_name TEXT NOT NULL,
        album_name TEXT,
        scrobble_time TIMESTAMP NOT NULL,
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
    # Tabla job_runs
    # =========================
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
        estado = t["status"] or "PENDING"
        estado_class = ""

        if estado == "OK":
            estado_class = "ok"
        elif estado == "WARN":
            estado_class = "warn"
        elif estado == "INCIDENT":
            estado_class = "incident"

        rows += f"""
        <tr>
            <td>{t['id']}</td>
            <td>{t['name']}</td>
            <td>{t['app_name']}</td>
            <td>{t['lastfm_user']}</td>
            <td class="{estado_class}">{estado}</td>
            <td>{t['last_scrobble_at'] or '-'}</td>
            <td>{t['idle_minutes']}</td>
            <td>{t['last_check_at'] or '-'}</td>
        </tr>
        """

    html = f"""
    <html>
    <head>
        <title>WatchEagle</title>
        <style>
            body {{
                font-family: Arial, sans-serif;
                background: #071226;
                color: #e5e7eb;
                padding: 30px;
                margin: 0;
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
                margin-top: 20px;
            }}
            th, td {{
                padding: 12px;
                border-bottom: 1px solid #1f2937;
                text-align: left;
            }}
            th {{
                background: #1a2740;
            }}
            .ok {{
                color: #22c55e;
                font-weight: bold;
            }}
            .warn {{
                color: #f59e0b;
                font-weight: bold;
            }}
            .incident {{
                color: #ef4444;
                font-weight: bold;
            }}
            a {{
                color: #c084fc;
                text-decoration: none;
                margin-right: 10px;
            }}
            .card {{
                background: #0f1b33;
                padding: 20px;
                border-radius: 12px;
                margin-bottom: 20px;
            }}
        </style>
    </head>
    <body>
        <h1>WatchEagle</h1>

        <div class="card">
            <p><strong>Monitores activos:</strong> {len(teams)}</p>

            <p>
                <a href="/run-check">run monitor</a> |
                <a href="/collect-now">collect scrobbles</a> |
                <a href="/scrobbles-count">scrobbles count</a> |
                <a href="/scrobbles-latest">latest scrobbles</a> |
                <a href="/top-artists-today">top artists</a> |
                <a href="/artists-vs-yesterday">artists vs yesterday</a> |
                <a href="/daily-plays">daily plays</a> |
                <a href="/fix-job-runs">fix job runs</a>
            </p>
        </div>

        <table>
            <thead>
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
            </thead>
            <tbody>
                {rows}
            </tbody>
        </table>
    </body>
    </html>
    """

    return html


@app.route("/health")
def health():
    init_db()
    return jsonify({"ok": True, "service": "WatchEagle"})


@app.route("/fix-job-runs")
def fix_job_runs():
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


@app.route("/seed-team")
def seed_team():
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
        VALUES (%s, %s, %s)
        ON CONFLICT DO NOTHING
        RETURNING id, name, app_name, lastfm_user
    """, (name, app_name, user))

    row = cur.fetchone()

    conn.commit()
    cur.close()
    conn.close()

    return jsonify({"ok": True, "created": row})


@app.route("/delete-team")
def delete_team():
    team_id = request.args.get("id")

    if not team_id:
        return jsonify({"ok": False, "error": "Falta id"}), 400

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        DELETE FROM teams
        WHERE id = %s
        RETURNING id
    """, (team_id,))

    row = cur.fetchone()

    conn.commit()
    cur.close()
    conn.close()

    return jsonify({"ok": True, "deleted": row})


@app.route("/update-team")
def update_team():
    team_id = request.args.get("id")
    name = request.args.get("name")
    app_name = request.args.get("app")
    user = request.args.get("user")

    if not team_id or not name or not app_name or not user:
        return jsonify({
            "ok": False,
            "error": "Usa /update-team?id=1&name=Equipo%2001&app=spotify&user=JeanCMP"
        }), 400

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        UPDATE teams
        SET name = %s,
            app_name = %s,
            lastfm_user = %s
        WHERE id = %s
        RETURNING id, name, app_name, lastfm_user
    """, (name, app_name, user, team_id))

    row = cur.fetchone()

    conn.commit()
    cur.close()
    conn.close()

    if not row:
        return jsonify({"ok": False, "error": "Equipo no encontrado"}), 404

    return jsonify({"ok": True, "updated": row})


@app.route("/collect-now")
def collect_now():
    result = subprocess.run(["python", "collect_scrobbles.py"], capture_output=True, text=True)
    return f"<pre>{result.stdout}\n{result.stderr}</pre>"


@app.route("/run-check")
def run_check():
    result = subprocess.run(["python", "watch_scrobbles.py"], capture_output=True, text=True)
    return f"<pre>{result.stdout}\n{result.stderr}</pre>"


@app.route("/scrobbles-count")
def scrobbles_count():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) AS total FROM scrobbles")
    row = cur.fetchone()

    cur.close()
    conn.close()

    return jsonify({"ok": True, "total": row["total"]})


@app.route("/scrobbles-latest")
def scrobbles_latest():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            team_name,
            lastfm_user,
            app_name,
            artist_name,
            track_name,
            album_name,
            scrobble_time,
            collected_at
        FROM scrobbles
        ORDER BY scrobble_time DESC
        LIMIT 50
    """)

    rows = cur.fetchall()

    cur.close()
    conn.close()

    return jsonify({"ok": True, "rows": rows})


@app.route("/top-artists-today")
def top_artists_today():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            artist_name,
            COUNT(*) AS plays
        FROM scrobbles
        WHERE DATE(scrobble_time) = CURRENT_DATE
        GROUP BY artist_name
        ORDER BY plays DESC, artist_name ASC
        LIMIT 20
    """)

    rows = cur.fetchall()

    cur.close()
    conn.close()

    return jsonify({"ok": True, "rows": rows})


@app.route("/artists-vs-yesterday")
def artists_vs_yesterday():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            artist_name,
            SUM(CASE WHEN DATE(scrobble_time) = CURRENT_DATE THEN 1 ELSE 0 END) AS today,
            SUM(CASE WHEN DATE(scrobble_time) = CURRENT_DATE - INTERVAL '1 day' THEN 1 ELSE 0 END) AS yesterday
        FROM scrobbles
        GROUP BY artist_name
        ORDER BY today DESC, yesterday DESC
        LIMIT 30
    """)

    rows = cur.fetchall()

    cur.close()
    conn.close()

    return jsonify({"ok": True, "rows": rows})


@app.route("/daily-plays")
def daily_plays():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            DATE(scrobble_time) AS day,
            COUNT(*) AS plays
        FROM scrobbles
        GROUP BY day
        ORDER BY day DESC
        LIMIT 30
    """)

    rows = cur.fetchall()

    cur.close()
    conn.close()

    return jsonify({"ok": True, "rows": rows})


@app.route("/debug-lastfm")
def debug_lastfm():
    user = request.args.get("user")

    if not user:
        return jsonify({"ok": False, "error": "Falta user"}), 400

    url = "https://ws.audioscrobbler.com/2.0/"
    params = {
        "method": "user.getrecenttracks",
        "user": user,
        "api_key": os.environ.get("LASTFM_API_KEY"),
        "format": "json",
        "limit": 1
    }

    r = requests.get(url, params=params, timeout=30)

    return f"<pre>{r.text}</pre>"


if __name__ == "__main__":
    init_db()
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)

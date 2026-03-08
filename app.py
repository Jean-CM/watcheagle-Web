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

    # Tabla equipos
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

    # Tabla scrobbles
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

    conn.commit()
    cur.close()
    conn.close()


# ---------- DASHBOARD ----------

@app.route("/")
def home():

    init_db()

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT id, name, app_name, lastfm_user, status, idle_minutes, last_scrobble_at, last_check_at
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
    font-family: Arial;
    background:#071226;
    color:white;
    padding:30px;
    }}

    table {{
    width:100%;
    border-collapse:collapse;
    }}

    th,td {{
    padding:10px;
    border-bottom:1px solid #333;
    }}

    th {{
    background:#1a2740;
    }}

    .ok {{color:#22c55e;font-weight:bold}}
    .warn {{color:#f59e0b;font-weight:bold}}
    .incident {{color:#ef4444;font-weight:bold}}

    </style>
    </head>

    <body>

    <h1>WatchEagle</h1>

    <p>Monitores activos: {len(teams)}</p>

    <p>
    <a href="/run-check">run monitor</a> |
    <a href="/collect-now">collect scrobbles</a> |
    <a href="/scrobbles-count">scrobbles count</a> |
    <a href="/scrobbles-latest">latest scrobbles</a> |
    <a href="/top-artists-today">top artists</a> |
    <a href="/artists-vs-yesterday">artists vs yesterday</a>
    </p>

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


# ---------- SEED TEAM ----------

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
    """,(name,app_name,user))

    row = cur.fetchone()

    conn.commit()

    cur.close()
    conn.close()

    return jsonify({"created":row})


# ---------- DELETE TEAM ----------

@app.route("/delete-team")
def delete_team():

    id = request.args.get("id")

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    DELETE FROM teams WHERE id=%s
    """,(id,))

    conn.commit()

    cur.close()
    conn.close()

    return jsonify({"deleted":id})


# ---------- COLLECT SCROBBLES ----------

@app.route("/collect-now")
def collect_now():

    result = subprocess.run(["python","collect_scrobbles.py"],capture_output=True,text=True)

    return f"<pre>{result.stdout}</pre>"


# ---------- MONITOR ----------

@app.route("/run-check")
def run_check():

    result = subprocess.run(["python","watch_scrobbles.py"],capture_output=True,text=True)

    return f"<pre>{result.stdout}</pre>"


# ---------- COUNT ----------

@app.route("/scrobbles-count")
def scrobbles_count():

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) AS total FROM scrobbles")

    row = cur.fetchone()

    cur.close()
    conn.close()

    return jsonify(row)


# ---------- LATEST ----------

@app.route("/scrobbles-latest")
def scrobbles_latest():

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    SELECT team_name,artist_name,track_name,scrobble_time
    FROM scrobbles
    ORDER BY scrobble_time DESC
    LIMIT 50
    """)

    rows = cur.fetchall()

    cur.close()
    conn.close()

    return jsonify(rows)


# ---------- TOP ARTISTS ----------

@app.route("/top-artists-today")
def top_artists_today():

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    SELECT artist_name,COUNT(*) plays
    FROM scrobbles
    WHERE DATE(scrobble_time)=CURRENT_DATE
    GROUP BY artist_name
    ORDER BY plays DESC
    LIMIT 20
    """)

    rows = cur.fetchall()

    cur.close()
    conn.close()

    return jsonify(rows)


# ---------- VS YESTERDAY ----------

@app.route("/artists-vs-yesterday")
def artists_vs_yesterday():

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    SELECT
    artist_name,

    SUM(CASE WHEN DATE(scrobble_time)=CURRENT_DATE THEN 1 ELSE 0 END) today,

    SUM(CASE WHEN DATE(scrobble_time)=CURRENT_DATE-INTERVAL '1 day' THEN 1 ELSE 0 END) yesterday

    FROM scrobbles

    GROUP BY artist_name

    ORDER BY today DESC
    LIMIT 30
    """)

    rows = cur.fetchall()

    cur.close()
    conn.close()

    return jsonify(rows)


# ---------- DEBUG LASTFM ----------

@app.route("/debug-lastfm")
def debug_lastfm():

    user=request.args.get("user")

    url="https://ws.audioscrobbler.com/2.0/"

    params={
    "method":"user.getrecenttracks",
    "user":user,
    "api_key":os.environ.get("LASTFM_API_KEY"),
    "format":"json",
    "limit":1
    }

    r=requests.get(url,params=params)

    return f"<pre>{r.text}</pre>"


# ---------- HEALTH ----------

@app.route("/health")
def health():
    return {"ok":True}


if __name__ == "__main__":

    port=int(os.environ.get("PORT",10000))

    app.run(host="0.0.0.0",port=port)

@app.route("/daily-plays")
def daily_plays():

    conn=get_conn()
    cur=conn.cursor()

    cur.execute("""

    SELECT
    DATE(scrobble_time) day,
    COUNT(*) plays

    FROM scrobbles

    GROUP BY day
    ORDER BY day DESC
    LIMIT 30

    """)

    rows=cur.fetchall()

    cur.close()
    conn.close()

    return jsonify(rows)

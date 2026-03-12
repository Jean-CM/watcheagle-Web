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

    conn.commit()
    cur.close()
    conn.close()


@app.route("/")
def home():

    init_db()

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT id,name,app_name,lastfm_user,status,
        idle_minutes,last_scrobble_at,last_check_at
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
color:#e5e7eb;
padding:30px;
}}

table {{
width:100%;
border-collapse:collapse;
background:#0f1b33;
}}

th,td {{
padding:12px;
border-bottom:1px solid #1f2937;
}}

th {{
background:#1a2740;
}}

.ok {{color:#22c55e;font-weight:bold;}}
.warn {{color:#f59e0b;font-weight:bold;}}
.incident {{color:#ef4444;font-weight:bold;}}

button {{
padding:10px 16px;
border-radius:8px;
border:none;
font-weight:bold;
cursor:pointer;
}}

</style>

</head>

<body>

<div style="display:flex;justify-content:space-between;margin-bottom:20px">

<h1>WatchEagle</h1>

<div style="display:flex;gap:10px">

<button onclick="window.location.reload()" style="background:#2563eb;color:white">
Refrescar
</button>

<button onclick="window.location.href='/run-check'" style="background:#16a34a;color:white">
Correr chequeo
</button>

</div>

</div>

<p>Monitores activos: {len(teams)}</p>

<table>

<thead>
<tr>
<th>ID</th>
<th>Equipo</th>
<th>App</th>
<th>Usuario Last.fm</th>
<th>Estado</th>
<th>Último scrobble</th>
<th>Idle</th>
<th>Último check</th>
</tr>
</thead>

<tbody>

{rows if rows else '<tr><td colspan="8">No hay equipos cargados</td></tr>'}

</tbody>

</table>

</body>
</html>
"""

    return html


@app.route("/health")
def health():
    return {"ok": True}


@app.route("/seed-team")
def seed_team():

    name = request.args.get("name")
    app_name = request.args.get("app")
    user = request.args.get("user")

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    INSERT INTO teams (name,app_name,lastfm_user,status)
    VALUES (%s,%s,%s,'PENDING')
    ON CONFLICT (lastfm_user) DO NOTHING
    RETURNING id,name
    """, (name, app_name, user))

    row = cur.fetchone()

    conn.commit()
    cur.close()
    conn.close()

    return {"created": row}


@app.route("/update-team")
def update_team():

    id = request.args.get("id")
    name = request.args.get("name")
    app = request.args.get("app")
    user = request.args.get("user")

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    UPDATE teams
    SET name=%s,app_name=%s,lastfm_user=%s
    WHERE id=%s
    RETURNING id,name
    """, (name, app, user, id))

    row = cur.fetchone()

    conn.commit()
    cur.close()
    conn.close()

    return {"updated": row}


@app.route("/delete-team")
def delete_team():

    id = request.args.get("id")

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("DELETE FROM teams WHERE id=%s RETURNING id", (id,))
    row = cur.fetchone()

    conn.commit()
    cur.close()
    conn.close()

    return {"deleted": row}


@app.route("/delete-many")
def delete_many():

    ids = request.args.get("ids")

    id_list = [int(x) for x in ids.split(",")]

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("DELETE FROM teams WHERE id = ANY(%s)", (id_list,))

    conn.commit()
    cur.close()
    conn.close()

    return {"deleted_ids": id_list}


@app.route("/reset-teams")
def reset_teams():

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("DELETE FROM teams")

    conn.commit()
    cur.close()
    conn.close()

    return {"ok": True, "message": "All teams deleted"}


@app.route("/load-teams")
def load_teams():

    total = int(request.args.get("total", 10))
    prefix = request.args.get("prefix", "equipo")
    app = request.args.get("app", "spotify")

    conn = get_conn()
    cur = conn.cursor()

    created = []

    for i in range(1, total + 1):

        name = f"{prefix}{str(i).zfill(2)}"

        cur.execute("""
        INSERT INTO teams (name,app_name,lastfm_user,status)
        VALUES (%s,%s,%s,'PENDING')
        ON CONFLICT (lastfm_user) DO NOTHING
        RETURNING id,name
        """, (name, app, name))

        row = cur.fetchone()

        if row:
            created.append(row)

    conn.commit()
    cur.close()
    conn.close()

    return {"total_created": len(created)}


@app.route("/load-batch")
def load_batch():

    spotify = int(request.args.get("spotify", 0))
    tidal = int(request.args.get("tidal", 0))
    apple = int(request.args.get("apple", 0))

    conn = get_conn()
    cur = conn.cursor()

    created = []

    def create_group(total, prefix, app):

        for i in range(1, total + 1):

            name = f"{prefix}{str(i).zfill(2)}"

            cur.execute("""
            INSERT INTO teams (name,app_name,lastfm_user,status)
            VALUES (%s,%s,%s,'PENDING')
            ON CONFLICT (lastfm_user) DO NOTHING
            RETURNING id,name
            """, (name, app, name))

            row = cur.fetchone()

            if row:
                created.append(row)

    create_group(spotify, "equipoS", "spotify")
    create_group(tidal, "equipoT", "tidal")
    create_group(apple, "equipoA", "apple")

    conn.commit()
    cur.close()
    conn.close()

    return {
        "created": len(created),
        "spotify": spotify,
        "tidal": tidal,
        "apple": apple
    }


@app.route("/debug-lastfm")
def debug_lastfm():

    user = request.args.get("user")

    url = "https://ws.audioscrobbler.com/2.0/"

    params = {
        "method": "user.getrecenttracks",
        "user": user,
        "api_key": os.environ.get("LASTFM_API_KEY"),
        "format": "json",
        "limit": 1
    }

    r = requests.get(url, params=params)

    return r.text


@app.route("/run-check")
def run_check():

    result = subprocess.run(
        ["python", "watch_scrobbles.py"],
        capture_output=True,
        text=True
    )

    return f"<pre>{result.stdout}</pre>"


if __name__ == "__main__":
    init_db()
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)

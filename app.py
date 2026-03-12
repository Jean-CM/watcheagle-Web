from flask import Flask, request, jsonify, redirect
import os
import psycopg2
from psycopg2.extras import RealDictCursor
import subprocess
import requests

app = Flask(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL")
LASTFM_API_KEY = os.environ.get("LASTFM_API_KEY")


def get_conn():
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)


def lastfm_user_exists(username: str) -> bool:
    if not username or not LASTFM_API_KEY:
        return False

    url = "https://ws.audioscrobbler.com/2.0/"
    params = {
        "method": "user.getinfo",
        "user": username,
        "api_key": LASTFM_API_KEY,
        "format": "json"
    }

    try:
        r = requests.get(url, params=params, timeout=20)
        data = r.json()

        if "error" in data:
            return False

        return "user" in data
    except Exception:
        return False


def init_db():
    conn = get_conn()
    cur = conn.cursor()

    # Teams
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

    # Scrobbles
    cur.execute("""
    CREATE TABLE IF NOT EXISTS scrobbles (
        id SERIAL PRIMARY KEY,
        team_id INTEGER REFERENCES teams(id) ON DELETE CASCADE,
        team_name VARCHAR(100),
        lastfm_user VARCHAR(100),
        app_name VARCHAR(50),
        artist VARCHAR(255),
        track VARCHAR(255),
        album VARCHAR(255),
        scrobbled_at TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)

    # Safe migrations
    cur.execute("ALTER TABLE scrobbles ADD COLUMN IF NOT EXISTS team_id INTEGER;")
    cur.execute("ALTER TABLE scrobbles ADD COLUMN IF NOT EXISTS team_name VARCHAR(100);")
    cur.execute("ALTER TABLE scrobbles ADD COLUMN IF NOT EXISTS lastfm_user VARCHAR(100);")
    cur.execute("ALTER TABLE scrobbles ADD COLUMN IF NOT EXISTS app_name VARCHAR(50);")
    cur.execute("ALTER TABLE scrobbles ADD COLUMN IF NOT EXISTS artist VARCHAR(255);")
    cur.execute("ALTER TABLE scrobbles ADD COLUMN IF NOT EXISTS track VARCHAR(255);")
    cur.execute("ALTER TABLE scrobbles ADD COLUMN IF NOT EXISTS album VARCHAR(255);")
    cur.execute("ALTER TABLE scrobbles ADD COLUMN IF NOT EXISTS scrobbled_at TIMESTAMP;")
    cur.execute("ALTER TABLE scrobbles ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;")

    # Unique index for dedupe
    cur.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS idx_scrobbles_unique
    ON scrobbles (lastfm_user, artist, track, scrobbled_at);
    """)

    conn.commit()
    cur.close()
    conn.close()


def render_layout(title, body_html):
    return f"""
    <html>
    <head>
        <title>{title}</title>
        <style>
            body {{
                font-family: Arial, sans-serif;
                background:#071226;
                color:#e5e7eb;
                padding:30px;
                margin:0;
            }}
            .topbar {{
                display:flex;
                justify-content:space-between;
                align-items:center;
                margin-bottom:20px;
                gap:16px;
                flex-wrap:wrap;
            }}
            .nav {{
                display:flex;
                gap:10px;
                flex-wrap:wrap;
            }}
            .nav a, .btn {{
                background:#1a2740;
                color:white;
                text-decoration:none;
                padding:10px 16px;
                border-radius:8px;
                display:inline-block;
                border:none;
                cursor:pointer;
                font-weight:bold;
            }}
            .btn-green {{ background:#16a34a; }}
            .btn-blue {{ background:#2563eb; }}
            .btn-red {{ background:#dc2626; }}
            .btn-orange {{ background:#ea580c; }}
            .card {{
                background:#0f1b33;
                padding:20px;
                border-radius:12px;
                margin-bottom:20px;
            }}
            .grid {{
                display:grid;
                grid-template-columns:repeat(auto-fit, minmax(220px, 1fr));
                gap:16px;
                margin-bottom:20px;
            }}
            .kpi {{
                background:#0f1b33;
                padding:18px;
                border-radius:12px;
            }}
            .kpi .label {{
                color:#9ca3af;
                font-size:14px;
            }}
            .kpi .value {{
                font-size:28px;
                font-weight:bold;
                margin-top:8px;
            }}
            table {{
                width:100%;
                border-collapse:collapse;
                background:#0f1b33;
                border-radius:12px;
                overflow:hidden;
                margin-bottom:20px;
            }}
            th, td {{
                padding:12px;
                border-bottom:1px solid #1f2937;
                text-align:left;
            }}
            th {{
                background:#1a2740;
            }}
            .ok {{ color:#22c55e; font-weight:bold; }}
            .warn {{ color:#f59e0b; font-weight:bold; }}
            .incident {{ color:#ef4444; font-weight:bold; }}
            .hint {{
                color:#9ca3af;
                font-size:14px;
                margin-top:8px;
            }}
            code {{
                background:#111827;
                padding:2px 6px;
                border-radius:6px;
            }}
            input, textarea, select {{
                padding:10px;
                border-radius:8px;
                border:1px solid #334155;
                background:#0b1220;
                color:white;
                width:100%;
                box-sizing:border-box;
            }}
            textarea {{
                min-height:160px;
            }}
            .inline-form {{
                display:flex;
                gap:10px;
                flex-wrap:wrap;
                align-items:center;
                margin-top:12px;
            }}
            .two-col {{
                display:grid;
                grid-template-columns:1fr 1fr;
                gap:16px;
            }}
            @media (max-width: 900px) {{
                .two-col {{ grid-template-columns:1fr; }}
            }}
        </style>
        <script>
            function deleteById() {{
                const id = document.getElementById('deleteId').value;
                if (!id) {{
                    alert('Pon un ID');
                    return;
                }}
                if (confirm('¿Seguro que quieres borrar el equipo con ID ' + id + '?')) {{
                    window.location.href = '/delete-team?id=' + id;
                }}
            }}

            function resetAll() {{
                if (confirm('¿Seguro que quieres borrar TODOS los equipos, TODOS los scrobbles y reiniciar los IDs?')) {{
                    window.location.href = '/reset-teams';
                }}
            }}
        </script>
    </head>
    <body>
        <div class="topbar">
            <h1 style="margin:0;">{title}</h1>
            <div class="nav">
                <a href="/">Monitor</a>
                <a href="/analytics">Analytics</a>
                <button class="btn btn-blue" onclick="window.location.reload()">Refrescar</button>
                <a class="btn btn-green" href="/run-check">Correr chequeo</a>
                <a class="btn btn-green" href="/run-collector">Correr collector</a>
                <button class="btn btn-red" onclick="resetAll()">Borrar todo</button>
            </div>
        </div>
        {body_html}
    </body>
    </html>
    """


@app.route("/")
def home():
    init_db()

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT id, name, app_name, lastfm_user, status,
               idle_minutes, last_scrobble_at, last_check_at
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

    body = f"""
    <div class="card">
        <p><strong>Monitores activos:</strong> {len(teams)}</p>
        <p class="hint">Borrar todo + reiniciar IDs: <code>/reset-teams</code></p>
        <p class="hint">Eliminar 1 equipo: <code>/delete-team?id=7</code></p>
        <p class="hint">Carga simple: <code>/load-teams?total=100&prefix=equipo&app=spotify</code></p>
        <p class="hint">Carga por bloques: <code>/load-batch?spotify=40&tidal=30&apple=30</code></p>
    </div>

    <div class="two-col">
        <div class="card">
            <h2>Eliminar por ID</h2>
            <div class="inline-form">
                <input id="deleteId" type="number" placeholder="ID a borrar">
                <button class="btn btn-orange" onclick="deleteById()">Eliminar por ID</button>
            </div>
        </div>

        <div class="card">
            <h2>Importar equipos reales de Last.fm</h2>
            <p class="hint">Formato: <code>Nombre visible,app,usuario_real_lastfm</code></p>
            <p class="hint">Ejemplo:<br>
            <code>Equipo 01,spotify,JeanCMP</code><br>
            <code>equipoG01,spotify,equipoG01</code></p>

            <form method="POST" action="/import-real-teams">
                <textarea name="lines" placeholder="Equipo 01,spotify,JeanCMP&#10;equipoG01,spotify,equipoG01"></textarea>
                <div class="inline-form">
                    <button class="btn btn-green" type="submit">Importar solo usuarios existentes</button>
                </div>
            </form>
        </div>
    </div>

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
    """
    return render_layout("WatchEagle Monitor", body)


@app.route("/analytics")
def analytics():
    init_db()
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) AS c FROM scrobbles WHERE DATE(scrobbled_at) = CURRENT_DATE")
    plays_today = cur.fetchone()["c"]

    cur.execute("SELECT COUNT(*) AS c FROM scrobbles WHERE DATE(scrobbled_at) = CURRENT_DATE - INTERVAL '1 day'")
    plays_yesterday = cur.fetchone()["c"]

    diff = plays_today - plays_yesterday

    cur.execute("""
        SELECT COALESCE(artist, '-') AS artist, COUNT(*) AS plays
        FROM scrobbles
        WHERE DATE(scrobbled_at) = CURRENT_DATE
        GROUP BY artist
        ORDER BY plays DESC
        LIMIT 10
    """)
    top_artists = cur.fetchall()

    cur.execute("""
        SELECT COALESCE(track, '-') AS track, COALESCE(artist, '-') AS artist, COUNT(*) AS plays
        FROM scrobbles
        WHERE DATE(scrobbled_at) = CURRENT_DATE
        GROUP BY track, artist
        ORDER BY plays DESC
        LIMIT 10
    """)
    top_tracks = cur.fetchall()

    cur.execute("""
        SELECT COALESCE(team_name, '-') AS team_name, COUNT(*) AS plays
        FROM scrobbles
        WHERE DATE(scrobbled_at) = CURRENT_DATE
        GROUP BY team_name
        ORDER BY plays DESC
        LIMIT 15
    """)
    plays_by_team = cur.fetchall()

    cur.execute("""
        SELECT COALESCE(app_name, '-') AS app_name, COUNT(*) AS plays
        FROM scrobbles
        WHERE DATE(scrobbled_at) = CURRENT_DATE
        GROUP BY app_name
        ORDER BY plays DESC
    """)
    plays_by_app = cur.fetchall()

    cur.execute("""
        SELECT
            COALESCE(artist, '-') AS artist,
            SUM(CASE WHEN DATE(scrobbled_at) = CURRENT_DATE THEN 1 ELSE 0 END) AS hoy,
            SUM(CASE WHEN DATE(scrobbled_at) = CURRENT_DATE - INTERVAL '1 day' THEN 1 ELSE 0 END) AS ayer
        FROM scrobbles
        GROUP BY artist
        ORDER BY hoy DESC, ayer DESC
        LIMIT 15
    """)
    compare_artists = cur.fetchall()

    cur.close()
    conn.close()

    def rows_simple(items, cols):
        html = ""
        for item in items:
            html += "<tr>" + "".join(f"<td>{item[col] or '-'}</td>" for col in cols) + "</tr>"
        return html

    body = f"""
    <div class="grid">
        <div class="kpi">
            <div class="label">Plays hoy</div>
            <div class="value">{plays_today}</div>
        </div>
        <div class="kpi">
            <div class="label">Plays ayer</div>
            <div class="value">{plays_yesterday}</div>
        </div>
        <div class="kpi">
            <div class="label">Variación vs ayer</div>
            <div class="value">{diff}</div>
        </div>
    </div>

    <div class="card">
        <h2>Top artistas hoy</h2>
        <table>
            <thead><tr><th>Artista</th><th>Plays</th></tr></thead>
            <tbody>{rows_simple(top_artists, ['artist', 'plays']) if top_artists else '<tr><td colspan="2">Sin datos</td></tr>'}</tbody>
        </table>
    </div>

    <div class="card">
        <h2>Top canciones hoy</h2>
        <table>
            <thead><tr><th>Canción</th><th>Artista</th><th>Plays</th></tr></thead>
            <tbody>{rows_simple(top_tracks, ['track', 'artist', 'plays']) if top_tracks else '<tr><td colspan="3">Sin datos</td></tr>'}</tbody>
        </table>
    </div>

    <div class="card">
        <h2>Comparativa artistas: hoy vs ayer</h2>
        <table>
            <thead><tr><th>Artista</th><th>Hoy</th><th>Ayer</th></tr></thead>
            <tbody>{rows_simple(compare_artists, ['artist', 'hoy', 'ayer']) if compare_artists else '<tr><td colspan="3">Sin datos</td></tr>'}</tbody>
        </table>
    </div>

    <div class="card">
        <h2>Plays por equipo hoy</h2>
        <table>
            <thead><tr><th>Equipo</th><th>Plays</th></tr></thead>
            <tbody>{rows_simple(plays_by_team, ['team_name', 'plays']) if plays_by_team else '<tr><td colspan="2">Sin datos</td></tr>'}</tbody>
        </table>
    </div>

    <div class="card">
        <h2>Plays por app hoy</h2>
        <table>
            <thead><tr><th>App</th><th>Plays</th></tr></thead>
            <tbody>{rows_simple(plays_by_app, ['app_name', 'plays']) if plays_by_app else '<tr><td colspan="2">Sin datos</td></tr>'}</tbody>
        </table>
    </div>
    """
    return render_layout("WatchEagle Analytics", body)


@app.route("/health")
def health():
    init_db()
    return jsonify({"ok": True, "service": "WatchEagle"})


@app.route("/seed-team")
def seed_team():
    init_db()
    name = request.args.get("name")
    app_name = request.args.get("app")
    user = request.args.get("user")

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO teams (name, app_name, lastfm_user, status)
        VALUES (%s, %s, %s, 'PENDING')
        ON CONFLICT (lastfm_user) DO NOTHING
        RETURNING id, name, app_name, lastfm_user
    """, (name, app_name, user))
    row = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()
    return jsonify({"created": row})


@app.route("/import-real-teams", methods=["POST"])
def import_real_teams():
    init_db()

    lines_text = request.form.get("lines", "").strip()
    if not lines_text:
        return jsonify({"ok": False, "error": "No se recibió contenido"}), 400

    lines = [x.strip() for x in lines_text.splitlines() if x.strip()]

    conn = get_conn()
    cur = conn.cursor()

    created = []
    skipped = []

    for line in lines:
        parts = [p.strip() for p in line.split(",")]

        if len(parts) != 3:
            skipped.append({"line": line, "reason": "Formato inválido. Usa nombre,app,user"})
            continue

        team_name, app_name, lastfm_user = parts

        if not lastfm_user_exists(lastfm_user):
            skipped.append({"line": line, "reason": "Usuario Last.fm no existe"})
            continue

        cur.execute("""
            INSERT INTO teams (name, app_name, lastfm_user, status)
            VALUES (%s, %s, %s, 'PENDING')
            ON CONFLICT (lastfm_user) DO NOTHING
            RETURNING id, name, app_name, lastfm_user
        """, (team_name, app_name, lastfm_user))

        row = cur.fetchone()
        if row:
            created.append(row)
        else:
            skipped.append({"line": line, "reason": "Ya existía en la base"})

    conn.commit()
    cur.close()
    conn.close()

    summary = {
        "ok": True,
        "created_count": len(created),
        "skipped_count": len(skipped),
        "created": created,
        "skipped": skipped
    }

    return render_layout(
        "Resultado importación",
        f"""
        <div class="card">
            <p><strong>Creados:</strong> {len(created)}</p>
            <p><strong>Omitidos:</strong> {len(skipped)}</p>
            <p><a class="btn btn-blue" href="/">Volver al monitor</a></p>
        </div>

        <div class="card">
            <h2>Creados</h2>
            <pre>{created}</pre>
        </div>

        <div class="card">
            <h2>Omitidos</h2>
            <pre>{skipped}</pre>
        </div>
        """
    )


@app.route("/update-team")
def update_team():
    init_db()
    team_id = request.args.get("id")
    name = request.args.get("name")
    app_name = request.args.get("app")
    user = request.args.get("user")

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        UPDATE teams
        SET name=%s, app_name=%s, lastfm_user=%s
        WHERE id=%s
        RETURNING id, name, app_name, lastfm_user
    """, (name, app_name, user, team_id))
    row = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()
    return jsonify({"updated": row})


@app.route("/delete-team")
def delete_team():
    init_db()
    team_id = request.args.get("id")

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM teams WHERE id=%s RETURNING id", (team_id,))
    row = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()

    return jsonify({"deleted": row})


@app.route("/delete-many")
def delete_many():
    init_db()
    ids = request.args.get("ids")
    id_list = [int(x) for x in ids.split(",")]

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM teams WHERE id = ANY(%s)", (id_list,))
    conn.commit()
    cur.close()
    conn.close()

    return jsonify({"deleted_ids": id_list})


@app.route("/reset-teams")
def reset_teams():
    init_db()

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("DELETE FROM scrobbles;")
    cur.execute("DELETE FROM teams;")

    cur.execute("ALTER SEQUENCE IF EXISTS scrobbles_id_seq RESTART WITH 1;")
    cur.execute("ALTER SEQUENCE IF EXISTS teams_id_seq RESTART WITH 1;")

    conn.commit()
    cur.close()
    conn.close()

    return jsonify({
        "ok": True,
        "message": "All teams and scrobbles deleted. IDs restarted from 1."
    })


@app.route("/load-teams")
def load_teams():
    init_db()
    total = int(request.args.get("total", 10))
    prefix = request.args.get("prefix", "equipo")
    app_name = request.args.get("app", "spotify")

    conn = get_conn()
    cur = conn.cursor()
    created = []

    for i in range(1, total + 1):
        name = f"{prefix}{str(i).zfill(2)}"
        cur.execute("""
            INSERT INTO teams (name, app_name, lastfm_user, status)
            VALUES (%s, %s, %s, 'PENDING')
            ON CONFLICT (lastfm_user) DO NOTHING
            RETURNING id, name
        """, (name, app_name, name))
        row = cur.fetchone()
        if row:
            created.append(row)

    conn.commit()
    cur.close()
    conn.close()

    return jsonify({"total_created": len(created)})


@app.route("/load-batch")
def load_batch():
    init_db()
    spotify = int(request.args.get("spotify", 0))
    tidal = int(request.args.get("tidal", 0))
    apple = int(request.args.get("apple", 0))

    conn = get_conn()
    cur = conn.cursor()
    created = []

    def create_group(total, prefix, app_name):
        for i in range(1, total + 1):
            name = f"{prefix}{str(i).zfill(2)}"
            cur.execute("""
                INSERT INTO teams (name, app_name, lastfm_user, status)
                VALUES (%s, %s, %s, 'PENDING')
                ON CONFLICT (lastfm_user) DO NOTHING
                RETURNING id, name
            """, (name, app_name, name))
            row = cur.fetchone()
            if row:
                created.append(row)

    create_group(spotify, "equipoS", "spotify")
    create_group(tidal, "equipoT", "tidal")
    create_group(apple, "equipoA", "apple")

    conn.commit()
    cur.close()
    conn.close()

    return jsonify({
        "created": len(created),
        "spotify": spotify,
        "tidal": tidal,
        "apple": apple
    })


@app.route("/debug-lastfm")
def debug_lastfm():
    user = request.args.get("user")
    url = "https://ws.audioscrobbler.com/2.0/"
    params = {
        "method": "user.getrecenttracks",
        "user": user,
        "api_key": LASTFM_API_KEY,
        "format": "json",
        "limit": 5
    }
    r = requests.get(url, params=params, timeout=30)
    return f"<pre>{r.text}</pre>"


@app.route("/run-check")
def run_check():
    result = subprocess.run(["python", "watch_scrobbles.py"], capture_output=True, text=True)
    return f"<pre>{result.stdout}\n{result.stderr}</pre>"


@app.route("/run-collector")
def run_collector():
    result = subprocess.run(["python", "collect_scrobbles.py"], capture_output=True, text=True)
    return f"<pre>{result.stdout}\n{result.stderr}</pre>"


if __name__ == "__main__":
    init_db()
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)

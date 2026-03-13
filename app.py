from flask import Flask, request, jsonify, redirect
import os
import psycopg2
from psycopg2.extras import RealDictCursor
import subprocess
import requests
import json

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
        artist VARCHAR(255),
        track VARCHAR(255),
        album VARCHAR(255),
        scrobbled_at TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)

    cur.execute("ALTER TABLE scrobbles ADD COLUMN IF NOT EXISTS team_id INTEGER;")
    cur.execute("ALTER TABLE scrobbles ADD COLUMN IF NOT EXISTS team_name VARCHAR(100);")
    cur.execute("ALTER TABLE scrobbles ADD COLUMN IF NOT EXISTS lastfm_user VARCHAR(100);")
    cur.execute("ALTER TABLE scrobbles ADD COLUMN IF NOT EXISTS app_name VARCHAR(50);")
    cur.execute("ALTER TABLE scrobbles ADD COLUMN IF NOT EXISTS artist VARCHAR(255);")
    cur.execute("ALTER TABLE scrobbles ADD COLUMN IF NOT EXISTS track VARCHAR(255);")
    cur.execute("ALTER TABLE scrobbles ADD COLUMN IF NOT EXISTS album VARCHAR(255);")
    cur.execute("ALTER TABLE scrobbles ADD COLUMN IF NOT EXISTS scrobbled_at TIMESTAMP;")
    cur.execute("ALTER TABLE scrobbles ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;")

    cur.execute("""
    DO $$
    BEGIN
        IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name='scrobbles' AND column_name='artist_name'
        ) AND NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name='scrobbles' AND column_name='artist'
        ) THEN
            ALTER TABLE scrobbles RENAME COLUMN artist_name TO artist;
        END IF;

        IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name='scrobbles' AND column_name='track_name'
        ) AND NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name='scrobbles' AND column_name='track'
        ) THEN
            ALTER TABLE scrobbles RENAME COLUMN track_name TO track;
        END IF;

        IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name='scrobbles' AND column_name='album_name'
        ) AND NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name='scrobbles' AND column_name='album'
        ) THEN
            ALTER TABLE scrobbles RENAME COLUMN album_name TO album;
        END IF;

        IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name='scrobbles' AND column_name='scrobble_time'
        ) AND NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name='scrobbles' AND column_name='scrobbled_at'
        ) THEN
            ALTER TABLE scrobbles RENAME COLUMN scrobble_time TO scrobbled_at;
        END IF;
    END $$;
    """)

    cur.execute("""
    DO $$
    BEGIN
        IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name='scrobbles' AND column_name='artist_name'
        ) THEN
            UPDATE scrobbles
            SET artist = COALESCE(artist, artist_name)
            WHERE artist IS NULL;
        END IF;

        IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name='scrobbles' AND column_name='track_name'
        ) THEN
            UPDATE scrobbles
            SET track = COALESCE(track, track_name)
            WHERE track IS NULL;
        END IF;

        IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name='scrobbles' AND column_name='album_name'
        ) THEN
            UPDATE scrobbles
            SET album = COALESCE(album, album_name)
            WHERE album IS NULL;
        END IF;

        IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name='scrobbles' AND column_name='scrobble_time'
        ) THEN
            UPDATE scrobbles
            SET scrobbled_at = COALESCE(scrobbled_at, scrobble_time)
            WHERE scrobbled_at IS NULL;
        END IF;
    END $$;
    """)

    cur.execute("""
    DO $$
    BEGIN
        IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name='scrobbles' AND column_name='artist_name'
        ) THEN
            ALTER TABLE scrobbles ALTER COLUMN artist_name DROP NOT NULL;
        END IF;

        IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name='scrobbles' AND column_name='track_name'
        ) THEN
            ALTER TABLE scrobbles ALTER COLUMN track_name DROP NOT NULL;
        END IF;

        IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name='scrobbles' AND column_name='album_name'
        ) THEN
            ALTER TABLE scrobbles ALTER COLUMN album_name DROP NOT NULL;
        END IF;

        IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name='scrobbles' AND column_name='scrobble_time'
        ) THEN
            ALTER TABLE scrobbles ALTER COLUMN scrobble_time DROP NOT NULL;
        END IF;
    END $$;
    """)

    cur.execute("ALTER TABLE scrobbles ALTER COLUMN artist DROP NOT NULL;")
    cur.execute("ALTER TABLE scrobbles ALTER COLUMN track DROP NOT NULL;")
    cur.execute("ALTER TABLE scrobbles ALTER COLUMN album DROP NOT NULL;")
    cur.execute("ALTER TABLE scrobbles ALTER COLUMN scrobbled_at DROP NOT NULL;")

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
        <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
        <style>
            :root {{
                --jatune-gold: #FACC15;
                --jatune-cream: #FEFCE8;
                --jatune-ink: #1F2937;
                --jatune-ink-soft: #334155;
                --jatune-card: rgba(255, 255, 255, 0.72);
                --jatune-border: rgba(250, 204, 21, 0.28);
                --jatune-shadow: 0 18px 50px rgba(15, 23, 42, 0.14);
            }}

            * {{
                box-sizing: border-box;
            }}

            body {{
                font-family: Arial, sans-serif;
                margin: 0;
                padding: 22px;
                color: var(--jatune-ink);
                background:
                    radial-gradient(circle at top left, rgba(250,204,21,0.35), transparent 28%),
                    radial-gradient(circle at top right, rgba(250,204,21,0.18), transparent 24%),
                    linear-gradient(180deg, #fffdf4 0%, #FEFCE8 48%, #fff9db 100%);
            }}

            .shell {{
                max-width: 1600px;
                margin: 0 auto;
            }}

            .brandbar {{
                display: flex;
                justify-content: space-between;
                align-items: center;
                gap: 18px;
                margin-bottom: 18px;
                flex-wrap: wrap;
            }}

            .brand-left {{
                display: flex;
                align-items: center;
                gap: 14px;
            }}

            .brand-logo {{
                width: 68px;
                height: 68px;
                border-radius: 18px;
                object-fit: cover;
                border: 2px solid rgba(250, 204, 21, 0.45);
                box-shadow: 0 10px 30px rgba(250, 204, 21, 0.18);
                background: #ffffff;
            }}

            .brand-copy h1 {{
                margin: 0;
                font-size: 26px;
                font-weight: 800;
                color: #111827;
                letter-spacing: -0.4px;
            }}

            .brand-copy .sub {{
                margin-top: 4px;
                font-size: 13px;
                color: #6B7280;
            }}

            .brand-badge {{
                display: inline-flex;
                align-items: center;
                gap: 8px;
                margin-top: 6px;
                padding: 6px 10px;
                border-radius: 999px;
                background: rgba(250, 204, 21, 0.14);
                color: #8a6510;
                font-size: 12px;
                font-weight: 700;
                border: 1px solid rgba(250, 204, 21, 0.22);
            }}

            .nav {{
                display: flex;
                gap: 8px;
                flex-wrap: wrap;
                align-items: center;
            }}

            .nav a, .btn {{
                text-decoration: none;
                border: none;
                cursor: pointer;
                border-radius: 12px;
                padding: 10px 14px;
                font-size: 13px;
                font-weight: 800;
                color: #111827;
                background: rgba(255,255,255,0.8);
                border: 1px solid rgba(250, 204, 21, 0.18);
                box-shadow: 0 8px 22px rgba(15, 23, 42, 0.06);
                transition: transform 0.15s ease, box-shadow 0.15s ease;
            }}

            .nav a:hover, .btn:hover {{
                transform: translateY(-1px);
                box-shadow: 0 12px 28px rgba(15, 23, 42, 0.09);
            }}

            .btn-blue {{
                background: linear-gradient(180deg, #FACC15 0%, #f2bb07 100%);
                color: #1f2937;
            }}

            .btn-green {{
                background: linear-gradient(180deg, #fde047 0%, #FACC15 100%);
                color: #1f2937;
            }}

            .btn-red {{
                background: linear-gradient(180deg, #fb7185 0%, #f43f5e 100%);
                color: white;
            }}

            .btn-orange {{
                background: linear-gradient(180deg, #fbbf24 0%, #f59e0b 100%);
                color: #1f2937;
            }}

            .card, .kpi, .compact-card, .chart-card, table {{
                background: var(--jatune-card);
                backdrop-filter: blur(10px);
                border: 1px solid var(--jatune-border);
                box-shadow: var(--jatune-shadow);
            }}

            .card {{
                padding: 14px;
                border-radius: 18px;
                margin-bottom: 14px;
            }}

            .grid {{
                display: grid;
                grid-template-columns: repeat(4, minmax(140px, 1fr));
                gap: 12px;
                margin-bottom: 14px;
            }}

            .kpi {{
                padding: 14px;
                border-radius: 18px;
            }}

            .kpi .label {{
                color: #6B7280;
                font-size: 12px;
                font-weight: 700;
            }}

            .kpi .value {{
                font-size: 28px;
                font-weight: 900;
                margin-top: 8px;
                color: #111827;
            }}

            .mini-grid {{
                display: grid;
                grid-template-columns: repeat(3, minmax(120px, 1fr));
                gap: 10px;
                margin-bottom: 12px;
            }}

            .mini-kpi {{
                background: rgba(255,255,255,0.82);
                padding: 12px;
                border-radius: 14px;
                border: 1px solid rgba(250, 204, 21, 0.18);
            }}

            .mini-kpi .label {{
                color: #6B7280;
                font-size: 11px;
                font-weight: 700;
            }}

            .mini-kpi .value {{
                font-size: 21px;
                font-weight: 900;
                margin-top: 4px;
                color: #111827;
            }}

            table {{
                width: 100%;
                border-collapse: separate;
                border-spacing: 0;
                border-radius: 18px;
                overflow: hidden;
                margin-bottom: 14px;
            }}

            th, td {{
                padding: 11px 12px;
                border-bottom: 1px solid rgba(15, 23, 42, 0.06);
                text-align: left;
                font-size: 13px;
                color: #1F2937;
            }}

            th {{
                background: rgba(250, 204, 21, 0.18);
                color: #6B7280;
                font-weight: 800;
            }}

            tr:last-child td {{
                border-bottom: none;
            }}

            .ok {{
                color: #15803d;
                font-weight: 900;
            }}

            .warn {{
                color: #b45309;
                font-weight: 900;
            }}

            .incident {{
                color: #dc2626;
                font-weight: 900;
            }}

            .hint {{
                color: #6B7280;
                font-size: 11px;
                margin-top: 4px;
            }}

            code {{
                background: rgba(255,255,255,0.9);
                padding: 2px 6px;
                border-radius: 8px;
                font-size: 11px;
                color: #374151;
                border: 1px solid rgba(15,23,42,0.05);
            }}

            input, textarea, select {{
                padding: 9px 10px;
                border-radius: 12px;
                border: 1px solid rgba(15, 23, 42, 0.09);
                background: rgba(255,255,255,0.9);
                color: #111827;
                width: 100%;
                font-size: 13px;
            }}

            textarea {{
                min-height: 72px;
                resize: vertical;
            }}

            .inline-form {{
                display: flex;
                gap: 8px;
                flex-wrap: wrap;
                align-items: center;
                margin-top: 8px;
            }}

            .monitor-layout {{
                display: grid;
                grid-template-columns: 220px 220px 1fr 220px;
                gap: 12px;
                margin-bottom: 14px;
                align-items: start;
            }}

            .compact-card {{
                padding: 14px;
                border-radius: 18px;
            }}

            .compact-card h3 {{
                margin: 0 0 8px 0;
                font-size: 15px;
                color: #111827;
            }}

            .chart-card {{
                padding: 16px;
                border-radius: 18px;
                margin-bottom: 14px;
            }}

            .section-title {{
                margin: 0 0 8px 0;
                font-size: 20px;
                color: #111827;
                letter-spacing: -0.2px;
            }}

            .subtle {{
                color: #6B7280;
                font-size: 12px;
            }}

            @media (max-width: 1200px) {{
                .grid {{
                    grid-template-columns: repeat(2, minmax(140px, 1fr));
                }}
                .mini-grid {{
                    grid-template-columns: 1fr;
                }}
                .monitor-layout {{
                    grid-template-columns: 1fr;
                }}
            }}

            @media (max-width: 768px) {{
                .brand-left {{
                    align-items: flex-start;
                }}
                .brand-copy h1 {{
                    font-size: 22px;
                }}
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

            function runCollectorAndReturn() {{
                window.open('/run-collector', '_blank');
            }}

            function runCheckAndReturn() {{
                window.open('/run-check', '_blank');
            }}
        </script>
    </head>
    <body>
        <div class="shell">
            <div class="brandbar">
                <div class="brand-left">
                    <img src="/static/watch_eagle.png" alt="JATune" class="brand-logo">
                    <div class="brand-copy">
                        <h1>{title}</h1>
                        <div class="sub">Centro analítico musical • JaTune Intelligence</div>
                        <div class="brand-badge">JATune</div>
                    </div>
                </div>

                <div class="nav">
                    <a href="/">Monitor</a>
                    <a href="/analytics">Analytics</a>
                    <button class="btn btn-blue" onclick="window.location.reload()">Refrescar</button>
                    <button class="btn btn-green" onclick="runCheckAndReturn()">Correr chequeo</button>
                    <button class="btn btn-green" onclick="runCollectorAndReturn()">Correr collector</button>
                    <button class="btn btn-red" onclick="resetAll()">Borrar todo</button>
                </div>
            </div>

            {body_html}
        </div>
    </body>
    </html>
    """


@app.route("/")
def home():
    init_db()

    app_filter = request.args.get("app", "all")
    status_filter = request.args.get("status", "all")

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT DISTINCT app_name
        FROM teams
        WHERE active = TRUE
        ORDER BY app_name
    """)
    apps = cur.fetchall()

    where_clauses = ["active = TRUE"]
    params = []

    if app_filter != "all":
        where_clauses.append("app_name = %s")
        params.append(app_filter)

    if status_filter != "all":
        where_clauses.append("status = %s")
        params.append(status_filter)

    where_sql = " AND ".join(where_clauses)

    cur.execute(f"""
        SELECT
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE status = 'OK') AS ok_count,
            COUNT(*) FILTER (WHERE status = 'WARN') AS warn_count,
            COUNT(*) FILTER (WHERE status = 'INCIDENT') AS incident_count
        FROM teams
        WHERE {where_sql}
    """, params)
    summary = cur.fetchone()

    cur.execute(f"""
        SELECT id, name, app_name, lastfm_user, status, idle_minutes
        FROM teams
        WHERE {where_sql}
        ORDER BY id ASC
    """, params)
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
            <td>{t['idle_minutes']}</td>
        </tr>
        """

    app_options = '<option value="all">Todas</option>'
    for a in apps:
        selected = "selected" if a["app_name"] == app_filter else ""
        app_options += f'<option value="{a["app_name"]}" {selected}>{a["app_name"]}</option>'

    status_options = ""
    for opt in ["all", "OK", "WARN", "INCIDENT", "PENDING"]:
        label = "Todos" if opt == "all" else opt
        selected = "selected" if opt == status_filter else ""
        status_options += f'<option value="{opt}" {selected}>{label}</option>'

    body = f"""
    <div class="grid">
        <div class="kpi">
            <div class="label">Equipos activos</div>
            <div class="value">{summary['total'] or 0}</div>
        </div>
        <div class="kpi">
            <div class="label">🟢 OK</div>
            <div class="value">{summary['ok_count'] or 0}</div>
        </div>
        <div class="kpi">
            <div class="label">🟡 WARN</div>
            <div class="value">{summary['warn_count'] or 0}</div>
        </div>
        <div class="kpi">
            <div class="label">🔴 INCIDENT</div>
            <div class="value">{summary['incident_count'] or 0}</div>
        </div>
    </div>

    <div class="monitor-layout">
        <div class="compact-card">
            <h3>Eliminar por ID</h3>
            <input id="deleteId" type="number" placeholder="ID a borrar">
            <div class="inline-form">
                <button class="btn btn-orange" onclick="deleteById()">Eliminar</button>
            </div>
        </div>

        <div class="compact-card">
            <h3>Filtros</h3>
            <form method="GET" action="/">
                <div class="hint">App</div>
                <select name="app">{app_options}</select>
                <div class="hint" style="margin-top:8px;">Estado</div>
                <select name="status">{status_options}</select>
                <div class="inline-form">
                    <button class="btn btn-blue" type="submit">Aplicar</button>
                </div>
            </form>
        </div>

        <div class="compact-card">
            <h3>Importar equipos reales de Last.fm</h3>
            <p class="hint">Formato: <code>Nombre visible,app,usuario_real_lastfm</code></p>
            <form method="POST" action="/import-real-teams">
                <textarea name="lines" placeholder="Equipo T01,tidal,equipoS01&#10;equipoG01,spotify,equipoG01"></textarea>
                <div class="inline-form">
                    <button class="btn btn-green" type="submit">Importar usuarios válidos</button>
                </div>
            </form>
        </div>

        <div class="compact-card">
            <h3>Acciones rápidas</h3>
            <p class="hint"><code>/run-check</code></p>
            <p class="hint"><code>/run-collector</code></p>
            <p class="hint"><code>/analytics</code></p>
            <p class="hint"><code>/debug-lastfm?user=JeanCMP</code></p>
        </div>
    </div>

    <div class="card">
        <h2 class="section-title">Vista operativa</h2>
        <div class="subtle">Monitoreo central de equipos y pausas de reproducción.</div>
    </div>

    <table>
        <thead>
            <tr>
                <th>ID</th>
                <th>Equipo</th>
                <th>App</th>
                <th>Usuario Last.fm</th>
                <th>Estado</th>
                <th>Min pausado</th>
            </tr>
        </thead>
        <tbody>
            {rows if rows else '<tr><td colspan="6">No hay equipos cargados</td></tr>'}
        </tbody>
    </table>
    """
    return render_layout("WatchEagle Monitor", body)


@app.route("/analytics")
def analytics():
    init_db()

    app_filter = request.args.get("app", "all")
    month_filter = request.args.get("month", "all")

    conn = get_conn()
    cur = conn.cursor()

    where_clauses = []
    params = []

    if app_filter != "all":
        where_clauses.append("app_name = %s")
        params.append(app_filter)

    if month_filter != "all":
        where_clauses.append("to_char(scrobbled_at, 'YYYY-MM') = %s")
        params.append(month_filter)

    where_sql = ""
    if where_clauses:
        where_sql = " AND " + " AND ".join(where_clauses)

    cur.execute("SELECT DISTINCT app_name FROM scrobbles WHERE app_name IS NOT NULL ORDER BY app_name")
    apps = cur.fetchall()

    cur.execute("""
        SELECT DISTINCT to_char(scrobbled_at, 'YYYY-MM') AS month_key
        FROM scrobbles
        WHERE scrobbled_at IS NOT NULL
        ORDER BY month_key DESC
    """)
    months = cur.fetchall()

    cur.execute(f"""
        SELECT COUNT(*) AS c
        FROM scrobbles
        WHERE DATE(scrobbled_at) = CURRENT_DATE
        {where_sql}
    """, params)
    plays_today = cur.fetchone()["c"]

    cur.execute(f"""
        SELECT COUNT(*) AS c
        FROM scrobbles
        WHERE DATE(scrobbled_at) = CURRENT_DATE - INTERVAL '1 day'
        {where_sql}
    """, params)
    plays_yesterday = cur.fetchone()["c"]

    diff = plays_today - plays_yesterday

    cur.execute(f"""
        SELECT COUNT(*) AS c
        FROM scrobbles
        WHERE date_trunc('month', scrobbled_at) = date_trunc('month', CURRENT_DATE)
        {where_sql}
    """, params)
    month_current_total = cur.fetchone()["c"]

    cur.execute(f"""
        SELECT COUNT(*) AS c
        FROM scrobbles
        WHERE date_trunc('month', scrobbled_at) = date_trunc('month', CURRENT_DATE - INTERVAL '1 month')
        {where_sql}
    """, params)
    month_previous_total = cur.fetchone()["c"]

    month_diff = month_current_total - month_previous_total

    cur.execute(f"""
        SELECT
            COALESCE(SUM(CASE WHEN LOWER(app_name) = 'spotify' THEN 1 ELSE 0 END), 0) AS spotify_total,
            COALESCE(SUM(CASE WHEN LOWER(app_name) = 'tidal' THEN 1 ELSE 0 END), 0) AS tidal_total,
            COALESCE(SUM(CASE WHEN LOWER(app_name) = 'apple' OR LOWER(app_name) = 'apple music' THEN 1 ELSE 0 END), 0) AS apple_total
        FROM scrobbles
        WHERE 1=1 {where_sql}
    """, params)
    app_totals = cur.fetchone()

    cur.execute(f"""
        SELECT COALESCE(artist, '-') AS artist, COUNT(*) AS plays
        FROM scrobbles
        WHERE 1=1 {where_sql}
        GROUP BY artist
        ORDER BY plays DESC
        LIMIT 10
    """, params)
    top_artists = cur.fetchall()

    cur.execute(f"""
        SELECT COALESCE(track, '-') AS track, COALESCE(artist, '-') AS artist, COUNT(*) AS plays
        FROM scrobbles
        WHERE 1=1 {where_sql}
        GROUP BY track, artist
        ORDER BY plays DESC
        LIMIT 10
    """, params)
    top_tracks = cur.fetchall()

    cur.execute(f"""
        SELECT COALESCE(team_name, '-') AS team_name, COUNT(*) AS plays
        FROM scrobbles
        WHERE 1=1 {where_sql}
        GROUP BY team_name
        ORDER BY plays DESC
        LIMIT 15
    """, params)
    plays_by_team = cur.fetchall()

    cur.execute(f"""
        SELECT COALESCE(app_name, '-') AS app_name, COUNT(*) AS plays
        FROM scrobbles
        WHERE 1=1 {where_sql}
        GROUP BY app_name
        ORDER BY plays DESC
    """, params)
    plays_by_app = cur.fetchall()

    cur.execute(f"""
        SELECT
            COALESCE(artist, '-') AS artist,
            SUM(CASE WHEN DATE(scrobbled_at) = CURRENT_DATE THEN 1 ELSE 0 END) AS hoy,
            SUM(CASE WHEN DATE(scrobbled_at) = CURRENT_DATE - INTERVAL '1 day' THEN 1 ELSE 0 END) AS ayer
        FROM scrobbles
        WHERE 1=1 {where_sql}
        GROUP BY artist
        ORDER BY hoy DESC, ayer DESC
        LIMIT 15
    """, params)
    compare_artists = cur.fetchall()

    cur.execute(f"""
        SELECT
            to_char(
                date_trunc('hour', scrobbled_at)
                + floor(extract(minute from scrobbled_at) / 30) * interval '30 minutes',
                'YYYY-MM-DD HH24:MI'
            ) AS slot_30m,
            COUNT(*) AS plays
        FROM scrobbles
        WHERE scrobbled_at >= NOW() - INTERVAL '24 hours'
        {where_sql}
        GROUP BY 1
        ORDER BY 1 DESC
        LIMIT 24
    """, params)
    plays_30m = cur.fetchall()

    cur.execute(f"""
        SELECT
            to_char(DATE(scrobbled_at), 'YYYY-MM-DD') AS day_label,
            COUNT(*) AS plays
        FROM scrobbles
        WHERE 1=1 {where_sql}
        GROUP BY DATE(scrobbled_at)
        ORDER BY DATE(scrobbled_at) ASC
    """, params)
    daily_line = cur.fetchall()

    cur.close()
    conn.close()

    line_labels = [x["day_label"] for x in daily_line]
    line_values = [x["plays"] for x in daily_line]

    def rows_simple(items, cols):
        html = ""
        for item in items:
            html += "<tr>" + "".join(f"<td>{item[col] or '-'}</td>" for col in cols) + "</tr>"
        return html

    app_options = '<option value="all">Todas</option>'
    for a in apps:
        selected = "selected" if a["app_name"] == app_filter else ""
        app_options += f'<option value="{a["app_name"]}" {selected}>{a["app_name"]}</option>'

    month_options = '<option value="all">Todos</option>'
    for m in months:
        selected = "selected" if m["month_key"] == month_filter else ""
        month_options += f'<option value="{m["month_key"]}" {selected}>{m["month_key"]}</option>'

    body = f"""
    <div class="monitor-layout" style="grid-template-columns:220px 220px 1fr 1fr;">
        <div class="compact-card">
            <h3>Filtro app</h3>
            <form method="GET" action="/analytics">
                <select name="app">{app_options}</select>
                <input type="hidden" name="month" value="{month_filter}">
                <div class="inline-form">
                    <button class="btn btn-blue" type="submit">Aplicar</button>
                </div>
            </form>
        </div>

        <div class="compact-card">
            <h3>Filtro mes</h3>
            <form method="GET" action="/analytics">
                <select name="month">{month_options}</select>
                <input type="hidden" name="app" value="{app_filter}">
                <div class="inline-form">
                    <button class="btn btn-blue" type="submit">Aplicar</button>
                </div>
            </form>
        </div>

        <div class="kpi">
            <div class="label">Plays hoy</div>
            <div class="value">{plays_today}</div>
        </div>

        <div class="kpi">
            <div class="label">Plays ayer / Variación</div>
            <div class="value">{plays_yesterday} / {diff}</div>
        </div>
    </div>

    <div class="grid" style="grid-template-columns:2fr 1fr;">
        <div class="kpi">
            <div class="label">Mes actual vs mes anterior</div>
            <div class="value">{month_current_total} / {month_previous_total}</div>
            <div class="hint">Variación mensual: {month_diff}</div>
        </div>

        <div class="card" style="margin-bottom:0;">
            <div class="label" style="color:#6B7280;font-size:12px;margin-bottom:8px;">Reproducciones por app (según filtros)</div>
            <div class="mini-grid">
                <div class="mini-kpi">
                    <div class="label">Spotify</div>
                    <div class="value">{app_totals['spotify_total']}</div>
                </div>
                <div class="mini-kpi">
                    <div class="label">Tidal</div>
                    <div class="value">{app_totals['tidal_total']}</div>
                </div>
                <div class="mini-kpi">
                    <div class="label">Apple</div>
                    <div class="value">{app_totals['apple_total']}</div>
                </div>
            </div>
        </div>
    </div>

    <div class="chart-card">
        <h2 class="section-title">Tendencia diaria de reproducciones</h2>
        <div class="subtle">Pulso diario consolidado del consumo musical.</div>
        <canvas id="dailyLineChart" height="95"></canvas>
    </div>

    <div class="card">
        <h2 class="section-title">Top artistas</h2>
        <table>
            <thead><tr><th>Artista</th><th>Plays</th></tr></thead>
            <tbody>{rows_simple(top_artists, ['artist', 'plays']) if top_artists else '<tr><td colspan="2">Sin datos</td></tr>'}</tbody>
        </table>
    </div>

    <div class="card">
        <h2 class="section-title">Top canciones</h2>
        <table>
            <thead><tr><th>Canción</th><th>Artista</th><th>Plays</th></tr></thead>
            <tbody>{rows_simple(top_tracks, ['track', 'artist', 'plays']) if top_tracks else '<tr><td colspan="3">Sin datos</td></tr>'}</tbody>
        </table>
    </div>

    <div class="card">
        <h2 class="section-title">Comparativa artistas: hoy vs ayer</h2>
        <table>
            <thead><tr><th>Artista</th><th>Hoy</th><th>Ayer</th></tr></thead>
            <tbody>{rows_simple(compare_artists, ['artist', 'hoy', 'ayer']) if compare_artists else '<tr><td colspan="3">Sin datos</td></tr>'}</tbody>
        </table>
    </div>

    <div class="card">
        <h2 class="section-title">Plays por equipo</h2>
        <table>
            <thead><tr><th>Equipo</th><th>Plays</th></tr></thead>
            <tbody>{rows_simple(plays_by_team, ['team_name', 'plays']) if plays_by_team else '<tr><td colspan="2">Sin datos</td></tr>'}</tbody>
        </table>
    </div>

    <div class="card">
        <h2 class="section-title">Plays por app</h2>
        <table>
            <thead><tr><th>App</th><th>Plays</th></tr></thead>
            <tbody>{rows_simple(plays_by_app, ['app_name', 'plays']) if plays_by_app else '<tr><td colspan="2">Sin datos</td></tr>'}</tbody>
        </table>
    </div>

    <div class="card">
        <h2 class="section-title">Reproducciones cada 30 minutos (últimas 24h)</h2>
        <table>
            <thead><tr><th>Bloque</th><th>Plays</th></tr></thead>
            <tbody>{rows_simple(plays_30m, ['slot_30m', 'plays']) if plays_30m else '<tr><td colspan="2">Sin datos</td></tr>'}</tbody>
        </table>
    </div>

    <script>
        const ctx = document.getElementById('dailyLineChart').getContext('2d');
        new Chart(ctx, {{
            type: 'line',
            data: {{
                labels: {json.dumps(line_labels)},
                datasets: [{{
                    label: 'Reproducciones por día',
                    data: {json.dumps(line_values)},
                    borderColor: '#FACC15',
                    backgroundColor: 'rgba(250,204,21,0.18)',
                    tension: 0.28,
                    fill: true
                }}]
            }},
            options: {{
                responsive: true,
                plugins: {{
                    legend: {{
                        labels: {{
                            color: '#374151'
                        }}
                    }}
                }},
                scales: {{
                    x: {{
                        ticks: {{ color: '#4B5563' }},
                        grid: {{ color: 'rgba(15,23,42,0.08)' }}
                    }},
                    y: {{
                        ticks: {{ color: '#4B5563' }},
                        grid: {{ color: 'rgba(15,23,42,0.08)' }}
                    }}
                }}
            }}
        }});
    </script>
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

    return render_layout(
        "Resultado importación",
        f"""
        <div class="card">
            <p><strong>Creados:</strong> {len(created)}</p>
            <p><strong>Omitidos:</strong> {len(skipped)}</p>
            <p><a class="btn btn-blue" href="/">Volver al monitor</a></p>
        </div>

        <div class="card">
            <h2 class="section-title">Creados</h2>
            <pre>{created}</pre>
        </div>

        <div class="card">
            <h2 class="section-title">Omitidos</h2>
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
    conn.commit()
    cur.close()
    conn.close()

    return redirect("/")


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

    return redirect("/")


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

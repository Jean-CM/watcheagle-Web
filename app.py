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

    # ===== teams =====
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

    # ===== scrobbles =====
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

    # Agregar columnas nuevas si faltan
    cur.execute("ALTER TABLE scrobbles ADD COLUMN IF NOT EXISTS team_id INTEGER;")
    cur.execute("ALTER TABLE scrobbles ADD COLUMN IF NOT EXISTS team_name VARCHAR(100);")
    cur.execute("ALTER TABLE scrobbles ADD COLUMN IF NOT EXISTS lastfm_user VARCHAR(100);")
    cur.execute("ALTER TABLE scrobbles ADD COLUMN IF NOT EXISTS app_name VARCHAR(50);")
    cur.execute("ALTER TABLE scrobbles ADD COLUMN IF NOT EXISTS artist VARCHAR(255);")
    cur.execute("ALTER TABLE scrobbles ADD COLUMN IF NOT EXISTS track VARCHAR(255);")
    cur.execute("ALTER TABLE scrobbles ADD COLUMN IF NOT EXISTS album VARCHAR(255);")
    cur.execute("ALTER TABLE scrobbles ADD COLUMN IF NOT EXISTS scrobbled_at TIMESTAMP;")
    cur.execute("ALTER TABLE scrobbles ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;")

    # Resolver esquemas viejos
    cur.execute("""
    DO $$
    BEGIN
        -- Si existe artist_name y NO existe artist, renombrar
        IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name='scrobbles' AND column_name='artist_name'
        ) AND NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name='scrobbles' AND column_name='artist'
        ) THEN
            ALTER TABLE scrobbles RENAME COLUMN artist_name TO artist;
        END IF;

        -- Si existe track_name y NO existe track, renombrar
        IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name='scrobbles' AND column_name='track_name'
        ) AND NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name='scrobbles' AND column_name='track'
        ) THEN
            ALTER TABLE scrobbles RENAME COLUMN track_name TO track;
        END IF;

        -- Si existe album_name y NO existe album, renombrar
        IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name='scrobbles' AND column_name='album_name'
        ) AND NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name='scrobbles' AND column_name='album'
        ) THEN
            ALTER TABLE scrobbles RENAME COLUMN album_name TO album;
        END IF;
    END $$;
    """)

    # Si las columnas viejas siguen existiendo, copiar datos y quitar NOT NULL
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
            ALTER TABLE scrobbles ALTER COLUMN artist_name DROP NOT NULL;
        END IF;

        IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name='scrobbles' AND column_name='track_name'
        ) THEN
            UPDATE scrobbles
            SET track = COALESCE(track, track_name)
            WHERE track IS NULL;
            ALTER TABLE scrobbles ALTER COLUMN track_name DROP NOT NULL;
        END IF;

        IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name='scrobbles' AND column_name='album_name'
        ) THEN
            UPDATE scrobbles
            SET album = COALESCE(album, album_name)
            WHERE album IS NULL;
            ALTER TABLE scrobbles ALTER COLUMN album_name DROP NOT NULL;
        END IF;
    END $$;
    """)

    # Quitar NOT NULL heredados en columnas nuevas
    cur.execute("ALTER TABLE scrobbles ALTER COLUMN artist DROP NOT NULL;")
    cur.execute("ALTER TABLE scrobbles ALTER COLUMN track DROP NOT NULL;")
    cur.execute("ALTER TABLE scrobbles ALTER COLUMN album DROP NOT NULL;")

    # Índice único para evitar duplicados
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
            body {{
                font-family: Arial, sans-serif;
                background: #071226;
                color: #e5e7eb;
                padding: 20px;
                margin: 0;
            }}

            .topbar {{
                display: flex;
                justify-content: space-between;
                align-items: center;
                margin-bottom: 14px;
                gap: 12px;
                flex-wrap: wrap;
            }}

            .nav {{
                display: flex;
                gap: 8px;
                flex-wrap: wrap;
            }}

            .nav a, .btn {{
                background: #1a2740;
                color: white;
                text-decoration: none;
                padding: 9px 13px;
                border-radius: 8px;
                display: inline-block;
                border: none;
                cursor: pointer;
                font-weight: bold;
                font-size: 13px;
            }}

            .btn-green {{ background: #16a34a; }}
            .btn-blue {{ background: #2563eb; }}
            .btn-red {{ background: #dc2626; }}
            .btn-orange {{ background: #ea580c; }}

            .card {{
                background: #0f1b33;
                padding: 14px;
                border-radius: 12px;
                margin-bottom: 14px;
            }}

            .grid {{
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
                gap: 12px;
                margin-bottom: 14px;
            }}

            .kpi {{
                background: #0f1b33;
                padding: 14px;
                border-radius: 12px;
            }}

            .kpi .label {{
                color: #9ca3af;
                font-size: 12px;
            }}

            .kpi .value {{
                font-size: 26px;
                font-weight: bold;
                margin-top: 8px;
            }}

            table {{
                width: 100%;
                border-collapse: collapse;
                background: #0f1b33;
                border-radius: 12px;
                overflow: hidden;
                margin-bottom: 14px;
            }}

            th, td {{
                padding: 10px;
                border-bottom: 1px solid #1f2937;
                text-align: left;
                font-size: 13px;
            }}

            th {{
                background: #1a2740;
            }}

            .ok {{ color: #22c55e; font-weight: bold; }}
            .warn {{ color: #f59e0b; font-weight: bold; }}
            .incident {{ color: #ef4444; font-weight: bold; }}

            .hint {{
                color: #9ca3af;
                font-size: 12px;
                margin-top: 5px;
            }}

            code {{
                background: #111827;
                padding: 2px 6px;
                border-radius: 6px;
                font-size: 11px;
            }}

            input, textarea, select {{
                padding: 9px;
                border-radius: 8px;
                border: 1px solid #334155;
                background: #0b1220;
                color: white;
                width: 100%;
                box-sizing: border-box;
                font-size: 13px;
            }}

            textarea {{
                min-height: 78px;
            }}

            .inline-form {{
                display: flex;
                gap: 8px;
                flex-wrap: wrap;
                align-items: center;
                margin-top: 8px;
            }}

            .quick-grid {{
                display: grid;
                grid-template-columns: 220px 1fr 220px;
                gap: 12px;
                margin-bottom: 14px;
            }}

            .compact-card {{
                background: #0f1b33;
                padding: 12px;
                border-radius: 12px;
            }}

            .compact-card h3 {{
                margin: 0 0 8px 0;
                font-size: 16px;
            }}

            .filters {{
                display: grid;
                grid-template-columns: repeat(4, minmax(150px, 1fr));
                gap: 10px;
                align-items: end;
                margin-bottom: 14px;
            }}

            .chart-card {{
                background: #0f1b33;
                padding: 14px;
                border-radius: 12px;
                margin-bottom: 14px;
            }}

            @media (max-width: 1100px) {{
                .quick-grid {{
                    grid-template-columns: 1fr;
                }}
                .filters {{
                    grid-template-columns: 1fr;
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
        <div class="topbar">
            <h1 style="margin:0;">{title}</h1>
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

    <form method="GET" action="/" class="filters">
        <div class="card" style="margin-bottom:0;">
            <div class="hint">Filtrar por app</div>
            <select name="app">{app_options}</select>
        </div>
        <div class="card" style="margin-bottom:0;">
            <div class="hint">Filtrar por estado</div>
            <select name="status">{status_options}</select>
        </div>
        <div class="card" style="margin-bottom:0; display:flex; align-items:end;">
            <button class="btn btn-blue" type="submit">Aplicar filtros</button>
        </div>
        <div class="card" style="margin-bottom:0;">
            <div class="hint">Atajo</div>
            <code>/reset-teams</code>
        </div>
    </form>

    <div class="quick-grid">
        <div class="compact-card">
            <h3>Eliminar por ID</h3>
            <input id="deleteId" type="number" placeholder="ID a borrar">
            <div class="inline-form">
                <button class="btn btn-orange" onclick="deleteById()">Eliminar</button>
            </div>
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
    <form method="GET" action="/analytics" class="filters">
        <div class="card" style="margin-bottom:0;">
            <div class="hint">Filtrar por app</div>
            <select name="app">{app_options}</select>
        </div>
        <div class="card" style="margin-bottom:0;">
            <div class="hint">Filtrar por mes</div>
            <select name="month">{month_options}</select>
        </div>
        <div class="card" style="margin-bottom:0;display:flex;align-items:end;">
            <button class="btn btn-blue" type="submit">Aplicar filtros</button>
        </div>
    </form>

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

    <div class="chart-card">
        <h2 style="margin-top:0;">Tendencia diaria de reproducciones</h2>
        <canvas id="dailyLineChart" height="100"></canvas>
    </div>

    <div class="card">
        <h2>Top artistas</h2>
        <table>
            <thead><tr><th>Artista</th><th>Plays</th></tr></thead>
            <tbody>{rows_simple(top_artists, ['artist', 'plays']) if top_artists else '<tr><td colspan="2">Sin datos</td></tr>'}</tbody>
        </table>
    </div>

    <div class="card">
        <h2>Top canciones</h2>
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
        <h2>Plays por equipo</h2>
        <table>
            <thead><tr><th>Equipo</th><th>Plays</th></tr></thead>
            <tbody>{rows_simple(plays_by_team, ['team_name', 'plays']) if plays_by_team else '<tr><td colspan="2">Sin datos</td></tr>'}</tbody>
        </table>
    </div>

    <div class="card">
        <h2>Plays por app</h2>
        <table>
            <thead><tr><th>App</th><th>Plays</th></tr></thead>
            <tbody>{rows_simple(plays_by_app, ['app_name', 'plays']) if plays_by_app else '<tr><td colspan="2">Sin datos</td></tr>'}</tbody>
        </table>
    </div>

    <div class="card">
        <h2>Reproducciones cada 30 minutos (últimas 24h)</h2>
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
                    borderColor: '#3b82f6',
                    backgroundColor: 'rgba(59,130,246,0.15)',
                    tension: 0.25,
                    fill: true
                }}]
            }},
            options: {{
                responsive: true,
                plugins: {{
                    legend: {{
                        labels: {{
                            color: '#e5e7eb'
                        }}
                    }}
                }},
                scales: {{
                    x: {{
                        ticks: {{ color: '#e5e7eb' }},
                        grid: {{ color: '#1f2937' }}
                    }},
                    y: {{
                        ticks: {{ color: '#e5e7eb' }},
                        grid: {{ color: '#1f2937' }}
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

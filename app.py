from flask import Flask, request, jsonify, redirect, render_template_string
import os
import psycopg2
from psycopg2.extras import RealDictCursor
import subprocess
import requests
import json
from collections import defaultdict

app = Flask(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL")
LASTFM_API_KEY = os.environ.get("LASTFM_API_KEY")

# =========================
# CONFIG CATALOGO / RATES
# =========================
ARTIST_CATALOG = [
    {"artist": "Jeantune", "author": "Jean C", "distributor": "Distrokid"},
    {"artist": "JCSTUDIO", "author": "Jean C", "distributor": "Distrokid"},
    {"artist": "JMAR", "author": "Jean C", "distributor": "Ditto"},
    {"artist": "YlegMoon", "author": "Angely", "distributor": "Distrokid"},
    {"artist": "Batytune", "author": "Angely", "distributor": "Distrokid"},
    {"artist": "Jzentrix", "author": "Dari", "distributor": "Distrokid"},
    {"artist": "JironPulse", "author": "Micha", "distributor": "Distrokid"},
    {"artist": "God Herd", "author": "Jean C", "distributor": "TuneCore"},
    {"artist": "JJ Legacy", "author": "Jean C", "distributor": "Symphonic"},
    {"artist": "Cielaurum", "author": "Angely", "distributor": "Ditto"},
    {"artist": "QuietMetric", "author": "Dari", "distributor": "Ditto"},
    {"artist": "AetherFocus", "author": "Jean C", "distributor": "Ditto"},
    {"artist": "ZukiPop", "author": "Jean C", "distributor": "Distrokid"},
    {"artist": "LexiGo", "author": "Jean C", "distributor": "Distrokid"},
    {"artist": "VYRONEX", "author": "Jean C", "distributor": "Distrokid"},
    {"artist": "AEROVIA", "author": "Jean C", "distributor": "Distrokid"},
]

ARTIST_LOOKUP = {item["artist"].lower(): item for item in ARTIST_CATALOG}
DISTRIBUTORS = sorted(list({item["distributor"] for item in ARTIST_CATALOG}))
COUNTRIES = ["EE", "UK", "CA", "MX", "ES", "DO", "CO", "AR", "CL", "PE", "BR"]

APP_ICONS = {
    "spotify": "🟢",
    "tidal": "♣️",
    "apple": "🍎",
    "apple music": "🍎"
}


def get_rate_for_app(app_name: str) -> float:
    if not app_name:
        return 0.0
    name = app_name.lower().strip()
    if name == "spotify":
        return 0.0035
    if name == "tidal":
        return 0.0065
    if name in ("apple", "apple music"):
        return 0.005
    return 0.0


def format_money(value):
    return f"${value:,.2f}"


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


# =========================
# DB INIT
# =========================
def init_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS teams (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        app_name VARCHAR(50) NOT NULL,
        lastfm_user VARCHAR(100) NOT NULL UNIQUE,
        country_code VARCHAR(5),
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
        country_code VARCHAR(5),
        artist VARCHAR(255),
        track VARCHAR(255),
        album VARCHAR(255),
        scrobbled_at TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)

    # Compatibilidad con esquemas viejos
    cur.execute("ALTER TABLE teams ADD COLUMN IF NOT EXISTS country_code VARCHAR(5);")
    cur.execute("ALTER TABLE scrobbles ADD COLUMN IF NOT EXISTS country_code VARCHAR(5);")

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
            UPDATE scrobbles SET artist = COALESCE(artist, artist_name) WHERE artist IS NULL;
            ALTER TABLE scrobbles ALTER COLUMN artist_name DROP NOT NULL;
        END IF;

        IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name='scrobbles' AND column_name='track_name'
        ) THEN
            UPDATE scrobbles SET track = COALESCE(track, track_name) WHERE track IS NULL;
            ALTER TABLE scrobbles ALTER COLUMN track_name DROP NOT NULL;
        END IF;

        IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name='scrobbles' AND column_name='album_name'
        ) THEN
            UPDATE scrobbles SET album = COALESCE(album, album_name) WHERE album IS NULL;
            ALTER TABLE scrobbles ALTER COLUMN album_name DROP NOT NULL;
        END IF;

        IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name='scrobbles' AND column_name='scrobble_time'
        ) THEN
            UPDATE scrobbles SET scrobbled_at = COALESCE(scrobbled_at, scrobble_time) WHERE scrobbled_at IS NULL;
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


# =========================
# UI / LAYOUT
# =========================
def build_subtitle(mode: str, app_filter: str, month_filter: str, country_filter: str, distributor_filter: str):
    parts = [mode]
    parts.append(f"app: {app_filter if app_filter != 'all' else 'todas'}")
    parts.append(f"mes: {month_filter if month_filter != 'all' else 'todos'}")
    parts.append(f"país: {country_filter if country_filter != 'all' else 'todos'}")
    parts.append(f"distribuidora: {distributor_filter if distributor_filter != 'all' else 'todas'}")
    return " • ".join(parts)


def render_layout(title, body_html, subtitle):
    html = f"""
    <!doctype html>
    <html>
    <head>
        <meta charset="utf-8">
        <title>{title}</title>
        <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
        <style>
            :root {{
                --bg: #f7f5ea;
                --bg2: #fefce8;
                --card: rgba(255,255,255,0.82);
                --text: #0f172a;
                --soft: #64748b;
                --gold: #d4a514;
                --gold2: #facc15;
                --line: rgba(15,23,42,0.08);
                --ok: #16a34a;
                --warn: #ca8a04;
                --incident: #dc2626;
                --shadow: 0 10px 26px rgba(15,23,42,0.07);
            }}

            * {{ box-sizing: border-box; }}
            body {{
                margin: 0;
                font-family: Inter, Arial, sans-serif;
                color: var(--text);
                background:
                    radial-gradient(circle at top left, rgba(250,204,21,0.16), transparent 22%),
                    linear-gradient(180deg, #faf8ee 0%, var(--bg2) 45%, #f4f0df 100%);
                padding: 22px;
            }}

            .shell {{
                max-width: 1620px;
                margin: 0 auto;
            }}

            .top {{
                display: flex;
                justify-content: space-between;
                align-items: center;
                gap: 16px;
                margin-bottom: 18px;
                flex-wrap: wrap;
            }}

            .brand {{
                display: flex;
                align-items: center;
                gap: 14px;
            }}

            .brand img {{
                width: 72px;
                height: 72px;
                border-radius: 20px;
                object-fit: cover;
                background: white;
                border: 1px solid rgba(212,165,20,0.18);
                box-shadow: 0 10px 22px rgba(212,165,20,0.10);
            }}

            .brand-title {{
                font-size: 28px;
                font-weight: 900;
                margin: 0;
                letter-spacing: -0.4px;
            }}

            .brand-sub {{
                font-size: 13px;
                color: var(--soft);
                margin-top: 4px;
            }}

            .badge {{
                display: inline-block;
                margin-top: 7px;
                padding: 6px 10px;
                border-radius: 999px;
                font-size: 12px;
                font-weight: 800;
                color: #8a6a00;
                background: rgba(250,204,21,0.14);
                border: 1px solid rgba(212,165,20,0.16);
            }}

            .nav {{
                display: flex;
                gap: 8px;
                flex-wrap: wrap;
            }}

            .nav a, .nav button, .btn {{
                border: none;
                text-decoration: none;
                cursor: pointer;
                border-radius: 12px;
                padding: 10px 14px;
                font-size: 13px;
                font-weight: 800;
                color: #0f172a;
                background: rgba(255,255,255,0.86);
                border: 1px solid rgba(212,165,20,0.15);
                box-shadow: 0 8px 20px rgba(15,23,42,0.05);
            }}

            .btn-gold {{
                background: linear-gradient(180deg, #f1cc39 0%, #ddb215 100%);
            }}
            .btn-red {{
                background: linear-gradient(180deg, #fb7185 0%, #f43f5e 100%);
                color: white;
            }}
            .btn-orange {{
                background: linear-gradient(180deg, #f6c14b 0%, #e8aa0f 100%);
            }}

            .card, .kpi, .compact, .chart-card, table {{
                background: var(--card);
                border: 1px solid rgba(212,165,20,0.12);
                box-shadow: var(--shadow);
                backdrop-filter: blur(6px);
            }}

            .grid4 {{
                display: grid;
                grid-template-columns: repeat(4, minmax(140px, 1fr));
                gap: 12px;
                margin-bottom: 14px;
            }}

            .kpi {{
                border-radius: 18px;
                padding: 16px;
                position: relative;
                overflow: hidden;
            }}

            .kpi::before {{
                content: "";
                position: absolute;
                inset: 0 0 auto 0;
                height: 4px;
                background: linear-gradient(90deg, rgba(212,165,20,0.85), rgba(250,204,21,0.25));
            }}

            .kpi.ok::before {{ background: linear-gradient(90deg, rgba(22,163,74,0.95), rgba(22,163,74,0.24)); }}
            .kpi.warn::before {{ background: linear-gradient(90deg, rgba(202,138,4,0.95), rgba(202,138,4,0.24)); }}
            .kpi.incident::before {{ background: linear-gradient(90deg, rgba(220,38,38,0.95), rgba(220,38,38,0.24)); }}

            .kpi-label {{
                color: var(--soft);
                font-size: 12px;
                font-weight: 700;
            }}
            .kpi-value {{
                font-size: 29px;
                font-weight: 900;
                margin-top: 8px;
            }}

            .layout4 {{
                display: grid;
                grid-template-columns: 220px 220px 220px 1fr;
                gap: 12px;
                margin-bottom: 14px;
                align-items: start;
            }}

            .compact {{
                border-radius: 18px;
                padding: 14px;
            }}

            .compact h3 {{
                margin: 0 0 8px 0;
                font-size: 15px;
            }}

            .hint {{
                color: var(--soft);
                font-size: 11px;
                margin-top: 4px;
            }}

            .summary-row {{
                display: flex;
                flex-wrap: wrap;
                gap: 8px;
                margin-top: 10px;
            }}

            .chip {{
                display: inline-flex;
                align-items: center;
                gap: 8px;
                padding: 8px 12px;
                border-radius: 999px;
                background: rgba(250,204,21,0.10);
                border: 1px solid rgba(212,165,20,0.15);
                color: #7b6208;
                font-size: 12px;
                font-weight: 800;
            }}

            input, textarea, select {{
                width: 100%;
                padding: 10px;
                border-radius: 12px;
                border: 1px solid rgba(15,23,42,0.08);
                background: rgba(255,255,255,0.9);
                color: #0f172a;
                font-size: 13px;
            }}

            textarea {{
                min-height: 80px;
                resize: vertical;
            }}

            .inline {{
                display: flex;
                gap: 8px;
                flex-wrap: wrap;
                align-items: center;
                margin-top: 8px;
            }}

            .section-title {{
                margin: 0 0 8px 0;
                font-size: 20px;
                letter-spacing: -0.2px;
            }}

            .section-sub {{
                color: var(--soft);
                font-size: 12px;
                margin-bottom: 12px;
            }}

            .chart-card, .card {{
                border-radius: 20px;
                padding: 16px;
                margin-bottom: 14px;
            }}

            .mini-grid {{
                display: grid;
                grid-template-columns: repeat(3, minmax(120px, 1fr));
                gap: 10px;
            }}

            .mini-card {{
                background: rgba(255,255,255,0.90);
                border-radius: 14px;
                padding: 12px;
                border: 1px solid rgba(212,165,20,0.10);
            }}

            .mini-icon {{
                font-size: 18px;
            }}

            .mini-label {{
                color: var(--soft);
                font-size: 11px;
                font-weight: 700;
                margin-top: 4px;
            }}

            .mini-value {{
                font-size: 22px;
                font-weight: 900;
                margin-top: 4px;
            }}

            table {{
                width: 100%;
                border-collapse: separate;
                border-spacing: 0;
                overflow: hidden;
                border-radius: 18px;
                margin-bottom: 14px;
            }}

            th, td {{
                padding: 11px 12px;
                border-bottom: 1px solid var(--line);
                text-align: left;
                font-size: 13px;
            }}

            th {{
                background: rgba(250,204,21,0.10);
                color: #5f6775;
                font-weight: 800;
            }}

            tr:last-child td {{
                border-bottom: none;
            }}

            .status {{
                display: inline-flex;
                align-items: center;
                gap: 8px;
                padding: 6px 10px;
                border-radius: 999px;
                font-size: 12px;
                font-weight: 900;
            }}
            .status.ok {{ background: rgba(22,163,74,0.10); color: var(--ok); }}
            .status.warn {{ background: rgba(202,138,4,0.10); color: var(--warn); }}
            .status.incident {{ background: rgba(220,38,38,0.10); color: var(--incident); }}

            .bars {{
                display: flex;
                flex-direction: column;
                gap: 12px;
            }}

            .bar-row {{
                display: grid;
                grid-template-columns: 180px 1fr 80px;
                gap: 12px;
                align-items: center;
            }}

            .bar-label {{
                font-size: 13px;
                font-weight: 700;
            }}

            .bar-track {{
                width: 100%;
                height: 14px;
                background: rgba(250,204,21,0.12);
                border-radius: 999px;
                overflow: hidden;
            }}

            .bar-fill {{
                height: 100%;
                background: linear-gradient(90deg, #d8b021 0%, #facc15 60%, #f4df8d 100%);
                border-radius: 999px;
            }}

            .bar-value {{
                text-align: right;
                font-weight: 800;
                color: #334155;
                font-size: 13px;
            }}

            #modal-overlay {{
                position: fixed;
                inset: 0;
                background: rgba(15,23,42,0.45);
                display: none;
                align-items: center;
                justify-content: center;
                z-index: 9999;
            }}

            #modal {{
                width: min(900px, 92vw);
                max-height: 85vh;
                overflow: auto;
                background: white;
                border-radius: 20px;
                padding: 20px;
                box-shadow: 0 20px 50px rgba(15,23,42,0.25);
            }}

            #modal pre {{
                white-space: pre-wrap;
                word-break: break-word;
                background: #0f172a;
                color: #f8fafc;
                padding: 14px;
                border-radius: 14px;
                overflow: auto;
            }}

            @media (max-width: 1280px) {{
                .grid4 {{
                    grid-template-columns: repeat(2, minmax(140px, 1fr));
                }}
                .mini-grid {{
                    grid-template-columns: 1fr;
                }}
                .layout4 {{
                    grid-template-columns: 1fr;
                }}
                .bar-row {{
                    grid-template-columns: 1fr;
                }}
            }}
        </style>
        <script>
            function openModal(title, content) {{
                document.getElementById("modal-title").innerText = title;
                document.getElementById("modal-body").innerHTML = content;
                document.getElementById("modal-overlay").style.display = "flex";
            }}

            function closeModal() {{
                document.getElementById("modal-overlay").style.display = "none";
            }}

            async function runAction(url, title) {{
                openModal(title, "<p>Ejecutando...</p>");
                try {{
                    const response = await fetch(url);
                    const text = await response.text();
                    document.getElementById("modal-body").innerHTML = "<pre>" + text.replace(/</g, "&lt;").replace(/>/g, "&gt;") + "</pre>";
                }} catch (e) {{
                    document.getElementById("modal-body").innerHTML = "<pre>Error: " + e + "</pre>";
                }}
            }}

            function deleteById() {{
                const id = document.getElementById("deleteId").value;
                if (!id) {{
                    alert("Pon un ID");
                    return;
                }}
                if (confirm("¿Seguro que quieres borrar el equipo con ID " + id + "?")) {{
                    window.location.href = "/delete-team?id=" + id;
                }}
            }}

            function resetAll() {{
                if (confirm("¿Seguro que quieres borrar TODOS los equipos, TODOS los scrobbles y reiniciar los IDs?")) {{
                    window.location.href = "/reset-teams";
                }}
            }}
        </script>
    </head>
    <body>
        <div class="shell">
            <div class="top">
                <div class="brand">
                    <img src="/static/logo_jatune.png" alt="JATune">
                    <div>
                        <div class="brand-title">{title}</div>
                        <div class="brand-sub">{subtitle}</div>
                        <div class="badge">JATune</div>
                    </div>
                </div>

                <div class="nav">
                    <a href="/">Monitor</a>
                    <a href="/analytics">Analytics</a>
                    <a href="/revenue">Revenue</a>
                    <button class="btn btn-gold" onclick="window.location.reload()">Refrescar</button>
                    <button class="btn btn-gold" onclick="runAction('/run-check', 'Resultado del chequeo')">Correr chequeo</button>
                    <button class="btn btn-gold" onclick="runAction('/run-collector', 'Resultado del collector')">Correr collector</button>
                    <button class="btn btn-red" onclick="resetAll()">Borrar todo</button>
                </div>
            </div>

            {body_html}
        </div>

        <div id="modal-overlay" onclick="closeModal()">
            <div id="modal" onclick="event.stopPropagation()">
                <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px;">
                    <h3 id="modal-title" style="margin:0;">Resultado</h3>
                    <button class="btn" onclick="closeModal()">Cerrar</button>
                </div>
                <div id="modal-body"></div>
            </div>
        </div>
    </body>
    </html>
    """
    return render_template_string(html)


# =========================
# HELPERS ANALYTICS
# =========================
def build_filters():
    app_filter = request.args.get("app", "all")
    month_filter = request.args.get("month", "all")
    country_filter = request.args.get("country", "all")
    distributor_filter = request.args.get("distributor", "all")
    return app_filter, month_filter, country_filter, distributor_filter


def sql_filters(app_filter, month_filter, country_filter):
    where = []
    params = []

    if app_filter != "all":
        where.append("app_name = %s")
        params.append(app_filter)

    if month_filter != "all":
        where.append("to_char(scrobbled_at, 'YYYY-MM') = %s")
        params.append(month_filter)

    if country_filter != "all":
        where.append("country_code = %s")
        params.append(country_filter)

    where_sql = ""
    if where:
        where_sql = " AND " + " AND ".join(where)

    return where_sql, params


def filter_catalog_by_distributor(rows, distributor_filter):
    if distributor_filter == "all":
        return rows
    return [r for r in rows if r["distributor"] == distributor_filter]


def rows_simple(items, cols, money_cols=None):
    money_cols = money_cols or []
    html = ""
    for item in items:
        tds = []
        for col in cols:
            value = item.get(col, "-")
            if col in money_cols:
                value = format_money(value or 0)
            tds.append(f"<td>{value}</td>")
        html += "<tr>" + "".join(tds) + "</tr>"
    return html


def top_artist_bars(items):
    if not items:
        return '<div class="hint">Sin datos</div>'
    max_val = max([x["plays"] for x in items], default=1)
    html = ['<div class="bars">']
    for item in items:
        pct = 0 if max_val == 0 else round((item["plays"] / max_val) * 100, 2)
        html.append(f"""
        <div class="bar-row">
            <div class="bar-label">{item['artist']}</div>
            <div class="bar-track"><div class="bar-fill" style="width:{pct}%;"></div></div>
            <div class="bar-value">{item['plays']}</div>
        </div>
        """)
    html.append("</div>")
    return "".join(html)


# =========================
# MONITOR
# =========================
@app.route("/")
def home():
    init_db()
    app_filter = request.args.get("app", "all")
    status_filter = request.args.get("status", "all")
    country_filter = request.args.get("country", "all")

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT DISTINCT app_name FROM teams WHERE active = TRUE ORDER BY app_name")
    apps = cur.fetchall()

    cur.execute("""
        SELECT DISTINCT COALESCE(country_code, '-') AS country_code
        FROM teams
        WHERE active = TRUE
        ORDER BY country_code
    """)
    countries_db = cur.fetchall()

    where = ["active = TRUE"]
    params = []

    if app_filter != "all":
        where.append("app_name = %s")
        params.append(app_filter)

    if status_filter != "all":
        where.append("status = %s")
        params.append(status_filter)

    if country_filter != "all":
        where.append("country_code = %s")
        params.append(country_filter)

    where_sql = " AND ".join(where)

    cur.execute(f"""
        SELECT
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE status = 'OK') AS ok_count,
            COUNT(*) FILTER (WHERE status = 'WARN') AS warn_count,
            COUNT(*) FILTER (WHERE status = 'INCIDENT') AS incident_count,
            COALESCE(AVG(idle_minutes),0) AS avg_idle,
            COALESCE(MAX(idle_minutes),0) AS max_idle
        FROM teams
        WHERE {where_sql}
    """, params)
    summary = cur.fetchone()

    cur.execute(f"""
        SELECT id, name, app_name, lastfm_user, country_code, status, idle_minutes
        FROM teams
        WHERE {where_sql}
        ORDER BY id ASC
    """, params)
    teams = cur.fetchall()

    cur.execute(f"""
        SELECT app_name, COUNT(*) AS total
        FROM teams
        WHERE {where_sql}
        GROUP BY app_name
        ORDER BY total DESC
        LIMIT 1
    """, params)
    dominant_app = cur.fetchone()

    cur.execute(f"""
        SELECT country_code, COUNT(*) AS total
        FROM teams
        WHERE {where_sql}
        GROUP BY country_code
        ORDER BY total DESC
        LIMIT 1
    """, params)
    dominant_country = cur.fetchone()

    cur.close()
    conn.close()

    app_options = '<option value="all">Todas</option>'
    for a in apps:
        selected = "selected" if a["app_name"] == app_filter else ""
        app_options += f'<option value="{a["app_name"]}" {selected}>{a["app_name"]}</option>'

    country_options = '<option value="all">Todos</option>'
    all_country_values = sorted(set([c["country_code"] for c in countries_db if c["country_code"]] + COUNTRIES))
    for c in all_country_values:
        selected = "selected" if c == country_filter else ""
        country_options += f'<option value="{c}" {selected}>{c}</option>'

    status_options = ""
    for opt in ["all", "OK", "WARN", "INCIDENT", "PENDING"]:
        label = "Todos" if opt == "all" else opt
        selected = "selected" if opt == status_filter else ""
        status_options += f'<option value="{opt}" {selected}>{label}</option>'

    rows = ""
    for t in teams:
        status = t["status"] or "PENDING"
        if status == "OK":
            badge = '<span class="status ok">● OK</span>'
        elif status == "WARN":
            badge = '<span class="status warn">● WARN</span>'
        elif status == "INCIDENT":
            badge = '<span class="status incident">● INCIDENT</span>'
        else:
            badge = f"<span>{status}</span>"

        rows += f"""
        <tr>
            <td>{t['id']}</td>
            <td>{t['name']}</td>
            <td>{t['app_name']}</td>
            <td>{t['lastfm_user']}</td>
            <td>{t['country_code'] or '-'}</td>
            <td>{badge}</td>
            <td>{t['idle_minutes']}</td>
        </tr>
        """

    body = f"""
    <div class="grid4">
        <div class="kpi">
            <div class="kpi-label">Equipos activos</div>
            <div class="kpi-value">{summary['total'] or 0}</div>
        </div>
        <div class="kpi ok">
            <div class="kpi-label">Semáforo OK</div>
            <div class="kpi-value">{summary['ok_count'] or 0}</div>
        </div>
        <div class="kpi warn">
            <div class="kpi-label">Semáforo WARN</div>
            <div class="kpi-value">{summary['warn_count'] or 0}</div>
        </div>
        <div class="kpi incident">
            <div class="kpi-label">Semáforo INCIDENT</div>
            <div class="kpi-value">{summary['incident_count'] or 0}</div>
        </div>
    </div>

    <div class="layout4">
        <div class="compact">
            <h3>Eliminar por ID</h3>
            <input id="deleteId" type="number" placeholder="ID a borrar">
            <div class="inline">
                <button class="btn btn-orange" onclick="deleteById()">Eliminar</button>
            </div>
        </div>

        <div class="compact">
            <h3>Filtros</h3>
            <form method="GET" action="/">
                <div class="hint">App</div>
                <select name="app">{app_options}</select>
                <div class="hint" style="margin-top:8px;">Estado</div>
                <select name="status">{status_options}</select>
                <div class="hint" style="margin-top:8px;">País</div>
                <select name="country">{country_options}</select>
                <div class="inline">
                    <button class="btn btn-gold" type="submit">Aplicar</button>
                </div>
            </form>
        </div>

        <div class="compact">
            <h3>Importar equipos reales</h3>
            <div class="hint">Formato: Nombre,App,UsuarioLastFM,Pais</div>
            <form method="POST" action="/import-real-teams">
                <textarea name="lines" placeholder="equipoT01,tidal,equipoS01,EE&#10;equipoG01,spotify,equipoG01,UK"></textarea>
                <div class="inline">
                    <button class="btn btn-gold" type="submit">Importar</button>
                </div>
            </form>
        </div>

        <div class="compact">
            <h3>Resumen productivo</h3>
            <div class="summary-row">
                <span class="chip">Promedio pausado: {int(summary['avg_idle'] or 0)} min</span>
                <span class="chip">Máximo pausado: {int(summary['max_idle'] or 0)} min</span>
                <span class="chip">App dominante: {dominant_app['app_name'] if dominant_app else '-'}</span>
                <span class="chip">País dominante: {dominant_country['country_code'] if dominant_country else '-'}</span>
            </div>
        </div>
    </div>

    <div class="card">
        <div class="section-title">Vista operativa</div>
        <div class="section-sub">Monitoreo central de equipos, apps, países y pausas de reproducción.</div>
    </div>

    <table>
        <thead>
            <tr>
                <th>ID</th>
                <th>Equipo</th>
                <th>App</th>
                <th>Usuario Last.fm</th>
                <th>País</th>
                <th>Estado</th>
                <th>Min pausado</th>
            </tr>
        </thead>
        <tbody>
            {rows if rows else '<tr><td colspan="7">No hay equipos cargados</td></tr>'}
        </tbody>
    </table>
    """

    subtitle = build_subtitle("Monitor operativo", app_filter, "all", country_filter, "all")
    return render_layout("WatchEagle ULTRA", body, subtitle)


# =========================
# ANALYTICS
# =========================
@app.route("/analytics")
def analytics():
    init_db()

    app_filter, month_filter, country_filter, distributor_filter = build_filters()
    where_sql, params = sql_filters(app_filter, month_filter, country_filter)

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT DISTINCT app_name FROM scrobbles WHERE app_name IS NOT NULL ORDER BY app_name")
    apps = cur.fetchall()

    cur.execute("""
        SELECT DISTINCT to_char(scrobbled_at, 'YYYY-MM') AS month_key
        FROM scrobbles
        WHERE scrobbled_at IS NOT NULL
        ORDER BY month_key DESC
    """)
    months = cur.fetchall()

    cur.execute("""
        SELECT DISTINCT COALESCE(country_code, '-') AS country_code
        FROM scrobbles
        WHERE country_code IS NOT NULL
        ORDER BY country_code
    """)
    countries_db = cur.fetchall()

    # KPIs
    cur.execute(f"SELECT COUNT(*) AS c FROM scrobbles WHERE DATE(scrobbled_at)=CURRENT_DATE {where_sql}", params)
    plays_today = cur.fetchone()["c"]

    cur.execute(f"SELECT COUNT(*) AS c FROM scrobbles WHERE DATE(scrobbled_at)=CURRENT_DATE - INTERVAL '1 day' {where_sql}", params)
    plays_yesterday = cur.fetchone()["c"]

    cur.execute(f"""
        SELECT COUNT(*) AS c
        FROM scrobbles
        WHERE date_trunc('month', scrobbled_at)=date_trunc('month', CURRENT_DATE)
        {where_sql}
    """, params)
    month_current_total = cur.fetchone()["c"]

    cur.execute(f"""
        SELECT COUNT(*) AS c
        FROM scrobbles
        WHERE date_trunc('month', scrobbled_at)=date_trunc('month', CURRENT_DATE - INTERVAL '1 month')
        {where_sql}
    """, params)
    month_previous_total = cur.fetchone()["c"]

    diff = plays_today - plays_yesterday
    month_diff = month_current_total - month_previous_total

    # Totales por app
    cur.execute(f"""
        SELECT
            COALESCE(SUM(CASE WHEN LOWER(app_name)='spotify' THEN 1 ELSE 0 END),0) AS spotify_total,
            COALESCE(SUM(CASE WHEN LOWER(app_name)='tidal' THEN 1 ELSE 0 END),0) AS tidal_total,
            COALESCE(SUM(CASE WHEN LOWER(app_name) IN ('apple','apple music') THEN 1 ELSE 0 END),0) AS apple_total
        FROM scrobbles
        WHERE 1=1 {where_sql}
    """, params)
    app_totals = cur.fetchone()

    # Tendencia diaria por app
    cur.execute(f"""
        SELECT to_char(DATE(scrobbled_at),'YYYY-MM-DD') AS day_label,
               LOWER(COALESCE(app_name,'unknown')) AS app_name,
               COUNT(*) AS plays
        FROM scrobbles
        WHERE 1=1 {where_sql}
        GROUP BY DATE(scrobbled_at), LOWER(COALESCE(app_name,'unknown'))
        ORDER BY DATE(scrobbled_at) ASC
    """, params)
    daily_by_app_raw = cur.fetchall()

    # Top artistas
    cur.execute(f"""
        SELECT COALESCE(artist,'-') AS artist, COUNT(*) AS plays
        FROM scrobbles
        WHERE 1=1 {where_sql}
        GROUP BY artist
        ORDER BY plays DESC
        LIMIT 10
    """, params)
    top_artists = cur.fetchall()

    # Top canciones 5
    cur.execute(f"""
        SELECT COALESCE(track,'-') AS track, COALESCE(artist,'-') AS artist, COUNT(*) AS plays
        FROM scrobbles
        WHERE 1=1 {where_sql}
        GROUP BY track, artist
        ORDER BY plays DESC
        LIMIT 5
    """, params)
    top_tracks = cur.fetchall()

    # Comparativa artistas
    cur.execute(f"""
        SELECT
            COALESCE(artist,'-') AS artist,
            SUM(CASE WHEN DATE(scrobbled_at)=CURRENT_DATE THEN 1 ELSE 0 END) AS hoy,
            SUM(CASE WHEN DATE(scrobbled_at)=CURRENT_DATE - INTERVAL '1 day' THEN 1 ELSE 0 END) AS ayer,
            SUM(CASE WHEN date_trunc('month', scrobbled_at)=date_trunc('month', CURRENT_DATE) THEN 1 ELSE 0 END) AS mes_actual,
            SUM(CASE WHEN date_trunc('month', scrobbled_at)=date_trunc('month', CURRENT_DATE - INTERVAL '1 month') THEN 1 ELSE 0 END) AS mes_anterior
        FROM scrobbles
        WHERE 1=1 {where_sql}
        GROUP BY artist
        ORDER BY mes_actual DESC, hoy DESC
        LIMIT 20
    """, params)
    compare_artists = cur.fetchall()

    # Plays por equipo
    cur.execute(f"""
        SELECT COALESCE(team_name,'-') AS team_name, COUNT(*) AS plays
        FROM scrobbles
        WHERE 1=1 {where_sql}
        GROUP BY team_name
        ORDER BY plays DESC
        LIMIT 15
    """, params)
    plays_by_team = cur.fetchall()

    # Catálogo
    cur.execute(f"""
        SELECT COALESCE(artist,'-') AS artist,
               LOWER(COALESCE(app_name,'')) AS app_name,
               COUNT(*) AS plays
        FROM scrobbles
        WHERE 1=1 {where_sql}
        GROUP BY artist, LOWER(COALESCE(app_name,''))
    """, params)
    catalog_raw = cur.fetchall()

    # Revenue por día
    cur.execute(f"""
        SELECT DATE(scrobbled_at) AS day_label,
               LOWER(COALESCE(app_name,'')) AS app_name,
               COUNT(*) AS plays
        FROM scrobbles
        WHERE 1=1 {where_sql}
        GROUP BY DATE(scrobbled_at), LOWER(COALESCE(app_name,''))
        ORDER BY DATE(scrobbled_at) DESC
    """, params)
    earnings_daily_raw = cur.fetchall()

    cur.close()
    conn.close()

    # series por app
    all_days = sorted(list({row["day_label"] for row in daily_by_app_raw}))
    app_series = {
        "spotify": [0] * len(all_days),
        "tidal": [0] * len(all_days),
        "apple": [0] * len(all_days),
    }
    day_index = {d: i for i, d in enumerate(all_days)}

    for row in daily_by_app_raw:
        day = row["day_label"]
        appn = row["app_name"]
        plays = row["plays"]
        if appn in app_series:
            app_series[appn][day_index[day]] = plays
        elif appn == "apple music":
            app_series["apple"][day_index[day]] = plays

    # artist bars
    max_artist = max([x["plays"] for x in top_artists], default=1)

    # catalog rows
    artist_map = defaultdict(lambda: {"plays": 0, "earnings": 0.0})
    for row in catalog_raw:
        artist_map[row["artist"].lower()]["plays"] += row["plays"]
        artist_map[row["artist"].lower()]["earnings"] += row["plays"] * get_rate_for_app(row["app_name"])

    catalog_rows = []
    for item in ARTIST_CATALOG:
        if distributor_filter != "all" and item["distributor"] != distributor_filter:
            continue
        stats = artist_map.get(item["artist"].lower(), {"plays": 0, "earnings": 0.0})
        catalog_rows.append({
            "artist": item["artist"],
            "author": item["author"],
            "distributor": item["distributor"],
            "plays": stats["plays"],
            "earnings": stats["earnings"]
        })
    catalog_rows = sorted(catalog_rows, key=lambda x: x["plays"], reverse=True)

    # earnings daily rows
    earnings_map = defaultdict(lambda: {"plays": 0, "earnings": 0.0})
    for row in earnings_daily_raw:
        day = str(row["day_label"])
        rate = get_rate_for_app(row["app_name"])
        earnings_map[day]["plays"] += row["plays"]
        earnings_map[day]["earnings"] += row["plays"] * rate

    daily_earnings_rows = []
    for day in sorted(earnings_map.keys(), reverse=True):
        daily_earnings_rows.append({
            "day": day,
            "plays": earnings_map[day]["plays"],
            "earnings": earnings_map[day]["earnings"]
        })

    current_context_earnings = sum(r["earnings"] for r in catalog_rows)

    # filter options
    app_options = '<option value="all">Todas</option>'
    for a in apps:
        selected = "selected" if a["app_name"] == app_filter else ""
        app_options += f'<option value="{a["app_name"]}" {selected}>{a["app_name"]}</option>'

    month_options = '<option value="all">Todos</option>'
    for m in months:
        selected = "selected" if m["month_key"] == month_filter else ""
        month_options += f'<option value="{m["month_key"]}" {selected}>{m["month_key"]}</option>'

    country_options = '<option value="all">Todos</option>'
    all_country_values = sorted(set([c["country_code"] for c in countries_db if c["country_code"]] + COUNTRIES))
    for c in all_country_values:
        selected = "selected" if c == country_filter else ""
        country_options += f'<option value="{c}" {selected}>{c}</option>'

    distributor_options = '<option value="all">Todas</option>'
    for d in DISTRIBUTORS:
        selected = "selected" if d == distributor_filter else ""
        distributor_options += f'<option value="{d}" {selected}>{d}</option>'

    body = f"""
    <div class="layout4">
        <div class="compact">
            <h3>Filtro app</h3>
            <form method="GET" action="/analytics">
                <select name="app">{app_options}</select>
                <input type="hidden" name="month" value="{month_filter}">
                <input type="hidden" name="country" value="{country_filter}">
                <input type="hidden" name="distributor" value="{distributor_filter}">
                <div class="inline"><button class="btn btn-gold" type="submit">Aplicar</button></div>
            </form>
        </div>

        <div class="compact">
            <h3>Filtro mes</h3>
            <form method="GET" action="/analytics">
                <select name="month">{month_options}</select>
                <input type="hidden" name="app" value="{app_filter}">
                <input type="hidden" name="country" value="{country_filter}">
                <input type="hidden" name="distributor" value="{distributor_filter}">
                <div class="inline"><button class="btn btn-gold" type="submit">Aplicar</button></div>
            </form>
        </div>

        <div class="compact">
            <h3>País / distribuidora</h3>
            <form method="GET" action="/analytics">
                <div class="hint">País</div>
                <select name="country">{country_options}</select>
                <div class="hint" style="margin-top:8px;">Distribuidora</div>
                <select name="distributor">{distributor_options}</select>
                <input type="hidden" name="app" value="{app_filter}">
                <input type="hidden" name="month" value="{month_filter}">
                <div class="inline"><button class="btn btn-gold" type="submit">Aplicar</button></div>
            </form>
        </div>

        <div class="compact">
            <h3>Contexto ejecutivo</h3>
            <div class="summary-row">
                <span class="chip">App: {app_filter if app_filter != 'all' else 'todas'}</span>
                <span class="chip">Mes: {month_filter if month_filter != 'all' else 'todos'}</span>
                <span class="chip">País: {country_filter if country_filter != 'all' else 'todos'}</span>
                <span class="chip">Distribuidora: {distributor_filter if distributor_filter != 'all' else 'todas'}</span>
                <span class="chip">Ganancias contexto: {format_money(current_context_earnings)}</span>
            </div>
        </div>
    </div>

    <div class="grid4">
        <div class="kpi">
            <div class="kpi-label">Plays hoy</div>
            <div class="kpi-value">{plays_today}</div>
        </div>
        <div class="kpi">
            <div class="kpi-label">Plays ayer / variación</div>
            <div class="kpi-value">{plays_yesterday} / {diff}</div>
        </div>
        <div class="kpi">
            <div class="kpi-label">Mes actual / mes anterior</div>
            <div class="kpi-value">{month_current_total} / {month_previous_total}</div>
        </div>
        <div class="kpi">
            <div class="kpi-label">Variación mensual</div>
            <div class="kpi-value">{month_diff}</div>
        </div>
    </div>

    <div class="card">
        <div class="section-title">Tarjetas por app</div>
        <div class="mini-grid">
            <div class="mini-card">
                <div class="mini-icon">🟢</div>
                <div class="mini-label">Spotify</div>
                <div class="mini-value">{app_totals['spotify_total']}</div>
            </div>
            <div class="mini-card">
                <div class="mini-icon">🔷</div>
                <div class="mini-label">Tidal</div>
                <div class="mini-value">{app_totals['tidal_total']}</div>
            </div>
            <div class="mini-card">
                <div class="mini-icon">🍎</div>
                <div class="mini-label">Apple</div>
                <div class="mini-value">{app_totals['apple_total']}</div>
            </div>
        </div>
    </div>

    <div class="chart-card">
        <div class="section-title">Tendencia diaria de reproducciones por app</div>
        <div class="section-sub">Pulso diario segmentado entre Spotify, Tidal y Apple.</div>
        <canvas id="dailyAppsChart" height="95"></canvas>
    </div>

    <div class="card">
        <div class="section-title">Top artistas</div>
        <div class="section-sub">Ranking visual consolidado.</div>
        {top_artist_bars(top_artists)}
    </div>

    <div class="card">
        <div class="section-title">Top 5 canciones</div>
        <table>
            <thead><tr><th>Canción</th><th>Artista</th><th>Plays</th></tr></thead>
            <tbody>{rows_simple(top_tracks, ['track', 'artist', 'plays']) if top_tracks else '<tr><td colspan="3">Sin datos</td></tr>'}</tbody>
        </table>
    </div>

    <div class="card">
        <div class="section-title">Comparativa artistas: hoy vs ayer + mes actual/anterior</div>
        <table>
            <thead>
                <tr>
                    <th>Artista</th>
                    <th>Hoy</th>
                    <th>Ayer</th>
                    <th>Mes actual</th>
                    <th>Mes anterior</th>
                </tr>
            </thead>
            <tbody>{rows_simple(compare_artists, ['artist', 'hoy', 'ayer', 'mes_actual', 'mes_anterior']) if compare_artists else '<tr><td colspan="5">Sin datos</td></tr>'}</tbody>
        </table>
    </div>

    <div class="card">
        <div class="section-title">Plays por equipo</div>
        <table>
            <thead><tr><th>Equipo</th><th>Plays</th></tr></thead>
            <tbody>{rows_simple(plays_by_team, ['team_name', 'plays']) if plays_by_team else '<tr><td colspan="2">Sin datos</td></tr>'}</tbody>
        </table>
    </div>

    <div class="card">
        <div class="section-title">Catálogo de artistas • plays y ganancias estimadas</div>
        <div class="section-sub">Spotify = 0.0035 • Tidal = 0.006</div>
        <table>
            <thead>
                <tr>
                    <th>Artista</th>
                    <th>Autor</th>
                    <th>Distribuidora</th>
                    <th>Total plays</th>
                    <th>Ganancias estimadas</th>
                </tr>
            </thead>
            <tbody>{rows_simple(catalog_rows, ['artist', 'author', 'distributor', 'plays', 'earnings'], money_cols=['earnings']) if catalog_rows else '<tr><td colspan="5">Sin datos</td></tr>'}</tbody>
        </table>
    </div>

    <div class="card">
        <div class="section-title">Ganancias por día</div>
        <table>
            <thead><tr><th>Día</th><th>Total plays</th><th>Ganancias estimadas</th></tr></thead>
            <tbody>{rows_simple(daily_earnings_rows, ['day', 'plays', 'earnings'], money_cols=['earnings']) if daily_earnings_rows else '<tr><td colspan="3">Sin datos</td></tr>'}</tbody>
        </table>
    </div>

    <script>
        const ctx = document.getElementById('dailyAppsChart').getContext('2d');
        new Chart(ctx, {{
            type: 'line',
            data: {{
                labels: {json.dumps(all_days)},
                datasets: [
                    {{
                        label: 'Spotify',
                        data: {json.dumps(app_series['spotify'])},
                        borderColor: '#1DB954',
                        backgroundColor: 'rgba(29,185,84,0.10)',
                        tension: 0.28,
                        fill: false
                    }},
                    {{
                        label: 'Tidal',
                        data: {json.dumps(app_series['tidal'])},
                        borderColor: '#2563EB',
                        backgroundColor: 'rgba(37,99,235,0.10)',
                        tension: 0.28,
                        fill: false
                    }},
                    {{
                        label: 'Apple',
                        data: {json.dumps(app_series['apple'])},
                        borderColor: '#A855F7',
                        backgroundColor: 'rgba(168,85,247,0.10)',
                        tension: 0.28,
                        fill: false
                    }}
                ]
            }},
            options: {{
                responsive: true,
                plugins: {{
                    legend: {{
                        labels: {{ color: '#475569' }}
                    }}
                }},
                scales: {{
                    x: {{
                        ticks: {{ color: '#64748b' }},
                        grid: {{ color: 'rgba(15,23,42,0.06)' }}
                    }},
                    y: {{
                        ticks: {{ color: '#64748b' }},
                        grid: {{ color: 'rgba(15,23,42,0.06)' }}
                    }}
                }}
            }}
        }});
    </script>
    """

    subtitle = build_subtitle("Analytics ejecutivo", app_filter, month_filter, country_filter, distributor_filter)
    return render_layout("WatchEagle ULTRA", body, subtitle)


# =========================
# REVENUE
# =========================
@app.route("/revenue")
def revenue():
    init_db()

    app_filter, month_filter, country_filter, distributor_filter = build_filters()
    where_sql, params = sql_filters(app_filter, month_filter, country_filter)

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT DISTINCT app_name FROM scrobbles WHERE app_name IS NOT NULL ORDER BY app_name")
    apps = cur.fetchall()

    cur.execute("""
        SELECT DISTINCT to_char(scrobbled_at, 'YYYY-MM') AS month_key
        FROM scrobbles
        WHERE scrobbled_at IS NOT NULL
        ORDER BY month_key DESC
    """)
    months = cur.fetchall()

    cur.execute("""
        SELECT DISTINCT COALESCE(country_code, '-') AS country_code
        FROM scrobbles
        WHERE country_code IS NOT NULL
        ORDER BY country_code
    """)
    countries_db = cur.fetchall()

    # raw for revenue
    cur.execute(f"""
        SELECT
            DATE(scrobbled_at) AS day_label,
            LOWER(COALESCE(app_name,'')) AS app_name,
            COUNT(*) AS plays
        FROM scrobbles
        WHERE 1=1 {where_sql}
        GROUP BY DATE(scrobbled_at), LOWER(COALESCE(app_name,''))
        ORDER BY DATE(scrobbled_at) ASC
    """, params)
    revenue_daily_raw = cur.fetchall()

    cur.execute(f"""
        SELECT
            COALESCE(country_code, '-') AS country_code,
            LOWER(COALESCE(app_name,'')) AS app_name,
            COUNT(*) AS plays
        FROM scrobbles
        WHERE 1=1 {where_sql}
        GROUP BY country_code, LOWER(COALESCE(app_name,''))
        ORDER BY country_code
    """, params)
    revenue_country_raw = cur.fetchall()

    cur.execute(f"""
        SELECT
            COALESCE(artist,'-') AS artist,
            LOWER(COALESCE(app_name,'')) AS app_name,
            COUNT(*) AS plays
        FROM scrobbles
        WHERE 1=1 {where_sql}
        GROUP BY artist, LOWER(COALESCE(app_name,''))
    """, params)
    revenue_artist_raw = cur.fetchall()

    cur.close()
    conn.close()

    # by day
    day_map = defaultdict(lambda: {"plays": 0, "revenue": 0.0})
    for row in revenue_daily_raw:
        day = str(row["day_label"])
        rate = get_rate_for_app(row["app_name"])
        plays = row["plays"]
        day_map[day]["plays"] += plays
        day_map[day]["revenue"] += plays * rate

    revenue_by_day = []
    for day in sorted(day_map.keys(), reverse=True):
        revenue_by_day.append({
            "day": day,
            "plays": day_map[day]["plays"],
            "revenue": day_map[day]["revenue"],
            "rpm": 0 if day_map[day]["plays"] == 0 else round((day_map[day]["revenue"] / day_map[day]["plays"]) * 1000, 2)
        })

    # by country
    country_map = defaultdict(lambda: {"plays": 0, "revenue": 0.0})
    for row in revenue_country_raw:
        country = row["country_code"]
        rate = get_rate_for_app(row["app_name"])
        plays = row["plays"]
        country_map[country]["plays"] += plays
        country_map[country]["revenue"] += plays * rate

    revenue_by_country = []
    for country, vals in country_map.items():
        revenue_by_country.append({
            "country": country,
            "plays": vals["plays"],
            "revenue": vals["revenue"],
            "rpm": 0 if vals["plays"] == 0 else round((vals["revenue"] / vals["plays"]) * 1000, 2)
        })
    revenue_by_country = sorted(revenue_by_country, key=lambda x: x["revenue"], reverse=True)

    # by artist / distributor
    artist_map = defaultdict(lambda: {"plays": 0, "revenue": 0.0})
    distributor_map = defaultdict(lambda: {"plays": 0, "revenue": 0.0})
    for row in revenue_artist_raw:
        artist = row["artist"]
        rate = get_rate_for_app(row["app_name"])
        plays = row["plays"]
        revenue = plays * rate

        artist_map[artist.lower()]["plays"] += plays
        artist_map[artist.lower()]["revenue"] += revenue

    revenue_by_artist = []
    for item in ARTIST_CATALOG:
        if distributor_filter != "all" and item["distributor"] != distributor_filter:
            continue
        stats = artist_map.get(item["artist"].lower(), {"plays": 0, "revenue": 0.0})
        revenue_by_artist.append({
            "artist": item["artist"],
            "author": item["author"],
            "distributor": item["distributor"],
            "plays": stats["plays"],
            "revenue": stats["revenue"],
            "rpm": 0 if stats["plays"] == 0 else round((stats["revenue"] / stats["plays"]) * 1000, 2)
        })
        distributor_map[item["distributor"]]["plays"] += stats["plays"]
        distributor_map[item["distributor"]]["revenue"] += stats["revenue"]

    revenue_by_artist = sorted(revenue_by_artist, key=lambda x: x["revenue"], reverse=True)

    revenue_by_distributor = []
    for dist, vals in distributor_map.items():
        revenue_by_distributor.append({
            "distributor": dist,
            "plays": vals["plays"],
            "revenue": vals["revenue"],
            "rpm": 0 if vals["plays"] == 0 else round((vals["revenue"] / vals["plays"]) * 1000, 2)
        })
    revenue_by_distributor = sorted(revenue_by_distributor, key=lambda x: x["revenue"], reverse=True)

    total_revenue = sum(x["revenue"] for x in revenue_by_artist)
    revenue_today = revenue_by_day[0]["revenue"] if revenue_by_day else 0.0
    revenue_month = total_revenue
    best_market = revenue_by_country[0]["country"] if revenue_by_country else "-"
    avg_rpm = 0 if sum(x["plays"] for x in revenue_by_artist) == 0 else round((total_revenue / sum(x["plays"] for x in revenue_by_artist)) * 1000, 2)

    country_labels = [x["country"] for x in revenue_by_country]
    country_revenues = [round(x["revenue"], 2) for x in revenue_by_country]

    day_labels = [x["day"] for x in reversed(revenue_by_day)]
    day_revenues = [round(x["revenue"], 2) for x in reversed(revenue_by_day)]

    # options
    app_options = '<option value="all">Todas</option>'
    for a in apps:
        selected = "selected" if a["app_name"] == app_filter else ""
        app_options += f'<option value="{a["app_name"]}" {selected}>{a["app_name"]}</option>'

    month_options = '<option value="all">Todos</option>'
    for m in months:
        selected = "selected" if m["month_key"] == month_filter else ""
        month_options += f'<option value="{m["month_key"]}" {selected}>{m["month_key"]}</option>'

    country_options = '<option value="all">Todos</option>'
    all_country_values = sorted(set([c["country_code"] for c in countries_db if c["country_code"]] + COUNTRIES))
    for c in all_country_values:
        selected = "selected" if c == country_filter else ""
        country_options += f'<option value="{c}" {selected}>{c}</option>'

    distributor_options = '<option value="all">Todas</option>'
    for d in DISTRIBUTORS:
        selected = "selected" if d == distributor_filter else ""
        distributor_options += f'<option value="{d}" {selected}>{d}</option>'

    body = f"""
    <div class="layout4">
        <div class="compact">
            <h3>Filtro app</h3>
            <form method="GET" action="/revenue">
                <select name="app">{app_options}</select>
                <input type="hidden" name="month" value="{month_filter}">
                <input type="hidden" name="country" value="{country_filter}">
                <input type="hidden" name="distributor" value="{distributor_filter}">
                <div class="inline"><button class="btn btn-gold" type="submit">Aplicar</button></div>
            </form>
        </div>

        <div class="compact">
            <h3>Filtro mes</h3>
            <form method="GET" action="/revenue">
                <select name="month">{month_options}</select>
                <input type="hidden" name="app" value="{app_filter}">
                <input type="hidden" name="country" value="{country_filter}">
                <input type="hidden" name="distributor" value="{distributor_filter}">
                <div class="inline"><button class="btn btn-gold" type="submit">Aplicar</button></div>
            </form>
        </div>

        <div class="compact">
            <h3>País / distribuidora</h3>
            <form method="GET" action="/revenue">
                <div class="hint">País</div>
                <select name="country">{country_options}</select>
                <div class="hint" style="margin-top:8px;">Distribuidora</div>
                <select name="distributor">{distributor_options}</select>
                <input type="hidden" name="app" value="{app_filter}">
                <input type="hidden" name="month" value="{month_filter}">
                <div class="inline"><button class="btn btn-gold" type="submit">Aplicar</button></div>
            </form>
        </div>

        <div class="compact">
            <h3>Insight ejecutivo</h3>
            <div class="summary-row">
                <span class="chip">Revenue actual: {format_money(total_revenue)}</span>
                <span class="chip">Mejor mercado: {best_market}</span>
                <span class="chip">RPM promedio: {avg_rpm}</span>
            </div>
        </div>
    </div>

    <div class="grid4">
        <div class="kpi">
            <div class="kpi-label">Revenue hoy</div>
            <div class="kpi-value">{format_money(revenue_today)}</div>
        </div>
        <div class="kpi">
            <div class="kpi-label">Revenue del contexto</div>
            <div class="kpi-value">{format_money(revenue_month)}</div>
        </div>
        <div class="kpi">
            <div class="kpi-label">RPM promedio</div>
            <div class="kpi-value">{avg_rpm}</div>
        </div>
        <div class="kpi">
            <div class="kpi-label">Mercado top</div>
            <div class="kpi-value">{best_market}</div>
        </div>
    </div>

    <div class="chart-card">
        <div class="section-title">Revenue por país</div>
        <canvas id="countryRevenueChart" height="95"></canvas>
    </div>

    <div class="chart-card">
        <div class="section-title">Revenue trend por día</div>
        <canvas id="dailyRevenueChart" height="95"></canvas>
    </div>

    <div class="card">
        <div class="section-title">Ranking de países</div>
        <table>
            <thead><tr><th>País</th><th>Plays</th><th>Revenue</th><th>RPM</th></tr></thead>
            <tbody>{rows_simple(revenue_by_country, ['country', 'plays', 'revenue', 'rpm'], money_cols=['revenue']) if revenue_by_country else '<tr><td colspan="4">Sin datos</td></tr>'}</tbody>
        </table>
    </div>

    <div class="card">
        <div class="section-title">Ranking de artistas</div>
        <table>
            <thead><tr><th>Artista</th><th>Autor</th><th>Distribuidora</th><th>Plays</th><th>Revenue</th><th>RPM</th></tr></thead>
            <tbody>{rows_simple(revenue_by_artist, ['artist', 'author', 'distributor', 'plays', 'revenue', 'rpm'], money_cols=['revenue']) if revenue_by_artist else '<tr><td colspan="6">Sin datos</td></tr>'}</tbody>
        </table>
    </div>

    <div class="card">
        <div class="section-title">Ranking de distribuidoras</div>
        <table>
            <thead><tr><th>Distribuidora</th><th>Plays</th><th>Revenue</th><th>RPM</th></tr></thead>
            <tbody>{rows_simple(revenue_by_distributor, ['distributor', 'plays', 'revenue', 'rpm'], money_cols=['revenue']) if revenue_by_distributor else '<tr><td colspan="4">Sin datos</td></tr>'}</tbody>
        </table>
    </div>

    <div class="card">
        <div class="section-title">Ganancias por día</div>
        <table>
            <thead><tr><th>Día</th><th>Plays</th><th>Revenue</th><th>RPM</th></tr></thead>
            <tbody>{rows_simple(revenue_by_day, ['day', 'plays', 'revenue', 'rpm'], money_cols=['revenue']) if revenue_by_day else '<tr><td colspan="4">Sin datos</td></tr>'}</tbody>
        </table>
    </div>

    <script>
        new Chart(document.getElementById('countryRevenueChart').getContext('2d'), {{
            type: 'bar',
            data: {{
                labels: {json.dumps(country_labels)},
                datasets: [{{
                    label: 'Revenue',
                    data: {json.dumps(country_revenues)},
                    backgroundColor: 'rgba(212,165,20,0.72)',
                    borderColor: '#d4a514',
                    borderWidth: 1
                }}]
            }},
            options: {{
                responsive: true,
                plugins: {{ legend: {{ labels: {{ color: '#475569' }} }} }},
                scales: {{
                    x: {{ ticks: {{ color: '#64748b' }}, grid: {{ color: 'rgba(15,23,42,0.06)' }} }},
                    y: {{ ticks: {{ color: '#64748b' }}, grid: {{ color: 'rgba(15,23,42,0.06)' }} }}
                }}
            }}
        }});

        new Chart(document.getElementById('dailyRevenueChart').getContext('2d'), {{
            type: 'line',
            data: {{
                labels: {json.dumps(day_labels)},
                datasets: [{{
                    label: 'Revenue por día',
                    data: {json.dumps(day_revenues)},
                    borderColor: '#d4a514',
                    backgroundColor: 'rgba(212,165,20,0.12)',
                    tension: 0.28,
                    fill: true
                }}]
            }},
            options: {{
                responsive: true,
                plugins: {{ legend: {{ labels: {{ color: '#475569' }} }} }},
                scales: {{
                    x: {{ ticks: {{ color: '#64748b' }}, grid: {{ color: 'rgba(15,23,42,0.06)' }} }},
                    y: {{ ticks: {{ color: '#64748b' }}, grid: {{ color: 'rgba(15,23,42,0.06)' }} }}
                }}
            }}
        }});
    </script>
    """

    subtitle = build_subtitle("Revenue intelligence", app_filter, month_filter, country_filter, distributor_filter)
    return render_layout("WatchEagle ULTRA", body, subtitle)


# =========================
# EQUIPOS
# =========================
@app.route("/import-real-teams", methods=["POST"])
def import_real_teams():
    init_db()

    text = request.form.get("lines", "").strip()
    if not text:
        return "No se recibió contenido", 400

    lines = [x.strip() for x in text.splitlines() if x.strip()]

    conn = get_conn()
    cur = conn.cursor()

    created = []
    skipped = []

    for line in lines:
        parts = [p.strip() for p in line.split(",")]
        if len(parts) not in (3, 4):
            skipped.append({"line": line, "reason": "Formato inválido. Usa nombre,app,user,pais"})
            continue

        team_name = parts[0]
        app_name = parts[1]
        lastfm_user = parts[2]
        country_code = parts[3].upper() if len(parts) == 4 else None

        if not lastfm_user_exists(lastfm_user):
            skipped.append({"line": line, "reason": "Usuario Last.fm no existe"})
            continue

        cur.execute("""
            INSERT INTO teams (name, app_name, lastfm_user, country_code, status)
            VALUES (%s, %s, %s, %s, 'PENDING')
            ON CONFLICT (lastfm_user) DO NOTHING
            RETURNING id, name, app_name, lastfm_user, country_code
        """, (team_name, app_name, lastfm_user, country_code))
        row = cur.fetchone()

        if row:
            created.append(row)
        else:
            skipped.append({"line": line, "reason": "Ya existía en base"})

    conn.commit()
    cur.close()
    conn.close()

    return f"Creados: {len(created)}\nOmitidos: {len(skipped)}\n\nCreados:\n{created}\n\nOmitidos:\n{skipped}"


@app.route("/delete-team")
def delete_team():
    init_db()
    team_id = request.args.get("id")

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM teams WHERE id=%s", (team_id,))
    conn.commit()
    cur.close()
    conn.close()
    return redirect("/")


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


# =========================
# OPERACIÓN
# =========================
@app.route("/run-check")
def run_check():
    try:
        result = subprocess.run(
            ["python", "watch_scrobbles.py"],
            capture_output=True,
            text=True,
            timeout=600
        )
        output = f"STDOUT:\n{result.stdout}\n\nSTDERR:\n{result.stderr}"
        return output, 200, {"Content-Type": "text/plain; charset=utf-8"}
    except Exception as e:
        return f"Error ejecutando watch_scrobbles.py:\n{str(e)}", 500, {"Content-Type": "text/plain; charset=utf-8"}


@app.route("/run-collector")
def run_collector():
    try:
        result = subprocess.run(
            ["python", "collect_scrobbles.py"],
            capture_output=True,
            text=True,
            timeout=1800
        )
        output = f"STDOUT:\n{result.stdout}\n\nSTDERR:\n{result.stderr}"
        return output, 200, {"Content-Type": "text/plain; charset=utf-8"}
    except Exception as e:
        return f"Error ejecutando collect_scrobbles.py:\n{str(e)}", 500, {"Content-Type": "text/plain; charset=utf-8"}


@app.route("/health")
def health():
    init_db()
    return jsonify({"ok": True, "service": "WatchEagle ULTRA"})


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


if __name__ == "__main__":
    init_db()
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)

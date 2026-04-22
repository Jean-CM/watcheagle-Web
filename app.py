from flask import Flask, request, jsonify
import subprocess
from helpers import get_conn, init_db

app = Flask(__name__)

PLATFORM_RATES = {
    "spotify": {"min": 0.0030, "max": 0.0050},
    "apple": {"min": 0.0070, "max": 0.0100},
    "apple music": {"min": 0.0070, "max": 0.0100},
    "tidal": {"min": 0.0120, "max": 0.0150},
    "youtube": {"min": 0.0007, "max": 0.0020},
    "youtube music": {"min": 0.0007, "max": 0.0020},
}


def safe_int(value, default=0):
    try:
        return int(value or 0)
    except Exception:
        return default


def safe_float(value, default=0.0):
    try:
        return float(value or 0)
    except Exception:
        return default


def format_money(value):
    return f"${value:,.2f}"


def menu_link(label, view_name, current_view):
    active = "menu-link active" if current_view == view_name else "menu-link"
    return f'<a class="{active}" href="/?view={view_name}">{label}</a>'


def base_page(title, current_view, body_html):
    nav = (
        menu_link("Monitor", "monitor", current_view)
        + menu_link("Analisis", "analisis", current_view)
        + menu_link("Ganancias promedios", "ganancias", current_view)
        + menu_link("Monitor Plays", "monitor-plays", current_view)
    )

    utilities = """
    <div class="utilities">
        <a href="/ping">ping</a>
        <a href="/healthz">healthz</a>
        <a href="/run-check">run-check</a>
        <a href="/collect-now">collect-now</a>
        <a href="/scrobbles-count">scrobbles-count</a>
        <a href="/fix-job-runs">fix-job-runs</a>
    </div>
    """

    return f"""
    <html>
    <head>
        <title>WatchEagle</title>
        <style>
            * {{
                box-sizing: border-box;
            }}
            body {{
                margin: 0;
                font-family: Arial, sans-serif;
                background: #071226;
                color: #e5e7eb;
                padding: 24px;
            }}
            .page {{
                max-width: 1600px;
                margin: 0 auto;
            }}
            .header {{
                margin-bottom: 18px;
            }}
            .header h1 {{
                margin: 0;
                font-size: 30px;
                font-weight: 800;
            }}
            .sub {{
                color: #94a3b8;
                margin-top: 6px;
            }}
            .menu {{
                display: flex;
                flex-wrap: wrap;
                gap: 10px;
                margin: 18px 0 14px 0;
            }}
            .menu-link {{
                text-decoration: none;
                color: #c084fc;
                background: rgba(192,132,252,.08);
                border: 1px solid rgba(192,132,252,.35);
                padding: 10px 14px;
                border-radius: 10px;
                font-size: 14px;
                font-weight: 700;
            }}
            .menu-link.active {{
                background: rgba(59,130,246,.14);
                color: #93c5fd;
                border-color: rgba(147,197,253,.5);
            }}
            .utilities {{
                display: flex;
                flex-wrap: wrap;
                gap: 10px;
                margin-bottom: 20px;
            }}
            .utilities a {{
                text-decoration: none;
                color: #a5b4fc;
                background: rgba(165,180,252,.08);
                border: 1px solid rgba(165,180,252,.25);
                padding: 8px 12px;
                border-radius: 10px;
                font-size: 13px;
            }}
            .grid {{
                display: grid;
                grid-template-columns: repeat(4, 1fr);
                gap: 14px;
                margin-bottom: 18px;
            }}
            .grid-3 {{
                display: grid;
                grid-template-columns: repeat(3, 1fr);
                gap: 14px;
                margin-bottom: 18px;
            }}
            .grid-2 {{
                display: grid;
                grid-template-columns: repeat(2, 1fr);
                gap: 18px;
                margin-bottom: 18px;
            }}
            .card {{
                background: #0f1b33;
                border: 1px solid #1e293b;
                border-radius: 16px;
                padding: 18px;
                box-shadow: 0 8px 24px rgba(0,0,0,.18);
            }}
            .metric-label {{
                color: #94a3b8;
                font-size: 13px;
                margin-bottom: 10px;
            }}
            .metric-value {{
                font-size: 30px;
                font-weight: 800;
            }}
            .ok {{
                color: #22c55e;
            }}
            .warn {{
                color: #f59e0b;
            }}
            .incident {{
                color: #ef4444;
            }}
            .pending {{
                color: #94a3b8;
            }}
            .section-title {{
                margin: 0 0 12px 0;
                font-size: 20px;
                font-weight: 700;
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
                background: #0f1b33;
                border: 1px solid #1e293b;
                border-radius: 16px;
                overflow: hidden;
            }}
            th {{
                background: #1a2740;
                color: #f8fafc;
                font-weight: 700;
                font-size: 14px;
                padding: 14px 12px;
                text-align: left;
            }}
            td {{
                padding: 13px 12px;
                border-top: 1px solid #1e293b;
                font-size: 14px;
            }}
            tr:hover td {{
                background: rgba(255,255,255,.02);
            }}
            .badge {{
                display: inline-block;
                padding: 5px 10px;
                border-radius: 999px;
                font-size: 12px;
                font-weight: 700;
                letter-spacing: .3px;
            }}
            .muted {{
                color: #94a3b8;
            }}
            .mini-row {{
                display: flex;
                justify-content: space-between;
                align-items: center;
                padding: 10px 0;
                border-bottom: 1px solid #1e293b;
                gap: 12px;
            }}
            .mini-row:last-child {{
                border-bottom: 0;
            }}
            .tiny {{
                font-size: 12px;
                color: #94a3b8;
            }}
            @media (max-width: 1200px) {{
                .grid, .grid-3, .grid-2 {{
                    grid-template-columns: 1fr 1fr;
                }}
            }}
            @media (max-width: 760px) {{
                .grid, .grid-3, .grid-2 {{
                    grid-template-columns: 1fr;
                }}
                body {{
                    padding: 14px;
                }}
            }}
        </style>
    </head>
    <body>
        <div class="page">
            <div class="header">
                <h1>WatchEagle</h1>
                <div class="sub">{title}</div>
            </div>
            <div class="menu">{nav}</div>
            {utilities}
            {body_html}
        </div>
    </body>
    </html>
    """


def render_monitor(cur):
    cur.execute("""
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN status = 'OK' THEN 1 ELSE 0 END) AS ok_count,
            SUM(CASE WHEN status = 'WARN' THEN 1 ELSE 0 END) AS warn_count,
            SUM(CASE WHEN status = 'INCIDENT' THEN 1 ELSE 0 END) AS incident_count,
            SUM(CASE WHEN status = 'PENDING' THEN 1 ELSE 0 END) AS pending_count
        FROM teams
        WHERE active = TRUE
    """)
    summary = cur.fetchone()

    total = safe_int(summary["total"])
    ok_count = safe_int(summary["ok_count"])
    warn_count = safe_int(summary["warn_count"])
    incident_count = safe_int(summary["incident_count"])

    cur.execute("""
        SELECT COUNT(*) AS total_today
        FROM scrobbles
        WHERE DATE(scrobble_time) = CURRENT_DATE
    """)
    total_today = safe_int(cur.fetchone()["total_today"])

    cur.execute("""
        SELECT
            id,
            name,
            app_name,
            lastfm_user,
            status,
            last_scrobble_at,
            idle_minutes,
            last_check_at
        FROM teams
        WHERE active = TRUE
        ORDER BY id ASC
    """)
    teams = cur.fetchall()

    rows = ""
    for t in teams:
        estado = t["status"] or "PENDING"

        if estado == "OK":
            badge_color = "#22c55e"
            badge_bg = "rgba(34,197,94,.15)"
        elif estado == "WARN":
            badge_color = "#f59e0b"
            badge_bg = "rgba(245,158,11,.15)"
        elif estado == "INCIDENT":
            badge_color = "#ef4444"
            badge_bg = "rgba(239,68,68,.15)"
        else:
            badge_color = "#94a3b8"
            badge_bg = "rgba(148,163,184,.15)"

        rows += f"""
        <tr>
            <td>{t['id']}</td>
            <td>{t['name']}</td>
            <td>{t['app_name']}</td>
            <td>{t['lastfm_user']}</td>
            <td><span class="badge" style="color:{badge_color};background:{badge_bg};border:1px solid {badge_color};">{estado}</span></td>
            <td>{t['last_scrobble_at'] or '-'}</td>
            <td>{t['idle_minutes'] if t['idle_minutes'] is not None else '-'}</td>
            <td>{t['last_check_at'] or '-'}</td>
        </tr>
        """

    if not rows:
        rows = '<tr><td colspan="8" class="muted" style="text-align:center;">No hay equipos cargados todavía.</td></tr>'

    return f"""
    <div class="grid">
        <div class="card"><div class="metric-label">Monitores activos</div><div class="metric-value">{total}</div></div>
        <div class="card"><div class="metric-label">OK</div><div class="metric-value ok">{ok_count}</div></div>
        <div class="card"><div class="metric-label">WARN</div><div class="metric-value warn">{warn_count}</div></div>
        <div class="card"><div class="metric-label">INCIDENT</div><div class="metric-value incident">{incident_count}</div></div>
    </div>

    <div class="card" style="margin-bottom:18px;">
        <div class="metric-label">Scrobbles hoy</div>
        <div class="metric-value">{total_today}</div>
    </div>

    <div class="section-title">Estado de equipos</div>
    <table>
        <thead>
            <tr>
                <th>ID</th>
                <th>Equipo</th>
                <th>App</th>
                <th>User</th>
                <th>Status</th>
                <th>Ultimo scrobble</th>
                <th>Idle</th>
                <th>Ultimo check</th>
            </tr>
        </thead>
        <tbody>
            {rows}
        </tbody>
    </table>
    """


def render_analisis(cur):
    cur.execute("""
        SELECT COUNT(*) AS plays_today
        FROM scrobbles
        WHERE DATE(scrobble_time) = CURRENT_DATE
    """)
    plays_today = safe_int(cur.fetchone()["plays_today"])

    cur.execute("""
        SELECT COUNT(*) AS plays_month
        FROM scrobbles
        WHERE DATE_TRUNC('month', scrobble_time) = DATE_TRUNC('month', CURRENT_DATE)
    """)
    plays_month = safe_int(cur.fetchone()["plays_month"])

    cur.execute("""
        SELECT artist_name, COUNT(*) AS plays
        FROM scrobbles
        WHERE DATE(scrobble_time) = CURRENT_DATE
        GROUP BY artist_name
        ORDER BY plays DESC, artist_name ASC
        LIMIT 10
    """)
    top_artists = cur.fetchall()

    cur.execute("""
        SELECT track_name, artist_name, COUNT(*) AS plays
        FROM scrobbles
        WHERE DATE(scrobble_time) = CURRENT_DATE
        GROUP BY track_name, artist_name
        ORDER BY plays DESC, artist_name ASC, track_name ASC
        LIMIT 10
    """)
    top_tracks = cur.fetchall()

    cur.execute("""
        SELECT
            DATE(scrobble_time) AS day,
            COUNT(*) AS plays
        FROM scrobbles
        GROUP BY DATE(scrobble_time)
        ORDER BY day DESC
        LIMIT 7
    """)
    recent_days = cur.fetchall()

    cur.execute("""
        SELECT
            artist_name,
            SUM(CASE WHEN DATE(scrobble_time) = CURRENT_DATE THEN 1 ELSE 0 END) AS today_plays,
            SUM(CASE WHEN DATE(scrobble_time) = CURRENT_DATE - INTERVAL '1 day' THEN 1 ELSE 0 END) AS yesterday_plays
        FROM scrobbles
        GROUP BY artist_name
        HAVING
            SUM(CASE WHEN DATE(scrobble_time) = CURRENT_DATE THEN 1 ELSE 0 END) > 0
            OR
            SUM(CASE WHEN DATE(scrobble_time) = CURRENT_DATE - INTERVAL '1 day' THEN 1 ELSE 0 END) > 0
        ORDER BY today_plays DESC, yesterday_plays DESC
        LIMIT 10
    """)
    versus_rows = cur.fetchall()

    artists_html = "".join([
        f'<div class="mini-row"><span>{r["artist_name"]}</span><strong>{r["plays"]}</strong></div>'
        for r in top_artists
    ]) or '<div class="muted">Sin datos todavía.</div>'

    tracks_html = "".join([
        f'<div class="mini-row"><span>{r["artist_name"]} - {r["track_name"]}</span><strong>{r["plays"]}</strong></div>'
        for r in top_tracks
    ]) or '<div class="muted">Sin datos todavía.</div>'

    day_rows = ""
    for r in recent_days:
        day_rows += f"""
        <tr>
            <td>{r['day']}</td>
            <td>{r['plays']}</td>
        </tr>
        """
    if not day_rows:
        day_rows = '<tr><td colspan="2" class="muted" style="text-align:center;">Sin datos.</td></tr>'

    versus_html = ""
    for r in versus_rows:
        delta = safe_int(r["today_plays"]) - safe_int(r["yesterday_plays"])
        delta_color = "#22c55e" if delta >= 0 else "#ef4444"
        versus_html += f"""
        <tr>
            <td>{r['artist_name']}</td>
            <td>{r['today_plays']}</td>
            <td>{r['yesterday_plays']}</td>
            <td style="color:{delta_color};font-weight:700;">{delta}</td>
        </tr>
        """
    if not versus_html:
        versus_html = '<tr><td colspan="4" class="muted" style="text-align:center;">Sin datos.</td></tr>'

    return f"""
    <div class="grid">
        <div class="card"><div class="metric-label">Plays hoy</div><div class="metric-value">{plays_today}</div></div>
        <div class="card"><div class="metric-label">Plays del mes</div><div class="metric-value">{plays_month}</div></div>
        <div class="card"><div class="metric-label">Top artistas hoy</div><div class="metric-value">{len(top_artists)}</div></div>
        <div class="card"><div class="metric-label">Top canciones hoy</div><div class="metric-value">{len(top_tracks)}</div></div>
    </div>

    <div class="grid-2">
        <div class="card">
            <div class="section-title">Top artistas hoy</div>
            {artists_html}
        </div>

        <div class="card">
            <div class="section-title">Top canciones hoy</div>
            {tracks_html}
        </div>
    </div>

    <div class="grid-2">
        <div>
            <div class="section-title">Plays por dia (ultimos 7 dias)</div>
            <table>
                <thead><tr><th>Dia</th><th>Plays</th></tr></thead>
                <tbody>{day_rows}</tbody>
            </table>
        </div>

        <div>
            <div class="section-title">Artistas hoy vs ayer</div>
            <table>
                <thead><tr><th>Artista</th><th>Hoy</th><th>Ayer</th><th>Delta</th></tr></thead>
                <tbody>{versus_html}</tbody>
            </table>
        </div>
    </div>
    """


def render_ganancias(cur):
    cur.execute("""
        SELECT
            LOWER(app_name) AS platform,
            COUNT(*) AS plays
        FROM scrobbles
        WHERE DATE_TRUNC('month', scrobble_time) = DATE_TRUNC('month', CURRENT_DATE)
        GROUP BY LOWER(app_name)
        ORDER BY plays DESC
    """)
    platform_rows = cur.fetchall()

    total_min = 0.0
    total_max = 0.0
    table_rows = ""

    for r in platform_rows:
        platform = (r["platform"] or "").strip().lower()
        plays = safe_int(r["plays"])

        rate = PLATFORM_RATES.get(platform)
        if not rate:
            rate = PLATFORM_RATES.get("spotify")

        est_min = plays * rate["min"]
        est_max = plays * rate["max"]

        total_min += est_min
        total_max += est_max

        avg_rate = (rate["min"] + rate["max"]) / 2
        avg_revenue = plays * avg_rate

        table_rows += f"""
        <tr>
            <td>{platform.title()}</td>
            <td>{plays}</td>
            <td>{format_money(rate['min'])}</td>
            <td>{format_money(rate['max'])}</td>
            <td>{format_money(est_min)}</td>
            <td>{format_money(est_max)}</td>
            <td>{format_money(avg_revenue)}</td>
        </tr>
        """

    if not table_rows:
        table_rows = '<tr><td colspan="7" class="muted" style="text-align:center;">Sin datos para calcular ganancias.</td></tr>'

    cur.execute("""
        SELECT
            team_name,
            COUNT(*) AS plays
        FROM scrobbles
        WHERE DATE_TRUNC('month', scrobble_time) = DATE_TRUNC('month', CURRENT_DATE)
        GROUP BY team_name
        ORDER BY plays DESC
        LIMIT 10
    """)
    top_teams = cur.fetchall()

    team_html = "".join([
        f'<div class="mini-row"><span>{r["team_name"]}</span><strong>{r["plays"]}</strong></div>'
        for r in top_teams
    ]) or '<div class="muted">Sin datos todavía.</div>'

    return f"""
    <div class="grid-3">
        <div class="card">
            <div class="metric-label">Ganancia minima estimada del mes</div>
            <div class="metric-value">{format_money(total_min)}</div>
        </div>
        <div class="card">
            <div class="metric-label">Ganancia maxima estimada del mes</div>
            <div class="metric-value">{format_money(total_max)}</div>
        </div>
        <div class="card">
            <div class="metric-label">Promedio estimado del rango</div>
            <div class="metric-value">{format_money((total_min + total_max) / 2)}</div>
        </div>
    </div>

    <div class="grid-2">
        <div>
            <div class="section-title">Ganancias estimadas por plataforma</div>
            <table>
                <thead>
                    <tr>
                        <th>Plataforma</th>
                        <th>Streams</th>
                        <th>Pago min / stream</th>
                        <th>Pago max / stream</th>
                        <th>Min estimado</th>
                        <th>Max estimado</th>
                        <th>Promedio estimado</th>
                    </tr>
                </thead>
                <tbody>
                    {table_rows}
                </tbody>
            </table>
        </div>

        <div class="card">
            <div class="section-title">Promedios por equipo (mes)</div>
            {team_html}
            <div class="tiny" style="margin-top:14px;">
                Basado en rangos estimados que definiste por plataforma.
            </div>
        </div>
    </div>
    """


def render_monitor_plays(cur):
    cur.execute("""
        SELECT
            artist_name,
            track_name,
            COUNT(*) AS plays_mes
        FROM scrobbles
        WHERE DATE_TRUNC('month', scrobble_time) = DATE_TRUNC('month', CURRENT_DATE)
        GROUP BY artist_name, track_name
        HAVING COUNT(*) < 1000
        ORDER BY plays_mes DESC, artist_name ASC, track_name ASC
        LIMIT 100
    """)
    rows = cur.fetchall()

    total_under = len(rows)
    near_goal = len([r for r in rows if safe_int(r["plays_mes"]) >= 800])
    critical = len([r for r in rows if safe_int(r["plays_mes"]) < 800])

    table_rows = ""
    for r in rows:
        plays = safe_int(r["plays_mes"])
        faltan = 1000 - plays
        recomendacion = faltan

        if plays >= 800:
            color = "#f59e0b"
            label = "PUSH"
        else:
            color = "#ef4444"
            label = "CRITICO"

        table_rows += f"""
        <tr>
            <td>{r['artist_name']}</td>
            <td>{r['track_name']}</td>
            <td>{plays}</td>
            <td>{faltan}</td>
            <td>
                <span class="badge" style="color:{color};background:rgba(255,255,255,.04);border:1px solid {color};">
                    Dar {recomendacion} reproducciones
                </span>
            </td>
        </tr>
        """

    if not table_rows:
        table_rows = '<tr><td colspan="5" class="muted" style="text-align:center;">No hay canciones por debajo de 1000 este mes.</td></tr>'

    return f"""
    <div class="grid-3">
        <div class="card">
            <div class="metric-label">Canciones debajo de 1000</div>
            <div class="metric-value">{total_under}</div>
        </div>
        <div class="card">
            <div class="metric-label">Cerca de meta (800+)</div>
            <div class="metric-value warn">{near_goal}</div>
        </div>
        <div class="card">
            <div class="metric-label">Criticas (&lt;800)</div>
            <div class="metric-value incident">{critical}</div>
        </div>
    </div>

    <div class="section-title">Seguimiento de canciones con menos de 1000 plays</div>
    <table>
        <thead>
            <tr>
                <th>Artista</th>
                <th>Cancion</th>
                <th>Plays &lt;1000</th>
                <th>Faltan para 1000</th>
                <th>Recomendacion</th>
            </tr>
        </thead>
        <tbody>
            {table_rows}
        </tbody>
    </table>
    """


@app.route("/")
def home():
    try:
        init_db()
        view = (request.args.get("view") or "monitor").strip().lower()

        conn = get_conn()
        cur = conn.cursor()

        if view == "analisis":
            body = render_analisis(cur)
            title = "Vista de analisis musical"
        elif view == "ganancias":
            body = render_ganancias(cur)
            title = "Vista de ganancias promedios por plataforma"
        elif view == "monitor-plays":
            body = render_monitor_plays(cur)
            title = "Vista de seguimiento de canciones por debajo de 1000 plays"
        else:
            view = "monitor"
            body = render_monitor(cur)
            title = "Vista de monitoreo operativo"

        cur.close()
        conn.close()

        return base_page(title, view, body)

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
            started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            finished_at TIMESTAMP NULL
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
        RETURNING id, name, app_name, lastfm_user
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
        return f"<pre>{result.stdout}\\n{result.stderr}</pre>"
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
        return f"<pre>{result.stdout}\\n{result.stderr}</pre>"
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


@app.route("/tracks-under-1000")
def tracks_under_1000():
    try:
        init_db()
        conn = get_conn()
        cur = conn.cursor()

        cur.execute("""
            SELECT
                artist_name,
                track_name,
                COUNT(*) AS plays_mes
            FROM scrobbles
            WHERE DATE_TRUNC('month', scrobble_time) = DATE_TRUNC('month', CURRENT_DATE)
            GROUP BY artist_name, track_name
            HAVING COUNT(*) < 1000
            ORDER BY plays_mes DESC, artist_name ASC, track_name ASC
        """)

        rows = cur.fetchall()
        cur.close()
        conn.close()

        result = []
        for r in rows:
            plays = safe_int(r["plays_mes"])
            faltan = 1000 - plays
            result.append({
                "artist_name": r["artist_name"],
                "track_name": r["track_name"],
                "plays_mes": plays,
                "faltan": faltan,
                "recomendacion_reproducciones": faltan
            })

        return jsonify({"ok": True, "rows": result})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=10000)

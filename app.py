from flask import Flask, request, jsonify, redirect
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


def avg_rate_for_platform(platform):
    platform = (platform or "").strip().lower()
    rate = PLATFORM_RATES.get(platform, PLATFORM_RATES["spotify"])
    return (rate["min"] + rate["max"]) / 2


def menu_link(label, view_name, current_view):
    active = "menu-link active" if current_view == view_name else "menu-link"
    return f'<a class="{active}" href="/?view={view_name}">{label}</a>'


def badge_style(status):
    status = (status or "PENDING").upper()
    if status == "OK":
        return "#22c55e", "rgba(34,197,94,.15)"
    if status == "WARN":
        return "#f59e0b", "rgba(245,158,11,.15)"
    if status == "INCIDENT":
        return "#ef4444", "rgba(239,68,68,.15)"
    return "#94a3b8", "rgba(148,163,184,.15)"


def svg_line_chart(points, width=1000, height=260, stroke="#60a5fa"):
    if not points:
        return '<div class="muted">Sin datos para graficar.</div>'

    values = [float(p["value"]) for p in points]
    max_value = max(values) if values else 1
    max_value = max(max_value, 1)

    left_pad = 50
    right_pad = 20
    top_pad = 20
    bottom_pad = 40

    chart_w = width - left_pad - right_pad
    chart_h = height - top_pad - bottom_pad

    coords = []
    for i, p in enumerate(points):
        x = left_pad + (chart_w * i / max(len(points) - 1, 1))
        y = top_pad + chart_h - ((float(p["value"]) / max_value) * chart_h)
        coords.append((x, y, p["label"], p["value"]))

    polyline = " ".join([f"{x},{y}" for x, y, _, _ in coords])
    area_points = polyline + f" {coords[-1][0]},{top_pad + chart_h} {coords[0][0]},{top_pad + chart_h}"

    x_labels = ""
    for x, y, label, value in coords:
        x_labels += f'<text x="{x}" y="{height - 12}" text-anchor="middle" font-size="11" fill="#94a3b8">{label}</text>'
        x_labels += f'<circle cx="{x}" cy="{y}" r="3" fill="{stroke}"></circle>'

    y_lines = ""
    for n in range(5):
        val = max_value * (4 - n) / 4
        y = top_pad + chart_h * n / 4
        y_lines += f'<line x1="{left_pad}" y1="{y}" x2="{width-right_pad}" y2="{y}" stroke="rgba(148,163,184,.12)" />'
        y_lines += f'<text x="8" y="{y+4}" font-size="11" fill="#94a3b8">{int(val)}</text>'

    return f"""
    <svg viewBox="0 0 {width} {height}" width="100%" height="{height}">
        {y_lines}
        <polygon points="{area_points}" fill="rgba(37,99,235,.12)"></polygon>
        <polyline points="{polyline}" fill="none" stroke="{stroke}" stroke-width="3"></polyline>
        {x_labels}
    </svg>
    """


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
        <a href="/collect-all">collect-all</a>
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
                background:
                    radial-gradient(circle at top left, rgba(59,130,246,.10), transparent 28%),
                    radial-gradient(circle at top right, rgba(168,85,247,.08), transparent 25%),
                    #061126;
                color: #e5e7eb;
                padding: 24px;
            }}

            .page {{
                max-width: 1700px;
                margin: 0 auto;
            }}

            .header {{
                margin-bottom: 18px;
            }}

            .header h1 {{
                margin: 0;
                font-size: 30px;
                font-weight: 900;
                color: #f8fafc;
            }}

            .sub {{
                color: #9fb0c8;
                margin-top: 6px;
                font-size: 15px;
            }}

            .menu {{
                display: flex;
                flex-wrap: wrap;
                gap: 10px;
                margin: 18px 0 14px 0;
            }}

            .menu-link {{
                text-decoration: none;
                color: #d8b4fe;
                background: rgba(168,85,247,.08);
                border: 1px solid rgba(168,85,247,.35);
                padding: 10px 14px;
                border-radius: 12px;
                font-size: 14px;
                font-weight: 700;
                transition: .2s ease;
            }}

            .menu-link:hover {{
                transform: translateY(-1px);
                border-color: rgba(168,85,247,.6);
            }}

            .menu-link.active {{
                background: rgba(59,130,246,.16);
                color: #bfdbfe;
                border-color: rgba(96,165,250,.55);
                box-shadow: 0 0 0 1px rgba(96,165,250,.15) inset;
            }}

            .utilities {{
                display: flex;
                flex-wrap: wrap;
                gap: 10px;
                margin-bottom: 20px;
            }}

            .utilities a {{
                text-decoration: none;
                color: #c7d2fe;
                background: rgba(99,102,241,.08);
                border: 1px solid rgba(99,102,241,.25);
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
                background: linear-gradient(180deg, rgba(18,32,60,.96), rgba(13,24,47,.98));
                border: 1px solid rgba(59,130,246,.14);
                border-radius: 18px;
                padding: 18px;
                box-shadow: 0 10px 28px rgba(0,0,0,.22);
            }}

            .metric-label {{
                color: #9fb0c8;
                font-size: 13px;
                margin-bottom: 10px;
            }}

            .metric-value {{
                font-size: 34px;
                font-weight: 900;
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

            .section-title {{
                margin: 0 0 12px 0;
                font-size: 21px;
                font-weight: 800;
                color: #f8fafc;
            }}

            table {{
                width: 100%;
                border-collapse: collapse;
                background: linear-gradient(180deg, rgba(17,28,52,.98), rgba(13,23,44,.98));
                border: 1px solid rgba(59,130,246,.14);
                border-radius: 18px;
                overflow: hidden;
            }}

            th {{
                background: rgba(30,41,72,.92);
                color: #f8fafc;
                font-weight: 700;
                font-size: 14px;
                padding: 14px 12px;
                text-align: left;
            }}

            td {{
                padding: 13px 12px;
                border-top: 1px solid rgba(51,65,85,.7);
                font-size: 14px;
                vertical-align: top;
            }}

            tr:hover td {{
                background: rgba(255,255,255,.02);
            }}

            .badge {{
                display: inline-block;
                padding: 5px 10px;
                border-radius: 999px;
                font-size: 12px;
                font-weight: 800;
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
                border-bottom: 1px solid rgba(51,65,85,.7);
                gap: 12px;
            }}

            .mini-row:last-child {{
                border-bottom: 0;
            }}

            .tiny {{
                font-size: 12px;
                color: #94a3b8;
            }}

            .inline-form {{
                display: grid;
                grid-template-columns: 1.2fr 1fr 1.2fr auto;
                gap: 12px;
                align-items: end;
            }}

            .field {{
                display: flex;
                flex-direction: column;
                gap: 6px;
            }}

            .field label {{
                font-size: 12px;
                color: #9fb0c8;
                font-weight: 700;
            }}

            .field input, .field select, .field textarea {{
                background: #0b1730;
                color: #f8fafc;
                border: 1px solid rgba(96,165,250,.22);
                border-radius: 12px;
                padding: 12px;
                outline: none;
                width: 100%;
            }}

            .field textarea {{
                min-height: 100px;
                resize: vertical;
            }}

            .btn-primary, .btn-danger, .btn-secondary {{
                border: 0;
                border-radius: 12px;
                padding: 12px 16px;
                font-weight: 800;
                cursor: pointer;
                text-decoration: none;
                display: inline-block;
                text-align: center;
            }}

            .btn-primary {{
                background: linear-gradient(135deg, #2563eb, #7c3aed);
                color: white;
            }}

            .btn-danger {{
                background: linear-gradient(135deg, #dc2626, #b91c1c);
                color: white;
            }}

            .btn-secondary {{
                background: linear-gradient(135deg, #334155, #1e293b);
                color: white;
            }}

            .form-note {{
                margin-top: 10px;
                font-size: 12px;
                color: #94a3b8;
            }}

            .actions {{
                display: flex;
                gap: 8px;
                flex-wrap: wrap;
            }}

            @media (max-width: 1200px) {{
                .grid, .grid-3, .grid-2 {{
                    grid-template-columns: 1fr 1fr;
                }}
                .inline-form {{
                    grid-template-columns: 1fr 1fr;
                }}
            }}

            @media (max-width: 760px) {{
                .grid, .grid-3, .grid-2, .inline-form {{
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
            SUM(CASE WHEN status = 'INCIDENT' THEN 1 ELSE 0 END) AS incident_count
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
        ORDER BY id ASC
    """)
    teams = cur.fetchall()

    rows = ""
    for t in teams:
        badge_color, badge_bg = badge_style(t["status"])
        rows += f"""
        <tr>
            <td>{t['id']}</td>
            <td>{t['name']}</td>
            <td>{t['app_name']}</td>
            <td>{t['lastfm_user']}</td>
            <td><span class="badge" style="color:{badge_color};background:{badge_bg};border:1px solid {badge_color};">{t['status'] or 'PENDING'}</span></td>
            <td>{t['last_scrobble_at'] or '-'}</td>
            <td>{t['idle_minutes'] if t['idle_minutes'] is not None else '-'}</td>
            <td>{t['last_check_at'] or '-'}</td>
            <td>
                <div class="actions">
                    <a class="btn-secondary" href="/edit-team-form?id={t['id']}">Editar</a>
                    <a class="btn-danger" href="/delete-team?id={t['id']}" onclick="return confirm('¿Seguro que deseas borrar este equipo?')">Borrar</a>
                </div>
            </td>
        </tr>
        """

    if not rows:
        rows = '<tr><td colspan="9" class="muted" style="text-align:center;">No hay equipos cargados todavía.</td></tr>'

    add_form = """
    <div class="card" style="margin-bottom:18px;">
        <div class="section-title">Agregar usuario Last.fm al monitoreo</div>
        <form class="inline-form" method="GET" action="/seed-team">
            <div class="field">
                <label>Nombre del equipo</label>
                <input type="text" name="name" placeholder="Ej: Equipo 01" required>
            </div>
            <div class="field">
                <label>App</label>
                <select name="app" required>
                    <option value="spotify">spotify</option>
                    <option value="apple">apple</option>
                    <option value="tidal">tidal</option>
                    <option value="youtube">youtube</option>
                </select>
            </div>
            <div class="field">
                <label>Usuario Last.fm</label>
                <input type="text" name="user" placeholder="Ej: JeanCMP" required>
            </div>
            <button class="btn-primary" type="submit">Agregar</button>
        </form>
        <div class="form-note">
            Aquí registras usuarios existentes de Last.fm para que WatchEagle los monitoree.
        </div>
    </div>
    """

    batch_form = """
    <div class="card" style="margin-bottom:18px;">
        <div class="section-title">Carga masiva de equipos</div>
        <form method="GET" action="/seed-batch">
            <div class="grid-2" style="margin-bottom:12px;">
                <div class="field">
                    <label>Prefijo del equipo</label>
                    <input type="text" name="prefix" placeholder="Ej: Equipo S" required>
                </div>
                <div class="field">
                    <label>App</label>
                    <select name="app" required>
                        <option value="spotify">spotify</option>
                        <option value="apple">apple</option>
                        <option value="tidal">tidal</option>
                        <option value="youtube">youtube</option>
                    </select>
                </div>
            </div>

            <div class="field" style="margin-bottom:12px;">
                <label>Usuarios Last.fm separados por coma</label>
                <textarea name="users" placeholder="equipoS01,equipoS02,equipoS03" required></textarea>
            </div>

            <button class="btn-primary" type="submit">Cargar varios equipos</button>
            <div class="form-note">
                Sí, con esto puedes cargar más de 10 equipos a la vez. Solo separa los usuarios por coma.
            </div>
        </form>
    </div>
    """

    return f"""
    {add_form}
    {batch_form}

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
                <th>Acciones</th>
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
        SELECT
            DATE(scrobble_time) AS day,
            COUNT(*) AS plays
        FROM scrobbles
        WHERE scrobble_time >= CURRENT_DATE - INTERVAL '14 days'
        GROUP BY DATE(scrobble_time)
        ORDER BY day ASC
    """)
    daily_rows = cur.fetchall()

    daily_points = [
        {"label": str(r["day"])[5:], "value": safe_int(r["plays"])}
        for r in daily_rows
    ]
    daily_chart = svg_line_chart(daily_points)

    cur.execute("""
        SELECT
            lastfm_user,
            COUNT(*) AS plays
        FROM scrobbles
        WHERE DATE_TRUNC('month', scrobble_time) = DATE_TRUNC('month', CURRENT_DATE)
        GROUP BY lastfm_user
        ORDER BY plays DESC, lastfm_user ASC
        LIMIT 20
    """)
    users_rows = cur.fetchall()

    user_rows_html = ""
    for r in users_rows:
        user_rows_html += f"""
        <tr>
            <td>{r['lastfm_user']}</td>
            <td>{r['plays']}</td>
        </tr>
        """
    if not user_rows_html:
        user_rows_html = '<tr><td colspan="2" class="muted" style="text-align:center;">Sin datos.</td></tr>'

    cur.execute("""
        SELECT artist_name, COUNT(*) AS plays
        FROM scrobbles
        WHERE DATE(scrobble_time) = CURRENT_DATE
        GROUP BY artist_name
        ORDER BY plays DESC, artist_name ASC
        LIMIT 10
    """)
    top_artists = cur.fetchall()

    artists_html = "".join([
        f'<div class="mini-row"><span>{r["artist_name"]}</span><strong>{r["plays"]}</strong></div>'
        for r in top_artists
    ]) or '<div class="muted">Sin datos todavía.</div>'

    cur.execute("""
        SELECT track_name, artist_name, COUNT(*) AS plays
        FROM scrobbles
        WHERE DATE_TRUNC('month', scrobble_time) = DATE_TRUNC('month', CURRENT_DATE)
        GROUP BY track_name, artist_name
        ORDER BY plays DESC, artist_name ASC, track_name ASC
        LIMIT 10
    """)
    top_tracks = cur.fetchall()

    tracks_html = "".join([
        f'<div class="mini-row"><span>{r["artist_name"]} - {r["track_name"]}</span><strong>{r["plays"]}</strong></div>'
        for r in top_tracks
    ]) or '<div class="muted">Sin datos todavía.</div>'

    avg_daily = round(plays_month / max(len(daily_rows), 1), 2)

    return f"""
    <div class="grid">
        <div class="card"><div class="metric-label">Plays hoy</div><div class="metric-value">{plays_today}</div></div>
        <div class="card"><div class="metric-label">Plays del mes</div><div class="metric-value">{plays_month}</div></div>
        <div class="card"><div class="metric-label">Promedio diario</div><div class="metric-value">{avg_daily}</div></div>
        <div class="card"><div class="metric-label">Usuarios con plays este mes</div><div class="metric-value">{len(users_rows)}</div></div>
    </div>

    <div class="card" style="margin-bottom:18px;">
        <div class="section-title">Reproducciones diarias (últimos 15 días)</div>
        {daily_chart}
    </div>

    <div class="grid-2">
        <div class="card">
            <div class="section-title">Top artistas hoy</div>
            {artists_html}
        </div>

        <div class="card">
            <div class="section-title">Top canciones del mes</div>
            {tracks_html}
        </div>
    </div>

    <div>
        <div class="section-title">Reproducciones por usuario Last.fm</div>
        <table>
            <thead>
                <tr>
                    <th>Usuario Last.fm</th>
                    <th>Reproducciones mes</th>
                </tr>
            </thead>
            <tbody>
                {user_rows_html}
            </tbody>
        </table>
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
        rate = PLATFORM_RATES.get(platform, PLATFORM_RATES["spotify"])

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
            DATE(scrobble_time) AS day,
            LOWER(app_name) AS platform,
            COUNT(*) AS plays
        FROM scrobbles
        WHERE scrobble_time >= CURRENT_DATE - INTERVAL '14 days'
        GROUP BY DATE(scrobble_time), LOWER(app_name)
        ORDER BY day ASC
    """)
    daily_gain_raw = cur.fetchall()

    day_map = {}
    for r in daily_gain_raw:
        day = str(r["day"])[5:]
        plays = safe_int(r["plays"])
        rate = avg_rate_for_platform(r["platform"])
        day_map.setdefault(day, 0.0)
        day_map[day] += plays * rate

    daily_points = [{"label": k, "value": round(v, 2)} for k, v in day_map.items()]
    daily_gain_chart = svg_line_chart(daily_points, stroke="#34d399")

    cur.execute("""
        SELECT
            artist_name,
            LOWER(app_name) AS platform,
            COUNT(*) AS plays
        FROM scrobbles
        WHERE DATE(scrobble_time) = CURRENT_DATE
        GROUP BY artist_name, LOWER(app_name)
        ORDER BY plays DESC
    """)
    artist_gain_raw = cur.fetchall()

    artist_map = {}
    for r in artist_gain_raw:
        artist = r["artist_name"]
        plays = safe_int(r["plays"])
        rate = avg_rate_for_platform(r["platform"])
        artist_map.setdefault(artist, 0.0)
        artist_map[artist] += plays * rate

    artist_rows = sorted(artist_map.items(), key=lambda x: x[1], reverse=True)[:15]
    artist_rows_html = ""
    for artist, gain in artist_rows:
        artist_rows_html += f"""
        <tr>
            <td>{artist}</td>
            <td>{format_money(gain)}</td>
        </tr>
        """
    if not artist_rows_html:
        artist_rows_html = '<tr><td colspan="2" class="muted" style="text-align:center;">Sin datos.</td></tr>'

    cur.execute("""
        SELECT
            lastfm_user,
            LOWER(app_name) AS platform,
            COUNT(*) AS plays
        FROM scrobbles
        WHERE DATE_TRUNC('month', scrobble_time) = DATE_TRUNC('month', CURRENT_DATE)
        GROUP BY lastfm_user, LOWER(app_name)
        ORDER BY plays DESC
    """)
    user_gain_raw = cur.fetchall()

    user_map = {}
    for r in user_gain_raw:
        user = r["lastfm_user"]
        plays = safe_int(r["plays"])
        rate = avg_rate_for_platform(r["platform"])
        user_map.setdefault(user, 0.0)
        user_map[user] += plays * rate

    user_rows = sorted(user_map.items(), key=lambda x: x[1], reverse=True)[:20]
    user_rows_html = ""
    for user, gain in user_rows:
        user_rows_html += f"""
        <tr>
            <td>{user}</td>
            <td>{format_money(gain)}</td>
        </tr>
        """
    if not user_rows_html:
        user_rows_html = '<tr><td colspan="2" class="muted" style="text-align:center;">Sin datos.</td></tr>'

    avg_total = (total_min + total_max) / 2
    avg_daily_gain = round(avg_total / 30, 2)

    recommendation = """
    <div class="card">
        <div class="section-title">Recomendaciones</div>
        <div class="mini-row"><span>1. Prioriza usuarios que más generan</span><strong>ROI rápido</strong></div>
        <div class="mini-row"><span>2. Empuja artistas con mejor rendimiento diario</span><strong>Más rentables</strong></div>
        <div class="mini-row"><span>3. Cruza esta vista con Monitor Plays</span><strong>Meta 1K</strong></div>
    </div>
    """

    return f"""
    <div class="grid-3">
        <div class="card">
            <div class="metric-label">Ganancia mínima estimada del mes</div>
            <div class="metric-value">{format_money(total_min)}</div>
        </div>
        <div class="card">
            <div class="metric-label">Ganancia máxima estimada del mes</div>
            <div class="metric-value">{format_money(total_max)}</div>
        </div>
        <div class="card">
            <div class="metric-label">Promedio diario estimado</div>
            <div class="metric-value">{format_money(avg_daily_gain)}</div>
        </div>
    </div>

    <div class="card" style="margin-bottom:18px;">
        <div class="section-title">Ganancias por día (últimos 15 días)</div>
        {daily_gain_chart}
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
        {recommendation}
    </div>

    <div class="grid-2">
        <div>
            <div class="section-title">Ganancias por artista (hoy)</div>
            <table>
                <thead>
                    <tr>
                        <th>Artista</th>
                        <th>Ganancia estimada</th>
                    </tr>
                </thead>
                <tbody>
                    {artist_rows_html}
                </tbody>
            </table>
        </div>

        <div>
            <div class="section-title">Ganancias por usuario Last.fm (mes)</div>
            <table>
                <thead>
                    <tr>
                        <th>Usuario Last.fm</th>
                        <th>Ganancia estimada</th>
                    </tr>
                </thead>
                <tbody>
                    {user_rows_html}
                </tbody>
            </table>
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
        color = "#f59e0b" if plays >= 800 else "#ef4444"

        table_rows += f"""
        <tr>
            <td>{r['artist_name']}</td>
            <td>{r['track_name']}</td>
            <td>{plays}</td>
            <td>{faltan}</td>
            <td><span class="badge" style="color:{color};background:rgba(255,255,255,.04);border:1px solid {color};">Dar {recomendacion} reproducciones</span></td>
        </tr>
        """

    if not table_rows:
        table_rows = '<tr><td colspan="5" class="muted" style="text-align:center;">No hay canciones por debajo de 1000 este mes.</td></tr>'

    return f"""
    <div class="grid-3">
        <div class="card"><div class="metric-label">Canciones debajo de 1000</div><div class="metric-value">{total_under}</div></div>
        <div class="card"><div class="metric-label">Cerca de meta (800+)</div><div class="metric-value warn">{near_goal}</div></div>
        <div class="card"><div class="metric-label">Criticas (&lt;800)</div><div class="metric-value incident">{critical}</div></div>
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
        <tbody>{table_rows}</tbody>
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


@app.route("/edit-team-form")
def edit_team_form():
    try:
        team_id = request.args.get("id")
        if not team_id:
            return "Falta id", 400

        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT id, name, app_name, lastfm_user
            FROM teams
            WHERE id = %s
        """, (team_id,))
        team = cur.fetchone()
        cur.close()
        conn.close()

        if not team:
            return "Equipo no encontrado", 404

        return f"""
        <html>
        <head><title>Editar equipo</title></head>
        <body style="background:#061126;color:white;font-family:Arial;padding:24px;">
            <h2>Editar equipo #{team['id']}</h2>
            <form method="GET" action="/update-team" style="max-width:600px;">
                <input type="hidden" name="id" value="{team['id']}">

                <div style="margin-bottom:12px;">
                    <label>Nombre del equipo</label><br>
                    <input type="text" name="name" value="{team['name']}" style="width:100%;padding:12px;border-radius:10px;">
                </div>

                <div style="margin-bottom:12px;">
                    <label>App</label><br>
                    <input type="text" name="app" value="{team['app_name']}" style="width:100%;padding:12px;border-radius:10px;">
                </div>

                <div style="margin-bottom:12px;">
                    <label>Usuario Last.fm</label><br>
                    <input type="text" name="user" value="{team['lastfm_user']}" style="width:100%;padding:12px;border-radius:10px;">
                </div>

                <button type="submit" style="padding:12px 16px;border-radius:10px;background:#2563eb;color:white;border:0;font-weight:700;">Guardar cambios</button>
                <a href="/?view=monitor" style="margin-left:10px;color:#c4b5fd;">Volver</a>
            </form>
        </body>
        </html>
        """
    except Exception as e:
        return f"<pre>{str(e)}</pre>", 500


@app.route("/update-team")
def update_team():
    try:
        team_id = request.args.get("id")
        name = request.args.get("name")
        app_name = request.args.get("app")
        user = request.args.get("user")

        if not team_id or not name or not app_name or not user:
            return jsonify({"ok": False, "error": "Faltan parámetros"}), 400

        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            UPDATE teams
            SET name = %s, app_name = %s, lastfm_user = %s
            WHERE id = %s
        """, (name, app_name, user, team_id))
        conn.commit()
        cur.close()
        conn.close()

        return redirect("/?view=monitor")
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/delete-team")
def delete_team():
    try:
        team_id = request.args.get("id")
        if not team_id:
            return jsonify({"ok": False, "error": "Falta id"}), 400

        conn = get_conn()
        cur = conn.cursor()
        cur.execute("DELETE FROM teams WHERE id = %s", (team_id,))
        conn.commit()
        cur.close()
        conn.close()

        return redirect("/?view=monitor")
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
            return jsonify({"ok": False, "error": "Usa /seed-team?name=Equipo%2001&app=spotify&user=JeanCMP"}), 400

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

        return redirect("/?view=monitor")
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/seed-batch")
def seed_batch():
    try:
        prefix = (request.args.get("prefix") or "").strip()
        app_name = (request.args.get("app") or "").strip()
        users_raw = request.args.get("users") or ""

        if not prefix or not app_name or not users_raw:
            return jsonify({"ok": False, "error": "Usa prefix, app y users"}), 400

        users = [u.strip() for u in users_raw.split(",") if u.strip()]
        if not users:
            return jsonify({"ok": False, "error": "No hay usuarios válidos"}), 400

        conn = get_conn()
        cur = conn.cursor()

        for idx, user in enumerate(users, start=1):
            team_name = f"{prefix} {idx:02d}"
            cur.execute("""
                INSERT INTO teams(name, app_name, lastfm_user)
                VALUES(%s, %s, %s)
                ON CONFLICT (lastfm_user) DO NOTHING
            """, (team_name, app_name, user))

        conn.commit()
        cur.close()
        conn.close()

        return redirect("/?view=monitor")
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


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
        return jsonify({"ok": True, "database": "connected", "teams": row["total"]})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


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


@app.route("/collect-all")
def collect_all():
    try:
        result = subprocess.run(
            ["python", "backfill_scrobbles.py"],
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

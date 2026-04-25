from flask import Flask, request, jsonify, redirect
import os
import sys
import subprocess
import threading
import json
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


def safe_int(v, d=0):
    try:
        return int(v or 0)
    except Exception:
        return d


def money(v):
    return f"${float(v or 0):,.2f}"


def avg_rate(platform):
    p = (platform or "").strip().lower()
    r = PLATFORM_RATES.get(p, PLATFORM_RATES["spotify"])
    return (r["min"] + r["max"]) / 2


def current_filters():
    month = request.args.get("month") or ""
    platform = request.args.get("platform") or ""
    return month.strip(), platform.strip().lower()


def month_where(alias="s"):
    month, platform = current_filters()
    clauses = []
    params = []

    if month:
        clauses.append(f"TO_CHAR({alias}.scrobble_time, 'YYYY-MM') = %s")
        params.append(month)
    else:
        clauses.append(f"DATE_TRUNC('month', {alias}.scrobble_time) = DATE_TRUNC('month', CURRENT_DATE)")

    if platform:
        clauses.append(f"LOWER({alias}.app_name) = %s")
        params.append(platform)

    return " AND ".join(clauses), params


def filter_query(view):
    month, platform = current_filters()
    q = f"?view={view}"
    if month:
        q += f"&month={month}"
    if platform:
        q += f"&platform={platform}"
    return q


def filter_form(view):
    month, platform = current_filters()

    selected = {
        "spotify": "selected" if platform == "spotify" else "",
        "apple": "selected" if platform == "apple" else "",
        "tidal": "selected" if platform == "tidal" else "",
        "youtube": "selected" if platform == "youtube" else "",
        "": "selected" if not platform else "",
    }

    return f"""
    <div class="card" style="margin-bottom:18px;">
        <div class="section-title">Filtros</div>
        <form class="form-grid" method="GET" action="/">
            <input type="hidden" name="view" value="{view}">
            <div class="field">
                <label>Mes</label>
                <input type="month" name="month" value="{month}">
            </div>
            <div class="field">
                <label>Plataforma</label>
                <select name="platform">
                    <option value="" {selected[""]}>Todas</option>
                    <option value="spotify" {selected["spotify"]}>Spotify</option>
                    <option value="apple" {selected["apple"]}>Apple Music</option>
                    <option value="tidal" {selected["tidal"]}>Tidal</option>
                    <option value="youtube" {selected["youtube"]}>YouTube Music</option>
                </select>
            </div>
            <div></div>
            <button class="btn btn-primary">Aplicar</button>
        </form>
    </div>
    """


def run_python_script(script_name, timeout=900):
    try:
        path = os.path.join(os.getcwd(), script_name)
        result = subprocess.run(
            [sys.executable, path],
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        return f"""
        <pre>
SCRIPT: {script_name}
RETURN CODE: {result.returncode}

STDOUT:
{result.stdout}

STDERR:
{result.stderr}
        </pre>
        """
    except Exception as e:
        return f"<pre>ERROR ejecutando {script_name}: {str(e)}</pre>", 500


def run_background(script_name):
    def task():
        subprocess.run([sys.executable, os.path.join(os.getcwd(), script_name)])

    threading.Thread(target=task, daemon=True).start()


def nav_link(label, view, current):
    active = "active" if view == current else ""
    return f'<a class="nav-link {active}" href="/{filter_query(view)}">{label}</a>'


def badge(status):
    s = (status or "PENDING").upper()
    cls = {"OK": "ok", "WARN": "warn", "INCIDENT": "incident", "PENDING": "pending"}.get(s, "pending")
    return f'<span class="badge {cls}">{s}</span>'


def base_page(title, view, body):
    nav = (
        nav_link("Monitor", "monitor", view)
        + nav_link("Análisis Pro", "analisis", view)
        + nav_link("Ganancias Pro", "ganancias", view)
        + nav_link("Monitor Plays", "monitor-plays", view)
    )

    return f"""
<!doctype html>
<html>
<head>
<title>WatchEagle</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<style>
* {{ box-sizing:border-box; }}
body {{
    margin:0;
    font-family:Arial, sans-serif;
    background:
      radial-gradient(circle at top left, rgba(59,130,246,.15), transparent 28%),
      radial-gradient(circle at top right, rgba(168,85,247,.12), transparent 25%),
      #061126;
    color:#e5e7eb;
    padding:24px;
}}
.page {{ max-width:1750px; margin:0 auto; }}
h1 {{ margin:0; font-size:34px; font-weight:900; color:#fff; }}
.subtitle {{ color:#9fb0c8; margin:6px 0 18px; }}
.nav,.tools {{ display:flex; flex-wrap:wrap; gap:10px; margin-bottom:16px; }}
.nav-link,.tool-link {{
    text-decoration:none;
    color:#d8b4fe;
    background:rgba(168,85,247,.09);
    border:1px solid rgba(168,85,247,.35);
    padding:10px 14px;
    border-radius:12px;
    font-weight:800;
    font-size:14px;
}}
.nav-link.active {{
    background:rgba(59,130,246,.18);
    color:#bfdbfe;
    border-color:rgba(96,165,250,.65);
}}
.tool-link {{
    color:#c7d2fe;
    background:rgba(99,102,241,.08);
    border-color:rgba(99,102,241,.25);
    font-size:13px;
}}
.grid {{ display:grid; grid-template-columns:repeat(4,1fr); gap:14px; margin-bottom:18px; }}
.grid-3 {{ display:grid; grid-template-columns:repeat(3,1fr); gap:14px; margin-bottom:18px; }}
.grid-2 {{ display:grid; grid-template-columns:repeat(2,1fr); gap:18px; margin-bottom:18px; }}
.card {{
    background:linear-gradient(180deg,rgba(18,32,60,.96),rgba(13,24,47,.98));
    border:1px solid rgba(96,165,250,.16);
    border-radius:18px;
    padding:18px;
    box-shadow:0 10px 28px rgba(0,0,0,.24);
}}
.label {{ color:#9fb0c8; font-size:13px; margin-bottom:10px; }}
.value {{ font-size:34px; font-weight:900; }}
.section-title {{ font-size:21px; font-weight:900; margin:0 0 12px; color:#f8fafc; }}
table {{
    width:100%;
    border-collapse:collapse;
    background:linear-gradient(180deg,rgba(17,28,52,.98),rgba(13,23,44,.98));
    border:1px solid rgba(96,165,250,.16);
    border-radius:18px;
    overflow:hidden;
    margin-bottom:18px;
}}
th {{
    background:rgba(30,41,72,.94);
    color:#f8fafc;
    padding:14px 12px;
    font-size:14px;
    text-align:left;
}}
td {{
    padding:13px 12px;
    border-top:1px solid rgba(51,65,85,.75);
    font-size:14px;
    vertical-align:top;
}}
tr:hover td {{ background:rgba(255,255,255,.025); }}
.badge {{
    display:inline-block;
    padding:5px 10px;
    border-radius:999px;
    font-size:12px;
    font-weight:900;
    border:1px solid currentColor;
}}
.ok {{ color:#22c55e; background:rgba(34,197,94,.14); }}
.warn {{ color:#f59e0b; background:rgba(245,158,11,.14); }}
.incident {{ color:#ef4444; background:rgba(239,68,68,.14); }}
.pending {{ color:#94a3b8; background:rgba(148,163,184,.12); }}
.green {{ color:#34d399; }}
.red {{ color:#ef4444; }}
.yellow {{ color:#f59e0b; }}
.blue {{ color:#60a5fa; }}
.muted {{ color:#94a3b8; }}
.field {{ display:flex; flex-direction:column; gap:6px; }}
.field label {{ color:#9fb0c8; font-size:12px; font-weight:800; }}
input,select,textarea {{
    background:#0b1730;
    color:white;
    border:1px solid rgba(96,165,250,.24);
    border-radius:12px;
    padding:12px;
    width:100%;
}}
textarea {{ min-height:95px; }}
.form-grid {{ display:grid; grid-template-columns:1.2fr 1fr 1.2fr auto; gap:12px; align-items:end; }}
.btn {{
    border:0;
    border-radius:12px;
    padding:11px 14px;
    font-weight:900;
    color:white;
    text-decoration:none;
    display:inline-block;
    cursor:pointer;
}}
.btn-primary {{ background:linear-gradient(135deg,#2563eb,#7c3aed); }}
.btn-danger {{ background:linear-gradient(135deg,#dc2626,#991b1b); }}
.btn-secondary {{ background:linear-gradient(135deg,#334155,#1e293b); }}
.actions {{ display:flex; flex-wrap:wrap; gap:8px; }}
.mini-row {{
    display:flex;
    justify-content:space-between;
    border-bottom:1px solid rgba(51,65,85,.7);
    padding:10px 0;
    gap:12px;
}}
.mini-row:last-child {{ border-bottom:0; }}
canvas {{ width:100% !important; max-height:330px; }}
@media(max-width:1100px) {{
    .grid,.grid-3,.grid-2,.form-grid {{ grid-template-columns:1fr; }}
}}
</style>
</head>
<body>
<div class="page">
    <h1>WatchEagle</h1>
    <div class="subtitle">{title}</div>
    <div class="nav">{nav}</div>
    <div class="tools">
        <a class="tool-link" href="/run-check">run-check</a>
        <a class="tool-link" href="/collect-now">collect-now</a>
        <a class="tool-link" href="/collect-all">collect-all background</a>
        <a class="tool-link" href="/scrobbles-count">scrobbles-count</a>
        <a class="tool-link" href="/healthz">healthz</a>
    </div>
    {body}
</div>
</body>
</html>
"""


def render_monitor(cur):
    month, platform = current_filters()
    where, params = month_where("s")

    cur.execute("""
        SELECT
            COUNT(*) total,
            SUM(CASE WHEN status='OK' THEN 1 ELSE 0 END) ok_count,
            SUM(CASE WHEN status='WARN' THEN 1 ELSE 0 END) warn_count,
            SUM(CASE WHEN status='INCIDENT' THEN 1 ELSE 0 END) incident_count
        FROM teams
        WHERE active = TRUE
    """)
    s = cur.fetchone()

    cur.execute(f"""
        SELECT COUNT(*) total_today
        FROM scrobbles s
        WHERE {where}
    """, params)
    total_filtered = safe_int(cur.fetchone()["total_today"])

    if platform:
        cur.execute("""
            SELECT id,name,app_name,lastfm_user,status,last_scrobble_at,idle_minutes,last_check_at
            FROM teams
            WHERE LOWER(app_name) = %s
            ORDER BY id ASC
        """, (platform,))
    else:
        cur.execute("""
            SELECT id,name,app_name,lastfm_user,status,last_scrobble_at,idle_minutes,last_check_at
            FROM teams
            ORDER BY id ASC
        """)
    teams = cur.fetchall()

    rows = ""
    for t in teams:
        rows += f"""
        <tr>
            <td>{t['id']}</td><td>{t['name']}</td><td>{t['app_name']}</td><td>{t['lastfm_user']}</td>
            <td>{badge(t['status'])}</td><td>{t['last_scrobble_at'] or '-'}</td>
            <td>{t['idle_minutes'] if t['idle_minutes'] is not None else '-'}</td>
            <td>{t['last_check_at'] or '-'}</td>
            <td>
                <div class="actions">
                    <a class="btn btn-secondary" href="/edit-team-form?id={t['id']}">Editar</a>
                    <a class="btn btn-danger" href="/delete-team?id={t['id']}" onclick="return confirm('¿Borrar equipo?')">Borrar</a>
                </div>
            </td>
        </tr>
        """

    if not rows:
        rows = '<tr><td colspan="9" class="muted" style="text-align:center;">No hay equipos.</td></tr>'

    return f"""
    {filter_form("monitor")}

    <div class="card" style="margin-bottom:18px;">
        <div class="section-title">Agregar usuario Last.fm</div>
        <form class="form-grid" method="GET" action="/seed-team">
            <div class="field"><label>Equipo</label><input name="name" placeholder="Box-01 01" required></div>
            <div class="field"><label>App</label>
                <select name="app">
                    <option value="spotify">spotify</option><option value="apple">apple</option>
                    <option value="tidal">tidal</option><option value="youtube">youtube</option>
                </select>
            </div>
            <div class="field"><label>Usuario Last.fm</label><input name="user" placeholder="equipoC01" required></div>
            <button class="btn btn-primary">Agregar</button>
        </form>
    </div>

    <div class="card" style="margin-bottom:18px;">
        <div class="section-title">Carga masiva</div>
        <form method="GET" action="/seed-batch">
            <div class="grid-2">
                <div class="field"><label>Prefijo</label><input name="prefix" placeholder="Box-03" required></div>
                <div class="field"><label>App</label>
                    <select name="app">
                        <option value="spotify">spotify</option><option value="apple">apple</option>
                        <option value="tidal">tidal</option><option value="youtube">youtube</option>
                    </select>
                </div>
            </div>
            <div class="field" style="margin-top:12px;"><label>Usuarios separados por coma</label><textarea name="users" required></textarea></div>
            <br><button class="btn btn-primary">Cargar varios</button>
        </form>
    </div>

    <div class="grid">
        <div class="card"><div class="label">Monitores activos</div><div class="value">{safe_int(s['total'])}</div></div>
        <div class="card"><div class="label">OK</div><div class="value green">{safe_int(s['ok_count'])}</div></div>
        <div class="card"><div class="label">WARN</div><div class="value yellow">{safe_int(s['warn_count'])}</div></div>
        <div class="card"><div class="label">INCIDENT</div><div class="value red">{safe_int(s['incident_count'])}</div></div>
    </div>

    <div class="card" style="margin-bottom:18px;">
        <div class="label">Scrobbles filtrados</div>
        <div class="value blue">{total_filtered}</div>
    </div>

    <div class="section-title">Estado de equipos</div>
    <table>
        <thead><tr><th>ID</th><th>Equipo</th><th>App</th><th>User</th><th>Status</th><th>Último scrobble</th><th>Idle</th><th>Último check</th><th>Acciones</th></tr></thead>
        <tbody>{rows}</tbody>
    </table>
    """


def render_analisis(cur):
    where, params = month_where("s")

    cur.execute(f"SELECT COUNT(*) c FROM scrobbles s WHERE DATE(s.scrobble_time)=CURRENT_DATE" + (f" AND LOWER(s.app_name)=%s" if current_filters()[1] else ""), ([current_filters()[1]] if current_filters()[1] else []))
    plays_today = safe_int(cur.fetchone()["c"])

    cur.execute(f"SELECT COUNT(*) c FROM scrobbles s WHERE {where}", params)
    plays_month = safe_int(cur.fetchone()["c"])

    cur.execute(f"""
        SELECT DATE(s.scrobble_time) AS play_day, COUNT(*) AS plays
        FROM scrobbles s
        WHERE {where}
        GROUP BY DATE(s.scrobble_time)
        ORDER BY play_day ASC
    """, params)
    daily = cur.fetchall()

    labels = [str(r["play_day"])[5:] for r in daily]
    values = [safe_int(r["plays"]) for r in daily]

    cur.execute(f"""
        SELECT s.lastfm_user, COUNT(*) AS plays
        FROM scrobbles s
        WHERE {where}
        GROUP BY s.lastfm_user
        ORDER BY plays DESC
        LIMIT 25
    """, params)
    users = cur.fetchall()
    user_rows = "".join([f"<tr><td>{r['lastfm_user']}</td><td>{r['plays']}</td></tr>" for r in users]) or '<tr><td colspan="2" class="muted">Sin datos</td></tr>'

    cur.execute(f"""
        SELECT s.artist_name, COUNT(*) AS plays
        FROM scrobbles s
        WHERE {where}
        GROUP BY s.artist_name
        ORDER BY plays DESC
        LIMIT 10
    """, params)
    artists = cur.fetchall()
    artist_html = "".join([f"<div class='mini-row'><span>{r['artist_name']}</span><strong>{r['plays']}</strong></div>" for r in artists]) or '<div class="muted">Sin datos</div>'

    avg_daily = round(plays_month / max(len(daily), 1), 2)

    return f"""
    {filter_form("analisis")}

    <div class="grid">
        <div class="card"><div class="label">Plays hoy</div><div class="value">{plays_today}</div></div>
        <div class="card"><div class="label">Plays filtrados</div><div class="value">{plays_month}</div></div>
        <div class="card"><div class="label">Promedio diario</div><div class="value">{avg_daily}</div></div>
        <div class="card"><div class="label">Usuarios activos</div><div class="value">{len(users)}</div></div>
    </div>

    <div class="card" style="margin-bottom:18px;">
        <div class="section-title">Reproducciones diarias</div>
        <canvas id="playsChart"></canvas>
    </div>

    <div class="grid-2">
        <div class="card"><div class="section-title">Top artistas</div>{artist_html}</div>
        <div>
            <div class="section-title">Reproducciones por usuario Last.fm</div>
            <table><thead><tr><th>Usuario</th><th>Reproducciones</th></tr></thead><tbody>{user_rows}</tbody></table>
        </div>
    </div>

    <script>
    new Chart(document.getElementById('playsChart'), {{
        type:'line',
        data:{{labels:{json.dumps(labels)},datasets:[{{label:'Reproducciones',data:{json.dumps(values)},borderColor:'#60a5fa',backgroundColor:'rgba(96,165,250,.15)',tension:.35,fill:true}}]}},
        options:{{responsive:true,plugins:{{legend:{{labels:{{color:'#e5e7eb'}}}}}},scales:{{x:{{ticks:{{color:'#94a3b8'}}}},y:{{ticks:{{color:'#94a3b8'}}}}}}}}
    }});
    </script>
    """


def render_ganancias(cur):
    where, params = month_where("s")

    cur.execute(f"""
        SELECT LOWER(s.app_name) AS platform, COUNT(*) AS plays
        FROM scrobbles s
        WHERE {where}
        GROUP BY LOWER(s.app_name)
        ORDER BY plays DESC
    """, params)
    platforms = cur.fetchall()

    total_min = total_max = 0
    platform_rows = ""

    for r in platforms:
        p = (r["platform"] or "").lower()
        plays = safe_int(r["plays"])
        rate = PLATFORM_RATES.get(p, PLATFORM_RATES["spotify"])
        mn, mx = plays * rate["min"], plays * rate["max"]
        total_min += mn
        total_max += mx
        platform_rows += f"<tr><td>{p.title()}</td><td>{plays}</td><td>{money(mn)}</td><td>{money(mx)}</td><td>{money((mn+mx)/2)}</td></tr>"

    if not platform_rows:
        platform_rows = '<tr><td colspan="5" class="muted">Sin datos</td></tr>'

    cur.execute(f"""
        SELECT DATE(s.scrobble_time) AS play_day, LOWER(s.app_name) AS platform, COUNT(*) AS plays
        FROM scrobbles s
        WHERE {where}
        GROUP BY DATE(s.scrobble_time), LOWER(s.app_name)
        ORDER BY play_day ASC
    """, params)
    daily = cur.fetchall()

    day_map = {}
    for r in daily:
        d = str(r["play_day"])[5:]
        day_map[d] = day_map.get(d, 0) + safe_int(r["plays"]) * avg_rate(r["platform"])

    gain_labels = list(day_map.keys())
    gain_values = [round(v, 2) for v in day_map.values()]

    cur.execute(f"""
        SELECT s.artist_name, LOWER(s.app_name) AS platform, COUNT(*) AS plays
        FROM scrobbles s
        WHERE {where}
        GROUP BY s.artist_name, LOWER(s.app_name)
    """, params)
    raw_artists = cur.fetchall()

    artist_map = {}
    for r in raw_artists:
        artist_map[r["artist_name"]] = artist_map.get(r["artist_name"], 0) + safe_int(r["plays"]) * avg_rate(r["platform"])

    artist_rows = "".join([f"<tr><td>{a}</td><td>{money(v)}</td></tr>" for a, v in sorted(artist_map.items(), key=lambda x: x[1], reverse=True)[:20]]) or '<tr><td colspan="2" class="muted">Sin datos</td></tr>'

    cur.execute(f"""
        SELECT s.lastfm_user, LOWER(s.app_name) AS platform, COUNT(*) AS plays
        FROM scrobbles s
        WHERE {where}
        GROUP BY s.lastfm_user, LOWER(s.app_name)
    """, params)
    raw_users = cur.fetchall()

    user_map = {}
    for r in raw_users:
        user_map[r["lastfm_user"]] = user_map.get(r["lastfm_user"], 0) + safe_int(r["plays"]) * avg_rate(r["platform"])

    user_rows = "".join([f"<tr><td>{u}</td><td>{money(v)}</td></tr>" for u, v in sorted(user_map.items(), key=lambda x: x[1], reverse=True)[:25]]) or '<tr><td colspan="2" class="muted">Sin datos</td></tr>'

    return f"""
    {filter_form("ganancias")}

    <div class="grid-3">
        <div class="card"><div class="label">Mínimo estimado</div><div class="value">{money(total_min)}</div></div>
        <div class="card"><div class="label">Máximo estimado</div><div class="value">{money(total_max)}</div></div>
        <div class="card"><div class="label">Promedio estimado</div><div class="value">{money((total_min+total_max)/2)}</div></div>
    </div>

    <div class="card" style="margin-bottom:18px;">
        <div class="section-title">Ganancias por día</div>
        <canvas id="gainChart"></canvas>
    </div>

    <div class="grid-2">
        <div>
            <div class="section-title">Ganancias por plataforma</div>
            <table><thead><tr><th>Plataforma</th><th>Streams</th><th>Min</th><th>Max</th><th>Promedio</th></tr></thead><tbody>{platform_rows}</tbody></table>
        </div>
        <div class="card">
            <div class="section-title">Recomendación ejecutiva</div>
            <div class="mini-row"><span>Empuja usuarios con más generación</span><strong>ROI</strong></div>
            <div class="mini-row"><span>Cruza con Monitor Plays</span><strong>Meta 1K</strong></div>
            <div class="mini-row"><span>Prioriza artistas con alza diaria</span><strong>Escala</strong></div>
        </div>
    </div>

    <div class="grid-2">
        <div><div class="section-title">Ganancias por artista</div><table><thead><tr><th>Artista</th><th>Ganancia</th></tr></thead><tbody>{artist_rows}</tbody></table></div>
        <div><div class="section-title">Ganancias por usuario Last.fm</div><table><thead><tr><th>Usuario</th><th>Ganancia</th></tr></thead><tbody>{user_rows}</tbody></table></div>
    </div>

    <script>
    new Chart(document.getElementById('gainChart'), {{
        type:'line',
        data:{{labels:{json.dumps(gain_labels)},datasets:[{{label:'Ganancia estimada',data:{json.dumps(gain_values)},borderColor:'#34d399',backgroundColor:'rgba(52,211,153,.15)',tension:.35,fill:true}}]}},
        options:{{responsive:true,plugins:{{legend:{{labels:{{color:'#e5e7eb'}}}}}},scales:{{x:{{ticks:{{color:'#94a3b8'}}}},y:{{ticks:{{color:'#94a3b8'}}}}}}}}
    }});
    </script>
    """


def render_monitor_plays(cur):
    where, params = month_where("s")

    cur.execute(f"""
        SELECT s.artist_name, s.track_name, COUNT(*) AS plays
        FROM scrobbles s
        WHERE {where}
        GROUP BY s.artist_name, s.track_name
        HAVING COUNT(*) < 1000
        ORDER BY plays DESC
        LIMIT 150
    """, params)
    rows = cur.fetchall()

    near = len([r for r in rows if safe_int(r["plays"]) >= 900])
    push = len([r for r in rows if 800 <= safe_int(r["plays"]) < 900])
    critical = len([r for r in rows if safe_int(r["plays"]) < 800])

    table_rows = ""
    for r in rows:
        plays = safe_int(r["plays"])
        faltan = 1000 - plays
        if plays >= 900:
            estado = '<span class="badge ok">🔥 Cerca</span>'
        elif plays >= 800:
            estado = '<span class="badge warn">⚠️ Push</span>'
        else:
            estado = '<span class="badge incident">🚨 Crítico</span>'

        table_rows += f"""
        <tr>
            <td>{r['artist_name']}</td><td>{r['track_name']}</td><td>{plays}</td><td>{faltan}</td>
            <td>Dar {faltan} reproducciones</td><td>{estado}</td>
        </tr>
        """

    if not table_rows:
        table_rows = '<tr><td colspan="6" class="muted">No hay canciones debajo de 1000.</td></tr>'

    return f"""
    {filter_form("monitor-plays")}

    <div class="grid-3">
        <div class="card"><div class="label">🔥 Cerca 900-999</div><div class="value green">{near}</div></div>
        <div class="card"><div class="label">⚠️ Push 800-899</div><div class="value yellow">{push}</div></div>
        <div class="card"><div class="label">🚨 Crítico &lt;800</div><div class="value red">{critical}</div></div>
    </div>

    <div class="card" style="margin-bottom:18px;">
        <div class="section-title">Recomendación automática</div>
        <div class="mini-row"><span>Primero empuja canciones 900-999</span><strong>Más fácil cerrar 1K</strong></div>
        <div class="mini-row"><span>Luego 800-899</span><strong>Requieren presión</strong></div>
        <div class="mini-row"><span>Menos de 800</span><strong>Solo si hay prioridad comercial</strong></div>
    </div>

    <div class="section-title">Monitor Plays Pro</div>
    <table>
        <thead><tr><th>Artista</th><th>Canción</th><th>Plays</th><th>Faltan 1K</th><th>Recomendación</th><th>Estado</th></tr></thead>
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
            title = "Vista analítica pro"
        elif view == "ganancias":
            body = render_ganancias(cur)
            title = "Vista de ganancias pro"
        elif view == "monitor-plays":
            body = render_monitor_plays(cur)
            title = "Seguimiento de canciones debajo de 1000"
        else:
            view = "monitor"
            body = render_monitor(cur)
            title = "Monitoreo operativo"

        cur.close()
        conn.close()
        return base_page(title, view, body)

    except Exception as e:
        return f"<pre>ERROR EN HOME:\n{str(e)}</pre>", 500


@app.route("/seed-team")
def seed_team():
    name = request.args.get("name")
    app_name = request.args.get("app")
    user = request.args.get("user")

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO teams(name, app_name, lastfm_user)
        VALUES(%s,%s,%s)
        ON CONFLICT(lastfm_user) DO NOTHING
    """, (name, app_name, user))
    conn.commit()
    cur.close()
    conn.close()
    return redirect("/?view=monitor")


@app.route("/seed-batch")
def seed_batch():
    prefix = request.args.get("prefix")
    app_name = request.args.get("app")
    users = [u.strip() for u in (request.args.get("users") or "").split(",") if u.strip()]

    conn = get_conn()
    cur = conn.cursor()
    for i, user in enumerate(users, start=1):
        cur.execute("""
            INSERT INTO teams(name, app_name, lastfm_user)
            VALUES(%s,%s,%s)
            ON CONFLICT(lastfm_user) DO NOTHING
        """, (f"{prefix} {i:02d}", app_name, user))
    conn.commit()
    cur.close()
    conn.close()
    return redirect("/?view=monitor")


@app.route("/edit-team-form")
def edit_team_form():
    team_id = request.args.get("id")
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT id,name,app_name,lastfm_user FROM teams WHERE id=%s", (team_id,))
    t = cur.fetchone()
    cur.close()
    conn.close()

    if not t:
        return "Equipo no encontrado", 404

    return f"""
    <body style="background:#061126;color:white;font-family:Arial;padding:24px;">
    <h2>Editar equipo #{t['id']}</h2>
    <form method="GET" action="/update-team" style="max-width:600px;">
        <input type="hidden" name="id" value="{t['id']}">
        <p>Equipo</p><input name="name" value="{t['name']}" style="width:100%;padding:12px;">
        <p>App</p><input name="app" value="{t['app_name']}" style="width:100%;padding:12px;">
        <p>User</p><input name="user" value="{t['lastfm_user']}" style="width:100%;padding:12px;">
        <br><br><button>Guardar</button>
        <a href="/?view=monitor" style="color:#c4b5fd;">Volver</a>
    </form>
    </body>
    """


@app.route("/update-team")
def update_team():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        UPDATE teams SET name=%s, app_name=%s, lastfm_user=%s WHERE id=%s
    """, (
        request.args.get("name"),
        request.args.get("app"),
        request.args.get("user"),
        request.args.get("id"),
    ))
    conn.commit()
    cur.close()
    conn.close()
    return redirect("/?view=monitor")


@app.route("/delete-team")
def delete_team():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM teams WHERE id=%s", (request.args.get("id"),))
    conn.commit()
    cur.close()
    conn.close()
    return redirect("/?view=monitor")


@app.route("/run-check")
def run_check():
    return run_python_script("watch_scrobbles.py")


@app.route("/collect-now")
def collect_now():
    return run_python_script("collect_scrobbles.py")


@app.route("/collect-all")
def collect_all():
    run_background("backfill_scrobbles.py")
    return """
    <body style="background:#061126;color:white;font-family:Arial;padding:24px;">
    <h2>Backfill iniciado en background 🚀</h2>
    <p>Vuelve al dashboard y revisa las métricas en unos minutos.</p>
    <a style="color:#93c5fd;" href="/">Volver</a>
    </body>
    """


@app.route("/healthz")
def healthz():
    try:
        init_db()
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) total FROM teams")
        r = cur.fetchone()
        cur.close()
        conn.close()
        return jsonify({"ok": True, "teams": r["total"]})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/scrobbles-count")
def scrobbles_count():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) total FROM scrobbles")
    r = cur.fetchone()
    cur.close()
    conn.close()
    return jsonify({"ok": True, "total": r["total"]})


if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=10000)

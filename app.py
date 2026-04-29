from flask import Flask, request, jsonify, redirect
import os
import sys
import subprocess
import json
import threading
from datetime import datetime

from helpers import get_conn, init_db

app = Flask(__name__)

PLATFORM_RATES = {
    "spotify": {"min": 0.0035, "max": 0.0050},
    "apple": {"min": 0.0070, "max": 0.0100},
    "apple music": {"min": 0.0070, "max": 0.0100},
    "tidal": {"min": 0.0120, "max": 0.0150},
    "youtube": {"min": 0.0007, "max": 0.0020},
    "youtube music": {"min": 0.0007, "max": 0.0020},
}

ARTIST_METADATA = [
    ("Jeantune", "Distrokid"),
    ("JCSTUDIO", "Distrokid"),
    ("JMAR", "Ditto"),
    ("YlegMoon", "Distrokid"),
    ("Batytune", "Distrokid"),
    ("Jzentrix", "Distrokid"),
    ("JironPulse", "Distrokid"),
    ("God Herd", "TuneCore"),
    ("JJ Legacy", "Symphonic"),
    ("Cielaurum", "Ditto"),
    ("QuietMetric", "Ditto"),
    ("AetherFocus", "Ditto"),
    ("ZukiPop", "Distrokid"),
    ("LexiGo", "Distrokid"),
    ("VYRONEX", "Distrokid"),
    ("AEROVIA", "Distrokid"),
    ("TechMich", "Distrokid"),
    ("KRYONEXIS", "Symphonic"),
]

JOB_LOG_DIR = "/tmp/watcheagle_jobs"
os.makedirs(JOB_LOG_DIR, exist_ok=True)


# ============================================================
# UTILIDADES
# ============================================================

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
    month = (request.args.get("month") or "").strip()
    platform = (request.args.get("platform") or "").strip().lower()
    distributor = (request.args.get("distributor") or "").strip()
    return month, platform, distributor


def month_range(month):
    if month:
        start = datetime.strptime(month, "%Y-%m")
    else:
        now = datetime.utcnow()
        start = datetime(now.year, now.month, 1)

    if start.month == 12:
        end = datetime(start.year + 1, 1, 1)
    else:
        end = datetime(start.year, start.month + 1, 1)

    return start, end


def month_where(alias="s"):
    """
    Filtro optimizado.
    Antes usábamos TO_CHAR(scrobble_time, 'YYYY-MM'), eso pone la DB lenta.
    Ahora usamos rango de fechas para que PostgreSQL pueda usar índices.
    """
    month, platform, distributor = current_filters()
    start, end = month_range(month)

    clauses = [
        f"{alias}.scrobble_time >= %s",
        f"{alias}.scrobble_time < %s",
    ]
    params = [start, end]

    if platform:
        clauses.append(f"LOWER({alias}.app_name) = %s")
        params.append(platform)

    if distributor:
        clauses.append(f"""
            EXISTS (
                SELECT 1
                FROM artist_metadata am
                WHERE LOWER(am.artist_name) = LOWER({alias}.artist_name)
                AND am.distributor = %s
            )
        """)
        params.append(distributor)

    return " AND ".join(clauses), params


def filter_query(view):
    month, platform, distributor = current_filters()
    q = f"?view={view}"

    if month:
        q += f"&month={month}"
    if platform:
        q += f"&platform={platform}"
    if distributor:
        q += f"&distributor={distributor}"

    return q


def filter_form(view):
    month, platform, distributor = current_filters()

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
                    <option value="" {"selected" if not platform else ""}>Todas</option>
                    <option value="spotify" {"selected" if platform == "spotify" else ""}>Spotify</option>
                    <option value="apple" {"selected" if platform == "apple" else ""}>Apple Music</option>
                    <option value="tidal" {"selected" if platform == "tidal" else ""}>Tidal</option>
                    <option value="youtube" {"selected" if platform == "youtube" else ""}>YouTube Music</option>
                </select>
            </div>

            <div class="field">
                <label>Distribuidora</label>
                <select name="distributor">
                    <option value="" {"selected" if not distributor else ""}>Todas</option>
                    <option value="Distrokid" {"selected" if distributor == "Distrokid" else ""}>Distrokid</option>
                    <option value="Ditto" {"selected" if distributor == "Ditto" else ""}>Ditto</option>
                    <option value="TuneCore" {"selected" if distributor == "TuneCore" else ""}>TuneCore</option>
                    <option value="Symphonic" {"selected" if distributor == "Symphonic" else ""}>Symphonic</option>
                </select>
            </div>

            <button class="btn btn-primary">Aplicar</button>
        </form>
    </div>
    """


# ============================================================
# JOBS / SUBPROCESOS
# ============================================================

def start_logged_job(script_name, job_name, extra_env=None):
    log_path = os.path.join(JOB_LOG_DIR, f"{job_name}.log")

    def task():
        env = os.environ.copy()
        if extra_env:
            env.update(extra_env)

        with open(log_path, "w", encoding="utf-8") as f:
            f.write(f"JOB: {job_name}\n")
            f.write(f"SCRIPT: {script_name}\n")
            f.write(f"PATH: {os.path.join(os.getcwd(), script_name)}\n")
            f.write(f"PYTHON: {sys.executable}\n")
            f.write(f"STARTED UTC: {datetime.utcnow()}\n")
            if extra_env:
                f.write(f"EXTRA_ENV: {extra_env}\n")
            f.write("\n==================== OUTPUT ====================\n\n")
            f.flush()

            try:
                result = subprocess.run(
                    [sys.executable, "-u", os.path.join(os.getcwd(), script_name)],
                    stdout=f,
                    stderr=f,
                    text=True,
                    env=env,
                )

                f.write("\n==================== FINISHED ====================\n")
                f.write(f"FINISHED UTC: {datetime.utcnow()}\n")
                f.write(f"RETURN CODE: {result.returncode}\n")
                f.flush()

            except Exception as e:
                f.write("\n==================== ERROR ====================\n")
                f.write(str(e))
                f.write("\n")
                f.flush()

    threading.Thread(target=task, daemon=True).start()
    return log_path


def run_python_script(script_name, timeout=900):
    try:
        script_path = os.path.join(os.getcwd(), script_name)

        if not os.path.exists(script_path):
            return f"<pre>ERROR: No existe {script_path}</pre>", 500

        result = subprocess.run(
            [sys.executable, "-u", script_path],
            capture_output=True,
            text=True,
            timeout=timeout,
        )

        return f"""
<pre>
SCRIPT: {script_name}
PATH: {script_path}
PYTHON: {sys.executable}
RETURN CODE: {result.returncode}

STDOUT:
{result.stdout}

STDERR:
{result.stderr}
</pre>
"""
    except subprocess.TimeoutExpired:
        return f"<pre>ERROR: {script_name} tardó demasiado.</pre>", 500
    except Exception as e:
        return f"<pre>ERROR ejecutando {script_name}:\n{str(e)}</pre>", 500


# ============================================================
# UI
# ============================================================

def nav_link(label, view, current):
    active = "active" if view == current else ""
    return f'<a class="nav-link {active}" href="/{filter_query(view)}">{label}</a>'


def badge(status):
    s = (status or "PENDING").upper()
    cls = {
        "OK": "ok",
        "WARN": "warn",
        "INCIDENT": "incident",
        "PENDING": "pending",
    }.get(s, "pending")
    return f'<span class="badge {cls}">{s}</span>'


def base_page(title, view, body):
    nav = (
        nav_link("Ejecutivo", "ejecutivo", view)
        + nav_link("Monitor", "monitor", view)
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
.form-grid {{ display:grid; grid-template-columns:1fr 1fr 1fr auto; gap:12px; align-items:end; }}
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
pre {{
    background:#020617;
    color:#d1d5db;
    border:1px solid rgba(96,165,250,.24);
    border-radius:16px;
    padding:18px;
    white-space:pre-wrap;
}}
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
        <a class="tool-link" href="/collect-all">collect-all todos</a>
        <a class="tool-link" href="/job-log?job=collect-now">log collect-now</a>
        <a class="tool-link" href="/job-log?job=collect-all">log collect-all</a>
        <a class="tool-link" href="/job-log?job=collect-all-selected">log seleccionados</a>
        <a class="tool-link" href="/scrobbles-count">scrobbles-count</a>
        <a class="tool-link" href="/healthz">healthz</a>
        <a class="tool-link" href="/init-artist-metadata">init distribuidoras</a>
        <a class="tool-link" href="/init-performance-indexes">init performance</a>
    </div>
    {body}
</div>
</body>
</html>
"""


# ============================================================
# VISTAS
# ============================================================

def render_ejecutivo(cur):
    where, params = month_where("s")
    month, platform, distributor = current_filters()
    start, end = month_range(month)

    cur.execute(f"SELECT COUNT(*) AS plays FROM scrobbles s WHERE {where}", params)
    plays = safe_int(cur.fetchone()["plays"])

    cur.execute("""
        SELECT
            COUNT(*) total,
            SUM(CASE WHEN status='OK' THEN 1 ELSE 0 END) ok_count,
            SUM(CASE WHEN status='WARN' THEN 1 ELSE 0 END) warn_count,
            SUM(CASE WHEN status='INCIDENT' THEN 1 ELSE 0 END) incident_count
        FROM teams
        WHERE active = TRUE
    """)
    st = cur.fetchone()

    # ================= GANANCIAS EJECUTIVAS =================
    cur.execute(f"""
        SELECT LOWER(s.app_name) AS platform, COUNT(*) AS plays
        FROM scrobbles s
        WHERE {where}
        GROUP BY LOWER(s.app_name)
    """, params)
    exec_platforms = cur.fetchall()

    exec_total_min = 0
    exec_total_max = 0

    for r in exec_platforms:
        p = (r["platform"] or "").lower()
        platform_plays = safe_int(r["plays"])
        rate = PLATFORM_RATES.get(p, PLATFORM_RATES["spotify"])
        exec_total_min += platform_plays * rate["min"]
        exec_total_max += platform_plays * rate["max"]

    exec_avg_gain = (exec_total_min + exec_total_max) / 2

    # ================= PROYECCIÓN MENSUAL =================
    now = datetime.utcnow()

    total_days_month = (end - start).days

    if now < start:
        elapsed_days = 0
    elif now >= end:
        elapsed_days = total_days_month
    else:
        elapsed_days = max(1, (now.date() - start.date()).days + 1)

    daily_avg_plays = plays / max(elapsed_days, 1)
    projected_plays = int(daily_avg_plays * total_days_month)

    projected_avg_gain = 0
    projected_max_gain = 0

    if plays > 0:
        projected_avg_gain = (exec_avg_gain / plays) * projected_plays
        projected_max_gain = (exec_total_max / plays) * projected_plays

    # ================= TOP ARTISTAS =================
    cur.execute(f"""
        SELECT s.artist_name, COUNT(*) AS plays
        FROM scrobbles s
        WHERE {where}
        GROUP BY s.artist_name
        ORDER BY plays DESC
        LIMIT 8
    """, params)
    artists = cur.fetchall()

    artist_html = "".join([
        f"<div class='mini-row'><span>{r['artist_name']}</span><strong>{r['plays']}</strong></div>"
        for r in artists
    ]) or '<div class="muted">Sin datos</div>'

    # ================= DISTRIBUIDORA =================
    cur.execute(f"""
        SELECT COALESCE(am.distributor, 'Sin distribuidora') AS distributor, COUNT(*) AS plays
        FROM scrobbles s
        LEFT JOIN artist_metadata am ON LOWER(am.artist_name) = LOWER(s.artist_name)
        WHERE {where}
        GROUP BY COALESCE(am.distributor, 'Sin distribuidora')
        ORDER BY plays DESC
    """, params)
    dist = cur.fetchall()

    dist_labels = [r["distributor"] for r in dist]
    dist_values = [safe_int(r["plays"]) for r in dist]

    # ================= PLAYS DIARIOS =================
    cur.execute(f"""
        SELECT DATE(s.scrobble_time) AS play_day, COUNT(*) AS plays
        FROM scrobbles s
        WHERE {where}
        GROUP BY DATE(s.scrobble_time)
        ORDER BY play_day ASC
    """, params)
    daily = cur.fetchall()

    daily_labels = [str(r["play_day"])[5:] for r in daily]
    daily_values = [safe_int(r["plays"]) for r in daily]

    # ================= MENSAJE EJECUTIVO =================
    if projected_plays > plays:
        insight = f"Si mantiene este ritmo, el mes podría cerrar cerca de {projected_plays:,} plays."
    else:
        insight = "El mes ya está cerrado o sin datos suficientes para proyección."

    return f"""
    {filter_form("ejecutivo")}

    <div class="grid">
        <div class="card">
            <div class="label">Plays filtrados</div>
            <div class="value blue">{plays:,}</div>
        </div>

        <div class="card">
            <div class="label">Ganancia promedio</div>
            <div class="value green">{money(exec_avg_gain)}</div>
        </div>

        <div class="card">
            <div class="label">Ganancia máxima</div>
            <div class="value yellow">{money(exec_total_max)}</div>
        </div>

        <div class="card">
            <div class="label">Proyección plays mes</div>
            <div class="value">{projected_plays:,}</div>
        </div>
    </div>

    <div class="grid-3">
        <div class="card">
            <div class="label">Proyección ganancia promedio</div>
            <div class="value green">{money(projected_avg_gain)}</div>
        </div>

        <div class="card">
            <div class="label">Proyección ganancia máxima</div>
            <div class="value yellow">{money(projected_max_gain)}</div>
        </div>

        <div class="card">
            <div class="label">Equipos activos</div>
            <div class="value">{safe_int(st['total'])}</div>
        </div>
    </div>

    <div class="card" style="margin-bottom:18px;">
        <div class="section-title">Resumen ejecutivo</div>
        <div class="mini-row">
            <span>{insight}</span>
            <strong>Proyección</strong>
        </div>
        <div class="mini-row">
            <span>Días considerados del mes</span>
            <strong>{elapsed_days} / {total_days_month}</strong>
        </div>
        <div class="mini-row">
            <span>Promedio diario de plays</span>
            <strong>{round(daily_avg_plays, 2):,}</strong>
        </div>
    </div>

    <div class="grid-2">
        <div class="card">
            <div class="section-title">Tendencia diaria</div>
            <canvas id="execDailyChart"></canvas>
        </div>

        <div class="card">
            <div class="section-title">Plays por distribuidora</div>
            <canvas id="execDistChart"></canvas>
        </div>
    </div>

    <div class="grid-2">
        <div class="card">
            <div class="section-title">Top artistas</div>
            {artist_html}
        </div>

        <div class="card">
            <div class="section-title">Lectura ejecutiva</div>
            <div class="mini-row"><span>Ganancia actual promedio</span><strong>{money(exec_avg_gain)}</strong></div>
            <div class="mini-row"><span>Ganancia actual máxima</span><strong>{money(exec_total_max)}</strong></div>
            <div class="mini-row"><span>Proyección promedio mensual</span><strong>{money(projected_avg_gain)}</strong></div>
            <div class="mini-row"><span>Proyección máxima mensual</span><strong>{money(projected_max_gain)}</strong></div>
        </div>
    </div>

    <script>
    new Chart(document.getElementById('execDailyChart'), {{
        type:'line',
        data:{{
            labels:{json.dumps(daily_labels)},
            datasets:[{{
                label:'Plays diarios',
                data:{json.dumps(daily_values)},
                borderColor:'#34d399',
                backgroundColor:'rgba(52,211,153,.15)',
                tension:.35,
                fill:true
            }}]
        }},
        options:{{
            responsive:true,
            plugins:{{legend:{{labels:{{color:'#e5e7eb'}}}}}},
            scales:{{x:{{ticks:{{color:'#94a3b8'}}}},y:{{ticks:{{color:'#94a3b8'}}}}}}
        }}
    }});

    new Chart(document.getElementById('execDistChart'), {{
        type:'bar',
        data:{{
            labels:{json.dumps(dist_labels)},
            datasets:[{{
                label:'Plays',
                data:{json.dumps(dist_values)},
                backgroundColor:'rgba(96,165,250,.30)',
                borderColor:'#60a5fa',
                borderWidth:1
            }}]
        }},
        options:{{
            responsive:true,
            plugins:{{legend:{{labels:{{color:'#e5e7eb'}}}}}},
            scales:{{x:{{ticks:{{color:'#94a3b8'}}}},y:{{ticks:{{color:'#94a3b8'}}}}}}
        }}
    }});
    </script>
    """


def render_monitor(cur):
    where, params = month_where("s")
    _, platform, _ = current_filters()

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

    cur.execute(f"SELECT COUNT(*) total_filtered FROM scrobbles s WHERE {where}", params)
    total_filtered = safe_int(cur.fetchone()["total_filtered"])

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
            <td><input type="checkbox" name="team_ids" value="{t['id']}"> {t['id']}</td>
            <td>{t['name']}</td>
            <td>{t['app_name']}</td>
            <td>{t['lastfm_user']}</td>
            <td>{badge(t['status'])}</td>
            <td>{t['last_scrobble_at'] or '-'}</td>
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
                    <option value="spotify">spotify</option>
                    <option value="apple">apple</option>
                    <option value="tidal">tidal</option>
                    <option value="youtube">youtube</option>
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
                        <option value="spotify">spotify</option>
                        <option value="apple">apple</option>
                        <option value="tidal">tidal</option>
                        <option value="youtube">youtube</option>
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
    <form method="POST" action="/collect-all-selected">
        <div style="margin-bottom:12px;">
            <button class="btn btn-primary" type="submit">Collect All seleccionados</button>
        </div>

        <table>
            <thead>
                <tr>
                    <th>ID</th><th>Equipo</th><th>App</th><th>User</th><th>Status</th>
                    <th>Último scrobble</th><th>Idle</th><th>Último check</th><th>Acciones</th>
                </tr>
            </thead>
            <tbody>{rows}</tbody>
        </table>
    </form>
    """


def render_analisis(cur):
    where, params = month_where("s")
    _, platform, _ = current_filters()

    today_where = "DATE(s.scrobble_time)=CURRENT_DATE"
    today_params = []

    if platform:
        today_where += " AND LOWER(s.app_name)=%s"
        today_params.append(platform)

    cur.execute(f"SELECT COUNT(*) c FROM scrobbles s WHERE {today_where}", today_params)
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

    user_rows = "".join([
        f"<tr><td>{r['lastfm_user']}</td><td>{r['plays']}</td></tr>"
        for r in users
    ]) or '<tr><td colspan="2" class="muted">Sin datos</td></tr>'

    cur.execute(f"""
        SELECT s.artist_name, COUNT(*) AS plays
        FROM scrobbles s
        WHERE {where}
        GROUP BY s.artist_name
        ORDER BY plays DESC
        LIMIT 10
    """, params)
    artists = cur.fetchall()

    artist_html = "".join([
        f"<div class='mini-row'><span>{r['artist_name']}</span><strong>{r['plays']}</strong></div>"
        for r in artists
    ]) or '<div class="muted">Sin datos</div>'

    cur.execute(f"""
        SELECT
            COALESCE(am.distributor, 'Sin distribuidora') AS distributor,
            COUNT(*) AS plays
        FROM scrobbles s
        LEFT JOIN artist_metadata am
            ON LOWER(am.artist_name) = LOWER(s.artist_name)
        WHERE {where}
        GROUP BY COALESCE(am.distributor, 'Sin distribuidora')
        ORDER BY plays DESC
    """, params)
    distributor_rows = cur.fetchall()
    distributor_labels = [r["distributor"] for r in distributor_rows]
    distributor_values = [safe_int(r["plays"]) for r in distributor_rows]

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

    <div class="card" style="margin-bottom:18px;">
        <div class="section-title">Reproducciones por distribuidora</div>
        <canvas id="distributorChart"></canvas>
    </div>

    <div class="grid-2">
        <div class="card"><div class="section-title">Top artistas</div>{artist_html}</div>
        <div>
            <div class="section-title">Reproducciones por usuario Last.fm</div>
            <table>
                <thead><tr><th>Usuario</th><th>Reproducciones</th></tr></thead>
                <tbody>{user_rows}</tbody>
            </table>
        </div>
    </div>

    <script>
    new Chart(document.getElementById('playsChart'), {{
        type:'line',
        data:{{
            labels:{json.dumps(labels)},
            datasets:[{{
                label:'Reproducciones',
                data:{json.dumps(values)},
                borderColor:'#60a5fa',
                backgroundColor:'rgba(96,165,250,.15)',
                tension:.35,
                fill:true
            }}]
        }},
        options:{{
            responsive:true,
            plugins:{{legend:{{labels:{{color:'#e5e7eb'}}}}}},
            scales:{{x:{{ticks:{{color:'#94a3b8'}}}},y:{{ticks:{{color:'#94a3b8'}}}}}}
        }}
    }});

    new Chart(document.getElementById('distributorChart'), {{
        type:'bar',
        data:{{
            labels:{json.dumps(distributor_labels)},
            datasets:[{{
                label:'Reproducciones por distribuidora',
                data:{json.dumps(distributor_values)},
                backgroundColor:'rgba(168,85,247,.35)',
                borderColor:'#a855f7',
                borderWidth:1
            }}]
        }},
        options:{{
            responsive:true,
            plugins:{{legend:{{labels:{{color:'#e5e7eb'}}}}}},
            scales:{{x:{{ticks:{{color:'#94a3b8'}}}},y:{{ticks:{{color:'#94a3b8'}}}}}}
        }}
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

    artist_rows = "".join([
        f"<tr><td>{a}</td><td>{money(v)}</td></tr>"
        for a, v in sorted(artist_map.items(), key=lambda x: x[1], reverse=True)[:20]
    ]) or '<tr><td colspan="2" class="muted">Sin datos</td></tr>'

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

    user_rows = "".join([
        f"<tr><td>{u}</td><td>{money(v)}</td></tr>"
        for u, v in sorted(user_map.items(), key=lambda x: x[1], reverse=True)[:25]
    ]) or '<tr><td colspan="2" class="muted">Sin datos</td></tr>'

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
            <table>
                <thead><tr><th>Plataforma</th><th>Streams</th><th>Min</th><th>Max</th><th>Promedio</th></tr></thead>
                <tbody>{platform_rows}</tbody>
            </table>
        </div>
        <div class="card">
            <div class="section-title">Recomendación ejecutiva</div>
            <div class="mini-row"><span>Empuja usuarios con más generación</span><strong>ROI</strong></div>
            <div class="mini-row"><span>Cruza con Monitor Plays</span><strong>Meta 1K</strong></div>
            <div class="mini-row"><span>Prioriza artistas con alza diaria</span><strong>Escala</strong></div>
        </div>
    </div>

    <div class="grid-2">
        <div>
            <div class="section-title">Ganancias por artista</div>
            <table><thead><tr><th>Artista</th><th>Ganancia</th></tr></thead><tbody>{artist_rows}</tbody></table>
        </div>
        <div>
            <div class="section-title">Ganancias por usuario Last.fm</div>
            <table><thead><tr><th>Usuario</th><th>Ganancia</th></tr></thead><tbody>{user_rows}</tbody></table>
        </div>
    </div>

    <script>
    new Chart(document.getElementById('gainChart'), {{
        type:'line',
        data:{{
            labels:{json.dumps(gain_labels)},
            datasets:[{{
                label:'Ganancia estimada',
                data:{json.dumps(gain_values)},
                borderColor:'#34d399',
                backgroundColor:'rgba(52,211,153,.15)',
                tension:.35,
                fill:true
            }}]
        }},
        options:{{
            responsive:true,
            plugins:{{legend:{{labels:{{color:'#e5e7eb'}}}}}},
            scales:{{x:{{ticks:{{color:'#94a3b8'}}}},y:{{ticks:{{color:'#94a3b8'}}}}}}
        }}
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
        LIMIT 200
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
            <td>{r['artist_name']}</td>
            <td>{r['track_name']}</td>
            <td>{plays}</td>
            <td>{faltan}</td>
            <td>Dar {faltan} reproducciones</td>
            <td>{estado}</td>
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
        <thead>
            <tr><th>Artista</th><th>Canción</th><th>Plays</th><th>Faltan 1K</th><th>Recomendación</th><th>Estado</th></tr>
        </thead>
        <tbody>{table_rows}</tbody>
    </table>
    """


# ============================================================
# RUTAS PRINCIPALES
# ============================================================

@app.route("/")
def home():
    try:
        init_db()
        view = (request.args.get("view") or "ejecutivo").strip().lower()

        conn = get_conn()
        cur = conn.cursor()

        if view == "monitor":
            body = render_monitor(cur)
            title = "Monitoreo operativo"
        elif view == "analisis":
            body = render_analisis(cur)
            title = "Vista analítica pro"
        elif view == "ganancias":
            body = render_ganancias(cur)
            title = "Vista de ganancias pro"
        elif view == "monitor-plays":
            body = render_monitor_plays(cur)
            title = "Seguimiento de canciones debajo de 1000"
        else:
            view = "ejecutivo"
            body = render_ejecutivo(cur)
            title = "Tablero ejecutivo"

        cur.close()
        conn.close()
        return base_page(title, view, body)

    except Exception as e:
        return f"<pre>ERROR EN HOME:\n{str(e)}</pre>", 500


# ============================================================
# EQUIPOS
# ============================================================

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


# ============================================================
# JOBS
# ============================================================

@app.route("/run-check")
def run_check():
    return run_python_script("watch_scrobbles.py", timeout=900)


@app.route("/collect-now")
def collect_now():
    start_logged_job("collect_scrobbles.py", "collect-now")
    return redirect("/job-log?job=collect-now")


@app.route("/collect-all")
def collect_all():
    start_logged_job("backfill_scrobbles.py", "collect-all")
    return redirect("/job-log?job=collect-all")


@app.route("/collect-all-selected", methods=["POST"])
def collect_all_selected():
    team_ids = request.form.getlist("team_ids")

    if not team_ids:
        return """
        <body style="background:#061126;color:white;font-family:Arial;padding:24px;">
            <h2>No seleccionaste equipos</h2>
            <a style="color:#93c5fd;" href="/?view=monitor">Volver</a>
        </body>
        """

    ids_text = ",".join(team_ids)

    start_logged_job(
        "backfill_scrobbles.py",
        "collect-all-selected",
        extra_env={"TEAM_IDS": ids_text},
    )

    return redirect("/job-log?job=collect-all-selected")


@app.route("/collect_now")
def collect_now_alias():
    return collect_now()


@app.route("/collect_all")
def collect_all_alias():
    return collect_all()


@app.route("/job-log")
def job_log():
    job = request.args.get("job", "collect-now")
    log_path = os.path.join(JOB_LOG_DIR, f"{job}.log")

    if not os.path.exists(log_path):
        content = "Job iniciado. Esperando logs..."
    else:
        with open(log_path, "r", encoding="utf-8", errors="ignore") as f:
            content = f.read()

    return f"""
    <html>
    <head>
        <meta http-equiv="refresh" content="5">
        <title>WatchEagle Job Log</title>
        <style>
            body {{
                background:#061126;
                color:white;
                font-family:Arial;
                padding:24px;
            }}
            pre {{
                background:#020617;
                color:#d1d5db;
                border:1px solid #334155;
                border-radius:14px;
                padding:18px;
                white-space:pre-wrap;
            }}
            a {{ color:#93c5fd; }}
        </style>
    </head>
    <body>
        <h1>WatchEagle Job Log</h1>
        <p><a href="/">Volver al dashboard</a></p>
        <p>Se actualiza cada 5 segundos.</p>
        <pre>{content}</pre>
    </body>
    </html>
    """


# ============================================================
# INIT / HEALTH
# ============================================================

@app.route("/init-artist-metadata")
def init_artist_metadata():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS artist_metadata (
            id SERIAL PRIMARY KEY,
            artist_name TEXT UNIQUE NOT NULL,
            author TEXT,
            distributor TEXT
        )
    """)

    for artist, author, distributor in ARTIST_METADATA:
        cur.execute("""
            INSERT INTO artist_metadata (artist_name, author, distributor)
            VALUES (%s, %s, %s)
            ON CONFLICT (artist_name)
            DO UPDATE SET
                author = EXCLUDED.author,
                distributor = EXCLUDED.distributor
        """, (artist, author, distributor))

    conn.commit()
    cur.close()
    conn.close()

    return jsonify({"ok": True, "inserted_or_updated": len(ARTIST_METADATA)})


@app.route("/init-performance-indexes")
def init_performance_indexes():
    conn = get_conn()
    cur = conn.cursor()

    statements = [
        "CREATE INDEX IF NOT EXISTS idx_scrobbles_time ON scrobbles (scrobble_time)",
        "CREATE INDEX IF NOT EXISTS idx_scrobbles_app_time ON scrobbles (LOWER(app_name), scrobble_time)",
        "CREATE INDEX IF NOT EXISTS idx_scrobbles_artist_lower ON scrobbles (LOWER(artist_name))",
        "CREATE INDEX IF NOT EXISTS idx_scrobbles_user_time ON scrobbles (lastfm_user, scrobble_time)",
        "CREATE INDEX IF NOT EXISTS idx_scrobbles_artist_track_time ON scrobbles (artist_name, track_name, scrobble_time)",
        "CREATE INDEX IF NOT EXISTS idx_artist_metadata_artist_lower ON artist_metadata (LOWER(artist_name))",
        "CREATE INDEX IF NOT EXISTS idx_artist_metadata_distributor ON artist_metadata (distributor)",
        "CREATE INDEX IF NOT EXISTS idx_teams_app ON teams (LOWER(app_name))",
    ]

    for sql in statements:
        cur.execute(sql)

    conn.commit()
    cur.close()
    conn.close()

    return jsonify({"ok": True, "created": len(statements)})


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

from flask import Flask, request, jsonify, redirect
import subprocess
import os
import sys
from helpers import get_conn, init_db

app = Flask(__name__)

# =========================
# CONFIG
# =========================

PLATFORM_RATES = {
    "spotify": {"min": 0.0035, "max": 0.0050},
    "apple": {"min": 0.0070, "max": 0.0100},
    "apple music": {"min": 0.0070, "max": 0.0100},
    "tidal": {"min": 0.0120, "max": 0.0150},
    "youtube": {"min": 0.0007, "max": 0.0020},
    "youtube music": {"min": 0.0007, "max": 0.0020},
}

# =========================
# HELPERS
# =========================

def safe_int(v, d=0):
    try:
        return int(v or 0)
    except:
        return d

def format_money(v):
    return f"${v:,.2f}"

def avg_rate(platform):
    p = (platform or "").lower()
    r = PLATFORM_RATES.get(p, PLATFORM_RATES["spotify"])
    return (r["min"] + r["max"]) / 2

# 🔥 FIX CLAVE: ejecutar scripts correctamente en Render
def run_python_script(script_name):
    try:
        script_path = os.path.join(os.getcwd(), script_name)

        if not os.path.exists(script_path):
            return f"<pre>ERROR: No existe {script_path}</pre>", 500

        result = subprocess.run(
            [sys.executable, script_path],
            capture_output=True,
            text=True,
            timeout=1800
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
    except Exception as e:
        return f"<pre>ERROR ejecutando {script_name}: {str(e)}</pre>", 500

# =========================
# UI BASE
# =========================

def base(title, body):
    return f"""
    <html>
    <head>
        <title>WatchEagle</title>
        <style>
            body {{
                background:#061126;
                color:white;
                font-family:Arial;
                padding:20px;
            }}
            a {{ color:#60a5fa; margin-right:10px; }}
            table {{ width:100%; border-collapse:collapse; margin-top:10px; }}
            td,th {{ padding:10px; border-bottom:1px solid #1e293b; }}
            .card {{
                background:#0b1730;
                padding:15px;
                border-radius:10px;
                margin-bottom:15px;
            }}
        </style>
    </head>
    <body>
        <h1>WatchEagle</h1>

        <div>
            <a href="/">Monitor</a>
            <a href="/?view=analisis">Analisis</a>
            <a href="/?view=ganancias">Ganancias</a>
            <a href="/?view=monitor-plays">Monitor Plays</a>
        </div>

        <hr>

        <div>
            <a href="/run-check">run-check</a>
            <a href="/collect-now">collect-now</a>
            <a href="/collect-all">collect-all</a>
        </div>

        <hr>

        <h2>{title}</h2>

        {body}
    </body>
    </html>
    """

# =========================
# VISTAS
# =========================

def view_monitor(cur):
    cur.execute("SELECT * FROM teams ORDER BY id")
    teams = cur.fetchall()

    rows = ""
    for t in teams:
        rows += f"""
        <tr>
            <td>{t['id']}</td>
            <td>{t['name']}</td>
            <td>{t['app_name']}</td>
            <td>{t['lastfm_user']}</td>
            <td>
                <a href="/delete-team?id={t['id']}">❌</a>
            </td>
        </tr>
        """

    return f"""
    <div class="card">
        <form action="/seed-team">
            <input name="name" placeholder="Equipo">
            <input name="app" placeholder="App">
            <input name="user" placeholder="LastFM user">
            <button>Agregar</button>
        </form>
    </div>

    <table>
        <tr>
            <th>ID</th>
            <th>Equipo</th>
            <th>App</th>
            <th>User</th>
            <th></th>
        </tr>
        {rows}
    </table>
    """

def view_analisis(cur):
    cur.execute("""
        SELECT DATE(scrobble_time) d, COUNT(*) c
        FROM scrobbles
        GROUP BY d
        ORDER BY d DESC
        LIMIT 15
    """)
    rows = cur.fetchall()

    data = "".join([f"<tr><td>{r['d']}</td><td>{r['c']}</td></tr>" for r in rows])

    return f"""
    <table>
        <tr><th>Día</th><th>Reproducciones</th></tr>
        {data}
    </table>
    """

def view_ganancias(cur):
    cur.execute("""
        SELECT artist_name, COUNT(*) plays
        FROM scrobbles
        GROUP BY artist_name
        ORDER BY plays DESC
        LIMIT 20
    """)
    rows = cur.fetchall()

    html = ""
    for r in rows:
        money = r["plays"] * avg_rate("spotify")
        html += f"<tr><td>{r['artist_name']}</td><td>{format_money(money)}</td></tr>"

    return f"""
    <table>
        <tr><th>Artista</th><th>Ganancia</th></tr>
        {html}
    </table>
    """

def view_monitor_plays(cur):
    cur.execute("""
        SELECT artist_name, track_name, COUNT(*) c
        FROM scrobbles
        GROUP BY artist_name, track_name
        HAVING COUNT(*) < 1000
        ORDER BY c DESC
        LIMIT 50
    """)
    rows = cur.fetchall()

    html = ""
    for r in rows:
        faltan = 1000 - r["c"]
        html += f"""
        <tr>
            <td>{r['artist_name']}</td>
            <td>{r['track_name']}</td>
            <td>{r['c']}</td>
            <td>{faltan}</td>
        </tr>
        """

    return f"""
    <table>
        <tr>
            <th>Artista</th>
            <th>Canción</th>
            <th>Plays</th>
            <th>Faltan</th>
        </tr>
        {html}
    </table>
    """

# =========================
# ROUTES
# =========================

@app.route("/")
def home():
    try:
        init_db()
        view = request.args.get("view", "monitor")

        conn = get_conn()
        cur = conn.cursor()

        if view == "analisis":
            body = view_analisis(cur)
        elif view == "ganancias":
            body = view_ganancias(cur)
        elif view == "monitor-plays":
            body = view_monitor_plays(cur)
        else:
            body = view_monitor(cur)

        cur.close()
        conn.close()

        return base(view, body)

    except Exception as e:
        return f"<pre>{str(e)}</pre>", 500


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
        ON CONFLICT DO NOTHING
    """, (name, app_name, user))

    conn.commit()
    cur.close()
    conn.close()

    return redirect("/")


@app.route("/delete-team")
def delete_team():
    id = request.args.get("id")

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("DELETE FROM teams WHERE id=%s", (id,))
    conn.commit()

    cur.close()
    conn.close()

    return redirect("/")


# =========================
# JOBS (FIXED)
# =========================

@app.route("/run-check")
def run_check():
    return run_python_script("watch_scrobbles.py")


@app.route("/collect-now")
def collect_now():
    return run_python_script("collect_scrobbles.py")


@app.route("/collect-all")
def collect_all():
    return run_python_script("backfill_scrobbles.py")


# =========================
# HEALTH
# =========================

@app.route("/healthz")
def healthz():
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM teams")
        cur.close()
        conn.close()
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}, 500


# =========================

if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=10000)

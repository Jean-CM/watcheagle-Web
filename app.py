from flask import Flask, request, redirect
import psycopg2
from psycopg2.extras import RealDictCursor
import os
import subprocess
import sys
import threading

app = Flask(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL")


# =========================
# DB
# =========================
def get_conn():
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)


# =========================
# SCRIPT RUNNER
# =========================
def run_python_script(script_name):
    try:
        script_path = os.path.join(os.getcwd(), script_name)

        result = subprocess.run(
            [sys.executable, script_path],
            capture_output=True,
            text=True,
            timeout=600
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
        return f"<pre>ERROR: {str(e)}</pre>"


# =========================
# BACKGROUND (CLAVE)
# =========================
def run_background(script):
    def task():
        subprocess.run([sys.executable, os.path.join(os.getcwd(), script)])

    threading.Thread(target=task).start()


# =========================
# ROUTES JOBS
# =========================
@app.route("/collect-now")
def collect_now():
    return run_python_script("collect_scrobbles.py")


@app.route("/collect-all")
def collect_all():
    run_background("backfill_scrobbles.py")
    return "<h3>Backfill iniciado en background 🚀</h3>"


@app.route("/run-check")
def run_check():
    return run_python_script("watch_scrobbles.py")


# =========================
# CRUD TEAMS
# =========================
@app.route("/add", methods=["POST"])
def add():
    name = request.form["name"]
    app_name = request.form["app"]
    user = request.form["user"]

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO teams (name, app_name, lastfm_user)
        VALUES (%s, %s, %s)
    """, (name, app_name, user))

    conn.commit()
    cur.close()
    conn.close()

    return redirect("/")


@app.route("/delete/<int:id>")
def delete(id):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("DELETE FROM teams WHERE id = %s", (id,))

    conn.commit()
    cur.close()
    conn.close()

    return redirect("/")


@app.route("/edit/<int:id>", methods=["POST"])
def edit(id):
    name = request.form["name"]
    app_name = request.form["app"]
    user = request.form["user"]

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        UPDATE teams
        SET name=%s, app_name=%s, lastfm_user=%s
        WHERE id=%s
    """, (name, app_name, user, id))

    conn.commit()
    cur.close()
    conn.close()

    return redirect("/")


# =========================
# MAIN DASHBOARD
# =========================
@app.route("/")
def index():
    view = request.args.get("view", "monitor")

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT * FROM teams ORDER BY id")
    teams = cur.fetchall()

    cur.execute("SELECT COUNT(*) FROM teams WHERE active = TRUE")
    total = cur.fetchone()["count"]

    cur.execute("SELECT COUNT(*) FROM teams WHERE status='OK'")
    ok = cur.fetchone()["count"]

    cur.execute("SELECT COUNT(*) FROM teams WHERE status='WARN'")
    warn = cur.fetchone()["count"]

    cur.execute("SELECT COUNT(*) FROM teams WHERE status='INCIDENT'")
    incident = cur.fetchone()["count"]

    cur.close()
    conn.close()

    return f"""
    <html>
    <head>
    <style>
    body {{
        background:#0b1220;
        color:white;
        font-family:Arial;
    }}
    a {{
        color:#7c9cff;
        margin-right:10px;
    }}
    .card {{
        display:inline-block;
        padding:20px;
        margin:10px;
        background:#121a2b;
        border-radius:10px;
    }}
    table {{
        width:100%;
        margin-top:20px;
        border-collapse:collapse;
    }}
    th, td {{
        padding:10px;
        border-bottom:1px solid #333;
    }}
    .ok {{color:#00ff88}}
    .warn {{color:orange}}
    .incident {{color:red}}
    </style>
    </head>

    <body>

    <h1>WatchEagle</h1>

    <div>
        <a href="/?view=monitor">Monitor</a>
        <a href="/?view=analysis">Análisis</a>
        <a href="/?view=gains">Ganancias</a>
        <a href="/?view=plays">Monitor Plays</a>
    </div>

    <br>

    <div>
        <a href="/run-check">run-check</a>
        <a href="/collect-now">collect-now</a>
        <a href="/collect-all">collect-all</a>
    </div>

    <div class="card">Monitores: {total}</div>
    <div class="card ok">OK: {ok}</div>
    <div class="card warn">WARN: {warn}</div>
    <div class="card incident">INCIDENT: {incident}</div>

    <h2>Agregar equipo</h2>
    <form method="post" action="/add">
        <input name="name" placeholder="Equipo">
        <input name="app" placeholder="App">
        <input name="user" placeholder="LastFM user">
        <button>Agregar</button>
    </form>

    <table>
        <tr>
            <th>ID</th>
            <th>Equipo</th>
            <th>App</th>
            <th>User</th>
            <th>Status</th>
            <th>Acciones</th>
        </tr>

        {''.join([f'''
        <tr>
            <td>{t["id"]}</td>
            <td>{t["name"]}</td>
            <td>{t["app_name"]}</td>
            <td>{t["lastfm_user"]}</td>
            <td class="{t["status"].lower()}">{t["status"]}</td>
            <td>
                <a href="/delete/{t["id"]}">❌</a>
            </td>
        </tr>
        ''' for t in teams])}

    </table>

    </body>
    </html>
    """

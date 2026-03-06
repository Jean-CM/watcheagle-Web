from flask import Flask, request, jsonify
import os
import psycopg2
from psycopg2.extras import RealDictCursor

app = Flask(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL")


def get_conn():
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)


def init_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS teams (
        id SERIAL PRIMARY KEY,
        name VARCHAR(50) NOT NULL,
        app_name VARCHAR(50) NOT NULL,
        lastfm_user VARCHAR(100) NOT NULL UNIQUE,
        status VARCHAR(20) DEFAULT 'PENDING',
        last_scrobble_at TIMESTAMP NULL,
        last_check_at TIMESTAMP NULL,
        idle_minutes INTEGER DEFAULT 0,
        active BOOLEAN DEFAULT TRUE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)

    conn.commit()
    cur.close()
    conn.close()


@app.route("/")
def home():
    init_db()

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, name, app_name, lastfm_user, status, idle_minutes, last_check_at
        FROM teams
        WHERE active = TRUE
        ORDER BY id ASC;
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
            <td>{t['idle_minutes']}</td>
            <td>{t['last_check_at'] or '-'}</td>
        </tr>
        """

    html = f"""
    <html>
    <head>
        <title>WatchEagle</title>
        <style>
            body {{
                font-family: Arial, sans-serif;
                background: #071226;
                color: #e5e7eb;
                margin: 0;
                padding: 30px;
            }}
            h1 {{
                margin-bottom: 10px;
            }}
            .card {{
                background: #0f1b33;
                padding: 20px;
                border-radius: 12px;
                margin-bottom: 20px;
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
                background: #0f1b33;
                border-radius: 12px;
                overflow: hidden;
            }}
            th, td {{
                padding: 12px;
                border-bottom: 1px solid #1f2937;
                text-align: left;
            }}
            th {{
                background: #1a2740;
            }}
            .ok {{
                color: #22c55e;
                font-weight: bold;
            }}
            .warn {{
                color: #f59e0b;
                font-weight: bold;
            }}
            .incident {{
                color: #ef4444;
                font-weight: bold;
            }}
            .hint {{
                margin-top: 14px;
                font-size: 14px;
                color: #9ca3af;
            }}
            code {{
                background: #111827;
                padding: 2px 6px;
                border-radius: 6px;
            }}
        </style>
    </head>
    <body>
        <h1>WatchEagle</h1>
        <div class="card">
            <p><strong>Estado:</strong> Dashboard activo</p>
            <p><strong>Monitores activos:</strong> {len(teams)}</p>
            <p class="hint">Carga equipos con: <code>/seed-team?name=Equipo%2001&app=spotify&user=equipoC01</code></p>
            <p class="hint">Actualiza equipos con: <code>/update-team?id=1&name=Equipo%2001&app=spotify&user=equipoC01</code></p>
        </div>

        <table>
            <thead>
                <tr>
                    <th>ID</th>
                    <th>Equipo</th>
                    <th>App</th>
                    <th>Usuario Last.fm</th>
                    <th>Estado</th>
                    <th>Idle (min)</th>
                    <th>Último check</th>
                </tr>
            </thead>
            <tbody>
                {rows if rows else '<tr><td colspan="7">No hay equipos cargados todavía.</td></tr>'}
            </tbody>
        </table>
    </body>
    </html>
    """
    return html


@app.route("/health")
def health():
    init_db()
    return jsonify({"ok": True, "service": "WatchEagle"})


@app.route("/seed-team")
def seed_team():
    init_db()

    name = request.args.get("name")
    app_name = request.args.get("app")
    lastfm_user = request.args.get("user")

    if not name or not app_name or not lastfm_user:
        return jsonify({
            "ok": False,
            "error": "Faltan parámetros. Usa ?name=Equipo%2001&app=spotify&user=equipoC01"
        }), 400

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO teams (name, app_name, lastfm_user, status)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (lastfm_user)
        DO NOTHING
        RETURNING id, name, app_name, lastfm_user, status;
    """, (name, app_name, lastfm_user, "PENDING"))

    row = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()

    if row:
        return jsonify({"ok": True, "created": row})

    return jsonify({
        "ok": True,
        "message": "Ese usuario ya existía, no se duplicó."
    })


@app.route("/update-team")
def update_team():
    init_db()

    team_id = request.args.get("id")
    name = request.args.get("name")
    app_name = request.args.get("app")
    lastfm_user = request.args.get("user")

    if not team_id or not name or not app_name or not lastfm_user:
        return jsonify({
            "ok": False,
            "error": "Faltan parámetros. Usa ?id=1&name=Equipo%2001&app=spotify&user=equipoC01"
        }), 400

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        UPDATE teams
        SET name = %s,
            app_name = %s,
            lastfm_user = %s
        WHERE id = %s
        RETURNING id, name, app_name, lastfm_user, status;
    """, (name, app_name, lastfm_user, team_id))

    row = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()

    if row:
        return jsonify({"ok": True, "updated": row})

    return jsonify({"ok": False, "error": "No se encontró el equipo"}), 404


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)

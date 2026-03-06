from flask import Flask
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
        rows += f"""
        <tr>
            <td>{t['id']}</td>
            <td>{t['name']}</td>
            <td>{t['app_name']}</td>
            <td>{t['lastfm_user']}</td>
            <td>{t['status']}</td>
            <td>{t['idle_minutes']}</td>
            <td>{t['last_check_at'] or '-'}</td>
        </tr>
        """

    html = f"""
    <html>
    <head>
        <title>Last.fm Watchdog</title>
        <style>
            body {{
                font-family: Arial, sans-serif;
                background: #0b1220;
                color: #e5e7eb;
                margin: 0;
                padding: 30px;
            }}
            h1 {{
                margin-bottom: 10px;
            }}
            .card {{
                background: #111827;
                padding: 20px;
                border-radius: 12px;
                margin-bottom: 20px;
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
                background: #111827;
                border-radius: 12px;
                overflow: hidden;
            }}
            th, td {{
                padding: 12px;
                border-bottom: 1px solid #1f2937;
                text-align: left;
            }}
            th {{
                background: #1f2937;
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
        </style>
    </head>
    <body>
        <h1>Last.fm Watchdog</h1>
        <div class="card">
            <p><strong>Estado:</strong> Dashboard activo</p>
            <p><strong>Monitores activos:</strong> {len(teams)}</p>
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


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)

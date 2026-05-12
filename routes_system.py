from datetime import datetime

from utils import safe_int
from layout import badge


def db_size(cur):
    try:
        cur.execute("SELECT pg_size_pretty(pg_database_size(current_database())) AS size")
        return cur.fetchone()['size']
    except Exception:
        return '-'


def table_count(cur, table_name):
    try:
        cur.execute(f"SELECT COUNT(*) AS total FROM {table_name}")
        return safe_int(cur.fetchone()['total'])
    except Exception:
        return 0


def render_sistema(cur):
    total_teams = table_count(cur, 'teams')
    total_scrobbles = table_count(cur, 'scrobbles')
    total_jobs = table_count(cur, 'job_runs')
    database_size = db_size(cur)

    cur.execute('''
        SELECT COUNT(*) AS total
        FROM teams
        WHERE active = TRUE
    ''')
    active_teams = safe_int(cur.fetchone()['total'])

    cur.execute('''
        SELECT COUNT(*) AS total
        FROM teams
        WHERE active = TRUE
          AND status IN ('WARN', 'INCIDENT')
    ''')
    risky_teams = safe_int(cur.fetchone()['total'])

    cur.execute('''
        SELECT COUNT(*) AS total
        FROM scrobbles
        WHERE scrobble_time >= CURRENT_DATE
    ''')
    today_scrobbles = safe_int(cur.fetchone()['total'])

    cur.execute('''
        SELECT COUNT(*) AS total
        FROM scrobbles
        WHERE scrobble_time >= date_trunc('month', CURRENT_DATE)
    ''')
    month_scrobbles = safe_int(cur.fetchone()['total'])

    cur.execute('''
        SELECT job_name, status, started_at, finished_at
        FROM job_runs
        ORDER BY started_at DESC
        LIMIT 12
    ''')
    jobs = cur.fetchall()

    job_rows = ''
    for j in jobs:
        job_rows += f'''
        <tr>
            <td>{j['job_name']}</td>
            <td>{badge(j['status'])}</td>
            <td>{j['started_at'] or '-'}</td>
            <td>{j['finished_at'] or '-'}</td>
            <td><a class="btn btn-secondary" href="/job-log?job={j['job_name']}">Log</a></td>
        </tr>
        '''
    if not job_rows:
        job_rows = '<tr><td colspan="5" class="muted">Sin jobs registrados.</td></tr>'

    cur.execute('''
        SELECT app_name, COUNT(*) AS total
        FROM teams
        WHERE active = TRUE
        GROUP BY app_name
        ORDER BY app_name ASC
    ''')
    apps = cur.fetchall()

    app_rows = ''
    for a in apps:
        app_rows += f'<tr><td>{a["app_name"]}</td><td>{safe_int(a["total"]):,}</td></tr>'
    if not app_rows:
        app_rows = '<tr><td colspan="2" class="muted">Sin equipos activos.</td></tr>'

    cur.execute('''
        SELECT artist_name, COUNT(*) AS plays
        FROM scrobbles
        WHERE scrobble_time >= date_trunc('month', CURRENT_DATE)
        GROUP BY artist_name
        ORDER BY plays DESC
        LIMIT 12
    ''')
    artists = cur.fetchall()

    artist_rows = ''
    for a in artists:
        artist_rows += f'<tr><td>{a["artist_name"]}</td><td>{safe_int(a["plays"]):,}</td></tr>'
    if not artist_rows:
        artist_rows = '<tr><td colspan="2" class="muted">Sin data del mes.</td></tr>'

    health = 'OK'
    if risky_teams > 0:
        health = 'WARN'
    if active_teams == 0 or total_scrobbles == 0:
        health = 'INCIDENT'

    return f'''
    <div class="grid">
        <div class="card"><div class="label">Salud del sistema</div><div class="value">{badge(health)}</div></div>
        <div class="card"><div class="label">Tamaño DB</div><div class="value blue">{database_size}</div></div>
        <div class="card"><div class="label">Equipos activos</div><div class="value green">{active_teams}</div></div>
        <div class="card"><div class="label">Equipos en riesgo</div><div class="value yellow">{risky_teams}</div></div>
    </div>

    <div class="grid">
        <div class="card"><div class="label">Scrobbles total</div><div class="value blue">{total_scrobbles:,}</div></div>
        <div class="card"><div class="label">Scrobbles hoy</div><div class="value green">{today_scrobbles:,}</div></div>
        <div class="card"><div class="label">Scrobbles mes</div><div class="value yellow">{month_scrobbles:,}</div></div>
        <div class="card"><div class="label">Jobs registrados</div><div class="value">{total_jobs:,}</div></div>
    </div>

    <div class="card" style="margin-bottom:18px;">
        <div class="section-title">Acciones técnicas rápidas</div>
        <div style="display:flex; gap:10px; flex-wrap:wrap;">
            <a class="btn btn-primary" href="/cache-clear">Limpiar cache</a>
            <a class="btn btn-secondary" href="/healthz">Healthz</a>
            <a class="btn btn-secondary" href="/scrobbles-count">Scrobbles count</a>
            <a class="btn btn-secondary" href="/cleanup-non-owned-scrobbles-preview">Preview limpieza externos</a>
        </div>
    </div>

    <div class="grid-2">
        <div class="card">
            <div class="section-title">Equipos activos por app</div>
            <table><thead><tr><th>App</th><th>Equipos</th></tr></thead><tbody>{app_rows}</tbody></table>
        </div>
        <div class="card">
            <div class="section-title">Artistas del mes</div>
            <table><thead><tr><th>Artista</th><th>Plays</th></tr></thead><tbody>{artist_rows}</tbody></table>
        </div>
    </div>

    <div class="card">
        <div class="section-title">Últimos jobs</div>
        <table>
            <thead><tr><th>Job</th><th>Status</th><th>Inicio</th><th>Fin</th><th>Log</th></tr></thead>
            <tbody>{job_rows}</tbody>
        </table>
    </div>
    '''

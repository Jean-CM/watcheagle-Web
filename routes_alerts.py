import os

from config import JOB_LOG_DIR
from layout import badge
from utils import safe_int, money


def read_collect_health():
    log_path = os.path.join(JOB_LOG_DIR, 'collect-now.log')
    if not os.path.exists(log_path):
        return {
            'status': 'SIN LOG',
            'return_code': '-',
            'inserted': '-',
            'finished': '-',
            'summary': 'No existe log reciente de collect-now.'
        }

    try:
        with open(log_path, 'r', encoding='utf-8', errors='ignore') as f:
            lines = f.read().splitlines()
    except Exception as e:
        return {
            'status': 'ERROR',
            'return_code': '-',
            'inserted': '-',
            'finished': '-',
            'summary': str(e)
        }

    return_code = '-'
    inserted = '-'
    finished = '-'
    for line in lines:
        if line.startswith('RETURN CODE:'):
            return_code = line.replace('RETURN CODE:', '').strip()
        elif 'Total scrobbles insertados:' in line:
            inserted = line.split('Total scrobbles insertados:', 1)[-1].strip()
        elif line.startswith('FINISHED UTC:'):
            finished = line.replace('FINISHED UTC:', '').strip()

    status = 'OK' if return_code == '0' else 'ERROR' if return_code != '-' else 'PENDING'
    return {
        'status': status,
        'return_code': return_code,
        'inserted': inserted,
        'finished': finished,
        'summary': '\n'.join(lines[-8:]) if lines else 'Sin líneas recientes.'
    }


def render_alertas(cur):
    collect = read_collect_health()

    cur.execute('''
        SELECT COUNT(*) AS total
        FROM teams
        WHERE active = TRUE
          AND last_scrobble_at IS NULL
    ''')
    sin_data = safe_int(cur.fetchone()['total'])

    cur.execute('''
        SELECT COUNT(*) AS total
        FROM teams
        WHERE active = TRUE
          AND COALESCE(idle_minutes, 999999) >= 180
    ''')
    dormidos_3h = safe_int(cur.fetchone()['total'])

    cur.execute('''
        SELECT COUNT(*) AS total
        FROM teams
        WHERE active = TRUE
          AND COALESCE(idle_minutes, 999999) >= 60
          AND COALESCE(idle_minutes, 999999) < 180
    ''')
    dormidos_1h = safe_int(cur.fetchone()['total'])

    cur.execute('''
        SELECT COUNT(*) AS total
        FROM teams t
        LEFT JOIN scrobbles s
            ON s.lastfm_user = t.lastfm_user
           AND s.scrobble_time >= CURRENT_DATE
        WHERE t.active = TRUE
        GROUP BY t.id
        HAVING COUNT(s.id) = 0
    ''')
    cero_hoy_rows = cur.fetchall()
    cero_hoy = len(cero_hoy_rows)

    cur.execute('''
        SELECT id, name, app_name, lastfm_user, status, last_scrobble_at, idle_minutes, last_check_at
        FROM teams
        WHERE active = TRUE
          AND (
                last_scrobble_at IS NULL
                OR COALESCE(idle_minutes, 999999) >= 60
              )
        ORDER BY
            CASE
                WHEN last_scrobble_at IS NULL THEN 1
                WHEN COALESCE(idle_minutes, 999999) >= 180 THEN 2
                WHEN COALESCE(idle_minutes, 999999) >= 60 THEN 3
                ELSE 4
            END,
            COALESCE(idle_minutes, 999999) DESC,
            id ASC
        LIMIT 100
    ''')
    teams = cur.fetchall()

    team_rows = ''
    for t in teams:
        idle = t['idle_minutes']
        if t['last_scrobble_at'] is None:
            alert = '<span class="badge incident">SIN DATA</span>'
            action = 'Validar usuario Last.fm y ejecutar collect'
        elif idle is not None and idle >= 180:
            alert = '<span class="badge incident">CRÍTICO +3H</span>'
            action = 'Revisar equipo/app y correr collect selectivo'
        else:
            alert = '<span class="badge warn">WARN +1H</span>'
            action = 'Monitorear o correr collect'

        team_rows += f'''
        <tr>
            <td>{t['id']}</td>
            <td>{t['name']}</td>
            <td>{t['app_name']}</td>
            <td>{t['lastfm_user']}</td>
            <td>{badge(t['status'])}</td>
            <td>{t['last_scrobble_at'] or '-'}</td>
            <td>{idle if idle is not None else '-'}</td>
            <td>{alert}</td>
            <td>{action}</td>
        </tr>
        '''

    if not team_rows:
        team_rows = '<tr><td colspan="9" class="muted">Sin alertas de equipos. Todo se ve estable.</td></tr>'

    cur.execute('''
        SELECT artist_name, track_name, COUNT(*) AS plays
        FROM scrobbles
        WHERE scrobble_time >= date_trunc('month', CURRENT_DATE)
        GROUP BY artist_name, track_name
        HAVING COUNT(*) < 500
        ORDER BY plays ASC, artist_name ASC, track_name ASC
        LIMIT 50
    ''')
    songs = cur.fetchall()

    song_rows = ''
    for r in songs:
        plays = safe_int(r['plays'])
        missing = max(1000 - plays, 0)
        song_rows += f'''
        <tr>
            <td>{r['artist_name']}</td>
            <td>{r['track_name']}</td>
            <td>{plays:,}</td>
            <td>{missing:,}</td>
            <td>{money(plays * 0.0054)}</td>
            <td><span class="badge incident">ALTA</span></td>
        </tr>
        '''

    if not song_rows:
        song_rows = '<tr><td colspan="6" class="muted">Sin canciones en prioridad alta.</td></tr>'

    risk_score = (sin_data * 3) + (dormidos_3h * 3) + (dormidos_1h * 1) + (1 if collect['status'] == 'ERROR' else 0)
    risk_label = 'OK' if risk_score == 0 else 'WARN' if risk_score <= 5 else 'INCIDENT'

    return f'''
    <div class="grid">
        <div class="card"><div class="label">Estado general</div><div class="value">{badge(risk_label)}</div></div>
        <div class="card"><div class="label">Sin data</div><div class="value red">{sin_data}</div></div>
        <div class="card"><div class="label">Dormidos +3H</div><div class="value red">{dormidos_3h}</div></div>
        <div class="card"><div class="label">Dormidos +1H</div><div class="value yellow">{dormidos_1h}</div></div>
    </div>

    <div class="grid-3">
        <div class="card"><div class="label">Equipos con 0 plays hoy</div><div class="value yellow">{cero_hoy}</div></div>
        <div class="card"><div class="label">Último collect</div><div class="value">{badge(collect['status'])}</div></div>
        <div class="card"><div class="label">Insertados último collect</div><div class="value blue">{collect['inserted']}</div></div>
    </div>

    <div class="card" style="margin-bottom:18px;">
        <div class="section-title">Resumen del último collect</div>
        <div class="mini-row"><span>Return code</span><strong>{collect['return_code']}</strong></div>
        <div class="mini-row"><span>Finalizado</span><strong>{collect['finished']}</strong></div>
        <div class="mini-row"><span>Acción recomendada</span><strong>Si hay ERROR, revisar log antes de correr backfill</strong></div>
        <div style="margin-top:12px;"><a class="btn btn-secondary" href="/job-log?job=collect-now">Ver log collect-now</a></div>
    </div>

    <div class="card" style="margin-bottom:18px;">
        <div class="section-title">Alertas de equipos</div>
        <table>
            <thead><tr><th>ID</th><th>Equipo</th><th>App</th><th>User</th><th>Status</th><th>Último scrobble</th><th>Idle min</th><th>Alerta</th><th>Acción</th></tr></thead>
            <tbody>{team_rows}</tbody>
        </table>
    </div>

    <div class="card">
        <div class="section-title">Canciones prioridad alta del mes</div>
        <table>
            <thead><tr><th>Artista</th><th>Canción</th><th>Plays</th><th>Faltan 1K</th><th>Ganancia</th><th>Prioridad</th></tr></thead>
            <tbody>{song_rows}</tbody>
        </table>
    </div>
    '''

import os
from datetime import datetime

from config import JOB_LOG_DIR
from utils import safe_int
from layout import badge


def read_last_collect_summary():
    log_path = os.path.join(JOB_LOG_DIR, 'collect-now.log')

    if not os.path.exists(log_path):
        return {
            'estado': 'SIN LOG',
            'started': '-',
            'finished': '-',
            'return_code': '-',
            'insertados': '-',
            'lineas': 'Aún no existe log de collect-now.'
        }

    try:
        with open(log_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
    except Exception as e:
        return {
            'estado': 'ERROR',
            'started': '-',
            'finished': '-',
            'return_code': '-',
            'insertados': '-',
            'lineas': str(e)
        }

    lines = content.splitlines()
    started = '-'
    finished = '-'
    return_code = '-'
    insertados = '-'

    for line in lines:
        if line.startswith('STARTED UTC:'):
            started = line.replace('STARTED UTC:', '').strip()
        elif line.startswith('FINISHED UTC:'):
            finished = line.replace('FINISHED UTC:', '').strip()
        elif line.startswith('RETURN CODE:'):
            return_code = line.replace('RETURN CODE:', '').strip()
        elif 'Total scrobbles insertados:' in line:
            insertados = line.split('Total scrobbles insertados:', 1)[-1].strip()

    if return_code == '0':
        estado = 'OK'
    elif return_code == '-':
        estado = 'EN PROCESO'
    else:
        estado = 'ERROR'

    tail = '\n'.join(lines[-12:]) if lines else 'Sin líneas recientes.'

    return {
        'estado': estado,
        'started': started,
        'finished': finished,
        'return_code': return_code,
        'insertados': insertados,
        'lineas': tail
    }


def render_operaciones(cur):
    cur.execute('''
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN status='OK' THEN 1 ELSE 0 END) AS ok_count,
            SUM(CASE WHEN status='WARN' THEN 1 ELSE 0 END) AS warn_count,
            SUM(CASE WHEN status='INCIDENT' THEN 1 ELSE 0 END) AS incident_count,
            SUM(CASE WHEN last_scrobble_at IS NULL THEN 1 ELSE 0 END) AS sin_scrobble
        FROM teams
        WHERE active = TRUE
    ''')
    s = cur.fetchone()

    cur.execute('''
        WITH today AS (
            SELECT lastfm_user, COUNT(*) AS plays_hoy
            FROM scrobbles
            WHERE scrobble_time >= CURRENT_DATE
            GROUP BY lastfm_user
        ), last_hour AS (
            SELECT lastfm_user, COUNT(*) AS plays_ultima_hora
            FROM scrobbles
            WHERE scrobble_time >= NOW() - INTERVAL '1 hour'
            GROUP BY lastfm_user
        )
        SELECT
            t.id,
            t.name,
            t.app_name,
            t.lastfm_user,
            t.status,
            t.last_scrobble_at,
            t.idle_minutes,
            COALESCE(td.plays_hoy, 0) AS plays_hoy,
            COALESCE(lh.plays_ultima_hora, 0) AS plays_ultima_hora
        FROM teams t
        LEFT JOIN today td ON td.lastfm_user = t.lastfm_user
        LEFT JOIN last_hour lh ON lh.lastfm_user = t.lastfm_user
        WHERE t.active = TRUE
        ORDER BY
            CASE
                WHEN t.last_scrobble_at IS NULL THEN 1
                WHEN COALESCE(t.idle_minutes, 999999) >= 180 THEN 2
                WHEN COALESCE(t.idle_minutes, 999999) >= 60 THEN 3
                ELSE 4
            END,
            COALESCE(t.idle_minutes, 999999) DESC,
            t.id ASC
    ''')
    rows = cur.fetchall()

    dormidos = 0
    sin_actividad = 0
    activos = 0
    total_plays_hoy = 0
    total_plays_hora = 0
    table_rows = ''

    for r in rows:
        idle = r['idle_minutes']
        plays_hoy = safe_int(r['plays_hoy'])
        plays_ultima_hora = safe_int(r['plays_ultima_hora'])
        total_plays_hoy += plays_hoy
        total_plays_hora += plays_ultima_hora

        if r['last_scrobble_at'] is None:
            salud = '<span class="badge incident">SIN DATA</span>'
            sin_actividad += 1
        elif idle is not None and idle >= 180:
            salud = '<span class="badge incident">DORMIDO +3H</span>'
            dormidos += 1
        elif idle is not None and idle >= 60:
            salud = '<span class="badge warn">DORMIDO +1H</span>'
            dormidos += 1
        else:
            salud = '<span class="badge ok">ACTIVO</span>'
            activos += 1

        table_rows += f'''
        <tr>
            <td>{r['id']}</td>
            <td>{r['name']}</td>
            <td>{r['app_name']}</td>
            <td>{r['lastfm_user']}</td>
            <td>{badge(r['status'])}</td>
            <td>{r['last_scrobble_at'] or '-'}</td>
            <td>{idle if idle is not None else '-'}</td>
            <td>{plays_hoy}</td>
            <td>{plays_ultima_hora}</td>
            <td>{salud}</td>
        </tr>
        '''

    if not table_rows:
        table_rows = '<tr><td colspan="10" class="muted">No hay equipos activos.</td></tr>'

    now = datetime.utcnow()
    collect = read_last_collect_summary()
    collect_badge = badge(collect['estado'])

    return f'''
    <div class="grid">
        <div class="card"><div class="label">Equipos activos</div><div class="value blue">{safe_int(s['total'])}</div></div>
        <div class="card"><div class="label">Operando bien</div><div class="value green">{activos}</div></div>
        <div class="card"><div class="label">Dormidos</div><div class="value yellow">{dormidos}</div></div>
        <div class="card"><div class="label">Sin data</div><div class="value red">{sin_actividad}</div></div>
    </div>

    <div class="grid-3">
        <div class="card"><div class="label">Plays hoy</div><div class="value green">{total_plays_hoy}</div></div>
        <div class="card"><div class="label">Plays última hora</div><div class="value blue">{total_plays_hora}</div></div>
        <div class="card"><div class="label">Último Collect</div><div class="value">{collect_badge}</div></div>
    </div>

    <div class="grid-2">
        <div class="card" style="margin-bottom:18px;">
            <div class="section-title">Centro Operacional</div>
            <div class="mini-row"><span>Última revisión visual</span><strong>{now.strftime('%Y-%m-%d %H:%M:%S')} UTC</strong></div>
            <div class="mini-row"><span>Consulta optimizada</span><strong>Solo hoy + última hora</strong></div>
            <div class="mini-row"><span>Regla dormido</span><strong>+60 min sin scrobble</strong></div>
            <div class="mini-row"><span>Regla crítico</span><strong>+180 min sin scrobble o sin data</strong></div>
        </div>

        <div class="card" style="margin-bottom:18px;">
            <div class="section-title">Resumen último collect</div>
            <div class="mini-row"><span>Estado</span><strong>{collect_badge}</strong></div>
            <div class="mini-row"><span>Inicio</span><strong>{collect['started']}</strong></div>
            <div class="mini-row"><span>Fin</span><strong>{collect['finished']}</strong></div>
            <div class="mini-row"><span>Return code</span><strong>{collect['return_code']}</strong></div>
            <div class="mini-row"><span>Insertados</span><strong>{collect['insertados']}</strong></div>
            <div style="margin-top:12px;"><a class="btn btn-secondary" href="/job-log?job=collect-now">Ver log completo</a></div>
        </div>
    </div>

    <div class="section-title">Salud operativa por equipo</div>
    <table>
        <thead>
            <tr>
                <th>ID</th>
                <th>Equipo</th>
                <th>App</th>
                <th>User</th>
                <th>Status</th>
                <th>Último scrobble</th>
                <th>Idle min</th>
                <th>Plays hoy</th>
                <th>Última hora</th>
                <th>Salud</th>
            </tr>
        </thead>
        <tbody>{table_rows}</tbody>
    </table>
    '''

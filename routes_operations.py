from datetime import datetime

from utils import safe_int
from layout import badge


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

    # IMPORTANTE:
    # No hacemos JOIN directo contra todo scrobbles, porque eso se pone lento con histórico grande.
    # Primero resumimos solo las últimas 24h/1h y luego unimos contra teams.
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

    return f'''
    <div class="grid">
        <div class="card"><div class="label">Equipos activos</div><div class="value blue">{safe_int(s['total'])}</div></div>
        <div class="card"><div class="label">Operando bien</div><div class="value green">{activos}</div></div>
        <div class="card"><div class="label">Dormidos</div><div class="value yellow">{dormidos}</div></div>
        <div class="card"><div class="label">Sin data</div><div class="value red">{sin_actividad}</div></div>
    </div>

    <div class="grid-2">
        <div class="card"><div class="label">Plays hoy</div><div class="value green">{total_plays_hoy}</div></div>
        <div class="card"><div class="label">Plays última hora</div><div class="value blue">{total_plays_hora}</div></div>
    </div>

    <div class="card" style="margin-bottom:18px;">
        <div class="section-title">Centro Operacional</div>
        <div class="mini-row"><span>Última revisión visual</span><strong>{now.strftime('%Y-%m-%d %H:%M:%S')} UTC</strong></div>
        <div class="mini-row"><span>Consulta optimizada</span><strong>Solo hoy + última hora</strong></div>
        <div class="mini-row"><span>Regla dormido</span><strong>+60 min sin scrobble</strong></div>
        <div class="mini-row"><span>Regla crítico</span><strong>+180 min sin scrobble o sin data</strong></div>
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

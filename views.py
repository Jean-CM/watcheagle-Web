import json
import math
from urllib.parse import urlencode

from flask import request

from config import PLATFORM_RATES
from utils import safe_int, money, current_filters, month_range
from layout import filter_form, badge


def month_where(alias='s'):
    month, platform, distributor = current_filters()
    start, end = month_range(month)
    clauses = [f'{alias}.scrobble_time >= %s', f'{alias}.scrobble_time < %s']
    params = [start, end]

    if platform:
        clauses.append(f'LOWER({alias}.app_name) = %s')
        params.append(platform)

    if distributor:
        clauses.append(f'''
            EXISTS (
                SELECT 1
                FROM artist_metadata am
                WHERE LOWER(am.artist_name) = LOWER({alias}.artist_name)
                AND am.distributor = %s
            )
        ''')
        params.append(distributor)

    return ' AND '.join(clauses), params


def avg_rate(platform):
    p = (platform or '').strip().lower()
    r = PLATFORM_RATES.get(p, PLATFORM_RATES['spotify'])
    return (r['min'] + r['max']) / 2


def render_ejecutivo(cur):
    return '<div class="card">Ejecutivo activo desde routes_executive.py</div>'


def render_monitor(cur):
    _, platform, _ = current_filters()

    cur.execute('''
        SELECT COUNT(*) total,
               SUM(CASE WHEN status='OK' THEN 1 ELSE 0 END) ok_count,
               SUM(CASE WHEN status='WARN' THEN 1 ELSE 0 END) warn_count,
               SUM(CASE WHEN status='INCIDENT' THEN 1 ELSE 0 END) incident_count,
               SUM(CASE WHEN last_scrobble_at IS NULL THEN 1 ELSE 0 END) sin_data,
               SUM(CASE WHEN COALESCE(idle_minutes, 999999) >= 60 THEN 1 ELSE 0 END) dormidos
        FROM teams
        WHERE active = TRUE
    ''')
    s = cur.fetchone()

    cur.execute('''
        SELECT app_name,
               COUNT(*) AS total,
               SUM(CASE WHEN status='OK' THEN 1 ELSE 0 END) AS ok_count,
               SUM(CASE WHEN status='WARN' THEN 1 ELSE 0 END) AS warn_count,
               SUM(CASE WHEN status='INCIDENT' THEN 1 ELSE 0 END) AS incident_count,
               SUM(CASE WHEN last_scrobble_at IS NULL THEN 1 ELSE 0 END) AS sin_data,
               SUM(CASE WHEN COALESCE(idle_minutes, 999999) >= 60 THEN 1 ELSE 0 END) AS dormidos
        FROM teams
        WHERE active = TRUE
        GROUP BY app_name
        ORDER BY app_name ASC
    ''')
    app_summary = cur.fetchall()

    app_rows = ''
    for r in app_summary:
        app_rows += f'''
        <tr>
            <td>{r['app_name']}</td>
            <td>{safe_int(r['total'])}</td>
            <td class="green">{safe_int(r['ok_count'])}</td>
            <td class="yellow">{safe_int(r['warn_count'])}</td>
            <td class="red">{safe_int(r['incident_count'])}</td>
            <td>{safe_int(r['dormidos'])}</td>
            <td>{safe_int(r['sin_data'])}</td>
        </tr>
        '''
    if not app_rows:
        app_rows = '<tr><td colspan="7" class="muted">Sin data por app.</td></tr>'

    if platform:
        cur.execute('''
            SELECT id, name, app_name, lastfm_user, status, last_scrobble_at, idle_minutes, last_check_at
            FROM teams
            WHERE LOWER(app_name) = %s
            ORDER BY
                CASE
                    WHEN last_scrobble_at IS NULL THEN 1
                    WHEN COALESCE(idle_minutes, 999999) >= 180 THEN 2
                    WHEN COALESCE(idle_minutes, 999999) >= 60 THEN 3
                    ELSE 4
                END,
                COALESCE(idle_minutes, 999999) DESC,
                id ASC
        ''', (platform,))
    else:
        cur.execute('''
            SELECT id, name, app_name, lastfm_user, status, last_scrobble_at, idle_minutes, last_check_at
            FROM teams
            ORDER BY
                CASE
                    WHEN last_scrobble_at IS NULL THEN 1
                    WHEN COALESCE(idle_minutes, 999999) >= 180 THEN 2
                    WHEN COALESCE(idle_minutes, 999999) >= 60 THEN 3
                    ELSE 4
                END,
                COALESCE(idle_minutes, 999999) DESC,
                id ASC
        ''')
    teams = cur.fetchall()

    rows = ''
    critical_rows = ''
    for t in teams:
        idle = t['idle_minutes']
        if t['last_scrobble_at'] is None:
            salud = '<span class="badge incident">SIN DATA</span>'
        elif idle is not None and idle >= 180:
            salud = '<span class="badge incident">DORMIDO +3H</span>'
        elif idle is not None and idle >= 60:
            salud = '<span class="badge warn">DORMIDO +1H</span>'
        else:
            salud = '<span class="badge ok">ACTIVO</span>'

        row_html = f'''
        <tr>
            <td><input type="checkbox" name="team_ids" value="{t['id']}"> {t['id']}</td>
            <td>{t['name']}</td>
            <td>{t['app_name']}</td>
            <td>{t['lastfm_user']}</td>
            <td>{badge(t['status'])}</td>
            <td>{t['last_scrobble_at'] or '-'}</td>
            <td>{idle if idle is not None else '-'}</td>
            <td>{t['last_check_at'] or '-'}</td>
            <td>{salud}</td>
        </tr>
        '''
        rows += row_html

        if t['last_scrobble_at'] is None or (idle is not None and idle >= 60):
            critical_rows += row_html

    if not rows:
        rows = '<tr><td colspan="9" class="muted">No hay equipos.</td></tr>'
    if not critical_rows:
        critical_rows = '<tr><td colspan="9" class="muted">Sin alertas críticas. Todo tranquilo por ahora.</td></tr>'

    if platform:
        cur.execute('''
            SELECT artist_name, track_name, app_name, lastfm_user, scrobble_time
            FROM scrobbles
            WHERE LOWER(app_name) = %s
            ORDER BY scrobble_time DESC
            LIMIT 12
        ''', (platform,))
    else:
        cur.execute('''
            SELECT artist_name, track_name, app_name, lastfm_user, scrobble_time
            FROM scrobbles
            ORDER BY scrobble_time DESC
            LIMIT 12
        ''')
    recent = cur.fetchall()

    recent_rows = ''
    for r in recent:
        recent_rows += f'''
        <tr>
            <td>{r['scrobble_time']}</td>
            <td>{r['app_name']}</td>
            <td>{r['lastfm_user']}</td>
            <td>{r['artist_name']}</td>
            <td>{r['track_name']}</td>
        </tr>
        '''
    if not recent_rows:
        recent_rows = '<tr><td colspan="5" class="muted">Sin reproducciones recientes.</td></tr>'

    return f'''
    {filter_form('monitor')}

    <div class="grid">
        <div class="card"><div class="label">Monitores activos</div><div class="value blue">{safe_int(s['total'])}</div></div>
        <div class="card"><div class="label">OK</div><div class="value green">{safe_int(s['ok_count'])}</div></div>
        <div class="card"><div class="label">WARN</div><div class="value yellow">{safe_int(s['warn_count'])}</div></div>
        <div class="card"><div class="label">INCIDENT</div><div class="value red">{safe_int(s['incident_count'])}</div></div>
    </div>

    <div class="grid-2">
        <div class="card">
            <div class="section-title">Resumen por app</div>
            <table>
                <thead><tr><th>App</th><th>Total</th><th>OK</th><th>WARN</th><th>INCIDENT</th><th>Dormidos</th><th>Sin data</th></tr></thead>
                <tbody>{app_rows}</tbody>
            </table>
        </div>
        <div class="card">
            <div class="section-title">Alertas rápidas</div>
            <div class="mini-row"><span>Dormidos +60 min</span><strong class="yellow">{safe_int(s['dormidos'])}</strong></div>
            <div class="mini-row"><span>Sin data</span><strong class="red">{safe_int(s['sin_data'])}</strong></div>
            <div class="mini-row"><span>Filtro actual app</span><strong>{platform or 'Todas'}</strong></div>
            <div class="mini-row"><span>Acción recomendada</span><strong>Revisar alertas y correr collect selectivo</strong></div>
        </div>
    </div>

    <div class="card" style="margin-bottom:18px;">
        <div class="section-title">Agregar usuario Last.fm</div>
        <form class="form-grid" method="GET" action="/seed-team">
            <div class="field"><label>Equipo</label><input name="name" required></div>
            <div class="field"><label>App</label><select name="app"><option value="spotify">spotify</option><option value="apple">apple</option><option value="tidal">tidal</option><option value="youtube">youtube</option></select></div>
            <div class="field"><label>Usuario Last.fm</label><input name="user" required></div>
            <button class="btn btn-primary">Agregar</button>
        </form>
    </div>

    <div class="card" style="margin-bottom:18px;">
        <div class="section-title">Reproducciones recientes</div>
        <table>
            <thead><tr><th>Hora</th><th>App</th><th>User</th><th>Artista</th><th>Canción</th></tr></thead>
            <tbody>{recent_rows}</tbody>
        </table>
    </div>

    <form method="POST" action="/collect-all-selected">
        <div style="margin-bottom:12px;"><button class="btn btn-primary" type="submit">Collect All seleccionados</button></div>

        <div class="card" style="margin-bottom:18px;">
            <div class="section-title">Equipos con alerta</div>
            <table>
                <thead><tr><th>ID</th><th>Equipo</th><th>App</th><th>User</th><th>Status</th><th>Último scrobble</th><th>Idle</th><th>Último check</th><th>Salud</th></tr></thead>
                <tbody>{critical_rows}</tbody>
            </table>
        </div>

        <div class="section-title">Todos los equipos</div>
        <table>
            <thead><tr><th>ID</th><th>Equipo</th><th>App</th><th>User</th><th>Status</th><th>Último scrobble</th><th>Idle</th><th>Último check</th><th>Salud</th></tr></thead>
            <tbody>{rows}</tbody>
        </table>
    </form>
    '''


def render_analisis_financiero(cur, view_name='analisis'):
    where, params = month_where('s')

    cur.execute(f'SELECT COUNT(*) c FROM scrobbles s WHERE {where}', params)
    plays_total = safe_int(cur.fetchone()['c'])

    cur.execute(f'''
        SELECT DATE(s.scrobble_time) AS play_day,
               LOWER(s.app_name) AS platform,
               COUNT(*) AS plays
        FROM scrobbles s
        WHERE {where}
        GROUP BY DATE(s.scrobble_time), LOWER(s.app_name)
        ORDER BY play_day ASC
    ''', params)
    daily_platform = cur.fetchall()

    daily_map = {}
    daily_gain_map = {}
    for r in daily_platform:
        day = str(r['play_day'])[5:]
        plays = safe_int(r['plays'])
        gain = plays * avg_rate(r['platform'])
        daily_map[day] = daily_map.get(day, 0) + plays
        daily_gain_map[day] = daily_gain_map.get(day, 0) + gain

    labels = list(daily_map.keys())
    plays_values = [daily_map[d] for d in labels]
    gain_values = [round(daily_gain_map.get(d, 0), 2) for d in labels]
    avg_daily = round(plays_total / max(len(labels), 1), 2)

    cur.execute(f'''
        SELECT LOWER(s.app_name) AS platform, COUNT(*) AS plays
        FROM scrobbles s
        WHERE {where}
        GROUP BY LOWER(s.app_name)
        ORDER BY plays DESC
    ''', params)
    platforms = cur.fetchall()

    total_min = 0
    total_max = 0
    platform_rows = ''
    for r in platforms:
        p = (r['platform'] or 'spotify').lower()
        plays = safe_int(r['plays'])
        rate = PLATFORM_RATES.get(p, PLATFORM_RATES['spotify'])
        mn = plays * rate['min']
        mx = plays * rate['max']
        avg = (mn + mx) / 2
        total_min += mn
        total_max += mx
        platform_rows += f'<tr><td>{p.title()}</td><td>{plays:,}</td><td>{money(mn)}</td><td>{money(mx)}</td><td class="green">{money(avg)}</td></tr>'

    if not platform_rows:
        platform_rows = '<tr><td colspan="5" class="muted">Sin datos</td></tr>'

    cur.execute(f'''
        SELECT COALESCE(am.distributor, 'Sin distribuidora') AS distributor,
               COUNT(*) AS plays
        FROM scrobbles s
        LEFT JOIN artist_metadata am ON LOWER(am.artist_name) = LOWER(s.artist_name)
        WHERE {where}
        GROUP BY COALESCE(am.distributor, 'Sin distribuidora')
        ORDER BY plays DESC
        LIMIT 10
    ''', params)
    distributors = cur.fetchall()

    dist_rows = ''
    for r in distributors:
        plays = safe_int(r['plays'])
        gain = plays * 0.0054
        dist_rows += f'<tr><td>{r["distributor"]}</td><td>{plays:,}</td><td class="green">{money(gain)}</td></tr>'
    if not dist_rows:
        dist_rows = '<tr><td colspan="3" class="muted">Sin datos</td></tr>'

    cur.execute(f'''
        SELECT s.artist_name, s.track_name, COUNT(*) AS plays
        FROM scrobbles s
        WHERE {where}
        GROUP BY s.artist_name, s.track_name
        ORDER BY plays DESC
        LIMIT 10
    ''', params)
    tracks = cur.fetchall()

    track_rows = ''
    for r in tracks:
        plays = safe_int(r['plays'])
        gain = plays * 0.0054
        track_rows += f'<tr><td>{r["artist_name"]}</td><td>{r["track_name"]}</td><td>{plays:,}</td><td class="green">{money(gain)}</td></tr>'
    if not track_rows:
        track_rows = '<tr><td colspan="4" class="muted">Sin datos</td></tr>'

    total_avg = (total_min + total_max) / 2

    return f'''
    {filter_form(view_name)}

    <div class="grid">
        <div class="card"><div class="label">Plays filtrados</div><div class="value blue">{plays_total:,}</div></div>
        <div class="card"><div class="label">Días con data</div><div class="value">{len(labels)}</div></div>
        <div class="card"><div class="label">Promedio diario</div><div class="value green">{avg_daily:,}</div></div>
        <div class="card"><div class="label">Ganancia promedio</div><div class="value green">{money(total_avg)}</div></div>
    </div>

    <div class="grid-3">
        <div class="card"><div class="label">Mínimo estimado</div><div class="value">{money(total_min)}</div></div>
        <div class="card"><div class="label">Máximo estimado</div><div class="value yellow">{money(total_max)}</div></div>
        <div class="card"><div class="label">Revenue por play promedio</div><div class="value blue">{money(total_avg / max(plays_total, 1))}</div></div>
    </div>

    <div class="grid-2">
        <div class="card"><div class="section-title">Tendencia diaria de plays</div><canvas id="playsChart"></canvas></div>
        <div class="card"><div class="section-title">Tendencia diaria de ganancias</div><canvas id="gainChart"></canvas></div>
    </div>

    <div class="grid-2">
        <div class="card"><div class="section-title">Plataformas</div><table><thead><tr><th>Plataforma</th><th>Streams</th><th>Min</th><th>Max</th><th>Promedio</th></tr></thead><tbody>{platform_rows}</tbody></table></div>
        <div class="card"><div class="section-title">Distribuidoras</div><table><thead><tr><th>Distribuidora</th><th>Plays</th><th>Ganancia</th></tr></thead><tbody>{dist_rows}</tbody></table></div>
    </div>

    <div class="card">
        <div class="section-title">Top canciones con revenue</div>
        <table><thead><tr><th>Artista</th><th>Canción</th><th>Plays</th><th>Ganancia</th></tr></thead><tbody>{track_rows}</tbody></table>
    </div>

    <script>
    new Chart(document.getElementById('playsChart'), {{type:'line',data:{{labels:{json.dumps(labels)},datasets:[{{label:'Plays',data:{json.dumps(plays_values)},borderColor:'#60a5fa',backgroundColor:'rgba(96,165,250,.15)',tension:.35,fill:true}}]}},options:{{responsive:true,plugins:{{legend:{{labels:{{color:'#e5e7eb'}}}}}},scales:{{x:{{ticks:{{color:'#94a3b8'}}}},y:{{ticks:{{color:'#94a3b8'}}}}}}}}}});
    new Chart(document.getElementById('gainChart'), {{type:'line',data:{{labels:{json.dumps(labels)},datasets:[{{label:'Ganancias',data:{json.dumps(gain_values)},borderColor:'#34d399',backgroundColor:'rgba(52,211,153,.15)',tension:.35,fill:true}}]}},options:{{responsive:true,plugins:{{legend:{{labels:{{color:'#e5e7eb'}}}}}},scales:{{x:{{ticks:{{color:'#94a3b8'}}}},y:{{ticks:{{color:'#94a3b8'}}}}}}}}}});
    </script>
    '''


def render_analisis(cur):
    return render_analisis_financiero(cur, 'analisis')


def render_ganancias(cur):
    return render_analisis_financiero(cur, 'ganancias')


def render_monitor_plays(cur):
    where, params = month_where('s')
    month, platform, distributor = current_filters()
    page = max(safe_int(request.args.get('page'), 1), 1)
    per_page = 50
    offset = (page - 1) * per_page

    cur.execute(f'''
        SELECT COUNT(*) AS total_tracks
        FROM (
            SELECT s.artist_name, s.track_name
            FROM scrobbles s
            WHERE {where}
            GROUP BY s.artist_name, s.track_name
            HAVING COUNT(*) < 1000
        ) x
    ''', params)
    total_tracks = safe_int(cur.fetchone()['total_tracks'])

    cur.execute(f'''
        SELECT s.artist_name, s.track_name, COUNT(*) AS plays
        FROM scrobbles s
        WHERE {where}
        GROUP BY s.artist_name, s.track_name
        HAVING COUNT(*) < 1000
        ORDER BY plays DESC, s.artist_name ASC, s.track_name ASC
        LIMIT %s OFFSET %s
    ''', [*params, per_page, offset])
    rows = cur.fetchall()

    cur.execute(f'''
        SELECT
            SUM(CASE WHEN plays >= 900 THEN 1 ELSE 0 END) AS near_goal,
            SUM(CASE WHEN plays < 500 THEN 1 ELSE 0 END) AS high_priority,
            SUM(1000 - plays) AS total_missing
        FROM (
            SELECT COUNT(*) AS plays
            FROM scrobbles s
            WHERE {where}
            GROUP BY s.artist_name, s.track_name
            HAVING COUNT(*) < 1000
        ) x
    ''', params)
    kpi = cur.fetchone()

    near_goal = safe_int(kpi['near_goal'] if kpi else 0)
    high_priority = safe_int(kpi['high_priority'] if kpi else 0)
    total_missing = safe_int(kpi['total_missing'] if kpi else 0)
    total_pages = max(math.ceil(total_tracks / per_page), 1)

    table_rows = ''
    for r in rows:
        plays = safe_int(r['plays'])
        faltan = max(1000 - plays, 0)
        avance = min(round((plays / 1000) * 100, 1), 100)
        gain = plays * 0.0054

        if plays >= 900:
            prioridad = '<span class="badge ok">CERCA</span>'
            recomendacion = 'Empujar cierre a 1K'
        elif plays >= 500:
            prioridad = '<span class="badge warn">MEDIA</span>'
            recomendacion = 'Mantener rotación'
        else:
            prioridad = '<span class="badge incident">ALTA</span>'
            recomendacion = 'Prioridad de empuje'

        table_rows += f'''
        <tr>
            <td>{r['artist_name']}</td>
            <td>{r['track_name']}</td>
            <td>{plays:,}</td>
            <td>{faltan:,}</td>
            <td>{avance}%</td>
            <td class="green">{money(gain)}</td>
            <td>{prioridad}</td>
            <td>{recomendacion}</td>
        </tr>
        '''

    if not table_rows:
        table_rows = '<tr><td colspan="8" class="muted">No hay canciones debajo de 1000.</td></tr>'

    base_args = {'view': 'monitor-plays'}
    if month:
        base_args['month'] = month
    if platform:
        base_args['platform'] = platform
    if distributor:
        base_args['distributor'] = distributor

    export_args = dict(base_args)
    export_link = '/export-monitor-plays.csv?' + urlencode(export_args)

    prev_args = dict(base_args)
    prev_args['page'] = max(page - 1, 1)
    next_args = dict(base_args)
    next_args['page'] = min(page + 1, total_pages)
    prev_link = '/?' + urlencode(prev_args)
    next_link = '/?' + urlencode(next_args)

    return f'''
    {filter_form('monitor-plays')}

    <div class="grid">
        <div class="card"><div class="label">Canciones bajo 1K</div><div class="value blue">{total_tracks}</div></div>
        <div class="card"><div class="label">Cerca de meta</div><div class="value green">{near_goal}</div></div>
        <div class="card"><div class="label">Prioridad alta</div><div class="value red">{high_priority}</div></div>
        <div class="card"><div class="label">Reproducciones faltantes</div><div class="value yellow">{total_missing:,}</div></div>
    </div>

    <div class="card" style="margin-bottom:18px;">
        <div class="section-title">Lectura de prioridad</div>
        <div class="mini-row"><span>Cerca de meta</span><strong>900 a 999 plays</strong></div>
        <div class="mini-row"><span>Prioridad media</span><strong>500 a 899 plays</strong></div>
        <div class="mini-row"><span>Prioridad alta</span><strong>Menos de 500 plays</strong></div>
        <div class="mini-row"><span>Descarga para playlist</span><strong><a class="btn btn-primary" href="{export_link}">Descargar canciones CSV</a></strong></div>
    </div>

    <div class="card" style="margin-bottom:18px;">
        <div class="mini-row"><span>Página</span><strong>{page} / {total_pages}</strong></div>
        <div class="mini-row"><span>Mostrando</span><strong>{len(rows)} canciones por página</strong></div>
        <div style="display:flex; gap:10px; flex-wrap:wrap; margin-top:12px;">
            <a class="btn btn-secondary" href="{prev_link}">Anterior</a>
            <a class="btn btn-secondary" href="{next_link}">Siguiente</a>
        </div>
    </div>

    <div class="section-title">Monitor Plays Pro</div>
    <table>
        <thead><tr><th>Artista</th><th>Canción</th><th>Plays</th><th>Faltan 1K</th><th>Avance</th><th>Ganancia</th><th>Prioridad</th><th>Recomendación</th></tr></thead>
        <tbody>{table_rows}</tbody>
    </table>
    '''

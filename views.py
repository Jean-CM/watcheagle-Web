import json

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
               SUM(CASE WHEN status='INCIDENT' THEN 1 ELSE 0 END) incident_count
        FROM teams
        WHERE active = TRUE
    ''')
    s = cur.fetchone()

    if platform:
        cur.execute('''
            SELECT id, name, app_name, lastfm_user, status, last_scrobble_at, idle_minutes, last_check_at
            FROM teams
            WHERE LOWER(app_name) = %s
            ORDER BY id ASC
        ''', (platform,))
    else:
        cur.execute('''
            SELECT id, name, app_name, lastfm_user, status, last_scrobble_at, idle_minutes, last_check_at
            FROM teams
            ORDER BY id ASC
        ''')
    teams = cur.fetchall()

    rows = ''
    for t in teams:
        rows += f'''
        <tr>
            <td><input type="checkbox" name="team_ids" value="{t['id']}"> {t['id']}</td>
            <td>{t['name']}</td>
            <td>{t['app_name']}</td>
            <td>{t['lastfm_user']}</td>
            <td>{badge(t['status'])}</td>
            <td>{t['last_scrobble_at'] or '-'}</td>
            <td>{t['idle_minutes'] if t['idle_minutes'] is not None else '-'}</td>
            <td>{t['last_check_at'] or '-'}</td>
        </tr>
        '''

    if not rows:
        rows = '<tr><td colspan="8" class="muted">No hay equipos.</td></tr>'

    return f'''
    {filter_form('monitor')}

    <div class="grid">
        <div class="card"><div class="label">Monitores activos</div><div class="value">{safe_int(s['total'])}</div></div>
        <div class="card"><div class="label">OK</div><div class="value green">{safe_int(s['ok_count'])}</div></div>
        <div class="card"><div class="label">WARN</div><div class="value yellow">{safe_int(s['warn_count'])}</div></div>
        <div class="card"><div class="label">INCIDENT</div><div class="value red">{safe_int(s['incident_count'])}</div></div>
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

    <form method="POST" action="/collect-all-selected">
        <div style="margin-bottom:12px;"><button class="btn btn-primary" type="submit">Collect All seleccionados</button></div>
        <table>
            <thead><tr><th>ID</th><th>Equipo</th><th>App</th><th>User</th><th>Status</th><th>Último scrobble</th><th>Idle</th><th>Último check</th></tr></thead>
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

    cur.execute(f'''
        SELECT s.artist_name, s.track_name, COUNT(*) AS plays
        FROM scrobbles s
        WHERE {where}
        GROUP BY s.artist_name, s.track_name
        HAVING COUNT(*) < 1000
        ORDER BY plays DESC
        LIMIT 200
    ''', params)
    rows = cur.fetchall()

    table_rows = ''
    for r in rows:
        plays = safe_int(r['plays'])
        faltan = max(1000 - plays, 0)
        table_rows += f'<tr><td>{r["artist_name"]}</td><td>{r["track_name"]}</td><td>{plays:,}</td><td>{faltan:,}</td><td>Dar {faltan:,} reproducciones</td></tr>'

    if not table_rows:
        table_rows = '<tr><td colspan="5" class="muted">No hay canciones debajo de 1000.</td></tr>'

    return f'''
    {filter_form('monitor-plays')}
    <div class="section-title">Monitor Plays Pro</div>
    <table>
        <thead><tr><th>Artista</th><th>Canción</th><th>Plays</th><th>Faltan 1K</th><th>Recomendación</th></tr></thead>
        <tbody>{table_rows}</tbody>
    </table>
    '''

import json
from datetime import datetime

from config import PLATFORM_RATES
from utils import safe_int, money, avg_rate, current_filters, month_range
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


def render_ejecutivo(cur):
    where, params = month_where('s')
    month, _, _ = current_filters()
    start, end = month_range(month)

    cur.execute(f'SELECT COUNT(*) AS plays FROM scrobbles s WHERE {where}', params)
    plays = safe_int(cur.fetchone()['plays'])

    cur.execute('SELECT COUNT(*) total FROM teams WHERE active = TRUE')
    st = cur.fetchone()

    cur.execute(f'''
        SELECT LOWER(s.app_name) AS platform, COUNT(*) AS plays
        FROM scrobbles s
        WHERE {where}
        GROUP BY LOWER(s.app_name)
    ''', params)
    exec_platforms = cur.fetchall()

    exec_total_min = 0
    exec_total_max = 0
    for r in exec_platforms:
        p = (r['platform'] or '').lower()
        platform_plays = safe_int(r['plays'])
        rate = PLATFORM_RATES.get(p, PLATFORM_RATES['spotify'])
        exec_total_min += platform_plays * rate['min']
        exec_total_max += platform_plays * rate['max']

    exec_avg_gain = (exec_total_min + exec_total_max) / 2

    now = datetime.utcnow()
    total_days_month = max((end - start).days, 1)
    if now < start:
        elapsed_days = 0
    elif now >= end:
        elapsed_days = total_days_month
    else:
        elapsed_days = max(1, (now.date() - start.date()).days + 1)

    daily_avg_plays = plays / max(elapsed_days, 1)
    projected_plays = int(daily_avg_plays * total_days_month)
    projected_avg_gain = (exec_avg_gain / plays) * projected_plays if plays > 0 else 0
    projected_max_gain = (exec_total_max / plays) * projected_plays if plays > 0 else 0

    cur.execute(f'''
        SELECT s.artist_name, COUNT(*) AS plays
        FROM scrobbles s
        WHERE {where}
        GROUP BY s.artist_name
        ORDER BY plays DESC
        LIMIT 8
    ''', params)
    artists = cur.fetchall()
    artist_html = ''.join([
        f"<div class='mini-row'><span>{r['artist_name']}</span><strong>{r['plays']}</strong></div>"
        for r in artists
    ]) or '<div class="muted">Sin datos</div>'

    cur.execute(f'''
        SELECT COALESCE(am.distributor, 'Sin distribuidora') AS distributor, COUNT(*) AS plays
        FROM scrobbles s
        LEFT JOIN artist_metadata am ON LOWER(am.artist_name) = LOWER(s.artist_name)
        WHERE {where}
        GROUP BY COALESCE(am.distributor, 'Sin distribuidora')
        ORDER BY plays DESC
    ''', params)
    dist = cur.fetchall()
    dist_labels = [r['distributor'] for r in dist]
    dist_values = [safe_int(r['plays']) for r in dist]

    cur.execute(f'''
        SELECT DATE(s.scrobble_time) AS play_day, COUNT(*) AS plays
        FROM scrobbles s
        WHERE {where}
        GROUP BY DATE(s.scrobble_time)
        ORDER BY play_day ASC
    ''', params)
    daily = cur.fetchall()
    daily_labels = [str(r['play_day'])[5:] for r in daily]
    daily_values = [safe_int(r['plays']) for r in daily]

    insight = f'Si mantiene este ritmo, el mes podría cerrar cerca de {projected_plays:,} plays.' if projected_plays > plays else 'El mes ya está cerrado o sin datos suficientes para proyección.'

    return f'''
    {filter_form('ejecutivo')}
    <div class="grid">
        <div class="card"><div class="label">Plays filtrados</div><div class="value blue">{plays:,}</div></div>
        <div class="card"><div class="label">Ganancia promedio</div><div class="value green">{money(exec_avg_gain)}</div></div>
        <div class="card"><div class="label">Ganancia máxima</div><div class="value yellow">{money(exec_total_max)}</div></div>
        <div class="card"><div class="label">Proyección plays mes</div><div class="value">{projected_plays:,}</div></div>
    </div>
    <div class="grid-3">
        <div class="card"><div class="label">Proyección ganancia promedio</div><div class="value green">{money(projected_avg_gain)}</div></div>
        <div class="card"><div class="label">Proyección ganancia máxima</div><div class="value yellow">{money(projected_max_gain)}</div></div>
        <div class="card"><div class="label">Equipos activos</div><div class="value">{safe_int(st['total'])}</div></div>
    </div>
    <div class="card" style="margin-bottom:18px;"><div class="section-title">Resumen ejecutivo</div><div class="mini-row"><span>{insight}</span><strong>Proyección</strong></div><div class="mini-row"><span>Días considerados del mes</span><strong>{elapsed_days} / {total_days_month}</strong></div><div class="mini-row"><span>Promedio diario de plays</span><strong>{round(daily_avg_plays, 2):,}</strong></div></div>
    <div class="grid-2"><div class="card"><div class="section-title">Tendencia diaria</div><canvas id="execDailyChart"></canvas></div><div class="card"><div class="section-title">Plays por distribuidora</div><canvas id="execDistChart"></canvas></div></div>
    <div class="grid-2"><div class="card"><div class="section-title">Top artistas</div>{artist_html}</div><div class="card"><div class="section-title">Lectura ejecutiva</div><div class="mini-row"><span>Ganancia actual promedio</span><strong>{money(exec_avg_gain)}</strong></div><div class="mini-row"><span>Ganancia actual máxima</span><strong>{money(exec_total_max)}</strong></div><div class="mini-row"><span>Proyección promedio mensual</span><strong>{money(projected_avg_gain)}</strong></div><div class="mini-row"><span>Proyección máxima mensual</span><strong>{money(projected_max_gain)}</strong></div></div></div>
    <script>
    new Chart(document.getElementById('execDailyChart'), {{type:'line',data:{{labels:{json.dumps(daily_labels)},datasets:[{{label:'Plays diarios',data:{json.dumps(daily_values)},borderColor:'#34d399',backgroundColor:'rgba(52,211,153,.15)',tension:.35,fill:true}}]}},options:{{responsive:true,plugins:{{legend:{{labels:{{color:'#e5e7eb'}}}}}},scales:{{x:{{ticks:{{color:'#94a3b8'}}}},y:{{ticks:{{color:'#94a3b8'}}}}}}}}}});
    new Chart(document.getElementById('execDistChart'), {{type:'bar',data:{{labels:{json.dumps(dist_labels)},datasets:[{{label:'Plays',data:{json.dumps(dist_values)},backgroundColor:'rgba(96,165,250,.30)',borderColor:'#60a5fa',borderWidth:1}}]}},options:{{responsive:true,plugins:{{legend:{{labels:{{color:'#e5e7eb'}}}}}},scales:{{x:{{ticks:{{color:'#94a3b8'}}}},y:{{ticks:{{color:'#94a3b8'}}}}}}}}}});
    </script>
    '''


def render_monitor(cur):
    _, platform, _ = current_filters()
    cur.execute('''
        SELECT COUNT(*) total,
            SUM(CASE WHEN status='OK' THEN 1 ELSE 0 END) ok_count,
            SUM(CASE WHEN status='WARN' THEN 1 ELSE 0 END) warn_count,
            SUM(CASE WHEN status='INCIDENT' THEN 1 ELSE 0 END) incident_count
        FROM teams WHERE active = TRUE
    ''')
    s = cur.fetchone()

    if platform:
        cur.execute('SELECT id,name,app_name,lastfm_user,status,last_scrobble_at,idle_minutes,last_check_at FROM teams WHERE LOWER(app_name)=%s ORDER BY id ASC', (platform,))
    else:
        cur.execute('SELECT id,name,app_name,lastfm_user,status,last_scrobble_at,idle_minutes,last_check_at FROM teams ORDER BY id ASC')
    teams = cur.fetchall()

    rows = ''
    for t in teams:
        rows += f'''
        <tr>
            <td><input type="checkbox" name="team_ids" value="{t['id']}"> {t['id']}</td>
            <td>{t['name']}</td><td>{t['app_name']}</td><td>{t['lastfm_user']}</td>
            <td>{badge(t['status'])}</td><td>{t['last_scrobble_at'] or '-'}</td><td>{t['idle_minutes'] if t['idle_minutes'] is not None else '-'}</td><td>{t['last_check_at'] or '-'}</td>
            <td><a class="btn btn-danger" href="/delete-team?id={t['id']}" onclick="return confirm('¿Borrar equipo?')">Borrar</a></td>
        </tr>
        '''
    if not rows:
        rows = '<tr><td colspan="9" class="muted">No hay equipos.</td></tr>'

    return f'''
    {filter_form('monitor')}
    <div class="card" style="margin-bottom:18px;"><div class="section-title">Agregar usuario Last.fm</div><form class="form-grid" method="GET" action="/seed-team"><div class="field"><label>Equipo</label><input name="name" required></div><div class="field"><label>App</label><select name="app"><option value="spotify">spotify</option><option value="apple">apple</option><option value="tidal">tidal</option><option value="youtube">youtube</option></select></div><div class="field"><label>Usuario Last.fm</label><input name="user" required></div><button class="btn btn-primary">Agregar</button></form></div>
    <div class="grid"><div class="card"><div class="label">Monitores activos</div><div class="value">{safe_int(s['total'])}</div></div><div class="card"><div class="label">OK</div><div class="value green">{safe_int(s['ok_count'])}</div></div><div class="card"><div class="label">WARN</div><div class="value yellow">{safe_int(s['warn_count'])}</div></div><div class="card"><div class="label">INCIDENT</div><div class="value red">{safe_int(s['incident_count'])}</div></div></div>
    <form method="POST" action="/collect-all-selected"><div style="margin-bottom:12px;"><button class="btn btn-primary" type="submit">Collect All seleccionados</button></div><table><thead><tr><th>ID</th><th>Equipo</th><th>App</th><th>User</th><th>Status</th><th>Último scrobble</th><th>Idle</th><th>Último check</th><th>Acciones</th></tr></thead><tbody>{rows}</tbody></table></form>
    '''


def render_analisis(cur):
    where, params = month_where('s')
    cur.execute(f'SELECT COUNT(*) c FROM scrobbles s WHERE {where}', params)
    plays_month = safe_int(cur.fetchone()['c'])
    cur.execute(f'SELECT DATE(s.scrobble_time) AS play_day, COUNT(*) AS plays FROM scrobbles s WHERE {where} GROUP BY DATE(s.scrobble_time) ORDER BY play_day ASC', params)
    daily = cur.fetchall()
    labels = [str(r['play_day'])[5:] for r in daily]
    values = [safe_int(r['plays']) for r in daily]
    avg_daily = round(plays_month / max(len(daily), 1), 2)
    return f'''
    {filter_form('analisis')}
    <div class="grid-3"><div class="card"><div class="label">Plays filtrados</div><div class="value">{plays_month}</div></div><div class="card"><div class="label">Días con data</div><div class="value">{len(daily)}</div></div><div class="card"><div class="label">Promedio diario</div><div class="value">{avg_daily}</div></div></div>
    <div class="card"><div class="section-title">Reproducciones diarias</div><canvas id="playsChart"></canvas></div>
    <script>new Chart(document.getElementById('playsChart'), {{type:'line',data:{{labels:{json.dumps(labels)},datasets:[{{label:'Reproducciones',data:{json.dumps(values)},borderColor:'#60a5fa',backgroundColor:'rgba(96,165,250,.15)',tension:.35,fill:true}}]}},options:{{responsive:true,plugins:{{legend:{{labels:{{color:'#e5e7eb'}}}}}},scales:{{x:{{ticks:{{color:'#94a3b8'}}}},y:{{ticks:{{color:'#94a3b8'}}}}}}}}}});</script>
    '''


def render_ganancias(cur):
    where, params = month_where('s')
    cur.execute(f'SELECT LOWER(s.app_name) AS platform, COUNT(*) AS plays FROM scrobbles s WHERE {where} GROUP BY LOWER(s.app_name) ORDER BY plays DESC', params)
    platforms = cur.fetchall()
    total_min = total_max = 0
    rows = ''
    for r in platforms:
        p = (r['platform'] or '').lower()
        plays = safe_int(r['plays'])
        rate = PLATFORM_RATES.get(p, PLATFORM_RATES['spotify'])
        mn, mx = plays * rate['min'], plays * rate['max']
        total_min += mn
        total_max += mx
        rows += f'<tr><td>{p.title()}</td><td>{plays}</td><td>{money(mn)}</td><td>{money(mx)}</td><td>{money((mn+mx)/2)}</td></tr>'
    if not rows:
        rows = '<tr><td colspan="5" class="muted">Sin datos</td></tr>'
    return f'''
    {filter_form('ganancias')}
    <div class="grid-3"><div class="card"><div class="label">Mínimo estimado</div><div class="value">{money(total_min)}</div></div><div class="card"><div class="label">Máximo estimado</div><div class="value">{money(total_max)}</div></div><div class="card"><div class="label">Promedio estimado</div><div class="value">{money((total_min+total_max)/2)}</div></div></div>
    <table><thead><tr><th>Plataforma</th><th>Streams</th><th>Min</th><th>Max</th><th>Promedio</th></tr></thead><tbody>{rows}</tbody></table>
    '''


def render_monitor_plays(cur):
    where, params = month_where('s')
    cur.execute(f'SELECT s.artist_name, s.track_name, COUNT(*) AS plays FROM scrobbles s WHERE {where} GROUP BY s.artist_name, s.track_name HAVING COUNT(*) < 1000 ORDER BY plays DESC LIMIT 200', params)
    rows = cur.fetchall()
    table_rows = ''
    for r in rows:
        plays = safe_int(r['plays'])
        faltan = 1000 - plays
        table_rows += f'<tr><td>{r["artist_name"]}</td><td>{r["track_name"]}</td><td>{plays}</td><td>{faltan}</td><td>Dar {faltan} reproducciones</td></tr>'
    if not table_rows:
        table_rows = '<tr><td colspan="5" class="muted">No hay canciones debajo de 1000.</td></tr>'
    return f'''
    {filter_form('monitor-plays')}
    <div class="section-title">Monitor Plays Pro</div>
    <table><thead><tr><th>Artista</th><th>Canción</th><th>Plays</th><th>Faltan 1K</th><th>Recomendación</th></tr></thead><tbody>{table_rows}</tbody></table>
    '''

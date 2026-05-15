from flask import request

from utils import safe_int, money
from layout import filter_form
from views import month_where, avg_rate


def priority_badge(plays):
    if plays >= 1000:
        return '<span class="badge ok">COMPLETA</span>', 'Mantener rotación'
    if plays >= 900:
        return '<span class="badge ok">CERCA</span>', 'Empujar cierre a 1K'
    if plays >= 500:
        return '<span class="badge warn">MEDIA</span>', 'Mantener rotación y subir frecuencia'
    return '<span class="badge incident">ALTA</span>', 'Prioridad de empuje para playlist'


def render_monitor_plays(cur):
    where, params = month_where('s')

    page = safe_int(request.args.get('page'), 1)
    if page < 1:
        page = 1
    per_page = safe_int(request.args.get('per_page'), 50)
    if per_page not in [25, 50, 100, 200]:
        per_page = 50
    offset = (page - 1) * per_page

    cur.execute(f'''
        SELECT COUNT(*) AS total_tracks
        FROM (
            SELECT s.artist_name, s.track_name, LOWER(COALESCE(s.app_name, 'spotify')) AS platform
            FROM scrobbles s
            WHERE {where}
            GROUP BY s.artist_name, s.track_name, LOWER(COALESCE(s.app_name, 'spotify'))
        ) x
    ''', params)
    total_all = safe_int(cur.fetchone()['total_tracks'])

    cur.execute(f'''
        SELECT COUNT(*) AS below_1k,
               SUM(CASE WHEN plays >= 900 AND plays < 1000 THEN 1 ELSE 0 END) AS near_goal,
               SUM(CASE WHEN plays < 500 THEN 1 ELSE 0 END) AS high_priority,
               SUM(CASE WHEN plays < 1000 THEN 1000 - plays ELSE 0 END) AS total_missing
        FROM (
            SELECT COUNT(*) AS plays
            FROM scrobbles s
            WHERE {where}
            GROUP BY s.artist_name, s.track_name, LOWER(COALESCE(s.app_name, 'spotify'))
        ) x
    ''', params)
    summary = cur.fetchone()

    cur.execute(f'''
        SELECT
            s.artist_name,
            s.track_name,
            LOWER(COALESCE(s.app_name, 'spotify')) AS platform,
            COUNT(*) AS plays,
            MAX(s.scrobble_time) AS last_play_at
        FROM scrobbles s
        WHERE {where}
        GROUP BY s.artist_name, s.track_name, LOWER(COALESCE(s.app_name, 'spotify'))
        ORDER BY
            CASE WHEN COUNT(*) < 1000 THEN 0 ELSE 1 END,
            COUNT(*) ASC,
            s.artist_name ASC,
            s.track_name ASC
        LIMIT %s OFFSET %s
    ''', [*params, per_page, offset])
    rows = cur.fetchall()

    table_rows = ''
    for r in rows:
        plays = safe_int(r['plays'])
        missing = max(1000 - plays, 0)
        progress = min(round((plays / 1000) * 100, 1), 100)
        gain = plays * avg_rate(r['platform'])
        prioridad, recomendacion = priority_badge(plays)

        table_rows += f'''
        <tr>
            <td>{r['artist_name']}</td>
            <td>{r['track_name']}</td>
            <td>{r['platform']}</td>
            <td>{plays:,}</td>
            <td>{missing:,}</td>
            <td>{progress}%</td>
            <td class="green">{money(gain)}</td>
            <td>{prioridad}</td>
            <td>{recomendacion}</td>
            <td>{r['last_play_at'] or '-'}</td>
        </tr>
        '''

    if not table_rows:
        table_rows = '<tr><td colspan="10" class="muted">Sin canciones para mostrar.</td></tr>'

    total_pages = max((total_all + per_page - 1) // per_page, 1)
    prev_page = max(page - 1, 1)
    next_page = min(page + 1, total_pages)

    base_q = 'view=monitor-plays'
    month = request.args.get('month') or ''
    platform = request.args.get('platform') or ''
    distributor = request.args.get('distributor') or ''
    if month:
        base_q += f'&month={month}'
    if platform:
        base_q += f'&platform={platform}'
    if distributor:
        base_q += f'&distributor={distributor}'
    base_q += f'&per_page={per_page}'

    download_q = request.query_string.decode('utf-8')
    if download_q:
        download_url = '/export-monitor-plays.csv?' + download_q
    else:
        download_url = '/export-monitor-plays.csv'

    return f'''
    {filter_form('monitor-plays')}

    <div class="grid">
        <div class="card"><div class="label">Canciones existentes</div><div class="value blue">{total_all:,}</div></div>
        <div class="card"><div class="label">Bajo 1K</div><div class="value yellow">{safe_int(summary['below_1k']):,}</div></div>
        <div class="card"><div class="label">Cerca de meta</div><div class="value green">{safe_int(summary['near_goal']):,}</div></div>
        <div class="card"><div class="label">Prioridad alta</div><div class="value red">{safe_int(summary['high_priority']):,}</div></div>
    </div>

    <div class="grid-2">
        <div class="card">
            <div class="section-title">Plan para playlist 1K</div>
            <div class="mini-row"><span>Reproducciones faltantes totales</span><strong class="yellow">{safe_int(summary['total_missing']):,}</strong></div>
            <div class="mini-row"><span>Orden recomendado</span><strong>Menos plays primero</strong></div>
            <div class="mini-row"><span>Uso sugerido</span><strong>Exportar CSV y crear playlist por prioridad</strong></div>
        </div>
        <div class="card">
            <div class="section-title">Acciones</div>
            <div class="mini-row"><span>Descargar canciones para análisis</span><a class="btn btn-primary" href="{download_url}">Descargar CSV</a></div>
            <div class="mini-row"><span>Página actual</span><strong>{page} / {total_pages}</strong></div>
            <div class="mini-row"><span>Filas por página</span><strong>{per_page}</strong></div>
        </div>
    </div>

    <div class="section-title">Todas las canciones existentes</div>
    <table>
        <thead><tr><th>Artista</th><th>Canción</th><th>App</th><th>Plays</th><th>Faltan 1K</th><th>Avance</th><th>Ganancia</th><th>Prioridad</th><th>Recomendación</th><th>Última reproducción</th></tr></thead>
        <tbody>{table_rows}</tbody>
    </table>

    <div class="card" style="margin-top:18px;">
        <div class="mini-row">
            <a class="btn btn-secondary" href="/?{base_q}&page={prev_page}">Anterior</a>
            <strong>Página {page} de {total_pages}</strong>
            <a class="btn btn-secondary" href="/?{base_q}&page={next_page}">Siguiente</a>
        </div>
        <div class="mini-row"><span>Vista protegida</span><strong>No se cambia el diseño base, solo se agregan datos y paginación.</strong></div>
    </div>
    '''

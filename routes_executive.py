from datetime import datetime, timedelta

from config import PLATFORM_RATES
from utils import safe_int, money, current_filters
from layout import filter_form


def platform_rate(platform):
    p = (platform or '').strip().lower()
    r = PLATFORM_RATES.get(p, PLATFORM_RATES['spotify'])
    return (r['min'] + r['max']) / 2


def pct_change(current, previous):
    current = float(current or 0)
    previous = float(previous or 0)
    if previous == 0 and current == 0:
        return '0% vs base'
    if previous == 0:
        return '+100% vs base'
    pct = ((current - previous) / previous) * 100
    sign = '+' if pct >= 0 else ''
    return f'{sign}{pct:.1f}%'


def trend_class(current, previous):
    try:
        return 'green' if float(current or 0) >= float(previous or 0) else 'red'
    except Exception:
        return 'muted'


def metric_card(label, value, compare_label, compare_value, color='blue'):
    return f'''
    <div class="card">
        <div class="label">{label}</div>
        <div class="value {color}">{value}</div>
        <div class="muted" style="margin-top:8px; font-size:13px;">
            <span class="{trend_class(compare_value[0], compare_value[1])}">{compare_label}: {pct_change(compare_value[0], compare_value[1])}</span>
        </div>
    </div>
    '''


def build_extra_filters(alias='s'):
    _, platform, distributor = current_filters()
    clauses = []
    params = []

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

    return clauses, params


def render_ejecutivo_fast(cur):
    today = datetime.utcnow().date()
    tomorrow = today + timedelta(days=1)
    yesterday = today - timedelta(days=1)
    month_start = today.replace(day=1)

    if month_start.month == 1:
        prev_month_start = month_start.replace(year=month_start.year - 1, month=12)
    else:
        prev_month_start = month_start.replace(month=month_start.month - 1)

    extra_clauses, extra_params = build_extra_filters('s')
    extra_sql = ''
    if extra_clauses:
        extra_sql = ' AND ' + ' AND '.join(extra_clauses)

    cur.execute(f'''
        SELECT
            CASE
                WHEN s.scrobble_time >= %s AND s.scrobble_time < %s THEN 'hoy'
                WHEN s.scrobble_time >= %s AND s.scrobble_time < %s THEN 'ayer'
                WHEN s.scrobble_time >= %s AND s.scrobble_time < %s THEN 'mes_actual'
                WHEN s.scrobble_time >= %s AND s.scrobble_time < %s THEN 'mes_pasado'
                ELSE 'otro'
            END AS periodo,
            LOWER(s.app_name) AS platform,
            COUNT(*) AS plays
        FROM scrobbles s
        WHERE s.scrobble_time >= %s
          AND s.scrobble_time < %s
          {extra_sql}
        GROUP BY periodo, LOWER(s.app_name)
    ''', [
        today, tomorrow,
        yesterday, today,
        month_start, tomorrow,
        prev_month_start, month_start,
        prev_month_start, tomorrow,
        *extra_params
    ])

    rows = cur.fetchall()

    metrics = {
        'hoy': {'plays': 0, 'gain': 0.0},
        'ayer': {'plays': 0, 'gain': 0.0},
        'mes_actual': {'plays': 0, 'gain': 0.0},
        'mes_pasado': {'plays': 0, 'gain': 0.0},
    }

    for r in rows:
        periodo = r['periodo']
        if periodo not in metrics:
            continue
        plays = safe_int(r['plays'])
        gain = plays * platform_rate(r['platform'])
        metrics[periodo]['plays'] += plays
        metrics[periodo]['gain'] += gain

    cur.execute('SELECT COUNT(*) total FROM teams WHERE active = TRUE')
    teams = cur.fetchone()

    cur.execute(f'''
        SELECT s.artist_name, COUNT(*) AS plays
        FROM scrobbles s
        WHERE s.scrobble_time >= %s
          AND s.scrobble_time < %s
          {extra_sql}
        GROUP BY s.artist_name
        ORDER BY plays DESC
        LIMIT 8
    ''', [month_start, tomorrow, *extra_params])
    artists = cur.fetchall()

    artist_html = ''.join([
    f"""
    <tr>
        <td>{r['artist_name']}</td>
        <td>{safe_int(r['plays']):,}</td>
        <td class='green'>{money(safe_int(r['plays']) * 0.0054)}</td>
    </tr>
    """
    for r in artists
]) or """
<tr>
    <td colspan='3' class='muted'>Sin datos</td>
</tr>
"""

    cur.execute(f'''
        SELECT COALESCE(am.distributor, 'Sin distribuidora') AS distributor, COUNT(*) AS plays
        FROM scrobbles s
        LEFT JOIN artist_metadata am ON LOWER(am.artist_name) = LOWER(s.artist_name)
        WHERE s.scrobble_time >= %s
          AND s.scrobble_time < %s
          {extra_sql}
        GROUP BY COALESCE(am.distributor, 'Sin distribuidora')
        ORDER BY plays DESC
        LIMIT 8
    ''', [month_start, tomorrow, *extra_params])
    dist = cur.fetchall()

    dist_html = ''.join([
        f"<div class='mini-row'><span>{r['distributor']}</span><strong>{r['plays']}</strong></div>"
        for r in dist
    ]) or '<div class="muted">Sin datos</div>'

    h = metrics['hoy']
    a = metrics['ayer']
    m = metrics['mes_actual']
    pm = metrics['mes_pasado']

    return f'''
    {filter_form('ejecutivo')}

    <div class="grid">
        {metric_card('Plays hoy', f"{h['plays']:,}", 'vs ayer', (h['plays'], a['plays']), 'blue')}
        {metric_card('Ganancia hoy', money(h['gain']), 'vs ayer', (h['gain'], a['gain']), 'green')}
        {metric_card('Plays mes actual', f"{m['plays']:,}", 'vs mes pasado', (m['plays'], pm['plays']), 'blue')}
        {metric_card('Ganancia mes actual', money(m['gain']), 'vs mes pasado', (m['gain'], pm['gain']), 'green')}
    </div>

    <div class="grid-3">
        <div class="card"><div class="label">Plays ayer</div><div class="value muted">{a['plays']:,}</div></div>
        <div class="card"><div class="label">Ganancia ayer</div><div class="value muted">{money(a['gain'])}</div></div>
        <div class="card"><div class="label">Equipos activos</div><div class="value yellow">{safe_int(teams['total'])}</div></div>
    </div>

    <div class="grid-2">
        <div class="card"><div class="section-title">Top artistas del mes</div>{artist_html}</div>
        <div class="card"><div class="section-title">Distribuidoras del mes</div>{dist_html}</div>
    </div>

    <div class="card" style="margin-bottom:18px;">
        <div class="section-title">Lectura ejecutiva</div>
        <div class="mini-row"><span>Mes actual vs mes pasado en plays</span><strong class="{trend_class(m['plays'], pm['plays'])}">{pct_change(m['plays'], pm['plays'])}</strong></div>
        <div class="mini-row"><span>Mes actual vs mes pasado en ganancias</span><strong class="{trend_class(m['gain'], pm['gain'])}">{pct_change(m['gain'], pm['gain'])}</strong></div>
        <div class="mini-row"><span>Hoy vs ayer en plays</span><strong class="{trend_class(h['plays'], a['plays'])}">{pct_change(h['plays'], a['plays'])}</strong></div>
        <div class="mini-row"><span>Hoy vs ayer en ganancias</span><strong class="{trend_class(h['gain'], a['gain'])}">{pct_change(h['gain'], a['gain'])}</strong></div>
    </div>
    '''

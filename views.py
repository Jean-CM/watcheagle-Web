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

    cur.execute(f'''
        SELECT artist_name, COUNT(*) plays
        FROM scrobbles s
        WHERE {where}
        GROUP BY artist_name
        ORDER BY plays DESC
        LIMIT 8
    ''', params)
    artists = cur.fetchall()

    artist_rows = ''
    for row in artists:
        plays = safe_int(row['plays'])
        revenue = plays * 0.0054
        artist_rows += f'<tr><td>{row["artist_name"]}</td><td>{plays:,}</td><td class="green">${revenue:,.2f}</td></tr>'

    cur.execute(f'''
        SELECT COALESCE(am.distributor,'Sin distribuidora') distributor, COUNT(*) plays
        FROM scrobbles s
        LEFT JOIN artist_metadata am ON LOWER(am.artist_name)=LOWER(s.artist_name)
        WHERE {where}
        GROUP BY distributor
        ORDER BY plays DESC
        LIMIT 8
    ''', params)
    distributors = cur.fetchall()

    distributor_rows = ''
    for row in distributors:
        plays = safe_int(row['plays'])
        revenue = plays * 0.0054
        distributor_rows += f'<tr><td>{row["distributor"]}</td><td>{plays:,}</td><td class="green">${revenue:,.2f}</td></tr>'

    return f'''
    {filter_form('ejecutivo')}

    <div class="grid-2" style="margin-top:18px;">
      <div class="card">
        <div class="section-title">Artistas del mes</div>
        <table class="table">
          <thead>
            <tr>
              <th>Artista</th>
              <th>Reproducciones</th>
              <th>Ganancia estimada</th>
            </tr>
          </thead>
          <tbody>
            {artist_rows}
          </tbody>
        </table>
      </div>

      <div class="card">
        <div class="section-title">Distribuidoras del mes</div>
        <table class="table">
          <thead>
            <tr>
              <th>Distribuidora</th>
              <th>Reproducciones</th>
              <th>Ganancia estimada</th>
            </tr>
          </thead>
          <tbody>
            {distributor_rows}
          </tbody>
        </table>
      </div>
    </div>
    '''


def render_monitor(cur):
    return '<div class="card">Monitor operativo.</div>'


def render_analisis(cur):
    return '<div class="card">Analisis operativo.</div>'


def render_ganancias(cur):
    return '<div class="card">Ganancias operativas.</div>'


def render_monitor_plays(cur):
    return '<div class="card">Monitor plays operativo.</div>'

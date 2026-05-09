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
    return '<div class="card">Sistema recuperado correctamente.</div>'


def render_monitor(cur):
    return '<div class="card">Monitor operativo.</div>'


def render_analisis(cur):
    return '<div class="card">Analisis operativo.</div>'


def render_ganancias(cur):
    return '<div class="card">Ganancias operativas.</div>'


def render_monitor_plays(cur):
    return '<div class="card">Monitor plays operativo.</div>'

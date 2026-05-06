import os
import threading
import requests
from datetime import datetime
from flask import jsonify, redirect

from helpers import get_conn, init_db
from config import LASTFM_API_KEY, LASTFM_HISTORY_MARGIN_DAYS, JOB_LOG_DIR
from utils import parse_ts, safe_int
from layout import badge


def ensure_lastfm_history_table(cur):
    cur.execute('''
        CREATE TABLE IF NOT EXISTS lastfm_history_status (
            id SERIAL PRIMARY KEY,
            team_id INTEGER UNIQUE,
            team_name TEXT,
            app_name TEXT,
            lastfm_user TEXT UNIQUE,
            lastfm_created_at TIMESTAMP NULL,
            first_scrobble_at TIMESTAMP NULL,
            last_scrobble_at TIMESTAMP NULL,
            total_scrobbles INTEGER DEFAULT 0,
            history_status TEXT DEFAULT 'PENDIENTE',
            recommendation TEXT,
            error_message TEXT,
            checked_at TIMESTAMP DEFAULT NOW()
        )
    ''')


def fetch_lastfm_user_created_at(lastfm_user):
    if not LASTFM_API_KEY:
        raise Exception('LASTFM_API_KEY no está configurado')

    url = 'https://ws.audioscrobbler.com/2.0/'
    params = {
        'method': 'user.getInfo',
        'user': lastfm_user,
        'api_key': LASTFM_API_KEY,
        'format': 'json'
    }

    r = requests.get(url, params=params, timeout=12)
    data = r.json()

    if 'error' in data:
        raise Exception(f"Last.fm API error {data.get('error')}: {data.get('message')}")

    registered = data.get('user', {}).get('registered', {})
    return parse_ts(registered.get('unixtime'))


def diagnose_one_team(cur, team):
    user = team['lastfm_user']
    team_id = team['id']
    name = team['name']
    app_name = team['app_name']
    error_message = None

    try:
        created_at = fetch_lastfm_user_created_at(user)
    except Exception as e:
        created_at = None
        error_message = str(e)

    cur.execute('''
        SELECT MIN(scrobble_time) first_scrobble_at,
               MAX(scrobble_time) last_scrobble_at,
               COUNT(*) total_scrobbles
        FROM scrobbles
        WHERE lastfm_user = %s
    ''', (user,))

    s = cur.fetchone()
    first_at = s['first_scrobble_at']
    last_at = s['last_scrobble_at']
    total = safe_int(s['total_scrobbles'])

    if error_message:
        status = 'ERROR_LASTFM'
        recommendation = 'Revisar usuario o API Last.fm'
    elif total == 0:
        status = 'SIN_DATA'
        recommendation = 'Ejecutar collect-all para este equipo'
    elif created_at and first_at:
        delta_days = (first_at.date() - created_at.date()).days
        if delta_days <= LASTFM_HISTORY_MARGIN_DAYS:
            status = 'COMPLETO'
            recommendation = 'Mantener collect-now'
        else:
            status = 'FALTA_HISTORICO'
            recommendation = f'Ejecutar collect-all: faltan aprox. {delta_days} días desde creación'
    else:
        status = 'PENDIENTE'
        recommendation = 'Revisar datos'

    cur.execute('''
        INSERT INTO lastfm_history_status (
            team_id, team_name, app_name, lastfm_user, lastfm_created_at,
            first_scrobble_at, last_scrobble_at, total_scrobbles,
            history_status, recommendation, error_message, checked_at
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
        ON CONFLICT (lastfm_user) DO UPDATE SET
            team_id = EXCLUDED.team_id,
            team_name = EXCLUDED.team_name,
            app_name = EXCLUDED.app_name,
            lastfm_created_at = EXCLUDED.lastfm_created_at,
            first_scrobble_at = EXCLUDED.first_scrobble_at,
            last_scrobble_at = EXCLUDED.last_scrobble_at,
            total_scrobbles = EXCLUDED.total_scrobbles,
            history_status = EXCLUDED.history_status,
            recommendation = EXCLUDED.recommendation,
            error_message = EXCLUDED.error_message,
            checked_at = NOW()
    ''', (
        team_id, name, app_name, user, created_at, first_at, last_at, total,
        status, recommendation, error_message
    ))

    return status


def run_lastfm_history_diagnostic():
    os.makedirs(JOB_LOG_DIR, exist_ok=True)
    log_path = os.path.join(JOB_LOG_DIR, 'lastfm-history.log')

    with open(log_path, 'w', encoding='utf-8') as f:
        f.write('JOB: lastfm-history\n')
        f.write(f'STARTED UTC: {datetime.utcnow()}\n')
        f.write('\n==================== OUTPUT ====================\n\n')

        try:
            init_db()
            conn = get_conn()
            cur = conn.cursor()
            ensure_lastfm_history_table(cur)

            cur.execute('''
                SELECT id,name,app_name,lastfm_user
                FROM teams
                WHERE active = TRUE
                ORDER BY id ASC
            ''')
            teams = cur.fetchall()
            counts = {}

            for i, team in enumerate(teams, start=1):
                status = diagnose_one_team(cur, team)
                counts[status] = counts.get(status, 0) + 1
                conn.commit()
                f.write(f"[{i}/{len(teams)}] {team['name']} | {team['lastfm_user']} | {status}\n")
                f.flush()

            f.write('\nRESUMEN:\n')
            for k, v in counts.items():
                f.write(f'{k}: {v}\n')

            cur.close()
            conn.close()
            f.write(f'\nFINISHED UTC: {datetime.utcnow()}\n')

        except Exception as e:
            f.write(f'\nERROR: {str(e)}\n')


def render_historico(cur):
    ensure_lastfm_history_table(cur)

    cur.execute('''
        SELECT
            COUNT(*) total,
            SUM(CASE WHEN history_status='COMPLETO' THEN 1 ELSE 0 END) completos,
            SUM(CASE WHEN history_status='FALTA_HISTORICO' THEN 1 ELSE 0 END) faltan,
            SUM(CASE WHEN history_status='SIN_DATA' THEN 1 ELSE 0 END) sin_data,
            SUM(CASE WHEN history_status='ERROR_LASTFM' THEN 1 ELSE 0 END) errores,
            MAX(checked_at) last_check
        FROM lastfm_history_status
    ''')
    s = cur.fetchone()

    cur.execute('''
        SELECT team_id, team_name, app_name, lastfm_user, lastfm_created_at,
               first_scrobble_at, last_scrobble_at, total_scrobbles,
               history_status, recommendation, checked_at, error_message
        FROM lastfm_history_status
        ORDER BY
            CASE history_status
                WHEN 'FALTA_HISTORICO' THEN 1
                WHEN 'SIN_DATA' THEN 2
                WHEN 'ERROR_LASTFM' THEN 3
                WHEN 'PENDIENTE' THEN 4
                ELSE 5
            END,
            team_id ASC
    ''')
    rows = cur.fetchall()

    table_rows = ''
    for r in rows:
        table_rows += f'''
        <tr>
            <td><input type="checkbox" name="team_ids" value="{r['team_id']}"> {r['team_id']}</td>
            <td>{r['team_name']}</td>
            <td>{r['app_name']}</td>
            <td>{r['lastfm_user']}</td>
            <td>{r['lastfm_created_at'] or '-'}</td>
            <td>{r['first_scrobble_at'] or '-'}</td>
            <td>{r['last_scrobble_at'] or '-'}</td>
            <td>{safe_int(r['total_scrobbles'])}</td>
            <td>{badge(r['history_status'])}</td>
            <td>{r['recommendation'] or '-'}</td>
        </tr>
        '''

    if not table_rows:
        table_rows = '<tr><td colspan="10" class="muted">No hay diagnóstico todavía. Ejecuta Actualizar diagnóstico Last.fm.</td></tr>'

    return f'''
    <div class="grid">
        <div class="card"><div class="label">Equipos diagnosticados</div><div class="value blue">{safe_int(s['total'])}</div></div>
        <div class="card"><div class="label">Histórico completo</div><div class="value green">{safe_int(s['completos'])}</div></div>
        <div class="card"><div class="label">Falta histórico</div><div class="value yellow">{safe_int(s['faltan'])}</div></div>
        <div class="card"><div class="label">Sin data / error</div><div class="value red">{safe_int(s['sin_data']) + safe_int(s['errores'])}</div></div>
    </div>

    <div class="card" style="margin-bottom:18px;">
        <div class="section-title">Control histórico Last.fm</div>
        <div class="mini-row"><span>Último diagnóstico</span><strong>{s['last_check'] or '-'}</strong></div>
        <div class="mini-row"><span>Regla</span><strong>Cacheado; no consulta Last.fm al abrir la vista</strong></div>
    </div>

    <div class="actions" style="margin-bottom:18px;">
        <a class="btn btn-primary" href="/refresh-lastfm-history">Actualizar diagnóstico Last.fm</a>
        <a class="btn btn-secondary" href="/job-log?job=lastfm-history">Ver log</a>
    </div>

    <form method="POST" action="/collect-all-selected">
        <div style="margin-bottom:12px;"><button class="btn btn-primary" type="submit">Collect All seleccionados</button></div>
        <table>
            <thead><tr><th>ID</th><th>Equipo</th><th>App</th><th>User</th><th>Creación Last.fm</th><th>Primer scrobble DB</th><th>Último scrobble DB</th><th>Total</th><th>Estado</th><th>Recomendación</th></tr></thead>
            <tbody>{table_rows}</tbody>
        </table>
    </form>
    '''


def register_history_routes(app):

    @app.route('/init-lastfm-history-table')
    def init_lastfm_history_table():
        conn = get_conn()
        cur = conn.cursor()
        ensure_lastfm_history_table(cur)
        conn.commit()
        cur.close()
        conn.close()
        return jsonify({'ok': True, 'table': 'lastfm_history_status'})

    @app.route('/refresh-lastfm-history')
    def refresh_lastfm_history():
        threading.Thread(target=run_lastfm_history_diagnostic, daemon=True).start()
        return redirect('/job-log?job=lastfm-history')

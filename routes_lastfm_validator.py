import requests
from datetime import datetime
from flask import jsonify

from config import LASTFM_API_KEY
from helpers import get_conn
from utils import safe_int
from layout import badge

LASTFM_URL = 'https://ws.audioscrobbler.com/2.0/'


def ensure_validation_table(cur):
    cur.execute('''
        CREATE TABLE IF NOT EXISTS lastfm_history_validation (
            id SERIAL PRIMARY KEY,
            team_id INTEGER UNIQUE,
            team_name TEXT,
            app_name TEXT,
            lastfm_user TEXT UNIQUE,
            lastfm_total INTEGER DEFAULT 0,
            db_total INTEGER DEFAULT 0,
            missing_total INTEGER DEFAULT 0,
            coverage_percent NUMERIC DEFAULT 0,
            first_db_scrobble TIMESTAMP NULL,
            last_db_scrobble TIMESTAMP NULL,
            status TEXT DEFAULT 'PENDIENTE',
            recommendation TEXT,
            error_message TEXT,
            checked_at TIMESTAMP DEFAULT NOW()
        )
    ''')


def lastfm_total_scrobbles(user):
    if not LASTFM_API_KEY:
        raise Exception('LASTFM_API_KEY no configurado')
    r = requests.get(LASTFM_URL, params={
        'method': 'user.getInfo',
        'user': user,
        'api_key': LASTFM_API_KEY,
        'format': 'json'
    }, timeout=15)
    data = r.json()
    if 'error' in data:
        raise Exception(f"Last.fm API error {data.get('error')}: {data.get('message')}")
    return safe_int(data.get('user', {}).get('playcount'), 0)


def validate_one(cur, team):
    user = team['lastfm_user']
    err = None
    lf_total = 0
    try:
        lf_total = lastfm_total_scrobbles(user)
    except Exception as e:
        err = str(e)

    cur.execute('''
        SELECT COUNT(*) db_total, MIN(scrobble_time) first_db, MAX(scrobble_time) last_db
        FROM scrobbles
        WHERE lastfm_user=%s
    ''', (user,))
    db = cur.fetchone()
    db_total = safe_int(db['db_total'])
    missing = max(lf_total - db_total, 0) if lf_total else 0
    coverage = round((db_total / lf_total) * 100, 2) if lf_total else (100 if db_total else 0)

    if err:
        status = 'ERROR_LASTFM'
        rec = 'Revisar usuario/API Last.fm'
    elif lf_total == 0 and db_total == 0:
        status = 'SIN_SCROBBLES'
        rec = 'Usuario sin scrobbles reportados por Last.fm'
    elif coverage >= 99:
        status = 'COMPLETO'
        rec = 'Histórico completo; mantener collect-now'
    elif coverage >= 95:
        status = 'CASI_COMPLETO'
        rec = 'Brecha menor; ejecutar collect-all selectivo si se requiere precisión total'
    else:
        status = 'INCOMPLETO'
        rec = f'Faltan aprox. {missing:,} scrobbles vs Last.fm; ejecutar collect-all selectivo'

    cur.execute('''
        INSERT INTO lastfm_history_validation (
            team_id, team_name, app_name, lastfm_user, lastfm_total, db_total,
            missing_total, coverage_percent, first_db_scrobble, last_db_scrobble,
            status, recommendation, error_message, checked_at
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
        ON CONFLICT (lastfm_user) DO UPDATE SET
            team_id=EXCLUDED.team_id,
            team_name=EXCLUDED.team_name,
            app_name=EXCLUDED.app_name,
            lastfm_total=EXCLUDED.lastfm_total,
            db_total=EXCLUDED.db_total,
            missing_total=EXCLUDED.missing_total,
            coverage_percent=EXCLUDED.coverage_percent,
            first_db_scrobble=EXCLUDED.first_db_scrobble,
            last_db_scrobble=EXCLUDED.last_db_scrobble,
            status=EXCLUDED.status,
            recommendation=EXCLUDED.recommendation,
            error_message=EXCLUDED.error_message,
            checked_at=NOW()
    ''', (
        team['id'], team['name'], team['app_name'], user, lf_total, db_total,
        missing, coverage, db['first_db'], db['last_db'], status, rec, err
    ))
    return status


def run_validation(cur):
    ensure_validation_table(cur)
    cur.execute('SELECT id,name,app_name,lastfm_user FROM teams WHERE active=TRUE ORDER BY id ASC')
    teams = cur.fetchall()
    counts = {}
    for team in teams:
        st = validate_one(cur, team)
        counts[st] = counts.get(st, 0) + 1
    return counts


def render_validation(cur):
    ensure_validation_table(cur)
    cur.execute('''
        SELECT COUNT(*) total,
               SUM(CASE WHEN status IN ('COMPLETO','CASI_COMPLETO') THEN 1 ELSE 0 END) ok,
               SUM(CASE WHEN status='INCOMPLETO' THEN 1 ELSE 0 END) incompletos,
               SUM(CASE WHEN status LIKE 'ERROR%' THEN 1 ELSE 0 END) errores,
               SUM(lastfm_total) lf_total,
               SUM(db_total) db_total,
               MAX(checked_at) last_check
        FROM lastfm_history_validation
    ''')
    s = cur.fetchone()
    lf_total = safe_int(s['lf_total'])
    db_total = safe_int(s['db_total'])
    global_cov = round((db_total / lf_total) * 100, 2) if lf_total else 0

    cur.execute('''
        SELECT * FROM lastfm_history_validation
        ORDER BY
          CASE status WHEN 'INCOMPLETO' THEN 1 WHEN 'ERROR_LASTFM' THEN 2 WHEN 'CASI_COMPLETO' THEN 3 ELSE 4 END,
          coverage_percent ASC,
          team_id ASC
    ''')
    rows = ''
    for r in cur.fetchall():
        rows += f'''<tr><td>{r['team_id']}</td><td>{r['team_name']}</td><td>{r['app_name']}</td><td>{r['lastfm_user']}</td><td>{safe_int(r['lastfm_total']):,}</td><td>{safe_int(r['db_total']):,}</td><td>{safe_int(r['missing_total']):,}</td><td>{float(r['coverage_percent'] or 0):.2f}%</td><td>{badge(r['status'])}</td><td>{r['recommendation'] or '-'}</td></tr>'''
    if not rows:
        rows = '<tr><td colspan="10" class="muted">Sin validación todavía.</td></tr>'

    return f'''
    <div class="grid">
      <div class="card"><div class="label">Equipos validados</div><div class="value blue">{safe_int(s['total'])}</div></div>
      <div class="card"><div class="label">OK / casi completo</div><div class="value green">{safe_int(s['ok'])}</div></div>
      <div class="card"><div class="label">Incompletos</div><div class="value yellow">{safe_int(s['incompletos'])}</div></div>
      <div class="card"><div class="label">Cobertura global</div><div class="value green">{global_cov}%</div></div>
    </div>
    <div class="card" style="margin-bottom:18px;">
      <div class="section-title">Validación real Last.fm vs DB</div>
      <div class="mini-row"><span>Total Last.fm</span><strong>{lf_total:,}</strong></div>
      <div class="mini-row"><span>Total WatchEagle DB</span><strong>{db_total:,}</strong></div>
      <div class="mini-row"><span>Última validación</span><strong>{s['last_check'] or '-'}</strong></div>
      <div style="margin-top:14px;"><a class="btn btn-primary" href="/lastfm/validate-run">Ejecutar validación real</a></div>
    </div>
    <div class="card"><div class="section-title">Detalle por equipo</div><table><thead><tr><th>ID</th><th>Equipo</th><th>App</th><th>User</th><th>Total Last.fm</th><th>Total DB</th><th>Faltan</th><th>Cobertura</th><th>Estado</th><th>Recomendación</th></tr></thead><tbody>{rows}</tbody></table></div>
    '''


def register_lastfm_validator_routes(app, get_conn, base_page):
    @app.route('/lastfm/validate')
    def lastfm_validate_home():
        conn = get_conn(); cur = conn.cursor()
        try:
            body = render_validation(cur)
            return base_page('Validación Last.fm','historico',body).replace('__LOAD_TIME__','0.00s').replace('__CACHE_STATUS__','No cache')
        finally:
            cur.close(); conn.close()

    @app.route('/lastfm/validate-run')
    def lastfm_validate_run():
        conn = get_conn(); cur = conn.cursor()
        try:
            counts = run_validation(cur)
            conn.commit()
            return jsonify({'ok': True, 'counts': counts, 'checked_at': datetime.utcnow().isoformat()})
        except Exception as e:
            conn.rollback()
            return jsonify({'ok': False, 'error': str(e)}), 500
        finally:
            cur.close(); conn.close()

import hashlib
from datetime import datetime, timedelta

from utils import safe_int
from whatsapp_alerts import send_whatsapp_message
from views import avg_rate


def ensure_alert_log(cur):
    cur.execute('''
        CREATE TABLE IF NOT EXISTS whatsapp_alert_log (
            id SERIAL PRIMARY KEY,
            alert_key TEXT UNIQUE,
            alert_type TEXT,
            target_kind TEXT,
            message TEXT,
            sent_at TIMESTAMP DEFAULT NOW()
        )
    ''')


def already_sent_recent(cur, alert_key, hours=3):
    ensure_alert_log(cur)
    cur.execute('''
        SELECT sent_at FROM whatsapp_alert_log
        WHERE alert_key=%s AND sent_at >= NOW() - (%s || ' hours')::interval
        LIMIT 1
    ''', (alert_key, str(hours)))
    return cur.fetchone() is not None


def log_sent(cur, alert_key, alert_type, target_kind, message):
    ensure_alert_log(cur)
    cur.execute('''
        INSERT INTO whatsapp_alert_log (alert_key, alert_type, target_kind, message, sent_at)
        VALUES (%s,%s,%s,%s,NOW())
        ON CONFLICT (alert_key) DO UPDATE SET
            message=EXCLUDED.message,
            sent_at=NOW()
    ''', (alert_key, alert_type, target_kind, message))


def alert_signature(parts):
    raw = '|'.join([str(x) for x in parts])
    return hashlib.sha1(raw.encode('utf-8')).hexdigest()


def build_auto_alert(cur):
    cur.execute('''SELECT COUNT(*) total FROM teams WHERE active=TRUE AND last_scrobble_at IS NULL''')
    sin_data = safe_int(cur.fetchone()['total'])
    cur.execute('''SELECT COUNT(*) total FROM teams WHERE active=TRUE AND COALESCE(idle_minutes,999999)>=180''')
    dormidos_3h = safe_int(cur.fetchone()['total'])
    cur.execute('''SELECT COUNT(*) total FROM teams WHERE active=TRUE AND COALESCE(idle_minutes,999999)>=60 AND COALESCE(idle_minutes,999999)<180''')
    dormidos_1h = safe_int(cur.fetchone()['total'])
    risk = (sin_data * 3) + (dormidos_3h * 3) + dormidos_1h
    if risk == 0:
        return None, None
    cur.execute('''SELECT id,name,app_name,lastfm_user,idle_minutes FROM teams WHERE active=TRUE AND (last_scrobble_at IS NULL OR COALESCE(idle_minutes,999999)>=60) ORDER BY COALESCE(idle_minutes,999999) DESC LIMIT 10''')
    teams = cur.fetchall()
    label = 'WARN' if risk <= 5 else 'INCIDENT'
    lines = [
        '🦅 WatchEagle - Alerta automática',
        f'UTC: {datetime.utcnow().strftime("%Y-%m-%d %H:%M")}',
        f'Estado: {label}',
        f'Sin data: {sin_data}',
        f'Dormidos +3H: {dormidos_3h}',
        f'Dormidos +1H: {dormidos_1h}',
        '',
        'Top equipos:'
    ]
    for t in teams:
        lines.append(f'- #{t["id"]} {t["name"]} | {t["app_name"]} | {t["lastfm_user"]} | idle={t["idle_minutes"] or "N/A"} min')
    key = 'auto_alert_' + alert_signature([sin_data, dormidos_3h, dormidos_1h] + [t['id'] for t in teams])
    return key, '\n'.join(lines)


def build_summary(cur):
    today = datetime.utcnow().date()
    week_start = today - timedelta(days=today.weekday())
    month_start = today.replace(day=1)

    def totals_since(start):
        cur.execute('''
            SELECT LOWER(COALESCE(app_name,'spotify')) platform, COUNT(*) plays
            FROM scrobbles
            WHERE scrobble_time >= %s
            GROUP BY LOWER(COALESCE(app_name,'spotify'))
        ''', (start,))
        plays = 0
        revenue = 0.0
        by = []
        for r in cur.fetchall():
            p = safe_int(r['plays'])
            platform = r['platform'] or 'spotify'
            plays += p
            revenue += p * avg_rate(platform)
            by.append(f'{platform}: {p:,}')
        return plays, revenue, ', '.join(by) if by else '-'

    d_plays, d_rev, d_by = totals_since(today)
    w_plays, w_rev, w_by = totals_since(week_start)
    m_plays, m_rev, m_by = totals_since(month_start)

    cur.execute('''
        SELECT artist_name, track_name, COUNT(*) plays
        FROM scrobbles
        WHERE scrobble_time >= %s
        GROUP BY artist_name, track_name
        ORDER BY plays DESC
        LIMIT 5
    ''', (today,))
    top = cur.fetchall()
    top_lines = [f'- {r["artist_name"]} - {r["track_name"]}: {safe_int(r["plays"]):,}' for r in top] or ['- Sin datos hoy']

    msg = '\n'.join([
        '🦅 WatchEagle - Resumen ejecutivo',
        f'UTC: {datetime.utcnow().strftime("%Y-%m-%d %H:%M")}',
        '',
        f'Hoy: {d_plays:,} plays | ${d_rev:,.2f}',
        f'Por plataforma: {d_by}',
        '',
        f'Semana: {w_plays:,} plays | ${w_rev:,.2f}',
        f'Por plataforma: {w_by}',
        '',
        f'Mes: {m_plays:,} plays | ${m_rev:,.2f}',
        f'Por plataforma: {m_by}',
        '',
        'Top canciones hoy:',
        *top_lines
    ])
    key = f'summary_{today.isoformat()}_{datetime.utcnow().hour}'
    return key, msg


def register_whatsapp_auto_routes(app, get_conn, base_page):
    @app.route('/alerts/ws-auto-check')
    def ws_auto_check():
        conn = get_conn(); cur = conn.cursor()
        try:
            key, msg = build_auto_alert(cur)
            if not msg:
                return {'ok': True, 'sent': False, 'reason': 'Sin alertas activas'}
            if already_sent_recent(cur, key, hours=3):
                return {'ok': True, 'sent': False, 'reason': 'Alerta repetida dentro de ventana anti-spam'}
            res = send_whatsapp_message(msg, kind='alert')
            log_sent(cur, key, 'auto_alert', 'alert', msg)
            conn.commit()
            return {'ok': True, 'sent': True, 'result': res}
        except Exception as e:
            conn.rollback()
            return {'ok': False, 'error': str(e)}, 500
        finally:
            cur.close(); conn.close()

    @app.route('/summary/ws-send')
    def ws_summary_send():
        conn = get_conn(); cur = conn.cursor()
        try:
            key, msg = build_summary(cur)
            if already_sent_recent(cur, key, hours=1):
                return {'ok': True, 'sent': False, 'reason': 'Resumen ya enviado en esta hora'}
            res = send_whatsapp_message(msg, kind='summary')
            log_sent(cur, key, 'summary', 'summary', msg)
            conn.commit()
            body = f'<div class="card"><div class="section-title">Resumen enviado</div><pre style="white-space:pre-wrap">{msg}</pre><a class="btn btn-primary" href="/?view=alertas">Volver</a></div>'
            return base_page('Resumen WhatsApp','alertas',body).replace('__LOAD_TIME__','0.00s').replace('__CACHE_STATUS__','No cache')
        except Exception as e:
            conn.rollback()
            body = f'<div class="card"><div class="section-title">Error resumen WhatsApp</div><pre style="white-space:pre-wrap">{str(e)}</pre></div>'
            return base_page('Error resumen','alertas',body).replace('__LOAD_TIME__','0.00s').replace('__CACHE_STATUS__','No cache'),500
        finally:
            cur.close(); conn.close()

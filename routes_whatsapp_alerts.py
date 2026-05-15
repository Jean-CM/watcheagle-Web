from datetime import datetime
from flask import request

from layout import badge
from utils import safe_int
from whatsapp_alerts import configured, status, send_whatsapp_message


def build_alert_summary(cur):
    cur.execute('''SELECT COUNT(*) total FROM teams WHERE active=TRUE AND last_scrobble_at IS NULL''')
    sin_data = safe_int(cur.fetchone()['total'])
    cur.execute('''SELECT COUNT(*) total FROM teams WHERE active=TRUE AND COALESCE(idle_minutes,999999)>=180''')
    dormidos_3h = safe_int(cur.fetchone()['total'])
    cur.execute('''SELECT COUNT(*) total FROM teams WHERE active=TRUE AND COALESCE(idle_minutes,999999)>=60 AND COALESCE(idle_minutes,999999)<180''')
    dormidos_1h = safe_int(cur.fetchone()['total'])
    cur.execute('''SELECT id,name,app_name,lastfm_user,last_scrobble_at,idle_minutes FROM teams WHERE active=TRUE AND (last_scrobble_at IS NULL OR COALESCE(idle_minutes,999999)>=60) ORDER BY COALESCE(idle_minutes,999999) DESC LIMIT 10''')
    teams = cur.fetchall()
    risk = (sin_data*3) + (dormidos_3h*3) + dormidos_1h
    label = 'OK' if risk == 0 else 'WARN' if risk <= 5 else 'INCIDENT'
    lines = [
        '🦅 WatchEagle - Alerta WhatsApp',
        f'Fecha UTC: {datetime.utcnow().strftime("%Y-%m-%d %H:%M")}',
        f'Estado: {label}',
        f'Sin data: {sin_data}',
        f'Dormidos +3H: {dormidos_3h}',
        f'Dormidos +1H: {dormidos_1h}',
        '',
        'Top equipos a revisar:'
    ]
    if not teams:
        lines.append('✅ Sin equipos críticos en este momento.')
    else:
        for t in teams:
            lines.append(f'- #{t["id"]} {t["name"]} | {t["app_name"]} | user={t["lastfm_user"]} | idle={t["idle_minutes"] or "N/A"} min')
    return '\n'.join(lines), label


def render_ws_panel(cur):
    st = status()
    return f'''
    <div class="card" style="margin-bottom:18px;">
        <div class="section-title">WhatsApp Alerts</div>
        <div class="mini-row"><span>Estado</span><strong>{badge('OK' if st['configured'] else 'PENDING')}</strong></div>
        <div class="mini-row"><span>WAHA URL</span><strong>{st['base_url'] or '-'}</strong></div>
        <div class="mini-row"><span>Session</span><strong>{st['session']}</strong></div>
        <div class="mini-row"><span>Chat ID</span><strong>{st['chat_id'] or '-'}</strong></div>
        <div class="mini-row"><span>API Key</span><strong>{'Configurada' if st['has_api_key'] else 'No configurada'}</strong></div>
        <div style="margin-top:14px;display:flex;gap:10px;flex-wrap:wrap;">
            <a class="btn btn-primary" href="/alerts/ws-test">Enviar prueba WhatsApp</a>
            <a class="btn btn-secondary" href="/alerts/ws-send-summary">Enviar resumen de alertas</a>
            <a class="btn btn-secondary" href="/?view=alertas">Volver a Alertas</a>
        </div>
    </div>
    '''


def register_whatsapp_alert_routes(app, get_conn, base_page):
    @app.route('/alerts/ws')
    def alerts_ws_home():
        conn = get_conn(); cur = conn.cursor()
        try:
            body = render_ws_panel(cur)
            return base_page('WhatsApp Alerts','alertas',body).replace('__LOAD_TIME__','0.00s').replace('__CACHE_STATUS__','No cache')
        finally:
            cur.close(); conn.close()

    @app.route('/alerts/ws-test')
    def alerts_ws_test():
        try:
            msg = '🦅 WatchEagle test: WhatsApp conectado correctamente.'
            result = send_whatsapp_message(msg)
            body = f'<div class="card"><div class="section-title">Prueba enviada</div><pre style="white-space:pre-wrap">{result}</pre><a class="btn btn-primary" href="/alerts/ws">Volver</a></div>'
            return base_page('WhatsApp Test','alertas',body).replace('__LOAD_TIME__','0.00s').replace('__CACHE_STATUS__','No cache')
        except Exception as e:
            body = f'<div class="card"><div class="section-title">Error WhatsApp</div><pre style="white-space:pre-wrap">{str(e)}</pre><a class="btn btn-primary" href="/alerts/ws">Volver</a></div>'
            return base_page('Error WhatsApp','alertas',body).replace('__LOAD_TIME__','0.00s').replace('__CACHE_STATUS__','No cache'),500

    @app.route('/alerts/ws-send-summary')
    def alerts_ws_send_summary():
        conn = get_conn(); cur = conn.cursor()
        try:
            msg, label = build_alert_summary(cur)
            result = send_whatsapp_message(msg)
            body = f'<div class="card"><div class="section-title">Resumen enviado</div><div class="mini-row"><span>Estado</span><strong>{badge(label)}</strong></div><pre style="white-space:pre-wrap">{msg}</pre><pre style="white-space:pre-wrap">{result}</pre><a class="btn btn-primary" href="/alerts/ws">Volver</a></div>'
            return base_page('Resumen WhatsApp','alertas',body).replace('__LOAD_TIME__','0.00s').replace('__CACHE_STATUS__','No cache')
        except Exception as e:
            body = f'<div class="card"><div class="section-title">Error enviando resumen</div><pre style="white-space:pre-wrap">{str(e)}</pre><a class="btn btn-primary" href="/alerts/ws">Volver</a></div>'
            return base_page('Error WhatsApp','alertas',body).replace('__LOAD_TIME__','0.00s').replace('__CACHE_STATUS__','No cache'),500
        finally:
            cur.close(); conn.close()

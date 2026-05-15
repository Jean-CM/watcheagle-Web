import os
import requests

WAHA_BASE_URL = os.getenv('WAHA_BASE_URL', '').rstrip('/')
WAHA_API_KEY = os.getenv('WAHA_API_KEY', '')
WAHA_SESSION = os.getenv('WAHA_SESSION', 'default')
WA_ALERT_CHAT_ID = os.getenv('WA_ALERT_CHAT_ID', '')
WA_SUMMARY_CHAT_ID = os.getenv('WA_SUMMARY_CHAT_ID', '')


def configured(kind='alert'):
    chat_id = WA_SUMMARY_CHAT_ID if kind == 'summary' else WA_ALERT_CHAT_ID
    return bool(WAHA_BASE_URL and chat_id)


def status():
    return {
        'configured_alerts': configured('alert'),
        'configured_summary': configured('summary'),
        'base_url': WAHA_BASE_URL,
        'session': WAHA_SESSION,
        'alert_chat_id': WA_ALERT_CHAT_ID,
        'summary_chat_id': WA_SUMMARY_CHAT_ID,
        'has_api_key': bool(WAHA_API_KEY),
    }


def send_whatsapp_message(message, chat_id=None, kind='alert'):
    default_chat = WA_SUMMARY_CHAT_ID if kind == 'summary' else WA_ALERT_CHAT_ID
    target = chat_id or default_chat
    if not WAHA_BASE_URL or not target:
        raise Exception('WhatsApp no configurado. Faltan WAHA_BASE_URL o CHAT_ID.')

    url = f'{WAHA_BASE_URL}/api/sendText'
    headers = {'Content-Type': 'application/json'}
    if WAHA_API_KEY:
        headers['X-Api-Key'] = WAHA_API_KEY

    payload = {
        'session': WAHA_SESSION,
        'chatId': target,
        'text': message,
    }

    r = requests.post(url, headers=headers, json=payload, timeout=25)
    if r.status_code >= 400:
        raise Exception(f'WAHA error {r.status_code}: {r.text[:500]}')
    return r.json() if r.text else {'ok': True}

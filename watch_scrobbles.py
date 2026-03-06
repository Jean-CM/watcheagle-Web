import os
import requests
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timezone, timedelta
from twilio.rest import Client

DATABASE_URL = os.environ.get("DATABASE_URL")
LASTFM_API_KEY = os.environ.get("LASTFM_API_KEY")

WARN_MIN = int(os.environ.get("WARN_MIN", 5))
INCIDENT_MIN = int(os.environ.get("INCIDENT_MIN", 30))
ALERT_COOLDOWN_MIN = int(os.environ.get("ALERT_COOLDOWN_MIN", 30))

TWILIO_ACCOUNT_SID = os.environ.get("TWILIO_ACCOUNT_SID")
TWILIO_AUTH_TOKEN = os.environ.get("TWILIO_AUTH_TOKEN")
WHATSAPP_FROM = os.environ.get("WHATSAPP_FROM")
WHATSAPP_TO = os.environ.get("WHATSAPP_TO")


def get_conn():
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)


def get_recent_track(lastfm_user):
    url = "https://ws.audioscrobbler.com/2.0/"
    params = {
        "method": "user.getrecenttracks",
        "user": lastfm_user,
        "api_key": LASTFM_API_KEY,
        "format": "json",
        "limit": 1
    }

    response = requests.get(url, params=params, timeout=30)

    try:
        data = response.json()
    except Exception:
        raise Exception(f"Respuesta no JSON. HTTP {response.status_code}: {response.text[:300]}")

    if "error" in data:
        raise Exception(f"Last.fm API error {data.get('error')}: {data.get('message')}")

    if response.status_code != 200:
        raise Exception(f"HTTP {response.status_code}: {data}")

    recenttracks = data.get("recenttracks", {})
    track = recenttracks.get("track", [])

    if isinstance(track, list) and len(track) > 0:
        latest = track[0]
    elif isinstance(track, dict) and track:
        latest = track
    else:
        return None, False

    nowplaying = latest.get("@attr", {}).get("nowplaying") == "true"

    if nowplaying:
        return datetime.now(timezone.utc), True

    date_info = latest.get("date", {})
    uts = date_info.get("uts")

    if not uts:
        return None, False

    return datetime.fromtimestamp(int(uts), tz=timezone.utc), False


def calculate_status(idle_minutes):
    if idle_minutes >= INCIDENT_MIN:
        return "INCIDENT"
    if idle_minutes >= WARN_MIN:
        return "WARN"
    return "OK"


def send_whatsapp_message(body):
    if not all([TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, WHATSAPP_FROM, WHATSAPP_TO]):
        print("[WA] Variables de WhatsApp incompletas. No se envió mensaje.")
        return False

    try:
        client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
        message = client.messages.create(
            from_=WHATSAPP_FROM,
            body=body,
            to=WHATSAPP_TO
        )
        print(f"[WA] Mensaje enviado SID={message.sid}")
        return True
    except Exception as e:
        print(f"[WA][ERROR] {e}")
        return False


def should_send_alert(last_alert_at, now_utc):
    if last_alert_at is None:
        return True
    return now_utc - last_alert_at >= timedelta(minutes=ALERT_COOLDOWN_MIN)


def main():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT id, name, app_name, lastfm_user, status, last_alert_at
        FROM teams
        WHERE active = TRUE
        ORDER BY id ASC
    """)
    teams = cur.fetchall()

    now_utc = datetime.now(timezone.utc)

    for team in teams:
        previous_status = team["status"] or "PENDING"

        try:
            last_scrobble_at, nowplaying = get_recent_track(team["lastfm_user"])

            if last_scrobble_at is None:
                idle_minutes = 9999
                new_status = "INCIDENT"
            else:
                delta = now_utc - last_scrobble_at
                idle_minutes = max(0, int(delta.total_seconds() // 60))
                new_status = "OK" if nowplaying else calculate_status(idle_minutes)

            cur.execute("""
                UPDATE teams
                SET status = %s,
                    last_scrobble_at = %s,
                    last_check_at = %s,
                    idle_minutes = %s
                WHERE id = %s
            """, (
                new_status,
                last_scrobble_at,
                now_utc,
                idle_minutes,
                team["id"]
            ))

            # ALERTA INCIDENT
            if new_status == "INCIDENT" and should_send_alert(team["last_alert_at"], now_utc):
                ultimo = last_scrobble_at.isoformat() if last_scrobble_at else "sin registro"
                body = (
                    f"🚨 INCIDENCIA\n"
                    f"Equipo: {team['name']}\n"
                    f"App: {team['app_name']}\n"
                    f"Usuario Last.fm: {team['lastfm_user']}\n"
                    f"Idle: {idle_minutes} min\n"
                    f"Último scrobble: {ultimo}"
                )
                sent = send_whatsapp_message(body)

                if sent:
                    cur.execute("""
                        UPDATE teams
                        SET last_alert_at = %s
                        WHERE id = %s
                    """, (now_utc, team["id"]))

            # RECUPERADO
            if previous_status == "INCIDENT" and new_status == "OK":
                body = (
                    f"✅ RECUPERADO\n"
                    f"Equipo: {team['name']}\n"
                    f"App: {team['app_name']}\n"
                    f"Usuario Last.fm: {team['lastfm_user']}\n"
                    f"Scrobbleando nuevamente."
                )
                send_whatsapp_message(body)

                cur.execute("""
                    UPDATE teams
                    SET last_alert_at = NULL
                    WHERE id = %s
                """, (team["id"],))

            print(f"[OK] {team['name']} | {team['lastfm_user']} | {new_status} | idle={idle_minutes}m")

        except Exception as e:
            cur.execute("""
                UPDATE teams
                SET status = %s,
                    last_check_at = %s
                WHERE id = %s
            """, (
                "INCIDENT",
                now_utc,
                team["id"]
            ))
            print(f"[ERROR] {team['name']} | {team['lastfm_user']} | {e}")

    conn.commit()
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()

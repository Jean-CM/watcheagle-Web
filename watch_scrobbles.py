import os
import requests
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timezone

# Twilio opcional
try:
    from twilio.rest import Client
except Exception:
    Client = None

DATABASE_URL = os.environ.get("DATABASE_URL")
LASTFM_API_KEY = os.environ.get("LASTFM_API_KEY")

WARN_MIN = int(os.environ.get("WARN_MIN", "15"))
INCIDENT_MIN = int(os.environ.get("INCIDENT_MIN", "30"))
ALERT_COOLDOWN_MIN = int(os.environ.get("ALERT_COOLDOWN_MIN", "120"))


def get_conn():
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)


def send_whatsapp_alert(message: str):
    """
    Envía alerta por WhatsApp usando Twilio si está configurado.
    Si no, solo imprime warning y sigue.
    """
    if Client is None:
        print("[WARN] Twilio no está instalado. Alerta no enviada.")
        return

    account_sid = os.environ.get("TWILIO_ACCOUNT_SID")
    auth_token = os.environ.get("TWILIO_AUTH_TOKEN")
    whatsapp_from = os.environ.get("WHATSAPP_FROM")
    whatsapp_to = os.environ.get("WHATSAPP_TO")

    if not all([account_sid, auth_token, whatsapp_from, whatsapp_to]):
        print("[WARN] Faltan variables de entorno de Twilio. Alerta no enviada.")
        return

    try:
        client = Client(account_sid, auth_token)
        client.messages.create(
            body=message,
            from_=f"whatsapp:{whatsapp_from}",
            to=f"whatsapp:{whatsapp_to}"
        )
        print(f"[OK] WhatsApp enviado: {message}")
    except Exception as e:
        print(f"[ERROR] No se pudo enviar WhatsApp: {e}")


def fetch_lastfm_recent_track(username: str):
    """
    Retorna:
      {
        "artist": ...,
        "track": ...,
        "album": ...,
        "scrobbled_at": datetime | None,
        "now_playing": bool
      }
    """
    if not LASTFM_API_KEY:
        raise Exception("LASTFM_API_KEY no está configurada")

    url = "https://ws.audioscrobbler.com/2.0/"
    params = {
        "method": "user.getrecenttracks",
        "user": username,
        "api_key": LASTFM_API_KEY,
        "format": "json",
        "limit": 1
    }

    response = requests.get(url, params=params, timeout=30)
    data = response.json()

    if "error" in data:
        raise Exception(f"Last.fm API error {data.get('error')}: {data.get('message')}")

    recenttracks = data.get("recenttracks", {})
    track_list = recenttracks.get("track", [])

    if isinstance(track_list, dict):
        track_list = [track_list]

    if not track_list:
        return {
            "artist": None,
            "track": None,
            "album": None,
            "scrobbled_at": None,
            "now_playing": False
        }

    tr = track_list[0]

    artist = ((tr.get("artist") or {}).get("#text", "") or "").strip() or None
    track = (tr.get("name", "") or "").strip() or None
    album = ((tr.get("album") or {}).get("#text", "") or "").strip() or None
    now_playing = tr.get("@attr", {}).get("nowplaying") == "true"

    date_info = tr.get("date", {})
    uts = date_info.get("uts")

    scrobbled_at = None
    if uts:
        scrobbled_at = datetime.fromtimestamp(int(uts), tz=timezone.utc)

    return {
        "artist": artist,
        "track": track,
        "album": album,
        "scrobbled_at": scrobbled_at,
        "now_playing": now_playing
    }


def minutes_since(dt: datetime | None):
    if not dt:
        return None
    now = datetime.now(timezone.utc)
    diff = now - dt
    return int(diff.total_seconds() // 60)


def determine_status(idle_minutes: int | None):
    if idle_minutes is None:
        return "PENDING"

    if idle_minutes >= INCIDENT_MIN:
        return "INCIDENT"
    if idle_minutes >= WARN_MIN:
        return "WARN"
    return "OK"


def should_send_alert(current_status: str, previous_status: str | None, last_alert_at):
    """
    Reglas simples:
    - Solo alertar si entra en INCIDENT
    - No repetir si sigue en INCIDENT y no ha pasado cooldown
    """
    if current_status != "INCIDENT":
        return False

    now = datetime.now(timezone.utc)

    if previous_status != "INCIDENT":
        return True

    if last_alert_at is None:
        return True

    diff_min = int((now - last_alert_at).total_seconds() // 60)
    return diff_min >= ALERT_COOLDOWN_MIN


def build_alert_message(team, recent, idle_minutes):
    country = team.get("country_code") or "-"
    app_name = team.get("app_name") or "-"
    artist = recent.get("artist") or "-"
    track = recent.get("track") or "-"
    scrobble_time = recent.get("scrobbled_at")

    scrobble_str = "-"
    if scrobble_time:
        scrobble_str = scrobble_time.strftime("%Y-%m-%d %H:%M:%S UTC")

    return (
        f"🚨 INCIDENCIA WatchEagle\n"
        f"Equipo: {team['name']}\n"
        f"App: {app_name}\n"
        f"Usuario: {team['lastfm_user']}\n"
        f"País: {country}\n"
        f"Min pausado: {idle_minutes}\n"
        f"Último artista: {artist}\n"
        f"Última canción: {track}\n"
        f"Último scrobble: {scrobble_str}"
    )


def main():
    if not DATABASE_URL:
        raise Exception("DATABASE_URL no está configurada")

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            id,
            name,
            app_name,
            lastfm_user,
            country_code,
            status,
            last_alert_at
        FROM teams
        WHERE active = TRUE
        ORDER BY id ASC
    """)
    teams = cur.fetchall()

    checked = 0
    ok_count = 0
    warn_count = 0
    incident_count = 0
    error_count = 0

    for team in teams:
        try:
            recent = fetch_lastfm_recent_track(team["lastfm_user"])
            idle_minutes = minutes_since(recent["scrobbled_at"])
            new_status = determine_status(idle_minutes)

            now_utc = datetime.now(timezone.utc)

            cur.execute("""
                UPDATE teams
                SET
                    last_scrobble_at = %s,
                    last_check_at = %s,
                    idle_minutes = %s,
                    status = %s
                WHERE id = %s
            """, (
                recent["scrobbled_at"],
                now_utc,
                idle_minutes if idle_minutes is not None else 0,
                new_status,
                team["id"]
            ))

            if should_send_alert(new_status, team["status"], team["last_alert_at"]):
                message = build_alert_message(team, recent, idle_minutes)
                send_whatsapp_alert(message)

                cur.execute("""
                    UPDATE teams
                    SET last_alert_at = %s
                    WHERE id = %s
                """, (now_utc, team["id"]))

            conn.commit()

            checked += 1
            if new_status == "OK":
                ok_count += 1
            elif new_status == "WARN":
                warn_count += 1
            elif new_status == "INCIDENT":
                incident_count += 1

            print(
                f"[OK] Check {team['name']} | {team['lastfm_user']} | "
                f"{team.get('country_code') or '-'} | status={new_status} | idle={idle_minutes}"
            )

        except Exception as e:
            conn.rollback()
            error_count += 1
            print(
                f"[ERROR] Check {team['name']} | {team['lastfm_user']} | "
                f"{team.get('country_code') or '-'} | {e}"
            )

    cur.close()
    conn.close()

    print("")
    print("===== RESUMEN WATCHEAGLE =====")
    print(f"Equipos chequeados: {checked}")
    print(f"OK: {ok_count}")
    print(f"WARN: {warn_count}")
    print(f"INCIDENT: {incident_count}")
    print(f"Errores: {error_count}")


if __name__ == "__main__":
    main()

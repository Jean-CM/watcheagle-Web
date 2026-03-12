import os
import requests
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timezone

DATABASE_URL = os.environ.get("DATABASE_URL")
LASTFM_API_KEY = os.environ.get("LASTFM_API_KEY")


def get_conn():
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)


def fetch_recent_tracks(user, limit=20):
    url = "https://ws.audioscrobbler.com/2.0/"
    params = {
        "method": "user.getrecenttracks",
        "user": user,
        "api_key": LASTFM_API_KEY,
        "format": "json",
        "limit": limit
    }

    response = requests.get(url, params=params, timeout=30)
    data = response.json()

    if "error" in data:
        raise Exception(f"Last.fm API error {data.get('error')}: {data.get('message')}")

    recenttracks = data.get("recenttracks", {}).get("track", [])

    if isinstance(recenttracks, dict):
        recenttracks = [recenttracks]

    return recenttracks


def main():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT id, name, app_name, lastfm_user
        FROM teams
        WHERE active = TRUE
        ORDER BY id ASC
    """)
    teams = cur.fetchall()

    inserted = 0

    for team in teams:
        try:
            tracks = fetch_recent_tracks(team["lastfm_user"], limit=20)

            for tr in tracks:
                if tr.get("@attr", {}).get("nowplaying") == "true":
                    continue

                artist = (tr.get("artist") or {}).get("#text", "")
                track = tr.get("name", "")
                album = (tr.get("album") or {}).get("#text", "")
                date_info = tr.get("date", {})
                uts = date_info.get("uts")

                if not uts:
                    continue

                scrobbled_at = datetime.fromtimestamp(int(uts), tz=timezone.utc)

                cur.execute("""
                    INSERT INTO scrobbles (
                        team_id, team_name, lastfm_user, app_name,
                        artist, track, album, scrobbled_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (lastfm_user, artist, track, scrobbled_at) DO NOTHING
                """, (
                    team["id"],
                    team["name"],
                    team["lastfm_user"],
                    team["app_name"],
                    artist,
                    track,
                    album,
                    scrobbled_at
                ))

                if cur.rowcount > 0:
                    inserted += 1

            print(f"[OK] Collector {team['name']} | {team['lastfm_user']}")

        except Exception as e:
            print(f"[ERROR] Collector {team['name']} | {team['lastfm_user']} | {e}")

    conn.commit()
    cur.close()
    conn.close()

    print(f"Total scrobbles insertados: {inserted}")


if __name__ == "__main__":
    main()

import os
import requests
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timezone

DATABASE_URL = os.environ.get("DATABASE_URL")
LASTFM_API_KEY = os.environ.get("LASTFM_API_KEY")


def get_conn():
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)


def fetch_recent_tracks_page(user, page=1, limit=200):
    url = "https://ws.audioscrobbler.com/2.0/"
    params = {
        "method": "user.getrecenttracks",
        "user": user,
        "api_key": LASTFM_API_KEY,
        "format": "json",
        "limit": limit,
        "page": page
    }

    response = requests.get(url, params=params, timeout=45)
    data = response.json()

    if "error" in data:
        raise Exception(f"Last.fm API error {data.get('error')}: {data.get('message')}")

    recenttracks = data.get("recenttracks", {})
    tracks = recenttracks.get("track", [])
    attr = recenttracks.get("@attr", {})

    if isinstance(tracks, dict):
        tracks = [tracks]

    total_pages = int(attr.get("totalPages", 1)) if attr.get("totalPages") else 1

    return tracks, total_pages


def insert_track(cur, team, tr):
    if tr.get("@attr", {}).get("nowplaying") == "true":
        return 0

    artist = ((tr.get("artist") or {}).get("#text", "") or "").strip()
    track = (tr.get("name", "") or "").strip()
    album = ((tr.get("album") or {}).get("#text", "") or "").strip()
    date_info = tr.get("date", {})
    uts = date_info.get("uts")

    if not uts:
        return 0

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
        artist if artist else None,
        track if track else None,
        album if album else None,
        scrobbled_at
    ))

    return 1 if cur.rowcount > 0 else 0


def collect_user_history(cur, team):
    inserted = 0
    page = 1
    total_pages = 1

    while page <= total_pages:
        tracks, total_pages = fetch_recent_tracks_page(team["lastfm_user"], page=page, limit=200)

        if not tracks:
            break

        for tr in tracks:
            inserted += insert_track(cur, team, tr)

        page += 1

    return inserted


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

    total_inserted = 0

    for team in teams:
        try:
            inserted = collect_user_history(cur, team)
            conn.commit()
            total_inserted += inserted
            print(f"[OK] Collector {team['name']} | {team['lastfm_user']} | insertados={inserted}")
        except Exception as e:
            conn.rollback()
            print(f"[ERROR] Collector {team['name']} | {team['lastfm_user']} | {e}")

    cur.close()
    conn.close()

    print(f"Total scrobbles insertados: {total_inserted}")


if __name__ == "__main__":
    main()

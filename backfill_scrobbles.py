import os
import requests
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timezone

DATABASE_URL = os.environ.get("DATABASE_URL")
LASTFM_API_KEY = os.environ.get("LASTFM_API_KEY")
BACKFILL_LIMIT = int(os.environ.get("BACKFILL_LIMIT", "200"))
BACKFILL_MAX_PAGES = int(os.environ.get("BACKFILL_MAX_PAGES", "10"))


def get_conn():
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)


def fetch_recent_tracks_page(user, page=1, limit=300):
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
    uts = ((tr.get("date") or {}).get("uts"))

    if not uts:
        return 0

    scrobbled_at = datetime.fromtimestamp(int(uts), tz=timezone.utc)

    cur.execute("""
        INSERT INTO scrobbles (
            team_id, team_name, lastfm_user, app_name, country_code,
            artist, track, album, scrobbled_at
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (lastfm_user, artist, track, scrobbled_at) DO NOTHING
    """, (
        team["id"], team["name"], team["lastfm_user"], team["app_name"], team["country_code"],
        artist if artist else None,
        track if track else None,
        album if album else None,
        scrobbled_at
    ))

    return 1 if cur.rowcount > 0 else 0


def main():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT t.id, t.name, t.app_name, t.lastfm_user, t.country_code
        FROM teams t
        LEFT JOIN scrobbles s ON s.team_id = t.id
        WHERE t.active = TRUE
        GROUP BY t.id, t.name, t.app_name, t.lastfm_user, t.country_code
        HAVING COUNT(s.id) = 0
        ORDER BY t.id ASC
    """)
    teams = cur.fetchall()

    total_inserted = 0

    for team in teams:
        inserted = 0
        try:
            page = 1
            total_pages = 1

            while page <= total_pages and page <= BACKFILL_MAX_PAGES:
                tracks, total_pages = fetch_recent_tracks_page(team["lastfm_user"], page=page, limit=BACKFILL_LIMIT)
                if not tracks:
                    break

                for tr in tracks:
                    inserted += insert_track(cur, team, tr)

                page += 1

            conn.commit()
            total_inserted += inserted
            print(f"[OK] Nuevo usuario {team['name']} | {team['lastfm_user']} | insertados={inserted}")
        except Exception as e:
            conn.rollback()
            print(f"[ERROR] Nuevo usuario {team['name']} | {team['lastfm_user']} | {e}")

    cur.close()
    conn.close()

    print(f"Total insertado para usuarios nuevos: {total_inserted}")


if __name__ == "__main__":
    main()

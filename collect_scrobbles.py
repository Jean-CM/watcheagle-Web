import os
import requests
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timezone

DATABASE_URL = os.environ.get("DATABASE_URL")
LASTFM_API_KEY = os.environ.get("LASTFM_API_KEY")
COLLECTOR_LIMIT = int(os.environ.get("COLLECTOR_LIMIT", "100"))
COLLECTOR_MAX_PAGES = int(os.environ.get("COLLECTOR_MAX_PAGES", "3"))


def get_conn():
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)


def fetch_recent_tracks_page(user, page=1, limit=100):
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


def get_latest_scrobble_for_user(cur, lastfm_user):
    cur.execute("""
        SELECT MAX(scrobbled_at) AS max_scrobble
        FROM scrobbles
        WHERE lastfm_user = %s
    """, (lastfm_user,))
    row = cur.fetchone()
    return row["max_scrobble"] if row and row["max_scrobble"] else None


def insert_track(cur, team, tr):
    if tr.get("@attr", {}).get("nowplaying") == "true":
        return 0, None

    artist = ((tr.get("artist") or {}).get("#text", "") or "").strip()
    track = (tr.get("name", "") or "").strip()
    album = ((tr.get("album") or {}).get("#text", "") or "").strip()
    date_info = tr.get("date", {})
    uts = date_info.get("uts")

    if not uts:
        return 0, None

    scrobbled_at = datetime.fromtimestamp(int(uts), tz=timezone.utc)

    cur.execute("""
        INSERT INTO scrobbles (
            team_id, team_name, lastfm_user, app_name, country_code,
            artist, track, album, scrobbled_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (lastfm_user, artist, track, scrobbled_at) DO NOTHING
    """, (
        team["id"],
        team["name"],
        team["lastfm_user"],
        team["app_name"],
        team.get("country_code"),
        artist if artist else None,
        track if track else None,
        album if album else None,
        scrobbled_at
    ))

    return (1 if cur.rowcount > 0 else 0), scrobbled_at


def collect_user_incremental(cur, team):
    inserted = 0
    latest_saved = get_latest_scrobble_for_user(cur, team["lastfm_user"])
    if latest_saved and latest_saved.tzinfo is None:
        latest_saved = latest_saved.replace(tzinfo=timezone.utc)

    page = 1
    total_pages = 1
    should_stop = False

    while page <= total_pages and page <= COLLECTOR_MAX_PAGES and not should_stop:
        tracks, total_pages = fetch_recent_tracks_page(
            team["lastfm_user"],
            page=page,
            limit=COLLECTOR_LIMIT
        )

        if not tracks:
            break

        for tr in tracks:
            if tr.get("@attr", {}).get("nowplaying") == "true":
                continue

            date_info = tr.get("date", {})
            uts = date_info.get("uts")
            if not uts:
                continue

            track_dt = datetime.fromtimestamp(int(uts), tz=timezone.utc)

            if latest_saved and track_dt <= latest_saved:
                should_stop = True
                break

            new_insert, _ = insert_track(cur, team, tr)
            inserted += new_insert

        page += 1

    return inserted


def main():
    if not DATABASE_URL:
        raise Exception("DATABASE_URL no está configurada")
    if not LASTFM_API_KEY:
        raise Exception("LASTFM_API_KEY no está configurada")

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT id, name, app_name, lastfm_user, country_code
        FROM teams
        WHERE active = TRUE
        ORDER BY id ASC
    """)
    teams = cur.fetchall()

    total_inserted = 0

    for team in teams:
        try:
            inserted = collect_user_incremental(cur, team)
            conn.commit()
            total_inserted += inserted
            print(f"[OK] Collector {team['name']} | {team['lastfm_user']} | {team.get('country_code') or '-'} | insertados={inserted}")
        except Exception as e:
            conn.rollback()
            print(f"[ERROR] Collector {team['name']} | {team['lastfm_user']} | {team.get('country_code') or '-'} | {e}")

    cur.close()
    conn.close()

    print(f"Total scrobbles insertados: {total_inserted}")


if __name__ == "__main__":
    main()

import os
import requests
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timezone

DATABASE_URL = os.environ.get("DATABASE_URL")
LASTFM_API_KEY = os.environ.get("LASTFM_API_KEY")


def get_conn():
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)


def ensure_schema():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS scrobbles (
        id SERIAL PRIMARY KEY,
        team_id INTEGER REFERENCES teams(id) ON DELETE CASCADE,
        team_name VARCHAR(50) NOT NULL,
        lastfm_user VARCHAR(100) NOT NULL,
        app_name VARCHAR(50) NOT NULL,
        artist_name TEXT NOT NULL,
        track_name TEXT NOT NULL,
        album_name TEXT,
        scrobble_time TIMESTAMP NOT NULL,
        collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)

    cur.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS uq_scrobble_unique
    ON scrobbles(team_id, track_name, artist_name, scrobble_time);
    """)

    conn.commit()
    cur.close()
    conn.close()


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
        return None

    nowplaying = latest.get("@attr", {}).get("nowplaying") == "true"

    # Si está sonando ahora mismo, no lo guardamos aún como scrobble definitivo
    # porque todavía podría no haberse scrobbleado formalmente.
    if nowplaying:
        return None

    artist_name = latest.get("artist", {}).get("#text", "").strip()
    track_name = latest.get("name", "").strip()
    album_name = latest.get("album", {}).get("#text", "").strip()

    date_info = latest.get("date", {})
    uts = date_info.get("uts")

    if not uts:
        return None

    scrobble_time = datetime.fromtimestamp(int(uts), tz=timezone.utc)

    return {
        "artist_name": artist_name,
        "track_name": track_name,
        "album_name": album_name,
        "scrobble_time": scrobble_time
    }


def save_scrobble(team, track):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO scrobbles (
            team_id,
            team_name,
            lastfm_user,
            app_name,
            artist_name,
            track_name,
            album_name,
            scrobble_time
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
        RETURNING id;
    """, (
        team["id"],
        team["name"],
        team["lastfm_user"],
        team["app_name"],
        track["artist_name"],
        track["track_name"],
        track["album_name"],
        track["scrobble_time"]
    ))

    row = cur.fetchone()
    conn.commit()
    cur.close()
    conn.close()

    return row is not None


def main():
    ensure_schema()

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, name, app_name, lastfm_user
        FROM teams
        WHERE active = TRUE
        ORDER BY id ASC
    """)
    teams = cur.fetchall()
    cur.close()
    conn.close()

    inserted = 0
    skipped = 0

    for team in teams:
        try:
            track = get_recent_track(team["lastfm_user"])

            if track is None:
                print(f"[SKIP] {team['name']} | {team['lastfm_user']} | sin scrobble final nuevo")
                skipped += 1
                continue

            was_inserted = save_scrobble(team, track)

            if was_inserted:
                print(
                    f"[INSERT] {team['name']} | {team['lastfm_user']} | "
                    f"{track['artist_name']} - {track['track_name']} | {track['scrobble_time']}"
                )
                inserted += 1
            else:
                print(
                    f"[DUPLICATE] {team['name']} | {team['lastfm_user']} | "
                    f"{track['artist_name']} - {track['track_name']} | {track['scrobble_time']}"
                )
                skipped += 1

        except Exception as e:
            print(f"[ERROR] {team['name']} | {team['lastfm_user']} | {e}")
            skipped += 1

    print(f"\nResumen: inserted={inserted}, skipped={skipped}")


if __name__ == "__main__":
    main()

from helpers import (
    init_db,
    get_active_teams,
    fetch_recent_tracks,
    normalize_tracks_payload,
    insert_scrobble,
    REFRESH_LIMIT,
    REFRESH_MAX_PAGES,
    utc_now,
    start_job,
    finish_job,
)
from datetime import timedelta

JOB_NAME = "run-refresh-24h"


def main():
    init_db()
    job_id = start_job(JOB_NAME, "Refresh últimas 24h iniciado")

    lines = []
    total_inserted = 0
    errors = 0

    cutoff = utc_now().replace(tzinfo=None) - timedelta(hours=24)
    teams = get_active_teams()

    for team in teams:
        try:
            inserted_for_team = 0
            stop_user = False

            for page in range(1, REFRESH_MAX_PAGES + 1):
                data = fetch_recent_tracks(team["lastfm_user"], limit=REFRESH_LIMIT, page=page)

                if "error" in data:
                    lines.append(f"[ERROR] Refresh24h {team['name']} | {team['lastfm_user']} | Last.fm API error {data.get('error')}: {data.get('message')}")
                    errors += 1
                    break

                tracks = normalize_tracks_payload(data)
                if not tracks:
                    break

                for item in tracks:
                    if item["now_playing"]:
                        continue
                    if not item.get("scrobbled_at"):
                        continue
                    if item["scrobbled_at"] < cutoff:
                        stop_user = True
                        break
                    if insert_scrobble(team, item):
                        inserted_for_team += 1
                        total_inserted += 1

                if stop_user:
                    break

            lines.append(f"[OK] Refresh24h {team['name']} | {team['lastfm_user']} | insertados={inserted_for_team}")
        except Exception as e:
            errors += 1
            lines.append(f"[ERROR] Refresh24h {team['name']} | {team['lastfm_user']} | {str(e)}")

    lines.append(f"Total insertado últimas 24 horas: {total_inserted}")

    output = "\n".join(lines)
    print(output)
    finish_job(job_id, "OK" if errors == 0 else "ERROR", output)


if __name__ == "__main__":
    main()

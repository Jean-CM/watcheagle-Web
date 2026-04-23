from helpers import (
    init_db,
    get_active_teams,
    fetch_recent_tracks,
    normalize_tracks_payload,
    insert_scrobble,
    BACKFILL_LIMIT,
    BACKFILL_MAX_PAGES,
    start_job,
    finish_job,
)

JOB_NAME = "run-backfill"


def main():
    init_db()
    job_id = start_job(JOB_NAME, "Backfill histórico iniciado")

    lines = []
    total_inserted = 0
    errors = 0

    teams = get_active_teams()

    for team in teams:
        try:
            inserted_for_team = 0

            for page in range(1, BACKFILL_MAX_PAGES + 1):
                data = fetch_recent_tracks(
                    team["lastfm_user"],
                    limit=BACKFILL_LIMIT,
                    page=page,
                )

                if "error" in data:
                    lines.append(
                        f"[ERROR] Backfill {team['name']} | {team['lastfm_user']} | "
                        f"Last.fm API error {data.get('error')}: {data.get('message')}"
                    )
                    errors += 1
                    break

                tracks = normalize_tracks_payload(data)
                if not tracks:
                    break

                page_inserted = 0

                for item in tracks:
                    if item.get("now_playing"):
                        continue

                    if not item.get("scrobbled_at"):
                        continue

                    if insert_scrobble(team, item):
                        inserted_for_team += 1
                        total_inserted += 1
                        page_inserted += 1

                if page_inserted == 0:
                    break

            lines.append(
                f"[OK] Backfill {team['name']} | {team['lastfm_user']} | insertados={inserted_for_team}"
            )

        except Exception as e:
            errors += 1
            lines.append(
                f"[ERROR] Backfill {team['name']} | {team['lastfm_user']} | {str(e)}"
            )

    lines.append("")
    lines.append(f"Total histórico insertado: {total_inserted}")

    output = "\n".join(lines)
    print(output)
    finish_job(job_id, "OK" if errors == 0 else "ERROR", output)


if __name__ == "__main__":
    main()

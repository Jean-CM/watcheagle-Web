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


def log(line):
    print(line, flush=True)


def main():
    init_db()
    job_id = start_job(JOB_NAME, "Backfill histórico iniciado")

    lines = []
    total_inserted = 0
    errors = 0

    teams = get_active_teams()

    log("===== BACKFILL HISTÓRICO WATCHEAGLE =====")
    log(f"Equipos activos: {len(teams)}")
    log(f"BACKFILL_LIMIT: {BACKFILL_LIMIT}")
    log(f"BACKFILL_MAX_PAGES: {BACKFILL_MAX_PAGES}")
    log("")

    for team_index, team in enumerate(teams, start=1):
        team_name = team["name"]
        lastfm_user = team["lastfm_user"]

        inserted_for_team = 0

        log(f"--------------------------------------------------")
        log(f"[START] {team_index}/{len(teams)} | {team_name} | {lastfm_user}")

        try:
            for page in range(1, BACKFILL_MAX_PAGES + 1):
                log(f"[PAGE] {team_name} | {lastfm_user} | página={page}")

                data = fetch_recent_tracks(
                    lastfm_user,
                    limit=BACKFILL_LIMIT,
                    page=page,
                )

                if "error" in data:
                    msg = (
                        f"[ERROR] Backfill {team_name} | {lastfm_user} | "
                        f"Last.fm API error {data.get('error')}: {data.get('message')}"
                    )
                    log(msg)
                    lines.append(msg)
                    errors += 1
                    break

                tracks = normalize_tracks_payload(data)

                if not tracks:
                    msg = f"[STOP] {team_name} | {lastfm_user} | página={page} sin tracks"
                    log(msg)
                    lines.append(msg)
                    break

                page_inserted = 0
                page_skipped_now_playing = 0
                page_skipped_no_date = 0
                page_duplicates = 0

                for item in tracks:
                    if item.get("now_playing"):
                        page_skipped_now_playing += 1
                        continue

                    if not item.get("scrobbled_at"):
                        page_skipped_no_date += 1
                        continue

                    inserted = insert_scrobble(team, item)

                    if inserted:
                        inserted_for_team += 1
                        total_inserted += 1
                        page_inserted += 1
                    else:
                        page_duplicates += 1

                log(
                    f"[OK PAGE] {team_name} | {lastfm_user} | página={page} | "
                    f"insertados={page_inserted} | duplicados={page_duplicates} | "
                    f"now_playing={page_skipped_now_playing} | sin_fecha={page_skipped_no_date}"
                )

                lines.append(
                    f"[OK PAGE] {team_name} | {lastfm_user} | página={page} | "
                    f"insertados={page_inserted} | duplicados={page_duplicates}"
                )

                # Si una página no insertó nada, probablemente ya llegamos a histórico existente.
                # Esto evita gastar tiempo dando vueltas como trompo.
                if page_inserted == 0:
                    log(
                        f"[STOP] {team_name} | {lastfm_user} | página={page} no insertó nada. "
                        f"Se detiene este usuario."
                    )
                    break

            msg = f"[DONE] {team_name} | {lastfm_user} | total_insertados={inserted_for_team}"
            log(msg)
            lines.append(msg)

        except Exception as e:
            errors += 1
            msg = f"[ERROR] Backfill {team_name} | {lastfm_user} | {str(e)}"
            log(msg)
            lines.append(msg)

    log("")
    log("===== RESUMEN BACKFILL =====")
    log(f"Total histórico insertado: {total_inserted}")
    log(f"Errores: {errors}")

    lines.append("")
    lines.append("===== RESUMEN BACKFILL =====")
    lines.append(f"Total histórico insertado: {total_inserted}")
    lines.append(f"Errores: {errors}")

    output = "\n".join(lines)
    finish_job(job_id, "OK" if errors == 0 else "ERROR", output)


if __name__ == "__main__":
    main()

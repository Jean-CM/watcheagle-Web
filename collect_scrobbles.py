from helpers import (
    init_db,
    get_active_teams,
    fetch_recent_tracks,
    normalize_tracks_payload,
    insert_scrobble,
    get_latest_scrobble_for_user,
    COLLECTOR_LIMIT,
    COLLECTOR_MAX_PAGES,
    start_job,
    finish_job,
)

JOB_NAME = "run-collector"


def log(msg):
    print(msg, flush=True)


def main():
    init_db()
    job_id = start_job(JOB_NAME, "Collector incremental iniciado")

    lines = []
    total_inserted = 0
    errors = 0

    teams = get_active_teams()

    log("===== COLLECTOR INCREMENTAL WATCHEAGLE =====")
    log(f"Equipos activos: {len(teams)}")
    log(f"COLLECTOR_LIMIT: {COLLECTOR_LIMIT}")
    log(f"COLLECTOR_MAX_PAGES: {COLLECTOR_MAX_PAGES}")
    log("")

    for team_index, team in enumerate(teams, start=1):
        team_name = team["name"]
        lastfm_user = team["lastfm_user"]

        log("--------------------------------------------------")
        log(f"[START] {team_index}/{len(teams)} | {team_name} | {lastfm_user}")

        try:
            newest = get_latest_scrobble_for_user(lastfm_user)
            inserted_for_team = 0
            stop_user = False

            for page in range(1, COLLECTOR_MAX_PAGES + 1):
                log(f"[PAGE] {team_name} | {lastfm_user} | página={page}")

                data = fetch_recent_tracks(
                    lastfm_user,
                    limit=COLLECTOR_LIMIT,
                    page=page,
                )

                if "error" in data:
                    msg = (
                        f"[ERROR] Collector {team_name} | {lastfm_user} | "
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
                page_duplicates = 0
                page_old = 0
                page_now_playing = 0
                page_no_date = 0

                for item in tracks:
                    if item.get("now_playing"):
                        page_now_playing += 1
                        continue

                    if not item.get("scrobbled_at"):
                        page_no_date += 1
                        continue

                    # Incremental: si ya llegamos a algo igual o más viejo que lo último guardado, paramos.
                    if newest and item["scrobbled_at"] <= newest:
                        page_old += 1
                        stop_user = True
                        break

                    if insert_scrobble(team, item):
                        page_inserted += 1
                        inserted_for_team += 1
                        total_inserted += 1
                    else:
                        page_duplicates += 1

                msg = (
                    f"[OK PAGE] {team_name} | {lastfm_user} | página={page} | "
                    f"insertados={page_inserted} | duplicados={page_duplicates} | "
                    f"viejos={page_old} | now_playing={page_now_playing} | sin_fecha={page_no_date}"
                )
                log(msg)
                lines.append(msg)

                if stop_user:
                    log(f"[STOP] {team_name} | {lastfm_user} | llegó al último scrobble guardado.")
                    break

                if len(tracks) < COLLECTOR_LIMIT:
                    log(
                        f"[STOP] {team_name} | {lastfm_user} | página={page} "
                        f"trajo {len(tracks)} tracks, menor que COLLECTOR_LIMIT."
                    )
                    break

            done = f"[DONE] Collector {team_name} | {lastfm_user} | insertados={inserted_for_team}"
            log(done)
            lines.append(done)

        except Exception as e:
            errors += 1
            msg = f"[ERROR] Collector {team_name} | {lastfm_user} | {str(e)}"
            log(msg)
            lines.append(msg)

    log("")
    log("===== RESUMEN COLLECTOR =====")
    log(f"Total scrobbles insertados: {total_inserted}")
    log(f"Errores: {errors}")

    lines.append("")
    lines.append("===== RESUMEN COLLECTOR =====")
    lines.append(f"Total scrobbles insertados: {total_inserted}")
    lines.append(f"Errores: {errors}")

    finish_job(job_id, "OK" if errors == 0 else "ERROR", "\n".join(lines))


if __name__ == "__main__":
    main()

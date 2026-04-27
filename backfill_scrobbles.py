import os

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

JOB_NAME = "run-backfill-full-selected"


def log(msg):
    print(msg, flush=True)


def filter_teams_by_env(teams):
    raw_ids = os.environ.get("TEAM_IDS", "").strip()

    if not raw_ids:
        return teams

    ids = set()
    for x in raw_ids.split(","):
        x = x.strip()
        if x.isdigit():
            ids.add(int(x))

    return [t for t in teams if int(t["id"]) in ids]


def main():
    init_db()
    job_id = start_job(JOB_NAME, "Backfill histórico completo iniciado")

    lines = []
    total_inserted = 0
    total_duplicates = 0
    errors = 0

    teams = get_active_teams()
    teams = filter_teams_by_env(teams)

    log("===== BACKFILL HISTÓRICO FULL WATCHEAGLE =====")
    log(f"Equipos seleccionados: {len(teams)}")
    log(f"BACKFILL_LIMIT: {BACKFILL_LIMIT}")
    log(f"BACKFILL_MAX_PAGES: {BACKFILL_MAX_PAGES}")
    log("Modo: FULL, no se detiene por duplicados")
    log("")

    for team_index, team in enumerate(teams, start=1):
        team_name = team["name"]
        lastfm_user = team["lastfm_user"]

        inserted_for_team = 0
        duplicates_for_team = 0

        log("--------------------------------------------------")
        log(f"[START] {team_index}/{len(teams)} | {team_name} | {lastfm_user}")

        try:
            for page in range(1, BACKFILL_MAX_PAGES + 1):
                log(f"[PAGE] {team_name} | {lastfm_user} | página={page}")

                data = fetch_recent_tracks(lastfm_user, limit=BACKFILL_LIMIT, page=page)

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
                page_duplicates = 0
                page_now_playing = 0
                page_no_date = 0

                for item in tracks:
                    if item.get("now_playing"):
                        page_now_playing += 1
                        continue

                    if not item.get("scrobbled_at"):
                        page_no_date += 1
                        continue

                    if insert_scrobble(team, item):
                        page_inserted += 1
                        inserted_for_team += 1
                        total_inserted += 1
                    else:
                        page_duplicates += 1
                        duplicates_for_team += 1
                        total_duplicates += 1

                msg = (
                    f"[OK PAGE] {team_name} | {lastfm_user} | página={page} | "
                    f"insertados={page_inserted} | duplicados={page_duplicates} | "
                    f"now_playing={page_now_playing} | sin_fecha={page_no_date}"
                )
                log(msg)
                lines.append(msg)

                if len(tracks) < BACKFILL_LIMIT:
                    log(f"[STOP] {team_name} | fin del histórico disponible.")
                    break

            done = (
                f"[DONE] {team_name} | {lastfm_user} | "
                f"insertados={inserted_for_team} | duplicados={duplicates_for_team}"
            )
            log(done)
            lines.append(done)

        except Exception as e:
            errors += 1
            msg = f"[ERROR] Backfill {team_name} | {lastfm_user} | {str(e)}"
            log(msg)
            lines.append(msg)

    log("")
    log("===== RESUMEN BACKFILL FULL =====")
    log(f"Total histórico insertado: {total_inserted}")
    log(f"Total duplicados: {total_duplicates}")
    log(f"Errores: {errors}")

    lines.append("")
    lines.append("===== RESUMEN BACKFILL FULL =====")
    lines.append(f"Total histórico insertado: {total_inserted}")
    lines.append(f"Total duplicados: {total_duplicates}")
    lines.append(f"Errores: {errors}")

    finish_job(job_id, "OK" if errors == 0 else "ERROR", "\n".join(lines))


if __name__ == "__main__":
    main()

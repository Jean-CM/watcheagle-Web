import os

from config import MONITOR_PLAYS_ARTISTS
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
ALLOWED_ARTISTS = {a.strip().lower() for a in MONITOR_PLAYS_ARTISTS if a and a.strip()}


def log(msg):
    print(msg, flush=True)


def is_allowed_artist(name):
    return (name or '').strip().lower() in ALLOWED_ARTISTS


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


def lastfm_attr(data):
    return data.get("recenttracks", {}).get("@attr", {}) if isinstance(data, dict) else {}


def safe_int_local(value, default=0):
    try:
        return int(value)
    except Exception:
        return default


def get_total_pages_from_payload(data):
    attr = lastfm_attr(data)
    return safe_int_local(attr.get("totalPages"), 0)


def get_total_scrobbles_from_payload(data):
    attr = lastfm_attr(data)
    return safe_int_local(attr.get("total"), 0)


def should_stop_by_page_cap(page, total_pages):
    """
    Regla nueva:
    - Si Last.fm informa totalPages, collect-all llega hasta ese total real.
    - BACKFILL_MAX_PAGES queda como límite de seguridad SOLO si FORCE_BACKFILL_PAGE_CAP=true.
    - Esto evita que un equipo con 80 páginas se quede cortado en 50.
    """
    force_cap = os.environ.get("FORCE_BACKFILL_PAGE_CAP", "false").strip().lower() in {"1", "true", "yes", "y"}
    if force_cap and BACKFILL_MAX_PAGES > 0 and page >= BACKFILL_MAX_PAGES:
        return True, f"[STOP] Límite de seguridad FORCE_BACKFILL_PAGE_CAP alcanzado: {BACKFILL_MAX_PAGES} páginas"
    if total_pages > 0 and page >= total_pages:
        return True, f"[STOP] Fin real Last.fm alcanzado: página {page}/{total_pages}"
    return False, ""


def main():
    init_db()
    job_id = start_job(JOB_NAME, "Backfill histórico completo iniciado")

    lines = []
    total_inserted = 0
    total_duplicates = 0
    total_skipped_artists = 0
    errors = 0

    teams = get_active_teams()
    teams = filter_teams_by_env(teams)

    log("===== BACKFILL HISTÓRICO FULL WATCHEAGLE =====")
    log(f"Equipos seleccionados: {len(teams)}")
    log(f"Artistas permitidos: {len(ALLOWED_ARTISTS)}")
    log(f"BACKFILL_LIMIT: {BACKFILL_LIMIT}")
    log(f"BACKFILL_MAX_PAGES configurado: {BACKFILL_MAX_PAGES}")
    log(f"FORCE_BACKFILL_PAGE_CAP: {os.environ.get('FORCE_BACKFILL_PAGE_CAP', 'false')}")
    log("Modo: FULL dinámico, usa totalPages real de Last.fm")
    log("")

    for team_index, team in enumerate(teams, start=1):
        team_name = team["name"]
        lastfm_user = team["lastfm_user"]
        inserted_for_team = 0
        duplicates_for_team = 0
        skipped_for_team = 0
        total_pages = 0
        total_lastfm = 0
        page = 1

        log("--------------------------------------------------")
        log(f"[START] {team_index}/{len(teams)} | {team_name} | {lastfm_user}")

        try:
            while True:
                log(f"[PAGE] {team_name} | {lastfm_user} | página={page}" + (f"/{total_pages}" if total_pages else ""))

                data = fetch_recent_tracks(lastfm_user, limit=BACKFILL_LIMIT, page=page)

                if "error" in data:
                    msg = f"[ERROR] Backfill {team_name} | {lastfm_user} | Last.fm API error {data.get('error')}: {data.get('message')}"
                    log(msg)
                    lines.append(msg)
                    errors += 1
                    break

                if page == 1:
                    total_pages = get_total_pages_from_payload(data)
                    total_lastfm = get_total_scrobbles_from_payload(data)
                    msg = f"[LASTFM META] {team_name} | {lastfm_user} | total_scrobbles={total_lastfm:,} | totalPages={total_pages:,} | limit={BACKFILL_LIMIT}"
                    log(msg)
                    lines.append(msg)

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
                page_skipped_artists = 0

                for item in tracks:
                    if item.get("now_playing"):
                        page_now_playing += 1
                        continue

                    if not item.get("scrobbled_at"):
                        page_no_date += 1
                        continue

                    if not is_allowed_artist(item.get("artist_name")):
                        page_skipped_artists += 1
                        skipped_for_team += 1
                        total_skipped_artists += 1
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
                    f"[OK PAGE] {team_name} | {lastfm_user} | página={page}" +
                    (f"/{total_pages}" if total_pages else "") +
                    f" | insertados={page_inserted} | duplicados={page_duplicates} | "
                    f"externos_omitidos={page_skipped_artists} | now_playing={page_now_playing} | sin_fecha={page_no_date}"
                )
                log(msg)
                lines.append(msg)

                stop, stop_msg = should_stop_by_page_cap(page, total_pages)
                if stop:
                    log(stop_msg)
                    lines.append(f"{team_name} | {lastfm_user} | {stop_msg}")
                    break

                if len(tracks) < BACKFILL_LIMIT:
                    msg = f"[STOP] {team_name} | fin del histórico disponible por página incompleta. tracks={len(tracks)} limit={BACKFILL_LIMIT}"
                    log(msg)
                    lines.append(msg)
                    break

                page += 1

            done = (
                f"[DONE] {team_name} | {lastfm_user} | pages_read={page}" +
                (f"/{total_pages}" if total_pages else "") +
                f" | lastfm_total={total_lastfm:,} | insertados={inserted_for_team} | "
                f"duplicados={duplicates_for_team} | externos_omitidos={skipped_for_team}"
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
    log(f"Total artistas externos omitidos: {total_skipped_artists}")
    log(f"Errores: {errors}")

    lines.append("")
    lines.append("===== RESUMEN BACKFILL FULL =====")
    lines.append(f"Total histórico insertado: {total_inserted}")
    lines.append(f"Total duplicados: {total_duplicates}")
    lines.append(f"Total artistas externos omitidos: {total_skipped_artists}")
    lines.append(f"Errores: {errors}")

    finish_job(job_id, "OK" if errors == 0 else "ERROR", "\n".join(lines))


if __name__ == "__main__":
    main()

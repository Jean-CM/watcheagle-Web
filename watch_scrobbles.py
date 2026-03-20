from helpers import (
    init_db,
    get_active_teams,
    fetch_recent_tracks,
    normalize_tracks_payload,
    update_team_status,
    get_status_from_idle,
    utc_now,
    start_job,
    finish_job,
)
from datetime import timezone

JOB_NAME = "run-check"


def main():
    init_db()
    job_id = start_job(JOB_NAME, "Chequeo iniciado")

    lines = []
    ok_count = 0
    warn_count = 0
    incident_count = 0
    errors = 0

    now_utc = utc_now()

    teams = get_active_teams()

    for team in teams:
        try:
            data = fetch_recent_tracks(team["lastfm_user"], limit=1, page=1)
            if "error" in data:
                errors += 1
                lines.append(f"[ERROR] Check {team['name']} | {team['lastfm_user']} | Last.fm API error {data.get('error')}: {data.get('message')}")
                update_team_status(team["id"], None, 0, "PENDING")
                continue

            tracks = normalize_tracks_payload(data)
            valid_tracks = [t for t in tracks if t.get("scrobbled_at")]

            if not valid_tracks:
                update_team_status(team["id"], None, 0, "PENDING")
                lines.append(f"[OK] Check {team['name']} | {team['lastfm_user']} | {team.get('country_code') or '-'} | status=PENDING | idle=None")
                continue

            latest = max(valid_tracks, key=lambda x: x["scrobbled_at"])
            last_dt_aware = latest["scrobbled_at"].replace(tzinfo=timezone.utc)
            idle_minutes = int((now_utc - last_dt_aware).total_seconds() // 60)
            status = get_status_from_idle(idle_minutes)

            update_team_status(team["id"], latest["scrobbled_at"], idle_minutes, status)

            if status == "OK":
                ok_count += 1
            elif status == "WARN":
                warn_count += 1
            elif status == "INCIDENT":
                incident_count += 1

            lines.append(
                f"[OK] Check {team['name']} | {team['lastfm_user']} | {team.get('country_code') or '-'} | status={status} | idle={idle_minutes}"
            )
        except Exception as e:
            errors += 1
            lines.append(f"[ERROR] Check {team['name']} | {team['lastfm_user']} | {str(e)}")

    lines.append("")
    lines.append("===== RESUMEN WATCHEAGLE =====")
    lines.append(f"Equipos chequeados: {len(teams)}")
    lines.append(f"OK: {ok_count}")
    lines.append(f"WARN: {warn_count}")
    lines.append(f"INCIDENT: {incident_count}")
    lines.append(f"Errores: {errors}")

    output = "\n".join(lines)
    print(output)
    finish_job(job_id, "OK" if errors == 0 else "ERROR", output)


if __name__ == "__main__":
    main()

from datetime import datetime
from helpers import get_conn, start_job, finish_job

def main():
    job_id = start_job("monthly-alerts", "inicio")

    today = datetime.now()
    day = today.day

    if day not in [20, 25]:
        return

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT artist_name, track_name, COUNT(*) AS plays
        FROM scrobbles
        WHERE DATE_TRUNC('month', scrobble_time) = DATE_TRUNC('month', CURRENT_DATE)
        GROUP BY artist_name, track_name
        HAVING COUNT(*) < 1000
        ORDER BY plays DESC
        LIMIT 10
    """)

    rows = cur.fetchall()

    print("ALERTA DE CIERRE DE MES")
    for r in rows:
        print(f"{r['artist_name']} - {r['track_name']} => {r['plays']} plays")

    finish_job(job_id, "OK", "alerta enviada")

if __name__ == "__main__":
    main()

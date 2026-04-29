from datetime import datetime

def month_range(month):
    if month:
        start = datetime.strptime(month, "%Y-%m")
    else:
        now = datetime.utcnow()
        start = datetime(now.year, now.month, 1)

    if start.month == 12:
        end = datetime(start.year + 1, 1, 1)
    else:
        end = datetime(start.year, start.month + 1, 1)

    return start, end


def build_month_where(month, platform, distributor, alias="s"):
    start, end = month_range(month)

    clauses = [
        f"{alias}.scrobble_time >= %s",
        f"{alias}.scrobble_time < %s",
    ]
    params = [start, end]

    if platform:
        clauses.append(f"LOWER({alias}.app_name) = %s")
        params.append(platform)

    if distributor:
        clauses.append(f"""
            EXISTS (
                SELECT 1
                FROM artist_metadata am
                WHERE LOWER(am.artist_name) = LOWER({alias}.artist_name)
                AND am.distributor = %s
            )
        """)
        params.append(distributor)

    return " AND ".join(clauses), params


def create_performance_indexes(cur):
    statements = [
        """CREATE INDEX IF NOT EXISTS idx_scrobbles_time ON scrobbles (scrobble_time)""",
        """CREATE INDEX IF NOT EXISTS idx_scrobbles_app_time ON scrobbles (LOWER(app_name), scrobble_time)""",
        """CREATE INDEX IF NOT EXISTS idx_scrobbles_artist_lower ON scrobbles (LOWER(artist_name))""",
        """CREATE INDEX IF NOT EXISTS idx_scrobbles_user_time ON scrobbles (lastfm_user, scrobble_time)""",
        """CREATE INDEX IF NOT EXISTS idx_scrobbles_artist_track_time ON scrobbles (artist_name, track_name, scrobble_time)""",
        """CREATE INDEX IF NOT EXISTS idx_artist_metadata_artist_lower ON artist_metadata (LOWER(artist_name))""",
        """CREATE INDEX IF NOT EXISTS idx_artist_metadata_distributor ON artist_metadata (distributor)""",
        """CREATE INDEX IF NOT EXISTS idx_teams_app ON teams (LOWER(app_name))""",
    ]

    for sql in statements:
        cur.execute(sql)

    return len(statements)

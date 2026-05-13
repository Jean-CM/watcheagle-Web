from flask import jsonify
from helpers import get_conn
from config import ARTIST_METADATA


def register_init_routes(app):

    # Registra rutas extra de Spotify Mapping sin tocar app_modular.py.
    # Se hace aquí porque routes_init ya se carga siempre en el arranque.
    try:
        from layout import base_page
        from routes_spotify_mapping import register_spotify_mapping_routes
        register_spotify_mapping_routes(app, get_conn, base_page)
    except Exception as e:
        print(f"[WARN] spotify mapping routes not registered: {e}")

    @app.route('/healthz')
    def healthz():
        try:
            conn = get_conn()
            cur = conn.cursor()
            cur.execute('SELECT COUNT(*) total FROM teams')
            r = cur.fetchone()
            cur.close()
            conn.close()
            return jsonify({'ok': True, 'teams': r['total']})
        except Exception as e:
            return jsonify({'ok': False, 'error': str(e)}), 500

    @app.route('/scrobbles-count')
    def scrobbles_count():
        conn = get_conn()
        cur = conn.cursor()
        cur.execute('SELECT COUNT(*) total FROM scrobbles')
        r = cur.fetchone()
        cur.close()
        conn.close()
        return jsonify({'ok': True, 'total': r['total']})

    @app.route('/init-artist-metadata')
    def init_artist_metadata():
        conn = get_conn()
        cur = conn.cursor()

        cur.execute('''
            CREATE TABLE IF NOT EXISTS artist_metadata (
                id SERIAL PRIMARY KEY,
                artist_name TEXT UNIQUE NOT NULL,
                author TEXT,
                distributor TEXT
            )
        ''')

        for artist, author, distributor in ARTIST_METADATA:
            cur.execute('''
                INSERT INTO artist_metadata (
                    artist_name,
                    author,
                    distributor
                ) VALUES (%s, %s, %s)
                ON CONFLICT (artist_name)
                DO UPDATE SET
                    author = EXCLUDED.author,
                    distributor = EXCLUDED.distributor
            ''', (artist, author, distributor))

        conn.commit()
        cur.close()
        conn.close()

        return jsonify({
            'ok': True,
            'inserted_or_updated': len(ARTIST_METADATA)
        })

    @app.route('/init-performance-indexes')
    def init_performance_indexes():
        conn = get_conn()
        cur = conn.cursor()

        statements = [
            'CREATE INDEX IF NOT EXISTS idx_scrobbles_time ON scrobbles (scrobble_time)',
            'CREATE INDEX IF NOT EXISTS idx_scrobbles_user ON scrobbles (lastfm_user)',
            'CREATE INDEX IF NOT EXISTS idx_scrobbles_user_time ON scrobbles (lastfm_user, scrobble_time DESC)',
            'CREATE INDEX IF NOT EXISTS idx_scrobbles_app_time ON scrobbles (LOWER(app_name), scrobble_time DESC)',
            'CREATE INDEX IF NOT EXISTS idx_scrobbles_artist_lower ON scrobbles (LOWER(artist_name))',
            'CREATE INDEX IF NOT EXISTS idx_scrobbles_artist_track_time ON scrobbles (artist_name, track_name, scrobble_time DESC)',
            'CREATE INDEX IF NOT EXISTS idx_teams_active ON teams (active)',
            'CREATE INDEX IF NOT EXISTS idx_teams_lastfm_user ON teams (lastfm_user)',
            'CREATE INDEX IF NOT EXISTS idx_teams_app ON teams (LOWER(app_name))',
            'CREATE INDEX IF NOT EXISTS idx_artist_metadata_artist_lower ON artist_metadata (LOWER(artist_name))',
            'CREATE INDEX IF NOT EXISTS idx_artist_metadata_distributor ON artist_metadata (distributor)',
            'CREATE INDEX IF NOT EXISTS idx_lastfm_history_status_status ON lastfm_history_status (history_status)',
            'CREATE INDEX IF NOT EXISTS idx_lastfm_history_status_team ON lastfm_history_status (team_id)'
        ]

        created = []
        skipped = []

        for sql in statements:
            try:
                cur.execute(sql)
                created.append(sql)
            except Exception as e:
                conn.rollback()
                skipped.append({'sql': sql, 'error': str(e)})

        conn.commit()
        cur.close()
        conn.close()

        return jsonify({
            'ok': True,
            'attempted': len(statements),
            'created_or_exists': len(created),
            'skipped': skipped
        })

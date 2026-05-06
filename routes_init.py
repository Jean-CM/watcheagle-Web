from flask import jsonify
from helpers import get_conn
from config import ARTIST_METADATA


def register_init_routes(app):

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

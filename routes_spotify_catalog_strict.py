import requests
from flask import request

from layout import badge
from utils import safe_int
from routes_spotify import token, headers, candidates, playlist_name, playlist_desc
from routes_spotify_mapping import ensure_spotify_map_table

API = 'https://api.spotify.com/v1'


def norm(v):
    return (v or '').strip().lower()


def ensure_playlist_runs(cur):
    cur.execute('''
        CREATE TABLE IF NOT EXISTS spotify_playlist_runs (
            id SERIAL PRIMARY KEY,
            playlist_name TEXT,
            playlist_url TEXT,
            strategy TEXT,
            tracks_found INTEGER DEFAULT 0,
            tracks_added INTEGER DEFAULT 0,
            tracks_not_found INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT NOW()
        )
    ''')


def get_catalog_uri(cur, artist, track):
    ensure_spotify_map_table(cur)
    cur.execute('''
        SELECT spotify_uri, spotify_title, spotify_artist
        FROM spotify_track_map
        WHERE LOWER(artist_name)=LOWER(%s)
          AND LOWER(track_name)=LOWER(%s)
          AND source='catalog'
        LIMIT 1
    ''', (artist, track))
    return cur.fetchone()


def add_tracks_strict(tok, playlist_id, items):
    added = []
    failed = []
    for item in items:
        uri = item['uri']
        rr = requests.post(
            f'{API}/playlists/{playlist_id}/items',
            headers=headers(tok),
            json={'uris': [uri]},
            timeout=30,
        )
        if rr.status_code >= 400:
            failed.append({**item, 'error': f'{rr.status_code} {rr.text[:180]}'})
        else:
            added.append(item)
    return added, failed


def create_catalog_playlist(cur, strategy='under900', limit=50):
    tok = token(cur)
    rows = candidates(cur, strategy, limit=limit)
    if not rows:
        raise Exception(f'No hay canciones candidatas para {strategy}.')

    selected = []
    not_in_catalog = []
    seen_uri = set()
    seen_key = set()

    for x in rows:
        artist = x['artist_name']
        track = x['track_name']
        key = (norm(artist), norm(track))
        if key in seen_key:
            continue
        seen_key.add(key)

        found = get_catalog_uri(cur, artist, track)
        if found and found.get('spotify_uri'):
            uri = found['spotify_uri']
            if uri not in seen_uri:
                seen_uri.add(uri)
                selected.append({
                    'artist_name': artist,
                    'track_name': track,
                    'plays': safe_int(x.get('plays')),
                    'uri': uri,
                    'spotify_title': found.get('spotify_title') or track,
                    'spotify_artist': found.get('spotify_artist') or artist,
                })
        else:
            not_in_catalog.append(x)

    if not selected:
        raise Exception('No encontré canciones exactas en el catálogo importado. No se creó playlist.')

    name = playlist_name(strategy)
    r = requests.post(
        f'{API}/me/playlists',
        headers=headers(tok),
        json={'name': name, 'description': playlist_desc(strategy), 'public': False},
        timeout=20,
    )
    if r.status_code >= 400:
        raise Exception(f'Error creando playlist: {r.status_code} {r.text[:400]}')

    playlist = r.json()
    added, failed = add_tracks_strict(tok, playlist['id'], selected)
    url = playlist.get('external_urls', {}).get('spotify', '')

    ensure_playlist_runs(cur)
    cur.execute('''
        INSERT INTO spotify_playlist_runs (playlist_name, playlist_url, strategy, tracks_found, tracks_added, tracks_not_found)
        VALUES (%s,%s,%s,%s,%s,%s)
    ''', (name, url, strategy + '_catalog_strict', len(selected), len(added), len(not_in_catalog) + len(failed)))

    return {
        'name': name,
        'url': url,
        'candidates': len(rows),
        'selected': selected,
        'added': added,
        'failed': failed,
        'not_in_catalog': not_in_catalog,
    }


def table_rows(items, mode='catalog'):
    if not items:
        return '<tr><td colspan="4" class="muted">Sin registros.</td></tr>'
    rows = ''
    for x in items:
        if mode == 'added':
            rows += f'<tr><td>{x.get("artist_name")}</td><td>{x.get("track_name")}</td><td>{x.get("spotify_artist")}</td><td>{x.get("spotify_title")}</td></tr>'
        elif mode == 'failed':
            rows += f'<tr><td>{x.get("artist_name")}</td><td>{x.get("track_name")}</td><td>{x.get("uri")}</td><td>{x.get("error")}</td></tr>'
        else:
            rows += f'<tr><td>{x.get("artist_name")}</td><td>{x.get("track_name")}</td><td>{safe_int(x.get("plays"))}</td><td>No existe en catálogo importado</td></tr>'
    return rows


def register_spotify_catalog_strict_routes(app, get_conn, base_page):
    @app.route('/spotify/create-playlist-catalog')
    def spotify_create_playlist_catalog():
        conn = get_conn()
        cur = conn.cursor()
        try:
            strategy = request.args.get('strategy') or 'under900'
            limit = min(max(safe_int(request.args.get('limit'), 50), 1), 300)
            res = create_catalog_playlist(cur, strategy, limit)
            conn.commit()

            body = f'''
            <div class="card" style="margin-bottom:18px;">
                <div class="section-title">Playlist creada desde catálogo estricto</div>
                <div class="mini-row"><span>Nombre</span><strong>{res['name']}</strong></div>
                <div class="mini-row"><span>Candidatas revisadas</span><strong>{res['candidates']}</strong></div>
                <div class="mini-row"><span>Encontradas exactas en catálogo</span><strong>{len(res['selected'])}</strong></div>
                <div class="mini-row"><span>Agregadas a Spotify</span><strong class="green">{len(res['added'])}</strong></div>
                <div class="mini-row"><span>No agregadas</span><strong class="yellow">{len(res['not_in_catalog']) + len(res['failed'])}</strong></div>
                <div style="margin-top:14px;"><a class="btn btn-primary" href="{res['url']}" target="_blank">Abrir en Spotify</a></div>
            </div>
            <div class="card" style="margin-bottom:18px;">
                <div class="section-title">Agregadas</div>
                <table><thead><tr><th>Artista DB</th><th>Canción DB</th><th>Artista Spotify</th><th>Canción Spotify</th></tr></thead><tbody>{table_rows(res['added'], 'added')}</tbody></table>
            </div>
            <div class="card" style="margin-bottom:18px;">
                <div class="section-title">No agregadas: no existen en catálogo importado</div>
                <table><thead><tr><th>Artista</th><th>Canción</th><th>Plays</th><th>Motivo</th></tr></thead><tbody>{table_rows(res['not_in_catalog'], 'not_catalog')}</tbody></table>
            </div>
            <div class="card">
                <div class="section-title">No agregadas: Spotify rechazó el URI</div>
                <table><thead><tr><th>Artista</th><th>Canción</th><th>URI</th><th>Error</th></tr></thead><tbody>{table_rows(res['failed'], 'failed')}</tbody></table>
            </div>
            '''
            return base_page('Playlist catálogo estricto', 'spotify', body).replace('__LOAD_TIME__','0.00s').replace('__CACHE_STATUS__','No cache')
        except Exception as e:
            conn.rollback()
            body = f'<div class="card"><div class="section-title">Error catálogo estricto</div><pre style="white-space:pre-wrap">{str(e)}</pre><a class="btn btn-primary" href="/spotify/catalog">Volver al catálogo</a></div>'
            return base_page('Error catálogo estricto', 'spotify', body).replace('__LOAD_TIME__','0.00s').replace('__CACHE_STATUS__','No cache'), 500
        finally:
            cur.close()
            conn.close()

from datetime import datetime
from urllib.parse import quote_plus

import requests
from flask import request

from layout import badge
from utils import safe_int
from routes_spotify import token, headers, candidates, playlist_name, playlist_desc, add_tracks

API = 'https://api.spotify.com/v1'


def norm(v):
    return (v or '').strip().lower()


def ensure_spotify_map_table(cur):
    cur.execute('''
        CREATE TABLE IF NOT EXISTS spotify_track_map (
            id SERIAL PRIMARY KEY,
            artist_name TEXT NOT NULL,
            track_name TEXT NOT NULL,
            spotify_uri TEXT NOT NULL,
            spotify_title TEXT,
            spotify_artist TEXT,
            source TEXT DEFAULT 'auto',
            updated_at TIMESTAMP DEFAULT NOW()
        )
    ''')
    cur.execute('''
        CREATE UNIQUE INDEX IF NOT EXISTS uq_spotify_track_map_key
        ON spotify_track_map (LOWER(artist_name), LOWER(track_name))
    ''')


def search_spotify_track(tok, artist, track, limit=5):
    queries = [
        f'track:"{track}" artist:"{artist}"',
        f'{artist} {track}',
        f'{track} {artist}',
        track,
    ]
    for q in queries:
        r = requests.get(
            f'{API}/search',
            headers=headers(tok),
            params={'q': q, 'type': 'track', 'limit': limit},
            timeout=15,
        )
        if r.status_code >= 400:
            continue
        items = r.json().get('tracks', {}).get('items', [])
        if items:
            item = items[0]
            return {
                'query': q,
                'uri': item.get('uri'),
                'title': item.get('name'),
                'artist': ', '.join([a.get('name', '') for a in item.get('artists', [])]),
                'url': item.get('external_urls', {}).get('spotify', '#'),
            }
    return None


def get_mapped_uri(cur, artist, track):
    ensure_spotify_map_table(cur)
    cur.execute('''
        SELECT spotify_uri
        FROM spotify_track_map
        WHERE LOWER(artist_name)=LOWER(%s)
          AND LOWER(track_name)=LOWER(%s)
        LIMIT 1
    ''', (artist, track))
    row = cur.fetchone()
    return row['spotify_uri'] if row else None


def save_map(cur, artist, track, uri, title=None, sp_artist=None, source='auto'):
    ensure_spotify_map_table(cur)
    cur.execute('''
        INSERT INTO spotify_track_map (artist_name, track_name, spotify_uri, spotify_title, spotify_artist, source, updated_at)
        VALUES (%s,%s,%s,%s,%s,%s,NOW())
        ON CONFLICT (LOWER(artist_name), LOWER(track_name)) DO UPDATE SET
            spotify_uri=EXCLUDED.spotify_uri,
            spotify_title=EXCLUDED.spotify_title,
            spotify_artist=EXCLUDED.spotify_artist,
            source=EXCLUDED.source,
            updated_at=NOW()
    ''', (artist, track, uri, title, sp_artist, source))


def create_playlist_with_mapping(cur, strategy='under900', limit=25):
    tok = token(cur)
    rows = candidates(cur, strategy, limit=limit)
    if not rows:
        raise Exception(f'No hay canciones candidatas para {strategy}.')

    uris = []
    not_found = []
    seen = set()

    for x in rows:
        artist = x['artist_name']
        track = x['track_name']
        key = (norm(artist), norm(track))
        if key in seen:
            continue
        seen.add(key)

        uri = get_mapped_uri(cur, artist, track)
        if not uri:
            found = search_spotify_track(tok, artist, track, limit=5)
            if found and found.get('uri'):
                uri = found['uri']
                save_map(cur, artist, track, uri, found.get('title'), found.get('artist'), 'auto')

        if uri and uri not in uris:
            uris.append(uri)
        else:
            not_found.append(x)

    if not uris:
        raise Exception('No encontré ninguna canción agregable. Usa Diagnóstico búsqueda para revisar nombres y guardar URI manualmente.')

    name = playlist_name(strategy)
    r = requests.post(
        f'{API}/me/playlists',
        headers=headers(tok),
        json={'name': name, 'description': playlist_desc(strategy), 'public': False},
        timeout=20,
    )
    if r.status_code >= 400:
        raise Exception(f'Error creando playlist: {r.status_code} {r.text[:500]}')

    playlist = r.json()
    added = add_tracks(tok, playlist['id'], uris)
    url = playlist.get('external_urls', {}).get('spotify', '')

    cur.execute('''
        INSERT INTO spotify_playlist_runs (playlist_name, playlist_url, strategy, tracks_found, tracks_added, tracks_not_found)
        VALUES (%s,%s,%s,%s,%s,%s)
    ''', (name, url, strategy + '_mapped', len(uris), added, len(not_found)))

    return {
        'name': name,
        'url': url,
        'candidates': len(rows),
        'found': len(uris),
        'added': added,
        'not_found': not_found[:50],
    }


def register_spotify_mapping_routes(app, get_conn, base_page):

    @app.route('/spotify/debug-search')
    def spotify_debug_search():
        conn = get_conn()
        cur = conn.cursor()
        try:
            ensure_spotify_map_table(cur)
            strategy = request.args.get('strategy') or 'under900'
            limit = min(max(safe_int(request.args.get('limit'), 10), 1), 50)
            tok = token(cur)
            rows = candidates(cur, strategy, limit=limit)

            trs = ''
            for x in rows:
                artist = x['artist_name']
                track = x['track_name']
                mapped = get_mapped_uri(cur, artist, track)
                found = None if mapped else search_spotify_track(tok, artist, track, limit=3)
                manual_url = f'https://open.spotify.com/search/{quote_plus(artist + " " + track)}'

                if mapped:
                    status = badge('OK')
                    result = 'MAPEADA'
                    action = '<span class="muted">Guardada</span>'
                elif found:
                    status = badge('OK')
                    result = f"{found['title']} - {found['artist']}"
                    action = f'<a class="btn btn-primary" href="/spotify/save-map?artist={quote_plus(artist)}&track={quote_plus(track)}&uri={quote_plus(found["uri"])}&title={quote_plus(found["title"] or "")}&sp_artist={quote_plus(found["artist"] or "")}">Guardar URI</a>'
                else:
                    status = badge('WARN')
                    result = 'SIN RESULTADO'
                    action = f'<a class="btn btn-secondary" href="{manual_url}" target="_blank">Buscar manual</a>'

                trs += f'''
                <tr>
                    <td>{artist}</td>
                    <td>{track}</td>
                    <td>{safe_int(x['plays'])}</td>
                    <td>{status}</td>
                    <td>{result}</td>
                    <td>{action}</td>
                </tr>
                '''

            if not trs:
                trs = '<tr><td colspan="6" class="muted">No hay candidatos.</td></tr>'

            body = f'''
            <div class="card" style="margin-bottom:18px;">
                <div class="section-title">Diagnóstico búsqueda Spotify</div>
                <div class="mini-row"><span>Estrategia</span><strong>{strategy}</strong></div>
                <div class="mini-row"><span>Límite</span><strong>{limit}</strong></div>
                <div style="margin-top:14px;display:flex;gap:10px;flex-wrap:wrap;">
                    <a class="btn btn-primary" href="/spotify/create-playlist-mapped?strategy={strategy}&limit={limit}">Crear playlist con mapeo</a>
                    <a class="btn btn-secondary" href="/?view=spotify">Volver</a>
                </div>
            </div>
            <div class="card">
                <table>
                    <thead><tr><th>Artista DB</th><th>Canción DB</th><th>Plays</th><th>Estado</th><th>Resultado Spotify</th><th>Acción</th></tr></thead>
                    <tbody>{trs}</tbody>
                </table>
            </div>
            '''
            return base_page('Diagnóstico Spotify', 'spotify', body).replace('__LOAD_TIME__','0.00s').replace('__CACHE_STATUS__','No cache')
        finally:
            cur.close()
            conn.close()

    @app.route('/spotify/save-map')
    def spotify_save_map():
        conn = get_conn()
        cur = conn.cursor()
        try:
            artist = request.args.get('artist') or ''
            track = request.args.get('track') or ''
            uri = request.args.get('uri') or ''
            title = request.args.get('title') or ''
            sp_artist = request.args.get('sp_artist') or ''
            if not artist or not track or not uri:
                return '<pre>Faltan artist, track o uri.</pre>', 400
            save_map(cur, artist, track, uri, title, sp_artist, 'manual')
            conn.commit()
            return '<script>history.back()</script><p>URI guardado. Puedes volver atrás.</p>'
        finally:
            cur.close()
            conn.close()

    @app.route('/spotify/create-playlist-mapped')
    def spotify_create_playlist_mapped():
        conn = get_conn()
        cur = conn.cursor()
        try:
            strategy = request.args.get('strategy') or 'under900'
            limit = min(max(safe_int(request.args.get('limit'), 25), 1), 120)
            res = create_playlist_with_mapping(cur, strategy, limit)
            conn.commit()
            nf = ''.join([f'<tr><td>{x["artist_name"]}</td><td>{x["track_name"]}</td></tr>' for x in res['not_found']]) or '<tr><td colspan="2" class="muted">Todas encontradas.</td></tr>'
            body = f'''
            <div class="card" style="margin-bottom:18px;">
                <div class="section-title">Playlist creada con mapeo</div>
                <div class="mini-row"><span>Nombre</span><strong>{res['name']}</strong></div>
                <div class="mini-row"><span>Candidatas</span><strong>{res['candidates']}</strong></div>
                <div class="mini-row"><span>Encontradas</span><strong>{res['found']}</strong></div>
                <div class="mini-row"><span>Agregadas</span><strong class="green">{res['added']}</strong></div>
                <div style="margin-top:14px;"><a class="btn btn-primary" href="{res['url']}" target="_blank">Abrir en Spotify</a></div>
            </div>
            <div class="card"><div class="section-title">No encontradas</div><table><thead><tr><th>Artista</th><th>Canción</th></tr></thead><tbody>{nf}</tbody></table></div>
            '''
            return base_page('Resultado Spotify', 'spotify', body).replace('__LOAD_TIME__','0.00s').replace('__CACHE_STATUS__','No cache')
        except Exception as e:
            conn.rollback()
            body = f'<div class="card"><div class="section-title">Error Spotify Mapping</div><pre style="white-space:pre-wrap">{str(e)}</pre><a class="btn btn-primary" href="/?view=spotify">Volver</a></div>'
            return base_page('Error Spotify Mapping', 'spotify', body).replace('__LOAD_TIME__','0.00s').replace('__CACHE_STATUS__','No cache'), 500
        finally:
            cur.close()
            conn.close()

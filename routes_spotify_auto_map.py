import re
import unicodedata
from difflib import SequenceMatcher
from urllib.parse import quote_plus

import requests
from flask import request

from layout import badge
from utils import safe_int
from routes_spotify import token, headers, candidates
from routes_spotify_mapping import ensure_spotify_map_table, get_mapped_uri, save_map

API = 'https://api.spotify.com/v1'


def clean_title(value):
    v = (value or '').lower().strip()
    v = ''.join(c for c in unicodedata.normalize('NFKD', v) if not unicodedata.combining(c))
    v = re.sub(r'\(.*?\)|\[.*?\]', ' ', v)
    v = re.sub(r'\b(feat|ft|featuring|remix|remastered|remaster|cover|version|edit|radio edit|explicit|clean|single)\b', ' ', v)
    v = re.sub(r'[^a-z0-9]+', ' ', v)
    return re.sub(r'\s+', ' ', v).strip()


def score_match(a, b):
    ca = clean_title(a)
    cb = clean_title(b)
    if not ca or not cb:
        return 0
    if ca == cb:
        return 100
    if ca in cb or cb in ca:
        return 92
    return int(SequenceMatcher(None, ca, cb).ratio() * 100)


def spotify_search_artist(tok, artist_name):
    r = requests.get(f'{API}/search', headers=headers(tok), params={'q': artist_name, 'type': 'artist', 'limit': 5}, timeout=20)
    if r.status_code >= 400:
        return None
    items = r.json().get('artists', {}).get('items', [])
    if not items:
        return None
    best = None
    best_score = -1
    for item in items:
        s = score_match(artist_name, item.get('name'))
        if s > best_score:
            best = item
            best_score = s
    return best if best_score >= 70 else items[0]


def spotify_artist_tracks(tok, artist_id, max_albums=20):
    tracks = []
    top = requests.get(f'{API}/artists/{artist_id}/top-tracks', headers=headers(tok), params={'market': 'US'}, timeout=20)
    if top.status_code < 400:
        tracks.extend(top.json().get('tracks', []))

    albums = requests.get(f'{API}/artists/{artist_id}/albums', headers=headers(tok), params={'include_groups': 'album,single,appears_on,compilation', 'limit': min(max_albums, 50), 'market': 'US'}, timeout=20)
    if albums.status_code >= 400:
        return tracks

    seen_album = set()
    for album in albums.json().get('items', []):
        album_id = album.get('id')
        if not album_id or album_id in seen_album:
            continue
        seen_album.add(album_id)
        tr = requests.get(f'{API}/albums/{album_id}/tracks', headers=headers(tok), params={'limit': 50, 'market': 'US'}, timeout=20)
        if tr.status_code >= 400:
            continue
        tracks.extend(tr.json().get('items', []))
    return tracks


def auto_find_track(tok, artist_name, track_name):
    artist = spotify_search_artist(tok, artist_name)
    if not artist:
        return None
    tracks = spotify_artist_tracks(tok, artist.get('id'))
    best = None
    best_score = -1
    for t in tracks:
        s = score_match(track_name, t.get('name'))
        if s > best_score:
            best = t
            best_score = s
    if best and best_score >= 82:
        sp_artists = ', '.join([a.get('name', '') for a in best.get('artists', [])])
        return {'uri': best.get('uri'), 'title': best.get('name'), 'artist': sp_artists or artist.get('name'), 'score': best_score, 'artist_found': artist.get('name')}
    return {'uri': None, 'title': best.get('name') if best else '', 'artist': artist.get('name'), 'score': best_score, 'artist_found': artist.get('name')}


def auto_map_candidates(cur, strategy='under900', limit=25):
    tok = token(cur)
    ensure_spotify_map_table(cur)
    rows = candidates(cur, strategy, limit=limit)
    mapped = []
    skipped = []
    already = []
    for x in rows:
        artist = x['artist_name']
        track = x['track_name']
        existing = get_mapped_uri(cur, artist, track)
        if existing:
            already.append({**x, 'uri': existing})
            continue
        found = auto_find_track(tok, artist, track)
        if found and found.get('uri'):
            save_map(cur, artist, track, found['uri'], found.get('title'), found.get('artist'), 'auto_catalog')
            mapped.append({**x, **found})
        else:
            skipped.append({**x, **(found or {})})
    return rows, mapped, already, skipped


def register_spotify_auto_map_routes(app, get_conn, base_page):
    @app.route('/spotify/auto-map')
    def spotify_auto_map():
        conn = get_conn()
        cur = conn.cursor()
        try:
            strategy = request.args.get('strategy') or 'under900'
            limit = min(max(safe_int(request.args.get('limit'), 25), 1), 50)
            rows, mapped, already, skipped = auto_map_candidates(cur, strategy, limit)
            conn.commit()
            mapped_rows = ''
            for r in mapped:
                mapped_rows += f'<tr><td>{r["artist_name"]}</td><td>{r["track_name"]}</td><td>{r.get("title","")}</td><td>{r.get("artist","")}</td><td>{r.get("score","")}</td><td>{badge("OK")}</td></tr>'
            if not mapped_rows:
                mapped_rows = '<tr><td colspan="6" class="muted">No se auto-mapearon canciones nuevas.</td></tr>'
            skipped_rows = ''
            for r in skipped:
                search_url = f'https://open.spotify.com/search/{quote_plus(r["artist_name"] + " " + r["track_name"])}'
                manual_url = f'/spotify/manual-map?artist={quote_plus(r["artist_name"])}&track={quote_plus(r["track_name"])}'
                skipped_rows += f'<tr><td>{r["artist_name"]}</td><td>{r["track_name"]}</td><td>{r.get("artist_found","")}</td><td>{r.get("title","")}</td><td>{r.get("score","")}</td><td><a class="btn btn-secondary" href="{search_url}" target="_blank">Buscar</a> <a class="btn btn-primary" href="{manual_url}">Mapear</a></td></tr>'
            if not skipped_rows:
                skipped_rows = '<tr><td colspan="6" class="muted">Nada pendiente.</td></tr>'
            body = f'''
            <div class="card" style="margin-bottom:18px;">
                <div class="section-title">Auto-mapeo Spotify</div>
                <div class="mini-row"><span>Estrategia</span><strong>{strategy}</strong></div>
                <div class="mini-row"><span>Candidatas revisadas</span><strong>{len(rows)}</strong></div>
                <div class="mini-row"><span>Nuevas mapeadas</span><strong class="green">{len(mapped)}</strong></div>
                <div class="mini-row"><span>Ya estaban mapeadas</span><strong>{len(already)}</strong></div>
                <div class="mini-row"><span>Pendientes</span><strong class="yellow">{len(skipped)}</strong></div>
                <div style="margin-top:14px;display:flex;gap:10px;flex-wrap:wrap;">
                    <a class="btn btn-primary" href="/spotify/create-playlist-mapped?strategy={strategy}&limit={limit}">Crear playlist con mapeo</a>
                    <a class="btn btn-secondary" href="/spotify/debug-search?strategy={strategy}&limit={limit}">Diagnóstico</a>
                    <a class="btn btn-secondary" href="/?view=spotify">Volver</a>
                </div>
            </div>
            <div class="card" style="margin-bottom:18px;"><div class="section-title">Mapeadas automáticamente</div><table><thead><tr><th>Artista DB</th><th>Canción DB</th><th>Spotify título</th><th>Spotify artista</th><th>Score</th><th>Estado</th></tr></thead><tbody>{mapped_rows}</tbody></table></div>
            <div class="card"><div class="section-title">Pendientes por revisar</div><table><thead><tr><th>Artista DB</th><th>Canción DB</th><th>Artista hallado</th><th>Mejor título</th><th>Score</th><th>Acción</th></tr></thead><tbody>{skipped_rows}</tbody></table></div>
            '''
            return base_page('Auto-mapeo Spotify', 'spotify', body).replace('__LOAD_TIME__','0.00s').replace('__CACHE_STATUS__','No cache')
        except Exception as e:
            conn.rollback()
            body = f'<div class="card"><div class="section-title">Error Auto-mapeo Spotify</div><pre style="white-space:pre-wrap">{str(e)}</pre><a class="btn btn-primary" href="/?view=spotify">Volver</a></div>'
            return base_page('Error Auto-mapeo Spotify', 'spotify', body).replace('__LOAD_TIME__','0.00s').replace('__CACHE_STATUS__','No cache'), 500
        finally:
            cur.close()
            conn.close()

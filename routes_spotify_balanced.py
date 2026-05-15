import math
import random
import requests
from flask import request

from routes_spotify import token, headers
from routes_spotify_control import metrics, tr_art, ensure_control
from utils import safe_int

API = 'https://api.spotify.com/v1'


def dm(v):
    try:
        p = [int(x) for x in (v or '').split(':')]
        if len(p) == 2:
            return round(p[0] + p[1] / 60, 2)
        if len(p) == 3:
            return round(p[0] * 60 + p[1] + p[2] / 60, 2)
    except Exception:
        pass
    return 0.0


def load_catalog(cur):
    cur.execute('''
        SELECT artist_name, track_name, spotify_uri, duration, bpm, popularity, energy
        FROM spotify_catalog
        WHERE spotify_uri IS NOT NULL
          AND spotify_uri <> ''
        ORDER BY artist_name ASC, track_name ASC
    ''')
    rows = cur.fetchall()
    songs = []
    seen = set()
    for r in rows:
        uri = r.get('spotify_uri')
        if not uri or uri in seen:
            continue
        seen.add(uri)
        songs.append({
            'artist_name': r.get('artist_name') or 'Sin artista',
            'track_name': r.get('track_name') or 'Sin título',
            'spotify_artist': r.get('artist_name') or 'Sin artista',
            'spotify_title': r.get('track_name') or 'Sin título',
            'uri': uri,
            'duration': dm(r.get('duration')),
            'bpm': safe_int(r.get('bpm')),
            'popularity': safe_int(r.get('popularity')),
            'energy': safe_int(r.get('energy')),
        })
    return songs


def balanced_split(songs, playlist_count=4):
    buckets = [{'songs': [], 'minutes': 0.0, 'artists': {}} for _ in range(playlist_count)]

    by_artist = {}
    for s in songs:
        by_artist.setdefault(s['artist_name'], []).append(s)

    artists = list(by_artist.keys())
    random.shuffle(artists)

    ordered = []
    more = True
    while more:
        more = False
        for a in artists:
            if by_artist[a]:
                ordered.append(by_artist[a].pop(0))
                more = True

    for song in ordered:
        artist = song['artist_name']
        best_idx = 0
        best_score = None
        for i, b in enumerate(buckets):
            artist_count = b['artists'].get(artist, 0)
            score = (artist_count * 9999) + b['minutes'] + (len(b['songs']) * 0.05)
            if best_score is None or score < best_score:
                best_score = score
                best_idx = i
        b = buckets[best_idx]
        b['songs'].append(song)
        b['minutes'] += float(song.get('duration') or 0)
        b['artists'][artist] = b['artists'].get(artist, 0) + 1

    return buckets


def add_bulk(tok, playlist_id, songs):
    ok = []
    bad = []
    for i in range(0, len(songs), 100):
        chunk = songs[i:i+100]
        uris = [x['uri'] for x in chunk if x.get('uri')]
        if not uris:
            continue
        r = requests.post(
            f'{API}/playlists/{playlist_id}/items',
            headers=headers(tok),
            json={'uris': uris},
            timeout=45,
        )
        if r.status_code >= 400:
            for x in chunk:
                bad.append({**x, 'error': f'{r.status_code} {r.text[:180]}'})
        else:
            ok.extend(chunk)
    return ok, bad


def save_control(cur, name, url, strategy, ok, bad, m, devices):
    ensure_control(cur)
    cur.execute('''
        INSERT INTO spotify_playlist_control (
            playlist_name, playlist_url, strategy, tracks_added, duration_minutes,
            artists_count, avg_bpm, avg_popularity, avg_energy, devices_count,
            estimated_daily, estimated_monthly
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ''', (
        name, url, strategy, len(ok), m['dur'], m['arts'], m['bpm'],
        m['pop'], m['en'], devices, m['daily'], m['monthly']
    ))
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
    cur.execute('''
        INSERT INTO spotify_playlist_runs (
            playlist_name, playlist_url, strategy, tracks_found, tracks_added, tracks_not_found
        ) VALUES (%s,%s,%s,%s,%s,%s)
    ''', (name, url, strategy, len(ok), len(ok), len(bad)))


def row_html(results):
    rows = ''
    total_songs = 0
    total_min = 0
    total_daily = 0
    total_month = 0
    failed = 0
    for r in results:
        m = r['metrics']
        total_songs += r['added']
        total_min += m['dur']
        total_daily += m['daily']
        total_month += m['monthly']
        failed += r['failed']
        status = 'OK 5H-6H' if 300 <= m['dur'] <= 360 else ('CORTA' if m['dur'] < 300 else 'LARGA')
        rows += f'<tr><td>{r["name"]}</td><td>{r["added"]}</td><td>{m["dur"]:.1f} min</td><td>{status}</td><td>{m["arts"]}</td><td>{int(m["daily"]):,}</td><td>{int(m["monthly"]):,}</td><td>{r["failed"]}</td><td><a class="btn btn-secondary" target="_blank" href="{r["url"]}">Abrir</a></td></tr>'
    return rows, total_songs, total_min, total_daily, total_month, failed


def register_spotify_balanced_routes(app, get_conn, base_page):
    @app.route('/spotify/monthly-balanced')
    def monthly_balanced():
        conn = None
        cur = None
        try:
            conn = get_conn()
            cur = conn.cursor()
            devices = min(max(safe_int(request.args.get('devices'), 1), 1), 1000)
            playlist_count = min(max(safe_int(request.args.get('count'), 4), 2), 8)
            songs = load_catalog(cur)
            if not songs:
                raise Exception('No hay canciones en spotify_catalog. Primero importa el catálogo.')

            buckets = balanced_split(songs, playlist_count)
            tok = token(cur)
            results = []
            detail = ''
            package_name = request.args.get('name') or 'Paquete Mensual Balanceado'

            for i, b in enumerate(buckets, start=1):
                name = f'{package_name} {i} · {round(b["minutes"] / 60, 1)}H'
                r = requests.post(
                    f'{API}/me/playlists',
                    headers=headers(tok),
                    json={
                        'name': name,
                        'description': 'Playlist mensual balanceada creada desde catálogo completo. Todas las canciones del catálogo se distribuyen sin repetirse dentro del paquete.',
                        'public': False,
                    },
                    timeout=30,
                )
                if r.status_code >= 400:
                    results.append({'name': name, 'url': '', 'added': 0, 'failed': len(b['songs']), 'metrics': {'dur': 0, 'arts': 0, 'daily': 0, 'monthly': 0}})
                    continue
                pl = r.json()
                ok, bad = add_bulk(tok, pl['id'], b['songs'])
                m = metrics(ok, devices)
                url = pl.get('external_urls', {}).get('spotify', '')
                save_control(cur, name, url, 'monthly_balanced_full_catalog', ok, bad, m, devices)
                results.append({'name': name, 'url': url, 'added': len(ok), 'failed': len(bad), 'metrics': m, 'artists': tr_art_like(ok)})
                detail += f'<div class="card"><div class="section-title">{name}: cantidad por artista</div><table><tbody>{tr_art_like(ok)}</tbody></table></div>'

            conn.commit()
            rows, total_songs, total_min, total_daily, total_month, failed = row_html(results)
            catalog_count = len(songs)
            omitted = max(catalog_count - total_songs - failed, 0)
            body = f'''
            <div class="card" style="margin-bottom:18px;">
                <div class="section-title">Paquete mensual balanceado creado</div>
                <div class="mini-row"><span>Canciones en catálogo</span><strong>{catalog_count:,}</strong></div>
                <div class="mini-row"><span>Canciones agregadas</span><strong class="green">{total_songs:,}</strong></div>
                <div class="mini-row"><span>Canciones rechazadas por Spotify</span><strong class="yellow">{failed:,}</strong></div>
                <div class="mini-row"><span>Canciones omitidas por lógica</span><strong>{omitted:,}</strong></div>
                <div class="mini-row"><span>Duración total distribuida</span><strong>{total_min/60:.1f}H</strong></div>
                <div class="mini-row"><span>Equipos 24/7</span><strong>{devices}</strong></div>
                <div class="mini-row"><span>Estimado total/día</span><strong class="green">{int(total_daily):,}</strong></div>
                <div class="mini-row"><span>Estimado total/mes</span><strong class="blue">{int(total_month):,}</strong></div>
                <div style="margin-top:14px;"><a class="btn btn-primary" href="/spotify/control">Volver al control</a></div>
            </div>
            <div class="card" style="margin-bottom:18px;">
                <div class="section-title">Playlists generadas</div>
                <table><thead><tr><th>Playlist</th><th>Canciones</th><th>Duración</th><th>Estado</th><th>Artistas</th><th>Est. día</th><th>Est. mes</th><th>Fallidas</th><th>Abrir</th></tr></thead><tbody>{rows}</tbody></table>
            </div>
            {detail}
            '''
            return base_page('Paquete mensual balanceado', 'spotify', body).replace('__LOAD_TIME__', '0.00s').replace('__CACHE_STATUS__', 'No cache')
        except Exception as e:
            if conn:
                conn.rollback()
            body = f'<div class="card"><div class="section-title">Error paquete balanceado</div><pre style="white-space:pre-wrap">{str(e)}</pre><a class="btn btn-primary" href="/spotify/control">Volver</a></div>'
            return base_page('Error paquete balanceado', 'spotify', body).replace('__LOAD_TIME__', '0.00s').replace('__CACHE_STATUS__', 'No cache'), 500
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()


def tr_art_like(items):
    counts = {}
    for x in items:
        a = x.get('artist_name') or 'Sin artista'
        counts[a] = counts.get(a, 0) + 1
    return ''.join([f'<tr><td>{a}</td><td>{q}</td></tr>' for a, q in sorted(counts.items(), key=lambda z: z[1], reverse=True)]) or '<tr><td colspan="2">Sin datos</td></tr>'

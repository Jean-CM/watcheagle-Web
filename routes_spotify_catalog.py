import csv
import io
import os
from datetime import datetime

from flask import request

from layout import badge
from routes_spotify_mapping import ensure_spotify_map_table, save_map
from utils import safe_int


CATALOG_PATH = os.path.join(os.path.dirname(__file__), 'data', 'trap_mood_vibes.csv')


def pick(row, *names):
    for name in names:
        if name in row and row.get(name) not in [None, '']:
            return str(row.get(name)).strip()
    return ''


def spotify_uri_from_id(value):
    v = (value or '').strip()
    if not v:
        return ''
    if v.startswith('spotify:track:'):
        return v.split('?', 1)[0]
    if 'open.spotify.com/track/' in v:
        v = v.split('/track/', 1)[1].split('?', 1)[0].split('/', 1)[0]
    return f'spotify:track:{v}' if v else ''


def ensure_spotify_catalog_table(cur):
    cur.execute('''
        CREATE TABLE IF NOT EXISTS spotify_catalog (
            id SERIAL PRIMARY KEY,
            track_name TEXT,
            artist_name TEXT,
            spotify_track_id TEXT UNIQUE,
            spotify_uri TEXT,
            isrc TEXT,
            album TEXT,
            album_date TEXT,
            label TEXT,
            duration TEXT,
            popularity INTEGER,
            bpm INTEGER,
            camelot TEXT,
            energy INTEGER,
            dance INTEGER,
            acoustic INTEGER,
            instrumental INTEGER,
            valence INTEGER,
            speech INTEGER,
            live INTEGER,
            loudness INTEGER,
            musical_key TEXT,
            time_signature TEXT,
            genres TEXT,
            source_file TEXT,
            updated_at TIMESTAMP DEFAULT NOW()
        )
    ''')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_spotify_catalog_artist_track ON spotify_catalog (LOWER(artist_name), LOWER(track_name))')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_spotify_catalog_isrc ON spotify_catalog (isrc)')


def import_catalog_rows(cur, rows, source_file='upload'):
    ensure_spotify_catalog_table(cur)
    ensure_spotify_map_table(cur)
    inserted = 0
    mapped = 0
    skipped = []

    for i, row in enumerate(rows, start=1):
        track_name = pick(row, 'Canción', 'Cancion', 'Track', 'Track Name', 'track_name')
        artist_name = pick(row, 'Artista', 'Artist', 'artist_name')
        track_id = pick(row, 'Id De Pista De Spotify', 'Id de pista de Spotify', 'Id De Pista De Spotify ', 'Spotify Track Id', 'spotify_track_id')
        isrc = pick(row, 'ISRC', 'isrc')

        if not track_name or not artist_name or not track_id:
            skipped.append({'row': i, 'reason': 'Falta canción, artista o Spotify Track ID'})
            continue

        spotify_uri = spotify_uri_from_id(track_id)
        if not spotify_uri.startswith('spotify:track:'):
            skipped.append({'row': i, 'reason': 'Spotify Track ID inválido'})
            continue

        cur.execute('''
            INSERT INTO spotify_catalog (
                track_name, artist_name, spotify_track_id, spotify_uri, isrc,
                album, album_date, label, duration, popularity, bpm, camelot,
                energy, dance, acoustic, instrumental, valence, speech, live,
                loudness, musical_key, time_signature, genres, source_file, updated_at
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
            ON CONFLICT (spotify_track_id) DO UPDATE SET
                track_name=EXCLUDED.track_name,
                artist_name=EXCLUDED.artist_name,
                spotify_uri=EXCLUDED.spotify_uri,
                isrc=EXCLUDED.isrc,
                album=EXCLUDED.album,
                album_date=EXCLUDED.album_date,
                label=EXCLUDED.label,
                duration=EXCLUDED.duration,
                popularity=EXCLUDED.popularity,
                bpm=EXCLUDED.bpm,
                camelot=EXCLUDED.camelot,
                energy=EXCLUDED.energy,
                dance=EXCLUDED.dance,
                acoustic=EXCLUDED.acoustic,
                instrumental=EXCLUDED.instrumental,
                valence=EXCLUDED.valence,
                speech=EXCLUDED.speech,
                live=EXCLUDED.live,
                loudness=EXCLUDED.loudness,
                musical_key=EXCLUDED.musical_key,
                time_signature=EXCLUDED.time_signature,
                genres=EXCLUDED.genres,
                source_file=EXCLUDED.source_file,
                updated_at=NOW()
        ''', (
            track_name,
            artist_name,
            track_id,
            spotify_uri,
            isrc,
            pick(row, 'Álbum', 'Album'),
            pick(row, 'Fecha Del Álbum', 'Fecha del álbum', 'Album Date'),
            pick(row, 'Sello', 'Label'),
            pick(row, 'Duración', 'Duracion', 'Duration'),
            safe_int(pick(row, 'Popularidad', 'Popularity'), 0),
            safe_int(pick(row, 'BPM'), 0),
            pick(row, 'Camelot'),
            safe_int(pick(row, 'Energía', 'Energia', 'Energy'), 0),
            safe_int(pick(row, 'Danza', 'Dance'), 0),
            safe_int(pick(row, 'Acústica', 'Acustica', 'Acoustic'), 0),
            safe_int(pick(row, 'Instrumental'), 0),
            safe_int(pick(row, 'Valencia', 'Valence'), 0),
            safe_int(pick(row, 'Discurso', 'Speech'), 0),
            safe_int(pick(row, 'En Directo', 'Live'), 0),
            safe_int(pick(row, 'Fuerte (Db)', 'Loudness'), 0),
            pick(row, 'Clave', 'Key'),
            pick(row, 'Compás', 'Compas', 'Time Signature'),
            pick(row, 'Géneros', 'Generos', 'Genres'),
            source_file,
        ))
        inserted += 1

        try:
            save_map(cur, artist_name, track_name, spotify_uri, track_name, artist_name, 'catalog')
            mapped += 1
        except Exception as e:
            skipped.append({'row': i, 'reason': f'Map error: {str(e)}'})

    return {'inserted': inserted, 'mapped': mapped, 'skipped': skipped[:50]}


def parse_csv_text(text):
    sample = text[:2048]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=',;\t')
    except Exception:
        dialect = csv.excel
    return list(csv.DictReader(io.StringIO(text), dialect=dialect))


def catalog_summary(cur):
    ensure_spotify_catalog_table(cur)
    cur.execute('SELECT COUNT(*) total FROM spotify_catalog')
    total = safe_int(cur.fetchone()['total'])
    cur.execute('SELECT COUNT(DISTINCT artist_name) total FROM spotify_catalog')
    artists = safe_int(cur.fetchone()['total'])
    cur.execute('SELECT COUNT(*) total FROM spotify_track_map')
    mapped = safe_int(cur.fetchone()['total'])
    return total, artists, mapped


def register_spotify_catalog_routes(app, get_conn, base_page):

    @app.route('/spotify/catalog')
    def spotify_catalog_home():
        conn = get_conn()
        cur = conn.cursor()
        try:
            total, artists, mapped = catalog_summary(cur)
            body = f'''
            <div class="grid">
                <div class="card"><div class="label">Tracks en catálogo</div><div class="value blue">{total:,}</div></div>
                <div class="card"><div class="label">Artistas</div><div class="value green">{artists:,}</div></div>
                <div class="card"><div class="label">Mapeos Spotify</div><div class="value yellow">{mapped:,}</div></div>
            </div>
            <div class="card" style="margin-bottom:18px;">
                <div class="section-title">Cargar catálogo Spotify</div>
                <form method="POST" action="/spotify/catalog-upload" enctype="multipart/form-data">
                    <div class="field"><label>CSV con Id De Pista De Spotify</label><input type="file" name="file" accept=".csv" required></div>
                    <button class="btn btn-primary" style="margin-top:14px;">Subir e importar</button>
                </form>
                <div style="margin-top:14px;display:flex;gap:10px;flex-wrap:wrap;">
                    <a class="btn btn-secondary" href="/spotify/catalog-import-bundled">Importar CSV incluido</a>
                    <a class="btn btn-secondary" href="/spotify/create-playlist-mapped?strategy=under900&limit=50">Crear playlist con catálogo</a>
                </div>
            </div>
            <div class="card">
                <div class="section-title">Regla de oro</div>
                <div class="mini-row"><span>Prioridad</span><strong>Spotify Track ID → spotify:track:id → playlist</strong></div>
                <div class="mini-row"><span>Resultado</span><strong>Sin búsqueda por nombre, sin errores de coincidencia.</strong></div>
            </div>
            '''
            return base_page('Catálogo Spotify', 'spotify', body).replace('__LOAD_TIME__','0.00s').replace('__CACHE_STATUS__','No cache')
        finally:
            cur.close()
            conn.close()

    @app.route('/spotify/catalog-upload', methods=['POST'])
    def spotify_catalog_upload():
        f = request.files.get('file')
        if not f:
            return '<pre>No se recibió archivo.</pre>', 400
        text = f.read().decode('utf-8-sig', errors='replace')
        rows = parse_csv_text(text)
        conn = get_conn()
        cur = conn.cursor()
        try:
            result = import_catalog_rows(cur, rows, f.filename)
            conn.commit()
            return catalog_result_page(base_page, result)
        except Exception as e:
            conn.rollback()
            return f'<pre>Error importando catálogo: {str(e)}</pre>', 500
        finally:
            cur.close()
            conn.close()

    @app.route('/spotify/catalog-import-bundled')
    def spotify_catalog_import_bundled():
        if not os.path.exists(CATALOG_PATH):
            return f'<pre>No existe archivo incluido: {CATALOG_PATH}</pre>', 404
        with open(CATALOG_PATH, 'r', encoding='utf-8-sig') as f:
            rows = parse_csv_text(f.read())
        conn = get_conn()
        cur = conn.cursor()
        try:
            result = import_catalog_rows(cur, rows, 'trap_mood_vibes.csv')
            conn.commit()
            return catalog_result_page(base_page, result)
        except Exception as e:
            conn.rollback()
            return f'<pre>Error importando catálogo incluido: {str(e)}</pre>', 500
        finally:
            cur.close()
            conn.close()


def catalog_result_page(base_page, result):
    skipped_rows = ''.join([f'<tr><td>{x.get("row")}</td><td>{x.get("reason")}</td></tr>' for x in result.get('skipped', [])])
    if not skipped_rows:
        skipped_rows = '<tr><td colspan="2" class="muted">Sin errores.</td></tr>'
    body = f'''
    <div class="card" style="margin-bottom:18px;">
        <div class="section-title">Catálogo importado</div>
        <div class="mini-row"><span>Tracks insertados/actualizados</span><strong class="green">{result['inserted']:,}</strong></div>
        <div class="mini-row"><span>Mapeos creados/actualizados</span><strong class="green">{result['mapped']:,}</strong></div>
        <div class="mini-row"><span>Filas con observación</span><strong class="yellow">{len(result.get('skipped', []))}</strong></div>
        <div style="margin-top:14px;display:flex;gap:10px;flex-wrap:wrap;">
            <a class="btn btn-primary" href="/spotify/create-playlist-mapped?strategy=under900&limit=50">Crear playlist con catálogo</a>
            <a class="btn btn-secondary" href="/spotify/catalog">Volver al catálogo</a>
        </div>
    </div>
    <div class="card"><div class="section-title">Observaciones</div><table><thead><tr><th>Fila</th><th>Detalle</th></tr></thead><tbody>{skipped_rows}</tbody></table></div>
    '''
    return base_page('Catálogo importado', 'spotify', body).replace('__LOAD_TIME__','0.00s').replace('__CACHE_STATUS__','No cache')

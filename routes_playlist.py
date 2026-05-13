import csv
import io
from datetime import datetime
from urllib.parse import quote_plus

from flask import Response

from config import MONITOR_PLAYS_ARTISTS
from layout import badge
from utils import safe_int, money


def allowed_artist_clause(alias='s'):
    artists = [a.strip().lower() for a in MONITOR_PLAYS_ARTISTS if a and a.strip()]
    if not artists:
        return '1=0', []
    placeholders = ','.join(['%s'] * len(artists))
    return f'LOWER({alias}.artist_name) IN ({placeholders})', artists


def priority_info(plays):
    plays = safe_int(plays)
    if plays >= 1000:
        return 'COMPLETA', 'Mantener rotación'
    if plays >= 900:
        return 'CERCA', 'Empujar cierre a 1K'
    if plays >= 500:
        return 'MEDIA', 'Subir frecuencia controlada'
    return 'ALTA', 'Meter en playlist prioritaria'


def playlist_rows(cur, limit=None):
    artist_sql, artist_params = allowed_artist_clause('s')
    limit_sql = ''
    params = list(artist_params)
    if limit:
        limit_sql = 'LIMIT %s'
        params.append(limit)

    cur.execute(f'''
        SELECT
            s.artist_name,
            s.track_name,
            COUNT(*) AS plays,
            MAX(s.scrobble_time) AS last_play_at
        FROM scrobbles s
        WHERE {artist_sql}
        GROUP BY s.artist_name, s.track_name
        ORDER BY
            CASE
                WHEN COUNT(*) < 500 THEN 1
                WHEN COUNT(*) >= 900 AND COUNT(*) < 1000 THEN 2
                WHEN COUNT(*) >= 500 AND COUNT(*) < 900 THEN 3
                ELSE 4
            END,
            COUNT(*) ASC,
            s.artist_name ASC,
            s.track_name ASC
        {limit_sql}
    ''', params)
    return cur.fetchall()


def render_playlist_builder(cur):
    rows = playlist_rows(cur, limit=300)

    total = len(rows)
    alta = 0
    media = 0
    cerca = 0
    completa = 0
    missing_total = 0
    table_rows = ''

    for r in rows:
        plays = safe_int(r['plays'])
        missing = max(1000 - plays, 0)
        progress = min(round((plays / 1000) * 100, 1), 100)
        priority, recommendation = priority_info(plays)
        missing_total += missing

        if priority == 'ALTA':
            alta += 1
        elif priority == 'MEDIA':
            media += 1
        elif priority == 'CERCA':
            cerca += 1
        else:
            completa += 1

        spotify_query = f"{r['artist_name']} {r['track_name']}"
        spotify_link = f"https://open.spotify.com/search/{quote_plus(spotify_query)}"

        table_rows += f'''
        <tr>
            <td>{r['artist_name']}</td>
            <td>{r['track_name']}</td>
            <td>{plays:,}</td>
            <td>{missing:,}</td>
            <td>{progress}%</td>
            <td>{badge(priority)}</td>
            <td>{recommendation}</td>
            <td><a class="btn btn-secondary" href="{spotify_link}" target="_blank">Buscar</a></td>
        </tr>
        '''

    if not table_rows:
        table_rows = '<tr><td colspan="8" class="muted">No hay canciones disponibles para construir playlist.</td></tr>'

    return f'''
    <div class="grid">
        <div class="card"><div class="label">Canciones evaluadas</div><div class="value blue">{total}</div></div>
        <div class="card"><div class="label">Prioridad alta</div><div class="value red">{alta}</div></div>
        <div class="card"><div class="label">Cerca de 1K</div><div class="value green">{cerca}</div></div>
        <div class="card"><div class="label">Faltantes a 1K</div><div class="value yellow">{missing_total:,}</div></div>
    </div>

    <div class="grid-3">
        <div class="card"><div class="label">Media</div><div class="value yellow">{media}</div></div>
        <div class="card"><div class="label">Completas</div><div class="value green">{completa}</div></div>
        <div class="card"><div class="label">Artistas permitidos</div><div class="value blue">{len(MONITOR_PLAYS_ARTISTS)}</div></div>
    </div>

    <div class="card" style="margin-bottom:18px;">
        <div class="section-title">Estrategia de playlist</div>
        <div class="mini-row"><span>Objetivo</span><strong>Subir canciones hacia 1,000 plays</strong></div>
        <div class="mini-row"><span>Orden recomendado</span><strong>Alta → Cerca de 1K → Media</strong></div>
        <div class="mini-row"><span>Uso práctico</span><strong>Exportar CSV, buscar canciones y armar playlist por bloques</strong></div>
        <div style="display:flex; gap:10px; flex-wrap:wrap; margin-top:14px;">
            <a class="btn btn-primary" href="/export-playlist-builder.csv">Descargar Playlist Builder CSV</a>
            <a class="btn btn-secondary" href="/?view=monitor-plays">Ver Monitor Plays</a>
        </div>
    </div>

    <div class="card">
        <div class="section-title">Canciones para playlist</div>
        <table>
            <thead>
                <tr>
                    <th>Artista</th>
                    <th>Canción</th>
                    <th>Plays</th>
                    <th>Faltan 1K</th>
                    <th>Avance</th>
                    <th>Prioridad</th>
                    <th>Acción</th>
                    <th>Spotify</th>
                </tr>
            </thead>
            <tbody>{table_rows}</tbody>
        </table>
    </div>
    '''


def export_playlist_builder_csv(cur):
    rows = playlist_rows(cur, limit=None)
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        'artist_name',
        'track_name',
        'spotify_search_query',
        'spotify_search_url',
        'plays',
        'missing_to_1000',
        'progress_percent',
        'priority',
        'recommendation',
        'estimated_revenue',
        'last_play_at',
    ])

    for r in rows:
        plays = safe_int(r['plays'])
        missing = max(1000 - plays, 0)
        progress = min(round((plays / 1000) * 100, 1), 100)
        priority, recommendation = priority_info(plays)
        query = f"{r['artist_name']} {r['track_name']}"
        url = f"https://open.spotify.com/search/{quote_plus(query)}"
        writer.writerow([
            r['artist_name'],
            r['track_name'],
            query,
            url,
            plays,
            missing,
            progress,
            priority,
            recommendation,
            round(plays * 0.0054, 4),
            r['last_play_at'],
        ])

    filename = f"watcheagle_playlist_builder_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv"
    return Response(
        output.getvalue(),
        mimetype='text/csv',
        headers={'Content-Disposition': f'attachment; filename={filename}'}
    )

from layout import badge
from utils import safe_int
from routes_spotify import configured, ensure_tables, SPOTIFY_RANDOM_TARGET_HOURS


def render_spotify_v2(cur):
    ensure_tables(cur)
    cur.execute('SELECT * FROM spotify_tokens WHERE id=1')
    row = cur.fetchone()
    cur.execute('SELECT playlist_name, playlist_url, strategy, tracks_added, tracks_not_found, created_at FROM spotify_playlist_runs ORDER BY created_at DESC LIMIT 12')
    runs = cur.fetchall()
    run_rows = ''.join([
        f'<tr><td>{r["created_at"]}</td><td>{r["strategy"]}</td><td>{r["playlist_name"]}</td><td>{safe_int(r["tracks_added"])}</td><td>{safe_int(r["tracks_not_found"])}</td><td><a class="btn btn-secondary" href="{r["playlist_url"]}" target="_blank">Abrir</a></td></tr>'
        for r in runs
    ]) or '<tr><td colspan="6" class="muted">Sin playlists creadas.</td></tr>'

    return f'''
    <div class="grid">
        <div class="card"><div class="label">Config Spotify</div><div class="value">{badge('OK' if configured() else 'INCIDENT')}</div></div>
        <div class="card"><div class="label">Autorización</div><div class="value">{badge('OK' if row else 'PENDING')}</div></div>
        <div class="card"><div class="label">Modo</div><div class="value blue">Privada forzada</div></div>
        <div class="card"><div class="label">Random</div><div class="value green">{SPOTIFY_RANDOM_TARGET_HOURS}H</div></div>
    </div>

    <div class="card" style="margin-bottom:18px;">
        <div class="section-title">Spotify Automation</div>
        <div class="mini-row"><span>Flujo activo</span><strong>Catálogo estricto + Centro de control</strong></div>
        <div class="mini-row"><span>Regla</span><strong>Solo agrega canciones con Spotify Track ID importado</strong></div>
        <div style="display:flex;gap:10px;flex-wrap:wrap;margin-top:14px;">
            <a class="btn btn-primary" href="/spotify/control">Centro de control</a>
            <a class="btn btn-secondary" href="/spotify/catalog">Catálogo Spotify</a>
            <a class="btn btn-secondary" href="/spotify/reset-auth">Reset + Autorizar Spotify</a>
            <a class="btn btn-secondary" href="/spotify/me">Probar usuario Spotify</a>
            <a class="btn btn-secondary" href="/?view=playlist-builder">Playlist Builder</a>
        </div>
    </div>

    <div class="card" style="margin-bottom:18px;">
        <div class="section-title">Acciones rápidas</div>
        <div style="display:flex;gap:10px;flex-wrap:wrap;">
            <a class="btn btn-primary" href="/spotify/control">Crear playlist con menú</a>
            <a class="btn btn-secondary" href="/spotify/create-playlist-catalog?strategy=under900&limit=50">Crear rápida &lt;900 catálogo</a>
            <a class="btn btn-secondary" href="/spotify/create-playlist-catalog?strategy=random8h&limit=300&target_hours=8">Crear rápida 8H</a>
        </div>
    </div>

    <div class="card">
        <div class="section-title">Últimas playlists</div>
        <table><thead><tr><th>Fecha</th><th>Estrategia</th><th>Playlist</th><th>Agregadas</th><th>No encontradas</th><th>Abrir</th></tr></thead><tbody>{run_rows}</tbody></table>
    </div>
    '''

import base64, random, requests, traceback
from datetime import datetime
from urllib.parse import urlencode
from flask import request, redirect
from config import SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET, SPOTIFY_REDIRECT_URI, SPOTIFY_RANDOM_TARGET_HOURS, MONITOR_PLAYS_ARTISTS
from layout import badge
from utils import safe_int

AUTH_URL='https://accounts.spotify.com/authorize'; TOKEN_URL='https://accounts.spotify.com/api/token'; API='https://api.spotify.com/v1'
SCOPES='playlist-modify-private user-read-email user-read-private'
PLAYLIST_NAMES={
 'under600':['Subiendo el Nivel','Modo Repetición','La Ruta al Hit','Impulso Urbano','Sonido en Crecimiento','Dale Play Sin Miedo'],
 'under900':['Camino al Mil','Empuje Final','La Vuelta Sigue','Rumbo al Top','Playlist de Empuje','Sigue Sonando'],
 'near1k':['A Un Paso del Mil','Cierre de Oro','Último Empujón','Casi Mil Plays','Zona de Cierre','A Punto de Romper'],
 'random8h':['Rotación Infinita','Ocho Horas de Vibra','Maratón de Hits','La Vuelta Completa','Todo el Catálogo','Sesión Sin Pausa']
}
DESCS=['Selección curada para mantener la música girando con intención.','Rotación pensada para descubrir, repetir y empujar canciones clave.','Bloque musical creado para dar movimiento a canciones con potencial.','Playlist dinámica con canciones seleccionadas por prioridad de crecimiento.']

def configured(): return bool(SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET and SPOTIFY_REDIRECT_URI)
def basic_auth(): return 'Basic '+base64.b64encode(f'{SPOTIFY_CLIENT_ID}:{SPOTIFY_CLIENT_SECRET}'.encode()).decode()
def headers(tok): return {'Authorization':f'Bearer {tok}','Content-Type':'application/json'}
def norm(v): return (v or '').strip().lower()
def playlist_name(strategy): return f'{random.choice(PLAYLIST_NAMES.get(strategy, PLAYLIST_NAMES["under900"]))} · {datetime.utcnow().strftime("%b %d")}'
def playlist_desc(strategy): return random.choice(DESCS)

def ensure_tables(cur):
    cur.execute('CREATE TABLE IF NOT EXISTS spotify_tokens (id INTEGER PRIMARY KEY DEFAULT 1, access_token TEXT, refresh_token TEXT, expires_at TIMESTAMP NULL, updated_at TIMESTAMP DEFAULT NOW())')
    cur.execute('CREATE TABLE IF NOT EXISTS spotify_playlist_runs (id SERIAL PRIMARY KEY, playlist_name TEXT, playlist_url TEXT, strategy TEXT, tracks_found INTEGER DEFAULT 0, tracks_added INTEGER DEFAULT 0, tracks_not_found INTEGER DEFAULT 0, created_at TIMESTAMP DEFAULT NOW())')

def save_token(cur, data):
    cur.execute("""INSERT INTO spotify_tokens (id,access_token,refresh_token,expires_at,updated_at) VALUES (1,%s,%s,NOW()+(%s || ' seconds')::interval,NOW()) ON CONFLICT (id) DO UPDATE SET access_token=EXCLUDED.access_token, refresh_token=COALESCE(EXCLUDED.refresh_token, spotify_tokens.refresh_token), expires_at=EXCLUDED.expires_at, updated_at=NOW()""", (data.get('access_token'), data.get('refresh_token'), safe_int(data.get('expires_in'),3600)))

def token(cur):
    ensure_tables(cur); cur.execute('SELECT * FROM spotify_tokens WHERE id=1'); row=cur.fetchone()
    if not row or not row.get('refresh_token'): raise Exception('Spotify no está autorizado. Usa Reset + Autorizar Spotify.')
    if row.get('expires_at') and row['expires_at'] > datetime.utcnow(): return row['access_token']
    r=requests.post(TOKEN_URL,headers={'Authorization':basic_auth()},data={'grant_type':'refresh_token','refresh_token':row['refresh_token']},timeout=20)
    if r.status_code>=400: raise Exception(f'Error refrescando Spotify: {r.status_code} {r.text[:250]}')
    data=r.json(); data['refresh_token']=data.get('refresh_token') or row['refresh_token']; save_token(cur,data); return data['access_token']

def me(tok):
    r=requests.get(f'{API}/me',headers=headers(tok),timeout=20)
    if r.status_code>=400: raise Exception(f'Error /me Spotify: {r.status_code} {r.text[:250]}')
    return r.json()

def search_uri(tok, artist, track):
    for q in [f'track:"{track}" artist:"{artist}"', f'{artist} {track}', f'{track} {artist}', track]:
        r=requests.get(f'{API}/search',headers=headers(tok),params={'q':q,'type':'track','limit':5},timeout=15)
        if r.status_code>=400: continue
        items=r.json().get('tracks',{}).get('items',[])
        if items: return items[0].get('uri')
    return None

def candidates(cur, strategy, limit=25):
    artists=[a.strip().lower() for a in MONITOR_PLAYS_ARTISTS if a.strip()]
    if not artists: raise Exception('MONITOR_PLAYS_ARTISTS está vacío.')
    ph=','.join(['%s']*len(artists)); base=f'LOWER(s.artist_name) IN ({ph})'
    having='COUNT(*) < 1000'; order='COUNT(*) ASC'
    if strategy=='under600': having='COUNT(*) < 600'
    elif strategy=='under900': having='COUNT(*) < 900'
    elif strategy=='near1k': having='COUNT(*) >= 900 AND COUNT(*) < 1000'; order='COUNT(*) DESC'
    elif strategy=='random8h': order='RANDOM()'
    cur.execute(f'SELECT s.artist_name,s.track_name,COUNT(*) plays FROM scrobbles s WHERE {base} GROUP BY s.artist_name,s.track_name HAVING {having} ORDER BY {order}, s.artist_name ASC LIMIT %s', [*artists, limit])
    rows=cur.fetchall()
    if strategy=='random8h': random.shuffle(rows)
    return rows

def add_tracks(tok, playlist_id, uris):
    added=0
    for i in range(0,len(uris),100):
        chunk=uris[i:i+100]
        rr=requests.post(f'{API}/playlists/{playlist_id}/items',headers=headers(tok),json={'uris':chunk},timeout=30)
        if rr.status_code>=400:
            raise Exception(f'Error agregando items: {rr.status_code} {rr.text[:500]}')
        added+=len(chunk)
    return added

def create_playlist(cur, strategy, limit=25):
    tok=token(cur); profile=me(tok); rows=candidates(cur,strategy,limit=limit)
    if not rows: raise Exception(f'No hay canciones candidatas para {strategy}.')
    uris=[]; nf=[]; seen=set()
    for x in rows:
        k=(norm(x['artist_name']),norm(x['track_name']))
        if k in seen: continue
        seen.add(k); uri=search_uri(tok,x['artist_name'],x['track_name'])
        if uri and uri not in uris: uris.append(uri)
        else: nf.append(x)
    if not uris: raise Exception(f'Spotify no encontró canciones agregables para {strategy}. No creé playlist vacía.')
    name=playlist_name(strategy)
    payload={'name':name,'description':playlist_desc(strategy),'public':False}
    r=requests.post(f'{API}/me/playlists',headers=headers(tok),json=payload,timeout=20)
    if r.status_code>=400: raise Exception(f'Error creando playlist: {r.status_code} {r.text[:500]} | user={profile.get("id")}')
    playlist=r.json(); added=add_tracks(tok,playlist['id'],uris)
    url=playlist.get('external_urls',{}).get('spotify','')
    cur.execute('INSERT INTO spotify_playlist_runs (playlist_name,playlist_url,strategy,tracks_found,tracks_added,tracks_not_found) VALUES (%s,%s,%s,%s,%s,%s)',(name,url,strategy,len(uris),added,len(nf)))
    return {'name':name,'url':url,'candidates':len(rows),'found':len(uris),'added':added,'not_found':nf[:40]}

def render_spotify(cur):
    ensure_tables(cur); cur.execute('SELECT * FROM spotify_tokens WHERE id=1'); row=cur.fetchone(); cur.execute('SELECT playlist_name,playlist_url,strategy,tracks_added,tracks_not_found,created_at FROM spotify_playlist_runs ORDER BY created_at DESC LIMIT 12'); runs=cur.fetchall()
    run_rows=''.join([f'<tr><td>{r["created_at"]}</td><td>{r["strategy"]}</td><td>{r["playlist_name"]}</td><td>{safe_int(r["tracks_added"])}</td><td>{safe_int(r["tracks_not_found"])}</td><td><a class="btn btn-secondary" href="{r["playlist_url"]}" target="_blank">Abrir</a></td></tr>' for r in runs]) or '<tr><td colspan="6" class="muted">Sin playlists creadas.</td></tr>'
    return f'''<div class="grid"><div class="card"><div class="label">Config Spotify</div><div class="value">{badge('OK' if configured() else 'INCIDENT')}</div></div><div class="card"><div class="label">Autorización</div><div class="value">{badge('OK' if row else 'PENDING')}</div></div><div class="card"><div class="label">Modo</div><div class="value blue">Privada forzada</div></div><div class="card"><div class="label">Random</div><div class="value green">{SPOTIFY_RANDOM_TARGET_HOURS}H</div></div></div><div class="card" style="margin-bottom:18px;"><div class="section-title">Spotify Automation</div><div class="mini-row"><span>Nombres</span><strong>Dinámicos, llamativos y sin marca técnica</strong></div><div class="mini-row"><span>Endpoint</span><strong>/playlists/id/items</strong></div><div style="display:flex;gap:10px;flex-wrap:wrap;margin-top:14px;"><a class="btn btn-primary" href="/spotify/reset-auth">Reset + Autorizar Spotify</a><a class="btn btn-secondary" href="/spotify/me">Probar usuario Spotify</a><a class="btn btn-secondary" href="/?view=playlist-builder">Playlist Builder</a></div></div><div class="card" style="margin-bottom:18px;"><div class="section-title">Crear playlists</div><div style="display:flex;gap:10px;flex-wrap:wrap;"><a class="btn btn-primary" href="/spotify/create-playlist?strategy=under900&limit=5">Prueba 5 canciones &lt;900</a><a class="btn btn-primary" href="/spotify/create-playlist?strategy=under600&limit=25">Crear &lt;600</a><a class="btn btn-primary" href="/spotify/create-playlist?strategy=under900&limit=25">Crear &lt;900</a><a class="btn btn-secondary" href="/spotify/create-playlist?strategy=near1k&limit=25">Crear 900-999</a><a class="btn btn-secondary" href="/spotify/create-playlist?strategy=random8h&limit=120">Crear random 8H</a></div></div><div class="card"><div class="section-title">Últimas playlists</div><table><thead><tr><th>Fecha</th><th>Estrategia</th><th>Playlist</th><th>Agregadas</th><th>No encontradas</th><th>Abrir</th></tr></thead><tbody>{run_rows}</tbody></table></div>'''

def error_page(base_page, msg):
    body=f'<div class="card"><div class="section-title">Error Spotify</div><pre style="white-space:pre-wrap">{msg}</pre><div style="margin-top:14px;"><a class="btn btn-primary" href="/?view=spotify">Volver a Spotify</a></div></div>'
    return base_page('Error Spotify','spotify',body).replace('__LOAD_TIME__','0.00s').replace('__CACHE_STATUS__','No cache')

def register_spotify_routes(app, get_conn, base_page):
    @app.route('/spotify/login')
    def spotify_login():
        if not configured(): return '<pre>Faltan variables Spotify.</pre>',400
        return redirect(AUTH_URL+'?'+urlencode({'client_id':SPOTIFY_CLIENT_ID,'response_type':'code','redirect_uri':SPOTIFY_REDIRECT_URI,'scope':SCOPES,'show_dialog':'true'}))
    @app.route('/spotify/reset-auth')
    def spotify_reset_auth():
        conn=get_conn(); cur=conn.cursor()
        try: ensure_tables(cur); cur.execute('DELETE FROM spotify_tokens'); conn.commit()
        finally: cur.close(); conn.close()
        return redirect('/spotify/login')
    @app.route('/spotify/me')
    def spotify_me():
        conn=get_conn(); cur=conn.cursor()
        try: p=me(token(cur)); return {'ok':True,'id':p.get('id'),'display_name':p.get('display_name'),'country':p.get('country'),'product':p.get('product')}
        except Exception as e: return {'ok':False,'error':str(e)},500
        finally: cur.close(); conn.close()
    def callback():
        code=request.args.get('code')
        if not code: return '<pre>Spotify callback sin code.</pre>',400
        r=requests.post(TOKEN_URL,headers={'Authorization':basic_auth()},data={'grant_type':'authorization_code','code':code,'redirect_uri':SPOTIFY_REDIRECT_URI},timeout=20)
        if r.status_code>=400: return f'<pre>Error token Spotify:\n{r.status_code}\n{r.text}</pre>',500
        conn=get_conn(); cur=conn.cursor()
        try: ensure_tables(cur); save_token(cur,r.json()); conn.commit()
        finally: cur.close(); conn.close()
        return redirect('/?view=spotify')
    app.add_url_rule('/spotify/callback','spotify_callback',callback); app.add_url_rule('/callback','spotify_callback_root',callback); app.add_url_rule('/spotify-callback','spotify_callback_alt',callback)
    @app.route('/spotify/create-playlist')
    def spotify_create_playlist():
        conn=None; cur=None
        try:
            strategy=request.args.get('strategy') or 'under900'
            limit=min(max(safe_int(request.args.get('limit'),25),1),120)
            conn=get_conn(); cur=conn.cursor()
            res=create_playlist(cur,strategy,limit=limit); conn.commit()
            nf=''.join([f'<tr><td>{x["artist_name"]}</td><td>{x["track_name"]}</td></tr>' for x in res['not_found']]) or '<tr><td colspan="2" class="muted">Todas encontradas.</td></tr>'
            body=f'<div class="card"><div class="section-title">Playlist creada</div><div class="mini-row"><span>Nombre</span><strong>{res["name"]}</strong></div><div class="mini-row"><span>Candidatas</span><strong>{res["candidates"]}</strong></div><div class="mini-row"><span>Encontradas</span><strong>{res["found"]}</strong></div><div class="mini-row"><span>Agregadas</span><strong class="green">{res["added"]}</strong></div><div style="margin-top:14px;"><a class="btn btn-primary" href="{res["url"]}" target="_blank">Abrir en Spotify</a></div></div><div class="card"><div class="section-title">No encontradas</div><table><thead><tr><th>Artista</th><th>Canción</th></tr></thead><tbody>{nf}</tbody></table></div>'
            return base_page('Resultado Spotify Playlist','spotify',body).replace('__LOAD_TIME__','0.00s').replace('__CACHE_STATUS__','No cache')
        except Exception as e:
            if conn: conn.rollback()
            return error_page(base_page, str(e)+'\n\n'+traceback.format_exc()[-1200:]),500
        finally:
            if cur: cur.close()
            if conn: conn.close()

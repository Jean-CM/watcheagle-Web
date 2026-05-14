import requests
from flask import request
from utils import safe_int
from routes_spotify import token, headers, candidates, playlist_name, playlist_desc
from routes_spotify_mapping import ensure_spotify_map_table

API='https://api.spotify.com/v1'

def low(v): return (v or '').strip().lower()
def dm(v):
    try:
        p=[int(x) for x in (v or '').split(':')]
        if len(p)==2: return round(p[0]+p[1]/60,2)
        if len(p)==3: return round(p[0]*60+p[1]+p[2]/60,2)
    except Exception: pass
    return 0

def ensure_control(cur):
    cur.execute('CREATE TABLE IF NOT EXISTS spotify_playlist_control (id SERIAL PRIMARY KEY, playlist_name TEXT, playlist_url TEXT, strategy TEXT, tracks_added INTEGER DEFAULT 0, duration_minutes NUMERIC DEFAULT 0, artists_count INTEGER DEFAULT 0, avg_bpm NUMERIC DEFAULT 0, avg_popularity NUMERIC DEFAULT 0, avg_energy NUMERIC DEFAULT 0, devices_count INTEGER DEFAULT 1, estimated_daily NUMERIC DEFAULT 0, estimated_monthly NUMERIC DEFAULT 0, created_at TIMESTAMP DEFAULT NOW())')

def hit(cur,a,t):
    ensure_spotify_map_table(cur)
    cur.execute('''SELECT m.spotify_uri,m.spotify_title,m.spotify_artist,c.duration,c.bpm,c.popularity,c.energy FROM spotify_track_map m LEFT JOIN spotify_catalog c ON LOWER(c.artist_name)=LOWER(m.artist_name) AND LOWER(c.track_name)=LOWER(m.track_name) WHERE LOWER(m.artist_name)=LOWER(%s) AND LOWER(m.track_name)=LOWER(%s) AND m.source='catalog' LIMIT 1''',(a,t))
    return cur.fetchone()

def pick(cur,strategy,limit,hours):
    rows=candidates(cur,strategy,limit=max(limit,300 if hours else limit))
    out=[]; miss=[]; seen=set(); minutes=0; target=hours*60
    for r in rows:
        if not target and len(out)>=limit: break
        a=r['artist_name']; t=r['track_name']; k=(low(a),low(t))
        if k in seen: continue
        seen.add(k); h=hit(cur,a,t)
        if not h or not h.get('spotify_uri'):
            miss.append(r); continue
        d=dm(h.get('duration'))
        out.append({'artist_name':a,'track_name':t,'spotify_artist':h.get('spotify_artist') or a,'spotify_title':h.get('spotify_title') or t,'uri':h.get('spotify_uri'),'duration':d,'bpm':safe_int(h.get('bpm')),'popularity':safe_int(h.get('popularity')),'energy':safe_int(h.get('energy'))})
        minutes+=d
        if target and minutes>=target: break
    return out,miss

def metrics(items,devices):
    n=len(items); dur=round(sum(float(x['duration'] or 0) for x in items),2); arts=len(set(low(x['artist_name']) for x in items))
    bpm=round(sum(x['bpm'] for x in items)/n,1) if n else 0; pop=round(sum(x['popularity'] for x in items)/n,1) if n else 0; en=round(sum(x['energy'] for x in items)/n,1) if n else 0
    daily=round(n*(1440/dur)*devices,0) if dur else 0
    return {'dur':dur,'arts':arts,'bpm':bpm,'pop':pop,'en':en,'daily':daily,'monthly':daily*30}

def add(tok,pid,items):
    ok=[]; bad=[]
    for x in items:
        r=requests.post(f'{API}/playlists/{pid}/items',headers=headers(tok),json={'uris':[x['uri']]},timeout=30)
        (bad if r.status_code>=400 else ok).append(x)
    return ok,bad

def tr_added(items):
    return ''.join([f'<tr><td>{x["artist_name"]}</td><td>{x["track_name"]}</td><td>{x["spotify_artist"]}</td><td>{x["spotify_title"]}</td></tr>' for x in items]) or '<tr><td colspan="4">Sin datos</td></tr>'
def tr_miss(items):
    return ''.join([f'<tr><td>{x.get("artist_name")}</td><td>{x.get("track_name")}</td><td>No está en catálogo</td></tr>' for x in items]) or '<tr><td colspan="3">Sin datos</td></tr>'
def tr_art(items):
    c={}
    for x in items: c[x['artist_name']]=c.get(x['artist_name'],0)+1
    return ''.join([f'<tr><td>{a}</td><td>{q}</td></tr>' for a,q in sorted(c.items(),key=lambda y:y[1],reverse=True)]) or '<tr><td colspan="2">Sin datos</td></tr>'

def register_spotify_control_routes(app,get_conn,base_page):
    @app.route('/spotify/control')
    def control():
        body='''<div class="card" style="margin-bottom:18px;"><div class="section-title">Centro de control Spotify</div><form class="form-grid" method="GET" action="/spotify/control-create"><div class="field"><label>Tipo</label><select name="strategy"><option value="under600">Catálogo &lt;600</option><option value="under900" selected>Catálogo &lt;900</option><option value="near1k">Catálogo 900-999</option><option value="random8h">Random catálogo</option></select></div><div class="field"><label>Cantidad máxima</label><select name="limit"><option>25</option><option selected>50</option><option>100</option><option>200</option><option>300</option></select></div><div class="field"><label>Duración objetivo</label><select name="hours"><option value="0">Sin límite</option><option value="2">2 horas</option><option value="4">4 horas</option><option value="8">8 horas</option></select></div><div class="field"><label>Equipos 24/7</label><input name="devices" type="number" min="1" max="1000" value="1"></div><button class="btn btn-primary">Crear playlist</button></form></div><div class="card"><div class="section-title">Paquete mensual balanceado</div><div class="mini-row"><span>Regla</span><strong>Usa todo el catálogo, sin repetir canciones dentro del paquete.</strong></div><div class="mini-row"><span>Resultado esperado</span><strong>4 playlists de 5H a 6H aproximadamente.</strong></div><form class="form-grid" method="GET" action="/spotify/monthly-balanced"><div class="field"><label>Cantidad de playlists</label><select name="count"><option selected>4</option><option>3</option><option>5</option><option>6</option></select></div><div class="field"><label>Equipos 24/7</label><input name="devices" type="number" min="1" max="1000" value="1"></div><button class="btn btn-primary">Crear paquete mensual balanceado</button></form></div>'''
        return base_page('Centro de control Spotify','spotify',body).replace('__LOAD_TIME__','0.00s').replace('__CACHE_STATUS__','No cache')
    @app.route('/spotify/control-create')
    def control_create():
        conn=get_conn(); cur=conn.cursor()
        try:
            strategy=request.args.get('strategy') or 'under900'; limit=min(max(safe_int(request.args.get('limit'),50),1),500); hours=safe_int(request.args.get('hours'),0); devices=min(max(safe_int(request.args.get('devices'),1),1),1000)
            items,missing=pick(cur,strategy,limit,hours)
            if not items: raise Exception('No hay canciones exactas en el catálogo para esta selección.')
            tok=token(cur); name=playlist_name(strategy)+(f' · {hours}H' if hours else '')
            r=requests.post(f'{API}/me/playlists',headers=headers(tok),json={'name':name,'description':playlist_desc(strategy),'public':False},timeout=20)
            if r.status_code>=400: raise Exception(r.text[:250])
            pl=r.json(); ok,bad=add(tok,pl['id'],items); m=metrics(ok,devices); ensure_control(cur)
            cur.execute('INSERT INTO spotify_playlist_control (playlist_name,playlist_url,strategy,tracks_added,duration_minutes,artists_count,avg_bpm,avg_popularity,avg_energy,devices_count,estimated_daily,estimated_monthly) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',(name,pl.get('external_urls',{}).get('spotify',''),strategy,len(ok),m['dur'],m['arts'],m['bpm'],m['pop'],m['en'],devices,m['daily'],m['monthly']))
            conn.commit()
            body=f'''<div class="card"><div class="section-title">Playlist creada</div><div class="mini-row"><span>Nombre</span><strong>{name}</strong></div><div class="mini-row"><span>Agregadas</span><strong class="green">{len(ok)}</strong></div><div class="mini-row"><span>No agregadas</span><strong class="yellow">{len(missing)+len(bad)}</strong></div><div style="margin-top:14px"><a class="btn btn-primary" target="_blank" href="{pl.get('external_urls',{}).get('spotify','')}">Abrir en Spotify</a> <a class="btn btn-secondary" href="/spotify/control">Volver</a></div></div><div class="grid"><div class="card"><div class="label">Artistas incluidos</div><div class="value green">{m['arts']}</div></div><div class="card"><div class="label">Duración total</div><div class="value blue">{m['dur']:.1f} min</div></div><div class="card"><div class="label">Promedio BPM</div><div class="value">{m['bpm']}</div></div><div class="card"><div class="label">Promedio popularidad</div><div class="value yellow">{m['pop']}</div></div><div class="card"><div class="label">Promedio energía</div><div class="value">{m['en']}</div></div><div class="card"><div class="label">Estimado día 24/7</div><div class="value green">{int(m['daily']):,}</div></div><div class="card"><div class="label">Estimado mes</div><div class="value blue">{int(m['monthly']):,}</div></div><div class="card"><div class="label">Equipos</div><div class="value">{devices}</div></div></div><div class="card"><div class="section-title">Cantidad por artista</div><table><tbody>{tr_art(ok)}</tbody></table></div><div class="card"><div class="section-title">Canciones agregadas</div><table><thead><tr><th>Artista DB</th><th>Canción DB</th><th>Artista Spotify</th><th>Canción Spotify</th></tr></thead><tbody>{tr_added(ok)}</tbody></table></div><div class="card"><div class="section-title">Canciones no agregadas</div><table><thead><tr><th>Artista</th><th>Canción</th><th>Motivo</th></tr></thead><tbody>{tr_miss(missing)}</tbody></table></div>'''
            return base_page('Reporte Spotify','spotify',body).replace('__LOAD_TIME__','0.00s').replace('__CACHE_STATUS__','No cache')
        except Exception as e:
            conn.rollback(); body=f'<div class="card"><div class="section-title">Error</div><pre>{str(e)}</pre><a class="btn btn-primary" href="/spotify/control">Volver</a></div>'; return base_page('Error Spotify','spotify',body).replace('__LOAD_TIME__','0.00s').replace('__CACHE_STATUS__','No cache'),500
        finally:
            cur.close(); conn.close()

import traceback
import requests
from flask import request

from routes_spotify import token, headers, playlist_name, playlist_desc
from routes_spotify_control import pick, metrics, ensure_control, tr_art
from utils import safe_int

API='https://api.spotify.com/v1'


def add_bulk(tok,pid,items):
    ok=[]; bad=[]
    for i in range(0,len(items),100):
        chunk=items[i:i+100]
        uris=[x['uri'] for x in chunk if x.get('uri')]
        if not uris:
            continue
        r=requests.post(f'{API}/playlists/{pid}/items',headers=headers(tok),json={'uris':uris},timeout=45)
        if r.status_code>=400:
            for x in chunk:
                bad.append({**x,'error':f'{r.status_code} {r.text[:180]}'})
        else:
            ok.extend(chunk)
    return ok,bad


def save_run(cur,name,url,strategy,ok,missing,bad,m,devices):
    ensure_control(cur)
    cur.execute('INSERT INTO spotify_playlist_control (playlist_name,playlist_url,strategy,tracks_added,duration_minutes,artists_count,avg_bpm,avg_popularity,avg_energy,devices_count,estimated_daily,estimated_monthly) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',(name,url,strategy,len(ok),m['dur'],m['arts'],m['bpm'],m['pop'],m['en'],devices,m['daily'],m['monthly']))
    cur.execute('CREATE TABLE IF NOT EXISTS spotify_playlist_runs (id SERIAL PRIMARY KEY, playlist_name TEXT, playlist_url TEXT, strategy TEXT, tracks_found INTEGER DEFAULT 0, tracks_added INTEGER DEFAULT 0, tracks_not_found INTEGER DEFAULT 0, created_at TIMESTAMP DEFAULT NOW())')
    cur.execute('INSERT INTO spotify_playlist_runs (playlist_name,playlist_url,strategy,tracks_found,tracks_added,tracks_not_found) VALUES (%s,%s,%s,%s,%s,%s)',(name,url,strategy,len(ok),len(ok),len(missing)+len(bad)))


def make_one(cur,tok,strategy,label,devices):
    items,missing=pick(cur,strategy,300,8)
    if not items:
        return {'ok':False,'label':label,'error':'Sin canciones exactas en catálogo','strategy':strategy}
    name=f'{label} · {playlist_name(strategy)} · 8H'
    r=requests.post(f'{API}/me/playlists',headers=headers(tok),json={'name':name,'description':playlist_desc(strategy),'public':False},timeout=30)
    if r.status_code>=400:
        return {'ok':False,'label':label,'error':r.text[:220],'strategy':strategy}
    pl=r.json()
    ok,bad=add_bulk(tok,pl['id'],items)
    m=metrics(ok,devices)
    url=pl.get('external_urls',{}).get('spotify','')
    save_run(cur,name,url,'monthly_'+strategy,ok,missing,bad,m,devices)
    return {'ok':True,'label':label,'name':name,'url':url,'added':len(ok),'missing':len(missing)+len(bad),'metrics':m,'artists':tr_art(ok)}


def register_spotify_monthly_routes(app,get_conn,base_page):
    @app.route('/spotify/monthly-8h')
    def monthly_8h():
        conn=None; cur=None
        try:
            conn=get_conn(); cur=conn.cursor()
            devices=min(max(safe_int(request.args.get('devices'),1),1),1000)
            tok=token(cur)
            packs=[('under600','Prioridad Alta'),('under900','Empuje General'),('near1k','Cierre a 1K'),('random8h','Rotación Random')]
            results=[]
            for s,l in packs:
                results.append(make_one(cur,tok,s,l,devices))
            conn.commit()
            rows=''; detail=''; total_daily=0; total_month=0
            for r in results:
                if r.get('ok'):
                    m=r['metrics']; total_daily+=m['daily']; total_month+=m['monthly']
                    rows+=f'<tr><td>{r["label"]}</td><td>{r["name"]}</td><td>{r["added"]}</td><td>{m["dur"]:.1f} min</td><td>{int(m["daily"]):,}</td><td>{int(m["monthly"]):,}</td><td>{r["missing"]}</td><td><a class="btn btn-secondary" target="_blank" href="{r["url"]}">Abrir</a></td></tr>'
                    detail+=f'<div class="card"><div class="section-title">{r["label"]}: cantidad por artista</div><table><tbody>{r["artists"]}</tbody></table></div>'
                else:
                    rows+=f'<tr><td>{r.get("label")}</td><td colspan="6">ERROR: {r.get("error")}</td><td></td></tr>'
            body=f'''<div class="card" style="margin-bottom:18px;"><div class="section-title">Paquete mensual 8H creado</div><div class="mini-row"><span>Equipos 24/7</span><strong>{devices}</strong></div><div class="mini-row"><span>Estimado total/día</span><strong class="green">{int(total_daily):,}</strong></div><div class="mini-row"><span>Estimado total/mes</span><strong class="blue">{int(total_month):,}</strong></div><div style="margin-top:14px"><a class="btn btn-primary" href="/spotify/control">Volver al control</a></div></div><div class="card"><div class="section-title">Playlists generadas</div><table><thead><tr><th>Tipo</th><th>Playlist</th><th>Canciones</th><th>Duración</th><th>Est. día</th><th>Est. mes</th><th>No agregadas</th><th>Abrir</th></tr></thead><tbody>{rows}</tbody></table></div>{detail}'''
            return base_page('Paquete mensual Spotify','spotify',body).replace('__LOAD_TIME__','0.00s').replace('__CACHE_STATUS__','No cache')
        except Exception as e:
            if conn: conn.rollback()
            msg=str(e)+'\n\n'+traceback.format_exc()[-1600:]
            body=f'<div class="card"><div class="section-title">Error paquete mensual</div><pre style="white-space:pre-wrap">{msg}</pre><a class="btn btn-primary" href="/spotify/control">Volver</a></div>'
            return base_page('Error paquete mensual','spotify',body).replace('__LOAD_TIME__','0.00s').replace('__CACHE_STATUS__','No cache'),500
        finally:
            if cur: cur.close()
            if conn: conn.close()

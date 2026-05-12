import csv
import io
import time
from datetime import datetime, timedelta

from flask import Flask, request, Response

from helpers import get_conn, init_db
from layout import base_page
from utils import safe_int
from views import (
    render_monitor,
    render_analisis,
    render_monitor_plays,
    month_where,
    monitor_artist_filter,
    avg_rate,
)
from routes_executive import render_ejecutivo_fast
from routes_operations import render_operaciones
from routes_history import render_historico, register_history_routes
from routes_jobs import register_job_routes
from routes_teams import register_team_routes
from routes_init import register_init_routes
from config import APP_PORT

app = Flask(__name__)

VIEW_CACHE = {}
CACHE_TTL_SECONDS = 90

try:
    init_db()
except Exception as e:
    print(f"[WARN] init_db startup failed: {e}")


def cache_key_for_request(view):
    args = tuple(sorted((k, v) for k, v in request.args.items()))
    return (view, args)


def is_cacheable(view):
    return view in {"ejecutivo", "operaciones", "analisis", "ganancias", "monitor-plays"}


def render_with_db(view):
    conn = None
    cur = None
    try:
        conn = get_conn()
        cur = conn.cursor()

        if view == "operaciones":
            return "Centro operacional", render_operaciones(cur), view
        if view == "monitor":
            return "Monitoreo operativo", render_monitor(cur), view
        if view == "historico":
            return "Control histórico Last.fm", render_historico(cur), view
        if view in {"analisis", "ganancias"}:
            return "Análisis financiero", render_analisis(cur), "analisis"
        if view == "monitor-plays":
            return "Seguimiento de canciones debajo de 1000", render_monitor_plays(cur), view

        return "Tablero ejecutivo", render_ejecutivo_fast(cur), "ejecutivo"

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


@app.route("/")
def home():
    started = time.perf_counter()
    try:
        view = (request.args.get("view") or "ejecutivo").strip().lower()
        key = cache_key_for_request(view)
        now = datetime.utcnow()

        if is_cacheable(view) and key in VIEW_CACHE:
            cached_at, html = VIEW_CACHE[key]
            age = (now - cached_at).total_seconds()
            if age <= CACHE_TTL_SECONDS:
                elapsed = time.perf_counter() - started
                return html.replace("__LOAD_TIME__", f"{elapsed:.2f}s").replace("__CACHE_STATUS__", f"Cache: HIT ({int(age)}s)")

        title, body, normalized_view = render_with_db(view)
        elapsed = time.perf_counter() - started
        html = base_page(title, normalized_view, body)
        html = html.replace("__LOAD_TIME__", f"{elapsed:.2f}s").replace("__CACHE_STATUS__", "Cache: MISS")

        if is_cacheable(normalized_view):
            VIEW_CACHE[key] = (now, html)

        return html

    except Exception as e:
        return f"<pre>ERROR EN HOME:\n{str(e)}</pre>", 500


@app.route("/export-monitor-plays.csv")
def export_monitor_plays_csv():
    conn = None
    cur = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        where, params = month_where('s')
        artist_sql, artist_params = monitor_artist_filter('s')
        where = f'{where} AND {artist_sql}'
        params = [*params, *artist_params]

        cur.execute(f'''
            SELECT
                s.artist_name,
                s.track_name,
                LOWER(COALESCE(s.app_name, 'spotify')) AS platform,
                COUNT(*) AS plays,
                MAX(s.scrobble_time) AS last_play_at
            FROM scrobbles s
            WHERE {where}
            GROUP BY s.artist_name, s.track_name, LOWER(COALESCE(s.app_name, 'spotify'))
            ORDER BY plays ASC, s.artist_name ASC, s.track_name ASC
        ''', params)
        rows = cur.fetchall()

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow([
            'artist_name',
            'track_name',
            'platform',
            'plays',
            'missing_to_1000',
            'progress_percent',
            'estimated_revenue',
            'priority',
            'recommendation',
            'last_play_at',
        ])

        for r in rows:
            plays = safe_int(r['plays'])
            missing = max(1000 - plays, 0)
            progress = min(round((plays / 1000) * 100, 1), 100)
            revenue = round(plays * avg_rate(r['platform']), 4)

            if plays >= 1000:
                priority = 'COMPLETA'
                recommendation = 'Mantener rotación'
            elif plays >= 900:
                priority = 'CERCA'
                recommendation = 'Empujar cierre a 1K'
            elif plays >= 500:
                priority = 'MEDIA'
                recommendation = 'Mantener rotación y subir frecuencia'
            else:
                priority = 'ALTA'
                recommendation = 'Prioridad de empuje para playlist'

            writer.writerow([
                r['artist_name'],
                r['track_name'],
                r['platform'],
                plays,
                missing,
                progress,
                revenue,
                priority,
                recommendation,
                r['last_play_at'],
            ])

        csv_data = output.getvalue()
        filename = f"watcheagle_monitor_plays_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv"
        return Response(
            csv_data,
            mimetype='text/csv',
            headers={'Content-Disposition': f'attachment; filename={filename}'}
        )

    except Exception as e:
        return f"<pre>ERROR EXPORTANDO CSV:\n{str(e)}</pre>", 500
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def non_owned_where(alias='s'):
    artist_sql, artist_params = monitor_artist_filter(alias)
    return f'NOT ({artist_sql})', artist_params


@app.route("/cleanup-non-owned-scrobbles-preview")
def cleanup_non_owned_scrobbles_preview():
    conn = None
    cur = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        where, params = non_owned_where('s')

        cur.execute(f"SELECT COUNT(*) AS total FROM scrobbles s WHERE {where}", params)
        total = safe_int(cur.fetchone()['total'])

        cur.execute(f"SELECT COUNT(DISTINCT artist_name) AS total FROM scrobbles s WHERE {where}", params)
        artists = safe_int(cur.fetchone()['total'])

        cur.execute(f'''
            SELECT artist_name, COUNT(*) AS plays
            FROM scrobbles s
            WHERE {where}
            GROUP BY artist_name
            ORDER BY plays DESC
            LIMIT 50
        ''', params)
        rows = cur.fetchall()

        table_rows = ''.join([
            f"<tr><td>{r['artist_name']}</td><td>{safe_int(r['plays']):,}</td></tr>"
            for r in rows
        ]) or '<tr><td colspan="2" class="muted">No hay artistas externos.</td></tr>'

        body = f'''
        <div class="grid-2">
            <div class="card"><div class="label">Scrobbles externos detectados</div><div class="value red">{total:,}</div></div>
            <div class="card"><div class="label">Artistas externos</div><div class="value yellow">{artists:,}</div></div>
        </div>
        <div class="card" style="margin-bottom:18px;">
            <div class="section-title">Acción segura</div>
            <div class="mini-row"><span>Preview</span><strong>Esto todavía NO borra nada</strong></div>
            <div class="mini-row"><span>Lista blanca</span><strong>MONITOR_PLAYS_ARTISTS</strong></div>
            <div style="margin-top:14px;">
                <a class="btn btn-primary" href="/cleanup-non-owned-scrobbles-run?confirm=YES_DELETE_EXTERNALS">Borrar externos confirmados</a>
            </div>
        </div>
        <div class="card">
            <div class="section-title">Top 50 artistas externos a borrar</div>
            <table><thead><tr><th>Artista</th><th>Scrobbles</th></tr></thead><tbody>{table_rows}</tbody></table>
        </div>
        '''
        html = base_page('Preview limpieza de scrobbles externos', 'operaciones', body)
        return html.replace('__LOAD_TIME__', '0.00s').replace('__CACHE_STATUS__', 'No cache')

    except Exception as e:
        return f"<pre>ERROR PREVIEW CLEANUP:\n{str(e)}</pre>", 500
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


@app.route("/cleanup-non-owned-scrobbles-run")
def cleanup_non_owned_scrobbles_run():
    if request.args.get('confirm') != 'YES_DELETE_EXTERNALS':
        return '<pre>Falta confirm=YES_DELETE_EXTERNALS. No se borró nada.</pre>', 400

    conn = None
    cur = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        where, params = non_owned_where('s')

        cur.execute(f"DELETE FROM scrobbles s WHERE {where}", params)
        deleted = cur.rowcount
        conn.commit()
        VIEW_CACHE.clear()

        body = f'''
        <div class="card">
            <div class="section-title">Limpieza completada</div>
            <div class="mini-row"><span>Scrobbles externos borrados</span><strong class="green">{deleted:,}</strong></div>
            <div class="mini-row"><span>Cache</span><strong>Limpiada</strong></div>
            <div style="margin-top:14px;"><a class="btn btn-primary" href="/?view=monitor-plays">Volver a Monitor Plays</a></div>
        </div>
        '''
        html = base_page('Limpieza completada', 'operaciones', body)
        return html.replace('__LOAD_TIME__', '0.00s').replace('__CACHE_STATUS__', 'No cache')

    except Exception as e:
        if conn:
            conn.rollback()
        return f"<pre>ERROR RUN CLEANUP:\n{str(e)}</pre>", 500
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


@app.route("/cache-clear")
def cache_clear():
    VIEW_CACHE.clear()
    return {"ok": True, "message": "Cache limpiado"}


register_job_routes(app)
register_team_routes(app)
register_init_routes(app)
register_history_routes(app)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=APP_PORT)

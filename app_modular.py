import time
from datetime import datetime, timedelta

from flask import Flask, request

from helpers import get_conn, init_db
from layout import base_page
from views import (
    render_ejecutivo,
    render_monitor,
    render_analisis,
    render_ganancias,
    render_monitor_plays,
)
from routes_operations import render_operaciones
from routes_history import render_historico, register_history_routes
from routes_jobs import register_job_routes
from routes_teams import register_team_routes
from routes_init import register_init_routes
from config import APP_PORT

app = Flask(__name__)

# Cache simple en memoria por instancia Render.
# Baja presión a Postgres en vistas pesadas. Se invalida por tiempo.
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
    # Monitor e histórico deben sentirse frescos; las demás pueden cachearse brevemente.
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
        if view == "analisis":
            return "Vista analítica pro", render_analisis(cur), view
        if view == "ganancias":
            return "Vista de ganancias pro", render_ganancias(cur), view
        if view == "monitor-plays":
            return "Seguimiento de canciones debajo de 1000", render_monitor_plays(cur), view

        return "Tablero ejecutivo", render_ejecutivo(cur), "ejecutivo"

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

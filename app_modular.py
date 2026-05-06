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

# Se inicializa una sola vez al arrancar el servicio.
# No debe correr en cada request porque crea/valida tablas e índices y pone lento el dashboard.
try:
    init_db()
except Exception as e:
    print(f"[WARN] init_db startup failed: {e}")


@app.route("/")
def home():
    conn = None
    cur = None
    try:
        view = (request.args.get("view") or "ejecutivo").strip().lower()
        conn = get_conn()
        cur = conn.cursor()

        if view == "operaciones":
            body = render_operaciones(cur)
            title = "Centro operacional"
        elif view == "monitor":
            body = render_monitor(cur)
            title = "Monitoreo operativo"
        elif view == "historico":
            body = render_historico(cur)
            title = "Control histórico Last.fm"
        elif view == "analisis":
            body = render_analisis(cur)
            title = "Vista analítica pro"
        elif view == "ganancias":
            body = render_ganancias(cur)
            title = "Vista de ganancias pro"
        elif view == "monitor-plays":
            body = render_monitor_plays(cur)
            title = "Seguimiento de canciones debajo de 1000"
        else:
            view = "ejecutivo"
            body = render_ejecutivo(cur)
            title = "Tablero ejecutivo"

        return base_page(title, view, body)

    except Exception as e:
        return f"<pre>ERROR EN HOME:\n{str(e)}</pre>", 500

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


register_job_routes(app)
register_team_routes(app)
register_init_routes(app)
register_history_routes(app)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=APP_PORT)

from datetime import datetime, timedelta

from utils import current_filters
from styles import BASE_CSS


def filter_query(view):
    date_from, date_to, month, platform, distributor = current_filters()
    q = f"?view={view}"
    if date_from:
        q += f"&date_from={date_from}"
    if date_to:
        q += f"&date_to={date_to}"
    if month and not (date_from or date_to):
        q += f"&month={month}"
    if platform:
        q += f"&platform={platform}"
    if distributor:
        q += f"&distributor={distributor}"
    return q


def default_dates(date_from, date_to):
    if date_from and date_to:
        return date_from, date_to
    today = datetime.utcnow().date()
    return str(today - timedelta(days=30)), str(today)


def filter_form(view):
    date_from, date_to, month, platform, distributor = current_filters()
    date_from_value, date_to_value = default_dates(date_from, date_to)
    return f"""
    <div class="card" style="margin-bottom:18px;">
        <div class="section-title">Filtros</div>
        <form class="form-grid" method="GET" action="/">
            <input type="hidden" name="view" value="{view}">
            <div class="field"><label>Desde</label><input type="date" name="date_from" value="{date_from_value}"></div>
            <div class="field"><label>Hasta</label><input type="date" name="date_to" value="{date_to_value}"></div>
            <div class="field"><label>Plataforma</label>
                <select name="platform">
                    <option value="" {"selected" if not platform else ""}>Todas</option>
                    <option value="spotify" {"selected" if platform == "spotify" else ""}>Spotify</option>
                    <option value="apple" {"selected" if platform == "apple" else ""}>Apple Music</option>
                    <option value="tidal" {"selected" if platform == "tidal" else ""}>Tidal</option>
                    <option value="youtube" {"selected" if platform == "youtube" else ""}>YouTube Music</option>
                </select>
            </div>
            <div class="field"><label>Distribuidora</label>
                <select name="distributor">
                    <option value="" {"selected" if not distributor else ""}>Todas</option>
                    <option value="Distrokid" {"selected" if distributor == "Distrokid" else ""}>Distrokid</option>
                    <option value="Ditto" {"selected" if distributor == "Ditto" else ""}>Ditto</option>
                    <option value="TuneCore" {"selected" if distributor == "TuneCore" else ""}>TuneCore</option>
                    <option value="Symphonic" {"selected" if distributor == "Symphonic" else ""}>Symphonic</option>
                </select>
            </div>
            <button class="btn btn-primary">Aplicar</button>
        </form>
    </div>
    """


def badge(status):
    s = (status or "PENDING").upper()
    cls = {
        "OK": "ok",
        "WARN": "warn",
        "INCIDENT": "incident",
        "PENDING": "pending",
        "COMPLETO": "ok",
        "FALTA_HISTORICO": "warn",
        "SIN_DATA": "incident",
        "ERROR_LASTFM": "incident",
    }.get(s, "pending")
    return f'<span class="badge {cls}">{s}</span>'


def nav_link(label, view, current):
    active = "active" if view == current else ""
    return f'<a class="nav-link {active}" href="/{filter_query(view)}">{label}</a>'


def system_banner():
    updated_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    return f"""
    <div class="card" style="margin-bottom:18px; padding:14px 18px;">
        <div class="mini-row" style="border-bottom:0; padding:0;">
            <span><strong class="green">● Sistema operativo</strong></span>
            <span class="muted">Última actualización visual: <strong>{updated_at}</strong> · __LOAD_TIME__ · __CACHE_STATUS__</span>
        </div>
    </div>
    """


def base_page(title, view, body):
    nav = (
        nav_link("Ejecutivo", "ejecutivo", view)
        + nav_link("Operaciones", "operaciones", view)
        + nav_link("Monitor", "monitor", view)
        + nav_link("Histórico Last.fm", "historico", view)
        + nav_link("Análisis", "analisis", view)
        + nav_link("Ganancias", "ganancias", view)
        + nav_link("Monitor Plays", "monitor-plays", view)
    )

    tools = """
    <div class="tools">
        <a class="tool-link" href="/run-check">run-check</a>
        <a class="tool-link" href="/collect-now">collect-now</a>
        <a class="tool-link" href="/collect-all">collect-all todos</a>
        <a class="tool-link" href="/job-log?job=collect-now">log collect-now</a>
        <a class="tool-link" href="/job-log?job=collect-all">log collect-all</a>
        <a class="tool-link" href="/job-log?job=collect-all-selected">log seleccionados</a>
        <a class="tool-link" href="/scrobbles-count">scrobbles-count</a>
        <a class="tool-link" href="/healthz">healthz</a>
        <a class="tool-link" href="/init-artist-metadata">init distribuidoras</a>
        <a class="tool-link" href="/init-performance-indexes">init performance</a>
        <a class="tool-link" href="/init-lastfm-history-table">init histórico</a>
        <a class="tool-link" href="/cache-clear">limpiar cache</a>
    </div>
    """

    return f"""
<!doctype html>
<html>
<head>
<title>WatchEagle</title>
<script src='https://cdn.jsdelivr.net/npm/chart.js'></script>
<style>{BASE_CSS}</style>
</head>
<body>
<div class='page'>
<h1>WatchEagle</h1>
<div class='subtitle'>{title}</div>
<div class='nav'>{nav}</div>
{tools}
{system_banner()}
{body}
</div>
</body>
</html>
"""

from datetime import datetime
from utils import current_filters
from styles import BASE_CSS


def filter_query(view):
    month, platform, distributor = current_filters()
    q = '?view=' + view
    if month:
        q += '&month=' + month
    if platform:
        q += '&platform=' + platform
    if distributor:
        q += '&distributor=' + distributor
    return q


def filter_form(view):
    month, platform, distributor = current_filters()
    return '<div class="card"><form method="GET" action="/"><input type="hidden" name="view" value="' + view + '"><label>Mes</label><input type="month" name="month" value="' + month + '"><label>Plataforma</label><input name="platform" value="' + platform + '"><label>Distribuidora</label><input name="distributor" value="' + distributor + '"><button>Aplicar</button></form></div>'


def badge(status):
    s = (status or 'PENDING').upper()
    return '<span class="badge">' + s + '</span>'


def nav_link(label, view, current):
    active = ' active' if view == current else ''
    return '<a class="nav-link' + active + '" href="/' + filter_query(view) + '">' + label + '</a>'


def system_banner():
    t = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')
    return '<div class="card">Sistema operativo · ' + t + ' · __LOAD_TIME__ · __CACHE_STATUS__</div>'


def base_page(title, view, body):
    nav = nav_link('Ejecutivo','ejecutivo',view) + nav_link('Operaciones','operaciones',view) + nav_link('Monitor','monitor',view) + nav_link('Histórico','historico',view) + nav_link('Análisis','analisis',view) + nav_link('Ganancias','ganancias',view) + nav_link('Monitor Plays','monitor-plays',view)
    tools = '<div class="tools"><a href="/collect-now">collect-now</a> <a href="/healthz">healthz</a> <a href="/cache-clear">limpiar cache</a></div>'
    return '<html><head><title>WatchEagle</title><style>' + BASE_CSS + '</style></head><body><div class="page"><h1>WatchEagle</h1><div class="subtitle">' + title + '</div><div class="nav">' + nav + '</div>' + tools + system_banner() + body + '</div></body></html>'

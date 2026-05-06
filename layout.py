from utils import current_filters
from styles import BASE_CSS


def filter_query(view):
    month, platform, distributor = current_filters()
    q = f"?view={view}"
    if month:
        q += f"&month={month}"
    if platform:
        q += f"&platform={platform}"
    if distributor:
        q += f"&distributor={distributor}"
    return q


def nav_link(label, view, current):
    active = "active" if view == current else ""
    return f'<a class="nav-link {active}" href="/{filter_query(view)}">{label}</a>'


def base_page(title, view, body):
    nav = (
        nav_link("Ejecutivo", "ejecutivo", view)
        + nav_link("Monitor", "monitor", view)
        + nav_link("Histórico", "historico", view)
        + nav_link("Análisis", "analisis", view)
        + nav_link("Ganancias", "ganancias", view)
    )

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
{body}
</div>
</body>
</html>
"""

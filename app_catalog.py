from app_modular import app

from helpers import get_conn
from layout import base_page
from routes_spotify_catalog import register_spotify_catalog_routes

register_spotify_catalog_routes(app, get_conn, base_page)

import os

PLATFORM_RATES = {
    "spotify": {"min": 0.0035, "max": 0.0050},
    "apple": {"min": 0.0070, "max": 0.0100},
    "apple music": {"min": 0.0070, "max": 0.0100},
    "tidal": {"min": 0.0120, "max": 0.0150},
    "youtube": {"min": 0.0007, "max": 0.0020},
    "youtube music": {"min": 0.0007, "max": 0.0020},
}

ARTIST_METADATA = [
    ("Jeantune", "Jean C", "Distrokid"),
    ("JCSTUDIO", "Jean C", "Distrokid"),
    ("JMAR", "Jean C", "Ditto"),
    ("YlegMoon", "Angely", "Distrokid"),
    ("Batytune", "Angely", "Distrokid"),
    ("Jzentrix", "Dari", "Distrokid"),
    ("JironPulse", "Micha", "Distrokid"),
    ("God Herd", "Jean C", "TuneCore"),
    ("JJ Legacy", "Jean C", "Symphonic"),
    ("Cielaurum", "Angely", "Ditto"),
    ("QuietMetric", "Dari", "Ditto"),
    ("AetherFocus", "Jean C", "Ditto"),
    ("ZukiPop", "Jean C", "Distrokid"),
    ("LexiGo", "Jean C", "Distrokid"),
    ("VYRONEX", "Jean C", "Distrokid"),
    ("AEROVIA", "Jean C", "Distrokid"),
    ("TechMich", "Micha", "Distrokid"),
    ("KRYONEXIS", "Angy", "Symphonic"),
]

# Lista blanca para Monitor Plays y export CSV.
# Solo estos artistas aparecerán en esa vista/descarga.
# Para agregar/quitar artistas, modifica esta lista y despliega de nuevo.
MONITOR_PLAYS_ARTISTS = [artist for artist, _, _ in ARTIST_METADATA]

JOB_LOG_DIR = os.getenv("JOB_LOG_DIR", "/tmp/watcheagle_jobs")
LASTFM_API_KEY = os.getenv("LASTFM_API_KEY", "")
LASTFM_HISTORY_MARGIN_DAYS = int(os.getenv("LASTFM_HISTORY_MARGIN_DAYS", "3"))
APP_PORT = int(os.getenv("PORT", "10000"))

@app.route("/debug-env")
def debug_env():
    return {
        "LASTFM_API_KEY_exists": bool(os.environ.get("LASTFM_API_KEY")),
        "LASTFM_API_KEY_prefix": (os.environ.get("LASTFM_API_KEY") or "")[:6]
    }

def get_recent_track(lastfm_user):
    url = "https://ws.audioscrobbler.com/2.0/"
    params = {
        "method": "user.getrecenttracks",
        "user": lastfm_user,
        "api_key": LASTFM_API_KEY,
        "format": "json",
        "limit": 1
    }

    response = requests.get(url, params=params, timeout=30)

    # No hacemos raise_for_status todavía; primero inspeccionamos la respuesta
    try:
        data = response.json()
    except Exception:
        raise Exception(f"Respuesta no JSON. HTTP {response.status_code}: {response.text[:300]}")

    # Last.fm a veces responde con error en el payload
    if "error" in data:
        raise Exception(f"Last.fm API error {data.get('error')}: {data.get('message')}")

    if response.status_code != 200:
        raise Exception(f"HTTP {response.status_code}: {data}")

    recenttracks = data.get("recenttracks", {})
    track = recenttracks.get("track", [])

    if isinstance(track, list) and len(track) > 0:
        latest = track[0]
    elif isinstance(track, dict) and track:
        latest = track
    else:
        return None, False

    nowplaying = latest.get("@attr", {}).get("nowplaying") == "true"

    if nowplaying:
        return datetime.now(timezone.utc), True

    date_info = latest.get("date", {})
    uts = date_info.get("uts")

    if not uts:
        return None, False

    return datetime.fromtimestamp(int(uts), tz=timezone.utc), False

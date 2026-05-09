from datetime import datetime
from flask import request
from config import PLATFORM_RATES


def safe_int(v, d=0):
    try:
        return int(v or 0)
    except Exception:
        return d


def money(v):
    return f"${float(v or 0):,.2f}"


def avg_rate(platform):
    p = (platform or "").strip().lower()
    r = PLATFORM_RATES.get(p, PLATFORM_RATES["spotify"])
    return (r["min"] + r["max"]) / 2


def parse_ts(ts):
    if ts is None:
        return None
    try:
        return datetime.utcfromtimestamp(int(ts))
    except Exception:
        return None


def current_filters():
    month = (request.args.get("month") or "").strip()
    platform = (request.args.get("platform") or "").strip().lower()
    distributor = (request.args.get("distributor") or "").strip()
    return month, platform, distributor


def month_range(month):
    if month:
        start = datetime.strptime(month, "%Y-%m")
    else:
        now = datetime.utcnow()
        start = datetime(now.year, now.month, 1)

    if start.month == 12:
        end = datetime(start.year + 1, 1, 1)
    else:
        end = datetime(start.year, start.month + 1, 1)

    return start, end

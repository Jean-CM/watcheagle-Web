from datetime import datetime, timedelta
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
    date_from = (request.args.get("date_from") or "").strip()
    date_to = (request.args.get("date_to") or "").strip()
    month = (request.args.get("month") or "").strip()
    platform = (request.args.get("platform") or "").strip().lower()
    distributor = (request.args.get("distributor") or "").strip()
    return date_from, date_to, month, platform, distributor


def date_range():
    date_from, date_to, month, _, _ = current_filters()

    if date_from and date_to:
        start = datetime.strptime(date_from, "%Y-%m-%d")
        end = datetime.strptime(date_to, "%Y-%m-%d") + timedelta(days=1)
        return start, end

    if month:
        start = datetime.strptime(month, "%Y-%m")
        if start.month == 12:
            end = datetime(start.year + 1, 1, 1)
        else:
            end = datetime(start.year, start.month + 1, 1)
        return start, end

    today = datetime.utcnow().date()
    start = datetime.combine(today - timedelta(days=30), datetime.min.time())
    end = datetime.combine(today + timedelta(days=1), datetime.min.time())
    return start, end


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

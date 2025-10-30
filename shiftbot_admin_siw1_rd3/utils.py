from __future__ import annotations
from datetime import datetime
import os, math, pytz

TZ = os.getenv("TIMEZONE", "Asia/Tashkent")
_tz = pytz.timezone(TZ)

def now_local() -> datetime:
    return datetime.now(_tz)

def today_local_str() -> str:
    return now_local().strftime("%Y-%m-%d")

def fmt_dt(dt: datetime | None) -> str:
    if not dt:
        return "—"
    return dt.astimezone(_tz).strftime("%d.%m %H:%M")

def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    # Расстояние в метрах между двумя точками lat/lon.
    R = 6371000.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    return R * c

def mm_to_hhmm(minutes: int) -> str:
    h = minutes // 60
    m = minutes % 60
    return f"{h:02d}:{m:02d}"

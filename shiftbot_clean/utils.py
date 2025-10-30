from __future__ import annotations
from datetime import datetime
import os, pytz, math as m

TZ = os.getenv("TIMEZONE", "Asia/Tashkent")
_tz = pytz.timezone(TZ)

def now_local() -> datetime:
    return datetime.now(_tz)

def today_local_str() -> str:
    return now_local().strftime("%Y-%m-%d")

def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371000.0
    phi1, phi2 = m.radians(lat1), m.radians(lat2)
    dphi, dl = m.radians(lat2 - lat1), m.radians(lon2 - lon1)
    a = m.sin(dphi/2)**2 + m.cos(phi1)*m.cos(phi2)*m.sin(dl/2)**2
    return R * 2 * m.atan2(m.sqrt(a), m.sqrt(1-a))

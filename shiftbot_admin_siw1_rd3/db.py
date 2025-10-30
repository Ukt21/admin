from __future__ import annotations
import aiosqlite
from dataclasses import dataclass
from typing import Optional, List, Tuple, Dict

DB_PATH = "./bot.db"

@dataclass
class User:
    id: int
    tg_id: int
    full_name: str
    department: Optional[str]

async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            '''
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tg_id INTEGER UNIQUE NOT NULL,
                full_name TEXT NOT NULL,
                department TEXT
            )
            '''
        )
        await db.execute(
            '''
            CREATE TABLE IF NOT EXISTS shifts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                work_date TEXT NOT NULL,  -- YYYY-MM-DD
                check_in TEXT,            -- ISO ts
                check_out TEXT,           -- ISO ts
                UNIQUE(user_id, work_date),
                FOREIGN KEY(user_id) REFERENCES users(id)
            )
            '''
        )
        await db.commit()

async def get_user_by_tg(tg_id: int) -> Optional[User]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM users WHERE tg_id = ?", (tg_id,))
        row = await cur.fetchone()
        if not row: return None
        return User(id=row["id"], tg_id=row["tg_id"], full_name=row["full_name"], department=row["department"])

async def create_user(tg_id: int, full_name: str) -> User:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT OR IGNORE INTO users(tg_id, full_name) VALUES(?, ?)", (tg_id, full_name))
        await db.commit()
    u = await get_user_by_tg(tg_id)
    assert u is not None
    return u

async def set_user_department(user_id: int, department: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET department = ? WHERE id = ?", (department, user_id))
        await db.commit()

async def get_or_create_shift(user_id: int, work_date: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT OR IGNORE INTO shifts(user_id, work_date) VALUES(?, ?)", (user_id, work_date))
        await db.commit()

async def set_check_in(user_id: int, work_date: str, ts_iso: str) -> Tuple[bool, str]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT check_in FROM shifts WHERE user_id=? AND work_date=?", (user_id, work_date))
        row = await cur.fetchone()
        if row and row["check_in"]:
            return False, "Уже отмечен приход"
        await db.execute("UPDATE shifts SET check_in=? WHERE user_id=? AND work_date=?", (ts_iso, user_id, work_date))
        await db.commit()
        return True, "Ок"

async def set_check_out(user_id: int, work_date: str, ts_iso: str) -> Tuple[bool, str]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT check_in, check_out FROM shifts WHERE user_id=? AND work_date=?", (user_id, work_date))
        row = await cur.fetchone()
        if not row or not row["check_in"]:
            return False, "Сначала отметь приход"
        if row["check_out"]:
            return False, "Уже отмечен уход"
        await db.execute("UPDATE shifts SET check_out=? WHERE user_id=? AND work_date=?", (ts_iso, user_id, work_date))
        await db.commit()
        return True, "Ок"

async def fetch_today_shifts(work_date: str) -> List[aiosqlite.Row]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            '''
            SELECT u.full_name, u.department, s.check_in, s.check_out
            FROM shifts s
            JOIN users u ON u.id = s.user_id
            WHERE s.work_date = ?
            ORDER BY u.department, u.full_name
            ''',
            (work_date,)
        )
        rows = await cur.fetchall()
        return rows

# === Подсчёт часов ===
from dateutil import parser as dtparser

def _minutes_between(check_in_iso: str | None, check_out_iso: str | None) -> int:
    if not check_in_iso or not check_out_iso:
        return 0
    try:
        t1 = dtparser.isoparse(check_in_iso)
        t2 = dtparser.isoparse(check_out_iso)
        delta = t2 - t1
        mins = int(delta.total_seconds() // 60)
        return max(mins, 0)
    except Exception:
        return 0

async def month_minutes_for_user(user_id: int, year: int, month: int) -> int:
    month_prefix = f"{year:04d}-{month:02d}-"
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT check_in, check_out FROM shifts WHERE user_id=? AND work_date LIKE ?",
            (user_id, month_prefix + "%"),
        )
        rows = await cur.fetchall()
    total = 0
    for r in rows:
        total += _minutes_between(r["check_in"], r["check_out"])
    return total

async def month_days_for_user(user_id: int, year: int, month: int) -> List[dict]:
    """Возвращает список {date, minutes} по дням (только закрытые смены)."""
    month_prefix = f"{year:04d}-{month:02d}-"
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT work_date, check_in, check_out FROM shifts WHERE user_id=? AND work_date LIKE ? ORDER BY work_date",
            (user_id, month_prefix + "%"),
        )
        rows = await cur.fetchall()
    out = []
    for r in rows:
        mins = _minutes_between(r["check_in"], r["check_out"])
        if mins > 0:
            out.append({"date": r["work_date"], "minutes": mins})
    return out

async def month_minutes_by_user(year: int, month: int, start_day: int = 1, end_day: int | None = None, department: str | None = None) -> List[dict]:
    """Агрегирует минуты по каждому пользователю за диапазон дней месяца; фильтр по департаменту опционален."""
    month_prefix = f"{year:04d}-{month:02d}-"
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        q = '''
            SELECT u.id AS user_id, u.full_name, u.department, s.work_date, s.check_in, s.check_out
            FROM shifts s
            JOIN users u ON u.id = s.user_id
            WHERE s.work_date LIKE ?
        '''
        params = [month_prefix + "%"]
        if department:
            q += " AND u.department = ?"
            params.append(department)
        q += " ORDER BY u.department, u.full_name, s.work_date"
        cur = await db.execute(q, params)
        rows = await cur.fetchall()

    # Границы по дням
    def _in_range(work_date: str) -> bool:
        day = int(work_date.split('-')[2])
        if end_day is None:
            return day >= start_day
        return start_day <= day <= end_day

    totals: Dict[int, dict] = {}
    for r in rows:
        if not _in_range(r["work_date"]):
            continue
        mins = _minutes_between(r["check_in"], r["check_out"])
        if mins <= 0:
            continue
        uid = r["user_id"]
        if uid not in totals:
            totals[uid] = {"user_id": uid, "full_name": r["full_name"], "department": r["department"], "minutes": 0}
        totals[uid]["minutes"] += mins

    return list(totals.values())

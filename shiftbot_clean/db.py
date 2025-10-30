from __future__ import annotations
import aiosqlite
from dataclasses import dataclass
from typing import Optional, List, Tuple
from dateutil import parser as dtparser

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
            "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY AUTOINCREMENT, tg_id INTEGER UNIQUE NOT NULL, full_name TEXT NOT NULL, department TEXT)"
        )
        await db.execute(
            "CREATE TABLE IF NOT EXISTS shifts (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, work_date TEXT NOT NULL, check_in TEXT, check_out TEXT, UNIQUE(user_id, work_date), FOREIGN KEY(user_id) REFERENCES users(id))"
        )
        await db.commit()

async def get_user_by_tg(tg_id: int) -> Optional[User]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM users WHERE tg_id = ?", (tg_id,))
        r = await cur.fetchone()
        if not r:
            return None
        return User(id=r["id"], tg_id=r["tg_id"], full_name=r["full_name"], department=r["department"])

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
            "SELECT u.full_name, u.department, s.check_in, s.check_out FROM shifts s JOIN users u ON u.id = s.user_id WHERE s.work_date = ? ORDER BY u.department, u.full_name",
            (work_date,),
        )
        return await cur.fetchall()

def _minutes_between(ci: str | None, co: str | None) -> int:
    if not ci or not co:
        return 0
    try:
        t1 = dtparser.isoparse(ci)
        t2 = dtparser.isoparse(co)
        return max(int((t2 - t1).total_seconds() // 60), 0)
    except Exception:
        return 0

async def month_minutes_for_user(user_id: int, y: int, m: int) -> int:
    pref = f"{y:04d}-{m:02d}-"
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT check_in, check_out FROM shifts WHERE user_id=? AND work_date LIKE ?", (user_id, pref + "%"))
        rows = await cur.fetchall()
    return sum(_minutes_between(r["check_in"], r["check_out"]) for r in rows)

async def month_days_for_user(user_id: int, y: int, m: int):
    pref = f"{y:04d}-{m:02d}-"
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT work_date, check_in, check_out FROM shifts WHERE user_id=? AND work_date LIKE ? ORDER BY work_date", (user_id, pref + "%"))
        rows = await cur.fetchall()
    out = []
    for r in rows:
        mins = _minutes_between(r["check_in"], r["check_out"])
        if mins > 0:
            out.append({"date": r["work_date"], "minutes": mins})
    return out
    import aiosqlite
from dateutil import parser as dtparser

async def range_days_for_user(user_id: int, from_date: str, to_date: str):
    """
    Возвращает список смен с минутами между двумя датами включительно.
    from_date / to_date — строки 'YYYY-MM-DD'
    """
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        q = """
            SELECT work_date, check_in, check_out
            FROM shifts
            WHERE user_id = ?
              AND work_date BETWEEN ? AND ?
            ORDER BY work_date
        """
        cur = await db.execute(q, (user_id, from_date, to_date))
        rows = await cur.fetchall()

    results = []
    for r in rows:
        ci, co = r["check_in"], r["check_out"]
        if not ci or not co:
            continue
        try:
            t1 = dtparser.isoparse(ci)
            t2 = dtparser.isoparse(co)
            mins = max(int((t2 - t1).total_seconds() // 60), 0)
        except Exception:
            mins = 0
        results.append({"date": r["work_date"], "minutes": mins})
    return results


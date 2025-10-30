# db.py — устойчивое хранилище для ShiftBot
import os
from datetime import datetime
from typing import Optional, List, Dict

import aiosqlite

# ---- Путь к БД (на Render укажи DB_PATH=/var/data/bot.db) ----
DB_PATH = os.getenv("DB_PATH", "./bot.db")


# =============== ИНИЦИАЛИЗАЦИЯ ===============
async def init_db() -> None:
    """Создаёт таблицы и включает устойчивые режимы SQLite."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA journal_mode=WAL;")
        await db.execute("PRAGMA synchronous=NORMAL;")
        await db.execute("PRAGMA foreign_keys=ON;")

        await db.execute("""
        CREATE TABLE IF NOT EXISTS users(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tg_id INTEGER UNIQUE NOT NULL,
            full_name TEXT NOT NULL,
            department TEXT
        )""")

        await db.execute("""
        CREATE TABLE IF NOT EXISTS shifts(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            work_date TEXT NOT NULL,            -- YYYY-MM-DD (локальная дата)
            check_in  TEXT,                     -- ISO время прихода
            check_out TEXT,                     -- ISO время ухода
            UNIQUE(user_id, work_date),
            FOREIGN KEY(user_id) REFERENCES users(id)
        )""")

        # Журнал событий (для восстановления и аудита)
        await db.execute("""
        CREATE TABLE IF NOT EXISTS events(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            work_date TEXT NOT NULL,
            kind TEXT NOT NULL,                 -- 'check_in' | 'check_out'
            ts_iso TEXT NOT NULL,
            lat REAL, lon REAL,
            meta TEXT,
            FOREIGN KEY(user_id) REFERENCES users(id)
        )""")

        await db.commit()


# =============== USERS ===============
async def get_user_by_tg(tg_id: int) -> Optional[aiosqlite.Row]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM users WHERE tg_id=?", (tg_id,))
        return await cur.fetchone()


async def create_user(tg_id: int, full_name: str) -> aiosqlite.Row:
    """Создаёт пользователя (или обновляет ФИО, если уже существует). Возвращает запись users."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR IGNORE INTO users(tg_id, full_name) VALUES(?, ?)",
            (tg_id, full_name)
        )
        # если запись была, обновим имя (кейс: сотрудник поправил написание)
        await db.execute(
            "UPDATE users SET full_name=? WHERE tg_id=?",
            (full_name, tg_id)
        )
        await db.commit()
    u = await get_user_by_tg(tg_id)
    assert u is not None
    return u


async def set_user_department(user_id: int, department: str) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET department=? WHERE id=?", (department, user_id))
        await db.commit()


# =============== SHIFTS ===============
async def get_or_create_shift(user_id: int, work_date: str) -> aiosqlite.Row:
    """Гарантирует наличие строки смены на дату. Возвращает запись."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        await db.execute(
            "INSERT OR IGNORE INTO shifts(user_id, work_date) VALUES(?, ?)",
            (user_id, work_date)
        )
        await db.commit()
        cur = await db.execute(
            "SELECT * FROM shifts WHERE user_id=? AND work_date=?",
            (user_id, work_date)
        )
        return await cur.fetchone()


async def set_check_in(user_id: int, work_date: str, ts_iso: str) -> (bool, str):
    """Отмечает приход. Возвращает (успех, сообщение)."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT check_in FROM shifts WHERE user_id=? AND work_date=?",
            (user_id, work_date)
        )
        row = await cur.fetchone()
        if row and row["check_in"]:
            return False, "Приход уже отмечен."
        await db.execute(
            "UPDATE shifts SET check_in=? WHERE user_id=? AND work_date=?",
            (ts_iso, user_id, work_date)
        )
        await db.commit()
        return True, "OK"


async def set_check_out(user_id: int, work_date: str, ts_iso: str) -> (bool, str):
    """Отмечает уход. Возвращает (успех, сообщение)."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT check_in, check_out FROM shifts WHERE user_id=? AND work_date=?",
            (user_id, work_date)
        )
        row = await cur.fetchone()
        if not row or not row["check_in"]:
            return False, "Сначала отметьте приход."
        if row["check_out"]:
            return False, "Уход уже отмечен."
        await db.execute(
            "UPDATE shifts SET check_out=? WHERE user_id=? AND work_date=?",
            (ts_iso, user_id, work_date)
        )
        await db.commit()
        return True, "OK"


async def fetch_today_shifts(date_str: str) -> List[aiosqlite.Row]:
    """Смены за дату с данными сотрудников (для отчёта менеджеру)."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("""
            SELECT s.*, u.full_name, u.department
            FROM shifts s
            JOIN users u ON u.id = s.user_id
            WHERE s.work_date = ?
            ORDER BY u.department, u.full_name
        """, (date_str,))
        return await cur.fetchall()


# =============== АУДИТ / ЖУРНАЛ ===============
async def log_event(user_id: int, work_date: str, kind: str,
                    ts_iso: str, lat: Optional[float] = None,
                    lon: Optional[float] = None, meta: Optional[str] = None) -> None:
    """Пишет событие (приход/уход) в журнал."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO events(user_id, work_date, kind, ts_iso, lat, lon, meta)
            VALUES(?,?,?,?,?,?,?)
        """, (user_id, work_date, kind, ts_iso, lat, lon, meta))
        await db.commit()


# =============== АГРЕГАЦИИ ДЛЯ ОТЧЁТОВ ===============
def _month_bounds(year: int, month: int) -> (str, str):
    start = f"{year}-{month:02d}-01"
    if month < 12:
        end = f"{year}-{month+1:02d}-01"
    else:
        end = f"{year+1}-01-01"
    return start, end


async def month_minutes_for_user(user_id: int, year: int, month: int) -> int:
    """Сумма минут за месяц по сотруднику."""
    start, end = _month_bounds(year, month)
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("""
            SELECT check_in, check_out
            FROM shifts
            WHERE user_id=? AND work_date>=? AND work_date<?
        """, (user_id, start, end))
        total = 0
        for r in await cur.fetchall():
            if r["check_in"] and r["check_out"]:
                dt_in = datetime.fromisoformat(r["check_in"])
                dt_out = datetime.fromisoformat(r["check_out"])
                total += max(int((dt_out - dt_in).total_seconds() // 60), 0)
        return total


async def month_days_for_user(user_id: int, year: int, month: int) -> List[Dict]:
    """
    Возвращает построчно смены сотрудника за месяц:
    [{"date": "YYYY-MM-DD", "minutes": int}, ...]
    """
    start, end = _month_bounds(year, month)
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("""
            SELECT work_date, check_in, check_out
            FROM shifts
            WHERE user_id=? AND work_date>=? AND work_date<?
            ORDER BY work_date
        """, (user_id, start, end))
        rows = await cur.fetchall()

    result: List[Dict] = []
    for r in rows:
        ci, co = r["check_in"], r["check_out"]
        if not ci or not co:
            continue
        dt_in = datetime.fromisoformat(ci)
        dt_out = datetime.fromisoformat(co)
        mins = max(int((dt_out - dt_in).total_seconds() // 60), 0)
        result.append({"date": r["work_date"], "minutes": mins})
    return result


async def range_days_for_user(user_id: int, from_date: str, to_date: str) -> List[Dict]:
    """
    Смены за произвольный диапазон (включительно):
    [{"date": "YYYY-MM-DD", "minutes": int}, ...]
    """
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("""
            SELECT work_date, check_in, check_out
            FROM shifts
            WHERE user_id=? AND work_date BETWEEN ? AND ?
            ORDER BY work_date
        """, (user_id, from_date, to_date))
        rows = await cur.fetchall()

    result: List[Dict] = []
    for r in rows:
        ci, co = r["check_in"], r["check_out"]
        if not ci or not co:
            continue
        dt_in = datetime.fromisoformat(ci)
        dt_out = datetime.fromisoformat(co)
        mins = max(int((dt_out - dt_in).total_seconds() // 60), 0)
        result.append({"date": r["work_date"], "minutes": mins})
    return result


async def month_minutes_by_user(year: int, month: int,
                                start_day: int = 1,
                                end_day: Optional[int] = None,
                                department: Optional[str] = None) -> List[Dict]:
    """
    Суммарные минуты по всем сотрудникам за часть месяца.
    Возвращает:
    [{"user_id": int, "full_name": str, "department": str|None, "minutes": int}, ...]
    """
    start = f"{year}-{month:02d}-{start_day:02d}"
    if end_day is None:
        _, end = _month_bounds(year, month)  # до первого числа след. месяца (не включительно)
    else:
        # включительно конечный день → делаем < next_day
        # но чтобы не усложнять, возьмём <= через BETWEEN: тогда ниже используем BETWEEN
        end = f"{year}-{month:02d}-{end_day:02d}"

    where_dept = "AND u.department = ?" if department else ""
    # Если end_day задан, используем BETWEEN, иначе полуинтервал [start, end)
    if end_day is None:
        sql_period = "s.work_date >= ? AND s.work_date < ?"
        params = [start, end]
    else:
        sql_period = "s.work_date BETWEEN ? AND ?"
        params = [start, end]

    if department:
        params.append(department)

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(f"""
            SELECT s.user_id, u.full_name, u.department, s.work_date, s.check_in, s.check_out
            FROM shifts s
            JOIN users u ON u.id = s.user_id
            WHERE {sql_period}
            {where_dept}
            ORDER BY s.user_id, s.work_date
        """, params)
        rows = await cur.fetchall()

    agg: Dict[int, Dict] = {}
    for r in rows:
        ci, co = r["check_in"], r["check_out"]
        if not ci or not co:
            continue
        mins = max(int((datetime.fromisoformat(co) - datetime.fromisoformat(ci)).total_seconds() // 60), 0)
        item = agg.setdefault(
            r["user_id"],
            {"user_id": r["user_id"], "full_name": r["full_name"], "department": r["department"], "minutes": 0}
        )
        item["minutes"] += mins

    return list(agg.values())

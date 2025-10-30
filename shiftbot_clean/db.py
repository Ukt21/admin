# Обновлённый db.py с постоянным хранением и фиксацией пользователя по tg_id
import os, aiosqlite
from datetime import datetime

DB_PATH = os.getenv("DB_PATH", "./bot.db")

async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA journal_mode=WAL;")
        await db.execute("PRAGMA synchronous=NORMAL;")
        await db.execute("PRAGMA foreign_keys=ON;")
        await db.execute('''CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tg_id INTEGER UNIQUE NOT NULL,
            full_name TEXT NOT NULL,
            department TEXT
        )''')
        await db.execute('''CREATE TABLE IF NOT EXISTS shifts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            work_date TEXT NOT NULL,
            check_in TEXT,
            check_out TEXT,
            UNIQUE(user_id, work_date),
            FOREIGN KEY(user_id) REFERENCES users(id)
        )''')
        await db.commit()

async def get_user_by_tg(tg_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM users WHERE tg_id=?", (tg_id,))
        return await cur.fetchone()

async def create_user(tg_id: int, full_name: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT OR IGNORE INTO users(tg_id, full_name) VALUES(?,?)", (tg_id, full_name))
        await db.execute("UPDATE users SET full_name=? WHERE tg_id=?", (full_name, tg_id))
        await db.commit()
    return await get_user_by_tg(tg_id)

async def set_user_department(user_id: int, department: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET department=? WHERE id=?", (department, user_id))
        await db.commit()

async def get_or_create_shift(user_id: int, work_date: str):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        await db.execute("INSERT OR IGNORE INTO shifts(user_id, work_date) VALUES(?,?)", (user_id, work_date))
        await db.commit()
        cur = await db.execute("SELECT * FROM shifts WHERE user_id=? AND work_date=?", (user_id, work_date))
        return await cur.fetchone()

async def set_check_in(user_id: int, work_date: str, ts: str):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT check_in FROM shifts WHERE user_id=? AND work_date=?", (user_id, work_date))
        row = await cur.fetchone()
        if row and row[0]:
            return False, "Приход уже отмечен."
        await db.execute("UPDATE shifts SET check_in=? WHERE user_id=? AND work_date=?", (ts, user_id, work_date))
        await db.commit()
        return True, "OK"

async def set_check_out(user_id: int, work_date: str, ts: str):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT check_out FROM shifts WHERE user_id=? AND work_date=?", (user_id, work_date))
        row = await cur.fetchone()
        if row and row[0]:
            return False, "Уход уже отмечен."
        await db.execute("UPDATE shifts SET check_out=? WHERE user_id=? AND work_date=?", (ts, user_id, work_date))
        await db.commit()
        return True, "OK"

async def fetch_today_shifts(date_str: str):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute('''SELECT s.*, u.full_name, u.department
                                  FROM shifts s JOIN users u ON u.id = s.user_id
                                  WHERE s.work_date=?''', (date_str,))
        return await cur.fetchall()

async def month_minutes_for_user(user_id: int, year: int, month: int):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        start = f"{year}-{month:02d}-01"
        end = f"{year}-{month+1:02d}-01" if month < 12 else f"{year+1}-01-01"
        cur = await db.execute('''SELECT check_in, check_out FROM shifts
                                  WHERE user_id=? AND work_date>=? AND work_date<?''', (user_id, start, end))
        total = 0
        for r in await cur.fetchall():
            if r[0] and r[1]:
                dt_in = datetime.fromisoformat(r[0])
                dt_out = datetime.fromisoformat(r[1])
                total += int((dt_out - dt_in).total_seconds() // 60)
        return total

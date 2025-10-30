from __future__ import annotations
import os
from aiogram import Bot, Dispatcher, F, Router
from aiogram.types import Message, CallbackQuery
from aiogram.enums import ParseMode
from aiogram.client.bot import DefaultBotProperties
from aiogram.filters import Command
from aiogram.fsm.storage.memory import MemoryStorage
from dotenv import load_dotenv
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from db import init_db, get_user_by_tg, create_user, set_user_department, get_or_create_shift, set_check_in, set_check_out, fetch_today_shifts, month_minutes_for_user, month_days_for_user
from keyboards import departments_kb, staff_menu, request_location_kb
from utils import now_local, today_local_str, haversine_m

load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
MANAGERS_CHAT_ID = int(os.getenv("MANAGERS_CHAT_ID", "0"))
TIMEZONE = os.getenv("TIMEZONE", "Asia/Tashkent")
REPORT_HOUR = int(os.getenv("REPORT_HOUR", "23"))
REPORT_MINUTE = int(os.getenv("REPORT_MINUTE", "30"))
DEPARTMENTS = [d.strip() for d in os.getenv("DEPARTMENTS", "Зал,Кухня,Бар,Хостес,Пицца,Технички").split(",") if d.strip()]
PLACE_LAT = float(os.getenv("PLACE_LAT", "0"))
PLACE_LON = float(os.getenv("PLACE_LON", "0"))
RADIUS_METERS = float(os.getenv("RADIUS_METERS", "150"))

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is not set")

bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())
rt = Router()

PENDING_ACTION: dict[int, str] = {}

async def manager_notify(text: str):
    if MANAGERS_CHAT_ID != 0:
        await bot.send_message(MANAGERS_CHAT_ID, text, disable_web_page_preview=True)

@rt.message(Command("help"))
async def cmd_help(m: Message):
    await m.answer(
        "👋 <b>ShiftBot — помощь</b>"
        "• /start — регистрация и подразделение"
        "• /menu — показать кнопки"
        "• /myhours [YYYY-MM] — мои часы"
        "• /mydays [YYYY-MM] — мои дни и часы"
        "• /report — отчёт за сегодня"
    )

@rt.message(Command("start"))
async def cmd_start(m: Message):
    await init_db()
    u = await get_user_by_tg(m.from_user.id)
    if u is None:
        u = await create_user(m.from_user.id, m.from_user.full_name or "Без имени")
    if not u.department:
        return await m.answer("🧩 Выбери своё подразделение:", reply_markup=departments_kb(DEPARTMENTS))
    await m.answer("✅ Готово. Используй меню ниже.", reply_markup=staff_menu("idle"))

@rt.callback_query(F.data.startswith("dept:"))
async def cb_dept(cq: CallbackQuery):
    u = await get_user_by_tg(cq.from_user.id)
    if u is None:
        u = await create_user(cq.from_user.id, cq.from_user.full_name or "Без имени")
    dept = cq.data.split(":", 1)[1]
    await set_user_department(u.id, dept)
    await cq.message.edit_text(f"✅ Подразделение: <b>{dept}</b>")
    await cq.message.answer("Готово. Используй меню ниже.", reply_markup=staff_menu("idle"))

@rt.message(Command("menu"))
async def cmd_menu(m: Message):
    await init_db()
    u = await get_user_by_tg(m.from_user.id)
    if u is None or not u.department:
        return await m.answer("Сначала /start.")
    rows = await fetch_today_shifts(today_local_str())
    status = "idle"
    for r in rows:
        if r["full_name"] == u.full_name:
            if r["check_in"] and not r["check_out"]:
                status = "checked_in"
            elif r["check_out"]:
                status = "checked_out"
            break
    await m.answer("📋 Меню", reply_markup=staff_menu(status))

@rt.message(F.text.startswith("🟢 Пришёл"))
async def ask_in(m: Message):
    await init_db()
    u = await get_user_by_tg(m.from_user.id)
    if u is None or not u.department:
        return await m.answer("Сначала /start.")
    PENDING_ACTION[m.from_user.id] = "in"
    await m.answer("📍 Отправь геолокацию — подтвердим приход.", reply_markup=request_location_kb())

@rt.message(F.text.startswith("🔴 Ушёл"))
async def ask_out(m: Message):
    await init_db()
    u = await get_user_by_tg(m.from_user.id)
    if u is None or not u.department:
        return await m.answer("Сначала /start.")
    PENDING_ACTION[m.from_user.id] = "out"
    await m.answer("📍 Отправь геолокацию — подтвердим уход.", reply_markup=request_location_kb())

def inside(lat: float, lon: float) -> tuple[bool, int]:
    d = int(haversine_m(lat, lon, PLACE_LAT, PLACE_LON))
    return (d <= RADIUS_METERS, d)

@rt.message(F.location)
async def on_loc(m: Message):
    await init_db()
    u = await get_user_by_tg(m.from_user.id)
    if u is None or not u.department:
        return await m.answer("Сначала /start.")
    act = PENDING_ACTION.get(m.from_user.id)
    if act not in ("in", "out"):
        return await m.answer("Сначала нажми кнопку в /menu.")
    ok, dist = inside(m.location.latitude, m.location.longitude)
    if not ok:
        await m.answer(f"❌ Вне геозоны (~{dist} м). Обратись к менеджеру.", reply_markup=staff_menu("idle"))
        await manager_notify(f"⚠️ {u.full_name} ({u.department}) попытка {('прихода' if act=='in' else 'ухода')} вне зоны (~{dist} м).")
        return
    wd = today_local_str()
    await get_or_create_shift(u.id, wd)
    ts = now_local().isoformat()
    if act == "in":
        ok, _ = await set_check_in(u.id, wd, ts)
        if not ok:
            return await m.answer("ℹ️ Приход уже отмечен сегодня.", reply_markup=staff_menu("checked_in"))
        await m.answer("✅ Приход отмечен. Удачной смены!", reply_markup=staff_menu("checked_in"))
        await manager_notify(f"🟢 {u.full_name} ({u.department}) пришёл в {now_local().strftime('%H:%M')} (в радиусе, {dist} м).")
    else:
        ok, msg = await set_check_out(u.id, wd, ts)
        if not ok:
            return await m.answer(msg, reply_markup=staff_menu("checked_in"))
        await m.answer("✅ Уход отмечен. Хорошего отдыха!", reply_markup=staff_menu("checked_out"))
        await manager_notify(f"🔴 {u.full_name} ({u.department}) ушёл в {now_local().strftime('%H:%M')} (в радиусе, {dist} м).")
    PENDING_ACTION.pop(m.from_user.id, None)

async def report_text() -> str:
    rows = await fetch_today_shifts(today_local_str())
    if not rows:
        return "Сегодня отметок нет."
    by = {}
    for r in rows:
        by.setdefault(r["department"] or "Без подразделения", []).append(r)
    parts = [f"📊 Отчёт за {today_local_str()}\n"]
    for d, arr in by.items():
        parts.append(f"<b>{d}</b>")
        for r in arr:
            ci = r["check_in"][11:16] if r["check_in"] else "—"
            co = r["check_out"][11:16] if r["check_out"] else "⏳"
            parts.append(f"• {r['full_name']}: {ci} — {co}")
        parts.append("")
    return "\n".join(parts).strip()

@rt.message(Command("report"))
async def cmd_report(m: Message):
    await m.answer(await report_text())

@rt.message(Command("myhours"))
async def cmd_myhours(m: Message):
    await init_db()
    u = await get_user_by_tg(m.from_user.id)
    if u is None:
        return await m.answer("Сначала /start.")
    args = m.text.split(maxsplit=1)
    if len(args) == 2:
        try:
            y, mon = map(int, args[1].split("-"))
        except Exception:
            return await m.answer("Формат: /myhours 2025-10")
    else:
        n = now_local()
        y, mon = n.year, n.month
    mins = await month_minutes_for_user(u.id, y, mon)
    await m.answer(f"⏱ Твои часы за {y}-{mon:02d}: <b>{mins//60} ч {mins%60:02d} мин</b>")

@rt.message(Command("mydays"))
async def cmd_mydays(m: Message):
    await init_db()
    u = await get_user_by_tg(m.from_user.id)
    if u is None:
        return await m.answer("Сначала /start.")
    args = m.text.split(maxsplit=1)
    if len(args) == 2:
        try:
            y, mon = map(int, args[1].split("-"))
        except Exception:
            return await m.answer("Формат: /mydays 2025-10")
    else:
        n = now_local()
        y, mon = n.year, n.month
    days = await month_days_for_user(u.id, y, mon)
    if not days:
        return await m.answer("Нет закрытых смен за период.")
    lines = [f"📅 Мои дни за {y}-{mon:02d}"]
    for d in days:
        lines.append(f"{d['date']}: {d['minutes']//60} ч {d['minutes']%60:02d} мин")
    await m.answer("\n".join(lines))

async def scheduler_setup(sched: AsyncIOScheduler):
    async def _send():
        await bot.send_message(MANAGERS_CHAT_ID, await report_text())
    sched.add_job(lambda: bot.loop.create_task(_send()), CronTrigger(hour=REPORT_HOUR, minute=REPORT_MINUTE, timezone=TIMEZONE), id="daily_report", replace_existing=True)

async def main():
    await init_db()
    dp.include_router(rt)
    sched = AsyncIOScheduler()
    await scheduler_setup(sched)
    sched.start()
    await dp.start_polling(bot)

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())

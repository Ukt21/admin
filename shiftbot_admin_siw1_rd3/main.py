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

from db import init_db, get_user_by_tg, create_user, set_user_department, get_or_create_shift, set_check_in, set_check_out, fetch_today_shifts, month_minutes_for_user, month_days_for_user, month_minutes_by_user
from keyboards import departments_kb, main_menu, location_menu
from utils import now_local, today_local_str, haversine_m, mm_to_hhmm

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

ADMIN_IDS = set(int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit())

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is not set")

bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())
rt = Router()

PENDING_ACTION: dict[int, str] = {}

def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS or (MANAGERS_CHAT_ID == user_id)  # простая проверка

async def manager_notify(text: str):
    if MANAGERS_CHAT_ID != 0:
        await bot.send_message(MANAGERS_CHAT_ID, text, disable_web_page_preview=True)

@rt.message(Command("start"))
async def cmd_start(m: Message):
    await init_db()
    user = await get_user_by_tg(m.from_user.id)
    if user is None:
        user = await create_user(m.from_user.id, m.from_user.full_name or "Без имени")
    if not user.department:
        await m.answer("Привет! Выбери своё подразделение для регистрации:", reply_markup=departments_kb(DEPARTMENTS))
        return
    await m.answer("Готово! Отмечай смены кнопками ниже.", reply_markup=main_menu())

@rt.callback_query(F.data.startswith("dept:"))
async def cb_set_dept(cq: CallbackQuery):
    dept = cq.data.split(":", 1)[1]
    user = await get_user_by_tg(cq.from_user.id)
    if user is None:
        user = await create_user(cq.from_user.id, cq.from_user.full_name or "Без имени")
    await set_user_department(user.id, dept)
    await cq.message.edit_text(f"Подразделение установлено: <b>{dept}</b> ✅")
    await cq.message.answer("Теперь можешь отмечать приход/уход.", reply_markup=main_menu())

@rt.message(F.text.casefold() == "пришёл")
@rt.message(F.text.casefold() == "пришел")
async def ask_location_in(m: Message):
    await init_db()
    user = await get_user_by_tg(m.from_user.id)
    if user is None or not user.department:
        return await m.answer("Сначала /start и выбери подразделение.")
    PENDING_ACTION[m.from_user.id] = "in"
    await m.answer("Отправь геолокацию для отметки прихода.", reply_markup=location_menu())

@rt.message(F.text.casefold() == "ушёл")
@rt.message(F.text.casefold() == "ушел")
async def ask_location_out(m: Message):
    await init_db()
    user = await get_user_by_tg(m.from_user.id)
    if user is None or not user.department:
        return await m.answer("Сначала /start и выбери подразделение.")
    PENDING_ACTION[m.from_user.id] = "out"
    await m.answer("Отправь геолокацию для отметки ухода.", reply_markup=location_menu())

def _inside_geofence(lat: float, lon: float) -> tuple[bool, float]:
    dist = haversine_m(lat, lon, PLACE_LAT, PLACE_LON)
    return (dist <= RADIUS_METERS), dist

@rt.message(F.location)
async def on_location(m: Message):
    await init_db()
    user = await get_user_by_tg(m.from_user.id)
    if user is None or not user.department:
        return await m.answer("Сначала /start и выбери подразделение.")
    act = PENDING_ACTION.get(m.from_user.id)
    if act not in ("in", "out"):
        return await m.answer("Сначала нажми «Пришёл» или «Ушёл».")

    lat = m.location.latitude
    lon = m.location.longitude
    ok, dist = _inside_geofence(lat, lon)
    if not ok:
        await m.answer(f"❌ Вне геозоны (расстояние ~{int(dist)} м). Обратись к менеджеру.")
        await manager_notify(f"⚠️ <b>{user.full_name}</b> ({user.department}) попытка отметки {('прихода' if act=='in' else 'ухода')} вне геозоны (~{int(dist)} м).")
        return

    work_date = today_local_str()
    await get_or_create_shift(user.id, work_date)
    ts = now_local().isoformat()

    if act == "in":
        success, msg = await set_check_in(user.id, work_date, ts)
        if not success:
            return await m.answer("Приход уже отмечен сегодня.")
        await m.answer("Приход с геолокацией отмечен ✅")
        await manager_notify(f"🟢 <b>{user.full_name}</b> ({user.department}) пришёл в {now_local().strftime('%H:%M')} (в радиусе, {int(dist)} м).")
    else:
        success, msg = await set_check_out(user.id, work_date, ts)
        if not success:
            return await m.answer(msg)
        await m.answer("Уход с геолокацией отмечен ✅")
        await manager_notify(f"🔴 <b>{user.full_name}</b> ({user.department}) ушёл в {now_local().strftime('%H:%M')} (в радиусе, {int(dist)} м).")

    PENDING_ACTION.pop(m.from_user.id, None)

async def build_report_text() -> str:
    rows = await fetch_today_shifts(today_local_str())
    if not rows:
        return "Сегодня отметок нет."
    by_dept = {}
    for r in rows:
        by_dept.setdefault(r["department"] or "Без подразделения", []).append(r)
    parts = [f"📊 Отчёт за {today_local_str()}\n"]
    for dept, items in by_dept.items():
        parts.append(f"<b>{dept}</b>")
        for r in items:
            fi = r["full_name"]
            ci = r["check_in"][11:16] if r["check_in"] else "—"
            co = r["check_out"][11:16] if r["check_out"] else "⏳"
            parts.append(f"• {fi}: {ci} — {co}")
        parts.append("")
    return "\n".join(parts).strip()

@rt.message(Command("report"))
async def cmd_report(m: Message):
    text = await build_report_text()
    await m.answer(text)

@rt.message(Command("myhours"))
async def cmd_myhours(m: Message):
    await init_db()
    u = await get_user_by_tg(m.from_user.id)
    if u is None:
        return await m.answer("Сначала /start.")
    args = m.text.split(maxsplit=1)
    if len(args) == 2:
        try:
            year, month = args[1].split("-")
            year = int(year); month = int(month)
        except Exception:
            return await m.answer("Формат: /myhours 2025-10")
    else:
        from utils import now_local
        now = now_local()
        year, month = now.year, now.month

    minutes = await month_minutes_for_user(u.id, year, month)
    await m.answer(f"⏱ Твои часы за {year}-{month:02d}: <b>{minutes//60} ч {minutes%60} мин</b>")

@rt.message(Command("mydays"))
async def cmd_mydays(m: Message):
    await init_db()
    u = await get_user_by_tg(m.from_user.id)
    if u is None:
        return await m.answer("Сначала /start.")
    args = m.text.split(maxsplit=1)
    if len(args) == 2:
        try:
            year, month = args[1].split("-")
            year = int(year); month = int(month)
        except Exception:
            return await m.answer("Формат: /mydays 2025-10")
    else:
        from utils import now_local
        now = now_local()
        year, month = now.year, now.month

    days = await month_days_for_user(u.id, year, month)
    if not days:
        return await m.answer("Нет закрытых смен за период.")
    lines = [f"📅 Твои дни за {year}-{month:02d}"]
    for d in days:
        lines.append(f"{d['date']}: {d['minutes']//60} ч {d['minutes']%60:02d} мин")
    await m.answer("\n".join(lines))

@rt.message(Command("hours_sum"))  # /hours_sum 2025-10 [part] [dept]
async def cmd_hours_sum(m: Message):
    await init_db()
    if not is_admin(m.from_user.id):
        return await m.answer("Команда доступна только администраторам.")
    args = m.text.split()
    if len(args) < 2:
        return await m.answer("Формат: /hours_sum YYYY-MM [part] [dept]\npart: 1 (1–15) | 2 (16–конец)")
    try:
        year, month = map(int, args[1].split("-"))
    except Exception:
        return await m.answer("Формат: /hours_sum YYYY-MM [part] [dept]")
    part = None
    dept = None
    if len(args) >= 3 and args[2] in ("1","2"):
        part = int(args[2])
    if len(args) >= 4:
        dept = " ".join(args[3:])

    # диапазоны
    if part == 1:
        start_day, end_day = 1, 15
    elif part == 2:
        start_day, end_day = 16, None
    else:
        start_day, end_day = 1, None

    items = await month_minutes_by_user(year, month, start_day=start_day, end_day=end_day, department=dept)
    if not items:
        return await m.answer("Нет данных за указанный период.")

    # агрегируем по департаменту
    by_dept = {}
    total_all = 0
    for r in items:
        d = r["department"] or "Без подразделения"
        by_dept.setdefault(d, []).append(r)
        total_all += r["minutes"]

    lines = [f"📊 Сумма часов {year}-{month:02d} " + (f"(часть {part}) " if part else "") + (f"dept: {dept}" if dept else ""), ""]
    for d, arr in by_dept.items():
        dept_total = sum(x["minutes"] for x in arr)
        lines.append(f"— <b>{d}</b>: {dept_total//60} ч {dept_total%60:02d} мин")
        for x in arr:
            lines.append(f"   • {x['full_name']}: {x['minutes']//60} ч {x['minutes']%60:02d} мин")
        lines.append("")
    lines.append(f"ИТОГО: <b>{total_all//60} ч {total_all%60:02d} мин</b>")
    await m.answer("\n".join(lines))

async def scheduler_setup(sched: AsyncIOScheduler):
    async def _send_report():
        text = await build_report_text()
        await bot.send_message(MANAGERS_CHAT_ID, text)

    sched.add_job(
        func=lambda: bot.loop.create_task(_send_report()),
        trigger=CronTrigger(hour=REPORT_HOUR, minute=REPORT_MINUTE, timezone=TIMEZONE),
        id="daily_report",
        replace_existing=True,
    )

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

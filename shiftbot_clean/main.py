from __future__ import annotations

import os, io, csv, zipfile, pathlib, asyncio
from datetime import date, timedelta

from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.bot import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    Message, CallbackQuery,
    ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove,
    InlineKeyboardMarkup, InlineKeyboardButton
)
from aiogram.exceptions import TelegramBadRequest

from dotenv import load_dotenv
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from db import (
    init_db, get_user_by_tg, create_user, set_user_department,
    get_or_create_shift, set_check_in, set_check_out,
    fetch_today_shifts, month_minutes_for_user, month_days_for_user,
    range_days_for_user, month_minutes_by_user, log_event
)
from utils import now_local, today_local_str, haversine_m

# ================== ENV ==================
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is not set")

MANAGERS_CHAT_ID = int(os.getenv("MANAGERS_CHAT_ID", "0"))
TIMEZONE = os.getenv("TIMEZONE", "Asia/Tashkent")
REPORT_HOUR = int(os.getenv("REPORT_HOUR", "23"))
REPORT_MINUTE = int(os.getenv("REPORT_MINUTE", "30"))
DEPARTMENTS = [d.strip() for d in os.getenv("DEPARTMENTS", "Зал,Кухня,Бар,Хостес,Пицца,Технички").split(",") if d.strip()]

PLACE_LAT = float(os.getenv("PLACE_LAT", "0"))
PLACE_LON = float(os.getenv("PLACE_LON", "0"))
RADIUS_METERS = float(os.getenv("RADIUS_METERS", "150"))

ADMIN_IDS = set(int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit())

# ================== BOT CORE ==================
bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())
rt = Router()

# runtime state
PENDING_ACTION: dict[int, str] = {}      # {user_id: "in"|"out"}
AWAIT_FULLNAME: set[int] = set()         # кто сейчас вводит ФИО
RANGE_PICK: dict[int, dict] = {}         # выбор периода

# ================== KEYBOARDS ==================
def staff_menu(status: str) -> ReplyKeyboardMarkup | ReplyKeyboardRemove:
    if status == "idle":
        kb = [[KeyboardButton(text="🟢 Пришёл")]]
    elif status == "checked_in":
        kb = [[KeyboardButton(text="🔴 Ушёл")]]
    else:
        return ReplyKeyboardRemove()
    return ReplyKeyboardMarkup(resize_keyboard=True, keyboard=kb,
                               input_field_placeholder="Отметь статус смены")

def request_location_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        resize_keyboard=True,
        one_time_keyboard=True,
        keyboard=[[KeyboardButton(text="📍 Отправить геолокацию", request_location=True)]],
        input_field_placeholder="Нажми, чтобы отправить геолокацию"
    )

def departments_kb(depts: list[str]) -> InlineKeyboardMarkup:
    rows, row = [], []
    for i, d in enumerate(depts, start=1):
        row.append(InlineKeyboardButton(text=d, callback_data=f"dept:{d}"))
        if i % 3 == 0:
            rows.append(row); row = []
    if row: rows.append(row)
    return InlineKeyboardMarkup(inline_keyboard=rows)

def profile_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🕒 Мои часы",  callback_data="prof:hours"),
         InlineKeyboardButton(text="📅 Мои смены", callback_data="prof:days")],
        [InlineKeyboardButton(text="📆 Выбрать период", callback_data="prof:period")]
    ])

# ================== CALENDAR HELPERS ==================
def _next_month(y: int, m: int) -> tuple[int, int]:  return (y + (m // 12), 1 if m == 12 else m + 1)
def _prev_month(y: int, m: int) -> tuple[int, int]:  return (y - 1 if m == 1 else y, 12 if m == 1 else m - 1)

def _month_matrix(y: int, m: int) -> list[list[str]]:
    first = date(y, m, 1); start_weekday = first.weekday()
    ny, nm = _next_month(y, m); last = date(ny, nm, 1) - timedelta(days=1)
    cells = [""] * start_weekday + [f"{d:02d}" for d in range(1, last.day + 1)]
    while len(cells) % 7 != 0: cells.append("")
    return [cells[i:i+7] for i in range(0, len(cells), 7)]

def _badge(d: date, f: date | None, t: date | None) -> str:
    if f and d == f: return "🟢"
    if t and d == t: return "🔴"
    if f and t and f < d < t: return "▫️"
    return ""

def _calendar_kb(y: int, m: int, step: str, f: date | None, t: date | None) -> InlineKeyboardMarkup:
    title = f"{y}-{m:02d} • {'Дата от' if step == 'from' else 'Дата до'}"
    rows = [[InlineKeyboardButton(text=title, callback_data="rng:noop")]]
    py, pm = _prev_month(y, m); ny, nm = _next_month(y, m)
    rows.append([
        InlineKeyboardButton(text="«", callback_data=f"rng:nav:{py}-{pm:02d}"),
        InlineKeyboardButton(text="Сегодня", callback_data="rng:today"),
        InlineKeyboardButton(text="»", callback_data=f"rng:nav:{ny}-{nm:02d}")
    ])
    rows.append([InlineKeyboardButton(text=t, callback_data="rng:noop") for t in ["Пн","Вт","Ср","Чт","Пт","Сб","Вс"]])
    for wk in _month_matrix(y, m):
        r = []
        for cell in wk:
            if not cell: r.append(InlineKeyboardButton(text=" ", callback_data="rng:noop")); continue
            d = date(y, m, int(cell))
            r.append(InlineKeyboardButton(text=f"{cell}{_badge(d, f, t)}", callback_data=f"rng:pick:{d.isoformat()}"))
        rows.append(r)
    rows.append([InlineKeyboardButton(text="♻️ Сброс", callback_data="rng:reset")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

# ================== HELPERS ==================
def inside(lat: float, lon: float) -> tuple[bool, int]:
    d = int(haversine_m(lat, lon, PLACE_LAT, PLACE_LON))
    return (d <= RADIUS_METERS, d)

async def manager_notify(text: str):
    if MANAGERS_CHAT_ID != 0:
        await bot.send_message(MANAGERS_CHAT_ID, text, disable_web_page_preview=True)

def is_admin(uid: int) -> bool:  return uid in ADMIN_IDS

async def report_text() -> str:
    rows = await fetch_today_shifts(today_local_str())
    if not rows: return "Сегодня отметок нет."
    by: dict[str, list] = {}
    for r in rows:
        by.setdefault(r["department"] or "Без подразделения", []).append(r)
    parts = [f"📊 Отчёт за {today_local_str()}\n"]
    for dname, arr in by.items():
        parts.append(f"<b>{dname}</b>")
        for r in arr:
            ci = r["check_in"][11:16] if r["check_in"] else "—"
            co = r["check_out"][11:16] if r["check_out"] else "⏳"
            parts.append(f"• {r['full_name']}: {ci} — {co}")
        parts.append("")
    return "\n".join(parts).strip()

async def safe_edit_text(msg, text: str, **kwargs):
    try: await msg.edit_text(text, **kwargs)
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e): raise

async def safe_edit_reply_markup(msg, **kwargs):
    try: await msg.edit_reply_markup(**kwargs)
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e): raise

# ================== HANDЛERS ==================
@rt.message(Command("help"))
async def cmd_help(m: Message):
    extra = "\n• /hours_sum YYYY-MM [1|2] [Отдел] — общий отчёт (админ)" if is_admin(m.from_user.id) else ""
    await m.answer(
        "👋 <b>ShiftBot — помощь</b>\n"
        "• /start — регистрация, выбор подразделения\n"
        "• /menu — показать кнопки\n"
        "• /myhours [YYYY-MM] — мои часы за месяц\n"
        "• /mydays [YYYY-MM] — мои дни и часы за месяц\n"
        "• /myperiod — часы за произвольный период (календарь)\n"
        "• /report — отчёт за сегодня" + extra
    )

@rt.message(Command("start"))
async def cmd_start(m: Message):
    await init_db()
    u = await get_user_by_tg(m.from_user.id)
    if u is None:
        await m.answer("👋 Добро пожаловать!\nВведите ваше <b>ФИО полностью</b> (например: Иванов Иван Иванович):")
        AWAIT_FULLNAME.add(m.from_user.id)
        return
    if not u["department"]:
        return await m.answer("🧩 Выбери своё подразделение:", reply_markup=departments_kb(DEPARTMENTS))

    # статус на сегодня
    status = "idle"
    today_rows = await fetch_today_shifts(today_local_str())
    for r in today_rows:
        if r["user_id"] == u["id"]:
            if r["check_in"] and not r["check_out"]:
                status = "checked_in"
            elif r["check_out"]:
                status = "checked_out"
            break
    await m.answer("✅ Готово. Используй меню ниже.", reply_markup=staff_menu(status))
    await m.answer("👤 Личный кабинет", reply_markup=profile_kb())

@rt.message(F.text.func(lambda t: isinstance(t, str) and len(t.strip()) >= 3))
async def reg_full_name(m: Message):
    if m.from_user.id not in AWAIT_FULLNAME:
        return
    full_name = (m.text or "").strip()
    if len(full_name.split()) < 2:
        return await m.answer("❗ Введите ФИО полностью (например: Иванов Иван Иванович).")
    await create_user(m.from_user.id, full_name)
    AWAIT_FULLNAME.discard(m.from_user.id)
    await m.answer("✅ Спасибо! Теперь выбери своё подразделение:", reply_markup=departments_kb(DEPARTMENTS))

@rt.callback_query(F.data.startswith("dept:"))
async def cb_set_dept(cq: CallbackQuery):
    u = await get_user_by_tg(cq.from_user.id)
    if u is None:
        u = await create_user(cq.from_user.id, cq.from_user.full_name or "Без имени")
    dept = cq.data.split(":", 1)[1]
    await set_user_department(u["id"], dept)
    await safe_edit_text(cq.message, f"✅ Подразделение: <b>{dept}</b>")
    await cq.message.answer("Готово. Используй меню ниже.", reply_markup=staff_menu("idle"))
    await cq.message.answer("👤 Личный кабинет", reply_markup=profile_kb())

@rt.message(Command("menu"))
async def cmd_menu(m: Message):
    await init_db()
    u = await get_user_by_tg(m.from_user.id)
    if u is None or not u["department"]:
        return await m.answer("Сначала /start.")
    status = "idle"
    rows = await fetch_today_shifts(today_local_str())
    for r in rows:
        if r["user_id"] == u["id"]:
            if r["check_in"] and not r["check_out"]:
                status = "checked_in"
            elif r["check_out"]:
                status = "checked_out"
            break
    await m.answer("📋 Меню", reply_markup=staff_menu(status))
    await m.answer("👤 Личный кабинет", reply_markup=profile_kb())

@rt.message(F.text.startswith("🟢 Пришёл"))
async def ask_loc_in(m: Message):
    await init_db()
    u = await get_user_by_tg(m.from_user.id)
    if u is None or not u["department"]:
        return await m.answer("Сначала /start.")
    PENDING_ACTION[m.from_user.id] = "in"
    await m.answer("📍 Отправь геолокацию — подтвердим приход.", reply_markup=request_location_kb())

@rt.message(F.text.startswith("🔴 Ушёл"))
async def ask_loc_out(m: Message):
    await init_db()
    u = await get_user_by_tg(m.from_user.id)
    if u is None or not u["department"]:
        return await m.answer("Сначала /start.")
    PENDING_ACTION[m.from_user.id] = "out"
    await m.answer("📍 Отправь геолокацию — подтвердим уход.", reply_markup=request_location_kb())

@rt.message(F.location)
async def on_location(m: Message):
    await init_db()
    u = await get_user_by_tg(m.from_user.id)
    if u is None or not u["department"]:
        return await m.answer("Сначала /start.")
    act = PENDING_ACTION.get(m.from_user.id)
    if act not in ("in", "out"):
        return await m.answer("Сначала нажми кнопку в /menu.")

    ok, dist = inside(m.location.latitude, m.location.longitude)
    if not ok:
        await m.answer(f"❌ Вне геозоны (~{dist} м). Обратись к менеджеру.", reply_markup=staff_menu("idle"))
        await manager_notify(f"⚠️ {u['full_name']} ({u['department']}) попытка {('прихода' if act=='in' else 'ухода')} вне зоны (~{dist} м).")
        return

    wd = today_local_str()
    await get_or_create_shift(u["id"], wd)
    ts = now_local().isoformat()

    if act == "in":
        ok, _ = await set_check_in(u["id"], wd, ts)
        if not ok:
            return await m.answer("ℹ️ Приход уже отмечен сегодня.", reply_markup=staff_menu("checked_in"))
        await log_event(u["id"], wd, "check_in", ts, m.location.latitude, m.location.longitude)
        await m.answer("✅ Приход отмечен. Удачной смены!", reply_markup=staff_menu("checked_in"))
        await manager_notify(f"🟢 {u['full_name']} ({u['department']}) пришёл в {now_local().strftime('%H:%M')} (в радиусе, {dist} м).")
    else:
        ok, msg = await set_check_out(u["id"], wd, ts)
        if not ok:
            return await m.answer(msg, reply_markup=staff_menu("checked_in"))
        await log_event(u["id"], wd, "check_out", ts, m.location.latitude, m.location.longitude)
        await m.answer("✅ Уход отмечен. Хорошего отдыха!", reply_markup=staff_menu("checked_out"))
        await manager_notify(f"🔴 {u['full_name']} ({u['department']}) ушёл в {now_local().strftime('%H:%M')} (в радиусе, {dist} м).")

    PENDING_ACTION.pop(m.from_user.id, None)

@rt.message(Command("report"))
async def cmd_report(m: Message):
    await m.answer(await report_text())

# ====== Личный кабинет (инлайн) ======
@rt.callback_query(F.data == "prof:hours")
async def cb_prof_hours(cq: CallbackQuery):
    u = await get_user_by_tg(cq.from_user.id)
    n = now_local(); y, mon = n.year, n.month
    mins = await month_minutes_for_user(u["id"], y, mon)
    await cq.message.answer(f"⏱ Твои часы за {y}-{mon:02d}: <b>{mins//60} ч {mins%60:02d} мин</b>")

@rt.callback_query(F.data == "prof:days")
async def cb_prof_days(cq: CallbackQuery):
    u = await get_user_by_tg(cq.from_user.id)
    n = now_local(); y, mon = n.year, n.month
    days = await month_days_for_user(u["id"], y, mon)
    if not days:
        return await cq.message.answer("Нет закрытых смен за текущий месяц.")
    lines = [f"📅 Мои дни за {y}-{mon:02d}"]
    for d in days:
        lines.append(f"{d['date']}: {d['minutes']//60} ч {d['minutes']%60:02d} мин")
    await cq.message.answer("\n".join(lines))

@rt.callback_query(F.data == "prof:period")
async def cb_prof_period(cq: CallbackQuery):
    t = now_local().date()
    RANGE_PICK[cq.from_user.id] = {"step": "from", "from": None, "to": None, "year": t.year, "month": t.month}
    await cq.message.answer("📆 Выбери <b>дату ОТ</b> (затем — дату ДО).",
                            reply_markup=_calendar_kb(t.year, t.month, "from", None, None))

# ====== Календарь периода ======
@rt.callback_query(F.data.startswith("rng:"))
async def cb_range(cq: CallbackQuery):
    uid = cq.from_user.id
    st = RANGE_PICK.get(uid)
    if not st:
        t = now_local().date()
        st = RANGE_PICK.setdefault(uid, {"step": "from", "from": None, "to": None, "year": t.year, "month": t.month})

    action, *rest = cq.data.split(":")[1:]

    if action == "noop":
        return await cq.answer()

    if action == "reset":
        t = now_local().date()
        st.update({"step": "from", "from": None, "to": None, "year": t.year, "month": t.month})
        return await safe_edit_reply_markup(cq.message,
            reply_markup=_calendar_kb(st["year"], st["month"], st["step"], st["from"], st["to"]))

    if action == "today":
        t = now_local().date()
        st["year"], st["month"] = t.year, t.month
        return await safe_edit_reply_markup(cq.message,
            reply_markup=_calendar_kb(st["year"], st["month"], st["step"], st["from"], st["to"]))

    if action == "nav":
        y, m = map(int, rest[0].split("-"))
        st["year"], st["month"] = y, m
        return await safe_edit_reply_markup(cq.message,
            reply_markup=_calendar_kb(st["year"], st["month"], st["step"], st["from"], st["to"]))

    if action == "pick":
        y, mo, d = map(int, rest[0].split("-"))
        picked = date(y, mo, d)
        if st["step"] == "from":
            st["from"], st["to"], st["step"] = picked, None, "to"
            await safe_edit_text(cq.message, "📆 Теперь выбери <b>дату ДО</b>.")
            return await safe_edit_reply_markup(cq.message,
                reply_markup=_calendar_kb(st["year"], st["month"], st["step"], st["from"], st["to"]))

        st["to"] = picked
        if st["from"] and st["to"] and st["to"] < st["from"]:
            st["from"], st["to"] = st["to"], st["from"]

        df, dt = st["from"], st["to"]
        if not (df and dt): return await cq.answer("Сначала выбери обе даты.")
        u = await get_user_by_tg(uid)
        days = await range_days_for_user(u["id"], df.isoformat(), dt.isoformat())
        total = sum(x["minutes"] for x in days)

        if not days:
            txt = f"⏱ Период: {df.isoformat()} — {dt.isoformat()}\nДанных нет (нет закрытых смен)."
        else:
            lines = [
                f"⏱ Период: <b>{df.isoformat()}</b> — <b>{dt.isoformat()}</b>",
                f"Итого: <b>{total//60} ч {total%60:02d} мин</b>", ""
            ] + [f"{x['date']}: {x['minutes']//60} ч {x['minutes']%60:02d} мин" for x in days]
            txt = "\n".join(lines)

        st.update({"step": "from", "from": None, "to": None})
        await safe_edit_text(cq.message, txt)
        return await safe_edit_reply_markup(cq.message,
            reply_markup=_calendar_kb(st["year"], st["month"], st["step"], st["from"], st["to"]))

# ====== Пользовательские команды ======
@rt.message(Command("myhours"))
async def cmd_myhours(m: Message):
    await init_db()
    u = await get_user_by_tg(m.from_user.id)
    if u is None: return await m.answer("Сначала /start.")
    args = m.text.split(maxsplit=1)
    if len(args) == 2:
        try: y, mon = map(int, args[1].split("-"))
        except: return await m.answer("Формат: /myhours 2025-10")
    else:
        n = now_local(); y, mon = n.year, n.month
    mins = await month_minutes_for_user(u["id"], y, mon)
    await m.answer(f"⏱ Твои часы за {y}-{mon:02d}: <b>{mins//60} ч {mins%60:02d} мин</b>")

@rt.message(Command("mydays"))
async def cmd_mydays(m: Message):
    await init_db()
    u = await get_user_by_tg(m.from_user.id)
    if u is None: return await m.answer("Сначала /start.")
    args = m.text.split(maxsplit=1)
    if len(args) == 2:
        try: y, mon = map(int, args[1].split("-"))
        except: return await m.answer("Формат: /mydays 2025-10")
    else:
        n = now_local(); y, mon = n.year, n.month
    days = await month_days_for_user(u["id"], y, mon)
    if not days: return await m.answer("Нет закрытых смен за период.")
    lines = [f"📅 Мои дни за {y}-{mon:02d}"] + [f"{d['date']}: {d['minutes']//60} ч {d['minutes']%60:02d} мин" for d in days]
    await m.answer("\n".join(lines))

# ====== АДМИН ======
@rt.message(Command("hours_sum"))
async def cmd_hours_sum(m: Message):
    if m.from_user.id not in ADMIN_IDS:
        return await m.answer("Команда доступна только администраторам.")
    args = m.text.split()
    if len(args) < 2:
        return await m.answer("Формат: /hours_sum YYYY-MM [1|2] [Отдел]\nПример: /hours_sum 2025-10 1 Кухня")
    try:
        year, month = map(int, args[1].split("-"))
    except:
        return await m.answer("Ошибка формата даты. Пример: 2025-10")

    part = None; dept = None
    if len(args) >= 3 and args[2] in ("1", "2"): part = int(args[2])
    if len(args) >= 4: dept = " ".join(args[3:])

    if part == 1: start_day, end_day = 1, 15
    elif part == 2: start_day, end_day = 16, None
    else: start_day, end_day = 1, None

    items = await month_minutes_by_user(year, month, start_day=start_day, end_day=end_day, department=dept)
    if not items: return await m.answer("Нет данных за указанный период.")

    by = {}; total_all = 0
    for r in items:
        d = r["department"] or "Без подразделения"
        by.setdefault(d, []).append(r)
        total_all += r["minutes"]

    lines = [f"📊 Сумма часов {year}-{month:02d} " + (f"(часть {part}) " if part else "") + (f"dept: {dept}" if dept else ""), ""]
    for d, arr in by.items():
        dept_total = sum(x["minutes"] for x in arr)
        lines.append(f"— <b>{d}</b>: {dept_total//60} ч {dept_total%60:02d} мин")
        for x in arr:
            lines.append(f"   • {x['full_name']}: {x['minutes']//60} ч {x['minutes']%60:02d} мин")
        lines.append("")
    lines.append(f"ИТОГО: <b>{total_all//60} ч {total_all%60:02d} мин</b>")
    await m.answer("\n".join(lines))

# ====== Бэкапы ======
async def export_month_csv_all(y:int, mon:int) -> io.BytesIO:
    buf = io.StringIO(); w = csv.writer(buf)
    w.writerow(["user_id","full_name","department","date","minutes","hours_hh:mm"])
    rows = await month_minutes_by_user(y, mon)
    for r in rows:
        hh, mm = r["minutes"]//60, r["minutes"]%60
        w.writerow([r["user_id"], r["full_name"], r["department"], f"{y}-{mon:02d}", r["minutes"], f"{hh:02d}:{mm:02d}"])
    data = io.BytesIO(buf.getvalue().encode("utf-8")); data.seek(0); return data

@rt.message(Command("backup_db"))
async def cmd_backup_db(m: Message):
    db_path = os.getenv("DB_PATH", "./bot.db")
    zbuf = io.BytesIO()
    with zipfile.ZipFile(zbuf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.write(db_path, arcname=pathlib.Path(db_path).name)
    zbuf.seek(0)
    await bot.send_document(MANAGERS_CHAT_ID or m.chat.id, ("bot_db.zip", zbuf))
    await m.answer("Бэкап БД отправлен.")

@rt.message(Command("backup_csv"))
async def cmd_backup_csv(m: Message):
    args = m.text.split(maxsplit=1)
    if len(args) != 2: return await m.answer("Формат: /backup_csv YYYY-MM")
    try: y, mon = map(int, args[1].split("-"))
    except: return await m.answer("Неверная дата. Пример: /backup_csv 2025-10")
    data = await export_month_csv_all(y, mon)
    await bot.send_document(MANAGERS_CHAT_ID or m.chat.id, ("hours.csv", data))
    await m.answer("CSV отправлен.")

@rt.message(Command("db_check"))
async def cmd_db_check(m: Message):
    import aiosqlite
    db_path = os.getenv("DB_PATH", "./bot.db")
    try:
        async with aiosqlite.connect(db_path) as db:
            cur = await db.execute("PRAGMA quick_check;"); row = await cur.fetchone()
        await m.answer(f"PRAGMA quick_check: {row[0]}")
    except Exception as e:
        await m.answer(f"Ошибка проверки: {e}")

# ================== SCHEDULER ==================
async def scheduler_setup(sched: AsyncIOScheduler):
    async def _send_report():
        try: await bot.send_message(MANAGERS_CHAT_ID, await report_text())
        except Exception: pass

    async def _night_backup():
        try:
            d = now_local().date()
            data = await export_month_csv_all(d.year, d.month)
            await bot.send_document(MANAGERS_CHAT_ID, ("hours.csv", data))
            db_path = os.getenv("DB_PATH", "./bot.db")
            zbuf = io.BytesIO()
            with zipfile.ZipFile(zbuf, "w", zipfile.ZIP_DEFLATED) as zf:
                zf.write(db_path, arcname=pathlib.Path(db_path).name)
            zbuf.seek(0)
            await bot.send_document(MANAGERS_CHAT_ID, ("bot_db.zip", zbuf))
            await bot.send_message(MANAGERS_CHAT_ID, "Ночной бэкап ✓")
        except Exception: pass

    sched.add_job(lambda: asyncio.create_task(_send_report()),
                  CronTrigger(hour=REPORT_HOUR, minute=REPORT_MINUTE, timezone=TIMEZONE),
                  id="daily_report", replace_existing=True)

    sched.add_job(lambda: asyncio.create_task(_night_backup()),
                  CronTrigger(hour=23, minute=59, timezone=TIMEZONE),
                  id="night_backup", replace_existing=True)

# ================== ENTRYPOINT ==================
async def main():
    await init_db()
    dp.include_router(rt)
    sched = AsyncIOScheduler()
    await scheduler_setup(sched)
    sched.start()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())

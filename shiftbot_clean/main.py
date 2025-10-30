# main.py
from __future__ import annotations

import os
from datetime import date, timedelta

from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.bot import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    Message, CallbackQuery,
    ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove,
    InlineKeyboardMarkup, InlineKeyboardButton
)

from dotenv import load_dotenv
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

# ---- DB / Utils (нужны соответствующие функции в db.py / utils.py) ----
from db import (
    init_db, get_user_by_tg, create_user, set_user_department,
    get_or_create_shift, set_check_in, set_check_out,
    fetch_today_shifts, month_minutes_for_user, month_days_for_user,
)
# диапазонные функции (добавляли ранее)
try:
    from db import range_days_for_user  #, range_minutes_for_user
except ImportError:
    range_days_for_user = None

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

DEPARTMENTS = [
    d.strip() for d in os.getenv(
        "DEPARTMENTS", "Зал,Кухня,Бар,Хостес,Пицца,Технички"
    ).split(",") if d.strip()
]

PLACE_LAT = float(os.getenv("PLACE_LAT", "0"))
PLACE_LON = float(os.getenv("PLACE_LON", "0"))
RADIUS_METERS = float(os.getenv("RADIUS_METERS", "150"))

# ================== BOT CORE ==================
bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())
rt = Router()

# pending action per user: {"in"|"out"}
PENDING_ACTION: dict[int, str] = {}

# выбор периода: { tg_id: {"step": "from"|"to", "from": date|None, "to": date|None, "year": int, "month": int} }
RANGE_PICK: dict[int, dict] = {}


# ================== KEYBOARDS ==================
def staff_menu(status: str) -> ReplyKeyboardMarkup | ReplyKeyboardRemove:
    """
    status: 'idle'|'checked_in'|'checked_out'
    """
    if status == "idle":
        kb = [[KeyboardButton(text="🟢 Пришёл")]]
    elif status == "checked_in":
        kb = [[KeyboardButton(text="🔴 Ушёл")]]
    else:
        return ReplyKeyboardRemove()
    return ReplyKeyboardMarkup(
        resize_keyboard=True,
        keyboard=kb,
        input_field_placeholder="Отметь статус смены"
    )


def request_location_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        resize_keyboard=True,
        one_time_keyboard=True,
        keyboard=[[KeyboardButton(text="📍 Отправить геолокацию", request_location=True)]],
        input_field_placeholder="Нажми, чтобы отправить геолокацию"
    )


def departments_kb(departments: list[str]) -> InlineKeyboardMarkup:
    rows, row = [], []
    for i, d in enumerate(departments, start=1):
        row.append(InlineKeyboardButton(text=d, callback_data=f"dept:{d}"))
        if i % 3 == 0:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    return InlineKeyboardMarkup(inline_keyboard=rows)


def profile_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🕒 Мои часы", callback_data="prof:hours"),
            InlineKeyboardButton(text="📅 Мои смены", callback_data="prof:days"),
        ],
        [
            InlineKeyboardButton(text="📆 Выбрать период", callback_data="prof:period"),
        ],
    ])


# ================== CALENDAR HELPERS ==================
def _next_month(y: int, m: int) -> tuple[int, int]:
    return (y + (m // 12), 1 if m == 12 else m + 1)

def _prev_month(y: int, m: int) -> tuple[int, int]:
    return (y - 1 if m == 1 else y, 12 if m == 1 else m - 1)

def _month_matrix(y: int, m: int) -> list[list[str]]:
    """Сетка календаря: недели по 7 ячеек, дни как 'DD' либо ''."""
    first = date(y, m, 1)
    # weekday(): 0=Пн ... 6=Вс
    start_weekday = first.weekday()  # 0..6
    ny, nm = _next_month(y, m)
    last = date(ny, nm, 1) - timedelta(days=1)
    days = last.day

    cells = [""] * start_weekday + [f"{d:02d}" for d in range(1, days + 1)]
    while len(cells) % 7 != 0:
        cells.append("")
    return [cells[i:i + 7] for i in range(0, len(cells), 7)]

def _badge(d: date, picked_from: date | None, picked_to: date | None) -> str:
    if picked_from and d == picked_from:
        return "🟢"
    if picked_to and d == picked_to:
        return "🔴"
    if picked_from and picked_to and picked_from < d < picked_to:
        return "▫️"
    return ""

def _calendar_kb(y: int, m: int, step: str, picked_from: date | None, picked_to: date | None) -> InlineKeyboardMarkup:
    title = f"{y}-{m:02d} • {'Дата от' if step == 'from' else 'Дата до'}"
    rows = [[InlineKeyboardButton(text=title, callback_data="rng:noop")]]

    py, pm = _prev_month(y, m)
    ny, nm = _next_month(y, m)
    rows.append([
        InlineKeyboardButton(text="«", callback_data=f"rng:nav:{py}-{pm:02d}"),
        InlineKeyboardButton(text="Сегодня", callback_data="rng:today"),
        InlineKeyboardButton(text="»", callback_data=f"rng:nav:{ny}-{nm:02d}"),
    ])

    rows.append([InlineKeyboardButton(text=t, callback_data="rng:noop") for t in ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]])

    for wk in _month_matrix(y, m):
        wk_btns = []
        for cell in wk:
            if not cell:
                wk_btns.append(InlineKeyboardButton(text=" ", callback_data="rng:noop"))
                continue
            d = date(y, m, int(cell))
            mark = _badge(d, picked_from, picked_to)
            wk_btns.append(InlineKeyboardButton(text=f"{cell}{mark}", callback_data=f"rng:pick:{d.isoformat()}"))
        rows.append(wk_btns)

    rows.append([InlineKeyboardButton(text="♻️ Сброс", callback_data="rng:reset")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


# ================== HELPERS ==================
def inside(lat: float, lon: float) -> tuple[bool, int]:
    d = int(haversine_m(lat, lon, PLACE_LAT, PLACE_LON))
    return (d <= RADIUS_METERS, d)

async def manager_notify(text: str):
    if MANAGERS_CHAT_ID != 0:
        await bot.send_message(MANAGERS_CHAT_ID, text, disable_web_page_preview=True)


async def report_text() -> str:
    rows = await fetch_today_shifts(today_local_str())
    if not rows:
        return "Сегодня отметок нет."
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


# ================== FSM (регистрация ФИО) ==================
class Register(StatesGroup):
    waiting_full_name = State()


# ================== HANDLERS ==================
@rt.message(Command("help"))
async def cmd_help(m: Message):
    await m.answer(
        "👋 <b>ShiftBot — помощь</b>\n"
        "• /start — регистрация, выбор подразделения\n"
        "• /menu — показать кнопки\n"
        "• /myhours [YYYY-MM] — мои часы за месяц\n"
        "• /mydays [YYYY-MM] — мои дни и часы за месяц\n"
        "• /myperiod — часы за произвольный период (календарь)\n"
        "• /report — отчёт за сегодня"
    )


@rt.message(Command("start"))
async def cmd_start(m: Message, state: FSMContext):
    await init_db()
    u = await get_user_by_tg(m.from_user.id)
    if u is None:
        await state.set_state(Register.waiting_full_name)
        return await m.answer(
            "👋 Добро пожаловать!\n"
            "Введите ваше <b>ФИО полностью</b> (например: Иванов Иван Иванович):"
        )
    if not u.department:
        return await m.answer("🧩 Выбери своё подразделение:", reply_markup=departments_kb(DEPARTMENTS))
    await m.answer("✅ Готово. Используй меню ниже.", reply_markup=staff_menu("idle"))
    await m.answer("👤 Личный кабинет", reply_markup=profile_kb())


@rt.message(Register.waiting_full_name)
async def reg_full_name(m: Message, state: FSMContext):
    full_name = (m.text or "").strip()
    if len(full_name.split()) < 2:
        return await m.answer("❗ Введите ФИО полностью (например: Иванов Иван Иванович).")
    u = await create_user(m.from_user.id, full_name)
    await state.clear()
    await m.answer(
        f"✅ Спасибо, {full_name}!\nТеперь выбери своё подразделение:",
        reply_markup=departments_kb(DEPARTMENTS)
    )


@rt.callback_query(F.data.startswith("dept:"))
async def cb_set_dept(cq: CallbackQuery):
    u = await get_user_by_tg(cq.from_user.id)
    if u is None:
        u = await create_user(cq.from_user.id, cq.from_user.full_name or "Без имени")
    dept = cq.data.split(":", 1)[1]
    await set_user_department(u.id, dept)
    await cq.message.edit_text(f"✅ Подразделение: <b>{dept}</b>")
    await cq.message.answer("Готово. Используй меню ниже.", reply_markup=staff_menu("idle"))
    await cq.message.answer("👤 Личный кабинет", reply_markup=profile_kb())


@rt.message(Command("menu"))
async def cmd_menu(m: Message):
    await init_db()
    u = await get_user_by_tg(m.from_user.id)
    if u is None or not u.department:
        return await m.answer("Сначала /start.")
    # определяем статус по сегодняшней смене
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
    await m.answer("👤 Личный кабинет", reply_markup=profile_kb())


@rt.message(F.text.startswith("🟢 Пришёл"))
async def ask_loc_in(m: Message):
    await init_db()
    u = await get_user_by_tg(m.from_user.id)
    if u is None or not u.department:
        return await m.answer("Сначала /start.")
    PENDING_ACTION[m.from_user.id] = "in"
    await m.answer("📍 Отправь геолокацию — подтвердим приход.", reply_markup=request_location_kb())


@rt.message(F.text.startswith("🔴 Ушёл"))
async def ask_loc_out(m: Message):
    await init_db()
    u = await get_user_by_tg(m.from_user.id)
    if u is None or not u.department:
        return await m.answer("Сначала /start.")
    PENDING_ACTION[m.from_user.id] = "out"
    await m.answer("📍 Отправь геолокацию — подтвердим уход.", reply_markup=request_location_kb())


@rt.message(F.location)
async def on_location(m: Message):
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


@rt.message(Command("myperiod"))
async def cmd_myperiod(m: Message):
    await init_db()
    u = await get_user_by_tg(m.from_user.id)
    if u is None or not u.department:
        return await m.answer("Сначала /start.")
    t = now_local().date()
    RANGE_PICK[m.from_user.id] = {"step": "from", "from": None, "to": None, "year": t.year, "month": t.month}
    await m.answer(
        "📆 Выбери <b>дату ОТ</b> (затем — дату ДО).",
        reply_markup=_calendar_kb(t.year, t.month, "from", None, None)
    )


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
        return await cq.message.edit_reply_markup(
            reply_markup=_calendar_kb(st["year"], st["month"], st["step"], st["from"], st["to"])
        )

    if action == "today":
        t = now_local().date()
        st["year"], st["month"] = t.year, t.month
        return await cq.message.edit_reply_markup(
            reply_markup=_calendar_kb(st["year"], st["month"], st["step"], st["from"], st["to"])
        )

    if action == "nav":
        ym = rest[0]
        y, m = map(int, ym.split("-"))
        st["year"], st["month"] = y, m
        return await cq.message.edit_reply_markup(
            reply_markup=_calendar_kb(st["year"], st["month"], st["step"], st["from"], st["to"])
        )

    if action == "pick":
        iso = rest[0]
        y, mo, d = map(int, iso.split("-"))
        picked = date(y, mo, d)

        if st["step"] == "from":
            st["from"] = picked
            st["to"] = None
            st["step"] = "to"
            await cq.message.edit_text("📆 Теперь выбери <b>дату ДО</b>.")
            return await cq.message.edit_reply_markup(
                reply_markup=_calendar_kb(st["year"], st["month"], st["step"], st["from"], st["to"])
            )

        # step == "to"
        st["to"] = picked
        if st["from"] and st["to"] and st["to"] < st["from"]:
            st["from"], st["to"] = st["to"], st["from"]

        df, dt = st["from"], st["to"]
        if not (df and dt):
            return await cq.answer("Сначала выбери обе даты.")

        # Считаем
        if range_days_for_user is None:
            return await cq.message.edit_text("Функции диапазона не найдены в db.py. Добавь range_days_for_user().")

        u = await get_user_by_tg(uid)
        days = await range_days_for_user(u.id, df.isoformat(), dt.isoformat())
        total = sum(x["minutes"] for x in days)

        if not days:
            txt = f"⏱ Период: {df.isoformat()} — {dt.isoformat()}\nДанных нет (нет закрытых смен)."
        else:
            lines = [
                f"⏱ Период: <b>{df.isoformat()}</b> — <b>{dt.isoformat()}</b>",
                f"Итого: <b>{total//60} ч {total%60:02d} мин</b>",
                ""
            ]
            for x in days:
                lines.append(f"{x['date']}: {x['minutes']//60} ч {x['minutes']%60:02d} мин")
            txt = "\n".join(lines)

        st.update({"step": "from", "from": None, "to": None})
        await cq.message.edit_text(txt)
        return await cq.message.edit_reply_markup(
            reply_markup=_calendar_kb(st["year"], st["month"], st["step"], st["from"], st["to"])
        )


@rt.callback_query(F.data.startswith("prof:"))
async def cb_profile(cq: CallbackQuery):
    action = cq.data.split(":")[1]
    u = await get_user_by_tg(cq.from_user.id)
    if u is None or not u.department:
        return await cq.answer("Сначала /start и выбери подразделение", show_alert=True)

    today = now_local().date()
    y, mon = today.year, today.month

    if action == "hours":
        mins = await month_minutes_for_user(u.id, y, mon)
        text = (
            f"🕒 <b>Мои часы</b>\n"
            f"Период: {y}-{mon:02d}\n"
            f"Итого: <b>{mins//60} ч {mins%60:02d} мин</b>"
        )
        try:
            await cq.message.edit_text(text, reply_markup=profile_kb())
        except Exception:
            await cq.message.answer(text, reply_markup=profile_kb())
        return await cq.answer()

    if action == "days":
        days = await month_days_for_user(u.id, y, mon)
        if not days:
            text = f"📅 <b>Мои смены</b>\nПериод: {y}-{mon:02d}\nНет закрытых смен."
        else:
            lines = [f"📅 <b>Мои смены</b>\nПериод: {y}-{mon:02d}", ""]
            for d in days:
                lines.append(f"{d['date']}: {d['minutes']//60} ч {d['minutes']%60:02d} мин")
            text = "\n".join(lines)
        try:
            await cq.message.edit_text(text, reply_markup=profile_kb())
        except Exception:
            await cq.message.answer(text, reply_markup=profile_kb())
        return await cq.answer()

    if action == "period":
        t = now_local().date()
        RANGE_PICK[cq.from_user.id] = {"step": "from", "from": None, "to": None, "year": t.year, "month": t.month}
        await cq.message.edit_text("📆 Выбери <b>дату ОТ</b> (потом — дату ДО).")
        await cq.message.edit_reply_markup(
            reply_markup=_calendar_kb(t.year, t.month, "from", None, None)
        )
        return await cq.answer()


# ================== SCHEDULER ==================
async def scheduler_setup(sched: AsyncIOScheduler):
    async def _send():
        await bot.send_message(MANAGERS_CHAT_ID, await report_text())

    sched.add_job(
        lambda: bot.loop.create_task(_send()),
        CronTrigger(hour=REPORT_HOUR, minute=REPORT_MINUTE, timezone=TIMEZONE),
        id="daily_report",
        replace_existing=True
    )


# ================== ENTRYPOINT ==================
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

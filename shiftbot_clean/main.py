# main.py
# Boss Control — учёт смен, часов и локации
# Python 3.10+, aiogram 3.x

import os
import sqlite3
from datetime import datetime, timedelta, timezone
from contextlib import closing
from typing import Optional, Tuple, List

from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart, Command
from aiogram.types import (
    Message, CallbackQuery, ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State

# ==========================
# Конфиг
# ==========================
BOT_TOKEN = os.getenv("BOT_TOKEN", "PASTE_YOUR_TOKEN_HERE")
TZ = timezone(timedelta(hours=+5))  # Ташкент/Узбекистан по умолчанию; поменяй при необходимости
DB_PATH = os.getenv("DB_PATH", "boss_control.db")

bot = Bot(BOT_TOKEN)
dp = Dispatcher()


# ==========================
# База данных (SQLite)
# ==========================
def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with closing(db()) as conn, conn:
        conn.executescript(
            """
            PRAGMA journal_mode=WAL;
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                full_name TEXT NOT NULL,
                username TEXT,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS shifts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                start_ts TEXT NOT NULL,
                end_ts TEXT,
                start_lat REAL,
                start_lon REAL,
                end_lat REAL,
                end_lon REAL,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            );

            CREATE INDEX IF NOT EXISTS idx_shifts_user ON shifts(user_id);
            """
        )


def get_user(user_id: int) -> Optional[sqlite3.Row]:
    with closing(db()) as conn, conn:
        cur = conn.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
        return cur.fetchone()


def upsert_user(user_id: int, full_name: str, username: Optional[str]):
    with closing(db()) as conn, conn:
        if get_user(user_id):
            conn.execute(
                "UPDATE users SET full_name=?, username=? WHERE user_id=?",
                (full_name, username, user_id),
            )
        else:
            conn.execute(
                "INSERT INTO users(user_id, full_name, username, created_at) VALUES (?,?,?,?)",
                (user_id, full_name, username, now_iso()),
            )


def open_shift_exists(user_id: int) -> Optional[sqlite3.Row]:
    with closing(db()) as conn, conn:
        cur = conn.execute(
            "SELECT * FROM shifts WHERE user_id=? AND end_ts IS NULL ORDER BY start_ts DESC LIMIT 1",
            (user_id,),
        )
        return cur.fetchone()


def start_shift(user_id: int, lat: Optional[float] = None, lon: Optional[float] = None):
    with closing(db()) as conn, conn:
        conn.execute(
            "INSERT INTO shifts(user_id, start_ts, start_lat, start_lon) VALUES (?,?,?,?)",
            (user_id, now_iso(), lat, lon),
        )


def end_shift(user_id: int, lat: Optional[float] = None, lon: Optional[float] = None) -> Optional[sqlite3.Row]:
    with closing(db()) as conn, conn:
        cur = conn.execute(
            "SELECT * FROM shifts WHERE user_id=? AND end_ts IS NULL ORDER BY start_ts DESC LIMIT 1",
            (user_id,),
        )
        row = cur.fetchone()
        if not row:
            return None
        conn.execute(
            "UPDATE shifts SET end_ts=?, end_lat=?, end_lon=? WHERE id=?",
            (now_iso(), lat, lon, row["id"]),
        )
        return row


def list_shifts_between(user_id: int, since: datetime, until: datetime) -> List[sqlite3.Row]:
    with closing(db()) as conn, conn:
        cur = conn.execute(
            """
            SELECT * FROM shifts
            WHERE user_id=?
              AND start_ts >= ?
              AND (end_ts <= ? OR end_ts IS NULL)
            ORDER BY start_ts ASC
            """,
            (user_id, since.isoformat(), until.isoformat()),
        )
        return cur.fetchall()


def month_bounds(dt: datetime) -> Tuple[datetime, datetime]:
    start = dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if start.month == 12:
        end = start.replace(year=start.year + 1, month=1)
    else:
        end = start.replace(month=start.month + 1)
    return start, end


def parse_iso(s: str) -> datetime:
    return datetime.fromisoformat(s)


def now_iso() -> str:
    return datetime.now(TZ).replace(microsecond=0).isoformat()


def shift_duration_sec(row: sqlite3.Row) -> int:
    start = parse_iso(row["start_ts"])
    end = parse_iso(row["end_ts"]) if row["end_ts"] else datetime.now(TZ)
    return int((end - start).total_seconds())


def human_td(seconds: int) -> str:
    h = seconds // 3600
    m = (seconds % 3600) // 60
    return f"{h} ч {m:02d} мин"


# ==========================
# FSM состояния
# ==========================
class Reg(StatesGroup):
    waiting_fullname = State()


class Report(StatesGroup):
    picking_from = State()
    picking_to = State()


# ==========================
# Клавиатуры
# ==========================
def main_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🟢 Пришёл"), KeyboardButton(text="🔴 Ушёл")],
            [KeyboardButton(text="🕒 Мои часы"), KeyboardButton(text="📅 Мои смены")],
            [KeyboardButton(text="📍 Отправить геопозицию", request_location=True)],
            [KeyboardButton(text="⚙️ Настройки")],
        ],
        resize_keyboard=True,
        input_field_placeholder="Выберите действие…",
    )


def settings_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="Изменить ФИО", callback_data="settings:rename")
    kb.button(text="Назад", callback_data="settings:back")
    return kb.as_markup()


def calendar_kb(year: int, month: int, mode: str) -> InlineKeyboardMarkup:
    """
    mode: 'from' либо 'to' — что выбираем сейчас
    """
    import calendar as cal

    cal.setfirstweekday(cal.MONDAY)
    _, last_day = cal.monthrange(year, month)

    kb = InlineKeyboardBuilder()
    kb.button(text=f"{year}-{month:02d} · Дата {('от' if mode=='from' else 'до')}", callback_data="noop")

    # Навигация по месяцам
    kb.row(
        InlineKeyboardButton(text="«", callback_data=f"cal:{mode}:nav:{year}:{month}:prev"),
        InlineKeyboardButton(text="Сегодня", callback_data=f"cal:{mode}:today"),
        InlineKeyboardButton(text="»", callback_data=f"cal:{mode}:nav:{year}:{month}:next"),
    )

    # Шапка дней
    for d in ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]:
        kb.button(text=d, callback_data="noop")
    kb.adjust(7)

    # Пустые ячейки до 1-го
    first_weekday = cal.monthrange(year, month)[0]  # 0=Пн
    for _ in range(first_weekday):
        kb.button(text=" ", callback_data="noop")

    # Дни месяца
    for day in range(1, last_day + 1):
        kb.button(text=f"{day:02d}", callback_data=f"cal:{mode}:pick:{year}:{month}:{day}")
    kb.adjust(7)

    # Сброс
    kb.row(InlineKeyboardButton(text="♻️ Сброс", callback_data="cal:reset"))
    return kb.as_markup()


# ==========================
# /start
# ==========================
@dp.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    init_db()

    u = get_user(message.from_user.id)
    if not u:
        await state.set_state(Reg.waiting_fullname)
        await message.answer(
            "Привет! Я бот учёта смен «Boss Control».\n\n"
            "Для начала напиши своё **ФИО** (как в табеле).",
        )
        return

    # Сводка месяца + меню
    now = datetime.now(TZ)
    start, end = month_bounds(now)
    total_sec = 0
    for row in list_shifts_between(message.from_user.id, start, end):
        total_sec += shift_duration_sec(row)

    await message.answer(
        f"🕒 Твои часы за {now.strftime('%Y-%m')}: **{human_td(total_sec)}**\n"
        f"Выбери *дату ОТ*, затем *дату ДО* в «Мои часы/Мои смены».",
        reply_markup=main_kb(),
    )


@dp.message(Reg.waiting_fullname)
async def reg_fullname(message: Message, state: FSMContext):
    full_name = message.text.strip()
    if len(full_name.split()) < 2:
        await message.answer("Пожалуйста, укажи ФИО полностью (минимум имя и фамилия).")
        return

    upsert_user(message.from_user.id, full_name, message.from_user.username)
    await state.clear()
    await message.answer(f"Готово! Запомнил тебя как: **{full_name}** ✅", reply_markup=main_kb())


# ==========================
# Локация
# ==========================
@dp.message(F.location)
async def got_location(message: Message):
    # Просто подтверждаем, локация будет прикреплена при приходе/уходе,
    # если они произойдут в течение «сессии» (тут для простоты — сразу при нажатии).
    lat = message.location.latitude
    lon = message.location.longitude
    await message.answer(f"📍 Локация получена: {lat:.5f}, {lon:.5f}\n"
                         f"Нажми «🟢 Пришёл» или «🔴 Ушёл», чтобы записать её к отметке.")


# ==========================
# Пришёл / Ушёл
# ==========================
@dp.message(F.text == "🟢 Пришёл")
async def arrived(message: Message):
    init_db()
    u = get_user(message.from_user.id)
    if not u:
        await message.answer("Сначала зарегистрируй ФИО. Напиши его сообщением.")
        return

    if open_shift_exists(message.from_user.id):
        await message.answer("У тебя уже есть открытая смена. Сначала отметь «🔴 Ушёл».")
        return

    lat, lon = None, None
    if message.location:
        lat, lon = message.location.latitude, message.location.longitude

    start_shift(message.from_user.id, lat, lon)
    await message.answer(f"✅ Отмечено: пришёл в {datetime.now(TZ).strftime('%H:%M')}")


@dp.message(F.text == "🔴 Ушёл")
async def left(message: Message):
    init_db()
    u = get_user(message.from_user.id)
    if not u:
        await message.answer("Сначала зарегистрируй ФИО. Напиши его сообщением.")
        return

    lat, lon = None, None
    if message.location:
        lat, lon = message.location.latitude, message.location.longitude

    row = end_shift(message.from_user.id, lat, lon)
    if not row:
        await message.answer("Открытой смены нет. Сначала нажми «🟢 Пришёл».")
        return

    dur = human_td(shift_duration_sec(row))
    await message.answer(f"👋 Отмечено: ушёл в {datetime.now(TZ).strftime('%H:%M')}\n"
                         f"⌛ Длительность смены: {dur}")


# ==========================
# Отчёты (диапазон дат)
# ==========================
def send_calendar_for_from() -> InlineKeyboardMarkup:
    dt = datetime.now(TZ)
    return calendar_kb(dt.year, dt.month, mode="from")


def send_calendar_for_to() -> InlineKeyboardMarkup:
    dt = datetime.now(TZ)
    return calendar_kb(dt.year, dt.month, mode="to")


@dp.message(F.text == "🕒 Мои часы")
async def my_hours(message: Message, state: FSMContext):
    await state.set_state(Report.picking_from)
    await state.update_data(report_kind="hours")
    await message.answer("Выбери **дату ОТ** (затем — дату ДО).", reply_markup=send_calendar_for_from())


@dp.message(F.text == "📅 Мои смены")
async def my_shifts(message: Message, state: FSMContext):
    await state.set_state(Report.picking_from)
    await state.update_data(report_kind="shifts")
    await message.answer("Выбери **дату ОТ** (затем — дату ДО).", reply_markup=send_calendar_for_from())


@dp.callback_query(F.data.startswith("cal:reset"))
async def cal_reset(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await cb.message.edit_text("Сбросил выбор диапазона. Нажми «🕒 Мои часы» или «📅 Мои смены».")
    await cb.answer()


@dp.callback_query(F.data.startswith("cal:from:today"))
async def cal_from_today(cb: CallbackQuery, state: FSMContext):
    dt = datetime.now(TZ).date()
    await state.update_data(date_from=str(dt))
    await state.set_state(Report.picking_to)
    await cb.message.edit_text(f"Дата ОТ: **{dt}**\nТеперь выбери **дату ДО**.", reply_markup=send_calendar_for_to())
    await cb.answer()


@dp.callback_query(F.data.startswith("cal:to:today"))
async def cal_to_today(cb: CallbackQuery, state: FSMContext):
    dt = datetime.now(TZ).date()
    data = await state.get_data()
    if "date_from" not in data:
        await cb.answer("Сначала выбери дату ОТ.", show_alert=True)
        return
    await state.update_data(date_to=str(dt))
    await finish_report(cb, state)


@dp.callback_query(F.data.startswith("cal:") & ~F.data.endswith("today") & ~F.data.endswith("reset"))
async def cal_common(cb: CallbackQuery, state: FSMContext):
    # cal:{mode}:{action}:{y}:{m}:{d or nav}
    parts = cb.data.split(":")
    _, mode, action, y, m, tail = parts[0], parts[1], parts[2], parts[3], parts[4], parts[5]

    if action == "nav":
        year = int(y)
        month = int(m)
        direction = tail  # prev/next
        if direction == "prev":
            month -= 1
            if month == 0:
                month = 12
                year -= 1
        else:
            month += 1
            if month == 13:
                month = 1
                year += 1
        kb = calendar_kb(year, month, mode)
        await cb.message.edit_reply_markup(kb)
        await cb.answer()
        return

    if action == "pick":
        year, month, day = int(y), int(m), int(tail)
        picked = datetime(year, month, day, tzinfo=TZ).date()
        if mode == "from":
            await state.update_data(date_from=str(picked))
            await state.set_state(Report.picking_to)
            await cb.message.edit_text(
                f"Дата ОТ: **{picked}**\nТеперь выбери **дату ДО**.",
                reply_markup=send_calendar_for_to(),
            )
        else:
            data = await state.get_data()
            if "date_from" not in data:
                await cb.answer("Сначала выбери дату ОТ.", show_alert=True)
                return
            await state.update_data(date_to=str(picked))
            await finish_report(cb, state)
        await cb.answer()
        return


async def finish_report(cb: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    date_from = datetime.fromisoformat(data["date_from"]).replace(tzinfo=TZ)
    date_to = datetime.fromisoformat(data["date_to"]).replace(tzinfo=TZ)
    if date_to < date_from:
        date_from, date_to = date_to, date_from

    # включительно по дату ДО (до конца дня)
    date_to_end = date_to + timedelta(days=1)

    rows = list_shifts_between(cb.from_user.id, date_from, date_to_end)
    kind = data.get("report_kind", "hours")

    if kind == "hours":
        total = sum(shift_duration_sec(r) for r in rows)
        text = (
            f"🕒 Часы c **{date_from.date()}** по **{date_to.date()}**:\n"
            f"Итого: **{human_td(total)}**\n\n"
            f"Совет: можно выбрать и весь месяц — результат посчитается автоматически."
        )
    else:
        if not rows:
            text = f"📅 Смены с **{date_from.date()}** по **{date_to.date()}**: ничего не найдено."
        else:
            lines = []
            for r in rows:
                start = parse_iso(r["start_ts"]).strftime("%Y-%m-%d %H:%M")
                end = parse_iso(r["end_ts"]).strftime("%Y-%m-%d %H:%M") if r["end_ts"] else "—"
                dur = human_td(shift_duration_sec(r))
                lines.append(f"• {start} → {end}  ({dur})")
            text = f"📅 Смены с **{date_from.date()}** по **{date_to.date()}**:\n" + "\n".join(lines)

    await state.clear()
    await cb.message.edit_text(text)


# ==========================
# Настройки
# ==========================
@dp.message(F.text == "⚙️ Настройки")
async def settings(message: Message):
    await message.answer("Настройки:", reply_markup=settings_kb())


@dp.callback_query(F.data == "settings:rename")
async def settings_rename(cb: CallbackQuery, state: FSMContext):
    await state.set_state(Reg.waiting_fullname)
    await cb.message.answer("Отправь новое **ФИО** сообщением.")
    await cb.answer()


@dp.callback_query(F.data == "settings:back")
async def settings_back(cb: CallbackQuery):
    await cb.message.edit_text("Готово. Выбери действие на клавиатуре ниже.", reply_markup=None)
    await cb.answer()


# ==========================
# Команды /help /id
# ==========================
@dp.message(Command("help"))
async def cmd_help(message: Message):
    await message.answer(
        "Команды:\n"
        "/start — главное меню\n"
        "/help — помощь\n"
        "/id — твой ID\n\n"
        "Кнопки:\n"
        "🟢 Пришёл / 🔴 Ушёл — отметка смены\n"
        "🕒 Мои часы — сумма часов за период\n"
        "📅 Мои смены — список смен\n"
        "📍 Отправить геопозицию — прикрепить локацию к отметке"
    )


@dp.message(Command("id"))
async def cmd_id(message: Message):
    await message.answer(f"Твой Telegram ID: `{message.from_user.id}`", parse_mode=None)


# ==========================
# Запуск
# ==========================
async def on_startup():
    init_db()
    print("Boss Control bot started.")


def main():
    import asyncio
    asyncio.run(dp.start_polling(bot, on_startup=on_startup()))


if __name__ == "__main__":
    main()

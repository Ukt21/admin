# main.py
# Boss Control — учёт смен, часов и локации
# Python 3.10+, aiogram 3.x

import os
import math
import sqlite3
from datetime import datetime, timedelta, timezone
from contextlib import closing
from typing import Optional, Tuple, List, Dict

from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart, Command
from aiogram.types import (
    Message, CallbackQuery, ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton, Location
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

# Координаты ресторана / офиса (центр зоны)
OFFICE_LAT = float(os.getenv("OFFICE_LAT", "41.31647163058427"))  # поставь свои
OFFICE_LON = float(os.getenv("OFFICE_LON", "69.25378645716818"))  # поставь свои
MAX_DISTANCE_METERS = float(os.getenv("MAX_DISTANCE_METERS", "250"))  # радиус допуска в метрах

bot = Bot(BOT_TOKEN)
dp = Dispatcher()

# Последняя локация пользователя (в оперативной памяти)
LAST_LOCATION: Dict[int, Tuple[float, float]] = {}


# ==========================
# Утилиты: время + расстояние
# ==========================
def now_iso() -> str:
    return datetime.now(TZ).replace(microsecond=0).isoformat()


def parse_iso(s: str) -> datetime:
    return datetime.fromisoformat(s)


def shift_duration_sec(row: sqlite3.Row) -> int:
    start = parse_iso(row["start_ts"])
    end = parse_iso(row["end_ts"]) if row["end_ts"] else datetime.now(TZ)
    return int((end - start).total_seconds())


def human_td(seconds: int) -> str:
    h = seconds // 3600
    m = (seconds % 3600) // 60
    return f"{h} ч {m:02d} мин"


def month_bounds(dt: datetime) -> Tuple[datetime, datetime]:
    start = dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if start.month == 12:
        end = start.replace(year=start.year + 1, month=1)
    else:
        end = start.replace(month=start.month + 1)
    return start, end


def distance_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Расстояние между точками (гаверсинус), метры."""
    R = 6371000.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)

    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


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
        f"Выбери *дату ОТ*, затем *дату ДО* в «Мои часы/Мои смены».\n\n"
        f"Для отметки смены сначала отправь геопозицию кнопкой «📍 Отправить геопозицию», "
        f"затем нажми «🟢 Пришёл» или «🔴 Ушёл».",
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
    lat = message.location.latitude
    lon = message.location.longitude
    user_id = message.from_user.id

    LAST_LOCATION[user_id] = (lat, lon)

    dist = distance_m(lat, lon, OFFICE_LAT, OFFICE_LON)
    inside = dist <= MAX_DISTANCE_METERS

    text = (
        f"📍 Локация сохранена: {lat:.5f}, {lon:.5f}\n"
        f"Расстояние до точки контроля: ~{int(dist)} м "
        f"(лимит {int(MAX_DISTANCE_METERS)} м).\n\n"
    )
    if inside:
        text += "Ты в допустимой зоне ✅\nТеперь нажми «🟢 Пришёл» или «🔴 Ушёл», чтобы зафиксировать смену."
    else:
        text += "Внимание: ты **вне допустимой зоны** ⛔\nЕсли это ошибка — уточни координаты точки контроля у менеджера."

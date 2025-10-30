from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton

def departments_kb(departments: list[str]) -> InlineKeyboardMarkup:
    rows = []
    row = []
    for i, d in enumerate(departments, start=1):
        row.append(InlineKeyboardButton(text=d, callback_data=f"dept:{d}"))
        if i % 3 == 0:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    return InlineKeyboardMarkup(inline_keyboard=rows)

def main_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="Пришёл"), KeyboardButton(text="Ушёл")]],
        resize_keyboard=True,
        input_field_placeholder="Отметь приход или уход",
    )

def location_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="📍 Отправить геолокацию", request_location=True)]],
        resize_keyboard=True,
        one_time_keyboard=True,
        input_field_placeholder="Нажми, чтобы отправить геолокацию",
    )

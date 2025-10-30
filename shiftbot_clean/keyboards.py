from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove

def staff_menu(status: str):
    if status == "idle":
        kb = [[KeyboardButton(text="🟢 Пришёл")]]
    elif status == "checked_in":
        kb = [[KeyboardButton(text="🔴 Ушёл")]]
    else:
        return ReplyKeyboardRemove()
    return ReplyKeyboardMarkup(resize_keyboard=True, keyboard=kb, input_field_placeholder="Отметь статус смены")

def request_location_kb():
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
            rows.append(row); row = []
    if row:
        rows.append(row)
    return InlineKeyboardMarkup(inline_keyboard=rows)

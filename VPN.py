# ==========================================================
# Stopka VPN - Полноценный VPN сервис
# Telegram Bot + Web Server (для Render)
# ==========================================================

import asyncio
import logging
import psycopg2
import psycopg2.errorcodes
import json
import os
import html
import io
import time
from collections import defaultdict
from datetime import datetime, timedelta
from aiohttp import web

import aiohttp
from aiogram import Bot, Dispatcher, F
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    BotCommand
)
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.utils.markdown import hbold

############################################################
# НАСТРОЙКИ
############################################################

BOT_TOKEN = os.environ.get("BOT_TOKEN", "ВСТАВЬ_ТОКЕН")
OWNER_ID = int(os.environ.get("OWNER_ID", 5604869107))

# DonationAlerts
DONATION_ACCESS_TOKEN = os.environ.get("DONATION_ACCESS_TOKEN", "")
DONATION_SECRET_KEY = os.environ.get("DONATION_SECRET_KEY", "")
DONATION_API_URL = "https://www.donationalerts.com/api/v1"

DATABASE_URL = os.environ.get("DATABASE_URL", "")
PORT = int(os.getenv("PORT", 8080))

MAX_DAYS = 3650
REFERRAL_DAYS = 7

############################################################
# ЛОГИ
############################################################

logging.basicConfig(level=logging.INFO)

############################################################
# BOT
############################################################

bot = Bot(
    token=BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)

storage = MemoryStorage()
dp = Dispatcher(storage=storage)

############################################################
# RATE LIMITER
############################################################

class RateLimiter:
    def __init__(self, max_requests=10, window=60):
        self.max_requests = max_requests
        self.window = window
        self.requests = defaultdict(list)
    
    def is_allowed(self, user_id):
        now = time.time()
        window_start = now - self.window
        
        self.requests[user_id] = [
            t for t in self.requests[user_id]
            if t > window_start
        ]
        
        if len(self.requests[user_id]) >= self.max_requests:
            return False
        
        self.requests[user_id].append(now)
        return True

rate_limiter = RateLimiter()

############################################################
# DATABASE
############################################################

IntegrityError = psycopg2.errors.lookup(psycopg2.errorcodes.UNIQUE_VIOLATION)

class PGConnection:
    def __init__(self, dsn):
        self._conn = psycopg2.connect(dsn)
        self._conn.autocommit = False

    def execute(self, query, params=()):
        pg_query = query.replace("?", "%s")
        cur = self._conn.cursor()
        try:
            cur.execute(pg_query, params)
        except Exception:
            self._conn.rollback()
            raise
        return cur

    def commit(self):
        self._conn.commit()

class Database:
    def __init__(self):
        if not DATABASE_URL:
            raise RuntimeError(
                "Не задана переменная окружения DATABASE_URL "
                "(строка подключения к PostgreSQL)."
            )
        self.conn = PGConnection(DATABASE_URL)
        self.create_tables()

    def create_tables(self):
        # Пользователи
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS users(
            id BIGINT PRIMARY KEY,
            username TEXT,
            name TEXT,
            expire_date TEXT,
            status TEXT,
            is_admin INTEGER DEFAULT 0,
            invited_by BIGINT DEFAULT 0,
            first_payment INTEGER DEFAULT 0,
            last_tariff TEXT,
            username_history TEXT DEFAULT '[]'
        )
        """)
        
        # Тикеты
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS tickets(
            id SERIAL PRIMARY KEY,
            user_id BIGINT,
            message TEXT,
            answer TEXT,
            status TEXT
        )
        """)
        
        # Платежи
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS payments(
            id SERIAL PRIMARY KEY,
            user_id BIGINT,
            payment_code TEXT UNIQUE,
            amount INTEGER,
            days INTEGER,
            status TEXT,
            date TEXT
        )
        """)
        
        # Промокоды
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS promo_codes(
            code TEXT PRIMARY KEY,
            days INTEGER,
            uses INTEGER DEFAULT 0,
            max_uses INTEGER
        )
        """)

        # Использованные промокоды
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS used_promos(
            user_id BIGINT,
            code TEXT,
            PRIMARY KEY (user_id, code)
        )
        """)
        
        # Рефералы
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS referrals(
            user_id BIGINT PRIMARY KEY,
            invited_by BIGINT,
            bonus_given INTEGER DEFAULT 0
        )
        """)
        
        # Логи админов
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS admin_logs(
            id SERIAL PRIMARY KEY,
            admin_id BIGINT,
            action TEXT,
            target_id BIGINT,
            created_at TEXT
        )
        """)
        
        # Обработанные донаты
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS processed_donations(
            donation_id TEXT PRIMARY KEY,
            processed_at TEXT
        )
        """)
        
        # Уведомления
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS notifications(
            user_id BIGINT,
            type TEXT,
            date TEXT,
            PRIMARY KEY (user_id, type)
        )
        """)
        
        self.conn.commit()

    # ===== User Methods =====
    def add_user(self, user_id, username, name):
        cursor = self.conn.execute("SELECT id FROM users WHERE id=?", (user_id,))
        if cursor.fetchone():
            return
        
        expire = datetime.now() + timedelta(days=3)
        expire_str = expire.strftime("%Y-%m-%d 23:59:59")
        
        self.conn.execute("""
            INSERT INTO users (id, username, name, expire_date, status, is_admin, invited_by, first_payment, last_tariff, username_history) 
            VALUES(?,?,?,?,?,?,0,0,'',?)
        """, (user_id, username, name, expire_str, "Активно", 0, json.dumps([])))
        self.conn.commit()

    def get_user(self, user_id):
        cursor = self.conn.execute("SELECT * FROM users WHERE id=?", (user_id,))
        return cursor.fetchone()

    def get_username(self, username):
        clean_user = username.replace("@", "").strip()
        cursor = self.conn.execute("SELECT * FROM users WHERE username=?", (clean_user,))
        res = cursor.fetchone()
        if not res and clean_user.isdigit():
            cursor = self.conn.execute("SELECT * FROM users WHERE id=?", (int(clean_user),))
            res = cursor.fetchone()
        return res

    def update_username(self, user_id, new_username):
        user = self.get_user(user_id)
        if not user:
            return
        history = json.loads(user[9] or '[]')
        history.append({
            "username": new_username,
            "date": datetime.now().isoformat()
        })
        self.conn.execute(
            "UPDATE users SET username=?, username_history=? WHERE id=?",
            (new_username, json.dumps(history[-10:]), user_id)
        )
        self.conn.commit()

    def is_admin(self, user_id):
        if user_id == OWNER_ID:
            return True
        user = self.get_user(user_id)
        if not user:
            return False
        return user[5] == 1

    def set_admin(self, user_id, is_admin):
        self.conn.execute("UPDATE users SET is_admin=? WHERE id=?", (1 if is_admin else 0, user_id))
        self.conn.commit()

    def mark_first_payment(self, user_id):
        self.conn.execute("UPDATE users SET first_payment=1 WHERE id=?", (user_id,))
        self.conn.commit()

    def save_last_tariff(self, user_id, tariff_id):
        self.conn.execute("UPDATE users SET last_tariff=? WHERE id=?", (tariff_id, user_id))
        self.conn.commit()

    def get_referral_count(self, user_id):
        cursor = self.conn.execute(
            "SELECT COUNT(*) FROM referrals WHERE invited_by=? AND bonus_given=1",
            (user_id,)
        )
        return cursor.fetchone()[0]

    def get_total_users_count(self):
        cursor = self.conn.execute("SELECT COUNT(*) FROM users")
        return cursor.fetchone()[0]

    def is_promo_used(self, user_id, code):
        cursor = self.conn.execute("SELECT 1 FROM used_promos WHERE user_id=? AND code=?", (user_id, code))
        return cursor.fetchone() is not None

    def mark_promo_used(self, user_id, code):
        self.conn.execute("INSERT INTO used_promos (user_id, code) VALUES(?,?)", (user_id, code))
        self.conn.commit()

    def notification_sent(self, user_id, ntype):
        cursor = self.conn.execute(
            "SELECT 1 FROM notifications WHERE user_id=? AND type=?",
            (user_id, ntype)
        )
        return cursor.fetchone() is not None

    def save_notification(self, user_id, ntype):
        self.conn.execute(
            """
            INSERT INTO notifications (user_id, type, date) VALUES(?,?,?)
            ON CONFLICT (user_id, type) DO UPDATE SET date = EXCLUDED.date
            """,
            (user_id, ntype, datetime.now().strftime("%Y-%m-%d"))
        )
        self.conn.commit()

    def is_donation_processed(self, donation_id):
        cursor = self.conn.execute("SELECT 1 FROM processed_donations WHERE donation_id=?", (donation_id,))
        return cursor.fetchone() is not None

    def mark_donation_processed(self, donation_id):
        self.conn.execute(
            "INSERT INTO processed_donations (donation_id, processed_at) VALUES(?,?)",
            (donation_id, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        )
        self.conn.commit()

    def add_admin_log(self, admin_id, action, target_id=0):
        self.conn.execute(
            """
            INSERT INTO admin_logs (admin_id, action, target_id, created_at)
            VALUES(?,?,?,?)
            """,
            (admin_id, action, target_id, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        )
        self.conn.commit()

db = Database()

############################################################
# FSM СОСТОЯНИЯ
############################################################

class TicketState(StatesGroup):
    waiting_text = State()

class AdminGiveState(StatesGroup):
    waiting_data = State()

class AdminToggleState(StatesGroup):
    waiting_username = State()

class ReplyState(StatesGroup):
    waiting_answer = State()

class BroadcastState(StatesGroup):
    waiting_text = State()

class PromoState(StatesGroup):
    waiting_code = State()

class PromoCreateState(StatesGroup):
    waiting_code = State()
    waiting_days = State()
    waiting_max_uses = State()

############################################################
# ТАРИФЫ
############################################################

TARIFFS = {
    "month": {"name": "Месяц", "days": 30, "price": 180},
    "half": {"name": "Полгода", "days": 180, "price": 980},
    "year": {"name": "Год", "days": 365, "price": 1960}
}

############################################################
# KEYBOARDS
############################################################

def profile_keyboard(is_admin=False):
    buttons = [
        [InlineKeyboardButton(text="💳 Оплата VPN", callback_data="payment")],
        [InlineKeyboardButton(text="📱 Подключить устройство", callback_data="connect_device")],
        [InlineKeyboardButton(text="🎁 Пригласить друга", callback_data="my_ref")],
        [InlineKeyboardButton(text="🎟 Промокод", callback_data="promo")]
    ]
    if is_admin:
        buttons.append([InlineKeyboardButton(text="🛠 Админ-панель", callback_data="admin")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def payment_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🗓 Месяц — 180₽", callback_data="pay_month")],
        [InlineKeyboardButton(text="📅 Полгода — 980₽", callback_data="pay_half")],
        [InlineKeyboardButton(text="📆 Год — 1960₽", callback_data="pay_year")],
        [InlineKeyboardButton(text="⬅ Назад", callback_data="profile")]
    ])

def back_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅ Назад", callback_data="profile")]
    ])

def admin_back_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅ Назад в админ-панель", callback_data="admin")]
    ])

def donation_keyboard(code):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💸 Оплатить DonationAlerts", url="https://www.donationalerts.com/")],
        [InlineKeyboardButton(text="🔎 Проверить оплату", callback_data=f"check_payment:{code}")],
        [InlineKeyboardButton(text="⬅ Назад", callback_data="payment")]
    ])

def support_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📨 Написать в поддержку", callback_data="create_ticket")]
    ])

def admin_keyboard():
    buttons = [
        [InlineKeyboardButton(text="👥 Пришло пользователей", callback_data="users_count")],
        [InlineKeyboardButton(text="📢 Рассылка", callback_data="broadcast")],
        [InlineKeyboardButton(text="🎟 Тикеты", callback_data="admin_tickets")],
        [InlineKeyboardButton(text="🎁 Промокоды", callback_data="promo_admin")],
        [InlineKeyboardButton(text="📅 Выдать дни", callback_data="admin_give")],
        [InlineKeyboardButton(text="👑 Назначить/Удалить админа", callback_data="admin_toggle")],
        [InlineKeyboardButton(text="⬅ Главное меню", callback_data="profile")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def ticket_list_keyboard(tickets):
    buttons = []
    for ticket in tickets:
        buttons.append([InlineKeyboardButton(text=f"🎟 #{ticket[0]} | {html.escape(ticket[2][:20])}", callback_data=f"ticket_{ticket[0]}")])
    buttons.append([InlineKeyboardButton(text="⬅ Назад в админ-панель", callback_data="admin")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def promo_admin_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Создать промокод", callback_data="promo_create")],
        [InlineKeyboardButton(text="📋 Список промокодов", callback_data="promo_list")],
        [InlineKeyboardButton(text="⬅ Назад в админ-панель", callback_data="admin")]
    ])

############################################################
# MIDDLEWARES
############################################################

@dp.message.middleware()
async def rate_limit_middleware(handler, message: Message, data: dict):
    if not rate_limiter.is_allowed(message.from_user.id):
        await message.answer("⏳ Слишком много запросов. Подождите немного.")
        return
    return await handler(message, data)

############################################################
# HELPER FUNCTIONS
############################################################

async def render_profile(user_id, target_message=None, callback=None):
    user = db.get_user(user_id)
    if not user:
        if callback:
            await callback.answer("Ошибка пользователя", show_alert=True)
        return

    try:
        expire = datetime.strptime(user[3], "%Y-%m-%d %H:%M:%S")
    except:
        try:
            expire = datetime.strptime(user[3], "%Y-%m-%d")
        except:
            expire = datetime.now()
    
    now = datetime.now()
    days = (expire - now).days
    if expire < now:
        days = 0

    expire_formatted = expire.strftime("%d.%m.%Y")
    status_icon = "🟢" if days > 0 else "🔴"
    safe_name = hbold(user[2] or "Пользователь")
    
    text = (
        f"👤 {safe_name}\n\n"
        f"🛡 <b>Stopka VPN</b>\n\n"
        f"{status_icon} Осталось: <b>{days} дней ({expire_formatted})</b>"
    )
    
    is_admin = db.is_admin(user_id)
    kb = profile_keyboard(is_admin)

    if callback:
        await callback.message.edit_text(text, reply_markup=kb)
        await callback.answer()
    elif target_message:
        await target_message.answer(text, reply_markup=kb)

############################################################
# COMMANDS: START, HELP, ABOUT
############################################################

@dp.message(Command("start"))
async def start(message: Message):
    user_id = message.from_user.id
    
    user = db.get_user(user_id)
    if user and user[1] != message.from_user.username:
        db.update_username(user_id, message.from_user.username or "")
    
    db.add_user(
        user_id,
        message.from_user.username or "",
        message.from_user.full_name
    )
    
    if user_id == OWNER_ID:
        db.set_admin(user_id, True)
    
    # Проверка реферала
    args = message.text.split()
    if len(args) > 1:
        ref = args[1]
        if ref.startswith("STOPKA"):
            try:
                inviter = int(ref.replace("STOPKA", ""))
                if inviter != user_id:
                    cursor = db.conn.execute("SELECT invited_by FROM users WHERE id=?", (user_id,))
                    existing = cursor.fetchone()
                    if existing and existing[0] == 0:
                        db.conn.execute("UPDATE users SET invited_by=? WHERE id=?", (inviter, user_id))
                        db.conn.execute("INSERT INTO referrals (user_id, invited_by, bonus_given) VALUES(?,?,0)", (user_id, inviter))
                        db.conn.commit()
            except:
                pass
    
    await render_profile(user_id, target_message=message)

@dp.message(Command("help"))
async def help_command(message: Message):
    await message.answer(
        "🛡 <b>Поддержка Stopka VPN</b>\n\n"
        "Если у вас возникли вопросы или проблемы с работой сервиса, вы можете обратиться в техподдержку.\n\n"
        "Нажмите кнопку ниже, чтобы отправить сообщение администраторам:",
        reply_markup=support_keyboard()
    )

@dp.message(Command("about"))
async def about_command(message: Message):
    await message.answer("👨‍💻 Создатели: @prostokiril, @ll1_what")

############################################################
# PROFILE & ACTIONS
############################################################

@dp.callback_query(F.data == "profile")
async def profile_callback(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await render_profile(callback.from_user.id, callback=callback)

@dp.callback_query(F.data == "connect_device")
async def connect_device(callback: CallbackQuery):
    await callback.answer("🛠 Функция в разработке", show_alert=True)

############################################################
# PAYMENTS
############################################################

@dp.callback_query(F.data == "payment")
async def payment(callback: CallbackQuery):
    await callback.message.edit_text(
        "💳 <b>Выберите тарифный план:</b>\n\n"
        "При покупке дни автоматически добавятся к вашей текущей подписке.",
        reply_markup=payment_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("pay_"))
async def process_pay(callback: CallbackQuery):
    tariff_id = callback.data.split("_")[1]
    tariff = TARIFFS.get(tariff_id)
    if not tariff:
        await callback.answer("Ошибка выбора тарифа", show_alert=True)
        return

    db.save_last_tariff(callback.from_user.id, tariff_id)
    
    def create_payment_code():
        import uuid
        return f"STOPKA-{uuid.uuid4().hex[:8].upper()}"

    code = create_payment_code()
    try:
        db.conn.execute("""
            INSERT INTO payments (user_id, payment_code, amount, days, status, date)
            VALUES(?,?,?,?,?,?)
        """, (callback.from_user.id, code, tariff['price'], tariff['days'], "waiting", datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        db.conn.commit()
    except IntegrityError:
        code = create_payment_code()
        db.conn.execute("""
            INSERT INTO payments (user_id, payment_code, amount, days, status, date)
            VALUES(?,?,?,?,?,?)
        """, (callback.from_user.id, code, tariff['price'], tariff['days'], "waiting", datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        db.conn.commit()

    await callback.message.edit_text(
        f"💳 <b>Оплата Stopka VPN</b>\n\n"
        f"Тариф: <b>{tariff['name']}</b> (+{tariff['days']} дней)\n"
        f"Сумма: <b>{tariff['price']}₽</b>\n\n"
        f"Ваш код оплаты:\n<code>{code}</code>\n\n"
        f"Укажите этот код в комментарии при переводе.",
        reply_markup=donation_keyboard(code)
    )
    await callback.answer()

############################################################
# REFERRAL & PROMO
############################################################

@dp.callback_query(F.data == "my_ref")
async def my_ref(callback: CallbackQuery):
    bot_info = await bot.get_me()
    link = f"https://t.me/{bot_info.username}?start=STOPKA{callback.from_user.id}"
    ref_count = db.get_referral_count(callback.from_user.id)
    await callback.message.edit_text(
        f"🎁 <b>Реферальная программа</b>\n\n"
        f"Приглашай друзей и получай бонусные дни VPN.\n\n"
        f"🔗 Твоя ссылка:\n<code>{link}</code>\n\n"
        f"👥 Приглашено друзей: <b>{ref_count}</b>\n"
        f"⭐ За каждого друга: +{REFERRAL_DAYS} дней VPN",
        reply_markup=back_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data == "promo")
async def promo_start(callback: CallbackQuery, state: FSMContext):
    await state.set_state(PromoState.waiting_code)
    await callback.message.edit_text(
        "🎟 <b>Введите промокод</b>\n\n"
        "Отправьте промокод сообщением:",
        reply_markup=back_keyboard()
    )
    await callback.answer()

@dp.message(PromoState.waiting_code)
async def promo_use(message: Message, state: FSMContext):
    code = message.text.upper().strip()
    user_id = message.from_user.id

    if db.is_promo_used(user_id, code):
        await message.answer("❌ Вы уже активировали этот промокод!")
        await state.clear()
        return

    promo = db.conn.execute("SELECT * FROM promo_codes WHERE code=?", (code,)).fetchone()
    if not promo:
        await message.answer("❌ Промокод не найден")
        await state.clear()
        return

    if promo[2] >= promo[3]:
        await message.answer("❌ Лимит использований промокода исчерпан")
        await state.clear()
        return

    user = db.get_user(user_id)
    if not user:
        await message.answer("❌ Ошибка пользователя")
        await state.clear()
        return

    try:
        expire = datetime.strptime(user[3], "%Y-%m-%d %H:%M:%S")
    except:
        expire = datetime.strptime(user[3], "%Y-%m-%d")

    now = datetime.now()
    if expire < now:
        expire = now

    days = promo[1]
    new_expire = expire + timedelta(days=days)
    new_expire_str = new_expire.strftime("%Y-%m-%d 23:59:59")

    db.conn.execute("UPDATE users SET expire_date=?, status='Активно' WHERE id=?", (new_expire_str, user_id))
    db.conn.execute("UPDATE promo_codes SET uses=uses+1 WHERE code=?", (code,))
    db.mark_promo_used(user_id, code)
    db.conn.commit()

    await state.clear()
    await message.answer(f"✅ Промокод активирован! Добавлено +{days} дней.")

############################################################
# SUPPORT TICKETS
############################################################

@dp.callback_query(F.data == "create_ticket")
async def create_ticket(callback: CallbackQuery, state: FSMContext):
    await state.set_state(TicketState.waiting_text)
    await callback.message.edit_text(
        "📝 Опишите вашу проблему в одном сообщении:",
        reply_markup=back_keyboard()
    )
    await callback.answer()

@dp.message(TicketState.waiting_text)
async def process_ticket(message: Message, state: FSMContext):
    text = message.text.strip()
    db.conn.execute(
        "INSERT INTO tickets (user_id, message, answer, status) VALUES(?,?,?,?)",
        (message.from_user.id, text, "", "Открыт")
    )
    db.conn.commit()
    await state.clear()
    await message.answer("✅ Ваше обращение отправлено поддержки!", reply_markup=back_keyboard())

############################################################
# ADMIN PANEL
############################################################

@dp.callback_query(F.data == "admin")
async def admin_panel(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    if not db.is_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа", show_alert=True)
        return

    await callback.message.edit_text(
        "🛠 <b>Панель администратора</b>",
        reply_markup=admin_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data == "users_count")
async def users_count(callback: CallbackQuery):
    if not db.is_admin(callback.from_user.id):
        await callback.answer("❌ Нет прав", show_alert=True)
        return
    total = db.get_total_users_count()
    await callback.message.edit_text(
        f"👥 <b>Статистика пользователей</b>\n\n"
        f"Всего пришло пользователей: <b>{total}</b>",
        reply_markup=admin_back_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data == "admin_toggle")
async def admin_toggle_start(callback: CallbackQuery, state: FSMContext):
    if not db.is_admin(callback.from_user.id):
        await callback.answer("❌ Нет прав", show_alert=True)
        return
    await state.set_state(AdminToggleState.waiting_username)
    await callback.message.edit_text(
        "👑 <b>Назначить / Удалить админа</b>\n\n"
        "Введите `@username` или `ID` пользователя.\n"
        "Если пользователь админ — статус заберётся, если не админ — выдастся.",
        reply_markup=admin_back_keyboard()
    )
    await callback.answer()

@dp.message(AdminToggleState.waiting_username)
async def admin_toggle_finish(message: Message, state: FSMContext):
    if not db.is_admin(message.from_user.id):
        return

    user_input = message.text.strip()
    user = db.get_username(user_input)

    if not user:
        await message.answer("❌ Пользователь не найден", reply_markup=admin_back_keyboard())
        await state.clear()
        return

    target_id = user[0]
    if target_id == OWNER_ID:
        await message.answer("❌ Нельзя изменить права владельца", reply_markup=admin_back_keyboard())
        await state.clear()
        return

    current_status = user[5] == 1
    new_status = not current_status
    db.set_admin(target_id, new_status)
    
    status_str = "теперь администратор" if new_status else "больше не администратор"
    db.add_admin_log(message.from_user.id, f"Изменил статус админа на {new_status}", target_id)
    
    await state.clear()
    await message.answer(f"✅ Пользователь {user[1] or target_id} {status_str}!", reply_markup=admin_back_keyboard())

@dp.callback_query(F.data == "admin_give")
async def admin_give(callback: CallbackQuery, state: FSMContext):
    if not db.is_admin(callback.from_user.id):
        await callback.answer("❌ Нет прав", show_alert=True)
        return
    await state.set_state(AdminGiveState.waiting_data)
    await callback.message.edit_text(
        "📅 <b>Выдать дни подписки</b>\n\n"
        "Введите данные в формате:\n<code>@username дни</code>\n\nПример: `@user 30`",
        reply_markup=admin_back_keyboard()
    )
    await callback.answer()

@dp.message(AdminGiveState.waiting_data)
async def give_days(message: Message, state: FSMContext):
    if not db.is_admin(message.from_user.id):
        return
    try:
        username, days_str = message.text.split()
        days = int(days_str)
    except:
        await message.answer("❌ Неверный формат. Пример: `@user 30`", reply_markup=admin_back_keyboard())
        return

    user = db.get_username(username)
    if not user:
        await message.answer("❌ Пользователь не найден", reply_markup=admin_back_keyboard())
        await state.clear()
        return

    try:
        expire = datetime.strptime(user[3], "%Y-%m-%d %H:%M:%S")
    except:
        expire = datetime.strptime(user[3], "%Y-%m-%d")

    now = datetime.now()
    if expire < now:
        expire = now

    expire += timedelta(days=days)
    expire_str = expire.strftime("%Y-%m-%d 23:59:59")

    db.conn.execute("UPDATE users SET expire_date=?, status='Активно' WHERE id=?", (expire_str, user[0]))
    db.conn.commit()
    db.add_admin_log(message.from_user.id, f"Выдал {days} дней", user[0])

    await state.clear()
    await message.answer(f"✅ Выдано {days} дней пользователю {username}", reply_markup=admin_back_keyboard())

# --- TICKETS ADMIN ---
@dp.callback_query(F.data == "admin_tickets")
async def admin_tickets(callback: CallbackQuery):
    if not db.is_admin(callback.from_user.id):
        await callback.answer("❌ Нет прав", show_alert=True)
        return
    cursor = db.conn.execute("SELECT * FROM tickets WHERE status='Открыт'")
    tickets = cursor.fetchall()
    if not tickets:
        await callback.message.edit_text("🎟 Открытых тикетов нет", reply_markup=admin_back_keyboard())
        return
    await callback.message.edit_text("🎟 <b>Открытые обращения:</b>", reply_markup=ticket_list_keyboard(tickets))

@dp.callback_query(F.data.startswith("ticket_"))
async def open_ticket(callback: CallbackQuery):
    if not db.is_admin(callback.from_user.id):
        return
    ticket_id = int(callback.data.split("_")[1])
    ticket = db.conn.execute("SELECT * FROM tickets WHERE id=?", (ticket_id,)).fetchone()
    if not ticket:
        await callback.answer("Тикет не найден", show_alert=True)
        return
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✉️ Ответить", callback_data=f"reply_{ticket_id}")],
        [InlineKeyboardButton(text="❌ Закрыть", callback_data=f"close_{ticket_id}")],
        [InlineKeyboardButton(text="⬅ Назад в админ-панель", callback_data="admin")]
    ])
    await callback.message.edit_text(
        f"🎟 <b>Тикет #{ticket[0]}</b>\nПользователь ID: {ticket[1]}\n\nСообщение:\n{ticket[2]}",
        reply_markup=keyboard
    )

@dp.callback_query(F.data.startswith("reply_"))
async def reply_ticket(callback: CallbackQuery, state: FSMContext):
    if not db.is_admin(callback.from_user.id):
        return
    ticket_id = int(callback.data.split("_")[1])
    await state.update_data(ticket_id=ticket_id)
    await state.set_state(ReplyState.waiting_answer)
    await callback.message.edit_text("✉️ Введите текст ответа:", reply_markup=admin_back_keyboard())

@dp.message(ReplyState.waiting_answer)
async def send_ticket_answer(message: Message, state: FSMContext):
    if not db.is_admin(message.from_user.id):
        return
    data = await state.get_data()
    ticket_id = data["ticket_id"]
    ticket = db.conn.execute("SELECT * FROM tickets WHERE id=?", (ticket_id,)).fetchone()
    if ticket:
        try:
            await bot.send_message(ticket[1], f"📩 <b>Ответ поддержки:</b>\n\n{message.text}")
        except:
            pass
        db.conn.execute("UPDATE tickets SET answer=?, status='Закрыт' WHERE id=?", (message.text, ticket_id))
        db.conn.commit()
    await state.clear()
    await message.answer("✅ Ответ отправлен!", reply_markup=admin_back_keyboard())

@dp.callback_query(F.data.startswith("close_"))
async def close_ticket(callback: CallbackQuery):
    if not db.is_admin(callback.from_user.id):
        return
    ticket_id = int(callback.data.split("_")[1])
    db.conn.execute("UPDATE tickets SET status='Закрыт' WHERE id=?", (ticket_id,))
    db.conn.commit()
    await callback.answer("✅ Тикет закрыт")
    await admin_tickets(callback)

# --- PROMO ADMIN ---
@dp.callback_query(F.data == "promo_admin")
async def promo_admin(callback: CallbackQuery):
    if not db.is_admin(callback.from_user.id):
        return
    await callback.message.edit_text("🎁 <b>Управление промокодами</b>", reply_markup=promo_admin_keyboard())

@dp.callback_query(F.data == "promo_list")
async def promo_list(callback: CallbackQuery):
    if not db.is_admin(callback.from_user.id):
        return
    promos = db.conn.execute("SELECT code, days, uses, max_uses FROM promo_codes").fetchall()
    if not promos:
        await callback.message.edit_text("📋 Промокодов нет", reply_markup=promo_admin_keyboard())
        return
    text = "📋 <b>Список промокодов:</b>\n\n"
    for p in promos:
        text += f"🎟 {p[0]}: +{p[1]} дней ({p[2]}/{p[3]})\n"
    await callback.message.edit_text(text, reply_markup=promo_admin_keyboard())

@dp.callback_query(F.data == "promo_create")
async def promo_create_start(callback: CallbackQuery, state: FSMContext):
    if not db.is_admin(callback.from_user.id):
        return
    await state.set_state(PromoCreateState.waiting_code)
    await callback.message.edit_text("Введите название промокода (например `SUMMER2026`):", reply_markup=admin_back_keyboard())

@dp.message(PromoCreateState.waiting_code)
async def promo_create_code(message: Message, state: FSMContext):
    await state.update_data(code=message.text.upper().strip())
    await state.set_state(PromoCreateState.waiting_days)
    await message.answer("Количество бонусных дней:", reply_markup=admin_back_keyboard())

@dp.message(PromoCreateState.waiting_days)
async def promo_create_days(message: Message, state: FSMContext):
    try:
        days = int(message.text)
        await state.update_data(days=days)
        await state.set_state(PromoCreateState.waiting_max_uses)
        await message.answer("Максимальное число активаций:", reply_markup=admin_back_keyboard())
    except:
        await message.answer("Введите число!")

@dp.message(PromoCreateState.waiting_max_uses)
async def promo_create_finish(message: Message, state: FSMContext):
    try:
        max_uses = int(message.text)
        data = await state.get_data()
        db.conn.execute(
            "INSERT INTO promo_codes (code, days, uses, max_uses) VALUES(?,?,0,?)",
            (data['code'], data['days'], max_uses)
        )
        db.conn.commit()
        await state.clear()
        await message.answer(f"✅ Промокод `{data['code']}` создан!", reply_markup=admin_back_keyboard())
    except Exception as e:
        await message.answer(f"Ошибка: {e}", reply_markup=admin_back_keyboard())

# --- BROADCAST ---
@dp.callback_query(F.data == "broadcast")
async def broadcast_start(callback: CallbackQuery, state: FSMContext):
    if not db.is_admin(callback.from_user.id):
        return
    await state.set_state(BroadcastState.waiting_text)
    await callback.message.edit_text("📢 Введите текст рассылки:", reply_markup=admin_back_keyboard())

@dp.message(BroadcastState.waiting_text)
async def broadcast_finish(message: Message, state: FSMContext):
    if not db.is_admin(message.from_user.id):
        return
    users = db.conn.execute("SELECT id FROM users").fetchall()
    count = 0
    for u in users:
        try:
            await bot.send_message(u[0], message.text)
            count += 1
            await asyncio.sleep(0.05)
        except:
            pass
    await state.clear()
    await message.answer(f"✅ Рассылка завершена! Доставлено {count} пользователям.", reply_markup=admin_back_keyboard())

############################################################
# CHECK PAYMENTS & NOTIFICATIONS TASK
############################################################

async def check_payments():
    while True:
        try:
            pass
        except Exception as e:
            logging.error(f"Ошибка проверки платежей: {e}")
        await asyncio.sleep(30)

async def subscription_notifications():
    while True:
        try:
            users = db.conn.execute("SELECT id, expire_date FROM users").fetchall()
            for u in users:
                try:
                    expire = datetime.strptime(u[1], "%Y-%m-%d %H:%M:%S")
                except:
                    continue
                days = (expire - datetime.now()).days
                if days == 3 and not db.notification_sent(u[0], "3days"):
                    try:
                        await bot.send_message(u[0], "⏰ Ваша подписка Stopka VPN закончится через 3 дня!")
                        db.save_notification(u[0], "3days")
                    except:
                        pass
        except Exception as e:
            logging.error(f"Ошибка уведомлений: {e}")
        await asyncio.sleep(3600)

async def set_commands():
    commands = [
        BotCommand(command="start", description="🚀 Запустить бота"),
        BotCommand(command="help", description="❓ Помощь и Поддержка"),
        BotCommand(command="about", description="👨‍💻 О создателях")
    ]
    await bot.set_my_commands(commands)

@dp.error()
async def error_handler(event, exception):
    logging.error(f"Ошибка: {exception}")
    return True

############################################################
# WEB SERVER FOR RENDER HEALTH CHECK
############################################################

async def handle_ping(request):
    return web.Response(text="Bot is running", status=200)

async def start_web_server():
    app = web.Application()
    app.router.add_get('/', handle_ping)
    app.router.add_get('/ping', handle_ping)
    
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', PORT)
    await site.start()
    logging.info(f"🌐 Веб-сервер запущен на порту {PORT}")

############################################################
# START BOT
############################################################

async def main():
    logging.info("🚀 Запуск Stopka VPN...")
    await set_commands()

    # Запуск HTTP сервера для Render Health Check
    await start_web_server()

    # Фоновые задачи
    asyncio.create_task(check_payments())
    asyncio.create_task(subscription_notifications())

    logging.info("✅ Stopka VPN запущен успешно!")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
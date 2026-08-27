# ==========================================================
# Stopka VPN — Полный рабочий код бота + интеграция с VLESS
# Telegram Bot + Web Server (для Render) + Автогенерация ключей
# ==========================================================

import asyncio
import logging
import aiohttp
import psycopg2
import psycopg2.errorcodes
import psycopg2.extensions as ext
import psycopg2.pool as pg_pool
import json
import os
import html
import time
import traceback
from collections import defaultdict
from datetime import datetime, timedelta
from aiohttp import web

from aiogram import Bot, Dispatcher, F
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    BotCommand,
    ErrorEvent,
    LabeledPrice,
    PreCheckoutQuery
)
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State

############################################################
# НАСТРОЙКИ
############################################################

BOT_TOKEN = os.environ.get("BOT_TOKEN", "ВСТАВЬ_ТОКЕН")
OWNER_ID = int(os.environ.get("OWNER_ID", 5604869107))

DATABASE_URL = os.environ.get("DATABASE_URL", "")
PORT = int(os.getenv("PORT", 8080))

# Настройки панели VPN (Marzban / Xray API)
VPN_API_URL = os.environ.get("VPN_API_URL", "https://your-vpn-panel.com")
VPN_ADMIN_USERNAME = os.environ.get("VPN_ADMIN_USERNAME", "admin")
VPN_ADMIN_PASSWORD = os.environ.get("VPN_ADMIN_PASSWORD", "password")

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

BOT_USERNAME = None  # кэшируется один раз при старте

############################################################
# VPN API CLIENT (Marzban / Xray)
############################################################

class VPNClient:
    def __init__(self):
        self.base_url = VPN_API_URL
        self.token = None
        self._session = None

    def _get_session(self):
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10))
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    async def login(self):
        session = self._get_session()
        try:
            async with session.post(
                f"{self.base_url}/api/admin/token",
                data={"username": VPN_ADMIN_USERNAME, "password": VPN_ADMIN_PASSWORD}
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    self.token = data.get("access_token")
        except Exception as e:
            logging.error(f"Ошибка авторизации в VPN панели: {e}")

    async def create_or_update_user(self, user_id, expire_timestamp):
        if not self.token:
            await self.login()

        headers = {"Authorization": f"Bearer {self.token}"}
        username = f"user_{user_id}"
        session = self._get_session()

        async with session.get(f"{self.base_url}/api/user/{username}", headers=headers) as resp:
            if resp.status == 200:
                async with session.put(
                    f"{self.base_url}/api/user/{username}",
                    headers=headers,
                    json={"expire": expire_timestamp, "status": "active"}
                ) as update_resp:
                    if update_resp.status == 200:
                        data = await update_resp.json()
                        links = data.get("links", [])
                        return links[0] if links else ""
            elif resp.status == 404:
                async with session.post(
                    f"{self.base_url}/api/user",
                    headers=headers,
                    json={
                        "username": username,
                        "expire": expire_timestamp,
                        "proxies": {"vless": {"flow": ""}},
                        "inbounds": {"vless": ["VLESS Reality"]}
                    }
                ) as create_resp:
                    if create_resp.status == 200:
                        data = await create_resp.json()
                        links = data.get("links", [])
                        return links[0] if links else ""
        return ""

    async def disable_user(self, user_id):
        if not self.token:
            await self.login()
        headers = {"Authorization": f"Bearer {self.token}"}
        username = f"user_{user_id}"
        session = self._get_session()
        try:
            async with session.put(
                f"{self.base_url}/api/user/{username}",
                headers=headers,
                json={"status": "disabled"}
            ) as resp:
                return resp.status == 200
        except Exception as e:
            logging.error(f"Ошибка отключения пользователя в VPN панели: {e}")
            return False

vpn_client = VPNClient()

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
        self.requests[user_id] = [t for t in self.requests[user_id] if t > window_start]
        if len(self.requests[user_id]) >= self.max_requests:
            return False
        self.requests[user_id].append(now)
        return True

rate_limiter = RateLimiter()

############################################################
# DATABASE
############################################################

IntegrityError = psycopg2.errors.lookup(psycopg2.errorcodes.UNIQUE_VIOLATION)

class _PoolWithSetup(pg_pool.ThreadedConnectionPool):
    def _connect(self, key=None):
        conn = super()._connect(key)
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SET search_path TO public;")
        return conn

class PGConnection:
    def __init__(self, dsn, minconn=1, maxconn=10):
        self._dsn = dsn
        self._pool = _PoolWithSetup(
            minconn, maxconn, dsn,
            connect_timeout=10,
            keepalives=1,
            keepalives_idle=30,
            keepalives_interval=10,
            keepalives_count=5
        )

    def execute(self, query, params=()):
        pg_query = query.replace("?", "%s")
        last_error = None
        max_attempts = self._pool.maxconn + 1
        for attempt in range(max_attempts):
            try:
                conn = self._pool.getconn()
            except pg_pool.PoolError as e:
                last_error = e
                time.sleep(0.05)
                continue
            try:
                if conn.closed:
                    raise psycopg2.InterfaceError("connection already closed")
                if conn.get_transaction_status() == ext.TRANSACTION_STATUS_INERROR:
                    conn.rollback()
                cur = conn.cursor()
                cur.execute(pg_query, params)
                self._pool.putconn(conn)
                return cur
            except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                last_error = e
                try:
                    self._pool.putconn(conn, close=True)
                except:
                    pass
                logging.warning(f"БД: мёртвое соединение из пула, пересоздаю (попытка {attempt + 1}/{max_attempts})")
                continue
            except Exception:
                try:
                    conn.rollback()
                except:
                    pass
                self._pool.putconn(conn)
                raise
        raise last_error

    def commit(self):
        pass

class Database:
    def __init__(self):
        if not DATABASE_URL:
            raise RuntimeError("Не задана переменная окружения DATABASE_URL.")
        self.conn = PGConnection(DATABASE_URL)
        self.create_tables()

    def create_tables(self):
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
            username_history TEXT DEFAULT '[]',
            balance INTEGER DEFAULT 0,
            vless_key TEXT DEFAULT ''
        )
        """)
        
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS tickets(
            id SERIAL PRIMARY KEY,
            user_id BIGINT,
            message TEXT,
            answer TEXT,
            status TEXT
        )
        """)
        self.conn.execute("ALTER TABLE tickets ADD COLUMN IF NOT EXISTS file_id TEXT DEFAULT ''")
        self.conn.execute("ALTER TABLE tickets ADD COLUMN IF NOT EXISTS file_type TEXT DEFAULT ''")
        self.conn.execute("ALTER TABLE tickets ADD COLUMN IF NOT EXISTS closed_at TEXT DEFAULT ''")

        cur = self.conn.execute("""
            SELECT 1 FROM information_schema.columns
            WHERE table_name='users' AND column_name='accepted_terms'
        """)
        column_existed = cur.fetchone() is not None
        self.conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS accepted_terms INTEGER DEFAULT 0")

        if not column_existed:
            self.conn.execute("UPDATE users SET accepted_terms=1")

        trial_col_existed = (self.conn.execute("""
            SELECT 1 FROM information_schema.columns
            WHERE table_name='users' AND column_name='trial_used'
        """)).fetchone() is not None
        self.conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS trial_used INTEGER DEFAULT 0")
        if not trial_col_existed:
            self.conn.execute("UPDATE users SET trial_used=1 WHERE accepted_terms=1")
        
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS promo_codes(
            code TEXT PRIMARY KEY,
            days INTEGER,
            uses INTEGER DEFAULT 0,
            max_uses INTEGER
        )
        """)

        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS used_promos(
            user_id BIGINT,
            code TEXT,
            PRIMARY KEY (user_id, code)
        )
        """)
        
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS referrals(
            user_id BIGINT PRIMARY KEY,
            invited_by BIGINT,
            bonus_given INTEGER DEFAULT 0
        )
        """)
        
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS admin_logs(
            id SERIAL PRIMARY KEY,
            admin_id BIGINT,
            action TEXT,
            target_id BIGINT,
            created_at TEXT
        )
        """)
        
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS notifications(
            user_id BIGINT,
            type TEXT,
            date TEXT,
            PRIMARY KEY (user_id, type)
        )
        """)
        
        self.conn.commit()

    def add_user(self, user_id, username, name):
        cursor = self.conn.execute("SELECT id FROM users WHERE id=?", (user_id,))
        if cursor.fetchone():
            return
        
        expire = datetime.now() + timedelta(days=0)
        expire_str = expire.strftime("%Y-%m-%d %H:%M:%S")
        
        self.conn.execute("""
            INSERT INTO users (id, username, name, expire_date, status, is_admin, invited_by, first_payment, last_tariff, username_history, balance, vless_key) 
            VALUES(?,?,?,?,?,0,0,0,'',?,0,'')
            ON CONFLICT (id) DO NOTHING
        """, (user_id, username or "", name or "", expire_str, "Отключено", json.dumps([])))
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
        try:
            history = json.loads(user[9] or '[]')
        except:
            history = []
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

    def disable_subscription(self, user_id):
        now_past = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d 00:00:00")
        self.conn.execute("UPDATE users SET expire_date=?, status='Отключено' WHERE id=?", (now_past, user_id))
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
        self.conn.execute("INSERT INTO used_promos (user_id, code) VALUES(?,?) ON CONFLICT DO NOTHING", (user_id, code))
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

class AdminDisableState(StatesGroup):
    waiting_username = State()

class AdminToggleState(StatesGroup):
    waiting_username = State()

class AdminProfileState(StatesGroup):
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
# ТАРИФЫ И КОНСТАНТЫ
############################################################

TARIFFS = {
    "month": {"name": "месяц", "days": 30, "price": 150, "stars": 150},
    "half": {"name": "полгода", "days": 180, "price": 800, "stars": 800},
    "year": {"name": "год", "days": 365, "price": 1600, "stars": 1600}
}

TRIAL_DAYS = 3
RUB_PER_DAY = 5

_last_action_time = {}

def is_rate_limited(user_id: int, action: str, cooldown: float) -> bool:
    key = (user_id, action)
    now = time.monotonic()
    last = _last_action_time.get(key, 0)
    if now - last < cooldown:
        return True
    _last_action_time[key] = now
    return False

TERMS_TEXT = """<b>УСЛОВИЯ ИСПОЛЬЗОВАНИЯ</b>

<b>1. Общие положения и терминология</b>
Настоящие Условия использования регулируют отношения между Пользователем и Сервисом Stopka VPN..."""

PRIVACY_TEXT = """<b>ПОЛИТИКА КОНФИДЕНЦИАЛЬНОСТИ</b>

<b>1. Какие данные собираются</b>
Для обеспечения работы Сервиса Stopka VPN может обрабатывать следующую информацию..."""

WELCOME_TEXT = (
    "🛡 <b>Stopka VPN</b>\n\n"
    f"Добро пожаловать! Мы дарим вам <b>{TRIAL_DAYS} дня</b> бесплатно 🎁\n\n"
    "Но для начала, пожалуйста, ознакомьтесь и примите:"
)

############################################################
# KEYBOARDS
############################################################

def profile_keyboard(is_admin=False):
    buttons = [
        [InlineKeyboardButton(text="💳 Оплата VPN", callback_data="payment")],
        [InlineKeyboardButton(text="📱 Добавить устройство", callback_data="get_vless_key")],
        [InlineKeyboardButton(text="🎁 Пригласить друга", callback_data="my_ref")],
        [InlineKeyboardButton(text="🎟 Промокод", callback_data="promo")]
    ]
    if is_admin:
        buttons.append([InlineKeyboardButton(text="🛠 Админ-панель", callback_data="admin")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def payment_method_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⭐️ Telegram Stars", callback_data="pay_type_stars")],
        [InlineKeyboardButton(text="💳 Любой картой", callback_data="pay_type_card")],
        [InlineKeyboardButton(text="⬅ Назад", callback_data="profile")]
    ])

def stars_payment_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"🗓 Месяц — {TARIFFS['month']['stars']} ⭐️", callback_data="stars_month")],
        [InlineKeyboardButton(text=f"📅 Полгода — {TARIFFS['half']['stars']} ⭐️", callback_data="stars_half")],
        [InlineKeyboardButton(text=f"📆 Год — {TARIFFS['year']['stars']} ⭐️", callback_data="stars_year")],
        [InlineKeyboardButton(text="⬅ Назад", callback_data="payment")]
    ])

def card_payment_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🗓 Месяц — 150₽", callback_data="pay_month")],
        [InlineKeyboardButton(text="📅 Полгода — 800₽", callback_data="pay_half")],
        [InlineKeyboardButton(text="📆 Год — 1600₽", callback_data="pay_year")],
        [InlineKeyboardButton(text="⬅ Назад", callback_data="payment")]
    ])

def manager_pay_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✈ Написать менеджеру", url="https://t.me/StopkaPayments_bot")],
        [InlineKeyboardButton(text="⬅ Назад в меню оплаты", callback_data="pay_type_card")]
    ])

def back_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅ Назад", callback_data="profile")]
    ])

def admin_back_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅ Назад в админ-панель", callback_data="admin")]
    ])

def support_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📨 Написать в поддержку", callback_data="create_ticket")]
    ])

def admin_keyboard():
    buttons = [
        [InlineKeyboardButton(text="👤 Просмотр профиля", callback_data="admin_view_profile")],
        [InlineKeyboardButton(text="🚫 Отключить подписку", callback_data="admin_disable")],
        [InlineKeyboardButton(text="📅 Выдать дни подписки", callback_data="admin_give")],
        [InlineKeyboardButton(text="👥 Статистика пользователей", callback_data="users_count")],
        [InlineKeyboardButton(text="📢 Рассылка", callback_data="broadcast")],
        [InlineKeyboardButton(text="🎟 Тикеты", callback_data="admin_tickets")],
        [InlineKeyboardButton(text="🎁 Промокоды", callback_data="promo_admin")],
        [InlineKeyboardButton(text="👑 Назначить/Удалить админа", callback_data="admin_toggle")],
        [InlineKeyboardButton(text="⬅ Главное меню", callback_data="profile")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def ticket_list_keyboard(tickets):
    buttons = []
    for ticket in tickets:
        preview = ticket[2][:20] if ticket[2] else ""
        if not preview:
            file_type = ticket[6] if len(ticket) > 6 else ""
            preview = "📷 Фото" if file_type == "photo" else ("📎 Файл" if file_type == "document" else "…")
        buttons.append([InlineKeyboardButton(text=f"🎟 #{ticket[0]} | {html.escape(preview)}", callback_data=f"ticket_{ticket[0]}")])
    buttons.append([InlineKeyboardButton(text="⬅ Назад в админ-панель", callback_data="admin")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def promo_admin_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Создать промокод", callback_data="promo_create")],
        [InlineKeyboardButton(text="📋 Список промокодов", callback_data="promo_list")],
        [InlineKeyboardButton(text="🗑 Очистить использованные", callback_data="promo_clear_confirm")],
        [InlineKeyboardButton(text="⬅ Назад в админ-панель", callback_data="admin")]
    ])

def promo_clear_confirm_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Да, очистить", callback_data="promo_clear_yes")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="promo_admin")]
    ])

def welcome_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📜 Условия пользования", callback_data="show_terms")],
        [InlineKeyboardButton(text="🔒 Политика конфиденциальности", callback_data="show_privacy")],
        [InlineKeyboardButton(text="✅ Подключить", callback_data="accept_terms")]
    ])

def legal_back_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅ Назад", callback_data="welcome_back")]
    ])

############################################################
# MIDDLEWARES & HELPERS
############################################################

@dp.message.middleware()
async def rate_limit_middleware(handler, message: Message, data: dict):
    if not rate_limiter.is_allowed(message.from_user.id):
        await message.answer("⏳ Слишком много запросов. Подождите немного.")
        return
    return await handler(message, data)

def calculate_days(expire_str):
    try:
        expire = datetime.strptime(expire_str, "%Y-%m-%d %H:%M:%S")
    except:
        try:
            expire = datetime.strptime(expire_str, "%Y-%m-%d")
        except:
            return 0
    now = datetime.now()
    if expire > now:
        return max(0, (expire.date() - now.date()).days)
    return 0

def build_profile_text(user_id, user_data):
    days = calculate_days(user_data[3])
    vpn_status = "✅ Активен" if days > 0 else "❌ Не активен"
    balance = days * RUB_PER_DAY

    text = (
        f"Stopka VPN🛡️\n\n"
        f"😎 Мой профиль\n"
        f"┌ 🆔 ID: <code>{user_id}</code>\n"
        f"├ ⭐ Подписка: Premium\n"
        f"├ 📱 Устройств: до 5\n"
        f"├ 💳 Баланс: {balance}₽ · Осталось: {days} дней\n"
        f"└ 🔑 VPN: {vpn_status}"
    )
    return text

async def safe_edit(callback: CallbackQuery, text: str, reply_markup=None):
    try:
        await callback.message.edit_text(text, reply_markup=reply_markup)
    except Exception:
        try:
            await callback.message.delete()
        except:
            pass
        await callback.message.answer(text, reply_markup=reply_markup)

async def render_profile(user_id, target_message=None, callback=None, user=None):
    if user is None:
        user = await asyncio.to_thread(db.get_user, user_id)
    if not user:
        if target_message:
            await asyncio.to_thread(db.add_user, user_id, target_message.from_user.username or "", target_message.from_user.full_name)
            user = await asyncio.to_thread(db.get_user, user_id)

    text = build_profile_text(user_id, user)
    is_admin_flag = (user_id == OWNER_ID) or (user is not None and user[5] == 1)
    kb = profile_keyboard(is_admin_flag)

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
    username = message.from_user.username or ""

    user = await asyncio.to_thread(db.get_user, user_id)
    if user:
        if (user[1] or "") != username:
            await asyncio.to_thread(db.update_username, user_id, username)
            user = await asyncio.to_thread(db.get_user, user_id)
    else:
        await asyncio.to_thread(db.add_user, user_id, username, message.from_user.full_name)
        user = await asyncio.to_thread(db.get_user, user_id)

    if user_id == OWNER_ID:
        await asyncio.to_thread(db.set_admin, user_id, True)
    
    args = message.text.split()
    if len(args) > 1:
        ref = args[1]
        if ref.startswith("STOPKA"):
            try:
                inviter = int(ref.replace("STOPKA", ""))
                if inviter != user_id:
                    cursor = await asyncio.to_thread(db.conn.execute, "SELECT invited_by FROM users WHERE id=?", (user_id,))
                    existing = cursor.fetchone()
                    if existing and existing[0] == 0:
                        await asyncio.to_thread(db.conn.execute, "UPDATE users SET invited_by=? WHERE id=?", (inviter, user_id))
                        await asyncio.to_thread(db.conn.execute, "INSERT INTO referrals (user_id, invited_by, bonus_given) VALUES(?,?,0) ON CONFLICT (user_id) DO NOTHING", (user_id, inviter))
                        await asyncio.to_thread(db.conn.commit)
            except Exception as e:
                logging.error(f"Ошибка обработки реферала: {e}")

    # Гарантированная подгрузка свежих данных пользователя
    user = await asyncio.to_thread(db.get_user, user_id)
    trial_used = user is not None and len(user) > 13 and user[13] == 1

    if not trial_used:
        await message.answer(WELCOME_TEXT, reply_markup=welcome_keyboard())
        return

    await render_profile(user_id, target_message=message, user=user)

@dp.message(Command("help"))
async def help_command(message: Message):
    await message.answer(
        "🛡 <b>Поддержка Stopka VPN</b>\n\n"
        "Опишите свой вопрос или проблему, а также приложите фото или файл — так мы сможем помочь быстрее.",
        reply_markup=support_keyboard()
    )

@dp.message(Command("about"))
async def about_command(message: Message):
    await message.answer("👨‍💻 Создатели: @prostokiril, @ll1_what")

############################################################
# ПРИВЕТСТВЕННЫЙ ЭКРАН & ПОЛИТИКИ
############################################################

@dp.callback_query(F.data == "show_terms")
async def show_terms(callback: CallbackQuery):
    await callback.message.edit_text(TERMS_TEXT, reply_markup=legal_back_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "show_privacy")
async def show_privacy(callback: CallbackQuery):
    await callback.message.edit_text(PRIVACY_TEXT, reply_markup=legal_back_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "welcome_back")
async def welcome_back(callback: CallbackQuery):
    await callback.answer()
    try:
        await callback.message.delete()
    except:
        pass
    await callback.message.answer(WELCOME_TEXT, reply_markup=welcome_keyboard())

@dp.callback_query(F.data == "accept_terms")
async def accept_terms(callback: CallbackQuery):
    user_id = callback.from_user.id

    await asyncio.to_thread(
        db.add_user, user_id, callback.from_user.username or "", callback.from_user.full_name
    )

    user = await asyncio.to_thread(db.get_user, user_id)
    trial_used = user is not None and len(user) > 13 and user[13] == 1

    if not trial_used:
        # Пробный период выдается strictly один раз за всю историю
        expire = datetime.now() + timedelta(days=TRIAL_DAYS)
        expire_str = expire.strftime("%Y-%m-%d 23:59:59")
        await asyncio.to_thread(
            db.conn.execute,
            "UPDATE users SET accepted_terms=1, trial_used=1, expire_date=?, status='Активно' WHERE id=?",
            (expire_str, user_id)
        )
        alert_text = f"🎉 Вам начислено {TRIAL_DAYS} дня VPN!"
    else:
        await asyncio.to_thread(db.conn.execute, "UPDATE users SET accepted_terms=1 WHERE id=?", (user_id,))
        alert_text = "✅ Условия приняты!"

    await asyncio.to_thread(db.conn.commit)
    user = await asyncio.to_thread(db.get_user, user_id)

    await callback.answer(alert_text, show_alert=True)

    text = build_profile_text(user_id, user)
    is_admin_flag = (user_id == OWNER_ID) or (user is not None and user[5] == 1)
    try:
        await callback.message.delete()
    except:
        pass
    await callback.message.answer(text, reply_markup=profile_keyboard(is_admin_flag))

############################################################
# PROFILE & VLESS KEY LOGIC
############################################################

@dp.callback_query(F.data == "profile")
async def profile_callback(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await render_profile(callback.from_user.id, callback=callback)

@dp.callback_query(F.data == "get_vless_key")
async def get_vless_key(callback: CallbackQuery):
    user_id = callback.from_user.id
    user = await asyncio.to_thread(db.get_user, user_id)
    days = calculate_days(user[3])

    if days <= 0:
        await callback.answer("❌ Подписка неактивна. Оплатите дни, чтобы получить ключ.", show_alert=True)
        return

    vless_key = user[11]
    if not vless_key:
        try:
            expire_dt = datetime.strptime(user[3], "%Y-%m-%d %H:%M:%S")
            timestamp = int(expire_dt.timestamp())
            vless_key = await vpn_client.create_or_update_user(user_id, timestamp)
            await asyncio.to_thread(db.conn.execute, "UPDATE users SET vless_key=? WHERE id=?", (vless_key, user_id))
            await asyncio.to_thread(db.conn.commit)
        except Exception as e:
            logging.error(f"Ошибка генерации ключа: {e}")

    if not vless_key:
        vless_key = f"vless://error-check-api-connection@panel:443?encryption=none&security=reality#StopkaVPN"

    await callback.message.edit_text(
        f"🔑 <b>Ваш ключ VLESS для Happ:</b>\n\n"
        f"Скопируйте эту строку и добавьте в приложение Happ:\n\n"
        f"<code>{vless_key}</code>",
        reply_markup=back_keyboard()
    )
    await callback.answer()

############################################################
# PAYMENTS
############################################################

@dp.callback_query(F.data == "payment")
async def payment_method_select(callback: CallbackQuery):
    await callback.message.edit_text(
        "💳 <b>Выберите способ оплаты:</b>",
        reply_markup=payment_method_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data == "pay_type_card")
async def payment_card_menu(callback: CallbackQuery):
    await callback.message.edit_text(
        "💳 <b>Выберите тарифный план (Оплата картой):</b>\n\n"
        "При покупке дни автоматически добавятся к вашей текущей подписке.",
        reply_markup=card_payment_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data == "pay_type_stars")
async def payment_stars_menu(callback: CallbackQuery):
    await callback.message.edit_text(
        "⭐️ <b>Выберите тарифный план (Оплата Telegram Stars):</b>\n\n"
        "Оплата произойдет мгновенно прямо в Telegram!",
        reply_markup=stars_payment_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("pay_"))
async def process_pay(callback: CallbackQuery):
    tariff_id = callback.data.split("_")[1]
    tariff = TARIFFS.get(tariff_id)
    if not tariff:
        await callback.answer("Ошибка выбора тарифа", show_alert=True)
        return

    msg_to_copy = f"Здравствуйте! Я по поводу оплаты {tariff['name']} за {tariff['price']} ₽"

    await callback.message.edit_text(
        f"💳 <b>Оплата тарифного плана</b>\n\n"
        f"Для оплаты напишите менеджеру в @StopkaPayments_bot.\n\n"
        f"📌 <b>Напишите ему это сообщение:</b>\n"
        f"<code>{msg_to_copy}</code>",
        reply_markup=manager_pay_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("stars_"))
async def process_stars_pay(callback: CallbackQuery):
    tariff_id = callback.data.split("_")[1]
    tariff = TARIFFS.get(tariff_id)
    if not tariff:
        await callback.answer("Ошибка выбора тарифа", show_alert=True)
        return

    prices = [LabeledPrice(label=f"Подписка Stopka VPN ({tariff['name']})", amount=tariff["stars"])]

    await bot.send_invoice(
        chat_id=callback.from_user.id,
        title=f"Подписка Stopka VPN — {tariff['name'].capitalize()}",
        description=f"Продление подписки VPN на {tariff['days']} дней",
        payload=f"stars_{tariff_id}_{callback.from_user.id}_{int(time.time())}",
        provider_token="",
        currency="XTR",
        prices=prices
    )
    await callback.answer()

@dp.pre_checkout_query()
async def process_pre_checkout(pre_checkout_query: PreCheckoutQuery):
    await bot.answer_pre_checkout_query(pre_checkout_query.id, ok=True)

@dp.message(F.successful_payment)
async def process_successful_payment(message: Message):
    payload = message.successful_payment.invoice_payload
    user_id = message.from_user.id
    
    if payload.startswith("stars_"):
        parts = payload.split("_")
        tariff_id = parts[1]
        tariff = TARIFFS.get(tariff_id)
        if tariff:
            days = tariff["days"]
            user = await asyncio.to_thread(db.get_user, user_id)
            if user:
                try:
                    expire = datetime.strptime(user[3], "%Y-%m-%d %H:%M:%S")
                except:
                    expire = datetime.now()
                
                now = datetime.now()
                if expire < now:
                    expire = now
                
                new_expire = expire + timedelta(days=days)
                new_expire_str = new_expire.strftime("%Y-%m-%d 23:59:59")
                
                await asyncio.to_thread(db.conn.execute, "UPDATE users SET expire_date=?, status='Активно', last_tariff=? WHERE id=?", (new_expire_str, tariff['name'], user_id))
                await asyncio.to_thread(db.conn.commit)

                try:
                    await vpn_client.create_or_update_user(user_id, int(new_expire.timestamp()))
                except:
                    pass

                if not user[7]: 
                    await asyncio.to_thread(db.conn.execute, "UPDATE users SET first_payment=1 WHERE id=?", (user_id,))
                    await asyncio.to_thread(db.conn.commit)

                    inviter_id = user[6]
                    if inviter_id:
                        row = (await asyncio.to_thread(
                            db.conn.execute, "SELECT bonus_given FROM referrals WHERE user_id=?", (user_id,)
                        )).fetchone()
                        if row and row[0] == 0:
                            inviter = await asyncio.to_thread(db.get_user, inviter_id)
                            if inviter:
                                try:
                                    inv_expire = datetime.strptime(inviter[3], "%Y-%m-%d %H:%M:%S")
                                except:
                                    inv_expire = datetime.now()
                                if inv_expire < datetime.now():
                                    inv_expire = datetime.now()
                                inv_new_expire = inv_expire + timedelta(days=REFERRAL_DAYS)
                                inv_new_expire_str = inv_new_expire.strftime("%Y-%m-%d 23:59:59")

                                await asyncio.to_thread(db.conn.execute, "UPDATE users SET expire_date=?, status='Активно' WHERE id=?", (inv_new_expire_str, inviter_id))
                                await asyncio.to_thread(db.conn.execute, "UPDATE referrals SET bonus_given=1 WHERE user_id=?", (user_id,))
                                await asyncio.to_thread(db.conn.commit)

                                try:
                                    await vpn_client.create_or_update_user(inviter_id, int(inv_new_expire.timestamp()))
                                except:
                                    pass
                                try:
                                    await bot.send_message(
                                        inviter_id,
                                        f"🎁 Ваш друг оформил подписку по вашей ссылке!\nВам начислено +{REFERRAL_DAYS} дней VPN."
                                    )
                                except:
                                    pass

                await message.answer(
                    f"🎉 <b>Оплата прошла успешно!</b>\n\n"
                    f"Вам добавлено <b>+{days} дней</b> подписки.\n"
                    f"Подписка активна до: <b>{new_expire_str}</b>",
                    reply_markup=back_keyboard()
                )

############################################################
# REFERRAL & PROMO
############################################################

@dp.callback_query(F.data == "my_ref")
async def my_ref(callback: CallbackQuery):
    global BOT_USERNAME
    if not BOT_USERNAME:
        bot_info = await bot.get_me()
        BOT_USERNAME = bot_info.username
    link = f"https://t.me/{BOT_USERNAME}?start=STOPKA{callback.from_user.id}"
    ref_count = await asyncio.to_thread(db.get_referral_count, callback.from_user.id)
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

    if is_rate_limited(user_id, "promo_attempt", cooldown=3):
        await message.answer("⏳ Слишком часто — попробуйте через пару секунд.")
        return

    if await asyncio.to_thread(db.is_promo_used, user_id, code):
        await message.answer("❌ Вы уже активировали этот промокод!")
        await state.clear()
        return

    promo = (await asyncio.to_thread(db.conn.execute, "SELECT * FROM promo_codes WHERE code=?", (code,))).fetchone()
    if not promo:
        await message.answer("❌ Промокод не найден")
        await state.clear()
        return

    if promo[2] >= promo[3]:
        await message.answer("❌ Лимит использований промокода исчерпан")
        await state.clear()
        return

    user = await asyncio.to_thread(db.get_user, user_id)
    if not user:
        await message.answer("❌ Ошибка пользователя")
        await state.clear()
        return

    try:
        expire = datetime.strptime(user[3], "%Y-%m-%d %H:%M:%S")
    except:
        expire = datetime.now()

    now = datetime.now()
    if expire < now:
        expire = now

    days = promo[1]
    new_expire = expire + timedelta(days=days)
    new_expire_str = new_expire.strftime("%Y-%m-%d 23:59:59")

    await asyncio.to_thread(db.conn.execute, "UPDATE users SET expire_date=?, status='Активно' WHERE id=?", (new_expire_str, user_id))
    await asyncio.to_thread(db.conn.execute, "UPDATE promo_codes SET uses=uses+1 WHERE code=?", (code,))
    await asyncio.to_thread(db.mark_promo_used, user_id, code)
    await asyncio.to_thread(db.conn.commit)

    try:
        await vpn_client.create_or_update_user(user_id, int(new_expire.timestamp()))
    except Exception as e:
        logging.error(f"Ошибка синхронизации с VPN панелью после промокода: {e}")

    await state.clear()
    await message.answer(f"✅ Промокод активирован! Добавлено +{days} дней.")

############################################################
# SUPPORT TICKETS
############################################################

@dp.callback_query(F.data == "create_ticket")
async def create_ticket(callback: CallbackQuery, state: FSMContext):
    await state.set_state(TicketState.waiting_text)
    await callback.message.edit_text(
        "📝 Опишите вашу проблему или напишите по поводу оплаты в одном сообщении.\n\n"
        "Можно приложить фото или файл (например, скриншот или чек):",
        reply_markup=back_keyboard()
    )
    await callback.answer()

@dp.message(TicketState.waiting_text, F.text | F.photo | F.document)
async def process_ticket(message: Message, state: FSMContext):
    if is_rate_limited(message.from_user.id, "create_ticket", cooldown=10):
        await message.answer("⏳ Обращение уже отправляется — подождите немного перед следующим.")
        return

    text = (message.text or message.caption or "").strip()
    file_id = ""
    file_type = ""
    if message.photo:
        file_id = message.photo[-1].file_id
        file_type = "photo"
    elif message.document:
        file_id = message.document.file_id
        file_type = "document"

    if not text and not file_id:
        await message.answer("❌ Пришлите текст, фото или файл с описанием проблемы.")
        return

    await asyncio.to_thread(db.conn.execute, 
        "INSERT INTO tickets (user_id, message, answer, status, file_id, file_type) VALUES(?,?,?,?,?,?)",
        (message.from_user.id, text, "", "Открыт", file_id, file_type)
    )
    await asyncio.to_thread(db.conn.commit)
    await state.clear()
    await message.answer("✅ Ваше обращение отправлено в поддержку!", reply_markup=back_keyboard())

############################################################
# ADMIN PANEL
############################################################

@dp.callback_query(F.data == "admin")
async def admin_panel(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    if not await asyncio.to_thread(db.is_admin, callback.from_user.id):
        await callback.answer("❌ Нет доступа. Вы не администратор!", show_alert=True)
        return

    await safe_edit(
        callback,
        "🛠 <b>Панель администратора</b>",
        reply_markup=admin_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data == "admin_view_profile")
async def admin_view_profile_start(callback: CallbackQuery, state: FSMContext):
    if not await asyncio.to_thread(db.is_admin, callback.from_user.id):
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    await state.set_state(AdminProfileState.waiting_username)
    await callback.message.edit_text(
        "👤 <b>Просмотр профиля пользователя</b>\n\n"
        "Введите `@username` или `ID` пользователя:",
        reply_markup=admin_back_keyboard()
    )
    await callback.answer()

@dp.message(AdminProfileState.waiting_username)
async def admin_view_profile_finish(message: Message, state: FSMContext):
    if not await asyncio.to_thread(db.is_admin, message.from_user.id):
        return

    user_input = message.text.strip()
    user = await asyncio.to_thread(db.get_username, user_input)

    if not user:
        await message.answer("❌ Пользователь не найден", reply_markup=admin_back_keyboard())
        await state.clear()
        return

    target_id = user[0]
    profile_text = build_profile_text(target_id, user)
    
    username_info = f"@{user[1]}" if user[1] else "Отсутствует"
    name_info = user[2] or "Не указано"
    
    full_info = (
        f"📊 <b>Информация о пользователе:</b>\n"
        f"👤 Имя: {html.escape(name_info)}\n"
        f"🏷 Юзернейм: {username_info}\n\n"
        f"{profile_text}"
    )

    await state.clear()
    await message.answer(full_info, reply_markup=admin_back_keyboard())

@dp.callback_query(F.data == "admin_disable")
async def admin_disable_start(callback: CallbackQuery, state: FSMContext):
    if not await asyncio.to_thread(db.is_admin, callback.from_user.id):
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    await state.set_state(AdminDisableState.waiting_username)
    await callback.message.edit_text(
        "🚫 <b>Отключение подписки</b>\n\n"
        "Введите `@username` или `ID` пользователя, у которого нужно отключить подписку:",
        reply_markup=admin_back_keyboard()
    )
    await callback.answer()

@dp.message(AdminDisableState.waiting_username)
async def admin_disable_finish(message: Message, state: FSMContext):
    if not await asyncio.to_thread(db.is_admin, message.from_user.id):
        return

    user_input = message.text.strip()
    user = await asyncio.to_thread(db.get_username, user_input)

    if not user:
        await message.answer("❌ Пользователь не найден", reply_markup=admin_back_keyboard())
        await state.clear()
        return

    await asyncio.to_thread(db.disable_subscription, user[0])
    await asyncio.to_thread(db.add_admin_log, message.from_user.id, "Отключил подписку", user[0])

    try:
        await bot.send_message(user[0], "❌ Ваша подписка Stopka VPN была отключена администратором.")
    except:
        pass

    await state.clear()
    await message.answer(f"✅ Подписка для пользователя {user[1] or user[0]} успешно отключена!", reply_markup=admin_back_keyboard())

@dp.callback_query(F.data == "users_count")
async def users_count(callback: CallbackQuery):
    if not await asyncio.to_thread(db.is_admin, callback.from_user.id):
        await callback.answer("❌ Нет прав", show_alert=True)
        return
    total = await asyncio.to_thread(db.get_total_users_count)
    await callback.message.edit_text(
        f"👥 <b>Статистика пользователей</b>\n\n"
        f"Всего пользователей: <b>{total}</b>",
        reply_markup=admin_back_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data == "admin_toggle")
async def admin_toggle_start(callback: CallbackQuery, state: FSMContext):
    if not await asyncio.to_thread(db.is_admin, callback.from_user.id):
        await callback.answer("❌ Нет прав", show_alert=True)
        return
    await state.set_state(AdminToggleState.waiting_username)
    await callback.message.edit_text(
        "👑 <b>Назначить / Удалить админа</b>\n\n"
        "Введите `@username` или `ID` пользователя:",
        reply_markup=admin_back_keyboard()
    )
    await callback.answer()

@dp.message(AdminToggleState.waiting_username)
async def admin_toggle_finish(message: Message, state: FSMContext):
    if not await asyncio.to_thread(db.is_admin, message.from_user.id):
        return

    user_input = message.text.strip()
    user = await asyncio.to_thread(db.get_username, user_input)

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
    await asyncio.to_thread(db.set_admin, target_id, new_status)
    
    status_str = "теперь администратор" if new_status else "больше не администратор"
    await asyncio.to_thread(db.add_admin_log, message.from_user.id, f"Изменил статус админа на {new_status}", target_id)
    
    await state.clear()
    await message.answer(f"✅ Пользователь {user[1] or target_id} {status_str}!", reply_markup=admin_back_keyboard())

@dp.callback_query(F.data == "admin_give")
async def admin_give(callback: CallbackQuery, state: FSMContext):
    if not await asyncio.to_thread(db.is_admin, callback.from_user.id):
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
    if not await asyncio.to_thread(db.is_admin, message.from_user.id):
        return
    try:
        username, days_str = message.text.split()
        days = int(days_str)
    except:
        await message.answer("❌ Неверный формат. Пример: `@user 30`", reply_markup=admin_back_keyboard())
        return

    user = await asyncio.to_thread(db.get_username, username)
    if not user:
        await message.answer("❌ Пользователь не найден", reply_markup=admin_back_keyboard())
        await state.clear()
        return

    try:
        expire = datetime.strptime(user[3], "%Y-%m-%d %H:%M:%S")
    except:
        expire = datetime.now()

    now = datetime.now()
    if expire < now:
        expire = now

    expire += timedelta(days=days)
    expire_str = expire.strftime("%Y-%m-%d 23:59:59")

    await asyncio.to_thread(db.conn.execute, "UPDATE users SET expire_date=?, status='Активно' WHERE id=?", (expire_str, user[0]))
    await asyncio.to_thread(db.conn.commit)
    await asyncio.to_thread(db.add_admin_log, message.from_user.id, f"Выдал {days} дней", user[0])

    try:
        await vpn_client.create_or_update_user(user[0], int(expire.timestamp()))
    except:
        pass

    await state.clear()
    await message.answer(f"✅ Выдано {days} дней пользователю {username}", reply_markup=admin_back_keyboard())

@dp.callback_query(F.data == "admin_tickets")
async def admin_tickets(callback: CallbackQuery):
    if not await asyncio.to_thread(db.is_admin, callback.from_user.id):
        await callback.answer("❌ Нет прав", show_alert=True)
        return
    cursor = await asyncio.to_thread(db.conn.execute, "SELECT * FROM tickets WHERE status='Открыт'")
    tickets = cursor.fetchall()
    if not tickets:
        await safe_edit(callback, "🎟 Открытых тикетов нет", reply_markup=admin_back_keyboard())
        return
    await safe_edit(callback, "🎟 <b>Открытые обращения:</b>", reply_markup=ticket_list_keyboard(tickets))

@dp.callback_query(F.data.startswith("ticket_"))
async def open_ticket(callback: CallbackQuery):
    if not await asyncio.to_thread(db.is_admin, callback.from_user.id):
        return
    ticket_id = int(callback.data.split("_")[1])
    ticket = (await asyncio.to_thread(db.conn.execute, "SELECT * FROM tickets WHERE id=?", (ticket_id,))).fetchone()
    if not ticket:
        await callback.answer("Тикет не найден", show_alert=True)
        return
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✉️ Ответить", callback_data=f"reply_{ticket_id}")],
        [InlineKeyboardButton(text="❌ Закрыть", callback_data=f"close_{ticket_id}")],
        [InlineKeyboardButton(text="⬅ Назад в админ-панель", callback_data="admin")]
    ])
    caption = f"🎟 <b>Тикет #{ticket[0]}</b>\nПользователь ID: {ticket[1]}\n\nСообщение:\n{ticket[2] or '—'}"
    file_id = ticket[5] if len(ticket) > 5 else ""
    file_type = ticket[6] if len(ticket) > 6 else ""

    if file_type == "photo" and file_id:
        await callback.message.delete()
        await callback.message.answer_photo(photo=file_id, caption=caption, reply_markup=keyboard)
    elif file_type == "document" and file_id:
        await callback.message.delete()
        await callback.message.answer_document(document=file_id, caption=caption, reply_markup=keyboard)
    else:
        await callback.message.edit_text(caption, reply_markup=keyboard)
    await callback.answer()

@dp.callback_query(F.data.startswith("reply_"))
async def reply_ticket(callback: CallbackQuery, state: FSMContext):
    if not await asyncio.to_thread(db.is_admin, callback.from_user.id):
        return
    ticket_id = int(callback.data.split("_")[1])
    await state.update_data(ticket_id=ticket_id)
    await state.set_state(ReplyState.waiting_answer)
    await safe_edit(callback, "✉️ Введите текст ответа:", reply_markup=admin_back_keyboard())
    await callback.answer()

@dp.message(ReplyState.waiting_answer)
async def send_ticket_answer(message: Message, state: FSMContext):
    if not await asyncio.to_thread(db.is_admin, message.from_user.id):
        return
    data = await state.get_data()
    ticket_id = data["ticket_id"]
    ticket = (await asyncio.to_thread(db.conn.execute, "SELECT * FROM tickets WHERE id=?", (ticket_id,))).fetchone()
    if ticket:
        try:
            await bot.send_message(ticket[1], f"📩 <b>Ответ поддержки:</b>\n\n{message.text}")
        except:
            pass
        await asyncio.to_thread(db.conn.execute, "UPDATE tickets SET answer=?, status='Закрыт', closed_at=? WHERE id=?", (message.text, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), ticket_id))
        await asyncio.to_thread(db.conn.commit)
    await state.clear()
    await message.answer("✅ Ответ отправлен!", reply_markup=admin_back_keyboard())

@dp.callback_query(F.data.startswith("close_"))
async def close_ticket(callback: CallbackQuery):
    if not await asyncio.to_thread(db.is_admin, callback.from_user.id):
        return
    ticket_id = int(callback.data.split("_")[1])
    await asyncio.to_thread(db.conn.execute, "UPDATE tickets SET status='Закрыт', closed_at=? WHERE id=?", (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), ticket_id))
    await asyncio.to_thread(db.conn.commit)
    await callback.answer("✅ Тикет закрыт")
    await admin_tickets(callback)

@dp.callback_query(F.data == "promo_admin")
async def promo_admin(callback: CallbackQuery):
    if not await asyncio.to_thread(db.is_admin, callback.from_user.id):
        return
    await callback.message.edit_text("🎁 <b>Управление промокодами</b>", reply_markup=promo_admin_keyboard())

@dp.callback_query(F.data == "promo_list")
async def promo_list(callback: CallbackQuery):
    if not await asyncio.to_thread(db.is_admin, callback.from_user.id):
        return
    promos = (await asyncio.to_thread(db.conn.execute, "SELECT code, days, uses, max_uses FROM promo_codes")).fetchall()
    if not promos:
        await callback.message.edit_text("📋 Промокодов нет", reply_markup=promo_admin_keyboard())
        return
    text = "📋 <b>Список промокодов:</b>\n\n"
    for p in promos:
        text += f"🎟 {p[0]}: +{p[1]} дней ({p[2]}/{p[3]})\n"
    await callback.message.edit_text(text, reply_markup=promo_admin_keyboard())

@dp.callback_query(F.data == "promo_clear_confirm")
async def promo_clear_confirm(callback: CallbackQuery):
    if not await asyncio.to_thread(db.is_admin, callback.from_user.id):
        return
    await callback.message.edit_text(
        "🗑 <b>Очистка промокодов</b>\n\n"
        "Будут удалены все промокоды, которые уже <b>полностью использованы</b>.\n\n"
        "Продолжить?",
        reply_markup=promo_clear_confirm_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data == "promo_clear_yes")
async def promo_clear_yes(callback: CallbackQuery):
    if not await asyncio.to_thread(db.is_admin, callback.from_user.id):
        return
    cursor = await asyncio.to_thread(
        db.conn.execute,
        "DELETE FROM promo_codes WHERE uses >= max_uses"
    )
    deleted = cursor.rowcount
    await asyncio.to_thread(db.conn.commit)
    await callback.message.edit_text(
        f"✅ Удалено полностью использованных промокодов: <b>{deleted}</b>",
        reply_markup=promo_admin_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data == "promo_create")
async def promo_create_start(callback: CallbackQuery, state: FSMContext):
    if not await asyncio.to_thread(db.is_admin, callback.from_user.id):
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
        await asyncio.to_thread(db.conn.execute, 
            "INSERT INTO promo_codes (code, days, uses, max_uses) VALUES(?,?,0,?) ON CONFLICT DO NOTHING",
            (data['code'], data['days'], max_uses)
        )
        await asyncio.to_thread(db.conn.commit)
        await state.clear()
        await message.answer(f"✅ Промокод `{data['code']}` создан!", reply_markup=admin_back_keyboard())
    except Exception as e:
        await message.answer(f"Ошибка: {e}", reply_markup=admin_back_keyboard())

@dp.callback_query(F.data == "broadcast")
async def broadcast_start(callback: CallbackQuery, state: FSMContext):
    if not await asyncio.to_thread(db.is_admin, callback.from_user.id):
        return
    await state.set_state(BroadcastState.waiting_text)
    await callback.message.edit_text(
        "📢 Отправьте сообщение для рассылки:",
        reply_markup=admin_back_keyboard()
    )

@dp.message(BroadcastState.waiting_text)
async def broadcast_finish(message: Message, state: FSMContext):
    if not await asyncio.to_thread(db.is_admin, message.from_user.id):
        return
    users = (await asyncio.to_thread(db.conn.execute, "SELECT id FROM users")).fetchall()
    count = 0
    for u in users:
        try:
            await message.copy_to(chat_id=u[0])
            count += 1
            await asyncio.sleep(0.05)
        except:
            pass
    await state.clear()
    await message.answer(f"✅ Рассылка завершена! Доставлено {count} пользователям.", reply_markup=admin_back_keyboard())

############################################################
# BACKGROUND TASKS
############################################################

async def subscription_checker():
    while True:
        try:
            users = (await asyncio.to_thread(db.conn.execute, "SELECT id, expire_date, status FROM users")).fetchall()
            for u in users:
                user_id, expire_str, status = u[0], u[1], u[2]
                try:
                    expire = datetime.strptime(expire_str, "%Y-%m-%d %H:%M:%S")
                except:
                    continue
                
                now = datetime.now()
                if expire < now and status == "Активно":
                    await asyncio.to_thread(db.disable_subscription, user_id)
                    try:
                        await vpn_client.disable_user(user_id)
                    except Exception as e:
                        logging.error(f"Не удалось отключить пользователя {user_id} в VPN панели: {e}")
                    try:
                        await bot.send_message(user_id, "❌ Ваша подписка на VPN истекла. Ключ отключен, продлите подписку для возобновления доступа.")
                    except:
                        pass
                
                days = (expire - now).days
                if days == 3 and status == "Активно" and not await asyncio.to_thread(db.notification_sent, user_id, "3days"):
                    try:
                        await bot.send_message(user_id, "⏰ Ваша подписка Stopka VPN закончится через 3 дня!")
                        await asyncio.to_thread(db.save_notification, user_id, "3days")
                    except:
                        pass
        except Exception as e:
            logging.error(f"Ошибка проверки подписок: {e}")
        await asyncio.sleep(3600)

LOGS_RETENTION_DAYS = 7
CLOSED_TICKETS_RETENTION_DAYS = 30
CLEANUP_INTERVAL_SECONDS = 24 * 3600

async def db_cleanup_task():
    while True:
        try:
            log_cutoff = (datetime.now() - timedelta(days=LOGS_RETENTION_DAYS)).strftime("%Y-%m-%d %H:%M:%S")
            ticket_cutoff = (datetime.now() - timedelta(days=CLOSED_TICKETS_RETENTION_DAYS)).strftime("%Y-%m-%d %H:%M:%S")

            cur1 = await asyncio.to_thread(db.conn.execute, "DELETE FROM admin_logs WHERE created_at < ?", (log_cutoff,))
            cur2 = await asyncio.to_thread(db.conn.execute, "DELETE FROM notifications WHERE date < ?", (log_cutoff[:10],))
            cur3 = await asyncio.to_thread(
                db.conn.execute,
                "DELETE FROM tickets WHERE status='Закрыт' AND closed_at != '' AND closed_at < ?",
                (ticket_cutoff,)
            )

            if cur1.rowcount or cur2.rowcount or cur3.rowcount:
                logging.info(
                    f"🧹 Автоочистка БД: admin_logs -{cur1.rowcount}, "
                    f"notifications -{cur2.rowcount}, закрытых тикетов -{cur3.rowcount}"
                )
        except Exception as e:
            logging.error(f"Ошибка автоочистки БД: {e}")
        await asyncio.sleep(CLEANUP_INTERVAL_SECONDS)

async def set_commands():
    commands = [
        BotCommand(command="start", description="🚀 Запустить бота"),
        BotCommand(command="help", description="❓ Помощь и Поддержка"),
        BotCommand(command="about", description="👨‍💻 О создателях")
    ]
    await bot.set_my_commands(commands)

@dp.error()
async def error_handler(event: ErrorEvent):
    exception = event.exception
    logging.error("Необработанная ошибка:\n" + "".join(
        traceback.format_exception(type(exception), exception, exception.__traceback__)
    ))
    try:
        if event.update and event.update.message:
            await event.update.message.answer("⚠️ Произошла ошибка, попробуйте ещё раз /start")
    except Exception:
        pass
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
    global BOT_USERNAME
    logging.info("🚀 Запуск Stopka VPN...")

    bot_info = await bot.get_me()
    BOT_USERNAME = bot_info.username

    await asyncio.gather(
        set_commands(),
        start_web_server(),
        bot.delete_webhook(drop_pending_updates=True)
    )

    asyncio.create_task(subscription_checker())
    asyncio.create_task(db_cleanup_task())

    logging.info("✅ Stopka VPN запущен успешно!")
    try:
        await dp.start_polling(bot)
    finally:
        await vpn_client.close()

if __name__ == "__main__":
    asyncio.run(main())
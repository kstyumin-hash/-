# ==========================================================
# Stopka VPN - Полноценный VPN сервис
# Telegram Bot + WireGuard VPN
# ==========================================================

import asyncio
import logging
import sqlite3
import json
import uuid
import os
import html
import subprocess
import qrcode
import io
import base64
import hashlib
import hmac
import time
from collections import defaultdict
from datetime import datetime, timedelta
from aiohttp import web
from PIL import Image

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
    BotCommand,
    FSInputFile
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

DATABASE = "stopka_vpn.db"
JSON_FILE = "Vpn_data.json"
PORT = int(os.getenv("PORT", 8080))

# VPN настройки
WIREGUARD_INTERFACE = os.environ.get("WIREGUARD_INTERFACE", "wg0")
WIREGUARD_SUBNET = os.environ.get("WIREGUARD_SUBNET", "10.0.0.0/24")
WIREGUARD_CONFIG_DIR = "/etc/wireguard"  # или "./wireguard" для локального тестирования

# Константы
MAX_DAYS = 3650
MAX_DEVICES = 5
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

class Database:
    def __init__(self):
        self.conn = sqlite3.connect(DATABASE, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.create_tables()

    def create_tables(self):
        # Пользователи
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS users(
            id INTEGER PRIMARY KEY,
            username TEXT,
            name TEXT,
            expire_date TEXT,
            status TEXT,
            is_admin INTEGER DEFAULT 0,
            invited_by INTEGER DEFAULT 0,
            first_payment INTEGER DEFAULT 0,
            last_tariff TEXT,
            username_history TEXT DEFAULT '[]'
        )
        """)
        
        # Тикеты
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS tickets(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            message TEXT,
            answer TEXT,
            status TEXT
        )
        """)
        
        # Платежи
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS payments(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            payment_code TEXT UNIQUE,
            amount INTEGER,
            days INTEGER,
            status TEXT,
            date TEXT
        )
        """)
        
        # История платежей
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS payment_history(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            amount INTEGER,
            days INTEGER,
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
        
        # Рефералы
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS referrals(
            user_id INTEGER PRIMARY KEY,
            invited_by INTEGER,
            bonus_given INTEGER DEFAULT 0
        )
        """)
        
        # Логи админов
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS admin_logs(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            admin_id INTEGER,
            action TEXT,
            target_id INTEGER,
            created_at TEXT
        )
        """)
        
        # Устройства (VPN)
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS devices(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            device_name TEXT,
            private_key TEXT,
            public_key TEXT,
            ip_address TEXT,
            config TEXT,
            created_at TEXT,
            last_active TEXT,
            FOREIGN KEY (user_id) REFERENCES users(id)
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
            user_id INTEGER,
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
        cursor = self.conn.execute("SELECT * FROM users WHERE username=?", (username.replace("@",""),))
        return cursor.fetchone()

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

    # ===== Device Methods =====
    def add_device(self, user_id, device_name, private_key, public_key, ip_address, config):
        created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.conn.execute("""
            INSERT INTO devices (user_id, device_name, private_key, public_key, ip_address, config, created_at, last_active)
            VALUES(?,?,?,?,?,?,?,?)
        """, (user_id, device_name, private_key, public_key, ip_address, config, created_at, created_at))
        self.conn.commit()
        return True

    def get_devices(self, user_id):
        cursor = self.conn.execute(
            "SELECT id, device_name, public_key, ip_address, config, created_at, last_active FROM devices WHERE user_id=?",
            (user_id,)
        )
        return cursor.fetchall()

    def get_device(self, device_id):
        cursor = self.conn.execute("SELECT * FROM devices WHERE id=?", (device_id,))
        return cursor.fetchone()

    def delete_device(self, device_id, user_id):
        device = self.get_device(device_id)
        if device:
            # Удаляем peer из WireGuard
            try:
                subprocess.run(
                    ["wg", "set", WIREGUARD_INTERFACE, "peer", device[3], "remove"]
                )
            except:
                pass
        self.conn.execute("DELETE FROM devices WHERE id=? AND user_id=?", (device_id, user_id))
        self.conn.commit()

    def update_device_active(self, device_id):
        self.conn.execute(
            "UPDATE devices SET last_active=? WHERE id=?",
            (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), device_id)
        )
        self.conn.commit()

    # ===== Referral Methods =====
    def get_referral_count(self, user_id):
        cursor = self.conn.execute(
            "SELECT COUNT(*) FROM referrals WHERE invited_by=? AND bonus_given=1",
            (user_id,)
        )
        return cursor.fetchone()[0]

    # ===== Payment Methods =====
    def add_payment_history(self, user_id, amount, days):
        self.conn.execute(
            "INSERT INTO payment_history (user_id, amount, days, date) VALUES(?,?,?,?)",
            (user_id, amount, days, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        )
        self.conn.commit()

    def get_total_income(self):
        cursor = self.conn.execute("SELECT SUM(amount) FROM payment_history")
        return cursor.fetchone()[0] or 0

    def get_active_users_count(self):
        cursor = self.conn.execute("SELECT COUNT(*) FROM users WHERE status='Активно'")
        return cursor.fetchone()[0]

    def get_total_users_count(self):
        cursor = self.conn.execute("SELECT COUNT(*) FROM users")
        return cursor.fetchone()[0]

    def get_open_tickets_count(self):
        cursor = self.conn.execute("SELECT COUNT(*) FROM tickets WHERE status='Открыт'")
        return cursor.fetchone()[0]

    # ===== Notification Methods =====
    def notification_sent(self, user_id, ntype):
        cursor = self.conn.execute(
            "SELECT 1 FROM notifications WHERE user_id=? AND type=?",
            (user_id, ntype)
        )
        return cursor.fetchone() is not None

    def save_notification(self, user_id, ntype):
        self.conn.execute(
            "INSERT OR REPLACE INTO notifications (user_id, type, date) VALUES(?,?,?)",
            (user_id, ntype, datetime.now().strftime("%Y-%m-%d"))
        )
        self.conn.commit()

    # ===== Donation Methods =====
    def is_donation_processed(self, donation_id):
        cursor = self.conn.execute("SELECT 1 FROM processed_donations WHERE donation_id=?", (donation_id,))
        return cursor.fetchone() is not None

    def mark_donation_processed(self, donation_id):
        self.conn.execute(
            "INSERT INTO processed_donations (donation_id, processed_at) VALUES(?,?)",
            (donation_id, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        )
        self.conn.commit()

    # ===== Log Methods =====
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
# WIREGUARD VPN MANAGER
############################################################

class WireGuardManager:
    def __init__(self):
        self.config_dir = WIREGUARD_CONFIG_DIR
        self.interface = WIREGUARD_INTERFACE
        self.subnet = WIREGUARD_SUBNET
        self.server_public_key = None
        self.server_ip = self.subnet.replace("/24", ".1")
        self._load_server_public_key()
    
    def _load_server_public_key(self):
        """Загружает публичный ключ сервера"""
        try:
            result = subprocess.run(
                ["wg", "show", self.interface, "public-key"],
                capture_output=True,
                text=True
            )
            if result.returncode == 0:
                self.server_public_key = result.stdout.strip()
            else:
                # Если ключ не найден, создаём через wg genkey
                logging.warning("Server public key not found, generating...")
                private_key = subprocess.check_output(["wg", "genkey"]).decode().strip()
                with open(f"{self.config_dir}/{self.interface}.key", "w") as f:
                    f.write(private_key)
                subprocess.run(["chmod", "600", f"{self.config_dir}/{self.interface}.key"])
                self._restart_wireguard()
                self._load_server_public_key()
        except Exception as e:
            logging.error(f"Error loading server key: {e}")
    
    def generate_peer_config(self, user_id, device_name):
        """Генерирует конфиг для нового WireGuard peer"""
        try:
            # Генерируем ключи
            private_key = subprocess.check_output(["wg", "genkey"]).decode().strip()
            public_key = subprocess.check_output(
                ["wg", "pubkey"],
                input=private_key.encode()
            ).decode().strip()
            
            # Генерируем IP
            ip = self._generate_ip()
            
            # Создаём конфиг для клиента
            client_config = f"""
[Interface]
PrivateKey = {private_key}
Address = {ip}/32
DNS = 1.1.1.1, 8.8.8.8

[Peer]
PublicKey = {self.server_public_key}
Endpoint = {self._get_server_endpoint()}
AllowedIPs = 0.0.0.0/0
PersistentKeepalive = 25
"""
            
            # Добавляем peer на сервер
            self._add_peer_to_server(public_key, ip)
            
            # Генерируем QR код
            qr_code = self._generate_qr(client_config)
            
            return {
                "private_key": private_key,
                "public_key": public_key,
                "ip": ip,
                "config": client_config,
                "qr": qr_code
            }
        except Exception as e:
            logging.error(f"Error generating peer config: {e}")
            raise
    
    def _generate_ip(self):
        """Генерирует уникальный IP в подсети"""
        existing_ips = self._get_existing_ips()
        base_ip = self.subnet.replace("/24", "")
        for i in range(2, 254):
            ip = f"{base_ip}.{i}"
            if ip not in existing_ips:
                return ip
        raise Exception("No free IP addresses available")
    
    def _get_existing_ips(self):
        """Получает список уже используемых IP"""
        cursor = db.conn.execute("SELECT ip_address FROM devices")
        return [row[0] for row in cursor.fetchall()]
    
    def _add_peer_to_server(self, public_key, ip):
        """Добавляет peer в WireGuard на сервере"""
        try:
            # Проверяем, существует ли уже peer
            check = subprocess.run(
                ["wg", "show", self.interface, "peers"],
                capture_output=True,
                text=True
            )
            if public_key in check.stdout:
                # Удаляем старый peer
                subprocess.run(
                    ["wg", "set", self.interface, "peer", public_key, "remove"]
                )
            
            # Добавляем новый peer
            subprocess.run([
                "wg", "set", self.interface,
                "peer", public_key,
                "allowed-ips", f"{ip}/32"
            ])
            
            # Сохраняем конфиг
            self._save_server_config()
        except Exception as e:
            logging.error(f"Error adding peer: {e}")
            raise
    
    def _save_server_config(self):
        """Сохраняет текущую конфигурацию WireGuard"""
        try:
            subprocess.run(["wg-quick", "save", self.interface])
        except Exception as e:
            logging.error(f"Error saving config: {e}")
    
    def _restart_wireguard(self):
        """Перезапускает WireGuard"""
        try:
            subprocess.run(["wg-quick", "down", self.interface])
            subprocess.run(["wg-quick", "up", self.interface])
        except Exception as e:
            logging.error(f"Error restarting WireGuard: {e}")
    
    def _get_server_endpoint(self):
        """Получает публичный адрес сервера"""
        # В реальном проекте должно быть из конфига
        return os.environ.get("SERVER_ENDPOINT", "your-server.com:51820")
    
    def _generate_qr(self, config):
        """Генерирует QR код для конфига"""
        qr = qrcode.QRCode(box_size=6, border=2)
        qr.add_data(config)
        qr.make(fit=True)
        img = qr.make_image(fill_color="black", back_color="white")
        
        # Сохраняем в bytes
        img_bytes = io.BytesIO()
        img.save(img_bytes, format='PNG')
        img_bytes.seek(0)
        return img_bytes.getvalue()

vpn_manager = WireGuardManager()

############################################################
# FSM СОСТОЯНИЯ
############################################################

class TicketState(StatesGroup):
    waiting_text = State()

class AdminGiveState(StatesGroup):
    waiting_data = State()

class AdminDisableState(StatesGroup):
    waiting_username = State()

class AdminCheckState(StatesGroup):
    waiting_username = State()

class ReplyState(StatesGroup):
    waiting_answer = State()

class AdminState(StatesGroup):
    waiting_username = State()

class DeviceState(StatesGroup):
    waiting_name = State()

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

def start_keyboard(is_admin=False):
    buttons = [[InlineKeyboardButton(text="🔌 Подключиться", callback_data="profile")]]
    if is_admin:
        buttons.append([InlineKeyboardButton(text="🛠 Админ-панель", callback_data="admin")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def profile_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💳 Оплата VPN", callback_data="payment")],
        [InlineKeyboardButton(text="📱 Мои устройства", callback_data="my_devices")],
        [InlineKeyboardButton(text="🔄 Продлить", callback_data="renew")],
        [InlineKeyboardButton(text="💳 История оплат", callback_data="payments_history")],
        [InlineKeyboardButton(text="🎁 Пригласить друга", callback_data="my_ref")],
        [InlineKeyboardButton(text="🎟 Промокод", callback_data="promo")],
        [InlineKeyboardButton(text="🛟 Поддержка", callback_data="support")]
    ])

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

def donation_keyboard(code):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💸 Оплатить DonationAlerts", url="https://www.donationalerts.com/")],
        [InlineKeyboardButton(text="🔎 Проверить оплату", callback_data=f"check_payment:{code}")],
        [InlineKeyboardButton(text="⬅ Назад", callback_data="payment")]
    ])

def support_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📨 Обратиться", callback_data="create_ticket")],
        [InlineKeyboardButton(text="⬅ Назад", callback_data="profile")]
    ])

def admin_keyboard(owner=False):
    buttons = [
        [InlineKeyboardButton(text="📊 Статистика", callback_data="stats")],
        [InlineKeyboardButton(text="💰 Доход", callback_data="income")],
        [InlineKeyboardButton(text="👥 Пользователи", callback_data="users_count")],
        [InlineKeyboardButton(text="📢 Рассылка", callback_data="broadcast")],
        [InlineKeyboardButton(text="🎟 Тикеты", callback_data="admin_tickets")],
        [InlineKeyboardButton(text="🎁 Промокоды", callback_data="promo_admin")],
        [InlineKeyboardButton(text="📅 Выдать дни", callback_data="admin_give")],
        [InlineKeyboardButton(text="🚫 Отключить подписку", callback_data="admin_disable")],
        [InlineKeyboardButton(text="👁 Посмотреть подписку", callback_data="admin_check")]
    ]
    if owner:
        buttons.append([InlineKeyboardButton(text="👑 Администраторы", callback_data="admins")])
        buttons.append([InlineKeyboardButton(text="📝 Логи админов", callback_data="admin_logs")])
    buttons.append([InlineKeyboardButton(text="⬅ Назад", callback_data="profile")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def ticket_list_keyboard(tickets):
    buttons = []
    for ticket in tickets:
        buttons.append([InlineKeyboardButton(text=f"🎟 #{ticket[0]} | {html.escape(ticket[2][:20])}", callback_data=f"ticket_{ticket[0]}")])
    buttons.append([InlineKeyboardButton(text="⬅ Назад", callback_data="admin")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def devices_keyboard(devices):
    buttons = []
    for device in devices:
        device_id, device_name, public_key, ip, config, created_at, last_active = device
        buttons.append([
            InlineKeyboardButton(
                text=f"📱 {html.escape(device_name)}",
                callback_data=f"device_info_{device_id}"
            )
        ])
        buttons.append([
            InlineKeyboardButton(
                text=f"🗑 Удалить",
                callback_data=f"device_delete_{device_id}"
            )
        ])
    buttons.append([InlineKeyboardButton(text="➕ Добавить устройство", callback_data="add_device")])
    buttons.append([InlineKeyboardButton(text="⬅ Назад", callback_data="profile")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def device_info_keyboard(device_id):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📋 Скопировать конфиг", callback_data=f"copy_config_{device_id}")],
        [InlineKeyboardButton(text="📥 Скачать QR", callback_data=f"download_qr_{device_id}")],
        [InlineKeyboardButton(text="🗑 Удалить устройство", callback_data=f"device_delete_{device_id}")],
        [InlineKeyboardButton(text="⬅ Назад", callback_data="my_devices")]
    ])

def promo_admin_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Создать промокод", callback_data="promo_create")],
        [InlineKeyboardButton(text="📋 Список промокодов", callback_data="promo_list")],
        [InlineKeyboardButton(text="⬅ Назад", callback_data="admin")]
    ])

############################################################
# MIDDLEWARES
############################################################

@dp.message()
async def rate_limit_middleware(message: Message):
    if not rate_limiter.is_allowed(message.from_user.id):
        await message.answer("⏳ Слишком много запросов. Подождите немного.")
        return False
    return True

############################################################
# START & HELP
############################################################

@dp.message(Command("start"))
async def start(message: Message):
    user_id = message.from_user.id
    
    # Обновляем username если изменился
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
    
    # Проверка реферального кода
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
                        db.conn.execute(
                            "UPDATE users SET invited_by=? WHERE id=?",
                            (inviter, user_id)
                        )
                        db.conn.execute(
                            "INSERT INTO referrals (user_id, invited_by, bonus_given) VALUES(?,?,0)",
                            (user_id, inviter)
                        )
                        db.conn.commit()
            except:
                pass
    
    await message.answer(
        "🎉 <b>Добро пожаловать в Stopka VPN!</b>\n\n"
        "🚀 Спасибо за ваш выбор!\n\n"
        "🎁 Мы дарим вам 3 пробных дня!\n\n"
        "👨‍💻 Создатели: @prostokiril, @ll1_what",
        reply_markup=start_keyboard(db.is_admin(user_id))
    )

@dp.message(Command("help"))
async def help_command(message: Message):
    await message.answer(
        "🛡 <b>Stopka VPN</b>\n\n"
        "Команды:\n"
        "/start — открыть меню\n"
        "/help — помощь\n\n"
        "👨‍💻 Создатели: @prostokiril, @ll1_what\n\n"
        "Если возникли проблемы — используйте поддержку."
    )

############################################################
# PROFILE
############################################################

@dp.callback_query(F.data == "profile")
async def profile(callback: CallbackQuery):
    user = db.get_user(callback.from_user.id)
    if not user:
        await callback.answer("Ошибка пользователя", show_alert=True)
        return

    try:
        expire = datetime.strptime(user[3], "%Y-%m-%d %H:%M:%S")
    except:
        expire = datetime.strptime(user[3], "%Y-%m-%d")
    
    days = (expire - datetime.now()).days
    if days < 0:
        days = 0

    devices_count = len(db.get_devices(callback.from_user.id))
    ref_count = db.get_referral_count(callback.from_user.id)

    status = "🟢 Активно" if days > 0 else "🔴 Неактивно"
    safe_name = hbold(callback.from_user.first_name)
    
    await callback.message.edit_text(
        f"👤 {safe_name}\n\n"
        f"🛡 <b>Stopka VPN</b>\n\n"
        f"🟢 Статус:\n<b>{status}</b>\n\n"
        f"⏳ Осталось:\n<b>{days} дней</b>\n\n"
        f"📱 Устройств:\n<b>{devices_count}/{MAX_DEVICES}</b>\n\n"
        f"🎁 Приглашено друзей:\n<b>{ref_count}</b>\n\n"
        f"👨‍💻 Создатели: @prostokiril, @ll1_what",
        reply_markup=profile_keyboard()
    )
    await callback.answer()

############################################################
# DEVICES (VPN)
############################################################

@dp.callback_query(F.data == "add_device")
async def add_device_start(callback: CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    
    # Проверяем активна ли подписка
    user = db.get_user(user_id)
    try:
        expire = datetime.strptime(user[3], "%Y-%m-%d %H:%M:%S")
    except:
        expire = datetime.strptime(user[3], "%Y-%m-%d")
    
    if expire < datetime.now():
        await callback.answer("❌ Ваша подписка истекла. Пополните её.", show_alert=True)
        return
    
    devices = db.get_devices(user_id)
    if len(devices) >= MAX_DEVICES:
        await callback.answer(f"❌ Достигнут лимит устройств ({MAX_DEVICES})", show_alert=True)
        return
    
    await state.set_state(DeviceState.waiting_name)
    await callback.message.edit_text(
        "📱 <b>Добавление устройства</b>\n\n"
        "Введите название устройства:\n"
        "Например: <b>iPhone 15</b>, <b>Ноутбук</b>, <b>Телевизор</b>",
        reply_markup=back_keyboard()
    )
    await callback.answer()

@dp.message(DeviceState.waiting_name)
async def add_device_finish(message: Message, state: FSMContext):
    device_name = message.text.strip()
    
    if len(device_name) < 2 or len(device_name) > 50:
        await message.answer("❌ Название должно быть от 2 до 50 символов")
        return
    
    devices = db.get_devices(message.from_user.id)
    if len(devices) >= MAX_DEVICES:
        await message.answer(f"❌ Достигнут лимит устройств ({MAX_DEVICES})")
        await state.clear()
        return
    
    try:
        # Создаём VPN конфиг
        peer = vpn_manager.generate_peer_config(message.from_user.id, device_name)
        
        # Сохраняем в базу
        db.add_device(
            message.from_user.id,
            device_name,
            peer["private_key"],
            peer["public_key"],
            peer["ip"],
            peer["config"]
        )
        
        await state.clear()
        
        # Отправляем конфиг и QR
        await message.answer(
            f"✅ <b>Устройство добавлено!</b>\n\n"
            f"📱 Название: {hbold(device_name)}\n"
            f"🔑 IP: <code>{peer['ip']}</code>\n\n"
            f"<b>Конфиг для WireGuard:</b>\n"
            f"<code>{peer['config']}</code>\n\n"
            f"📱 Отсканируйте QR код для быстрого подключения:",
            reply_markup=back_keyboard()
        )
        
        # Отправляем QR код
        photo = io.BytesIO(peer["qr"])
        photo.seek(0)
        await message.answer_photo(
            photo,
            caption=f"📱 QR код для {device_name}"
        )
        
        db.add_admin_log(message.from_user.id, f"Добавил устройство {device_name}")
        
    except Exception as e:
        logging.error(f"Error creating device: {e}")
        await message.answer(f"❌ Ошибка создания устройства: {str(e)}")
        await state.clear()

@dp.callback_query(F.data == "my_devices")
async def my_devices(callback: CallbackQuery):
    devices = db.get_devices(callback.from_user.id)
    
    if not devices:
        await callback.message.edit_text(
            "📋 <b>Мои устройства</b>\n\n"
            "У вас пока нет добавленных устройств.\n"
            "Нажмите кнопку ниже, чтобы добавить.\n\n"
            "👨‍💻 Создатели: @prostokiril, @ll1_what",
            reply_markup=devices_keyboard([])
        )
        await callback.answer()
        return
    
    text = "📋 <b>Мои устройства</b>\n\n"
    for device in devices:
        device_id, device_name, public_key, ip, config, created_at, last_active = device
        text += f"📱 <b>{html.escape(device_name)}</b>\n"
        text += f"🔑 IP: <code>{ip}</code>\n"
        text += f"📅 Добавлено: {created_at}\n\n"
    
    await callback.message.edit_text(text, reply_markup=devices_keyboard(devices))
    await callback.answer()

@dp.callback_query(F.data.startswith("device_info_"))
async def device_info(callback: CallbackQuery):
    device_id = int(callback.data.split("_")[2])
    device = db.get_device(device_id)
    
    if not device:
        await callback.answer("❌ Устройство не найдено", show_alert=True)
        return
    
    if device[1] != callback.from_user.id and not db.is_admin(callback.from_user.id):
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    
    device_id, user_id, device_name, private_key, public_key, ip, config, created_at, last_active = device
    
    await callback.message.edit_text(
        f"📱 <b>Информация об устройстве</b>\n\n"
        f"Название: {hbold(device_name)}\n"
        f"IP: <code>{ip}</code>\n"
        f"Публичный ключ: <code>{public_key[:20]}...</code>\n"
        f"Создано: {created_at}\n"
        f"Активно: {last_active}\n"
        f"ID пользователя: {user_id}",
        reply_markup=device_info_keyboard(device_id)
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("device_delete_"))
async def device_delete(callback: CallbackQuery):
    device_id = int(callback.data.split("_")[2])
    db.delete_device(device_id, callback.from_user.id)
    await callback.answer("✅ Устройство удалено")
    await my_devices(callback)

@dp.callback_query(F.data.startswith("copy_config_"))
async def copy_config(callback: CallbackQuery):
    device_id = int(callback.data.split("_")[2])
    device = db.get_device(device_id)
    
    if not device:
        await callback.answer("❌ Устройство не найдено", show_alert=True)
        return
    
    if device[1] != callback.from_user.id:
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    
    await callback.answer(f"🔑 Конфиг скопирован", show_alert=True)
    
    # Отправляем конфиг отдельным сообщением
    await callback.message.answer(
        f"<b>Конфиг для {device[2]}</b>\n\n"
        f"<code>{device[6]}</code>"
    )

@dp.callback_query(F.data.startswith("download_qr_"))
async def download_qr(callback: CallbackQuery):
    device_id = int(callback.data.split("_")[2])
    device = db.get_device(device_id)
    
    if not device:
        await callback.answer("❌ Устройство не найдено", show_alert=True)
        return
    
    if device[1] != callback.from_user.id:
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    
    # Генерируем QR заново
    qr = vpn_manager._generate_qr(device[6])
    photo = io.BytesIO(qr)
    photo.seek(0)
    await callback.message.answer_photo(
        photo,
        caption=f"📱 QR код для {device[2]}"
    )
    await callback.answer()

############################################################
# RENEW SUBSCRIPTION
############################################################

@dp.callback_query(F.data == "renew")
async def renew_subscription(callback: CallbackQuery):
    user = db.get_user(callback.from_user.id)
    last_tariff = user[8] if user else None
    
    if last_tariff and last_tariff in TARIFFS:
        tariff = TARIFFS[last_tariff]
        await callback.message.edit_text(
            f"🔄 <b>Продление подписки</b>\n\n"
            f"Ваш последний тариф: <b>{tariff['name']}</b>\n"
            f"Стоимость: <b>{tariff['price']}₽</b>\n\n"
            f"Хотите продлить его?",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="✅ Да, продлить", callback_data=f"pay_{last_tariff}")],
                [InlineKeyboardButton(text="⬅ Назад", callback_data="profile")]
            ])
        )
    else:
        await payment_menu(callback)

############################################################
# REFERRAL SYSTEM
############################################################

async def give_referral_bonus(inviter_id, new_user_id):
    user = db.get_user(inviter_id)
    if not user:
        return
    
    cursor = db.conn.execute(
        "SELECT bonus_given FROM referrals WHERE user_id=?",
        (new_user_id,)
    )
    ref = cursor.fetchone()
    if not ref or ref[0] == 1:
        return
    
    try:
        expire = datetime.strptime(user[3], "%Y-%m-%d %H:%M:%S")
    except:
        expire = datetime.strptime(user[3], "%Y-%m-%d")
    
    if expire < datetime.now():
        expire = datetime.now()
    expire += timedelta(days=REFERRAL_DAYS)
    expire_str = expire.strftime("%Y-%m-%d 23:59:59")
    
    db.conn.execute(
        "UPDATE users SET expire_date=? WHERE id=?",
        (expire_str, inviter_id)
    )
    db.conn.execute(
        "UPDATE referrals SET bonus_given=1 WHERE user_id=?",
        (new_user_id,)
    )
    db.conn.commit()
    
    await bot.send_message(
        inviter_id,
        f"🎉 <b>Ваш друг сделал первую оплату!</b>\n\n"
        f"⭐ Вам начислено +{REFERRAL_DAYS} дней VPN!\n\n"
        f"👨‍💻 Создатели: @prostokiril, @ll1_what"
    )

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
        f"⭐ За каждого друга: +{REFERRAL_DAYS} дней VPN\n"
        f"ℹ️ Бонус начисляется после первой оплаты друга\n\n"
        f"👨‍💻 Создатели: @prostokiril, @ll1_what",
        reply_markup=back_keyboard()
    )
    await callback.answer()

############################################################
# PROMO CODES (USER)
############################################################

@dp.callback_query(F.data == "promo")
async def promo_start(callback: CallbackQuery, state: FSMContext):
    await state.set_state(PromoState.waiting_code)
    await callback.message.edit_text(
        "🎟 <b>Введите промокод</b>\n\n"
        "Если у вас есть промокод, введите его ниже:",
        reply_markup=back_keyboard()
    )
    await callback.answer()

@dp.message(PromoState.waiting_code)
async def promo_use(message: Message, state: FSMContext):
    code = message.text.upper().strip()
    
    promo = db.conn.execute(
        "SELECT * FROM promo_codes WHERE code=?",
        (code,)
    ).fetchone()
    
    if not promo:
        await message.answer("❌ Промокод не найден")
        return
    
    if promo[2] >= promo[3]:
        await message.answer("❌ Лимит использований промокода исчерпан")
        return
    
    user = db.get_user(message.from_user.id)
    if not user:
        await message.answer("❌ Ошибка пользователя")
        return
    
    try:
        expire = datetime.strptime(user[3], "%Y-%m-%d %H:%M:%S")
    except:
        expire = datetime.strptime(user[3], "%Y-%m-%d")
    
    if expire < datetime.now():
        expire = datetime.now()
    expire += timedelta(days=promo[1])
    expire_str = expire.strftime("%Y-%m-%d 23:59:59")
    
    db.conn.execute(
        "UPDATE users SET expire_date=? WHERE id=?",
        (expire_str, message.from_user.id)
    )
    db.conn.execute(
        "UPDATE promo_codes SET uses=uses+1 WHERE code=?",
        (code,)
    )
    db.conn.commit()
    
    await state.clear()
    await message.answer(
        f"🎉 <b>Промокод активирован!</b>\n\n"
        f"⭐ Вам начислено +{promo[1]} дней VPN!\n\n"
        f"👨‍💻 Создатели: @prostokiril, @ll1_what"
    )

############################################################
# PAYMENTS
############################################################

@dp.callback_query(F.data == "payment")
async def payment_menu(callback: CallbackQuery):
    await callback.message.edit_text(
        "💳 <b>Тарифы Stopka VPN</b>\n\n"
        "🗓 Месяц — 180₽\n"
        "📅 Полгода — 980₽\n"
        "📆 Год — 1960₽\n\n"
        "👨‍💻 Создатели: @prostokiril, @ll1_what",
        reply_markup=payment_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data == "payments_history")
async def payments_history(callback: CallbackQuery):
    payments = db.conn.execute(
        """
        SELECT amount, days, status, date
        FROM payments
        WHERE user_id=?
        ORDER BY id DESC
        LIMIT 10
        """,
        (callback.from_user.id,)
    ).fetchall()
    
    if not payments:
        await callback.message.edit_text(
            "💳 <b>История оплат пуста</b>\n\n"
            "У вас пока нет оплат.",
            reply_markup=back_keyboard()
        )
        return
    
    text = "💳 <b>История оплат</b>\n\n"
    for p in payments:
        status = "✅" if p[2] == "paid" else "⏳"
        text += f"{status} {p[0]}₽ ({p[1]} дней) — {p[3]}\n"
    
    await callback.message.edit_text(
        text,
        reply_markup=back_keyboard()
    )
    await callback.answer()

def create_payment_code():
    return "STOPKA-" + str(uuid.uuid4())[:8].upper()

@dp.callback_query(F.data.startswith("pay_"))
async def create_payment(callback: CallbackQuery):
    tariff_id = callback.data.replace("pay_", "")
    tariff = TARIFFS.get(tariff_id)
    if not tariff:
        await callback.answer("Тариф не найден", show_alert=True)
        return
    
    # Сохраняем последний тариф
    db.save_last_tariff(callback.from_user.id, tariff_id)

    code = create_payment_code()
    try:
        db.conn.execute("""
            INSERT INTO payments (user_id, payment_code, amount, days, status, date) 
            VALUES(?,?,?,?,?,?)
        """, (callback.from_user.id, code, tariff["price"], tariff["days"], "waiting", datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        db.conn.commit()
    except sqlite3.IntegrityError:
        code = create_payment_code()
        db.conn.execute("""
            INSERT INTO payments (user_id, payment_code, amount, days, status, date) 
            VALUES(?,?,?,?,?,?)
        """, (callback.from_user.id, code, tariff["price"], tariff["days"], "waiting", datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        db.conn.commit()

    await callback.message.edit_text(
        f"💳 <b>Оплата Stopka VPN</b>\n\n"
        f"Тариф: {tariff['name']}\n"
        f"Сумма: {tariff['price']}₽\n\n"
        f"Ваш код оплаты:\n<code>{code}</code>\n\n"
        f"Укажите этот код при оплате DonationAlerts.\n\n"
        f"После оплаты нажмите кнопку проверки.\n\n"
        f"👨‍💻 Создатели: @prostokiril, @ll1_what",
        reply_markup=donation_keyboard(code)
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("check_payment:"))
async def check_payment(callback: CallbackQuery):
    code = callback.data.split(":")[1]
    cursor = db.conn.execute("SELECT * FROM payments WHERE payment_code=?", (code,))
    payment = cursor.fetchone()

    if not payment:
        await callback.answer("Платёж не найден", show_alert=True)
        return
    if payment[5] == "paid":
        await callback.answer("Оплата уже подтверждена", show_alert=True)
        return

    await callback.answer("⏳ Оплата ещё не найдена. Попробуйте позже.", show_alert=True)

############################################################
# SUPPORT
############################################################

@dp.callback_query(F.data == "support")
async def support(callback: CallbackQuery):
    await callback.message.edit_text(
        "🛟 <b>Поддержка Stopka VPN</b>\n\n"
        "Нужна помощь? 🤝\n"
        "Напишите нам, вместе разберёмся.\n\n"
        "👨‍💻 Создатели: @prostokiril, @ll1_what",
        reply_markup=support_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data == "create_ticket")
async def create_ticket(callback: CallbackQuery, state: FSMContext):
    await state.set_state(TicketState.waiting_text)
    await callback.message.edit_text(
        "📝 Отправьте сообщение для поддержки.\n\n"
        "Опишите проблему подробно.\n\n"
        "👨‍💻 Создатели: @prostokiril, @ll1_what"
    )
    await callback.answer()

@dp.message(TicketState.waiting_text)
async def save_ticket(message: Message, state: FSMContext):
    safe_text = html.escape(message.text)
    
    db.conn.execute("""
        INSERT INTO tickets (user_id, message, answer, status) 
        VALUES(?,?,?,?)
    """, (message.from_user.id, safe_text, "", "Открыт"))
    db.conn.commit()
    await state.clear()
    await message.answer(
        "✅ <b>Ваше обращение зарегистрировано!</b>\n\n"
        "Мы скоро свяжемся с вами.\n\n"
        "👨‍💻 Создатели: @prostokiril, @ll1_what"
    )

############################################################
# ADMIN PANEL
############################################################

@dp.callback_query(F.data == "admin")
async def admin_panel(callback: CallbackQuery):
    if not db.is_admin(callback.from_user.id):
        await callback.answer("❌ У вас нет прав", show_alert=True)
        return
    await callback.message.edit_text(
        "🛠 <b>Админ-панель Stopka VPN</b>\n\n"
        "Выберите действие:\n\n"
        "👨‍💻 Создатели: @prostokiril, @ll1_what",
        reply_markup=admin_keyboard(callback.from_user.id == OWNER_ID)
    )
    await callback.answer()

@dp.callback_query(F.data == "stats")
async def admin_stats(callback: CallbackQuery):
    if not db.is_admin(callback.from_user.id):
        await callback.answer("❌ Нет прав", show_alert=True)
        return
    
    users = db.get_total_users_count()
    active = db.get_active_users_count()
    tickets = db.get_open_tickets_count()
    total_income = db.get_total_income()
    
    await callback.message.edit_text(
        f"📊 <b>Статистика Stopka VPN</b>\n\n"
        f"👥 Пользователей: {users}\n"
        f"🟢 Активных: {active}\n"
        f"🎟 Открытых тикетов: {tickets}\n"
        f"💰 Доход: {total_income}₽\n\n"
        f"🚀 Сервис работает\n"
        f"👨‍💻 Создатели: @prostokiril, @ll1_what",
        reply_markup=back_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data == "income")
async def income(callback: CallbackQuery):
    if not db.is_admin(callback.from_user.id):
        await callback.answer("❌ Нет прав", show_alert=True)
        return
    
    total_income = db.get_total_income()
    
    monthly = db.conn.execute("""
        SELECT strftime('%Y-%m', date) as month, SUM(amount) as total
        FROM payment_history
        GROUP BY month
        ORDER BY month DESC
        LIMIT 6
    """).fetchall()
    
    text = f"💰 <b>Финансы Stopka VPN</b>\n\n"
    text += f"Всего заработано: <b>{total_income}₽</b>\n\n"
    
    if monthly:
        text += "📊 <b>Последние 6 месяцев:</b>\n"
        for m in monthly:
            text += f"📅 {m[0]}: {m[1]}₽\n"
    
    await callback.message.edit_text(text, reply_markup=back_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "users_count")
async def users_count(callback: CallbackQuery):
    if not db.is_admin(callback.from_user.id):
        await callback.answer("❌ Нет прав", show_alert=True)
        return
    
    total = db.get_total_users_count()
    active = db.get_active_users_count()
    expired = db.conn.execute("SELECT COUNT(*) FROM users WHERE status='Неактивно'").fetchone()[0]
    
    await callback.message.edit_text(
        f"👥 <b>Пользователи</b>\n\n"
        f"Всего: {total}\n"
        f"🟢 Активные: {active}\n"
        f"🔴 Неактивные: {expired}",
        reply_markup=back_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data == "admin_logs")
async def admin_logs(callback: CallbackQuery):
    if not db.is_admin(callback.from_user.id) or callback.from_user.id != OWNER_ID:
        await callback.answer("❌ Нет прав", show_alert=True)
        return
    
    logs = db.conn.execute(
        """
        SELECT admin_id, action, target_id, created_at
        FROM admin_logs
        ORDER BY id DESC
        LIMIT 20
        """
    ).fetchall()
    
    if not logs:
        await callback.message.edit_text(
            "📝 Логов пока нет",
            reply_markup=back_keyboard()
        )
        return
    
    text = "📝 <b>Последние логи</b>\n\n"
    for log in logs:
        text += f"👤 {log[0]} | {log[1]} | {log[2]} | {log[3]}\n"
    
    await callback.message.edit_text(text, reply_markup=back_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "admins")
async def admins_list(callback: CallbackQuery):
    if not db.is_admin(callback.from_user.id) or callback.from_user.id != OWNER_ID:
        await callback.answer("❌ Нет прав", show_alert=True)
        return
    
    admins = db.conn.execute(
        "SELECT id, username, name FROM users WHERE is_admin=1"
    ).fetchall()
    
    if not admins:
        await callback.message.edit_text(
            "👑 Администраторов нет",
            reply_markup=back_keyboard()
        )
        return
    
    text = "👑 <b>Администраторы</b>\n\n"
    for admin in admins:
        text += f"👤 {admin[1] or admin[2]} (ID: {admin[0]})\n"
    
    await callback.message.edit_text(text, reply_markup=back_keyboard())
    await callback.answer()

############################################################
# ADMIN BROADCAST
############################################################

@dp.callback_query(F.data == "broadcast")
async def broadcast_start(callback: CallbackQuery, state: FSMContext):
    if not db.is_admin(callback.from_user.id):
        await callback.answer("❌ Нет прав", show_alert=True)
        return
    
    await state.set_state(BroadcastState.waiting_text)
    await callback.message.edit_text(
        "📢 Отправьте текст рассылки:\n\n"
        "⚠️ Сообщение получит ВСЕ пользователи бота"
    )
    await callback.answer()

@dp.message(BroadcastState.waiting_text)
async def broadcast_send(message: Message, state: FSMContext):
    if not db.is_admin(message.from_user.id):
        await message.answer("❌ Нет прав")
        return
    
    text = html.escape(message.text)
    
    users = db.conn.execute("SELECT id FROM users").fetchall()
    sent = 0
    
    for user in users:
        try:
            await bot.send_message(
                user[0],
                f"📢 <b>Новости Stopka VPN</b>\n\n{text}\n\n"
                f"👨‍💻 Создатели: @prostokiril, @ll1_what"
            )
            sent += 1
        except:
            pass
    
    db.add_admin_log(message.from_user.id, f"Сделал рассылку ({sent} пользователей)")
    
    await state.clear()
    await message.answer(f"✅ Рассылка завершена\nОтправлено: {sent}")

############################################################
# ADMIN GIVE DAYS
############################################################

@dp.callback_query(F.data == "admin_give")
async def admin_give(callback: CallbackQuery, state: FSMContext):
    if not db.is_admin(callback.from_user.id):
        await callback.answer("❌ Нет прав", show_alert=True)
        return
    await state.set_state(AdminGiveState.waiting_data)
    await callback.message.edit_text(
        "Введите данные:\n\n"
        "<code>@username количество_дней</code>\n\n"
        "Пример:\n@user 30"
    )
    await callback.answer()

@dp.message(AdminGiveState.waiting_data)
async def give_days(message: Message, state: FSMContext):
    if not db.is_admin(message.from_user.id):
        await message.answer("❌ Нет прав")
        return
    
    try:
        username, days_str = message.text.split()
        days = int(days_str)
    except:
        await message.answer("❌ Неверный формат.\nИспользуйте: @username количество_дней")
        return
    
    if days <= 0:
        await message.answer("❌ Количество дней должно быть положительным числом")
        return
    if days > MAX_DAYS:
        await message.answer(f"❌ Максимальное количество дней: {MAX_DAYS}")
        return

    user = db.get_username(username)
    if not user:
        await message.answer("❌ Пользователь не найден")
        return

    try:
        expire = datetime.strptime(user[3], "%Y-%m-%d %H:%M:%S")
    except:
        expire = datetime.strptime(user[3], "%Y-%m-%d")
    
    if expire < datetime.now():
        expire = datetime.now()
    expire += timedelta(days=days)
    expire_str = expire.strftime("%Y-%m-%d 23:59:59")

    db.conn.execute(
        "UPDATE users SET expire_date=?, status='Активно' WHERE id=?",
        (expire_str, user[0])
    )
    db.conn.commit()
    
    db.add_admin_log(message.from_user.id, f"Выдал {days} дней", user[0])
    db.add_payment_history(user[0], 0, days)
    
    await state.clear()
    await message.answer(f"✅ Выдано {days} дней пользователю {username}")

############################################################
# ADMIN DISABLE
############################################################

@dp.callback_query(F.data == "admin_disable")
async def admin_disable(callback: CallbackQuery, state: FSMContext):
    if not db.is_admin(callback.from_user.id):
        await callback.answer("❌ Нет прав", show_alert=True)
        return
    await state.set_state(AdminDisableState.waiting_username)
    await callback.message.edit_text("Введите @username")
    await callback.answer()

@dp.message(AdminDisableState.waiting_username)
async def disable_user(message: Message, state: FSMContext):
    if not db.is_admin(message.from_user.id):
        await message.answer("❌ Нет прав")
        return
    
    username = message.text.replace("@","")
    user = db.get_username(username)
    if not user:
        await message.answer("❌ Пользователь не найден")
        return

    expire = datetime.now() - timedelta(days=1)
    expire_str = expire.strftime("%Y-%m-%d 23:59:59")
    
    db.conn.execute(
        "UPDATE users SET expire_date=?, status='Неактивно' WHERE id=?",
        (expire_str, user[0])
    )
    db.conn.commit()
    
    db.add_admin_log(message.from_user.id, "Отключил подписку", user[0])
    
    await state.clear()
    await message.answer("✅ Подписка отключена")

############################################################
# ADMIN CHECK
############################################################

@dp.callback_query(F.data == "admin_check")
async def admin_check(callback: CallbackQuery, state: FSMContext):
    if not db.is_admin(callback.from_user.id):
        await callback.answer("❌ Нет прав", show_alert=True)
        return
    await state.set_state(AdminCheckState.waiting_username)
    await callback.message.edit_text("Введите @username пользователя:")
    await callback.answer()

@dp.message(AdminCheckState.waiting_username)
async def check_user(message: Message, state: FSMContext):
    if not db.is_admin(message.from_user.id):
        await message.answer("❌ Нет прав")
        return
    
    username = message.text.replace("@", "")
    user = db.get_username(username)
    if not user:
        await message.answer("❌ Пользователь не найден")
        return

    try:
        expire = datetime.strptime(user[3], "%Y-%m-%d %H:%M:%S")
    except:
        expire = datetime.strptime(user[3], "%Y-%m-%d")
    
    days = (expire - datetime.now()).days
    if days < 0:
        days = 0

    devices = db.get_devices(user[0])
    ref_count = db.get_referral_count(user[0])
    
    await state.clear()
    await message.answer(
        f"👤 Пользователь: @{user[1]}\n\n"
        f"⏳ Осталось дней: {days}\n"
        f"📅 До: {user[3]}\n"
        f"📊 Статус: {user[4]}\n"
        f"📱 Устройств: {len(devices)}/{MAX_DEVICES}\n"
        f"🎁 Приглашено: {ref_count}"
    )

############################################################
# ADMIN TICKETS
############################################################

@dp.callback_query(F.data == "admin_tickets")
async def admin_tickets(callback: CallbackQuery):
    if not db.is_admin(callback.from_user.id):
        await callback.answer("❌ Нет прав", show_alert=True)
        return

    cursor = db.conn.execute("SELECT * FROM tickets WHERE status='Открыт'")
    tickets = cursor.fetchall()

    if not tickets:
        await callback.message.edit_text(
            "🎟 Открытых тикетов нет",
            reply_markup=admin_keyboard(callback.from_user.id == OWNER_ID)
        )
        return

    await callback.message.edit_text(
        "🎟 <b>Открытые обращения:</b>",
        reply_markup=ticket_list_keyboard(tickets)
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("ticket_"))
async def open_ticket(callback: CallbackQuery):
    if not db.is_admin(callback.from_user.id):
        await callback.answer("❌ Нет прав", show_alert=True)
        return
    
    ticket_id = int(callback.data.split("_")[1])
    cursor = db.conn.execute("SELECT * FROM tickets WHERE id=?", (ticket_id,))
    ticket = cursor.fetchone()

    if not ticket:
        await callback.answer("Тикет не найден", show_alert=True)
        return

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✉️ Ответить", callback_data=f"reply_{ticket_id}")],
        [InlineKeyboardButton(text="❌ Закрыть", callback_data=f"close_{ticket_id}")],
        [InlineKeyboardButton(text="⬅ Назад", callback_data="admin_tickets")]
    ])

    await callback.message.edit_text(
        f"🎟 <b>Тикет #{ticket[0]}</b>\n\n"
        f"👤 ID пользователя: {ticket[1]}\n\n"
        f"📝 Сообщение:\n{ticket[2]}",
        reply_markup=keyboard
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("reply_"))
async def reply_ticket(callback: CallbackQuery, state: FSMContext):
    if not db.is_admin(callback.from_user.id):
        await callback.answer("❌ Нет прав", show_alert=True)
        return
    
    ticket_id = int(callback.data.split("_")[1])
    await state.update_data(ticket_id=ticket_id)
    await state.set_state(ReplyState.waiting_answer)
    await callback.message.edit_text("✉️ Введите текст ответа пользователю:")
    await callback.answer()

@dp.message(ReplyState.waiting_answer)
async def send_ticket_answer(message: Message, state: FSMContext):
    if not db.is_admin(message.from_user.id):
        await message.answer("❌ Нет прав")
        return
    
    data = await state.get_data()
    ticket_id = data["ticket_id"]

    cursor = db.conn.execute("SELECT user_id FROM tickets WHERE id=?", (ticket_id,))
    ticket = cursor.fetchone()
    if not ticket:
        await message.answer("Ошибка тикета")
        return

    user_id = ticket[0]
    safe_answer = html.escape(message.text)
    
    await bot.send_message(
        user_id,
        f"📩 <b>Ответ поддержки:</b>\n\n{safe_answer}\n\n"
        f"👨‍💻 Создатели: @prostokiril, @ll1_what"
    )

    db.conn.execute(
        "UPDATE tickets SET answer=?, status='Закрыт' WHERE id=?",
        (safe_answer, ticket_id)
    )
    db.conn.commit()
    await state.clear()
    await message.answer("✅ Ответ отправлен")

@dp.callback_query(F.data.startswith("close_"))
async def close_ticket(callback: CallbackQuery):
    if not db.is_admin(callback.from_user.id):
        await callback.answer("❌ Нет прав", show_alert=True)
        return
    
    ticket_id = int(callback.data.split("_")[1])
    db.conn.execute("UPDATE tickets SET status='Закрыт' WHERE id=?", (ticket_id,))
    db.conn.commit()
    await callback.message.edit_text("✅ Тикет закрыт")
    await callback.answer()

############################################################
# ADMIN SETTINGS
############################################################

@dp.callback_query(F.data == "add_admin")
async def add_admin_start(callback: CallbackQuery, state: FSMContext):
    if not db.is_admin(callback.from_user.id) or callback.from_user.id != OWNER_ID:
        await callback.answer("❌ Только владелец может назначать админов", show_alert=True)
        return
    await state.update_data(action="add")
    await state.set_state(AdminState.waiting_username)
    await callback.message.edit_text("Введите @username нового администратора:")
    await callback.answer()

@dp.callback_query(F.data == "remove_admin")
async def remove_admin_start(callback: CallbackQuery, state: FSMContext):
    if not db.is_admin(callback.from_user.id) or callback.from_user.id != OWNER_ID:
        await callback.answer("❌ Только владелец может снимать админов", show_alert=True)
        return
    await state.update_data(action="remove")
    await state.set_state(AdminState.waiting_username)
    await callback.message.edit_text("Введите @username администратора для снятия полномочий:")
    await callback.answer()

@dp.message(AdminState.waiting_username)
async def admin_action_finish(message: Message, state: FSMContext):
    if not db.is_admin(message.from_user.id) or message.from_user.id != OWNER_ID:
        await message.answer("❌ Нет прав")
        return
    
    data = await state.get_data()
    action = data.get("action", "add")

    username = message.text.replace("@", "")
    user = db.get_username(username)
    if not user:
        await message.answer("❌ Пользователь не найден")
        return

    if user[0] == OWNER_ID:
        await message.answer("❌ Нельзя изменить права владельца")
        return

    if action == "add":
        db.set_admin(user[0], True)
        db.add_admin_log(message.from_user.id, f"Назначил админа {username}", user[0])
        await state.clear()
        await message.answer(f"✅ @{username} теперь администратор")
    else:
        db.set_admin(user[0], False)
        db.add_admin_log(message.from_user.id, f"Снял админа {username}", user[0])
        await state.clear()
        await message.answer(f"🗑 @{username} больше не администратор")

############################################################
# PROMO CODES (ADMIN)
############################################################

@dp.callback_query(F.data == "promo_admin")
async def promo_admin(callback: CallbackQuery):
    if not db.is_admin(callback.from_user.id):
        await callback.answer("❌ Нет прав", show_alert=True)
        return
    
    await callback.message.edit_text(
        "🎁 <b>Управление промокодами</b>\n\n"
        "Выберите действие:",
        reply_markup=promo_admin_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data == "promo_list")
async def promo_list(callback: CallbackQuery):
    if not db.is_admin(callback.from_user.id):
        await callback.answer("❌ Нет прав", show_alert=True)
        return
    
    promos = db.conn.execute(
        "SELECT code, days, uses, max_uses FROM promo_codes ORDER BY code"
    ).fetchall()
    
    if not promos:
        await callback.message.edit_text(
            "📋 Список промокодов пуст",
            reply_markup=promo_admin_keyboard()
        )
        return
    
    text = "📋 <b>Список промокодов</b>\n\n"
    for p in promos:
        text += f"🎟 {p[0]}: +{p[1]} дней ({p[2]}/{p[3]} использований)\n"
    
    await callback.message.edit_text(text, reply_markup=promo_admin_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "promo_create")
async def promo_create_start(callback: CallbackQuery, state: FSMContext):
    if not db.is_admin(callback.from_user.id):
        await callback.answer("❌ Нет прав", show_alert=True)
        return
    
    await state.set_state(PromoCreateState.waiting_code)
    await callback.message.edit_text(
        "🎁 <b>Создание промокода</b>\n\n"
        "Введите код промокода (латиница, цифры):\n"
        "Например: <code>SUMMER2026</code>",
        reply_markup=back_keyboard()
    )
    await callback.answer()

@dp.message(PromoCreateState.waiting_code)
async def promo_create_code(message: Message, state: FSMContext):
    if not db.is_admin(message.from_user.id):
        await message.answer("❌ Нет прав")
        return
    
    code = message.text.upper().strip()
    
    existing = db.conn.execute("SELECT code FROM promo_codes WHERE code=?", (code,)).fetchone()
    if existing:
        await message.answer("❌ Такой промокод уже существует")
        return
    
    if len(code) < 3 or len(code) > 20:
        await message.answer("❌ Код должен быть от 3 до 20 символов")
        return
    
    await state.update_data(code=code)
    await state.set_state(PromoCreateState.waiting_days)
    await message.answer(
        "📅 Введите количество дней, которое даёт промокод:\n"
        "Например: <code>30</code>"
    )

@dp.message(PromoCreateState.waiting_days)
async def promo_create_days(message: Message, state: FSMContext):
    if not db.is_admin(message.from_user.id):
        await message.answer("❌ Нет прав")
        return
    
    try:
        days = int(message.text.strip())
    except:
        await message.answer("❌ Введите число")
        return
    
    if days <= 0 or days > MAX_DAYS:
        await message.answer(f"❌ Количество дней должно быть от 1 до {MAX_DAYS}")
        return
    
    await state.update_data(days=days)
    await state.set_state(PromoCreateState.waiting_max_uses)
    await message.answer(
        "🔢 Введите максимальное количество использований:\n"
        "Например: <code>100</code>"
    )

@dp.message(PromoCreateState.waiting_max_uses)
async def promo_create_max_uses(message: Message, state: FSMContext):
    if not db.is_admin(message.from_user.id):
        await message.answer("❌ Нет прав")
        return
    
    try:
        max_uses = int(message.text.strip())
    except:
        await message.answer("❌ Введите число")
        return
    
    if max_uses <= 0:
        await message.answer("❌ Количество использований должно быть больше 0")
        return
    
    data = await state.get_data()
    code = data.get("code")
    days = data.get("days")
    
    db.conn.execute(
        "INSERT INTO promo_codes (code, days, uses, max_uses) VALUES(?,?,0,?)",
        (code, days, max_uses)
    )
    db.conn.commit()
    
    db.add_admin_log(message.from_user.id, f"Создал промокод {code} (+{days} дней, {max_uses} использований)")
    
    await state.clear()
    await message.answer(
        f"✅ <b>Промокод создан!</b>\n\n"
        f"🎟 Код: <code>{code}</code>\n"
        f"📅 Дней: {days}\n"
        f"🔢 Использований: {max_uses}"
    )

############################################################
# DONATIONALERTS API
############################################################

async def get_donation_alerts():
    if not DONATION_ACCESS_TOKEN:
        logging.warning("DonationAlerts токен не настроен")
        return []
    
    headers = {"Authorization": f"Bearer {DONATION_ACCESS_TOKEN}"}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{DONATION_API_URL}/alerts/donations", headers=headers) as response:
                if response.status != 200:
                    logging.error(f"DonationAlerts ошибка: {response.status}")
                    return []
                data = await response.json()
                return data.get("data", [])
    except Exception as e:
        logging.error(f"DonationAlerts API: {e}")
        return []

async def activate_payment(payment, donation_id):
    user_id = payment[1]
    days = payment[4]
    amount = payment[3]
    user = db.get_user(user_id)
    if not user:
        return

    try:
        expire = datetime.strptime(user[3], "%Y-%m-%d %H:%M:%S")
    except:
        expire = datetime.strptime(user[3], "%Y-%m-%d")
    
    if expire < datetime.now():
        expire = datetime.now()
    expire += timedelta(days=days)
    expire_str = expire.strftime("%Y-%m-%d 23:59:59")

    db.conn.execute(
        "UPDATE users SET expire_date=?, status='Активно' WHERE id=?",
        (expire_str, user_id)
    )
    db.conn.execute(
        "UPDATE payments SET status='paid' WHERE id=?",
        (payment[0],)
    )
    db.mark_donation_processed(donation_id)
    db.add_payment_history(user_id, amount, days)
    
    if user[7] == 0:
        db.mark_first_payment(user_id)
        if user[6] > 0:
            await give_referral_bonus(user[6], user_id)
    
    db.conn.commit()

    await bot.send_message(
        user_id,
        f"🎉 <b>Оплата получена!</b>\n\n"
        f"✅ Подписка продлена на {days} дней.\n\n"
        f"👨‍💻 Создатели: @prostokiril, @ll1_what"
    )

async def check_payments():
    while True:
        try:
            donations = await get_donation_alerts()
            for donation in donations:
                donation_id = str(donation.get("id", ""))
                message = str(donation.get("message", ""))
                amount = int(donation.get("amount", 0))
                
                if db.is_donation_processed(donation_id):
                    continue
                
                cursor = db.conn.execute(
                    "SELECT * FROM payments WHERE payment_code=? AND amount=? AND status='waiting'",
                    (message, amount)
                )
                payment = cursor.fetchone()
                if payment:
                    await activate_payment(payment, donation_id)
        except Exception as e:
            logging.error(f"Проверка оплат: {e}")
        await asyncio.sleep(60)

############################################################
# BACKGROUND TASKS
############################################################

async def check_subscriptions():
    while True:
        try:
            cursor = db.conn.execute("SELECT id, expire_date FROM users")
            users = cursor.fetchall()
            now = datetime.now()
            for user in users:
                try:
                    expire = datetime.strptime(user[1], "%Y-%m-%d %H:%M:%S")
                except:
                    expire = datetime.strptime(user[1], "%Y-%m-%d")
                
                if expire < now:
                    db.conn.execute("UPDATE users SET status='Неактивно' WHERE id=?", (user[0],))
                else:
                    db.conn.execute("UPDATE users SET status='Активно' WHERE id=?", (user[0],))
            db.conn.commit()
        except Exception as e:
            logging.error(f"Ошибка проверки подписок: {e}")
        await asyncio.sleep(60)

async def subscription_notifications():
    while True:
        try:
            users = db.conn.execute("SELECT id, expire_date FROM users").fetchall()
            
            for user in users:
                try:
                    expire = datetime.strptime(user[1], "%Y-%m-%d %H:%M:%S")
                except:
                    expire = datetime.strptime(user[1], "%Y-%m-%d")
                
                days = (expire - datetime.now()).days
                
                if days == 7 and not db.notification_sent(user[0], "7days"):
                    await bot.send_message(
                        user[0],
                        f"⏰ <b>Stopka VPN</b>\n\n"
                        f"Ваша подписка закончится через <b>7 дней</b>.\n\n"
                        f"Продлите её заранее 💳\n\n"
                        f"👨‍💻 Создатели: @prostokiril, @ll1_what"
                    )
                    db.save_notification(user[0], "7days")
                
                elif days == 3 and not db.notification_sent(user[0], "3days"):
                    await bot.send_message(
                        user[0],
                        f"⏰ <b>Stopka VPN</b>\n\n"
                        f"Ваша подписка закончится через <b>3 дня</b>.\n\n"
                        f"Продлите её заранее 💳\n\n"
                        f"👨‍💻 Создатели: @prostokiril, @ll1_what"
                    )
                    db.save_notification(user[0], "3days")
                
                elif days == 1 and not db.notification_sent(user[0], "1day"):
                    await bot.send_message(
                        user[0],
                        f"⚠️ <b>Stopka VPN</b>\n\n"
                        f"Ваша подписка закончится <b>завтра</b>!\n\n"
                        f"Срочно продлите её 💳\n\n"
                        f"👨‍💻 Создатели: @prostokiril, @ll1_what"
                    )
                    db.save_notification(user[0], "1day")
                
                elif days < 0 and not db.notification_sent(user[0], "expired"):
                    await bot.send_message(
                        user[0],
                        f"❌ <b>Stopka VPN</b>\n\n"
                        f"Ваша подписка <b>закончилась</b>!\n\n"
                        f"Для восстановления доступа оплатите тариф 💳\n\n"
                        f"👨‍💻 Создатели: @prostokiril, @ll1_what"
                    )
                    db.save_notification(user[0], "expired")
                    
        except Exception as e:
            logging.error(f"Notify error {e}")
        await asyncio.sleep(3600)

async def export_json():
    while True:
        try:
            cursor = db.conn.execute("SELECT * FROM users")
            users = cursor.fetchall()
            data = []
            for user in users:
                data.append({
                    "id": user[0],
                    "username": user[1],
                    "name": user[2],
                    "expire_date": user[3],
                    "status": user[4],
                    "is_admin": bool(user[5]),
                    "invited_by": user[6],
                    "first_payment": bool(user[7]),
                    "last_tariff": user[8]
                })
            with open(JSON_FILE, "w", encoding="utf-8") as file:
                json.dump(data, file, indent=4, ensure_ascii=False)
        except Exception as e:
            logging.error(f"Ошибка JSON: {e}")
        await asyncio.sleep(60)

############################################################
# WEBHOOK FOR DONATIONALERTS
############################################################

async def donation_webhook(request):
    """Обработка webhook от DonationAlerts"""
    try:
        # Проверка подписи
        signature = request.headers.get("X-DonationAlerts-Signature", "")
        body = await request.text()
        
        if DONATION_SECRET_KEY:
            computed = hmac.new(
                DONATION_SECRET_KEY.encode(),
                body.encode(),
                hashlib.sha256
            ).hexdigest()
            if not hmac.compare_digest(computed, signature):
                return web.Response(status=403)
        
        data = await request.json()
        donation = data.get("data", {})
        
        donation_id = str(donation.get("id", ""))
        message = str(donation.get("message", ""))
        amount = int(donation.get("amount", 0))
        
        if db.is_donation_processed(donation_id):
            return web.Response(status=200)
        
        cursor = db.conn.execute(
            "SELECT * FROM payments WHERE payment_code=? AND amount=? AND status='waiting'",
            (message, amount)
        )
        payment = cursor.fetchone()
        if payment:
            await activate_payment(payment, donation_id)
        
        return web.Response(status=200)
    except Exception as e:
        logging.error(f"Webhook error: {e}")
        return web.Response(status=500)

############################################################
# RENDER SERVER
############################################################

async def render_server():
    app = web.Application()
    
    async def home(request):
        return web.Response(text="Stopka VPN is running")
    
    app.router.add_get("/", home)
    app.router.add_post("/payment", donation_webhook)
    
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    logging.info(f"🌐 HTTP сервер запущен на порту {PORT}")

############################################################
# BOT COMMANDS & ERROR HANDLER
############################################################

async def set_commands():
    commands = [
        BotCommand(command="start", description="🚀 Запустить бота"),
        BotCommand(command="help", description="❓ Помощь")
    ]
    await bot.set_my_commands(commands)

@dp.error()
async def error_handler(event, exception):
    logging.error(f"Ошибка: {exception}")
    return True

############################################################
# START BOT
############################################################

async def main():
    logging.info("🚀 Запуск Stopka VPN...")
    await set_commands()
    await render_server()

    asyncio.create_task(check_subscriptions())
    asyncio.create_task(export_json())
    asyncio.create_task(check_payments())
    asyncio.create_task(subscription_notifications())

    logging.info("✅ Stopka VPN запущен успешно!")
    logging.info("👨‍💻 Создатели: @prostokiril, @ll1_what")
    logging.info(f"🤖 Бот: @{(await bot.get_me()).username}")
    logging.info(f"🌐 Сервер: http://localhost:{PORT}")
    logging.info(f"📡 Webhook: http://localhost:{PORT}/payment")
    
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
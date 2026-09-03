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
import uuid
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

# Настройки панели VPN (3x-ui)
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

BOT_USERNAME = None  # кэшируется один раз при старте, чтобы не дёргать get_me() на каждый клик

############################################################
# VPN API CLIENT (3x-ui)
############################################################

class VPNClient:
    """Клиент для панели 3x-ui (https://github.com/MHSanaei/3x-ui).

    В отличие от Marzban, у 3x-ui:
    - авторизация через cookie сессии (не Bearer-токен) — держим один
      aiohttp.ClientSession на весь клиент, он сам хранит cookie между запросами;
    - клиенты создаются/обновляются через inbound: нужно знать ID нужного
      inbound'а (переменная окружения THREEXUI_INBOUND_ID);
    - есть НАТИВНОЕ ограничение по количеству устройств — поле limitIp
      у клиента (переменная окружения THREEXUI_DEVICE_LIMIT, по умолчанию 5);
    - готовая ссылка не возвращается напрямую, как в Marzban — используется
      сервис подписки 3x-ui: https://host:SUB_PORT/SUB_PATH/{subId}.
    """
    def __init__(self):
        self.base_url = VPN_API_URL.rstrip("/")
        self.username = VPN_ADMIN_USERNAME
        self.password = VPN_ADMIN_PASSWORD
        self.inbound_id = int(os.environ.get("THREEXUI_INBOUND_ID", "1"))
        self.device_limit = int(os.environ.get("THREEXUI_DEVICE_LIMIT", "5"))
        self.sub_port = os.environ.get("THREEXUI_SUB_PORT", "")
        self.sub_path = os.environ.get("THREEXUI_SUB_PATH", "sub").strip("/")
        self._session = None
        self._logged_in = False

    def _get_session(self):
        # Один переиспользуемый session — он же хранит cookie сессии 3x-ui
        # между запросами, поэтому логиниться заново на каждый вызов не нужно.
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
                f"{self.base_url}/login",
                data={"username": self.username, "password": self.password}
            ) as resp:
                try:
                    data = await resp.json()
                except Exception:
                    data = {}
                self._logged_in = resp.status == 200 and data.get("success", False)
                if not self._logged_in:
                    logging.error(f"3x-ui: не удалось авторизоваться (status={resp.status})")
        except Exception as e:
            logging.error(f"Ошибка авторизации в 3x-ui: {e}")
            self._logged_in = False

    async def _api_get(self, path):
        if not self._logged_in:
            await self.login()
        session = self._get_session()
        for attempt in range(2):
            try:
                async with session.get(f"{self.base_url}{path}") as resp:
                    if resp.status == 401 and attempt == 0:
                        await self.login()
                        continue
                    try:
                        return await resp.json()
                    except Exception:
                        return None
            except Exception as e:
                logging.error(f"Ошибка запроса к 3x-ui ({path}): {e}")
                return None
        return None

    async def _api_post(self, path, payload):
        if not self._logged_in:
            await self.login()
        session = self._get_session()
        for attempt in range(2):
            try:
                async with session.post(
                    f"{self.base_url}{path}",
                    json=payload,
                    headers={"Content-Type": "application/json"}
                ) as resp:
                    if resp.status == 401 and attempt == 0:
                        await self.login()
                        continue
                    try:
                        data = await resp.json()
                    except Exception:
                        # 3x-ui иногда отвечает пустой строкой вместо JSON — известная особенность панели.
                        data = None
                    return resp.status == 200 and data is not None and data.get("success", False)
            except Exception as e:
                logging.error(f"Ошибка запроса к 3x-ui ({path}): {e}")
                return False
        return False

    async def _find_existing_client(self, email):
        """Возвращает (client_dict) существующего клиента по email или None."""
        data = await self._api_get(f"/panel/api/inbounds/get/{self.inbound_id}")
        if not data or not data.get("success") or not data.get("obj"):
            return None
        try:
            settings = json.loads(data["obj"].get("settings", "{}"))
        except Exception:
            return None
        for client in settings.get("clients", []):
            if client.get("email") == email:
                return client
        return None

    def _build_sub_link(self, sub_id):
        if not sub_id:
            return ""
        if self.sub_port:
            host = self.base_url.split("://")[-1].split("/")[0].split(":")[0]
            scheme = "https" if self.base_url.startswith("https") else "http"
            return f"{scheme}://{host}:{self.sub_port}/{self.sub_path}/{sub_id}"
        # Сервер подписки не настроен через ENV — отдаём хотя бы sub_id,
        # чтобы не возвращать пустую строку молча.
        return sub_id

    async def create_or_update_user(self, user_id, expire_timestamp):
        email = f"user_{user_id}"
        expiry_ms = int(expire_timestamp) * 1000  # 3x-ui ждёт миллисекунды, не секунды

        existing = await self._find_existing_client(email)

        if existing:
            client_uuid = existing.get("id")
            sub_id = existing.get("subId") or uuid.uuid4().hex
            client_payload = {
                "id": client_uuid,
                "email": email,
                "enable": True,
                "expiryTime": expiry_ms,
                "limitIp": self.device_limit,
                "totalGB": existing.get("totalGB", 0),
                "tgId": existing.get("tgId", ""),
                "subId": sub_id,
                "reset": existing.get("reset", 0),
                "flow": existing.get("flow", "")
            }
            ok = await self._api_post(
                f"/panel/api/inbounds/updateClient/{client_uuid}",
                {"id": self.inbound_id, "settings": json.dumps({"clients": [client_payload]})}
            )
        else:
            client_uuid = str(uuid.uuid4())
            sub_id = uuid.uuid4().hex
            client_payload = {
                "id": client_uuid,
                "email": email,
                "enable": True,
                "expiryTime": expiry_ms,
                "limitIp": self.device_limit,
                "totalGB": 0,
                "tgId": "",
                "subId": sub_id,
                "reset": 0,
                "flow": ""
            }
            ok = await self._api_post(
                "/panel/api/inbounds/addClient",
                {"id": self.inbound_id, "settings": json.dumps({"clients": [client_payload]})}
            )

        if not ok:
            return ""
        return self._build_sub_link(sub_id)

    async def disable_user(self, user_id):
        # Реально отключает доступ на стороне 3x-ui (а не только в БД бота) —
        # без этого Happ/v2rayTun продолжали бы работать с ключом после истечения дней.
        email = f"user_{user_id}"
        existing = await self._find_existing_client(email)
        if not existing:
            return False
        existing["enable"] = False
        return await self._api_post(
            f"/panel/api/inbounds/updateClient/{existing['id']}",
            {"id": self.inbound_id, "settings": json.dumps({"clients": [existing]})}
        )

    async def delete_client(self, client_uuid):
        # Полное удаление клиента на панели 3x-ui — именно это делает старый
        # ключ нерабочим (в отличие от updateClient, id/subId остаются старыми).
        if not self._logged_in:
            await self.login()
        session = self._get_session()
        for attempt in range(2):
            try:
                async with session.post(
                    f"{self.base_url}/panel/api/inbounds/{self.inbound_id}/delClient/{client_uuid}"
                ) as resp:
                    if resp.status == 401 and attempt == 0:
                        await self.login()
                        continue
                    try:
                        data = await resp.json()
                    except Exception:
                        data = None
                    return resp.status == 200 and data is not None and data.get("success", False)
            except Exception as e:
                logging.error(f"Ошибка удаления клиента в 3x-ui: {e}")
                return False
        return False

    async def reset_user_key(self, user_id, expire_timestamp):
        """Полностью пересоздаёт ключ пользователя: старый клиент удаляется
        на панели 3x-ui (перестаёт работать сразу и необратимо), взамен
        создаётся новый клиент с новым id и новым subId — то есть выдаётся
        совсем другой ключ, а не обновление старого."""
        email = f"user_{user_id}"
        existing = await self._find_existing_client(email)
        if existing and existing.get("id"):
            await self.delete_client(existing["id"])
        # После удаления create_or_update_user не найдёт старого клиента
        # и создаст новый — с новым uuid и subId.
        return await self.create_or_update_user(user_id, expire_timestamp)

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
# PER-USER LOCK
############################################################
# aiogram по умолчанию обрабатывает каждый апдейт как отдельную независимую
# asyncio-задачу — то есть если один и тот же пользователь присылает два
# /start подряд быстро (или /start и нажатие "Подключить" почти одновременно),
# оба обработчика реально выполняются ПАРАЛЛЕЛЬНО, а не по очереди. Каждый
# запрос к БД сам по себе консистентен (см. фикс PGConnection.execute выше),
# но между несколькими awaited шагами ОДНОГО обработчика другой параллельный
# обработчик того же user_id может успеть вклиниться и сработать на
# промежуточном/устаревшем состоянии (например, второй /start стартует и
# читает профиль раньше, чем первый /start успел дописать username и
# закоммититься). Лок на user_id сериализует такие пересекающиеся вызовы —
# второй дождётся, пока первый полностью завершится, и увидит уже
# гарантированно актуальное состояние.
user_locks = defaultdict(asyncio.Lock)

############################################################
# DATABASE
############################################################

IntegrityError = psycopg2.errors.lookup(psycopg2.errorcodes.UNIQUE_VIOLATION)

class _PoolWithSetup(pg_pool.ThreadedConnectionPool):
    """Пул соединений psycopg2: при создании КАЖДОГО нового физического
    соединения применяет те же настройки, что раньше выставлялись один раз
    для единственного общего соединения (autocommit, search_path)."""
    def _connect(self, key=None):
        conn = super()._connect(key)
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SET search_path TO public;")
        return conn

class _FetchedResult:
    """Лёгкая замена курсора: хранит уже вычитанные из БД строки и rowcount.

    ВАЖНО: раньше PGConnection.execute() возвращал вызывающему коду "живой"
    psycopg2-курсор ПОСЛЕ того, как физическое соединение уже было отдано
    обратно в пул (putconn). Пока вызывающий код ещё не успел сделать
    fetchone()/fetchall() на этом курсоре, то же самое соединение из пула мог
    забрать другой параллельный запрос (например, несколько человек почти
    одновременно жмут /start по реферальной ссылке) и начать выполнять на
    нём свой запрос — psycopg2-соединение не рассчитано на одновременное
    использование двумя запросами сразу. Из-за этого /start мог падать с
    ошибками у пользователей, пришедших по ссылке во время всплеска трафика.
    Теперь строки вычитываются и rowcount фиксируется ДО того, как
    соединение возвращается в пул — соединение больше никем не используется
    в момент возврата."""
    __slots__ = ("_rows", "_pos", "rowcount")

    def __init__(self, rows, rowcount):
        self._rows = rows
        self._pos = 0
        self.rowcount = rowcount

    def fetchone(self):
        if self._pos >= len(self._rows):
            return None
        row = self._rows[self._pos]
        self._pos += 1
        return row

    def fetchall(self):
        rows = self._rows[self._pos:]
        self._pos = len(self._rows)
        return rows

class PGConnection:
    def __init__(self, dsn, minconn=1, maxconn=10):
        self._dsn = dsn
        # Раньше было одно соединение на весь бот (с локом на очередь запросов) —
        # это было БЕЗОПАСНО, но все запросы шли строго по очереди, один за другим.
        # Пул даёт до `maxconn` реально параллельных соединений: несколько
        # пользователей могут обращаться к БД одновременно без взаимного ожидания.
        # ThreadedConnectionPool сам по себе потокобезопасен — свой лок не нужен.
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
        # После долгого простоя Neon "усыпляет" базу — и ВСЕ соединения в пуле
        # протухают одновременно (они простаивали вместе), а не одно. Поэтому
        # попыток должно хватать на весь пул, а не на пару соединений — иначе
        # первые запросы после сна всё равно с шансом получают битое соединение
        # два раза подряд и падают с ошибкой (особенно у новых пользователей —
        # на один /start уходит больше запросов к БД, а значит и больше шансов
        # попасть на протухшее соединение несколько раз подряд).
        max_attempts = self._pool.maxconn + 1
        for attempt in range(max_attempts):
            try:
                conn = self._pool.getconn()
            except pg_pool.PoolError as e:
                # Пул временно исчерпан (много запросов одновременно) —
                # короткая пауза и повтор, а не мгновенный отказ пользователю.
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
                # Вычитываем результат и rowcount, пока соединение ещё у нас,
                # и только потом отдаём его обратно в пул (см. _FetchedResult
                # выше — почему это критично для параллельных запросов).
                if cur.description is not None:
                    rows = cur.fetchall()
                else:
                    rows = []
                rowcount = cur.rowcount
                cur.close()
                self._pool.putconn(conn)
                return _FetchedResult(rows, rowcount)
            except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                last_error = e
                try:
                    self._pool.putconn(conn, close=True)
                except:
                    pass
                logging.warning(f"БД: мёртвое соединение из пула, пересоздаю и повторяю запрос (попытка {attempt + 1}/{max_attempts})")
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
        # Каждое соединение в пуле работает в autocommit — отдельный commit()
        # не нужен. Метод оставлен для совместимости с уже написанным кодом,
        # которое вызывает db.conn.commit() по всему боту.
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
        # vless_key был только внутри CREATE TABLE IF NOT EXISTS выше — для уже
        # существующей таблицы (как в проде) это ничего не добавляет, колонки
        # не было физически. Добавляем её той же миграцией, что и
        # accepted_terms/trial_used ниже — иначе INSERT нового пользователя
        # (упоминает vless_key) падает с UndefinedColumn для КАЖДОГО нового
        # пользователя, реферал тут вообще ни при чём.
        self.conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS vless_key TEXT DEFAULT ''")
        
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS tickets(
            id SERIAL PRIMARY KEY,
            user_id BIGINT,
            message TEXT,
            answer TEXT,
            status TEXT
        )
        """)
        # Вложения к тикетам (фото/файл) — добавляем колонки, если их ещё нет
        # (безопасно и для новой БД, и для уже существующей)
        self.conn.execute("ALTER TABLE tickets ADD COLUMN IF NOT EXISTS file_id TEXT DEFAULT ''")
        self.conn.execute("ALTER TABLE tickets ADD COLUMN IF NOT EXISTS file_type TEXT DEFAULT ''")
        # Нужна для авточистки: удаляем по возрасту ЗАКРЫТИЯ, а не создания,
        # и только закрытые тикеты — открытые обращения не трогаем никогда.
        self.conn.execute("ALTER TABLE tickets ADD COLUMN IF NOT EXISTS closed_at TEXT DEFAULT ''")

        # Флаг «принял условия использования / политику конфиденциальности» —
        # чтобы приветственный экран показывался пользователю ровно один раз.
        # Если колонки ещё не было — это миграция на уже работающей базе:
        # всех текущих пользователей амнистируем (иначе им внезапно покажется
        # приветственный экран и слетит уже оплаченная подписка).
        cur = self.conn.execute("""
            SELECT 1 FROM information_schema.columns
            WHERE table_name='users' AND column_name='accepted_terms'
        """)
        column_existed = cur.fetchone() is not None

        self.conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS accepted_terms INTEGER DEFAULT 0")

        if not column_existed:
            self.conn.execute("UPDATE users SET accepted_terms=1")

        # Отдельный, отдельно от accepted_terms, флаг "пробный период уже был
        # выдан этому пользователю" — чтобы истечение/отключение подписки
        # никогда не приводило к повторной раздаче бесплатных дней.
        trial_col_existed = (self.conn.execute("""
            SELECT 1 FROM information_schema.columns
            WHERE table_name='users' AND column_name='trial_used'
        """)).fetchone() is not None
        self.conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS trial_used INTEGER DEFAULT 0")
        if not trial_col_existed:
            # Всех, кто уже принял условия (грандфазеринг выше) — считаем
            # уже использовавшими пробный период, иначе им внезапно перевыдаст.
            self.conn.execute("UPDATE users SET trial_used=1 WHERE accepted_terms=1")
        logging.info(f"create_tables(): миграция trial_used — column_existed_before={trial_col_existed}")
        diag = self.conn.execute("""
            SELECT column_name, ordinal_position FROM information_schema.columns
            WHERE table_name='users' ORDER BY ordinal_position
        """).fetchall()
        logging.info(f"create_tables(): текущие колонки users по порядку = {diag}")
        
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
        # INSERT ... RETURNING вместо SELECT-затем-INSERT-затем-SELECT — три
        # обращения к БД для нового пользователя превращаются в одно. Меньше
        # круговых обращений — меньше шансов зацепить нестабильность пула
        # именно в тот момент, когда новый пользователь первый раз жмёт /start.
        expire = datetime.now() + timedelta(days=0)
        expire_str = expire.strftime("%Y-%m-%d %H:%M:%S")

        cursor = self.conn.execute("""
            INSERT INTO users (id, username, name, expire_date, status, is_admin, invited_by, first_payment, last_tariff, username_history, balance, vless_key) 
            VALUES(?,?,?,?,?,0,0,0,'',?,0,'')
            ON CONFLICT (id) DO NOTHING
            RETURNING *
        """, (user_id, username or "", name or "", expire_str, "Отключено", json.dumps([])))
        row = cursor.fetchone()
        self.conn.commit()
        return row

    def get_user(self, user_id):
        cursor = self.conn.execute("SELECT * FROM users WHERE id=?", (user_id,))
        return cursor.fetchone()

    def get_trial_status(self, user_id):
        """Отдельный запрос ИМЕННО по названиям колонок accepted_terms/trial_used,
        а не по позиции в SELECT * — так индексация в user[12]/user[13] никогда
        не сможет разъехаться со схемой, что бы ни случилось с порядком колонок."""
        cursor = self.conn.execute(
            "SELECT accepted_terms, trial_used FROM users WHERE id=?", (user_id,)
        )
        row = cursor.fetchone()
        if not row:
            return (0, 0)
        return (row[0] or 0, row[1] or 0)

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
# ТАРИФЫ
############################################################

TARIFFS = {
    "month": {"name": "месяц", "days": 30, "price": 150, "stars": 150},
    "half": {"name": "полгода", "days": 180, "price": 800, "stars": 800},
    "year": {"name": "год", "days": 365, "price": 1600, "stars": 1600}
}

############################################################
# ЮРИДИЧЕСКИЕ ДОКУМЕНТЫ
############################################################

TRIAL_DAYS = 3
RUB_PER_DAY = 5  # декоративный курс для профиля: показывается как {дни}×5₽, без реального смысла

############################################################
# ЛЁГКИЙ АНТИСПАМ
############################################################
# Простой кулдаун по действиям (не строгий, чтобы не мешать обычным пользователям) —
# защищает только от быстрого повторного долбления одной и той же кнопки/попыток.

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
Настоящие Условия использования (далее — «Документ») регулируют отношения между Пользователем (далее — «Вы», «Ваш», «Субъект») и Сервисом Stopka VPN (далее — «Мы», «Наш», «Оператор») в рамках предоставления услуг по изменению IP-адреса и шифрованию интернет-трафика. Начиная использовать Сервис, Вы подтверждаете, что полностью ознакомились с положениями Документа и принимаете их без каких-либо оговорок, исключений и условностей, за исключением случаев, прямо предусмотренных действующим законодательством Российской Федерации.

<b>2. Предмет соглашения и объем предоставляемых услуг</b>
Оператор обязуется предоставить Пользователю доступ к программно-аппаратному комплексу, обеспечивающему перенаправление интернет-соединения через удаленные серверы, расположенные в различных географических зонах. Пользователь понимает и соглашается, что фактическая скорость передачи данных, задержка (пинг) и стабильность соединения зависят от множества факторов, находящихся вне контроля Оператора, включая, но не ограничиваясь: загруженность каналов связи, качество оборудования провайдера, погодные условия, солнечную активность и действия органов государственной власти.

<b>3. Права и обязанности Пользователя</b>
3.1. Вы имеете право подключаться к любому доступному серверу, представленному в списке, за исключением случаев технического обслуживания.
3.2. Вы имеете право прекратить использование Сервиса в любой момент без объяснения причин.
3.3. Вы имеете право обращаться в службу поддержки, однако Оператор не гарантирует мгновенного ответа в ночное время, выходные и праздничные дни, установленные на территории РФ.
3.4. Вы имеете право использовать Сервис на нескольких устройствах, однако несете ответственность за сохранность своего логина и пароля от третьих лиц.

<b>4. Ограничения и запреты</b>
Пользователю строго запрещается:
4.1. Использовать Сервис для проведения несанкционированных атак на информационные системы других лиц (DDoS, брутфорс, сканирование портов).
4.2. Распространять через соединение Stopka VPN материалы, пропагандирующие насилие, экстремизм, изготовление взрывчатых веществ или наркотических средств.
4.3. Нарушать авторские и смежные права, используя Сервис для массового нелегального скачивания торрентов в странах, где это преследуется по закону.
4.4. Перепродавать доступ к своему аккаунту третьим лицам или передавать его в аренду.

<b>5. Ограничение ответственности Оператора</b>
ОПЕРАТОР НЕ НЕСЕТ ОТВЕТСТВЕННОСТИ за любые косвенные, случайные или штрафные убытки Пользователя, возникшие в результате использования или невозможности использования Сервиса, включая, но не ограничиваясь: потерю данных, снижение производительности устройства, блокировку аккаунтов в социальных сетях по причине смены геолокации, а также за отказ в доступе к сайтам, если они используют собственные алгоритмы блокировки VPN-трафика. Сервис предоставляется «как есть» (AS-IS) без каких-либо явных или подразумеваемых гарантий.

<b>6. Срок действия и пролонгация</b>
Настоящие Условия вступают в силу с момента нажатия кнопки «Подключить» и действуют бессрочно до момента полного удаления Вашего аккаунта или прекращения деятельности Оператора. В случае изменения текста Условий, Оператор уведомляет Пользователя путем публикации новой редакции на официальном сайте за 10 (десять) календарных дней до вступления изменений в силу. Ваше молчаливое согласие с новой редакцией считается подтвержденным, если Вы продолжаете использовать Сервис по истечении указанного срока."""

PRIVACY_TEXT = """<b>ПОЛИТИКА КОНФИДЕНЦИАЛЬНОСТИ</b>

<b>1. Какие данные собираются</b>
Для идентификации Пользователя и обеспечения корректной работы Сервиса Stopka VPN может автоматически обрабатывать следующие категории информации:
1.1. Технические данные: Ваш реальный IP-адрес в момент подключения, MAC-адрес сетевого интерфейса, тип операционной системы, версия приложения, уникальный идентификатор устройства (Device ID), а также сведения о модели смартфона или компьютера.
1.2. Сессионная информация: Время входа в систему, время выхода, общий объем переданных и принятых мегабайт (трафик), а также выбранная страна сервера для подключения.
1.3. Платежная информация: Если Вы оформляете платную подписку, мы передаем Ваши данные (номер телефона или адрес электронной почты) в процессинговые центры, но не храним полные номера банковских карт на своих серверах (используется токенизация).

<b>2. Цели обработки данных</b>
2.1. Обеспечение стабильности работы сети и балансировки нагрузки между серверами.
2.2. Своевременное информирование Вас о технических сбоях и плановых технических работах.
2.3. Предотвращение мошеннических действий, попыток взлома аккаунтов и неестественно высокой нагрузки на инфраструктуру.
2.4. Ведения внутренней статистики для улучшения пользовательского опыта и интерфейса приложения.

<b>3. Передача данных третьим лицам</b>
Мы обязуемся НЕ передавать Ваши персональные данные коммерческим структурам для целей таргетированной рекламы без Вашего отдельного согласия. Однако, действуя в строгом соответствии с Федеральным законом № 242-ФЗ и № 374-ФЗ, Оператор оставляет за собой право предоставлять сведения о фактах подключения (время, IP, объем трафика) уполномоченным государственным органам (Роскомнадзору, ФСБ, МВД) на основании официального мотивированного запроса, оформленного в установленном законодательством порядке. В иных случаях данные не разглашаются.

<b>4. Хранение и сроки уничтожения</b>
Все логи подключений хранятся в зашифрованном виде на серверах, расположенных на территории Российской Федерации, в течение срока, необходимого для достижения целей обработки, но не менее 6 (шести) месяцев с момента окончания сессии. По истечении указанного срока данные подлежат автоматической анонимизации либо полному удалению с использованием методов гарантированного уничтожения информации.

<b>5. Ваши права как Субъекта данных</b>
В соответствии с ФЗ-152 «О персональных данных», Вы имеете право:
5.1. Запросить полную выписку обо всех Ваших данных, хранящихся у Оператора (один раз в год бесплатно).
5.2. Требовать уточнения, блокировки или уничтожения Ваших данных, если они являются неполными, устаревшими или полученными незаконным путем.
5.3. Отозвать свое согласие на обработку персональных данных путем отправки письменного заявления на электронную почту поддержки (в этом случае доступ к Сервису будет прекращен в течение 3 (трех) рабочих дней).

<b>6. Cookie и сторонние аналитические модули</b>
При использовании веб-версии Сервиса применяются технические cookie-файлы, необходимые для аутентификации и хранения настроек языка. Мы не используем шпионские скрипты и не отслеживаем историю Ваших посещений веб-страниц в открытом виде, так как весь трафик внутри туннеля зашифрован и не подлежит анализу с нашей стороны.

<b>7. Меры безопасности</b>
Оператор применяет современные криптографические протоколы (включая AES-256) для защиты передаваемых данных. Внутренний доступ к серверам с логами строго регламентирован и имеют только 3 (три) уполномоченных сотрудника отдела технической эксплуатации, подписавших соглашение о неразглашении."""

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

def vless_key_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Сбросить ключ", callback_data="reset_vless_key")],
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
        # Считаем по календарным датам, а не округлением сырой разницы во времени —
        # иначе выдача "3 дней" в 9 утра могла показывать "4 дня" из-за округления.
        return max(0, (expire.date() - now.date()).days)
    return 0

def build_profile_text(user_id, user_data):
    days = calculate_days(user_data[3])
    vpn_status = "✅ Активен" if days > 0 else "❌ Не активен"
    balance = days * RUB_PER_DAY  # чисто декоративно: 5₽ = 1 день, без реального смысла

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
    """Пытается отредактировать текст сообщения. Если это невозможно —
    например, текущее сообщение с фото/файлом (как открытый тикет с
    вложением), а Telegram не даёт превратить его в текстовое через edit —
    удаляет его и отправляет новое текстовое сообщение вместо него."""
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
    # is_admin известен из уже полученной строки пользователя — не дёргаем БД повторно
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

    # Сериализуем обработку по user_id — см. комментарий у user_locks выше.
    async with user_locks[user_id]:
        user = await asyncio.to_thread(db.get_user, user_id)
        if user:
            if (user[1] or "") != username:
                await asyncio.to_thread(db.update_username, user_id, username)
                user = await asyncio.to_thread(db.get_user, user_id)
        else:
            # Новый пользователь — add_user делает один INSERT...RETURNING и
            # сразу отдаёт готовую строку, без отдельных SELECT до и после.
            user = await asyncio.to_thread(db.add_user, user_id, username, message.from_user.full_name)
            if user is None:
                # Редкая гонка: кто-то другой успел создать эту же строку
                # между нашей проверкой и INSERT — просто дочитываем её.
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
                        # invited_by=0 проверяется прямо в WHERE самого UPDATE (атомарно),
                        # а не отдельным SELECT заранее — исключает гонку при двойном /start.
                        bind_cur = await asyncio.to_thread(
                            db.conn.execute,
                            "UPDATE users SET invited_by=? WHERE id=? AND invited_by=0",
                            (inviter, user_id)
                        )
                        if bind_cur.rowcount == 1:
                            await asyncio.to_thread(db.conn.execute, "INSERT INTO referrals (user_id, invited_by, bonus_given) VALUES(?,?,0) ON CONFLICT (user_id) DO NOTHING", (user_id, inviter))
                            await asyncio.to_thread(db.conn.commit)
                except Exception as e:
                    logging.error(f"Ошибка обработки реферала: {e}")

        # Показываем приветственный экран, только если пробный период ЕЩЁ НИ РАЗУ
        # не выдавался этому пользователю. Раньше здесь была проверка вида "или
        # у пользователя уже активна подписка — тогда не показываем", но это
        # ломалось ровно в момент, когда дни заканчивались (отключение админом
        # или истечение срока): условие переставало выполняться, экран вылезал
        # заново, а "Подключить" выдавал ещё один бесплатный пробный период —
        # то есть подписку можно было продлевать бесплатно бесконечно. Теперь
        # источник истины один: trial_used, который выставляется один раз и
        # никогда не сбрасывается — ни отключением, ни истечением подписки.
        accepted_flag, trial_used_flag = await asyncio.to_thread(db.get_trial_status, user_id)
        trial_used = trial_used_flag == 1
        if not trial_used:
            logging.warning(
                f"/start: показываю экран политики для user_id={user_id}, "
                f"по имени колонки (accepted_terms, trial_used)=({accepted_flag}, {trial_used_flag}), "
                f"raw row len={len(user)}"
            )
            await message.answer(WELCOME_TEXT, reply_markup=welcome_keyboard())
            return

        await render_profile(user_id, target_message=message, user=user)

@dp.message(Command("help"))
async def help_command(message: Message):
    await message.answer(
        "🛡 <b>Поддержка Stopka VPN</b>\n\n"
        "Не переживайте — если что-то пошло не так, мы обязательно разберёмся и поможем 🤝\n\n"
        "Опишите свой вопрос или проблему, а также приложите фото или файл (например, скриншот ошибки или чек об оплате) — так мы сможем помочь быстрее.\n\n"
        "Нажмите кнопку ниже, чтобы написать администраторам:",
        reply_markup=support_keyboard()
    )

@dp.message(Command("about"))
async def about_command(message: Message):
    await message.answer(
        '<a href="https://telegra.ph/POLITIKA-KONFIDENCIALNOSTI-09-02-81">ПОЛИТИКА КОНФИДЕНЦИАЛЬНОСТИ</a>\n\n'
        '<a href="https://telegra.ph/USLOVIYA-POLZOVANIYA-09-02-2">УСЛОВИЯ ПОЛЬЗОВАНИЯ</a>\n\n'
        "👨‍💻 Создатели: @prostokiril, @ll1_coo",
        parse_mode="HTML",
        disable_web_page_preview=True
    )

############################################################
# ПРИВЕТСТВЕННЫЙ ЭКРАН (принятие условий)
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

    # Тот же лок, что и в /start — иначе "Подключить" мог бы обработаться
    # параллельно с ещё выполняющимся /start того же пользователя.
    async with user_locks[user_id]:
        # Подстраховка: если строки пользователя почему-то ещё нет — создаём её,
        # прежде чем обновлять (иначе UPDATE ... WHERE id=? тихо не найдёт строку
        # и accepted_terms не сохранится).
        await asyncio.to_thread(
            db.add_user, user_id, callback.from_user.username or "", callback.from_user.full_name
        )

        # Пробный период даётся ровно один раз в жизни аккаунта. Условие
        # "trial_used=0" стоит прямо в WHERE самого UPDATE (а не решается заранее
        # отдельным SELECT) — так выдача атомарна: даже если пользователь успевает
        # нажать "Подключить" два раза почти одновременно (двойной тап,
        # нестабильная сеть и т.п.), только ОДИН из двух запросов реально
        # обновит строку и получит trial_used=0->1, второй увидит rowcount=0
        # и не выдаст дни повторно.
        expire = datetime.now() + timedelta(days=TRIAL_DAYS)
        expire_str = expire.strftime("%Y-%m-%d 23:59:59")
        cur = await asyncio.to_thread(
            db.conn.execute,
            "UPDATE users SET accepted_terms=1, trial_used=1, expire_date=?, status='Активно' "
            "WHERE id=? AND (trial_used=0 OR trial_used IS NULL)",
            (expire_str, user_id)
        )
        if cur.rowcount == 1:
            alert_text = f"🎉 Вам начислено {TRIAL_DAYS} дня VPN!"
            logging.info(f"accept_terms: user_id={user_id} — пробный период выдан, trial_used выставлен в 1 (rowcount=1)")
        else:
            # Пробный период уже был использован раньше (или гонка — его только что
            # выдал параллельный запрос) — повторно дни не начисляем, просто
            # фиксируем принятие условий "для галочки".
            await asyncio.to_thread(db.conn.execute, "UPDATE users SET accepted_terms=1 WHERE id=?", (user_id,))
            alert_text = "✅ Готово!"
            logging.warning(f"accept_terms: user_id={user_id} — UPDATE trial_used=0->1 не затронул строк (rowcount={cur.rowcount}), пробный период НЕ выдан повторно")

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
        await callback.answer("❌ Подписка неактивна. Оплатите дни, чтобы получить ключ для Happ.", show_alert=True)
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
            logging.error(f"Ошибка генерации ключа через API: {e}")

    if not vless_key:
        vless_key = f"vless://error-check-api-connection@panel:443?encryption=none&security=reality#StopkaVPN"

    await callback.message.edit_text(
        f"🔑 <b>Ваш ключ VLESS для Happ:</b>\n\n"
        f"Скопируйте эту строку и добавьте в приложение Happ:\n\n"
        f"<code>{vless_key}</code>",
        reply_markup=vless_key_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data == "reset_vless_key")
async def reset_vless_key(callback: CallbackQuery):
    user_id = callback.from_user.id
    user = await asyncio.to_thread(db.get_user, user_id)
    days = calculate_days(user[3])

    if days <= 0:
        await callback.answer("❌ Подписка неактивна. Оплатите дни, чтобы получить ключ для Happ.", show_alert=True)
        return

    await callback.answer("🔄 Обновляем ключ…")

    new_key = ""
    try:
        expire_dt = datetime.strptime(user[3], "%Y-%m-%d %H:%M:%S")
        timestamp = int(expire_dt.timestamp())
        new_key = await vpn_client.reset_user_key(user_id, timestamp)
        await asyncio.to_thread(db.conn.execute, "UPDATE users SET vless_key=? WHERE id=?", (new_key, user_id))
        await asyncio.to_thread(db.conn.commit)
    except Exception as e:
        logging.error(f"Ошибка сброса ключа через API: {e}")

    if not new_key:
        await callback.message.edit_text(
            "❌ Не удалось сбросить ключ. Попробуйте позже или напишите в поддержку.",
            reply_markup=vless_key_keyboard()
        )
        return

    await callback.message.edit_text(
        f"🔄 <b>Ключ обновлён!</b>\n\n"
        f"Старый ключ отключён на сервере и больше не будет работать ни в одном приложении. "
        f"Обновите ключ в Happ на новый:\n\n"
        f"<code>{new_key}</code>",
        reply_markup=vless_key_keyboard()
    )

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

                # Реферальный бонус — начисляется один раз, в момент ПЕРВОЙ реальной
                # оплаты приглашённого (а не за сам факт перехода по ссылке — так
                # бонус не накрутить фейковыми регистрациями без покупки).
                if not user[7]:  # first_payment ещё не было
                    await asyncio.to_thread(db.conn.execute, "UPDATE users SET first_payment=1 WHERE id=?", (user_id,))
                    await asyncio.to_thread(db.conn.commit)

                    inviter_id = user[6]  # invited_by
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
        "Введите `@username` или `ID` пользователя:\n"
        "Если пользователь админ — статус заберётся, если не админ — выдастся.",
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
    caption = f"🎟 <b>Тикет #{ticket[0]}</b>\nПользователь ID: <code>{ticket[1]}</code>\n\nСообщение:\n{ticket[2] or '—'}"
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
        "Будут удалены все промокоды, которые уже <b>полностью использованы</b> "
        "(использований = максимум).\n"
        "Промокоды, у которых остались свободные активации, не тронутся.\n\n"
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
        "📢 Отправьте сообщение для рассылки (текст, фото или файл с подписью):",
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
            # copy_to пересылает любой тип сообщения (текст, фото, файл) как есть
            await message.copy_to(chat_id=u[0])
            count += 1
            await asyncio.sleep(0.05)
        except:
            pass
    await state.clear()
    await message.answer(f"✅ Рассылка завершена! Доставлено {count} пользователям.", reply_markup=admin_back_keyboard())

############################################################
# SUBSCRIPTION & EXPIRATION CHECKER TASK
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
                # Если время вышло, а статус всё ещё Активно — отключаем
                if expire < now and status == "Активно":
                    await asyncio.to_thread(db.disable_subscription, user_id)
                    try:
                        # Отключаем сам ключ на панели 3x-ui, иначе Happ/v2rayTun
                        # продолжат работать даже после истечения подписки в боте
                        await vpn_client.disable_user(user_id)
                    except Exception as e:
                        logging.error(f"Не удалось отключить пользователя {user_id} в VPN панели: {e}")
                    try:
                        await bot.send_message(user_id, "❌ Ваша подписка на VPN истекла. Ключ отключен, продлите подписку для возобновления доступа.")
                    except:
                        pass
                
                # Уведомление за 3 дня
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

LOGS_RETENTION_DAYS = 7       # admin_logs и notifications старше — удаляются
CLOSED_TICKETS_RETENTION_DAYS = 30  # закрытые тикеты старше — удаляются (открытые не трогаем никогда)
CLEANUP_INTERVAL_SECONDS = 24 * 3600  # проверка раз в сутки

async def db_cleanup_task():
    """Автоочистка накопительных таблиц, чтобы БД (Neon) не забивалась
    бесконечно растущими логами. Удаляет только то, что безопасно удалить:
    - admin_logs: чистый журнал действий админов, старше 30 дней.
    - notifications: старые отметки об отправленных уведомлениях, старше 30 дней.
    - tickets: только УЖЕ ЗАКРЫТЫЕ обращения старше 60 дней — открытые тикеты
      не удаляются никогда, вне зависимости от возраста.
    users, promo_codes, used_promos, referrals — не трогаются вообще."""
    while True:
        try:
            log_cutoff = (datetime.now() - timedelta(days=LOGS_RETENTION_DAYS)).strftime("%Y-%m-%d %H:%M:%S")
            ticket_cutoff = (datetime.now() - timedelta(days=CLOSED_TICKETS_RETENTION_DAYS)).strftime("%Y-%m-%d %H:%M:%S")

            cur1 = await asyncio.to_thread(db.conn.execute, "DELETE FROM admin_logs WHERE created_at < ?", (log_cutoff,))
            deleted_logs = cur1.rowcount

            cur2 = await asyncio.to_thread(db.conn.execute, "DELETE FROM notifications WHERE date < ?", (log_cutoff[:10],))
            deleted_notifs = cur2.rowcount

            cur3 = await asyncio.to_thread(
                db.conn.execute,
                "DELETE FROM tickets WHERE status='Закрыт' AND closed_at != '' AND closed_at < ?",
                (ticket_cutoff,)
            )
            deleted_tickets = cur3.rowcount

            if deleted_logs or deleted_notifs or deleted_tickets:
                logging.info(
                    f"🧹 Автоочистка БД: admin_logs -{deleted_logs}, "
                    f"notifications -{deleted_notifs}, закрытых тикетов -{deleted_tickets}"
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

    # Независимые задачи запуска — параллельно, а не одна за другой
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
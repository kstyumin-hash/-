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
import hmac
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

PANEL_ATTEMPTS = 3   # попыток на один запрос к панели 3x-ui при временных сбоях

class PanelError(Exception):
    """Панель 3x-ui недоступна или ответила непонятно. Это НЕ то же самое, что
    «клиента нет»: путать их нельзя, иначе бот попытается создать дубликат."""

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

    Устойчивость:
    - сессия панели протухает; новые версии 3x-ui на просроченную сессию отвечают
      404 (а не 401), старые — 401 или HTML-страницей логина. Все эти случаи ловятся:
      бот перелогинивается и повторяет запрос (раньше он «залипал» в состоянии
      «авторизован» до перезапуска, и все операции с панелью молча падали);
    - временные сбои (обрыв, таймаут, 5xx) повторяются;
    - записи в inbound сериализуются: панель перезаписывает settings inbound'а
      целиком, и параллельные addClient/updateClient могли затирать друг друга;
    - «не удалось спросить панель» отличается от «клиента нет» (PanelError).
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
        self._login_gen = 0        # растёт после каждого успешного логина
        self._login_lock = None    # локи создаются лениво — уже внутри event loop
        self._write_lock = None

    def _init_locks(self):
        if self._login_lock is None:
            self._login_lock = asyncio.Lock()
            self._write_lock = asyncio.Lock()

    def _get_session(self):
        # Один переиспользуемый session — он же хранит cookie сессии 3x-ui между
        # запросами. unsafe=True: иначе aiohttp не сохраняет cookie, если панель
        # открыта по IP-адресу, и каждый запрос шёл бы «неавторизованным».
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=8, connect=4),
                cookie_jar=aiohttp.CookieJar(unsafe=True),
                connector=aiohttp.TCPConnector(ttl_dns_cache=300, limit=10)
            )
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    async def _do_login(self):
        session = self._get_session()
        try:
            async with session.post(
                f"{self.base_url}/login",
                data={"username": self.username, "password": self.password}
            ) as resp:
                try:
                    data = await resp.json(content_type=None)
                except Exception:
                    data = {}
                self._logged_in = (resp.status == 200 and isinstance(data, dict)
                                   and bool(data.get("success", False)))
                if self._logged_in:
                    self._login_gen += 1
                else:
                    logging.error(f"3x-ui: не удалось авторизоваться (status={resp.status})")
        except Exception as e:
            logging.error(f"Ошибка авторизации в 3x-ui: {e}")
            self._logged_in = False

    async def _ensure_login(self, seen_gen):
        """Логинится, если никто другой не успел сделать это после seen_gen —
        при одновременном «протухании» сессии логин будет один, а не по числу запросов."""
        self._init_locks()
        async with self._login_lock:
            if self._logged_in and self._login_gen != seen_gen:
                return True
            await self._do_login()
            return self._logged_in

    async def login(self):
        return await self._ensure_login(self._login_gen)

    async def _request(self, method, path, **kwargs):
        """Возвращает (status, data), data — разобранный JSON или None.
        Перелогинивается при отклонённой сессии, повторяет запрос при временных сбоях.
        Бросает PanelError, если панель так и не ответила."""
        url = f"{self.base_url}{path}"
        relogged = False
        last_err = "нет ответа"
        for attempt in range(1, PANEL_ATTEMPTS + 1):
            gen = self._login_gen
            if not self._logged_in and not await self._ensure_login(gen):
                last_err = "не удалось авторизоваться"
            else:
                try:
                    async with self._get_session().request(method, url, **kwargs) as resp:
                        status = resp.status
                        try:
                            data = await resp.json(content_type=None)
                        except Exception:
                            data = None
                    if status >= 500 or status == 429:
                        last_err = f"HTTP {status}"
                    elif status in (401, 403, 404) or (status == 200 and data is None):
                        # Сессия отклонена (401/404) или вместо JSON пришла страница логина.
                        if relogged:
                            return status, data
                        relogged = True
                        last_err = f"сессия отклонена (HTTP {status})"
                        await self._ensure_login(gen)
                        continue
                    else:
                        return status, data
                except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                    last_err = repr(e)
            logging.warning(f"3x-ui {method} {path}: {last_err} (попытка {attempt}/{PANEL_ATTEMPTS})")
            if attempt < PANEL_ATTEMPTS:
                await asyncio.sleep(0.5 * attempt)
        raise PanelError(f"{method} {path}: {last_err}")

    async def _api_post(self, path, payload=None):
        try:
            if payload is None:
                status, data = await self._request("POST", path)
            else:
                status, data = await self._request("POST", path, json=payload)
        except PanelError as e:
            logging.error(f"Ошибка запроса к 3x-ui: {e}")
            return False
        ok = status == 200 and isinstance(data, dict) and bool(data.get("success", False))
        if not ok:
            logging.error(f"3x-ui {path}: отказ (HTTP {status}): {data}")
        return ok

    async def _find_existing_client(self, email):
        """Возвращает dict существующего клиента по email или None, если клиента нет.
        Бросает PanelError, если панель не ответила / inbound не прочитался."""
        status, data = await self._request("GET", f"/panel/api/inbounds/get/{self.inbound_id}")
        if status != 200 or not isinstance(data, dict) or not data.get("success") or not data.get("obj"):
            raise PanelError(f"не удалось прочитать inbound {self.inbound_id} (HTTP {status})")
        try:
            settings = json.loads(data["obj"].get("settings") or "{}")
        except Exception as e:
            raise PanelError(f"settings inbound {self.inbound_id} не разобрались: {e}")
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

    def _client_payload(self, base, client_uuid, email, expiry_ms, sub_id):
        # Для существующего клиента берём ВСЕ его поля (comment, security и т.д.) и меняем
        # только нужные — иначе updateClient затирал бы всё, чего нет в нашем словаре.
        payload = dict(base or {})
        payload.update({
            "id": client_uuid,
            "email": email,
            "enable": True,
            "expiryTime": expiry_ms,
            "limitIp": self.device_limit,
            "subId": sub_id,
        })
        for key, default in (("totalGB", 0), ("tgId", ""), ("reset", 0), ("flow", "")):
            payload.setdefault(key, default)
        return payload

    async def _create_or_update_locked(self, user_id, expire_timestamp):
        # Вызывать только под self._write_lock.
        email = f"user_{user_id}"
        expiry_ms = int(expire_timestamp) * 1000  # 3x-ui ждёт миллисекунды, не секунды

        try:
            existing = await self._find_existing_client(email)
        except PanelError as e:
            logging.error(f"3x-ui: не удалось проверить клиента {email}: {e}")
            return ""

        if not existing:
            client_uuid = str(uuid.uuid4())
            sub_id = uuid.uuid4().hex
            payload = self._client_payload(None, client_uuid, email, expiry_ms, sub_id)
            if await self._api_post(
                "/panel/api/inbounds/addClient",
                {"id": self.inbound_id, "settings": json.dumps({"clients": [payload]})}
            ):
                return self._build_sub_link(sub_id)
            # Ответ мог потеряться, хотя клиент уже создан, — проверяем, а не плодим дубль.
            try:
                existing = await self._find_existing_client(email)
            except PanelError:
                existing = None
            if not existing:
                return ""

        client_uuid = existing.get("id")
        sub_id = existing.get("subId") or uuid.uuid4().hex
        payload = self._client_payload(existing, client_uuid, email, expiry_ms, sub_id)
        ok = await self._api_post(
            f"/panel/api/inbounds/updateClient/{client_uuid}",
            {"id": self.inbound_id, "settings": json.dumps({"clients": [payload]})}
        )
        return self._build_sub_link(sub_id) if ok else ""

    async def create_or_update_user(self, user_id, expire_timestamp):
        self._init_locks()
        async with self._write_lock:
            return await self._create_or_update_locked(user_id, expire_timestamp)

    async def disable_user(self, user_id):
        # Реально отключает доступ на стороне 3x-ui (а не только в БД бота) —
        # без этого Happ/v2rayTun продолжали бы работать с ключом после истечения дней.
        self._init_locks()
        email = f"user_{user_id}"
        async with self._write_lock:
            try:
                existing = await self._find_existing_client(email)
            except PanelError as e:
                logging.error(f"3x-ui: не удалось проверить клиента {email}: {e}")
                return False
            if not existing:
                return False
            payload = dict(existing)
            payload["enable"] = False
            return await self._api_post(
                f"/panel/api/inbounds/updateClient/{existing['id']}",
                {"id": self.inbound_id, "settings": json.dumps({"clients": [payload]})}
            )

    async def delete_client(self, client_uuid):
        # Полное удаление клиента на панели 3x-ui — именно это делает старый
        # ключ нерабочим (в отличие от updateClient, id/subId остаются старыми).
        self._init_locks()
        async with self._write_lock:
            return await self._api_post(f"/panel/api/inbounds/{self.inbound_id}/delClient/{client_uuid}")

    async def reset_user_key(self, user_id, expire_timestamp):
        """Полностью пересоздаёт ключ пользователя: старый клиент удаляется
        на панели 3x-ui (перестаёт работать сразу и необратимо), взамен
        создаётся новый клиент с новым id и новым subId — то есть выдаётся
        совсем другой ключ, а не обновление старого."""
        self._init_locks()
        email = f"user_{user_id}"
        async with self._write_lock:
            try:
                existing = await self._find_existing_client(email)
            except PanelError as e:
                logging.error(f"3x-ui: не удалось проверить клиента {email}: {e}")
                return ""
            if existing and existing.get("id"):
                deleted = await self._api_post(
                    f"/panel/api/inbounds/{self.inbound_id}/delClient/{existing['id']}"
                )
                if not deleted:
                    # Ответ на удаление мог не разобраться — смотрим, удалился ли клиент на деле.
                    try:
                        still_there = await self._find_existing_client(email)
                    except PanelError:
                        still_there = existing
                    if still_there:
                        # Старый клиент жив: «новый» ключ оказался бы тем же самым.
                        logging.error(f"3x-ui: не удалось удалить старого клиента {email}, сброс ключа отменён")
                        return ""
            # После удаления создастся новый клиент — с новым uuid и subId.
            return await self._create_or_update_locked(user_id, expire_timestamp)

vpn_client = VPNClient()

############################################################
# PLATEGA.IO API CLIENT (СБП по QR, карты РФ, карточный эквайринг, международная)
############################################################

# Способы оплаты Platega.io: только СБП (QR) и банковская карта.
# Крипта, ЕРИП, SberPay, международная оплата и т.п. отключены.
PLATEGA_METHODS = {
    2:  {"name": "СБП (по QR-коду)",       "emoji": "🏦"},
    11: {"name": "Банковская карта",       "emoji": "💳"},
}

PLATEGA_CREATE_TIMEOUT = 8      # сек на одну попытку создания платежа
PLATEGA_CREATE_ATTEMPTS = 3     # сколько всего попыток при временных сбоях

class PlategaClient:
    """Клиент для Platega.io (https://docs.platega.io/) — приём оплаты картой,
    СБП по QR-коду и международными картами.

    Авторизация — заголовки X-MerchantId / X-Secret на каждый запрос (без
    отдельного логина, в отличие от 3x-ui)."""

    def __init__(self):
        self.base_url = os.environ.get("PLATEGA_BASE_URL", "https://app.platega.io").rstrip("/")
        self.merchant_id = os.environ.get("PLATEGA_MERCHANT_ID", "")
        self.secret = os.environ.get("PLATEGA_SECRET", "")
        self._session = None

    def _get_session(self):
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                headers={
                    "X-MerchantId": self.merchant_id,
                    "X-Secret": self.secret,
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                },
                timeout=aiohttp.ClientTimeout(total=20),
                connector=aiohttp.TCPConnector(ttl_dns_cache=300, limit=20)
            )
        return self._session

    def is_configured(self):
        return bool(self.merchant_id and self.secret)

    async def create_transaction(self, payment_method: int, amount: float, currency: str,
                                  description: str = "", payload: str = "", return_url: str = "",
                                  failed_url: str = ""):
        """Создаёт транзакцию и возвращает redirect-ссылку на оплату.
        ID транзакции по документации генерируется системой — id в запросе не передаём.
        Тело запроса строго по схеме CreateTransactionRequest (additionalProperties: false) —
        лишних полей (например metadata) быть не должно, иначе Platega вернёт 400.

        Используем /v2/transaction/process, а не старый /transaction/process:
        по независимым сообщениям других разработчиков, у части мерчантов на
        v1 нет каскадов для карточных платежей (paymentMethod=11), из-за чего
        приходит "No available card cascades". СБП на v1 обычно ещё работает."""
        if not self.is_configured():
            return None
        session = self._get_session()
        body = {
            "paymentMethod": payment_method,
            "paymentDetails": {"amount": float(amount), "currency": currency},
        }
        if description:
            body["description"] = description
        if payload:
            body["payload"] = payload
        if return_url:
            body["return"] = return_url
        if failed_url:
            body["failedUrl"] = failed_url
        # Короткий таймаут на попытку + повторы при временных сбоях (обрыв соединения,
        # таймаут, 5xx, 429) вместо одного долгого ожидания на 20 секунд. Повтор безопасен:
        # неоплаченная «лишняя» транзакция в Platega никому не начисляется — бот начисляет
        # только по той, чей id сохранил в своей БД. Ошибки 4xx (неверный запрос) не повторяем.
        url = f"{self.base_url}/v2/transaction/process"
        timeout = aiohttp.ClientTimeout(total=PLATEGA_CREATE_TIMEOUT, connect=5)
        for attempt in range(1, PLATEGA_CREATE_ATTEMPTS + 1):
            started = time.monotonic()
            try:
                async with session.post(url, json=body, timeout=timeout) as resp:
                    try:
                        data = await resp.json(content_type=None)
                    except Exception:
                        data = None
                    elapsed = time.monotonic() - started
                    if resp.status == 200 and isinstance(data, dict):
                        logging.info(f"Platega create_transaction OK за {elapsed:.1f}с (попытка {attempt})")
                        return data
                    logging.error(f"Platega create_transaction ошибка {resp.status} за {elapsed:.1f}с (попытка {attempt}): {data}")
                    # 200 с неразборчивым телом, 5xx, 408, 429 — временные, остальное — нет
                    if not (resp.status == 200 or resp.status >= 500 or resp.status in (408, 429)):
                        return None
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                logging.warning(f"Platega create_transaction сбой соединения за {time.monotonic() - started:.1f}с (попытка {attempt}): {e!r}")
            except Exception as e:
                logging.error(f"Platega create_transaction исключение: {e!r}")
                return None
            if attempt < PLATEGA_CREATE_ATTEMPTS:
                await asyncio.sleep(0.5 * attempt)
        return None

    async def get_transaction_status(self, transaction_id: str):
        if not self.is_configured():
            return None
        session = self._get_session()
        try:
            async with session.get(f"{self.base_url}/transaction/{transaction_id}") as resp:
                data = await resp.json(content_type=None)
                if resp.status != 200:
                    logging.error(f"Platega get_transaction_status ошибка {resp.status}: {data}")
                    return None
                return data
        except Exception as e:
            logging.error(f"Platega get_transaction_status исключение: {e}")
            return None

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

platega_client = PlategaClient()

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
        self.conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS last_key_reset TEXT DEFAULT ''")
        
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

        # Транзакции Platega.io (оплата картой/СБП по QR/международная) —
        # нужна отдельная таблица, чтобы по callback'у или ручной проверке
        # статуса знать, кому и какой тариф начислять, и не начислить дважды.
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS platega_transactions(
            transaction_id TEXT PRIMARY KEY,
            user_id BIGINT,
            tariff_id TEXT,
            payment_method INTEGER,
            amount NUMERIC,
            status TEXT DEFAULT 'PENDING',
            created_at TEXT
        )
        """)

        # Защита от двойного начисления за одну и ту же оплату Stars
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS stars_payments(
            charge_id TEXT PRIMARY KEY,
            user_id BIGINT,
            created_at TEXT
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

    def create_platega_transaction(self, transaction_id, user_id, tariff_id, payment_method, amount):
        self.conn.execute("""
            INSERT INTO platega_transactions (transaction_id, user_id, tariff_id, payment_method, amount, status, created_at)
            VALUES (?, ?, ?, ?, ?, 'PENDING', ?)
            ON CONFLICT (transaction_id) DO NOTHING
        """, (transaction_id, user_id, tariff_id, payment_method, amount, datetime.now().isoformat()))
        self.conn.commit()

    def claim_platega_transaction(self, transaction_id, from_status, to_status):
        """Атомарно переводит транзакцию из from_status в to_status. Возвращает
        строку (user_id, tariff_id) только тому, кто реально выполнил переход —
        параллельные/повторные callback'и получают None и ничего не начисляют."""
        cur = self.conn.execute(
            "UPDATE platega_transactions SET status=? WHERE transaction_id=? AND status=? "
            "RETURNING user_id, tariff_id",
            (to_status, transaction_id, from_status)
        )
        return cur.fetchone()

    def get_pending_platega_ids(self, since_iso):
        """ID транзакций Platega в статусе PENDING, созданных не раньше since_iso."""
        cur = self.conn.execute(
            "SELECT transaction_id FROM platega_transactions WHERE status='PENDING' AND created_at >= ?",
            (since_iso,)
        )
        return [r[0] for r in cur.fetchall()]

    def get_platega_transaction(self, transaction_id):
        cursor = self.conn.execute("SELECT * FROM platega_transactions WHERE transaction_id=?", (transaction_id,))
        return cursor.fetchone()

    def set_platega_transaction_status(self, transaction_id, status):
        self.conn.execute("UPDATE platega_transactions SET status=? WHERE transaction_id=?", (status, transaction_id))
        self.conn.commit()

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

def btn(text, callback_data=None, url=None, style=None):
    """Кнопка с цветом (Bot API 9.4: style = success зелёная / primary синяя /
    danger красная). На старых версиях aiogram/клиентов поле просто игнорируется."""
    kwargs = {"text": text}
    if callback_data is not None:
        kwargs["callback_data"] = callback_data
    if url is not None:
        kwargs["url"] = url
    if style:
        kwargs["style"] = style
    return InlineKeyboardButton(**kwargs)

def back_btn(callback_data, text="⬅ Назад"):
    return btn(text, callback_data=callback_data, style="danger")

def profile_keyboard(is_admin=False):
    buttons = [
        [btn("💳 Оплата VPN", callback_data="payment", style="success")],
        [btn("📱 Добавить устройство", callback_data="get_vless_key", style="primary")],
        [btn("🎁 Пригласить друга", callback_data="my_ref", style="primary")],
        [btn("🎟 Промокод", callback_data="promo", style="primary")]
    ]
    if is_admin:
        buttons.append([btn("🛠 Админ-панель", callback_data="admin", style="primary")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def payment_method_keyboard():
    buttons = [[InlineKeyboardButton(text="⭐️ Telegram Stars", callback_data="pay_type_stars")]]
    for method_id, info in PLATEGA_METHODS.items():
        buttons.append([InlineKeyboardButton(text=f"{info['emoji']} {info['name']}", callback_data=f"platega_method_{method_id}")])
    buttons.append([back_btn("profile")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def stars_payment_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"🗓 Месяц — {TARIFFS['month']['stars']} ⭐️", callback_data="stars_month")],
        [InlineKeyboardButton(text=f"📅 Полгода — {TARIFFS['half']['stars']} ⭐️", callback_data="stars_half")],
        [InlineKeyboardButton(text=f"📆 Год — {TARIFFS['year']['stars']} ⭐️", callback_data="stars_year")],
        [InlineKeyboardButton(text="⬅ Назад", callback_data="payment", style="danger")]
    ])

def platega_tariff_keyboard(method_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"🗓 Месяц — {TARIFFS['month']['price']}₽", callback_data=f"platega_tariff_month_{method_id}")],
        [InlineKeyboardButton(text=f"📅 Полгода — {TARIFFS['half']['price']}₽", callback_data=f"platega_tariff_half_{method_id}")],
        [InlineKeyboardButton(text=f"📆 Год — {TARIFFS['year']['price']}₽", callback_data=f"platega_tariff_year_{method_id}")],
        [back_btn("payment")]
    ])

PAY_FAILED_TEXT = (
    "❌ <b>Платёж отменён или не оплачен</b>\n\n"
    "Деньги не списаны, подписка не изменилась. Вы можете попробовать ещё раз."
)

def pay_failed_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [btn("💳 Посмотреть тарифы", callback_data="payment", style="success")]
    ])

def back_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅ Назад", callback_data="profile", style="danger")]
    ])

def vless_key_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Сбросить ключ", callback_data="reset_vless_key")],
        [btn("⬅ Назад", callback_data="profile", style="success")]
    ])

def admin_back_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅ Назад в админ-панель", callback_data="admin", style="danger")]
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
        [InlineKeyboardButton(text="⬅ Главное меню", callback_data="profile", style="danger")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def ticket_list_keyboard(tickets):
    buttons = []
    for ticket in tickets:
        preview = ticket[2][:20] if ticket[2] else ""
        if not preview:
            file_type = ticket[6] if len(ticket) > 6 else ""
            preview = "📷 Фото" if file_type == "photo" else ("📎 Файл" if file_type == "document" else "…")
        buttons.append([InlineKeyboardButton(text=f"🎟 #{ticket[0]} | {preview}", callback_data=f"ticket_{ticket[0]}")])
    buttons.append([InlineKeyboardButton(text="⬅ Назад в админ-панель", callback_data="admin", style="danger")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def promo_admin_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Создать промокод", callback_data="promo_create")],
        [InlineKeyboardButton(text="📋 Список промокодов", callback_data="promo_list")],
        [InlineKeyboardButton(text="🗑 Очистить использованные", callback_data="promo_clear_confirm")],
        [InlineKeyboardButton(text="⬅ Назад в админ-панель", callback_data="admin", style="danger")]
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
        [InlineKeyboardButton(text="⬅ Назад", callback_data="welcome_back", style="danger")]
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

KEY_RESET_COOLDOWN_HOURS = 24   # как часто можно перевыпускать ключ

@dp.callback_query(F.data == "reset_vless_key")
async def reset_vless_key(callback: CallbackQuery):
    user_id = callback.from_user.id
    user = await asyncio.to_thread(db.get_user, user_id)
    days = calculate_days(user[3])

    if days <= 0:
        await callback.answer("❌ Подписка неактивна. Оплатите дни, чтобы получить ключ для Happ.", show_alert=True)
        return

    new_key = ""
    # Под локом пользователя: проверка лимита и сброс — одним блоком, чтобы двойной клик
    # не позволил перевыпустить ключ дважды.
    async with user_locks[user_id]:
        row = (await asyncio.to_thread(
            db.conn.execute, "SELECT last_key_reset FROM users WHERE id=?", (user_id,)
        )).fetchone()
        last_reset = None
        if row and row[0]:
            try:
                last_reset = datetime.fromisoformat(row[0])
            except Exception:
                last_reset = None
        if last_reset:
            left = last_reset + timedelta(hours=KEY_RESET_COOLDOWN_HOURS) - datetime.now()
            if left.total_seconds() > 0:
                total_min = max(1, -(-int(left.total_seconds()) // 60))  # округление вверх до минуты
                h, m = divmod(total_min, 60)
                left_text = f"{h} ч {m} мин" if h else f"{m} мин"
                await callback.answer(
                    f"⏳ Ключ можно перевыпускать раз в {KEY_RESET_COOLDOWN_HOURS} часа. "
                    f"Следующий раз — через {left_text}.",
                    show_alert=True
                )
                return

        await callback.answer("🔄 Обновляем ключ…")

        try:
            expire_dt = datetime.strptime(user[3], "%Y-%m-%d %H:%M:%S")
            timestamp = int(expire_dt.timestamp())
            new_key = await vpn_client.reset_user_key(user_id, timestamp)
            if new_key:
                # Время сброса пишем только при успехе — неудачная попытка лимит не тратит
                await asyncio.to_thread(
                    db.conn.execute,
                    "UPDATE users SET vless_key=?, last_key_reset=? WHERE id=?",
                    (new_key, datetime.now().isoformat(), user_id)
                )
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

@dp.callback_query(F.data == "pay_type_stars")
async def payment_stars_menu(callback: CallbackQuery):
    await callback.message.edit_text(
        "⭐️ <b>Выберите тарифный план (Оплата Telegram Stars):</b>\n\n"
        "Оплата произойдет мгновенно прямо в Telegram!",
        reply_markup=stars_payment_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("platega_method_"))
async def platega_method_select(callback: CallbackQuery):
    try:
        method_id = int(callback.data.split("_")[2])
    except (IndexError, ValueError):
        await callback.answer("Способ оплаты недоступен", show_alert=True)
        return
    method = PLATEGA_METHODS.get(method_id)
    if not method:
        await callback.answer("Способ оплаты недоступен", show_alert=True)
        return
    if not platega_client.is_configured():
        await callback.answer("Эта оплата временно недоступна, попробуйте позже или оплатите звёздами.", show_alert=True)
        return
    await callback.message.edit_text(
        f"{method['emoji']} <b>{method['name']}</b>\n\n"
        f"Выберите тарифный план. Дни добавятся к вашей текущей подписке.",
        reply_markup=platega_tariff_keyboard(method_id)
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("platega_tariff_"))
async def process_platega_pay(callback: CallbackQuery):
    # формат: platega_tariff_{tariff_id}_{method_id}
    parts = callback.data.split("_")
    try:
        tariff_id, method_id = parts[2], int(parts[3])
    except (IndexError, ValueError):
        await callback.answer("Ошибка выбора тарифа", show_alert=True)
        return
    tariff = TARIFFS.get(tariff_id)
    method = PLATEGA_METHODS.get(method_id)
    if not tariff or not method:
        await callback.answer("Ошибка выбора тарифа", show_alert=True)
        return

    if not platega_client.is_configured():
        await callback.answer("Эта оплата временно недоступна, попробуйте позже или оплатите звёздами.", show_alert=True)
        return

    # Защита от спама кнопкой: каждый клик создаёт реальную транзакцию в Platega
    if is_rate_limited(callback.from_user.id, "platega_create", cooldown=3):
        await callback.answer("⏳ Секунду, ссылка уже создаётся…")
        return

    # Обратная связь уходит ПАРАЛЛЕЛЬНО с запросом в Platega (а не перед ним), а кнопки
    # тарифов на время создания убираются — повторные нажатия не плодят лишние платежи.
    async def _show_progress():
        try:
            await callback.answer("Создаём ссылку на оплату…")
            await callback.message.edit_text("⏳ <b>Создаём ссылку на оплату…</b>")
        except Exception:
            pass
    progress_task = asyncio.create_task(_show_progress())

    user = callback.from_user
    bot_username = BOT_USERNAME or (await bot.get_me()).username
    return_url = f"https://t.me/{bot_username}?start=pay_success"
    failed_url = f"https://t.me/{bot_username}?start=pay_failed"

    result = await platega_client.create_transaction(
        payment_method=method_id,
        amount=tariff["price"],
        currency="RUB",
        description=f"Подписка Stopka VPN ({tariff['name']})",
        payload=f"platega_{tariff_id}_{user.id}_{int(time.time())}",
        return_url=return_url,
        failed_url=failed_url
    )

    await progress_task  # чтобы «Создаём ссылку…» не перезаписало итоговое сообщение

    pay_url = (result.get("redirect") or result.get("url")) if result else None
    transaction_id = result.get("transactionId") if result else None

    if not result or not pay_url or not transaction_id:
        await callback.message.edit_text(
            "❌ Не удалось создать платёж. Попробуйте другой способ оплаты или напишите в поддержку.",
            reply_markup=payment_method_keyboard()
        )
        return

    # Транзакция уже создана в Platega — если не сохранить её в БД, оплата не начислится.
    # Поэтому при сбое БД (например, «проснулась» после простоя) пробуем ещё пару раз.
    saved = False
    for db_attempt in range(1, 4):
        try:
            await asyncio.to_thread(
                db.create_platega_transaction, transaction_id, user.id, tariff_id, method_id, tariff["price"]
            )
            saved = True
            break
        except Exception as e:
            logging.error(f"Не удалось сохранить транзакцию Platega {transaction_id} (попытка {db_attempt}/3): {e}")
            await asyncio.sleep(0.5 * db_attempt)
    if not saved:
        await callback.message.edit_text(
            "❌ Не удалось создать платёж. Попробуйте ещё раз через минуту.",
            reply_markup=payment_method_keyboard()
        )
        return

    expires_in = result.get("expiresIn", "00:15:00")
    pay_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💳 Перейти к оплате", url=pay_url)],
        [back_btn(f"platega_method_{method_id}")]
    ])
    await callback.message.edit_text(
        f"{method['emoji']} <b>{method['name']} — {tariff['name']}, {tariff['price']}₽</b>\n\n"
        f"Ссылка на оплату действительна {expires_in}. После оплаты подписка продлится автоматически — "
        f"ничего дополнительно присылать не нужно.",
        reply_markup=pay_kb
    )

def _end_of_day(dt):
    """Конец календарного дня — одно и то же время и в БД бота, и на панели 3x-ui,
    чтобы ключ не отключался раньше, чем показывает бот."""
    return dt.replace(hour=23, minute=59, second=59, microsecond=0)

_panel_resync = {}   # user_id -> expire_dt: срок, который не удалось записать на панель

async def _sync_panel(user_id: int, expire_dt) -> bool:
    """Обновляет срок ключа на панели. create_or_update_user при сбое возвращает
    пустую строку (а не исключение). Повторы при временных сбоях сети/панели делает
    сам клиент 3x-ui; если панель недоступна дольше — срок встаёт в очередь
    _panel_resync, и panel_resync_task дозапишет его автоматически."""
    try:
        res = await vpn_client.create_or_update_user(user_id, int(expire_dt.timestamp()))
        if res:
            _panel_resync.pop(user_id, None)
            return True
        logging.error(f"Панель 3x-ui не подтвердила продление для {user_id}")
    except Exception as e:
        logging.error(f"Ошибка синхронизации с панелью для {user_id}: {e}")
    _panel_resync[user_id] = expire_dt
    try:
        await bot.send_message(OWNER_ID, f"⚠️ Оплата прошла, но ключ {user_id} не обновился на панели 3x-ui (срок до {expire_dt:%Y-%m-%d}). Бот будет повторять попытку каждую минуту и напишет, когда получится.")
    except Exception:
        pass
    return False

async def panel_resync_task():
    """Дозаписывает на панель сроки, которые не удалось записать сразу (панель была
    недоступна). Работает под локом пользователя, поэтому не затрёт более новую оплату."""
    await asyncio.sleep(30)
    while True:
        try:
            for uid in list(_panel_resync.keys()):
                async with user_locks[uid]:
                    exp = _panel_resync.get(uid)  # перечитываем: за время ожидания лока срок мог обновиться
                    if exp is None:
                        continue
                    try:
                        res = await vpn_client.create_or_update_user(uid, int(exp.timestamp()))
                    except Exception as e:
                        logging.error(f"Повторная синхронизация {uid} с панелью не удалась: {e}")
                        res = ""
                    if res:
                        _panel_resync.pop(uid, None)
                if res:
                    logging.info(f"Панель 3x-ui: срок для {uid} дозаписан при повторной попытке")
                    try:
                        await bot.send_message(OWNER_ID, f"✅ Ключ {uid} успешно обновлён на панели 3x-ui при повторной попытке.")
                    except Exception:
                        pass
        except Exception as e:
            logging.error(f"Ошибка panel_resync_task: {e}")
        await asyncio.sleep(60)

async def _grant_referral_bonus(user_id: int, inviter_id: int):
    async with user_locks[inviter_id]:
        row = (await asyncio.to_thread(
            db.conn.execute,
            "UPDATE referrals SET bonus_given=1 WHERE user_id=? AND bonus_given=0 RETURNING user_id",
            (user_id,)
        )).fetchone()
        if not row:
            return
        inviter = await asyncio.to_thread(db.get_user, inviter_id)
        if not inviter:
            return
        try:
            inv_expire = datetime.strptime(inviter[3], "%Y-%m-%d %H:%M:%S")
        except Exception:
            inv_expire = datetime.now()
        if inv_expire < datetime.now():
            inv_expire = datetime.now()
        inv_new_expire = _end_of_day(inv_expire + timedelta(days=REFERRAL_DAYS))
        await asyncio.to_thread(
            db.conn.execute,
            "UPDATE users SET expire_date=?, status='Активно' WHERE id=?",
            (inv_new_expire.strftime("%Y-%m-%d %H:%M:%S"), inviter_id)
        )
        await _sync_panel(inviter_id, inv_new_expire)
    try:
        await bot.send_message(
            inviter_id,
            f"🎁 Ваш друг оформил подписку по вашей ссылке!\nВам начислено +{REFERRAL_DAYS} дней VPN."
        )
    except Exception:
        pass

async def activate_subscription(user_id: int, tariff_id: str):
    """Продлевает подписку на days тарифа, обновляет ключ на 3x-ui, начисляет
    реферальный бонус (один раз, при первой оплате приглашённого).
    Используется и для Stars, и для Platega. Под локом пользователя — две
    одновременные оплаты не затрут друг друга.
    Возвращает (days, new_expire_str) или None, если тариф/пользователь не найден."""
    tariff = TARIFFS.get(tariff_id)
    if not tariff:
        return None
    days = tariff["days"]

    async with user_locks[user_id]:
        user = await asyncio.to_thread(db.get_user, user_id)
        if not user:
            return None

        try:
            expire = datetime.strptime(user[3], "%Y-%m-%d %H:%M:%S")
        except Exception:
            expire = datetime.now()
        now = datetime.now()
        if expire < now:
            expire = now

        new_expire = _end_of_day(expire + timedelta(days=days))
        new_expire_str = new_expire.strftime("%Y-%m-%d %H:%M:%S")

        await asyncio.to_thread(
            db.conn.execute,
            "UPDATE users SET expire_date=?, status='Активно', last_tariff=? WHERE id=?",
            (new_expire_str, tariff['name'], user_id)
        )
        await _sync_panel(user_id, new_expire)

        first_row = (await asyncio.to_thread(
            db.conn.execute,
            "UPDATE users SET first_payment=1 WHERE id=? AND (first_payment=0 OR first_payment IS NULL) RETURNING invited_by",
            (user_id,)
        )).fetchone()
        inviter_id = first_row[0] if first_row else 0

    if inviter_id:
        try:
            await _grant_referral_bonus(user_id, inviter_id)
        except Exception as e:
            logging.error(f"Ошибка реферального бонуса ({user_id} -> {inviter_id}): {e}")

    return days, new_expire_str

@dp.callback_query(F.data.in_({"stars_month", "stars_half", "stars_year"}))
async def process_stars_pay(callback: CallbackQuery):
    tariff_id = callback.data.split("_")[1]
    tariff = TARIFFS.get(tariff_id)
    if not tariff:
        await callback.answer("Ошибка выбора тарифа", show_alert=True)
        return

    if is_rate_limited(callback.from_user.id, "stars_invoice", cooldown=3):
        await callback.answer("⏳ Счёт уже создаётся…")
        return

    if not await asyncio.to_thread(db.get_user, callback.from_user.id):
        await callback.answer("Сначала нажмите /start", show_alert=True)
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

def _parse_stars_payload(payload: str):
    """stars_{tariff}_{user_id}_{ts} -> (tariff_id, user_id) или None."""
    parts = (payload or "").split("_")
    if len(parts) != 4 or parts[0] != "stars":
        return None
    try:
        return parts[1], int(parts[2])
    except ValueError:
        return None

@dp.pre_checkout_query()
async def process_pre_checkout(pre_checkout_query: PreCheckoutQuery):
    parsed = _parse_stars_payload(pre_checkout_query.invoice_payload)
    ok = bool(parsed)
    if ok:
        tariff_id, payer_id = parsed
        tariff = TARIFFS.get(tariff_id)
        ok = (
            tariff is not None
            and payer_id == pre_checkout_query.from_user.id
            and pre_checkout_query.currency == "XTR"
            and pre_checkout_query.total_amount == tariff["stars"]
        )
    if ok:
        await pre_checkout_query.answer(ok=True)
    else:
        await pre_checkout_query.answer(ok=False, error_message="Счёт недействителен, создайте новый в боте.")

@dp.message(F.successful_payment)
async def process_successful_payment(message: Message):
    sp = message.successful_payment
    parsed = _parse_stars_payload(sp.invoice_payload)
    if not parsed:
        return
    tariff_id, payer_id = parsed
    user_id = message.from_user.id
    tariff = TARIFFS.get(tariff_id)
    if payer_id != user_id or not tariff or sp.currency != "XTR" or sp.total_amount != tariff["stars"]:
        logging.error(f"Stars: подозрительный платёж {sp.invoice_payload} от {user_id}: {sp.total_amount} {sp.currency}")
        return

    # Идемпотентность: один и тот же платёж никогда не начислится дважды
    claimed = (await asyncio.to_thread(
        db.conn.execute,
        "INSERT INTO stars_payments (charge_id, user_id, created_at) VALUES (?,?,?) "
        "ON CONFLICT (charge_id) DO NOTHING RETURNING charge_id",
        (sp.telegram_payment_charge_id, user_id, datetime.now().isoformat())
    )).fetchone()
    if not claimed:
        return

    try:
        result = await activate_subscription(user_id, tariff_id)
    except Exception as e:
        logging.error(f"Stars: ошибка начисления {user_id}/{tariff_id}: {e}")
        result = None

    if not result:
        # Начисление не удалось — снимаем отметку и сообщаем админу, деньги не теряем молча
        await asyncio.to_thread(db.conn.execute, "DELETE FROM stars_payments WHERE charge_id=?", (sp.telegram_payment_charge_id,))
        try:
            await bot.send_message(OWNER_ID, f"⚠️ Stars-оплата не начислена: user {user_id}, тариф {tariff_id}, charge {sp.telegram_payment_charge_id}")
        except Exception:
            pass
        await message.answer(
            "⚠️ Оплата получена, но подписка не активировалась автоматически. Напишите в поддержку — всё исправим.",
            reply_markup=support_keyboard()
        )
        return

    days, new_expire_str = result
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
        await vpn_client.create_or_update_user(user_id, int(new_expire.replace(hour=23, minute=59, second=59).timestamp()))
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
        await vpn_client.create_or_update_user(user[0], int(expire.replace(hour=23, minute=59, second=59).timestamp()))
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
        [InlineKeyboardButton(text="⬅ Назад в админ-панель", callback_data="admin", style="danger")]
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

LOGS_RETENTION_DAYS = 7              # admin_logs и notifications старше — удаляются
CLOSED_TICKETS_RETENTION_DAYS = 30   # закрытые тикеты (текст, ответ, file_id) старше — удаляются; открытые не трогаем
PLATEGA_POLL_INTERVAL = 15           # как часто бот спрашивает Platega о статусе, сек
PLATEGA_POLL_WINDOW_MIN = 360        # проверяем неоплаченные транзакции за последние N минут (6 ч — с запасом на простой бота)

async def platega_status_checker():
    """Сам опрашивает Platega по неоплаченным транзакциям, независимо от callback'а:
    - CONFIRMED — начисляет подписку (если callback не дошёл или начисление упало);
    - CANCELED — автоматически отправляет пользователю сообщение с кнопкой «Посмотреть тарифы».
    Переходы статусов атомарные, поэтому вместе с callback'ом начисление и сообщение
    выполнятся ровно один раз."""
    await asyncio.sleep(10)
    while True:
        try:
            if platega_client.is_configured():
                since = (datetime.now() - timedelta(minutes=PLATEGA_POLL_WINDOW_MIN)).isoformat()
                tx_ids = await asyncio.to_thread(db.get_pending_platega_ids, since)
                for tx_id in tx_ids:
                    data = await platega_client.get_transaction_status(tx_id)
                    status = str(data.get("status", "")).upper() if isinstance(data, dict) else ""
                    if status == "CONFIRMED":
                        # Оплата прошла, а callback не дошёл (или начисление упало) — начисляем сами.
                        _spawn(finalize_platega_confirmed(tx_id))
                    elif status == "CANCELED":
                        claimed = await asyncio.to_thread(db.claim_platega_transaction, tx_id, "PENDING", "CANCELED")
                        if claimed:
                            try:
                                await bot.send_message(claimed[0], PAY_FAILED_TEXT, reply_markup=pay_failed_keyboard())
                            except Exception as e:
                                logging.warning(f"Platega: не удалось отправить сообщение об отмене {claimed[0]}: {e}")
                    await asyncio.sleep(0.3)
        except Exception as e:
            logging.error(f"Ошибка platega_status_checker: {e}")
        await asyncio.sleep(PLATEGA_POLL_INTERVAL)

PLATEGA_DEAD_RETENTION_DAYS = 7      # отменённые/зависшие транзакции Platega
PLATEGA_DONE_RETENTION_DAYS = 180    # оплаченные транзакции — храним для учёта полгода
STARS_RETENTION_DAYS = 180
CLEANUP_INTERVAL_SECONDS = 24 * 3600  # проверка раз в сутки

def cleanup_memory_state():
    """Чистит словари в памяти, которые иначе растут бесконечно."""
    now_m = time.monotonic()
    for key in [k for k, t in _last_action_time.items() if now_m - t > 3600]:
        _last_action_time.pop(key, None)
    now_t = time.time()
    for uid in [u for u, ts in rate_limiter.requests.items() if not ts or now_t - max(ts) > rate_limiter.window]:
        rate_limiter.requests.pop(uid, None)
    for uid in [u for u, lock in user_locks.items() if not lock.locked()]:
        user_locks.pop(uid, None)

asyn
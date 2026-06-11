"""
core.py — VA UP! Shared Core

Содержит всю бизнес-логику, DB-хелперы, FSA-интеграцию и форматтеры.
Импортируется и из app.py, и из worker.py.

ВАЖНО: этот файл НЕ содержит никакого кода на уровне модуля кроме
константных определений и определений функций — никаких side effects
(нет _create_pool(), нет tg_setup_webhook(), нет init_scheduler()).
Инициализация выполняется явно из app.py и worker.py в их __main__-блоках.
"""

import os
import re
import sys
import json
import time
import logging
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse, urlencode, urlunparse, parse_qs
from logging.handlers import RotatingFileHandler

import psycopg2
import psycopg2.extras
import psycopg2.pool
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ═══════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════

BOT_TOKEN  = os.environ.get("TG_BOT_TOKEN", "")
CHAT_ID    = os.environ.get("TG_CHAT_ID", "")
FSA_KEY    = os.environ.get("FSA_API_KEY", "")
FSA_VA_ID  = os.environ.get("FSA_VA_ID", "56177")
FSA_KEY2   = os.environ.get("FSA_API_KEY2", "")
FSA_VA_ID2 = os.environ.get("FSA_VA_ID2", "")
PORT       = int(os.environ.get("PORT", 10000))
HOSTNAME   = os.environ.get("RENDER_EXTERNAL_HOSTNAME", "fshub-bot.onrender.com")

WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "")
MAX_DB_FLIGHTS = int(os.environ.get("MAX_DB_FLIGHTS", "5000"))
DATABASE_URL   = os.environ.get("DATABASE_URL", "")
ADMIN_ID       = os.environ.get("ADMIN_TG_ID", "44859840")

FSA_URL = "https://www.fsairlines.net/va_interface2.php"
TG_BASE = f"https://api.telegram.org/bot{BOT_TOKEN}"

FSA_ENRICH_DEPARTURE_DELAY = 90
FSA_ENRICH_ARRIVAL_DELAY   = 180

# ── Discord Webhooks ───────────────────────────────────────────
# Три отдельных вебхука для разных каналов Discord.
# Если переменная не задана — отправка в этот канал молча пропускается.
#
# DISCORD_WEBHOOK_FLIGHTS      → канал вылетов и посадок (реал-тайм)
# DISCORD_WEBHOOK_EVENT        → канал операции «Тихий Вжух» (реал-тайм)
# DISCORD_WEBHOOK_SCREENSHOTS  → канал скриншотов рейсов (реал-тайм)
# DISCORD_WEBHOOK_URL          → канал плановых задач по расписанию (worker.py)
DISCORD_WEBHOOK_FLIGHTS     = os.environ.get("DISCORD_WEBHOOK_FLIGHTS",     "")
DISCORD_WEBHOOK_EVENT       = os.environ.get("DISCORD_WEBHOOK_EVENT",       "")
DISCORD_WEBHOOK_SCREENSHOTS = os.environ.get("DISCORD_WEBHOOK_SCREENSHOTS", "")
DISCORD_WEBHOOK_URL         = os.environ.get("DISCORD_WEBHOOK_URL",         "")

# ═══════════════════════════════════════════════════════════════
# LOGGING
# ═══════════════════════════════════════════════════════════════

logger = logging.getLogger("va_up_bot")
logger.setLevel(logging.INFO)

formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

try:
    file_handler = RotatingFileHandler(
        "bot.log", maxBytes=1_000_000, backupCount=3, encoding="utf-8"
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
except Exception:
    logger.warning("Could not create log file, using console only")

# ═══════════════════════════════════════════════════════════════
# HTTP SESSION
# ═══════════════════════════════════════════════════════════════

session = requests.Session()
_retry = Retry(
    total=3, read=3, connect=3, backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET", "POST"],
)
_adapter = HTTPAdapter(max_retries=_retry)
session.mount("https://", _adapter)
session.mount("http://", _adapter)

# ═══════════════════════════════════════════════════════════════
# DISCORD
# ═══════════════════════════════════════════════════════════════

# Паттерны для конвертации Telegram HTML → plain text для description в embed
_RE_DC_BOLD = re.compile(r"<b>(.*?)</b>",              re.DOTALL)
_RE_DC_ITAL = re.compile(r"<i>(.*?)</i>",              re.DOTALL)
_RE_DC_CODE = re.compile(r"<code>(.*?)</code>",        re.DOTALL)
_RE_DC_LINK = re.compile(r'<a href="(.*?)">(.*?)</a>', re.DOTALL)
_RE_DC_TAGS = re.compile(r"<[^>]+>")

# Паттерны для разбора структурированных сообщений бота в поля embed
# Каждый паттерн ищет "👨‍✈️ Captain: <b>Имя</b>" → ("Captain", "Имя")
_RE_FIELD = re.compile(
    r"(?:👨‍✈️|🆔|🗺|📍|✈️|📊|📏|⛽|🏔|⭐|➡️|🕛|✅)\s*"
    r"(?:<b>)?([^:<\n]+?)(?:</b>)?:\s*<b>(.*?)</b>",
    re.DOTALL,
)
_RE_LINK_HREF = re.compile(r"<a href='(.*?)'>.*?</a>")


def _strip_html(text: str) -> str:
    """Убирает HTML-теги, оставляет читаемый текст."""
    text = _RE_DC_BOLD.sub(r"\1", text)
    text = _RE_DC_ITAL.sub(r"\1", text)
    text = _RE_DC_CODE.sub(r"`\1`", text)
    text = _RE_DC_LINK.sub(r"\2", text)
    text = _RE_DC_TAGS.sub("", text)
    return text.strip()


def _tg_to_discord(text: str) -> str:
    """Конвертирует Telegram HTML → Discord Markdown (для plain-text сообщений)."""
    text = _RE_DC_BOLD.sub(r"**\1**", text)
    text = _RE_DC_ITAL.sub(r"*\1*",   text)
    text = _RE_DC_CODE.sub(r"`\1`",   text)
    text = _RE_DC_LINK.sub(r"[\2](\1)", text)
    text = _RE_DC_TAGS.sub("", text)
    return text.strip()


# ── Цвета embed по типу события ───────────────────────────────
# Discord принимает цвет как целое число (decimal RGB)
_DC_COLOR_DEPARTURE  = 0x5865F2  # синий — вылет
_DC_COLOR_BUTTER     = 0xFEE75C  # жёлтый — butter landing (-50…0 fpm)
_DC_COLOR_SMOOTH     = 0x23A55A  # зелёный — smooth (-350…-50 fpm)
_DC_COLOR_STABLE     = 0x57F287  # светло-зелёный — stable (-500…-350 fpm)
_DC_COLOR_FIRM       = 0xF0A332  # оранжевый — firm (-600…-500 fpm)
_DC_COLOR_HARD       = 0xED4245  # красный — hard/unsafe (< -600 fpm)
_DC_COLOR_EVENT_OK   = 0xFEE75C  # жёлтый — лег засчитан
_DC_COLOR_EVENT_FAIL = 0xED4245  # красный — провал / крушение
_DC_COLOR_EVENT_WARN = 0xF0A332  # оранжевый — лимит / предупреждение
_DC_COLOR_EVENT_WIN  = 0x23A55A  # зелёный — финиш
_DC_COLOR_SCHEDULE   = 0x5865F2  # синий — плановые задачи
_DC_COLOR_SCREENSHOT = 0x2B2D31  # тёмный — скриншот

_DC_FOOTER = "VA UP!"


def _landing_color(rate: int) -> int:
    """Возвращает цвет embed по скорости снижения."""
    if rate >= -50:   return _DC_COLOR_BUTTER
    if rate >= -350:  return _DC_COLOR_SMOOTH
    if rate >= -500:  return _DC_COLOR_STABLE
    if rate >= -600:  return _DC_COLOR_FIRM
    return _DC_COLOR_HARD


def _dc_post(webhook_url: str, payload: dict) -> bool:
    """Низкоуровневая отправка в Discord Webhook. Возвращает True при успехе."""
    if not webhook_url:
        return False
    try:
        r = session.post(webhook_url, json=payload, timeout=10)
        if r.status_code in (200, 204):
            return True
        logger.warning(f"Discord webhook failed ({r.status_code}): {r.text[:200]}")
        return False
    except Exception as e:
        logger.error(f"Discord send error: {e}")
        return False


def discord_send(text: str, webhook_url: str = "") -> bool:
    """
    Отправляет plain-text сообщение (для плановых задач worker.py).
    Конвертирует Telegram HTML → Discord Markdown.
    """
    url = webhook_url or DISCORD_WEBHOOK_URL
    if not url:
        return False
    clean = _tg_to_discord(text)
    if len(clean) > 2000:
        clean = clean[:1997] + "..."
    return _dc_post(url, {"content": clean})


def discord_embed(
    webhook_url: str,
    title: str,
    color: int,
    fields: list,
    description: str = "",
    url: str = "",
    footer: str = _DC_FOOTER,
) -> bool:
    """
    Отправляет красивую embed-карточку в Discord.

    fields — список dict {"name": str, "value": str, "inline": bool}
    """
    if not webhook_url:
        return False

    embed: dict = {
        "title":  title,
        "color":  color,
        "fields": fields,
    }
    if description:
        embed["description"] = description
    if url:
        embed["url"] = url
    if footer:
        embed["footer"] = {"text": footer}

    return _dc_post(webhook_url, {"embeds": [embed]})


# ── Публичные функции отправки ─────────────────────────────────

def discord_send_departure(
    pilot: str, flight_no: str, dep: str, arr: str,
    aircraft: str, is_loading: bool = False,
) -> bool:
    """Embed для вылета в канал FLIGHTS."""
    route = "⏳ Загружаю маршрут..." if is_loading else f"{dep} → {arr}"
    fields = [
        {"name": "Пилот",    "value": pilot,     "inline": True},
        {"name": "Рейс",     "value": flight_no or "N/A", "inline": True},
        {"name": "Маршрут",  "value": route,     "inline": False},
        {"name": "Самолёт",  "value": aircraft,  "inline": True},
    ]
    return discord_embed(
        DISCORD_WEBHOOK_FLIGHTS,
        title="🛫  ВЫЛЕТ ПОДТВЕРЖДЁН — CLEARED FOR TAKEOFF",
        color=_DC_COLOR_DEPARTURE,
        fields=fields,
        footer=f"{_DC_FOOTER} • Желаем попутного ветра!",
    )


def discord_send_landing(
    pilot: str, flight_no: str, dep: str, arr: str,
    aircraft: str, airport: str, rate: int, rating: str,
    distance_nm=None, fuel_burnt=None, max_alt=None,
    report_url: str = "",
    is_loading: bool = False,
) -> bool:
    """Embed для посадки в канал FLIGHTS."""
    route = "⏳ Загружаю маршрут..." if is_loading else f"{dep} → {arr}"
    fields = [
        {"name": "Пилот",    "value": pilot,          "inline": True},
        {"name": "Рейс",     "value": flight_no or "N/A", "inline": True},
        {"name": "Маршрут",  "value": route,          "inline": False},
        {"name": "Аэропорт", "value": airport,        "inline": True},
        {"name": "Самолёт",  "value": aircraft,       "inline": True},
        {"name": "Посадка",  "value": f"**{rate} fpm** — {rating}", "inline": True},
    ]
    if distance_nm:
        fields.append({"name": "Дальность", "value": f"{distance_nm} nm", "inline": True})
    if fuel_burnt:
        fields.append({"name": "Топливо",   "value": f"{fuel_burnt} kg",  "inline": True})
    if max_alt:
        fields.append({"name": "Макс. высота", "value": f"{max_alt:,} ft", "inline": True})
    if report_url:
        fields.append({"name": "\u200b", "value": f"[📋 Открыть отчёт]({report_url})", "inline": False})

    return discord_embed(
        DISCORD_WEBHOOK_FLIGHTS,
        title="🛬  ПОСАДКА ВЫПОЛНЕНА — TOUCHDOWN",
        color=_landing_color(rate),
        fields=fields,
    )


def discord_send_hard_landing(pilot: str, rate: int) -> bool:
    """Embed Hard Landing Alert в канал FLIGHTS."""
    return discord_embed(
        DISCORD_WEBHOOK_FLIGHTS,
        title="⚠️  HARD LANDING ALERT",
        color=_DC_COLOR_HARD,
        fields=[
            {"name": "Пилот",   "value": pilot,          "inline": True},
            {"name": "Посадка", "value": f"{rate} fpm",  "inline": True},
            {"name": "Статус",  "value": "Aircraft inspection recommended", "inline": False},
        ],
    )


def discord_send_operation(
    title: str, color: int,
    fields: list, footer_extra: str = "",
) -> bool:
    """Универсальный embed для канала EVENT (операция «Тихий Вжух»)."""
    footer = f"{_DC_FOOTER} • Операция «Тихий Вжух»"
    if footer_extra:
        footer += f" • {footer_extra}"
    return discord_embed(
        DISCORD_WEBHOOK_EVENT,
        title=title,
        color=color,
        fields=fields,
        footer=footer,
    )


def discord_send_screenshots(
    screenshots: list,
    pilot: str = "",
    flight_no: str = "",
) -> None:
    """
    Отправляет скриншоты рейса в канал DISCORD_WEBHOOK_SCREENSHOTS.
    Каждый скриншот — отдельный embed с полноразмерной картинкой.
    До 3 скриншотов.
    """
    if not DISCORD_WEBHOOK_SCREENSHOTS:
        return

    caption = f"📸  Скриншоты рейса"
    if pilot:
        caption += f" — {pilot}"
    if flight_no and flight_no != "N/A":
        caption += f" ({flight_no})"

    for i, scr in enumerate(screenshots[:3], 1):
        img_url = scr.get("screenshot_url") or scr.get("url", "")
        if not img_url:
            continue

        embed = {
            "title": f"{caption} #{i}",
            "color": _DC_COLOR_SCREENSHOT,
            "image": {"url": img_url},
            "footer": {"text": _DC_FOOTER},
        }
        _dc_post(DISCORD_WEBHOOK_SCREENSHOTS, {"embeds": [embed]})
        time.sleep(0.5)  # небольшая пауза чтобы не флудить


def discord_send_flights(text: str) -> bool:
    """
    Резервная функция: plain-text → FLIGHTS.
    Используется в worker.py для плановых задач.
    Для реал-тайма используй discord_send_departure / discord_send_landing.
    """
    return discord_send(text, DISCORD_WEBHOOK_FLIGHTS)


def discord_send_event(text: str) -> bool:
    """
    Резервная функция: plain-text → EVENT.
    Используется в worker.py для плановых задач.
    Для реал-тайма используй discord_send_operation.
    """
    return discord_send(text, DISCORD_WEBHOOK_EVENT)

# ═══════════════════════════════════════════════════════════════
# DATABASE
# ═══════════════════════════════════════════════════════════════

_pool: Optional[psycopg2.pool.ThreadedConnectionPool] = None
_pool_lock = threading.Lock()


def _build_dsn(base_url: str) -> str:
    parsed = urlparse(base_url)
    params = {
        "sslmode": "require",
        "keepalives": "1",
        "keepalives_idle": "30",
        "keepalives_interval": "10",
        "keepalives_count": "5",
        "connect_timeout": "10",
    }
    existing = parse_qs(parsed.query)
    existing.update({k: [v] for k, v in params.items()})
    flat = {k: v[0] for k, v in existing.items()}
    new_query = urlencode(flat)
    return urlunparse(parsed._replace(query=new_query))


def _create_pool():
    global _pool
    dsn = _build_dsn(DATABASE_URL)
    _pool = psycopg2.pool.ThreadedConnectionPool(
        minconn=1,
        maxconn=10,
        dsn=dsn,
        cursor_factory=psycopg2.extras.RealDictCursor,
    )
    logger.info("PostgreSQL connection pool created (keepalives enabled, maxconn=10)")


def _test_conn(conn) -> bool:
    try:
        conn.cursor().execute("SELECT 1")
        return True
    except Exception:
        return False


def get_conn():
    global _pool
    for attempt in range(3):
        try:
            with _pool_lock:
                if _pool is None:
                    _create_pool()
            conn = _pool.getconn()
            if not _test_conn(conn):
                logger.warning(f"Dead connection (attempt {attempt + 1}), recreating pool")
                try:
                    _pool.putconn(conn, close=True)
                except Exception:
                    pass
                with _pool_lock:
                    _pool = None
                continue
            return conn
        except psycopg2.OperationalError as e:
            logger.warning(f"OperationalError getting conn (attempt {attempt + 1}): {e}")
            with _pool_lock:
                _pool = None
            if attempt == 2:
                raise
    raise RuntimeError("Could not get a database connection after 3 attempts")


def put_conn(conn):
    global _pool
    if _pool is not None:
        try:
            _pool.putconn(conn)
        except Exception as e:
            logger.warning(f"Error returning connection to pool: {e}")


def db_execute(query: str, params=None, fetch: str = "none"):
    last_error = None
    for attempt in range(2):
        conn = None
        try:
            conn = get_conn()
            with conn.cursor() as cur:
                cur.execute(query, params or ())
                conn.commit()
                if fetch == "one":
                    return cur.fetchone()
                if fetch == "all":
                    return cur.fetchall()
                return None
        except psycopg2.OperationalError as e:
            last_error = e
            logger.warning(f"OperationalError in db_execute (attempt {attempt + 1}): {e}")
            if conn is not None:
                try:
                    conn.rollback()
                except Exception:
                    pass
                try:
                    conn.close()
                except Exception:
                    pass
                conn = None
            with _pool_lock:
                _pool = None
        except Exception as e:
            last_error = e
            logger.error(f"Database error: {e}")
            if conn is not None:
                try:
                    conn.rollback()
                except Exception:
                    pass
                put_conn(conn)
                conn = None
            raise
        finally:
            if conn is not None:
                put_conn(conn)

    logger.error(f"Database error after 2 attempts: {last_error}")
    raise last_error


def _init_db():
    db_execute(
        """
        CREATE TABLE IF NOT EXISTS flights (
            id           SERIAL PRIMARY KEY,
            flight_id    TEXT UNIQUE,
            pilot        TEXT,
            flight_no    TEXT,
            departure    TEXT,
            arrival      TEXT,
            aircraft     TEXT,
            landing_rate INTEGER,
            profit       INTEGER,
            created_at   TIMESTAMPTZ DEFAULT NOW()
        )
        """
    )
    db_execute(
        """
        CREATE TABLE IF NOT EXISTS achievements (
            id          SERIAL PRIMARY KEY,
            pilot       TEXT,
            achievement TEXT,
            created_at  TIMESTAMPTZ DEFAULT NOW()
        )
        """
    )
    db_execute(
        """
        CREATE TABLE IF NOT EXISTS daily_economy (
            id      SERIAL PRIMARY KEY,
            day     DATE UNIQUE,
            income  BIGINT DEFAULT 0,
            expense BIGINT DEFAULT 0,
            net     BIGINT DEFAULT 0,
            detail  JSONB  DEFAULT '{}'
        )
        """
    )
    db_execute(
        """
        CREATE TABLE IF NOT EXISTS contest_entries (
            id           SERIAL PRIMARY KEY,
            flight_id    TEXT UNIQUE,
            pilot        TEXT,
            flight_no    TEXT,
            departure    TEXT,
            arrival      TEXT,
            aircraft     TEXT,
            landing_rate INTEGER,
            report_url   TEXT,
            contest_month TEXT,
            created_at   TIMESTAMPTZ DEFAULT NOW()
        )
        """
    )
    db_execute(
        """
        CREATE TABLE IF NOT EXISTS operation_pilots (
            id            SERIAL PRIMARY KEY,
            pilot_name    TEXT UNIQUE,
            aircraft      TEXT DEFAULT '',
            current_leg   INTEGER DEFAULT 1,
            total_points  INTEGER DEFAULT 0,
            ferry_num     INTEGER DEFAULT 1,
            status        TEXT DEFAULT 'active',
            registered_at TIMESTAMPTZ DEFAULT NOW()
        )
        """
    )
    # Миграция: добавить ferry_num если нет (для существующих БД)
    try:
        db_execute(
            "ALTER TABLE operation_pilots ADD COLUMN IF NOT EXISTS ferry_num INTEGER DEFAULT 1"
        )
    except Exception:
        pass

    db_execute(
        """
        CREATE TABLE IF NOT EXISTS operation_legs (
            id           SERIAL PRIMARY KEY,
            pilot_name   TEXT,
            leg_num      INTEGER,
            dep          TEXT,
            arr          TEXT,
            landing_rate INTEGER,
            base_points  INTEGER DEFAULT 0,
            coeff        NUMERIC(4,2) DEFAULT 1.0,
            net_bonus    INTEGER DEFAULT 0,
            points       INTEGER,
            on_network   BOOLEAN DEFAULT FALSE,
            flight_id    TEXT,
            report_url   TEXT,
            attempt      INTEGER DEFAULT 1,
            created_at   TIMESTAMPTZ DEFAULT NOW()
        )
        """
    )
    logger.info("Database tables ready")


# ═══════════════════════════════════════════════════════════════
# DB HELPERS — FLIGHTS
# ═══════════════════════════════════════════════════════════════

def db_add_flight(
    flight_id: Optional[str],
    pilot: str,
    flight_no: str,
    departure: str,
    arrival: str,
    aircraft: str,
    landing_rate: int,
    profit: Optional[int],
):
    """INSERT + чистка старых записей в одной транзакции."""
    last_error = None
    for attempt in range(2):
        conn = None
        try:
            conn = get_conn()
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO flights
                        (flight_id, pilot, flight_no, departure, arrival,
                         aircraft, landing_rate, profit)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (flight_id) DO NOTHING
                    """,
                    (flight_id, pilot, flight_no, departure, arrival,
                     aircraft, landing_rate, profit),
                )
                cur.execute(
                    """
                    DELETE FROM flights
                    WHERE id NOT IN (
                        SELECT id FROM flights ORDER BY id DESC LIMIT %s
                    )
                    """,
                    (MAX_DB_FLIGHTS,),
                )
            conn.commit()
            return
        except psycopg2.OperationalError as e:
            last_error = e
            logger.warning(f"OperationalError in db_add_flight (attempt {attempt + 1}): {e}")
            if conn is not None:
                try:
                    conn.rollback()
                except Exception:
                    pass
                try:
                    conn.close()
                except Exception:
                    pass
                conn = None
            with _pool_lock:
                _pool = None
        except Exception as e:
            logger.error(f"db_add_flight error: {e}")
            if conn is not None:
                try:
                    conn.rollback()
                except Exception:
                    pass
                put_conn(conn)
                conn = None
            raise
        finally:
            if conn is not None:
                put_conn(conn)
    raise last_error


def db_update_flight_route(flight_id: str, flight_no: str, departure: str, arrival: str) -> None:
    if not flight_id:
        return
    db_execute(
        """
        UPDATE flights
        SET flight_no  = %s,
            departure  = %s,
            arrival    = %s
        WHERE flight_id = %s
          AND (flight_no = 'N/A' OR departure = '????' OR arrival = '????')
        """,
        (flight_no, departure, arrival, flight_id),
    )
    db_execute(
        """
        UPDATE contest_entries
        SET flight_no  = %s,
            departure  = %s,
            arrival    = %s
        WHERE flight_id = %s
          AND (flight_no = 'N/A' OR departure = '????' OR arrival = '????')
        """,
        (flight_no, departure, arrival, flight_id),
    )


def db_last_flights(limit: int = 5) -> List:
    return db_execute(
        "SELECT * FROM flights ORDER BY id DESC LIMIT %s",
        (limit,), fetch="all",
    ) or []


def db_all_flights() -> List:
    return db_execute(
        "SELECT * FROM flights ORDER BY id DESC",
        fetch="all",
    ) or []


def db_flights_this_month() -> List:
    return db_execute(
        """
        SELECT * FROM flights
        WHERE DATE_TRUNC('month', created_at) = DATE_TRUNC('month', NOW())
        ORDER BY id DESC
        """,
        fetch="all",
    ) or []


def db_top_landings(limit: int = 10) -> List:
    return db_execute(
        """
        SELECT * FROM flights
        WHERE DATE_TRUNC('month', created_at) = DATE_TRUNC('month', NOW())
        ORDER BY ABS(landing_rate) ASC LIMIT %s
        """,
        (limit,), fetch="all",
    ) or []


def db_top_pilots_week(limit: int = 10) -> List:
    """
    Топ пилотов за последние 7 дней — прямо в SQL, без загрузки всех рейсов в память.
    Возвращает список dict с ключами pilot и cnt.
    """
    return db_execute(
        """
        SELECT pilot, COUNT(*) AS cnt
        FROM flights
        WHERE created_at >= NOW() - INTERVAL '7 days'
        GROUP BY pilot
        ORDER BY cnt DESC
        LIMIT %s
        """,
        (limit,), fetch="all",
    ) or []


# ═══════════════════════════════════════════════════════════════
# DB HELPERS — DAILY ECONOMY
# ═══════════════════════════════════════════════════════════════

def db_save_daily_economy(day: str, income: int, expense: int, detail: Dict):
    db_execute(
        """
        INSERT INTO daily_economy (day, income, expense, net, detail)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (day) DO UPDATE SET
            income  = EXCLUDED.income,
            expense = EXCLUDED.expense,
            net     = EXCLUDED.net,
            detail  = EXCLUDED.detail
        """,
        (day, income, expense, income - expense, json.dumps(detail)),
    )


def db_get_monthly_economy() -> List:
    return db_execute(
        """
        SELECT * FROM daily_economy
        WHERE DATE_TRUNC('month', day) = DATE_TRUNC('month', CURRENT_DATE)
        ORDER BY day ASC
        """,
        fetch="all",
    ) or []


def db_get_today_economy() -> Optional[Dict]:
    return db_execute(
        "SELECT * FROM daily_economy WHERE day = CURRENT_DATE",
        fetch="one",
    )


# ═══════════════════════════════════════════════════════════════
# DB HELPERS — CONTEST
# ═══════════════════════════════════════════════════════════════

CONTEST_POINTS_PER_LANDING = 100
CONTEST_MONTHLY_LIMIT      = 1000
CONTEST_RATE_MIN           = -30
CONTEST_RATE_MAX           = -10


def is_contest_landing(rate: int) -> bool:
    return CONTEST_RATE_MAX >= rate >= CONTEST_RATE_MIN


def db_contest_add(
    flight_id: str, pilot: str, flight_no: str,
    departure: str, arrival: str, aircraft: str,
    landing_rate: int, report_url: str,
):
    month = datetime.now().strftime("%Y-%m")
    db_execute(
        """
        INSERT INTO contest_entries
            (flight_id, pilot, flight_no, departure, arrival,
             aircraft, landing_rate, report_url, contest_month)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (flight_id) DO NOTHING
        """,
        (flight_id, pilot, flight_no, departure, arrival,
         aircraft, landing_rate, report_url, month),
    )


def db_contest_month(month: Optional[str] = None) -> List:
    m = month or datetime.now().strftime("%Y-%m")
    return db_execute(
        """
        SELECT * FROM contest_entries
        WHERE contest_month = %s
        ORDER BY created_at ASC
        """,
        (m,), fetch="all",
    ) or []


def db_contest_recent_months(n: int = 4) -> List[str]:
    rows = db_execute(
        """
        SELECT DISTINCT contest_month
        FROM contest_entries
        ORDER BY contest_month DESC
        LIMIT %s
        """,
        (n,), fetch="all",
    ) or []
    return [r["contest_month"] for r in rows]


# ═══════════════════════════════════════════════════════════════
# OPERATION «ТИХИЙ ВЖУХ»
# ═══════════════════════════════════════════════════════════════

OPERATION_NAME       = "Тихий Вжух"
OPERATION_START      = "2026-06-10"
OPERATION_END        = "2026-08-31"
OPERATION_HARD_CRASH = 1200
OPERATION_FAIL_RATE  = 600

OPERATION_LEGS = [
    (1,  "VTBS", "VVPQ",  290),
    (2,  "VVPQ", "RPVP",  880),
    (3,  "RPVP", "WAMM",  620),
    (4,  "WAMM", "WAJJ",  970),
    (5,  "WAJJ", "NWWW", 1900),
    (6,  "NWWW", "NFFN",  690),
    (7,  "NFFN", "NSFA",  670),
    (8,  "NSFA", "NTAA", 1350),
    (9,  "NTAA", "NTGJ",  970),
    (10, "NTGJ", "SCIP", 1430),
    (11, "SCIP", "SCFA", 2160),
    (12, "SCFA", "SGAS",  780),
    (13, "SGAS", "SBGL",  810),
]
OPERATION_LEG_MAP = {(dep, arr): (num, pts) for num, dep, arr, pts in OPERATION_LEGS}

OPERATION_AIRCRAFT_COEFF: Dict[str, float] = {
    "A310":  1.3,
    "A318":  1.3,
    "A319":  1.3,
    "A320":  1.1,
    "A321":  1.0,
    "A330":  1.0,
    "ATR72": 2.5,
    "AT72":  2.5,
    "B727":  2.0,
    "B736":  1.1,
    "B737":  1.1,
    "B738":  1.0,
    "B38M":  1.3,
    "B773":  1.0,
    "CRJ7":  1.5,
    "MD11":  1.1,
}
OPERATION_VATSIM_BONUS = 50
OPERATION_MAX_POINTS   = sum(pts for _, _, _, pts in OPERATION_LEGS)  # 13360


def op_get_aircraft_coeff(aircraft_icao: str) -> float:
    if not aircraft_icao:
        return 1.0
    key = aircraft_icao.upper().strip()
    if key in OPERATION_AIRCRAFT_COEFF:
        return OPERATION_AIRCRAFT_COEFF[key]
    for icao_key, coeff in OPERATION_AIRCRAFT_COEFF.items():
        if icao_key in key:
            return coeff
    return 1.0


def op_calc_points(base_pts: int, aircraft_icao: str, on_network: bool) -> tuple:
    coeff     = op_get_aircraft_coeff(aircraft_icao)
    base_calc = round(base_pts * coeff)
    net_bonus = OPERATION_VATSIM_BONUS if on_network else 0
    return base_calc + net_bonus, coeff, net_bonus


def operation_is_active() -> bool:
    today = datetime.now().date()
    try:
        start = datetime.strptime(OPERATION_START, "%Y-%m-%d").date()
        end   = datetime.strptime(OPERATION_END,   "%Y-%m-%d").date()
        return start <= today <= end
    except Exception:
        return False


def db_op_get_pilot(pilot_name: str) -> Optional[Dict]:
    return db_execute(
        "SELECT * FROM operation_pilots WHERE pilot_name = %s",
        (pilot_name,), fetch="one",
    )


def db_op_all_pilots() -> List:
    return db_execute(
        "SELECT * FROM operation_pilots ORDER BY total_points DESC",
        fetch="all",
    ) or []


def db_op_register_pilot(pilot_name: str, aircraft: str = "") -> bool:
    existing = db_op_get_pilot(pilot_name)
    if existing:
        if existing["status"] == "finished":
            new_ferry = existing["ferry_num"] + 1
            db_execute(
                """
                UPDATE operation_pilots
                SET current_leg = 1, status = 'active',
                    aircraft = %s, ferry_num = %s
                WHERE pilot_name = %s
                """,
                (aircraft, new_ferry, pilot_name),
            )
            return True
        return False
    db_execute(
        "INSERT INTO operation_pilots (pilot_name, aircraft, ferry_num) VALUES (%s, %s, 1)",
        (pilot_name, aircraft),
    )
    return True


def db_op_add_leg(
    pilot_name: str, leg_num: int, dep: str, arr: str,
    landing_rate: int, points: int, flight_id: str, report_url: str,
    base_points: int = 0, coeff: float = 1.0,
    net_bonus: int = 0, on_network: bool = False,
) -> None:
    attempt_row = db_execute(
        "SELECT COUNT(*) as cnt FROM operation_legs WHERE pilot_name=%s AND leg_num=%s",
        (pilot_name, leg_num), fetch="one",
    )
    attempt = (attempt_row["cnt"] + 1) if attempt_row else 1
    db_execute(
        """
        INSERT INTO operation_legs
            (pilot_name, leg_num, dep, arr, landing_rate,
             base_points, coeff, net_bonus, points, on_network,
             flight_id, report_url, attempt)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (pilot_name, leg_num, dep, arr, landing_rate,
         base_points, coeff, net_bonus, points, on_network,
         flight_id, report_url, attempt),
    )


def db_op_update_pilot(
    pilot_name: str, current_leg: int, total_points: int, status: str = "active"
) -> None:
    db_execute(
        """
        UPDATE operation_pilots
        SET current_leg = %s, total_points = %s, status = %s
        WHERE pilot_name = %s
        """,
        (current_leg, total_points, status, pilot_name),
    )


def db_op_start_new_ferry(pilot_name: str, aircraft: str) -> Optional[int]:
    pilot = db_op_get_pilot(pilot_name)
    if not pilot or pilot["status"] != "finished":
        return None
    new_ferry = pilot["ferry_num"] + 1
    db_execute(
        """
        UPDATE operation_pilots
        SET current_leg = 1, status = 'active',
            aircraft = %s, ferry_num = %s
        WHERE pilot_name = %s
        """,
        (aircraft, new_ferry, pilot_name),
    )
    return new_ferry


def db_op_admin_set(pilot_name: str, leg_num: int, points_delta: int) -> None:
    pilot = db_op_get_pilot(pilot_name)
    if not pilot:
        return
    new_points = max(0, pilot["total_points"] + points_delta)
    db_execute(
        "UPDATE operation_pilots SET total_points = %s WHERE pilot_name = %s",
        (new_points, pilot_name),
    )


def db_op_reset_pilot(pilot_name: str) -> None:
    db_execute(
        """
        UPDATE operation_pilots
        SET current_leg = 1, total_points = 0, status = 'active'
        WHERE pilot_name = %s
        """,
        (pilot_name,),
    )


def db_op_check_daily_limit(pilot_name: str) -> tuple:
    rows = db_execute(
        """
        SELECT created_at FROM operation_legs
        WHERE pilot_name = %s
          AND points > 0
          AND DATE(created_at AT TIME ZONE 'UTC') = CURRENT_DATE
        ORDER BY created_at ASC
        """,
        (pilot_name,), fetch="all",
    ) or []
    count = len(rows)
    return count < 2, count


# ═══════════════════════════════════════════════════════════════
# TELEGRAM
# ═══════════════════════════════════════════════════════════════

def tg_send(text: str, chat_id=None) -> bool:
    target = str(chat_id) if chat_id else CHAT_ID
    try:
        r = session.post(
            f"{TG_BASE}/sendMessage",
            json={
                "chat_id": target,
                "text": text[:4096],
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
            timeout=20,
        )
        if r.status_code != 200:
            logger.warning(f"Telegram send failed: {r.text}")
            return False
        return True
    except Exception as e:
        logger.exception(f"Telegram send error: {e}")
        return False


def tg_photo(url: str, caption: str) -> bool:
    try:
        r = session.post(
            f"{TG_BASE}/sendPhoto",
            json={
                "chat_id": CHAT_ID,
                "photo": url,
                "caption": caption[:1024],
                "parse_mode": "HTML",
            },
            timeout=30,
        )
        if r.status_code == 200:
            return True
        logger.warning(f"Photo failed: {r.text}")
    except Exception as e:
        logger.exception(f"Photo error: {e}")
    return tg_send(f'📸 <b>Screenshot</b>\n<a href="{url}">Open media</a>')


def tg_setup_webhook():
    url = f"https://{HOSTNAME}/bot/{BOT_TOKEN}"
    try:
        r = session.post(
            f"{TG_BASE}/setWebhook",
            json={"url": url},
            timeout=20,
        )
        logger.info(f"Telegram webhook: {r.text}")
    except Exception as e:
        logger.exception(f"Webhook setup error: {e}")


def tg_edit_message(chat_id, message_id: int, text: str) -> None:
    try:
        session.post(
            f"{TG_BASE}/editMessageText",
            json={
                "chat_id": str(chat_id),
                "message_id": message_id,
                "text": text[:4096],
                "parse_mode": "HTML",
                "reply_markup": {"inline_keyboard": []},
            },
            timeout=10,
        )
    except Exception as e:
        logger.exception(f"editMessageText error: {e}")


# ═══════════════════════════════════════════════════════════════
# FSA API
# ═══════════════════════════════════════════════════════════════

def fsa_call(function: str, extra: Optional[Dict] = None, key: str = "", va_id: str = ""):
    api_key   = key   or FSA_KEY
    api_va_id = va_id or FSA_VA_ID
    if not api_key:
        return None
    params = {
        "function": function,
        "va_id":    api_va_id,
        "apikey":   api_key,
        "format":   "json",
    }
    if extra:
        params.update(extra)
    try:
        r = session.get(FSA_URL, params=params, timeout=30)
        r.raise_for_status()
        body = r.json()
        if body.get("status") == "SUCCESS":
            return body.get("data")
        logger.warning(f"FSA failure: {body}")
        return None
    except Exception as e:
        logger.exception(f"FSA error: {e}")
        return None


def fsa_call2(function: str, extra: Optional[Dict] = None):
    if not FSA_KEY2 or not FSA_VA_ID2:
        return None
    return fsa_call(function, extra, key=FSA_KEY2, va_id=FSA_VA_ID2)


def _safe_ts(ts) -> Optional[datetime]:
    try:
        return datetime.fromtimestamp(float(ts))
    except Exception:
        return None


def fsa_daily_transactions() -> List[Dict]:
    data = fsa_call("getDailyTransactions")
    if not isinstance(data, list):
        return []
    today = datetime.now().date()
    result = []
    for t in data:
        dt = _safe_ts(t.get("ts"))
        if dt and dt.date() == today:
            result.append(t)
    return result


def fsa_active_flights() -> List[Dict]:
    data = fsa_call("getActiveFlights")
    return data if isinstance(data, list) else []


# ── Кэш fsa_airline_data (TTL 5 минут) ────────────────────────
_airline_data_cache: Optional[Dict] = None
_airline_data_ts: float = 0
_airline_data_lock = threading.Lock()


def fsa_airline_data() -> Optional[Dict]:
    """Возвращает данные авиакомпании с кэшированием на 5 минут."""
    global _airline_data_cache, _airline_data_ts
    with _airline_data_lock:
        if _airline_data_cache is not None and (time.time() - _airline_data_ts) < 300:
            return _airline_data_cache

    data = fsa_call("getAirlineData")
    result = None
    if isinstance(data, list) and data:
        result = data[0]
    elif isinstance(data, dict):
        result = data

    with _airline_data_lock:
        _airline_data_cache = result
        _airline_data_ts = time.time()
    return result


# ── Кэш пилотов FSAirlines ────────────────────────────────────
_fsa_pilot_cache: Dict[str, int] = {}
_fsa_pilot_cache_lock = threading.Lock()
_fsa_pilot_cache_ts: float = 0

# Кэш данных вылета: pilot_name → {dep, arr, flight_no, ts}
_departure_cache: Dict[str, Dict] = {}
_departure_cache_lock = threading.Lock()


def fsa_refresh_pilot_cache() -> None:
    global _fsa_pilot_cache_ts
    data = fsa_call("getPilotList")
    if not isinstance(data, list):
        return
    with _fsa_pilot_cache_lock:
        _fsa_pilot_cache.clear()
        for p in data:
            full_name = f"{p.get('name', '')} {p.get('surname', '')}".strip()
            if full_name and p.get("id"):
                _fsa_pilot_cache[full_name] = int(p["id"])
        _fsa_pilot_cache_ts = time.time()
    logger.info(f"FSA pilot cache refreshed: {len(_fsa_pilot_cache)} pilots")


def fsa_refresh_pilot_cache2() -> None:
    if not FSA_KEY2 or not FSA_VA_ID2:
        return
    data = fsa_call2("getPilotList")
    if not isinstance(data, list):
        return
    with _fsa_pilot_cache_lock:
        for p in data:
            full_name = f"{p.get('name', '')} {p.get('surname', '')}".strip()
            if full_name and p.get("id") and full_name not in _fsa_pilot_cache:
                _fsa_pilot_cache[full_name] = int(p["id"])
    logger.info(f"FSA2 pilot cache merged: total {len(_fsa_pilot_cache)} pilots")


def fsa_get_pilot_id(name: str) -> Optional[int]:
    if not _fsa_pilot_cache or (time.time() - _fsa_pilot_cache_ts) > 3600:
        fsa_refresh_pilot_cache()
        if FSA_KEY2 and FSA_VA_ID2:
            fsa_refresh_pilot_cache2()
    with _fsa_pilot_cache_lock:
        return _fsa_pilot_cache.get(name)


def fsa_get_pilot_status(pilot_id: int) -> Optional[Dict]:
    data = fsa_call("getPilotStatus", {"pilot_id": pilot_id})
    if isinstance(data, list) and data:
        return data[0]
    return data if isinstance(data, dict) else None


def fsa_get_recent_report(pilot_id: int, arrival_time: str) -> Optional[Dict]:
    data = fsa_call("getFlightReports", {"pilot_id": pilot_id, "count": 5})
    if not isinstance(data, list):
        return None
    try:
        arr_dt = datetime.fromisoformat(arrival_time.replace("Z", "+00:00"))
    except Exception:
        arr_dt = None

    for report in data:
        if arr_dt:
            try:
                rep_dt = _safe_ts(report.get("ts"))
                if rep_dt and abs((arr_dt.replace(tzinfo=None) - rep_dt).total_seconds()) < 600:
                    return report
            except Exception:
                pass
        else:
            return report
    return None


def _is_plan_empty(plan: Dict) -> bool:
    flight_no = plan.get("flight_no", "")
    departure = plan.get("departure", "")
    arrival   = plan.get("arrival", "")
    return (
        not flight_no or flight_no in ("N/A", "", "None") or
        not departure or departure in ("????", "", "None") or
        not arrival   or arrival   in ("????", "", "None")
    )


def _is_valid_icao(icao: str) -> bool:
    if not icao or len(icao) != 4:
        return False
    if not icao[0].isalpha():
        return False
    return icao.isalnum()


# ═══════════════════════════════════════════════════════════════
# FINANCIAL AGGREGATION
# ═══════════════════════════════════════════════════════════════

INTERNAL_TRANSFER_REASONS = {"Fleet Money Transfer"}


def _aggregate(transactions: List[Dict]) -> Dict:
    inc, exp = 0.0, 0.0
    inc_cat: Dict[str, float] = {}
    exp_cat: Dict[str, float] = {}
    internal_volume = 0.0

    for t in transactions:
        try:
            v = float(t.get("value", 0))
        except (ValueError, TypeError):
            continue
        r = t.get("reason") or "Other"

        if r in INTERNAL_TRANSFER_REASONS:
            internal_volume += abs(v)
            continue

        if v >= 0:
            inc += v
            inc_cat[r] = inc_cat.get(r, 0) + v
        else:
            exp += abs(v)
            exp_cat[r] = exp_cat.get(r, 0) + abs(v)

    return {
        "inc": inc, "exp": exp, "net": inc - exp,
        "inc_cat": inc_cat, "exp_cat": exp_cat,
        "internal_volume": internal_volume,
    }


def _top(d: Dict, n: int = 3) -> List[Tuple]:
    return sorted(d.items(), key=lambda x: x[1], reverse=True)[:n]


def _nem(net: float) -> str:
    return "📈" if net > 0 else "📉" if net < 0 else "➡️"


def snapshot_daily_economy():
    if not FSA_KEY:
        return
    txs = fsa_daily_transactions()
    if not txs:
        logger.info("snapshot_daily_economy: no transactions today")
        return
    ag = _aggregate(txs)
    day = datetime.now().strftime("%Y-%m-%d")
    detail = {
        "inc_cat": ag["inc_cat"],
        "exp_cat": ag["exp_cat"],
        "internal_volume": ag.get("internal_volume", 0),
    }
    db_save_daily_economy(
        day=day,
        income=int(ag["inc"]),
        expense=int(ag["exp"]),
        detail=detail,
    )
    logger.info(f"Economy snapshot saved for {day}: net={ag['net']:,.0f}")


# ═══════════════════════════════════════════════════════════════
# LANDING RATING
# ═══════════════════════════════════════════════════════════════

def landing_rating(rate: int) -> Tuple[str, str]:
    if rate < -1000: return "UNSAFE LANDING", "🔴"
    if rate < -600:  return "HARD LANDING",   "🟠"
    if rate < -500:  return "FIRM LANDING",   "🟡"
    if rate < -350:  return "STABLE LANDING", "🟢"
    if rate < -50:   return "SMOOTH LANDING", "✅"
    return                  "BUTTER LANDING", "🧈✨"


# ═══════════════════════════════════════════════════════════════
# COMMAND FORMATTERS
# ═══════════════════════════════════════════════════════════════

MONTH_NAMES = {
    1: "Январь", 2: "Февраль", 3: "Март", 4: "Апрель",
    5: "Май", 6: "Июнь", 7: "Июль", 8: "Август",
    9: "Сентябрь", 10: "Октябрь", 11: "Ноябрь", 12: "Декабрь",
}


def _month_label(m: str) -> str:
    try:
        dt = datetime.strptime(m, "%Y-%m")
        return f"{MONTH_NAMES[dt.month]} {dt.year}"
    except Exception:
        return m


def fmt_stats() -> str:
    now = datetime.now()
    month_label = f"{MONTH_NAMES[now.month]} {now.year}"
    flights = db_flights_this_month()
    if not flights:
        return f"📊 <b>ОПЕРАЦИИ VA UP!</b>\n📅 {month_label}\n\nРейсов пока нет."
    rates = [f["landing_rate"] for f in flights]
    avg = round(sum(rates) / len(rates))
    return (
        f"📊 <b>ОПЕРАЦИИ VA UP!</b>\n"
        f"📅 <i>{month_label}</i>\n\n"
        f"🛬 Рейсов выполнено: <b>{len(flights)}</b>\n"
        f"📐 Средняя посадка: <b>{avg} fpm</b>"
    )


def fmt_last(limit: int = 5) -> str:
    flights = db_last_flights(limit)
    if not flights:
        return "✈️ Рейсов пока не зафиксировано."
    lines = []
    for f in flights:
        rating, emoji = landing_rating(f["landing_rate"])
        dep = f["departure"] or "????"
        arr = f["arrival"]   or "????"
        fno = f["flight_no"] or "N/A"
        no_plan = (
            fno in ("N/A", "", "None") or
            dep in ("????", "", "None") or
            arr in ("????", "", "None")
        )
        if no_plan:
            lines.append(
                f"{emoji} <b>{f['pilot']}</b>\n"
                f"✈️ {f['aircraft'] or 'N/A'}\n"
                f"📊 {f['landing_rate']} fpm — <i>план не подан в RLM</i>"
            )
        else:
            lines.append(
                f"{emoji} <b>{fno}</b>\n"
                f"👨‍✈️ {f['pilot']}\n"
                f"🗺 {dep} → {arr}\n"
                f"📊 {f['landing_rate']} fpm"
            )
    return "✈️ <b>ПОСЛЕДНИЕ РЕЙСЫ</b>\n\n" + "\n\n".join(lines)


def fmt_top_landings(limit: int = 10) -> str:
    now = datetime.now()
    month_label = f"{MONTH_NAMES[now.month]} {now.year}"
    rows = db_top_landings(limit)
    if not rows:
        return f"🏆 <b>ЛУЧШИЕ ПОСАДКИ</b>\n📅 {month_label}\n\nПосадок пока нет."
    medals = ["🥇", "🥈", "🥉"]
    lines = []
    for i, row in enumerate(rows, 1):
        prefix = medals[i - 1] if i <= 3 else f"{i}."
        lines.append(f"{prefix} <b>{row['pilot']}</b> — {row['landing_rate']} fpm")
    return (
        f"🏆 <b>ЛУЧШИЕ ПОСАДКИ</b>\n"
        f"📅 <i>{month_label}</i>\n\n"
        + "\n".join(lines)
    )


def fmt_top_pilots() -> str:
    """Топ пилотов за 7 дней — SQL-запрос без загрузки всех рейсов в память."""
    rows = db_top_pilots_week(limit=10)
    if not rows:
        return "🏆 Рейсов за последние 7 дней нет."
    medals = {1: "🥇", 2: "🥈", 3: "🥉"}
    lines = [
        f"{medals.get(i, f'{i}.')} <b>{row['pilot']}</b> — {row['cnt']} рейс(ов)"
        for i, row in enumerate(rows, 1)
    ]
    return "🏆 <b>ТОП ПИЛОТЫ (7 дней)</b>\n\n" + "\n".join(lines)


def fmt_daily_economy() -> str:
    row = db_get_today_economy()
    if row:
        inc      = row["income"]
        exp      = row["expense"]
        net      = row["net"]
        detail   = row["detail"] or {}
        inc_cat  = detail.get("inc_cat", {})
        exp_cat  = detail.get("exp_cat", {})
        internal = detail.get("internal_volume", 0)
    else:
        txs = fsa_daily_transactions()
        if not txs:
            return "📊 Финансовых данных за сегодня пока нет."
        ag = _aggregate(txs)
        inc, exp, net = ag["inc"], ag["exp"], ag["net"]
        inc_cat, exp_cat = ag["inc_cat"], ag["exp_cat"]
        internal = ag.get("internal_volume", 0)

    # Баланс компании из кэша (не блокирует вебхук лишним HTTP-запросом)
    va_data = fsa_airline_data()
    budget  = va_data.get("budget") if va_data else None

    em   = _nem(net)
    sign = "+" if net >= 0 else ""

    budget_line = (
        f"💼 <b>Баланс компании: {budget:,.0f} v$</b>\n"
        if budget is not None else ""
    )

    msg = (
        f"📊 <b>ФИНАНСОВЫЙ ОТЧЁТ ЗА СЕГОДНЯ</b>\n\n"
        f"{budget_line}"
        f"\n"
        f"📈 <b>Оборот за сутки:</b>\n"
        f"💰 Доходы: <b>+{inc:,.0f} v$</b>\n"
        f"📉 Расходы: <b>-{exp:,.0f} v$</b>\n"
        f"{em} Итог: <b>{sign}{net:,.0f} v$</b>\n"
    )
    if internal:
        msg += f"↔️ <i>Внутр. переводы: {internal:,.0f} v$ (не учитываются)</i>\n"
    if inc_cat:
        msg += "\n🔝 <b>ОСНОВНЫЕ ДОХОДЫ:</b>\n"
        for reason, amount in _top(inc_cat):
            msg += f"   • {reason}: <b>+{amount:,.0f} v$</b>\n"
    if exp_cat:
        msg += "\n⚠️ <b>ОСНОВНЫЕ РАСХОДЫ:</b>\n"
        for reason, amount in _top(exp_cat):
            msg += f"   • {reason}: <b>-{amount:,.0f} v$</b>\n"
    return msg


def fmt_monthly_economy() -> str:
    now = datetime.now()
    month_label = f"{MONTH_NAMES[now.month]} {now.year}"

    def _fmt_net(v: int) -> str:
        sign = "+" if v >= 0 else ""
        return f"{sign}{v:,.0f} v$"

    va_data = fsa_airline_data()
    budget  = va_data.get("budget", 0) if va_data else None
    rows    = db_get_monthly_economy()

    budget_line = (
        f"💼 <b>Текущий баланс: {budget:,.0f} v$</b>\n"
        if budget is not None else
        f"💼 <b>Текущий баланс: недоступен</b>\n"
    )

    if not rows:
        return (
            f"🏆 <b>ФИНАНСОВЫЙ ДАЙДЖЕСТ ЗА МЕСЯЦ</b>\n"
            f"📅 <i>{month_label}</i>\n\n"
            f"{budget_line}\n"
            "📊 Данных по транзакциям пока нет.\n"
            "ℹ️ История собирается ежедневно в 23:50 UTC."
        )

    total_inc = sum(r["income"] for r in rows)
    total_exp = sum(r["expense"] for r in rows)
    month_net = total_inc - total_exp

    msg = (
        f"🏆 <b>ФИНАНСОВЫЙ ДАЙДЖЕСТ ЗА МЕСЯЦ</b>\n"
        f"📅 <i>{month_label}</i>\n\n"
        f"{budget_line}"
        f"\n"
        f"📈 <b>Динамика за месяц:</b>\n"
        f"💰 Доходы: <b>+{total_inc:,.0f} v$</b>\n"
        f"📉 Расходы: <b>-{total_exp:,.0f} v$</b>\n"
        f"{_nem(month_net)} Оборот: <b>{_fmt_net(month_net)}</b>\n\n"
        f"📊 Дней с данными: <b>{len(rows)}</b>"
    )
    if len(rows) > 1:
        best_row  = max(rows, key=lambda r: r["net"])
        worst_row = min(rows, key=lambda r: r["net"])
        msg += (
            f"\n🌟 Лучший день: <b>{best_row['day']}</b> ({_fmt_net(best_row['net'])})\n"
            f"⚠️ Худший день: <b>{worst_row['day']}</b> ({_fmt_net(worst_row['net'])})"
        )
    return msg


def fmt_active_flights() -> str:
    flights = fsa_active_flights()
    if not flights:
        return "✈️ В воздухе нет воздушных судов."
    lines = [
        f"✈️ <b>{f.get('number', 'N/A')}</b> "
        f"{f.get('departure', '???')} → {f.get('arrival', '???')} "
        f"[{f.get('passengers', 0)} пасс.]"
        for f in flights[:10]
    ]
    return f"🛫 <b>В ВОЗДУХЕ СЕЙЧАС ({len(flights)})</b>\n\n" + "\n".join(lines)


def fmt_va_info() -> str:
    data = fsa_airline_data()
    if not data:
        return "📊 Данные авиакомпании недоступны."
    return (
        f"🏢 <b>О КОМПАНИИ VA UP!</b>\n\n"
        f"📛 Название: <b>{data.get('name', 'N/A')}</b>\n"
        f"💰 Бюджет: <b>{data.get('budget', 0):,.0f} v$</b>\n"
        f"⭐ Репутация: <b>{data.get('reputation', 0)}</b>\n"
        f"📍 База: <b>{data.get('base', 'N/A')}</b>\n"
        f"✈️ Код: <b>{data.get('code', 'N/A')}</b>"
    )


def _fmt_contest_block(m: str) -> str:
    entries = db_contest_month(m)
    limit   = CONTEST_MONTHLY_LIMIT
    earned  = min(len(entries) * CONTEST_POINTS_PER_LANDING, limit)
    remain  = limit - earned
    label   = _month_label(m)

    if not entries:
        return f"📅 <b>{label}</b>\nПока нет кандидатов."

    lines = []
    for i, e in enumerate(entries, 1):
        in_limit = i <= limit
        pts_str  = f"⭐ +{CONTEST_POINTS_PER_LANDING} баллов" if in_limit else "— (лимит исчерпан)"
        lines.append(
            f"  {i}. <b>{e['pilot']}</b> "
            f"{e['flight_no']} {e['departure']}→{e['arrival']} "
            f"<b>{e['landing_rate']} fpm</b> {pts_str}\n"
            f"      🔍 Находится на проверке — результат в конце месяца"
        )
    return (
        f"📅 <b>{label}</b> — "
        f"Начислено: <b>{earned} балл.</b> | Остаток фонда: <b>{remain} балл.</b>\n"
        + "\n".join(lines)
    )


def fmt_contest(month: Optional[str] = None) -> str:
    slots  = CONTEST_MONTHLY_LIMIT // CONTEST_POINTS_PER_LANDING
    header = (
        f"🎯 <b>МАСТЕР ПОСАДКИ</b>\n"
        f"<i>Диапазон: от {CONTEST_RATE_MAX} до {CONTEST_RATE_MIN} fpm | "
        f"1 посадка = {CONTEST_POINTS_PER_LANDING} баллов | Фонд: {CONTEST_MONTHLY_LIMIT} баллов/мес</i>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
    )
    footer = "\n<i>⚠️ Финальная проверка (FSAirlines, штрафы, реал-тайм) — директор</i>"

    if month:
        return header + _fmt_contest_block(month) + footer

    current = datetime.now().strftime("%Y-%m")
    recent  = db_contest_recent_months(n=4)

    if current not in recent:
        months_to_show = [current] + recent[:3]
    else:
        months_to_show = [current] + [m for m in recent if m != current][:3]

    blocks = []
    for m in months_to_show:
        entries = db_contest_month(m)
        if not entries and m != current:
            continue
        blocks.append(_fmt_contest_block(m))

    return header + "\n\n".join(blocks) + footer


def fmt_operation() -> str:
    pilots     = db_op_all_pilots()
    total_legs = len(OPERATION_LEGS)
    max_pts    = OPERATION_MAX_POINTS

    header = (
        f"✈️ <b>ОПЕРАЦИЯ «{OPERATION_NAME}»</b>\n"
        f"<i>VTBS → SBGL • 13 этапов • {OPERATION_START} – {OPERATION_END}</i>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
    )

    if not pilots:
        return header + "Участников пока нет."

    lines  = []
    medals = {1: "🥇", 2: "🥈", 3: "🥉"}
    for i, p in enumerate(pilots, 1):
        prefix    = medals.get(i, f"{i}.")
        status    = "✅" if p["status"] == "finished" else ("💀" if p["status"] == "lost" else "🛫")
        leg_str   = f"Leg {p['current_leg']}/{total_legs}"
        pts_str   = f"{p['total_points']:,} очк."
        legs_done = max(0, p["current_leg"] - 1)
        if p["status"] == "finished":
            legs_done = total_legs
        bar        = "█" * legs_done + "░" * (total_legs - legs_done)
        aircraft   = f" ({p['aircraft']})" if p.get("aircraft") else ""
        ferry_str  = f" • Перегон #{p['ferry_num']}" if p.get("ferry_num", 1) > 1 else ""
        lines.append(
            f"{prefix} {status} <b>{p['pilot_name']}</b>{aircraft}{ferry_str}\n"
            f"   {bar} {pts_str} | {leg_str}"
        )

    footer = f"\n<i>Активен до {OPERATION_END}</i>"
    return header + "\n\n".join(lines) + footer


def fmt_operation_digest() -> str:
    """Еженедельный дайджест по ивенту."""
    pilots   = db_op_all_pilots()
    active   = [p for p in pilots if p["status"] == "active"]
    finished = [p for p in pilots if p["status"] == "finished"]
    now      = datetime.now()

    msg = (
        f"✈️ <b>ОПЕРАЦИЯ «{OPERATION_NAME}» — ДАЙДЖЕСТ</b>\n"
        f"<i>{now.strftime('%d.%m.%Y')}</i>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    )
    if finished:
        msg += f"🏁 <b>Финишировали ({len(finished)}):</b>\n"
        for p in finished:
            msg += f"  ✅ {p['pilot_name']} — {p['total_points']:,} очк.\n"
        msg += "\n"
    if active:
        msg += f"🛫 <b>В пути ({len(active)}):</b>\n"
        for p in active:
            next_leg = next(
                (f"Leg {n}: {dep}→{arr}" for n, dep, arr, _ in OPERATION_LEGS if n == p["current_leg"]),
                "завершён"
            )
            msg += f"  • {p['pilot_name']} | {next_leg} | {p['total_points']:,} очк.\n"
    if not pilots:
        msg += "Участников пока нет."
    return msg


def fmt_runway(icao: str) -> str:
    icao = icao.upper()
    url  = f"https://metar-taf.com/metar/{icao.lower()}"
    return (
        f"🛫 <b>METAR И ПОЛОСЫ — {icao}</b>\n\n"
        f"🔗 <a href='{url}'>Открыть METAR на metar-taf.com</a>\n\n"
        f"💡 <b>Как выбрать полосу:</b>\n"
        f"• Смотрим направление и скорость ветра в METAR\n"
        f"• Выбираем полосу с <b>встречным</b> ветром\n"
        f"• Курс полосы ≈ направление ветра (±30°)\n\n"
        f"📡 <i>METAR обновляется каждые 30 минут</i>"
    )
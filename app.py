import os
import sys
import time
import json
import logging
import threading
from logging.handlers import RotatingFileHandler
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse, urlencode, urlunparse, parse_qs

import psycopg2
import psycopg2.extras
import psycopg2.pool
import requests
from flask import Flask, request, jsonify
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ═══════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════

BOT_TOKEN  = os.environ.get("TG_BOT_TOKEN", "")
CHAT_ID    = os.environ.get("TG_CHAT_ID", "")
FSA_KEY    = os.environ.get("FSA_API_KEY", "")
FSA_VA_ID  = os.environ.get("FSA_VA_ID", "56177")
# Второй API — Профсоюз пилотов FSAirlines (добавить ключ когда договоритесь)
FSA_KEY2   = os.environ.get("FSA_API_KEY2", "")
FSA_VA_ID2 = os.environ.get("FSA_VA_ID2", "")
PORT       = int(os.environ.get("PORT", 10000))
HOSTNAME   = os.environ.get("RENDER_EXTERNAL_HOSTNAME", "fshub-bot.onrender.com")

WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "")
MAX_DB_FLIGHTS = int(os.environ.get("MAX_DB_FLIGHTS", "5000"))
DATABASE_URL   = os.environ.get("DATABASE_URL", "")
ADMIN_ID       = os.environ.get("ADMIN_TG_ID", "44859840")  # Telegram ID администратора

FSA_URL = "https://www.fsairlines.net/va_interface2.php"
TG_BASE = f"https://api.telegram.org/bot{BOT_TOKEN}"

# Задержки обогащения из FSAirlines (секунды)
# Увеличьте если FSAirlines не успевает обновить данные
FSA_ENRICH_DEPARTURE_DELAY = 90   # задержка для вылета
FSA_ENRICH_ARRIVAL_DELAY   = 180  # задержка для посадки (3 минуты)

if not BOT_TOKEN or not CHAT_ID:
    print("❌ TG_BOT_TOKEN or TG_CHAT_ID missing")
    sys.exit(1)

if not DATABASE_URL:
    print("❌ DATABASE_URL missing")
    sys.exit(1)

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

logger.info("Starting VA UP! PostgreSQL Edition")

# ═══════════════════════════════════════════════════════════════
# HTTP SESSION
# ═══════════════════════════════════════════════════════════════

session = requests.Session()
retry = Retry(
    total=3, read=3, connect=3, backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET", "POST"],
)
adapter = HTTPAdapter(max_retries=retry)
session.mount("https://", adapter)
session.mount("http://", adapter)

# ═══════════════════════════════════════════════════════════════
# DATABASE — PostgreSQL connection pool (с keepalives для Neon.tech)
# ═══════════════════════════════════════════════════════════════

_pool: Optional[psycopg2.pool.ThreadedConnectionPool] = None
_pool_lock = threading.Lock()


def _build_dsn(base_url: str) -> str:
    """
    Добавляет keepalive-параметры к DATABASE_URL.
    Neon.tech закрывает idle-соединения через ~5 минут —
    TCP keepalives не дают соединению «протухнуть».
    """
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
    """Проверяет, живо ли соединение."""
    try:
        conn.cursor().execute("SELECT 1")
        return True
    except Exception:
        return False


def get_conn():
    """Берёт соединение из пула; при мёртвом соединении пересоздаёт пул."""
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
    """
    Выполняет запрос с гарантированным возвратом соединения в пул.
    При OperationalError делает одну повторную попытку с новым соединением.
    """
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
                _pool = None  # пересоздадим пул на следующей попытке
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
            # Если соединение не было явно закрыто или возвращено — вернуть в пул
            if conn is not None:
                put_conn(conn)

    logger.error(f"Database error after {2} attempts: {last_error}")
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
            status        TEXT DEFAULT 'active',
            registered_at TIMESTAMPTZ DEFAULT NOW()
        )
        """
    )
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
    db_execute(
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
    db_execute(
        """
        DELETE FROM flights
        WHERE id NOT IN (
            SELECT id FROM flights ORDER BY id DESC LIMIT %s
        )
        """,
        (MAX_DB_FLIGHTS,),
    )


def db_update_flight_route(flight_id: str, flight_no: str, departure: str, arrival: str) -> None:
    """Обновляет маршрут рейса в БД после обогащения из FSAirlines."""
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
    """Рейсы за текущий календарный месяц."""
    return db_execute(
        """
        SELECT * FROM flights
        WHERE DATE_TRUNC('month', created_at) = DATE_TRUNC('month', NOW())
        ORDER BY id DESC
        """,
        fetch="all",
    ) or []


def db_top_landings(limit: int = 10) -> List:
    """Топ посадок за текущий календарный месяц."""
    return db_execute(
        """
        SELECT * FROM flights
        WHERE DATE_TRUNC('month', created_at) = DATE_TRUNC('month', NOW())
        ORDER BY ABS(landing_rate) ASC LIMIT %s
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


def db_get_monthly_economy(days: int = 30) -> List:
    """Финансовые данные за текущий календарный месяц."""
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
# OPERATION "ТИХИЙ ВЖУХ" — ДАННЫЕ
# ═══════════════════════════════════════════════════════════════

OPERATION_NAME       = "Тихий Вжух"
OPERATION_START      = "2026-06-10"
OPERATION_END        = "2026-08-31"
OPERATION_HARD_CRASH = 1200   # fpm — борт утерян, полный сброс
OPERATION_FAIL_RATE  = 600    # fpm — этап провален, 0 очков

# 13 этапов: (dep, arr, очки за успешную посадку)
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
    (13, "SGAS", "SBRJ",  810),
]
OPERATION_LEG_MAP = {(dep, arr): (num, pts) for num, dep, arr, pts in OPERATION_LEGS}

# Коэффициенты воздушных судов по ICAO-типу
OPERATION_AIRCRAFT_COEFF: Dict[str, float] = {
    "A310":  1.3,
    "A318":  1.3,
    "A319":  1.3,
    "A320":  1.1,
    "A321":  1.0,
    "A330":  1.0,
    "ATR72": 2.5,  # ATR-72
    "AT72":  2.5,  # альтернативный ICAO код ATR-72
    "B727":  2.0,
    "B736":  1.1,
    "B737":  1.1,
    "B738":  1.0,
    "B38M":  1.3,  # B738M
    "B773":  1.0,
    "CRJ7":  1.5,
    "MD11":  1.1,
}
OPERATION_VATSIM_BONUS = 50   # очков за полёт в VATSIM или IVAO

# Максимум очков без коэффициентов
OPERATION_MAX_POINTS = sum(pts for _, _, _, pts in OPERATION_LEGS)  # 13360


def op_get_aircraft_coeff(aircraft_icao: str) -> float:
    """Возвращает коэффициент ВС по ICAO-типу. По умолчанию 1.0."""
    if not aircraft_icao:
        return 1.0
    # Нормализуем: убираем пробелы, приводим к верхнему регистру
    key = aircraft_icao.upper().strip()
    # Прямое совпадение
    if key in OPERATION_AIRCRAFT_COEFF:
        return OPERATION_AIRCRAFT_COEFF[key]
    # Частичное совпадение (например "Boeing 737-800" → B738)
    for icao_key, coeff in OPERATION_AIRCRAFT_COEFF.items():
        if icao_key in key:
            return coeff
    return 1.0


def op_calc_points(base_pts: int, aircraft_icao: str, on_network: bool) -> tuple:
    """
    Рассчитывает итоговые очки за лег.
    Формула: round(base_pts × коэффициент_ВС) + бонус_сети
    Возвращает (итого, коэффициент, бонус_сети)
    """
    coeff       = op_get_aircraft_coeff(aircraft_icao)
    base_calc   = round(base_pts * coeff)
    net_bonus   = OPERATION_VATSIM_BONUS if on_network else 0
    total       = base_calc + net_bonus
    return total, coeff, net_bonus


def operation_is_active() -> bool:
    """Проверяет, активен ли ивент сейчас."""
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
    """Регистрирует пилота. Возвращает True если новый."""
    existing = db_op_get_pilot(pilot_name)
    if existing:
        return False
    db_execute(
        "INSERT INTO operation_pilots (pilot_name, aircraft) VALUES (%s, %s)",
        (pilot_name, aircraft),
    )
    return True


def db_op_add_leg(
    pilot_name: str, leg_num: int, dep: str, arr: str,
    landing_rate: int, points: int, flight_id: str, report_url: str,
    base_points: int = 0, coeff: float = 1.0,
    net_bonus: int = 0, on_network: bool = False,
) -> None:
    """Записывает выполненный этап."""
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


def db_op_admin_set(pilot_name: str, leg_num: int, points_delta: int) -> None:
    """Ручная корректировка очков администратором."""
    pilot = db_op_get_pilot(pilot_name)
    if not pilot:
        return
    new_points = max(0, pilot["total_points"] + points_delta)
    db_execute(
        "UPDATE operation_pilots SET total_points = %s WHERE pilot_name = %s",
        (new_points, pilot_name),
    )


def db_op_reset_pilot(pilot_name: str) -> None:
    """Полный сброс прогресса пилота (потеря борта)."""
    db_execute(
        """
        UPDATE operation_pilots
        SET current_leg = 1, total_points = 0, status = 'active'
        WHERE pilot_name = %s
        """,
        (pilot_name,),
    )


def db_op_check_daily_limit(pilot_name: str) -> tuple:
    """
    Проверяет лимит 2 лега в календарные сутки UTC.
    Счётчик сбрасывается в 00:00 UTC каждый день.

    Возвращает (allowed: bool, legs_today: int)
    """
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
    allowed = count < 2
    return allowed, count


# ═══════════════════════════════════════════════════════════════
# DB HELPERS — CONTEST
# ═══════════════════════════════════════════════════════════════

CONTEST_POINTS_PER_LANDING = 100  # баллов за посадку
CONTEST_MONTHLY_LIMIT      = 1000 # максимум баллов в месяц
CONTEST_RATE_MIN          = -30   # fpm (не мягче)
CONTEST_RATE_MAX          = -10   # fpm (не жёстче)


def is_contest_landing(rate: int) -> bool:
    """Проверяет, попадает ли посадка в диапазон конкурса."""
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
    """Возвращает список месяцев у которых есть записи — текущий + до n-1 предыдущих."""
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


def tg_send_with_cancel(text: str, chat_id) -> bool:
    """Отправляет сообщение с inline-кнопкой Отмена."""
    try:
        r = session.post(
            f"{TG_BASE}/sendMessage",
            json={
                "chat_id": str(chat_id),
                "text": text[:4096],
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
                "reply_markup": {
                    "inline_keyboard": [[
                        {"text": "❌ Отмена", "callback_data": "runway_cancel"}
                    ]]
                },
            },
            timeout=20,
        )
        if r.status_code != 200:
            logger.warning(f"tg_send_with_cancel failed: {r.text}")
            return False
        return True
    except Exception as e:
        logger.exception(f"tg_send_with_cancel error: {e}")
        return False


def tg_answer_callback(callback_query_id: str, text: str = "") -> None:
    """Закрывает «часики» на кнопке после нажатия."""
    try:
        session.post(
            f"{TG_BASE}/answerCallbackQuery",
            json={"callback_query_id": callback_query_id, "text": text},
            timeout=10,
        )
    except Exception as e:
        logger.exception(f"answerCallbackQuery error: {e}")


def tg_edit_message(chat_id, message_id: int, text: str) -> None:
    """Редактирует сообщение бота (убирает кнопку после отмены)."""
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


def tg_send_menu(chat_id) -> bool:
    """Отправляет главное меню с inline-кнопками."""
    try:
        r = session.post(
            f"{TG_BASE}/sendMessage",
            json={
                "chat_id": str(chat_id),
                "text": "✈️ <b>VA UP! Operations Panel</b>\n\nВыберите раздел:",
                "parse_mode": "HTML",
                "reply_markup": {
                    "inline_keyboard": [
                        [
                            {"text": "📊 Статистика",   "callback_data": "cmd_stats"},
                            {"text": "✈️ Последние рейсы", "callback_data": "cmd_last"},
                        ],
                        [
                            {"text": "🏆 Топ пилоты",   "callback_data": "cmd_top"},
                            {"text": "🛬 Топ посадки",  "callback_data": "cmd_top_landing"},
                        ],
                        [
                            {"text": "💰 Финансы",      "callback_data": "cmd_economy"},
                            {"text": "📅 За месяц",     "callback_data": "cmd_monthly"},
                        ],
                        [
                            {"text": "📡 Онлайн",       "callback_data": "cmd_live"},
                            {"text": "🏢 О компании",   "callback_data": "cmd_va"},
                        ],
                        [
                            {"text": "🛫 Полосы (Runway)", "callback_data": "cmd_runway"},
                            {"text": "🎯 Мастер Посадки",  "callback_data": "cmd_contest"},
                        ],
                        [
                            {"text": "✈️ Операция «Тихий Вжух»", "callback_data": "cmd_operation"},
                        ],
                    ]
                },
            },
            timeout=20,
        )
        if r.status_code != 200:
            logger.warning(f"tg_send_menu failed: {r.text}")
            return False
        return True
    except Exception as e:
        logger.exception(f"tg_send_menu error: {e}")
        return False


# Маппинг callback_data команд меню → функции-форматтеры
MENU_CALLBACKS: Dict[str, callable] = {}  # заполняется после определения форматтеров


def handle_callback_query(cq: Dict) -> None:
    """Обрабатывает нажатие inline-кнопки."""
    cq_id      = cq.get("id", "")
    data       = cq.get("data", "")
    chat_id    = str(cq.get("message", {}).get("chat", {}).get("id", ""))
    message_id = cq.get("message", {}).get("message_id")

    if data == "runway_cancel":
        with _awaiting_lock:
            _awaiting_icao.pop(chat_id, None)
        tg_answer_callback(cq_id, "Отменено")
        if message_id:
            tg_edit_message(chat_id, message_id, "✅ Запрос полосы отменён.")
        logger.info(f"Runway dialog cancelled by chat={chat_id}")
        return

    if data == "cmd_runway":
        tg_answer_callback(cq_id)
        with _awaiting_lock:
            _awaiting_icao[chat_id] = True
        tg_send_with_cancel(
            "✈️ Введите ICAO-код аэропорта:\n<i>Например: UHWW, UUEE, EGLL</i>",
            chat_id,
        )
        return

    if data in MENU_CALLBACKS:
        tg_answer_callback(cq_id)
        try:
            result = MENU_CALLBACKS[data]()
            tg_send(result, chat_id)
        except Exception as e:
            logger.exception(f"Menu callback {data} failed: {e}")
            tg_send("⚠️ Ошибка при выполнении команды.", chat_id)
        return

    tg_answer_callback(cq_id)


# ═══════════════════════════════════════════════════════════════
# FSA API
# ═══════════════════════════════════════════════════════════════

def fsa_call(function: str, extra: Optional[Dict] = None, key: str = "", va_id: str = ""):
    """Вызов FSAirlines API. Использует UP! по умолчанию."""
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
    """Вызов FSAirlines API Профсоюза (второй ключ)."""
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


def fsa_airline_data() -> Optional[Dict]:
    data = fsa_call("getAirlineData")
    if isinstance(data, list) and data:
        return data[0]
    return data if isinstance(data, dict) else None


def get_flight_profit(report_id: int) -> Optional[int]:
    # TODO: найти маппинг flight_id (FSHub) → report_id (FSAirlines)
    return None


# ─── FSAirlines: кэш пилотов и обогащение данных вылета ────────

# Кэш: "Имя Фамилия" → pilot_id в FSAirlines
# Обновляется при первом обращении и живёт до перезапуска сервиса
_fsa_pilot_cache: Dict[str, int] = {}
_fsa_pilot_cache_lock = threading.Lock()
_fsa_pilot_cache_ts: float = 0  # время последнего обновления

# Кэш данных вылета: pilot_name → {dep, arr, flight_no, ts}
# Используется при посадке если план FSHub пустой
_departure_cache: Dict[str, Dict] = {}
_departure_cache_lock = threading.Lock()


def fsa_refresh_pilot_cache() -> None:
    """Загружает список пилотов FSAirlines в кэш."""
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
    """Загружает список пилотов Профсоюза FSAirlines в кэш."""
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
    """
    Возвращает pilot_id FSAirlines по полному имени.
    Сначала ищет в UP!, затем в Профсоюзе.
    """
    if not _fsa_pilot_cache or (time.time() - _fsa_pilot_cache_ts) > 3600:
        fsa_refresh_pilot_cache()
        if FSA_KEY2 and FSA_VA_ID2:
            fsa_refresh_pilot_cache2()
    with _fsa_pilot_cache_lock:
        return _fsa_pilot_cache.get(name)


def fsa_get_pilot_status(pilot_id: int) -> Optional[Dict]:
    """Возвращает текущий статус пилота из FSAirlines."""
    data = fsa_call("getPilotStatus", {"pilot_id": pilot_id})
    if isinstance(data, list) and data:
        return data[0]
    return data if isinstance(data, dict) else None


def _is_plan_empty(plan: Dict) -> bool:
    """Проверяет, пустой ли план полёта из FSHub."""
    flight_no = plan.get("flight_no", "")
    departure = plan.get("departure", "")
    arrival   = plan.get("arrival", "")
    return (
        not flight_no or flight_no in ("N/A", "", "None") or
        not departure or departure in ("????", "", "None") or
        not arrival   or arrival   in ("????", "", "None")
    )


def _is_valid_icao(icao: str) -> bool:
    """
    Базовая валидация ICAO: 4 символа, начинается с буквы,
    только буквы и цифры. Защита от мусорных данных FSAirlines.
    """
    if not icao or len(icao) != 4:
        return False
    if not icao[0].isalpha():
        return False
    return icao.isalnum()


def _enrich_departure_from_fsa(
    message_id: int,
    chat_id_str: str,
    pilot_name: str,
    aircraft_name: str,
    delay: int = 90,
) -> None:
    """
    Запускается в отдельном треде через `delay` секунд после вылета.
    Запрашивает FSAirlines, получает маршрут и редактирует сообщение бота.
    """
    time.sleep(delay)
    logger.info(f"[Enrich] Запрос FSAirlines для пилота '{pilot_name}' (delay={delay}s)")

    pilot_id = fsa_get_pilot_id(pilot_name)
    if not pilot_id:
        logger.warning(f"[Enrich] Пилот '{pilot_name}' не найден в FSAirlines")
        tg_edit_message(
            chat_id_str, message_id,
            f"🛫 <b>ВЫЛЕТ ПОДТВЕРЖДЁН — CLEARED FOR TAKEOFF</b>\n\n"
            f"👨‍✈️ Captain: <b>{pilot_name}</b>\n"
            f"✈️ Aircraft: <b>{aircraft_name}</b>\n\n"
            f"⚠️ <i>Маршрут не найден — пилот не активировал план в RLM Client</i>\n"
            f"✈️ <i>Желаем попутного ветра и мягкой посадки!</i>"
        )
        return

    status = fsa_get_pilot_status(pilot_id)
    if not status:
        logger.warning(f"[Enrich] Статус пилота {pilot_id} не получен из FSAirlines")
        return

    dep       = status.get("departure", "????")
    arr       = status.get("arrival",   "????")
    flight_no = status.get("route_id",  "")  # route_id как запасной вариант

    # Попробуем получить номер рейса из активных рейсов FSAirlines
    active = fsa_active_flights()
    flt_no = "N/A"
    for f in active:
        if str(f.get("user_id")) == str(pilot_id):
            flt_no = f.get("number", "N/A")
            break

    logger.info(f"[Enrich] Получены данные FSA: {dep}→{arr} flight={flt_no}")

    # Сохраняем обогащённые данные в кэш для использования при посадке
    if _is_valid_icao(dep) and _is_valid_icao(arr):
        with _departure_cache_lock:
            _departure_cache[pilot_name] = {
                "dep":       dep,
                "arr":       arr,
                "flight_no": flt_no,
                "ts":        time.time(),
                "from_fsa":  True,
            }

    tg_edit_message(
        chat_id_str, message_id,
        f"🛫 <b>ВЫЛЕТ ПОДТВЕРЖДЁН — CLEARED FOR TAKEOFF</b>\n\n"
        f"👨‍✈️ Captain: <b>{pilot_name}</b>\n"
        f"🆔 Flight: <b>{flt_no}</b>\n"
        f"🗺 Route: <b>{dep} → {arr}</b>\n"
        f"✈️ Aircraft: <b>{aircraft_name}</b>\n\n"
        f"✈️ <i>Желаем попутного ветра и мягкой посадки!</i>"
    )


def fsa_get_recent_report(pilot_id: int, arrival_time: str) -> Optional[Dict]:
    """
    Ищет последний отчёт пилота в FSAirlines близкий по времени к arrival_time.
    arrival_time — строка ISO формата из _data.arrival_at.
    """
    data = fsa_call("getFlightReports", {"pilot_id": pilot_id, "count": 5})
    if not isinstance(data, list):
        return None
    try:
        arr_dt = datetime.fromisoformat(arrival_time.replace("Z", "+00:00"))
    except Exception:
        arr_dt = None

    for report in data:
        if arr_dt:
            # Сравниваем по timestamp рейса (поле ts)
            try:
                rep_dt = _safe_ts(report.get("ts"))
                if rep_dt and abs((arr_dt.replace(tzinfo=None) - rep_dt).total_seconds()) < 600:
                    return report
            except Exception:
                pass
        else:
            # Без времени — берём первый отчёт
            return report
    return None


def _enrich_completed_from_fsa(
    message_id: int,
    chat_id_str: str,
    pilot_name: str,
    aircraft_name: str,
    airport_name: str,
    rate: int,
    rating: str,
    emoji: str,
    extras: str,
    flight_link: str,
    arrival_time: str,
    delay: int = 60,
    flight_id_for_db: Optional[str] = None,
) -> None:
    """
    Запускается в отдельном треде через delay секунд после посадки.
    Запрашивает FSAirlines, получает маршрут и редактирует сообщение бота.
    """
    time.sleep(delay)
    logger.info(f"[Enrich arrival] Запрос FSAirlines для пилота '{pilot_name}'")

    pilot_id = fsa_get_pilot_id(pilot_name)
    dep, arr, flight_no = "????", "????", "N/A"

    if pilot_id:
        report = fsa_get_recent_report(pilot_id, arrival_time)
        if report:
            dep       = report.get("dep", "????") or "????"
            arr       = report.get("arr", "????") or "????"
            flight_no = report.get("number", "N/A") or "N/A"
            logger.info(f"[Enrich arrival] Найден маршрут FSA: {dep}→{arr} {flight_no}")

            # Обновляем БД если получили валидные данные
            if _is_valid_icao(dep) and _is_valid_icao(arr) and flight_id_for_db:
                db_update_flight_route(flight_id_for_db, flight_no, dep, arr)
                logger.info(f"[Enrich arrival] БД обновлена для flight_id={flight_id_for_db}")
        else:
            logger.warning(f"[Enrich arrival] Отчёт FSA не найден для пилота {pilot_id}")
    else:
        logger.warning(f"[Enrich arrival] Пилот '{pilot_name}' не найден в FSAirlines")

    route_line = (
        f"🗺 Route: <b>{dep} → {arr}</b>\n"
        if dep != "????" else
        f"🗺 Route: <i>не определён</i>\n"
    )

    tg_edit_message(
        chat_id_str, message_id,
        f"🛬 <b>ПОСАДКА ВЫПОЛНЕНА — TOUCHDOWN</b> {emoji}\n\n"
        f"👨‍✈️ Captain: <b>{pilot_name}</b>\n"
        f"🆔 Flight: <b>{flight_no}</b>\n"
        f"{route_line}"
        f"📍 Airport: <b>{airport_name}</b>\n"
        f"✈️ Aircraft: <b>{aircraft_name}</b>\n"
        f"📊 Landing Rate: <b>{rate} fpm</b> — {rating}"
        f"{extras}"
        f"{flight_link}"
    )


# ═══════════════════════════════════════════════════════════════
# FINANCIAL AGGREGATION
# ═══════════════════════════════════════════════════════════════

# Транзакции которые являются внутренними переводами между флотами.
# Они всегда парные (приход = расход) и не влияют на реальный Net,
# но засоряют отчёт — исключаем из отображения.
INTERNAL_TRANSFER_REASONS = {
    "Fleet Money Transfer",
}


def _aggregate(transactions: List[Dict]) -> Dict:
    inc, exp = 0.0, 0.0
    inc_cat: Dict[str, float] = {}
    exp_cat: Dict[str, float] = {}
    internal_volume = 0.0  # объём внутренних переводов (для справки)

    for t in transactions:
        try:
            v = float(t.get("value", 0))
        except (ValueError, TypeError):
            continue
        r = t.get("reason") or "Other"

        # Внутренние переводы: учитываем в net (они нейтральны),
        # но НЕ показываем в топах доходов/расходов
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
    if rate < -1000: return "UNSAFE LANDING",  "🔴"
    if rate < -600:  return "HARD LANDING",    "🟠"
    if rate < -500:  return "FIRM LANDING",    "🟡"
    if rate < -350:  return "STABLE LANDING",  "🟢"
    if rate < -50:   return "SMOOTH LANDING",  "✅"
    return                  "BUTTER LANDING",  "🧈✨"


# ═══════════════════════════════════════════════════════════════
# COMMAND FORMATTERS
# ═══════════════════════════════════════════════════════════════

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
            # Маршрут неизвестен — не показываем ???? мусор
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
    flights = db_all_flights()
    if not flights:
        return "🏆 Данных о рейсах пока нет."
    week_ago = datetime.utcnow() - timedelta(days=7)
    counts: Dict[str, int] = {}
    for f in flights:
        try:
            ca = f["created_at"]
            if isinstance(ca, str):
                ca = datetime.fromisoformat(ca)
            if ca.replace(tzinfo=None) >= week_ago:
                counts[f["pilot"]] = counts.get(f["pilot"], 0) + 1
        except Exception:
            pass
    if not counts:
        return "🏆 Рейсов за последние 7 дней нет."
    sorted_pilots = sorted(counts.items(), key=lambda x: x[1], reverse=True)[:10]
    medals = {1: "🥇", 2: "🥈", 3: "🥉"}
    lines = [
        f"{medals.get(i, f'{i}.')} <b>{pilot}</b> — {n} рейс(ов)"
        for i, (pilot, n) in enumerate(sorted_pilots, 1)
    ]
    return "🏆 <b>ТОП ПИЛОТЫ (7 дней)</b>\n\n" + "\n".join(lines)


def fmt_daily_economy() -> str:
    row = db_get_today_economy()
    if row:
        inc = row["income"]
        exp = row["expense"]
        net = row["net"]
        detail = row["detail"] or {}
        inc_cat = detail.get("inc_cat", {})
        exp_cat = detail.get("exp_cat", {})
        internal = detail.get("internal_volume", 0)
    else:
        txs = fsa_daily_transactions()
        if not txs:
            return "📊 Финансовых данных за сегодня пока нет."
        ag = _aggregate(txs)
        inc, exp, net = ag["inc"], ag["exp"], ag["net"]
        inc_cat, exp_cat = ag["inc_cat"], ag["exp_cat"]
        internal = ag.get("internal_volume", 0)

    # Текущий баланс компании из FSAirlines
    va_data = fsa_airline_data()
    budget = va_data.get("budget") if va_data else None

    em = _nem(net)
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

    # Форматирование знака без дублирования
    def _fmt_net(v: int) -> str:
        sign = "+" if v >= 0 else ""
        return f"{sign}{v:,.0f} v$"

    # Текущий баланс компании из FSAirlines
    va_data = fsa_airline_data()
    budget = va_data.get("budget", 0) if va_data else None

    # Месячная динамика из БД (снимки 23:50 каждый день)
    rows = db_get_monthly_economy()

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

    # Лучший/худший день только если данных больше одного дня
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


def _fmt_contest_block(m: str) -> str:
    """Форматирует блок одного месяца для вывода в /contest."""
    entries = db_contest_month(m)
    limit   = CONTEST_MONTHLY_LIMIT
    earned  = min(len(entries) * CONTEST_POINTS_PER_LANDING, limit)
    remain  = limit - earned
    label   = _month_label(m)

    if not entries:
        return (
            f"📅 <b>{label}</b>\n"
            f"Пока нет кандидатов."
        )

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
    """
    Без аргумента — текущий месяц + до 3 предыдущих с данными.
    С аргументом (YYYY-MM) — только указанный месяц.
    """
    slots = CONTEST_MONTHLY_LIMIT // CONTEST_POINTS_PER_LANDING
    header = (
        f"🎯 <b>МАСТЕР ПОСАДКИ</b>\n"
        f"<i>Диапазон: от {CONTEST_RATE_MAX} до {CONTEST_RATE_MIN} fpm | "
        f"1 посадка = {CONTEST_POINTS_PER_LANDING} баллов | Фонд: {CONTEST_MONTHLY_LIMIT} баллов/мес</i>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
    )
    footer = (
        f"\n<i>⚠️ Финальная проверка (FSAirlines, штрафы, реал-тайм) — директор</i>"
    )

    # Конкретный месяц по запросу
    if month:
        return header + _fmt_contest_block(month) + footer

    # Текущий + до 3 предыдущих месяцев с данными
    current = datetime.now().strftime("%Y-%m")
    recent  = db_contest_recent_months(n=4)

    # Убедимся что текущий месяц всегда первый, даже если данных нет
    if current not in recent:
        months_to_show = [current] + recent[:3]
    else:
        # Текущий в начале, остальные по убыванию
        months_to_show = [current] + [m for m in recent if m != current][:3]

    blocks = []
    for m in months_to_show:
        entries = db_contest_month(m)
        # Прошлые месяцы без данных — пропускаем
        if not entries and m != current:
            continue
        blocks.append(_fmt_contest_block(m))

    return header + "\n\n".join(blocks) + footer


def fmt_operation() -> str:
    """Таблица лидеров и прогресс участников ивента."""
    pilots = db_op_all_pilots()
    total_legs = len(OPERATION_LEGS)
    max_pts    = OPERATION_MAX_POINTS

    header = (
        f"✈️ <b>ОПЕРАЦИЯ «{OPERATION_NAME}»</b>\n"
        f"<i>VTBS → SBRJ • 13 этапов • {OPERATION_START} – {OPERATION_END}</i>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
    )

    if not pilots:
        return header + "Участников пока нет."

    lines = []
    medals = {1: "🥇", 2: "🥈", 3: "🥉"}
    for i, p in enumerate(pilots, 1):
        prefix  = medals.get(i, f"{i}.")
        status  = "✅" if p["status"] == "finished" else ("💀" if p["status"] == "lost" else "🛫")
        leg_str = f"Leg {p['current_leg']}/{total_legs}"
        pts_str = f"{p['total_points']:,} очк."
        bar_len = 10
        filled  = round(p["total_points"] / max_pts * bar_len) if max_pts else 0
        bar     = "█" * filled + "░" * (bar_len - filled)
        aircraft = f" ({p['aircraft']})" if p.get("aircraft") else ""
        lines.append(
            f"{prefix} {status} <b>{p['pilot_name']}</b>{aircraft}\n"
            f"   {bar} {pts_str} | {leg_str}"
        )

    footer = (
        f"\n<i>Макс. очков: {max_pts:,} | "
        f"Активен до {OPERATION_END}</i>"
    )
    return header + "\n\n".join(lines) + footer


def fmt_operation_digest() -> str:
    """Еженедельный дайджест по ивенту."""
    pilots = db_op_all_pilots()
    active = [p for p in pilots if p["status"] == "active"]
    finished = [p for p in pilots if p["status"] == "finished"]

    now = datetime.now()
    week_label = f"{now.strftime('%d.%m')} — еженедельный отчёт"

    msg = (
        f"✈️ <b>ОПЕРАЦИЯ «{OPERATION_NAME}» — ДАЙДЖЕСТ</b>\n"
        f"<i>{week_label}</i>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    )

    if finished:
        msg += f"🏁 <b>Завершили маршрут ({len(finished)}):</b>\n"
        for p in finished:
            msg += f"  ✅ {p['pilot_name']} — {p['total_points']:,} очк.\n"
        msg += "\n"

    if active:
        msg += f"🛫 <b>В пути ({len(active)}):</b>\n"
        for p in active:
            leg_info = next((f"{dep}→{arr}" for n, dep, arr, _ in OPERATION_LEGS if n == p["current_leg"]), "—")
            msg += f"  • {p['pilot_name']} — Leg {p['current_leg']}: {leg_info} | {p['total_points']:,} очк.\n"

    if not pilots:
        msg += "Участников пока нет."

    return msg


def fmt_operation_digest() -> str:
    """Еженедельный дайджест по ивенту."""
    pilots = db_op_all_pilots()
    active   = [p for p in pilots if p["status"] == "active"]
    finished = [p for p in pilots if p["status"] == "finished"]
    now = datetime.now()

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
    """Возвращает METAR и рекомендации по полосам через metar-taf.com."""
    icao = icao.upper()
    url = f"https://metar-taf.com/metar/{icao.lower()}"
    return (
        f"🛫 <b>METAR И ПОЛОСЫ — {icao}</b>\n\n"
        f"🔗 <a href='{url}'>Открыть METAR на metar-taf.com</a>\n\n"
        f"💡 <b>Как выбрать полосу:</b>\n"
        f"• Смотрим направление и скорость ветра в METAR\n"
        f"• Выбираем полосу с <b>встречным</b> ветром\n"
        f"• Курс полосы ≈ направление ветра (±30°)\n\n"
        f"📡 <i>METAR обновляется каждые 30 минут</i>"
    )


# ═══════════════════════════════════════════════════════════════
# FSHUB EVENT HANDLERS
# ═══════════════════════════════════════════════════════════════

_processed_events = set()
_processed_lock = threading.Lock()

# Состояние диалога: chat_id -> "awaiting_icao"
# Используется для /runway без аргумента — бот спрашивает ICAO и ждёт ответа
_awaiting_icao: Dict[str, bool] = {}
_awaiting_lock = threading.Lock()


def is_duplicate_event(event_id: str, event_type: str) -> bool:
    key = f"{event_type}:{event_id}"
    with _processed_lock:
        if key in _processed_events:
            return True
        _processed_events.add(key)
        if len(_processed_events) > 1000:
            _processed_events.clear()
        return False


def handle_departure(data: Dict):
    d = data.get("_data") or {}
    flight_id = str(d.get("id", ""))

    if flight_id and is_duplicate_event(flight_id, "departure"):
        logger.info(f"Пропуск дублирующего departure для рейса {flight_id}")
        return

    user     = d.get("user") or {}
    plan     = d.get("plan") or {}
    aircraft = d.get("aircraft") or {}

    pilot_name   = user.get("name", "Unknown")
    aircraft_name = aircraft.get("icao_name", "N/A")

    if not _is_plan_empty(plan):
        # Данные полные — сохраняем в кэш и отправляем сразу
        logger.info(f"[Departure] Полный план от FSHub для '{pilot_name}'")
        with _departure_cache_lock:
            _departure_cache[pilot_name] = {
                "dep":       plan.get("departure", "????"),
                "arr":       plan.get("arrival", "????"),
                "flight_no": plan.get("flight_no", "N/A"),
                "ts":        time.time(),
            }
        tg_send(
            f"🛫 <b>ВЫЛЕТ ПОДТВЕРЖДЁН — CLEARED FOR TAKEOFF</b>\n\n"
            f"👨‍✈️ Captain: <b>{pilot_name}</b>\n"
            f"🆔 Flight: <b>{plan.get('flight_no')}</b>\n"
            f"🗺 Route: <b>{plan.get('departure')} → {plan.get('arrival')}</b>\n"
            f"✈️ Aircraft: <b>{aircraft_name}</b>\n\n"
            f"✈️ <i>Желаем попутного ветра и мягкой посадки!</i>"
        )
    else:
        # Пустой план — отправляем краткое сообщение и запускаем обогащение
        logger.info(f"[Departure] Пустой план от FSHub для '{pilot_name}', запускаю обогащение через 90с")

        # Краткое сообщение — сохраняем message_id для последующего редактирования
        r = session.post(
            f"{TG_BASE}/sendMessage",
            json={
                "chat_id": CHAT_ID,
                "text": (
                    f"🛫 <b>ВЫЛЕТ ПОДТВЕРЖДЁН — CLEARED FOR TAKEOFF</b>\n\n"
                    f"👨‍✈️ Captain: <b>{pilot_name}</b>\n"
                    f"✈️ Aircraft: <b>{aircraft_name}</b>\n"
                    f"🗺 Route: <b>⏳ Загружаю маршрут...</b>\n\n"
                    f"✈️ <i>Желаем попутного ветра и мягкой посадки!</i>"
                ),
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
            timeout=20,
        )
        if r.status_code == 200:
            message_id = r.json().get("result", {}).get("message_id")
            if message_id:
                threading.Thread(
                    target=_enrich_departure_from_fsa,
                    args=(message_id, CHAT_ID, pilot_name, aircraft_name, FSA_ENRICH_DEPARTURE_DELAY),
                    daemon=True,
                ).start()
        else:
            logger.warning(f"[Departure] Не удалось отправить сообщение: {r.text}")


def handle_completed(data: Dict):
    """
    flight.completed — приходит после обработки рейса FSHub.
    Структура: _data.id = ID отчёта (правильный для URL),
               _data.arrival = данные о посадке,
               _data.departure = данные о вылете,
               _data.plan = план полёта.
    """
    d = data.get("_data") or {}
    report_id = str(d.get("id", ""))

    if report_id and is_duplicate_event(report_id, "completed"):
        logger.info(f"Пропуск дублирующего completed для рейса {report_id}")
        return

    # Данные о посадке — в блоке arrival
    arrival  = d.get("arrival") or {}
    plan     = d.get("plan") or {}
    user     = arrival.get("user") or d.get("user") or {}
    aircraft = arrival.get("aircraft") or d.get("aircraft") or {}
    airport  = arrival.get("airport") or {}

    # flight_no может быть в plan.callsign (flight.completed) или plan.flight_no
    flight_no = plan.get("callsign") or plan.get("flight_no", "N/A")

    # Маршрут: в flight.completed plan использует icao_dep/icao_arr
    dep = plan.get("icao_dep") or plan.get("departure", "????")
    arr = plan.get("icao_arr") or plan.get("arrival", "????")

    rate = int(arrival.get("landing_rate", 0))
    rating, emoji = landing_rating(rate)

    # Дополнительные данные рейса
    distance_nm = (d.get("distance") or {}).get("nm")
    fuel_burnt  = d.get("fuel_burnt")
    max_alt     = (d.get("max") or {}).get("alt")

    db_add_flight(
        flight_id=report_id or None,
        pilot=user.get("name", "Unknown"),
        flight_no=flight_no,
        departure=dep,
        arrival=arr,
        aircraft=aircraft.get("icao_name", "Unknown"),
        landing_rate=rate,
        profit=None,
    )

    flight_link = (
        f"\n🔗 <a href='https://fshub.io/flight/{report_id}/report'>Открыть отчёт о рейсе</a>"
        if report_id else ""
    )

    extras = ""
    if distance_nm:
        extras += f"\n📏 Distance: <b>{distance_nm} nm</b>"
    if fuel_burnt:
        extras += f"\n⛽ Fuel burnt: <b>{fuel_burnt} kg</b>"
    if max_alt:
        extras += f"\n🏔 Max altitude: <b>{max_alt:,} ft</b>"

    pilot_name    = user.get("name", "Unknown")
    aircraft_name = aircraft.get("icao_name", "Unknown")
    airport_name  = airport.get("name", "Unknown")
    arrival_time  = d.get("arrival_at", "") or ""
    plan_empty    = _is_plan_empty({"flight_no": flight_no, "departure": dep, "arrival": arr})

    msg_text = (
        f"🛬 <b>ПОСАДКА ВЫПОЛНЕНА — TOUCHDOWN</b> {emoji}\n\n"
        f"👨‍✈️ Captain: <b>{pilot_name}</b>\n"
        f"🆔 Flight: <b>{flight_no}</b>\n"
        f"🗺 Route: <b>{'⏳ Загружаю маршрут...' if plan_empty else f'{dep} → {arr}'}</b>\n"
        f"📍 Airport: <b>{airport_name}</b>\n"
        f"✈️ Aircraft: <b>{aircraft_name}</b>\n"
        f"📊 Landing Rate: <b>{rate} fpm</b> — {rating}"
        f"{extras}"
        f"{flight_link}"
    )

    if plan_empty and FSA_KEY:
        # Проверяем кэш данных вылета
        airport_icao = airport.get("icao", "").upper()
        cached = None
        with _departure_cache_lock:
            entry = _departure_cache.get(pilot_name)
            # Кэш актуален если не старше 24 часов
            if entry and (time.time() - entry.get("ts", 0)) < 86400:
                cached = entry

        if cached:
            cached_arr = cached.get("arr", "????").upper()
            cached_dep = cached.get("dep", "????")
            cached_fno = cached.get("flight_no", "N/A")

            # Валидируем данные из кэша
            if not _is_valid_icao(cached_dep) or not _is_valid_icao(cached_arr):
                logger.warning(f"[Completed] Невалидные ICAO в кэше: {cached_dep}→{cached_arr}, игнорируем")
                cached = None

        if cached:
            # Сравниваем аэропорт прилёта из кэша с фактическим из FSHub
            if airport_icao and cached_arr != airport_icao and _is_valid_icao(airport_icao):
                # Запасной аэропорт — ждём 15 минут пока пилот завершит рейс в FSAirlines
                logger.info(
                    f"[Completed] Запасной аэропорт для '{pilot_name}': "
                    f"план={cached_arr}, факт={airport_icao} — жду 15 мин"
                )
                # Отправляем с placeholder
                msg_placeholder = msg_text  # уже содержит ⏳
                r = session.post(
                    f"{TG_BASE}/sendMessage",
                    json={
                        "chat_id": CHAT_ID,
                        "text": msg_placeholder,
                        "parse_mode": "HTML",
                        "disable_web_page_preview": True,
                    },
                    timeout=20,
                )
                if r.status_code == 200:
                    message_id = r.json().get("result", {}).get("message_id")
                    if message_id:
                        threading.Thread(
                            target=_enrich_completed_from_fsa,
                            args=(
                                message_id, CHAT_ID, pilot_name, aircraft_name,
                                airport_name, rate, rating, emoji,
                                extras, flight_link, arrival_time, 900,  # 15 минут
                            ),
                            kwargs={"flight_id_for_db": report_id or None},
                            daemon=True,
                        ).start()
            else:
                # Аэропорт совпадает — используем данные из кэша сразу
                logger.info(f"[Completed] Используем кэш вылета для '{pilot_name}': {cached_dep}→{cached_arr}")
                final_msg = (
                    f"🛬 <b>ПОСАДКА ВЫПОЛНЕНА — TOUCHDOWN</b> {emoji}\n\n"
                    f"👨‍✈️ Captain: <b>{pilot_name}</b>\n"
                    f"🆔 Flight: <b>{cached_fno}</b>\n"
                    f"🗺 Route: <b>{cached_dep} → {cached_arr}</b>\n"
                    f"📍 Airport: <b>{airport_name}</b>\n"
                    f"✈️ Aircraft: <b>{aircraft_name}</b>\n"
                    f"📊 Landing Rate: <b>{rate} fpm</b> — {rating}"
                    f"{extras}"
                    f"{flight_link}"
                )
                tg_send(final_msg)
                # Обновляем БД если данные из кэша валидные
                if _is_valid_icao(cached_dep) and _is_valid_icao(cached_arr) and report_id:
                    db_update_flight_route(report_id, cached_fno, cached_dep, cached_arr)
                    logger.info(f"[Completed] БД обновлена из кэша для flight_id={report_id}")
                # Очищаем кэш после использования
                with _departure_cache_lock:
                    _departure_cache.pop(pilot_name, None)
        else:
            # Нет кэша — стандартное обогащение через FSAirlines с задержкой 3 мин
            logger.info(f"[Completed] Нет кэша вылета для '{pilot_name}', запускаю обогащение через {FSA_ENRICH_ARRIVAL_DELAY}с")
            r = session.post(
                f"{TG_BASE}/sendMessage",
                json={
                    "chat_id": CHAT_ID,
                    "text": msg_text,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True,
                },
                timeout=20,
            )
            if r.status_code == 200:
                message_id = r.json().get("result", {}).get("message_id")
                if message_id:
                    threading.Thread(
                        target=_enrich_completed_from_fsa,
                        args=(
                            message_id, CHAT_ID, pilot_name, aircraft_name,
                            airport_name, rate, rating, emoji,
                            extras, flight_link, arrival_time, FSA_ENRICH_ARRIVAL_DELAY,
                        ),
                        kwargs={"flight_id_for_db": report_id or None},
                        daemon=True,
                    ).start()
            else:
                logger.warning(f"[Completed] Не удалось отправить: {r.text}")
    else:
        tg_send(msg_text)

    if rate < -600:
        tg_send(
            f"⚠️ <b>HARD LANDING ALERT</b>\n\n"
            f"👨‍✈️ Pilot: <b>{user.get('name', 'Unknown')}</b>\n"
            f"📊 Landing Rate: <b>{rate} fpm</b>\n"
            f"✈️ Aircraft inspection recommended."
        )

    # ─── Проверка ивента «Тихий Вжух» ──────────────────────────
    if operation_is_active() and _is_valid_icao(dep) and _is_valid_icao(arr):
        leg_key = (dep.upper(), arr.upper())
        if leg_key in OPERATION_LEG_MAP:
            leg_num, leg_pts = OPERATION_LEG_MAP[leg_key]
            pilot = db_op_get_pilot(pilot_name)

            # Авторегистрация при первом совпавшем рейсе
            if not pilot:
                aircraft_name_op = aircraft.get("icao_name", "")
                db_op_register_pilot(pilot_name, aircraft_name_op)
                pilot = db_op_get_pilot(pilot_name)
                logger.info(f"[Operation] Авторегистрация пилота '{pilot_name}'")

            if pilot and pilot["status"] == "active":
                report_url_op = f"https://fshub.io/flight/{report_id}/report" if report_id else ""

                # ── Проверка лимита 2 лега в сутки UTC ──────────────
                allowed, legs_today = db_op_check_daily_limit(pilot_name)
                if not allowed:
                    tg_send(
                        f"⏳ <b>ЛИМИТ ЛЕГОВ — ОПЕРАЦИЯ «{OPERATION_NAME}»</b>\n\n"
                        f"👨‍✈️ <b>{pilot_name}</b>\n"
                        f"✈️ Leg {leg_num}: {dep} → {arr}\n\n"
                        f"❌ Сегодня уже выполнено <b>2 лега</b>.\n"
                        f"🕛 Счётчик сбросится в <b>00:00 UTC (03:00 МСК)</b>"
                    )
                    logger.info(
                        f"[Operation] {pilot_name} leg={leg_num} — лимит сутки превышен"
                    )
                    # Лег не засчитывается — выходим из блока
                else:
                    # Определяем тип ВС для коэффициента
                    aircraft_icao_type = (
                        d.get("aircraft") or {}
                    ).get("icao") or aircraft.get("icao_name", "")

                    # Проверяем полёт в сети VATSIM/IVAO
                    user_handles = user.get("handles") or {}
                    on_network   = bool(user_handles.get("vatsim") or user_handles.get("ivao"))

                    if rate <= -OPERATION_HARD_CRASH:
                        # Крушение — полный сброс
                        db_op_add_leg(
                            pilot_name, leg_num, dep, arr, rate,
                            0, report_id or "", report_url_op,
                        )
                        db_op_reset_pilot(pilot_name)
                        tg_send(
                            f"💥 <b>КРУШЕНИЕ — ОПЕРАЦИЯ «{OPERATION_NAME}»</b>\n\n"
                            f"👨‍✈️ <b>{pilot_name}</b>\n"
                            f"✈️ Leg {leg_num}: {dep} → {arr}\n"
                            f"📊 Посадка: <b>{rate} fpm</b>\n\n"
                            f"⚠️ Борт утерян. Весь прогресс сброшен.\n"
                            f"Пилот начинает с Leg 1."
                        )
                    elif rate <= -OPERATION_FAIL_RATE:
                        # Жёсткая посадка — этап провален, 0 очков
                        db_op_add_leg(
                            pilot_name, leg_num, dep, arr, rate,
                            0, report_id or "", report_url_op,
                        )
                        tg_send(
                            f"🔴 <b>ЭТАП ПРОВАЛЕН — ОПЕРАЦИЯ «{OPERATION_NAME}»</b>\n\n"
                            f"👨‍✈️ <b>{pilot_name}</b>\n"
                            f"✈️ Leg {leg_num}: {dep} → {arr}\n"
                            f"📊 Посадка: <b>{rate} fpm</b> — слишком жёстко!\n\n"
                            f"❌ Очки не начислены. Повторите Leg {leg_num}."
                        )
                    else:
                        # Успешная посадка — рассчитываем очки
                        earned, coeff, net_bonus = op_calc_points(leg_pts, aircraft_icao_type, on_network)
                        next_leg    = leg_num + 1
                        is_finished = next_leg > len(OPERATION_LEGS)
                        new_status  = "finished" if is_finished else "active"
                        new_points  = pilot["total_points"] + earned
    
                        db_op_add_leg(
                            pilot_name, leg_num, dep, arr, rate,
                            earned, report_id or "", report_url_op,
                            base_points=leg_pts, coeff=coeff,
                            net_bonus=net_bonus, on_network=on_network,
                        )
                        db_op_update_pilot(
                            pilot_name,
                            next_leg if not is_finished else leg_num,
                            new_points, new_status,
                        )
    
                        # Строка с деталями начисления
                        coeff_str  = f"×{coeff}" if coeff != 1.0 else ""
                        bonus_str  = f" +{net_bonus} (VATSIM/IVAO)" if net_bonus else ""
                        detail_str = f"{leg_pts}{coeff_str}{bonus_str} = <b>{earned}</b>"
    
                        if is_finished:
                            tg_send(
                                f"🏁 <b>ФИНИШ! ОПЕРАЦИЯ «{OPERATION_NAME}»</b>\n\n"
                                f"👨‍✈️ <b>{pilot_name}</b> завершил маршрут!\n"
                                f"✈️ Последний этап: {dep} → {arr}\n"
                                f"📊 Посадка: <b>{rate} fpm</b>\n"
                                f"⭐ Очки: {detail_str} | Итого: <b>{new_points:,}</b>\n\n"
                                f"🎉 Борт успешно перегнан в SBRJ!"
                            )
                        else:
                            next_info = next(
                                (f"{d2}→{a2}" for n2, d2, a2, _ in OPERATION_LEGS if n2 == next_leg), ""
                            )
                            # Инфо о лимите суток (включая текущий лег)
                            legs_after = legs_today + 1
                            if legs_after >= 2:
                                limit_str = (
                                    f"\n🕛 На сегодня лимит исчерпан. "
                                    f"Следующий лег — после <b>00:00 UTC (03:00 МСК)</b>"
                                )
                            else:
                                limit_str = f"\n✅ Сегодня можно выполнить ещё <b>1 лег</b>"

                            tg_send(
                                f"✅ <b>LEG {leg_num} ВЫПОЛНЕН — «{OPERATION_NAME}»</b>\n\n"
                                f"👨‍✈️ <b>{pilot_name}</b>\n"
                                f"✈️ {dep} → {arr}\n"
                                f"📊 Посадка: <b>{rate} fpm</b>\n"
                                f"⭐ Очки: {detail_str} | Итого: <b>{new_points:,}</b>\n"
                                f"➡️ Следующий: Leg {next_leg} {next_info}"
                                f"{limit_str}"
                            )
                    logger.info(
                        f"[Operation] {pilot_name} leg={leg_num} rate={rate} "
                        f"aircraft={aircraft_icao_type} network={on_network} pts={earned if rate > -OPERATION_FAIL_RATE else 0}"
                    )

    # ─── Проверка конкурса Мастер Посадки ───────────────────────
    if is_contest_landing(rate):
        report_url = f"https://fshub.io/flight/{report_id}/report" if report_id else ""
        db_contest_add(
            flight_id=report_id,
            pilot=user.get("name", "Unknown"),
            flight_no=flight_no,
            departure=dep,
            arrival=arr,
            aircraft=aircraft.get("icao_name", "Unknown"),
            landing_rate=rate,
            report_url=report_url,
        )
        entries  = db_contest_month()
        position = len(entries)
        slots    = CONTEST_MONTHLY_LIMIT // CONTEST_POINTS_PER_LANDING
        if position <= slots:
            tg_send(
                f"🎯 <b>КАНДИДАТ — МАСТЕР ПОСАДКИ!</b>\n\n"
                f"👨‍✈️ <b>{user.get('name', 'Unknown')}</b>\n"
                f"📊 Landing Rate: <b>{rate} fpm</b>\n"
                f"⭐ Позиция в этом месяце: <b>#{position}</b>\n"
                f"🏅 Начислено: <b>{CONTEST_POINTS_PER_LANDING} баллов</b>\n\n"
                f"🔍 Находится на проверке — результат в конце месяца"
            )
        else:
            tg_send(
                f"🎯 <b>СНАЙПЕРСКАЯ ПОСАДКА!</b>\n\n"
                f"👨‍✈️ <b>{user.get('name', 'Unknown')}</b>\n"
                f"📊 Landing Rate: <b>{rate} fpm</b>\n"
                f"📅 Фонд {CONTEST_MONTHLY_LIMIT} баллов на этот месяц исчерпан — ждём следующего!"
            )


def _send_screenshots_async(screenshots: List):
    for scr in screenshots[:3]:
        url = scr.get("screenshot_url")
        if url:
            tg_photo(url, "📸 <b>Flight Screenshot</b>")
            time.sleep(1)


def handle_screenshots(data: Dict):
    screenshots = data.get("_data", [])
    if screenshots:
        threading.Thread(
            target=_send_screenshots_async,
            args=(screenshots,),
            daemon=True,
        ).start()


def handle_achievement(data: Dict):
    d = data.get("_data", {})
    achievement = d.get("achievement", {})
    flight = d.get("flight", {})
    user = flight.get("user", {})
    tg_send(
        f"🏆 <b>ACHIEVEMENT UNLOCKED</b>\n\n"
        f"👨‍✈️ {user.get('name', 'Unknown')}\n"
        f"🎯 {achievement.get('title', 'Achievement')}"
    )


FSHUB_HANDLERS = {
    "flight.departed":   handle_departure,
    "flight.completed":  handle_completed,
    "screenshots.uploaded": handle_screenshots,
    "airline.achievement":  handle_achievement,
}

# ═══════════════════════════════════════════════════════════════
# TELEGRAM COMMANDS
# ═══════════════════════════════════════════════════════════════

COMMANDS = {
    "/stats":     fmt_stats,
    "/last":      fmt_last,
    "/top_landing": fmt_top_landings,
    "/top":       fmt_top_pilots,
    "/economy":   fmt_daily_economy,
    "/monthly":   fmt_monthly_economy,
    "/live":      fmt_active_flights,
    "/va":        fmt_va_info,
    "/operation": fmt_operation,
}

HELP_TEXT = (
    "<b>VA UP! Панель управления</b>\n\n"
    "📊 <b>Операции:</b>\n"
    "/stats — статистика операций\n"
    "/last — последние рейсы\n"
    "/top — топ пилоты (7 дней)\n"
    "/top_landing — лучшие посадки\n\n"
    "🛫 <b>Предполётная подготовка:</b>\n"
    "/runway ICAO — METAR и рабочая полоса\n"
    "   Пример: /runway UHWW\n\n"
    "💰 <b>Финансы:</b>\n"
    "/economy — финансовый отчёт за день\n"
    "/monthly — дайджест за месяц\n"
    "/live — рейсы в воздухе\n"
    "/va — информация о компании"
)

# Инициализируем маппинг после определения всех форматтеров
MENU_CALLBACKS.update({
    "cmd_stats":       fmt_stats,
    "cmd_last":        fmt_last,
    "cmd_top":         fmt_top_pilots,
    "cmd_top_landing": fmt_top_landings,
    "cmd_economy":     fmt_daily_economy,
    "cmd_monthly":     fmt_monthly_economy,
    "cmd_live":        fmt_active_flights,
    "cmd_va":          fmt_va_info,
    "cmd_contest":     fmt_contest,
    "cmd_operation":   fmt_operation,
})


def handle_tg_command(message: Dict):
    chat_id = message.get("chat", {}).get("id")
    text = (message.get("text") or "").strip()

    if not chat_id or not text:
        return

    # В канале/чате обрабатываем только команды с явным упоминанием бота:
    # /runway@vaup_bot UHWW — сработает.
    # /runway UHWW или обычный текст — игнорируем, чтобы не мешать общению.
    if str(chat_id) == str(CHAT_ID):
        # Если пользователь уже в режиме ожидания ICAO — пропускаем фильтр,
        # чтобы он мог ответить боту прямо в канале.
        with _awaiting_lock:
            user_is_awaiting = str(chat_id) in _awaiting_icao
        if not user_is_awaiting:
            # В канале реагируем только на команды с явным упоминанием бота:
            # /runway@up_va_bot — сработает, обычный текст — нет.
            first_word = text.split()[0] if text.split() else ""
            addressed_to_bot = "@" in first_word and first_word.startswith("/")
            if not addressed_to_bot:
                return  # тихо игнорируем

    logger.info(f"Command from chat={chat_id}: {text}")

    # Парсим команду один раз — используется во всех блоках ниже
    cmd_parts = text.split()
    base_cmd  = cmd_parts[0].split("@")[0] if cmd_parts else ""

    if text.startswith("/start") or text.startswith("/help") or text.startswith("/menu"):
        tg_send_menu(chat_id)
        return

    # ─── Обработка /operation_admin ────────────────────────────
    if base_cmd == "/operation_admin":
        if str(chat_id) != str(ADMIN_ID):
            tg_send("⛔ Нет доступа.", chat_id)
            return
        # /operation_admin add <имя> [самолёт]
        # /operation_admin set <имя> <очки_дельта>
        # /operation_admin reset <имя>
        # /operation_admin list
        parts = text.split(None, 3)
        sub = parts[1] if len(parts) > 1 else ""

        if sub == "add":
            # Формат: /operation_admin add Имя Фамилия | B738
            # Разделитель | отделяет имя от самолёта
            rest = text.split(None, 2)[2] if len(text.split(None, 2)) > 2 else ""
            if "|" in rest:
                pilot_part, aircraft_part = rest.split("|", 1)
                pilot   = pilot_part.strip()
                aircraft = aircraft_part.strip()
            else:
                pilot    = rest.strip()
                aircraft = ""
            if not pilot:
                tg_send("Использование: /operation_admin add Имя Фамилия | B738", chat_id)
                return
            ok = db_op_register_pilot(pilot, aircraft)
            tg_send(
                f"✅ Пилот <b>{pilot}</b> зарегистрирован{' на ' + aircraft if aircraft else ''}."
                if ok else f"⚠️ Пилот <b>{pilot}</b> уже зарегистрирован.",
                chat_id
            )

        elif sub == "set":
            # Формат: /operation_admin set Имя Фамилия | +500
            rest = text.split(None, 2)[2] if len(text.split(None, 2)) > 2 else ""
            if "|" not in rest:
                tg_send("Использование: /operation_admin set Имя Фамилия | +500", chat_id)
                return
            pilot_part, delta_part = rest.split("|", 1)
            pilot = pilot_part.strip()
            try:
                delta = int(delta_part.strip().replace("+", ""))
            except ValueError:
                tg_send("Дельта должна быть числом (например +500 или -200)", chat_id)
                return
            p = db_op_get_pilot(pilot)
            if not p:
                tg_send(f"Пилот <b>{pilot}</b> не найден.", chat_id)
                return
            db_op_admin_set(pilot, 0, delta)
            p2 = db_op_get_pilot(pilot)
            tg_send(f"✅ <b>{pilot}</b>: {p['total_points']:,} → <b>{p2['total_points']:,}</b> очков.", chat_id)

        elif sub == "leg":
            # Формат: /operation_admin leg Имя Фамилия | 3
            rest = text.split(None, 2)[2] if len(text.split(None, 2)) > 2 else ""
            if "|" not in rest:
                tg_send("Использование: /operation_admin leg Имя Фамилия | 3", chat_id)
                return
            pilot_part, leg_part = rest.split("|", 1)
            pilot = pilot_part.strip()
            try:
                leg = int(leg_part.strip())
            except ValueError:
                tg_send("Номер лега должен быть числом.", chat_id)
                return
            if not 1 <= leg <= len(OPERATION_LEGS) + 1:
                tg_send(f"Номер лега: 1–{len(OPERATION_LEGS)}.", chat_id)
                return
            p = db_op_get_pilot(pilot)
            if not p:
                tg_send(f"Пилот <b>{pilot}</b> не найден.", chat_id)
                return
            db_execute(
                "UPDATE operation_pilots SET current_leg = %s WHERE pilot_name = %s",
                (leg, pilot),
            )
            tg_send(f"✅ <b>{pilot}</b>: текущий лег установлен на <b>{leg}</b>.", chat_id)

        elif sub == "reset":
            # Формат: /operation_admin reset Имя Фамилия
            rest = text.split(None, 2)[2] if len(text.split(None, 2)) > 2 else ""
            pilot = rest.strip()
            if not pilot:
                tg_send("Использование: /operation_admin reset Имя Фамилия", chat_id)
                return
            p = db_op_get_pilot(pilot)
            if not p:
                tg_send(f"Пилот <b>{pilot}</b> не найден.", chat_id)
                return
            db_op_reset_pilot(pilot)
            tg_send(f"✅ Прогресс <b>{pilot}</b> сброшен.", chat_id)

        elif sub == "list":
            pilots = db_op_all_pilots()
            if not pilots:
                tg_send("Участников нет.", chat_id)
                return
            lines = []
            for p in pilots:
                lines.append(
                    f"• <b>{p['pilot_name']}</b> ({p['aircraft'] or '—'}) "
                    f"Leg {p['current_leg']} | {p['total_points']:,} очк. [{p['status']}]"
                )
            tg_send("📋 <b>Участники операции:</b>\n\n" + "\n".join(lines), chat_id)

        else:
            tg_send(
                "📋 <b>Команды администратора:</b>\n\n"
                "/operation_admin add Имя Фамилия | B738\n"
                "/operation_admin set Имя Фамилия | +500\n"
                "/operation_admin leg Имя Фамилия | 3\n"
                "/operation_admin reset Имя Фамилия\n"
                "/operation_admin list",
                chat_id,
            )
        return
    # ─── Конец /operation_admin ──────────────────────────────────

    # ─── Обработка /contest [YYYY-MM] ───────────────────────────
    if base_cmd == "/contest":
        # Опциональный аргумент: /contest 2026-05
        month_arg = None
        if len(cmd_parts) >= 2:
            import re
            raw = cmd_parts[1].strip()
            if re.match(r"^\d{4}-\d{2}$", raw):
                month_arg = raw
            else:
                tg_send(
                    "❌ Неверный формат месяца.\n"
                    "Пример: <code>/contest 2026-05</code>",
                    chat_id,
                )
                return
        tg_send(fmt_contest(month_arg), chat_id)
        return
    # ─── Конец обработки /contest ───────────────────────────────

    # ─── Обработка /runway ──────────────────────────────────────

    if base_cmd == "/runway":
        if len(cmd_parts) < 2:
            # Аргумент не передан — переходим в режим ожидания ICAO
            with _awaiting_lock:
                _awaiting_icao[str(chat_id)] = True
            tg_send_with_cancel(
                "✈️ Введите ICAO-код аэропорта:\n<i>Например: UHWW, UUEE, EGLL</i>",
                chat_id,
            )
        else:
            icao_raw = cmd_parts[1].upper()
            if len(icao_raw) != 4 or not icao_raw.isalnum():
                tg_send(
                    "❌ Некорректный ICAO-код. Должен содержать 4 символа (буквы и цифры).\n"
                    "Пример: <code>/runway UHWW</code>",
                    chat_id,
                )
            else:
                logger.info(f"Runway request: {icao_raw} from {chat_id}")
                tg_send(fmt_runway(icao_raw), chat_id)
        return
    # ─── Конец обработки /runway ────────────────────────────────

    # ─── Обработка ответа на запрос ICAO ────────────────────────
    with _awaiting_lock:
        is_awaiting = _awaiting_icao.pop(str(chat_id), False)

    if is_awaiting:
        icao_raw = text.upper().strip()
        if len(icao_raw) != 4 or not icao_raw.isalnum():
            tg_send(
                "❌ Некорректный ICAO-код. Должен содержать 4 символа (буквы и цифры).\n"
                "Попробуйте снова: <code>/runway UHWW</code>",
                chat_id,
            )
        else:
            logger.info(f"Runway request (dialog): {icao_raw} from {chat_id}")
            tg_send(fmt_runway(icao_raw), chat_id)
        return
    # ─── Конец обработки ответа ICAO ────────────────────────────

    cmd = text.split("@")[0]

    if cmd in COMMANDS:
        try:
            tg_send(COMMANDS[cmd](), chat_id)
        except Exception as e:
            logger.exception(f"Command failed: {e}")
            tg_send("⚠️ Command error.", chat_id)
        return

    tg_send("Unknown command. Use /help", chat_id)


# ═══════════════════════════════════════════════════════════════
# FLASK APP
# ═══════════════════════════════════════════════════════════════

app = Flask(__name__)


def job_listener(event):
    if event.exception:
        logger.error(f"Job {event.job_id} crashed: {event.exception}")
    else:
        logger.info(f"Job {event.job_id} executed successfully")


def init_scheduler():
    """
    Инициализирует и запускает планировщик.
    Вызывается ОДИН РАЗ в блоке STARTUP при загрузке модуля.
    Gunicorn запускается с --workers 1 --preload, поэтому планировщик
    живёт в единственном процессе и не дублируется.
    """
    scheduler = BackgroundScheduler(
        daemon=True,
        executors={"default": ThreadPoolExecutor(max_workers=2)},
        job_defaults={
            "coalesce": True,
            "max_instances": 1,
            "misfire_grace_time": 3600,
        },
        timezone="UTC",
    )

    # ─── Ежедневные задачи ──────────────────────────────────────
    scheduler.add_job(
        snapshot_daily_economy,
        "cron", hour=23, minute=50,
        id="daily_economy_snapshot",
    )
    # Финансовый отчёт за сутки — 23:00 МСК (20:00 UTC)
    scheduler.add_job(
        lambda: tg_send(fmt_daily_economy()),
        "cron", hour=20, minute=0,
        id="daily_economy_report",
    )
    # Статистика операций — 00:00 МСК (21:00 UTC)
    scheduler.add_job(
        lambda: tg_send(fmt_stats()),
        "cron", hour=21, minute=0,
        id="daily_stats",
    )

    # ─── Еженедельные задачи ────────────────────────────────────
    scheduler.add_job(
        lambda: tg_send(fmt_top_landings()),
        "cron", day_of_week="sun", hour=12, minute=0,
        id="weekly_landing_ranking",
    )
    scheduler.add_job(
        lambda: tg_send(fmt_operation_digest()) if operation_is_active() else None,
        "cron", day_of_week="sun", hour=11, minute=0,
        id="weekly_operation_digest",
    )
    scheduler.add_job(
        lambda: tg_send(fmt_operation()) if operation_is_active() else None,
        "cron", day_of_week="fri", hour=0, minute=0,
        id="weekly_operation_standings",
    )
    scheduler.add_job(
        lambda: tg_send(fmt_top_pilots()),
        "cron", day_of_week="sun", hour=10, minute=0,
        id="weekly_top_pilots",
    )
    scheduler.add_job(
        lambda: tg_send(
            "🛫 <b>СОВМЕСТНАЯ СУББОТНЯЯ ОПЕРАЦИЯ!</b>\n\n"
            "⏰ Москва: 09:00 ☀️  |  Камчатка: 18:00 🌙\n\n"
            "✈️ Предлагайте маршрут в комментариях!\nКто присоединяется? 👇"
        ),
        "cron", day_of_week="sat", hour=6, minute=0,
        id="saturday_inv",
    )
    scheduler.add_job(
        lambda: tg_send(
            "🏆 <b>ЕЖЕНЕДЕЛЬНЫЙ ВЫЗОВ ЭКИПАЖУ!</b>\n\n"
            "🔹 Цель: 3 рейса за 7 дней\n"
            "🔹 Бонус: лучшая посадка недели\n\nГотов принять вызов? 💪"
        ),
        "cron", day_of_week="mon", hour=8, minute=0,
        id="monday_challenge",
    )

    # ─── Ежемесячные задачи ─────────────────────────────────────
    scheduler.add_job(
        lambda: tg_send(fmt_monthly_economy()),
        "cron", day=1, hour=9, minute=0,
        id="monthly_digest",
    )

    scheduler.add_listener(job_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
    scheduler.start()

    jobs = scheduler.get_jobs()
    logger.info(f"Планировщик запущен. Активных задач: {len(jobs)}")
    for job in jobs:
        logger.info(f"  • {job.id} → следующий запуск: {job.next_run_time}")

    return scheduler


@app.route("/")
def home():
    return jsonify({
        "status": "running",
        "service": "VA UP! PostgreSQL Edition",
        "fsa_enabled": bool(FSA_KEY),
    })


@app.route("/health")
def health():
    return jsonify({"ok": True}), 200


@app.route("/webhook", methods=["GET", "POST"])
def fshub_webhook():
    if request.method == "GET":
        return jsonify({"status": "ok"})
    try:
        data = request.get_json(force=True)
        if not data:
            return jsonify({"error": "no json"}), 400

        event = data.get("_type", "")
        logger.info(f"FSHub событие: {event}")

        handler = FSHUB_HANDLERS.get(event)
        if handler:
            handler(data)

        return jsonify({"ok": True})
    except Exception as e:
        logger.exception(f"Webhook failure: {e}")
        return jsonify({"error": str(e)}), 500


@app.route(f"/bot/{BOT_TOKEN}", methods=["POST"])
def tg_webhook():
    try:
        data = request.get_json(force=True) or {}
        # Inline-кнопки приходят как callback_query, не как message
        if "callback_query" in data:
            handle_callback_query(data["callback_query"])
        else:
            message = data.get("message") or data.get("channel_post") or {}
            handle_tg_command(message)
    except Exception as e:
        logger.exception(f"Telegram webhook failure: {e}")
    return jsonify({"ok": True})


# ═══════════════════════════════════════════════════════════════
# STARTUP — выполняется один раз при загрузке модуля.
# Gunicorn с флагом --preload загружает модуль ДО форка воркеров,
# поэтому планировщик стартует ровно один раз.
#
# Start Command на Render:
#   gunicorn app:app --workers 1 --threads 4 --timeout 120 --preload
# ═══════════════════════════════════════════════════════════════

try:
    _create_pool()
    _init_db()
    tg_setup_webhook()
    init_scheduler()
except Exception as e:
    logger.exception(f"Startup failed: {e}")
    sys.exit(1)

logger.info(f"Сервис запущен на порту {PORT} — VA UP! готова к полётам")

# ═══════════════════════════════════════════════════════════════
# MAIN (для локального запуска)
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT, threaded=True)

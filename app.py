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
PORT       = int(os.environ.get("PORT", 10000))
HOSTNAME   = os.environ.get("RENDER_EXTERNAL_HOSTNAME", "fshub-bot.onrender.com")

WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "")
MAX_DB_FLIGHTS = int(os.environ.get("MAX_DB_FLIGHTS", "5000"))
DATABASE_URL   = os.environ.get("DATABASE_URL", "")

FSA_URL = "https://www.fsairlines.net/va_interface2.php"
TG_BASE = f"https://api.telegram.org/bot{BOT_TOKEN}"

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


def db_top_landings(limit: int = 10) -> List:
    return db_execute(
        "SELECT * FROM flights ORDER BY ABS(landing_rate) ASC LIMIT %s",
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
    return db_execute(
        """
        SELECT * FROM daily_economy
        WHERE day >= CURRENT_DATE - (%s * INTERVAL '1 day')
        ORDER BY day ASC
        """,
        (days,), fetch="all",
    ) or []


def db_get_today_economy() -> Optional[Dict]:
    return db_execute(
        "SELECT * FROM daily_economy WHERE day = CURRENT_DATE",
        fetch="one",
    )


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

def fsa_call(function: str, extra: Optional[Dict] = None):
    if not FSA_KEY:
        return None
    params = {
        "function": function,
        "va_id": FSA_VA_ID,
        "apikey": FSA_KEY,
        "format": "json",
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


# ═══════════════════════════════════════════════════════════════
# FINANCIAL AGGREGATION
# ═══════════════════════════════════════════════════════════════

def _aggregate(transactions: List[Dict]) -> Dict:
    inc, exp = 0.0, 0.0
    inc_cat: Dict[str, float] = {}
    exp_cat: Dict[str, float] = {}
    for t in transactions:
        try:
            v = float(t.get("value", 0))
        except (ValueError, TypeError):
            continue
        r = t.get("reason") or "Other"
        if v >= 0:
            inc += v
            inc_cat[r] = inc_cat.get(r, 0) + v
        else:
            exp += abs(v)
            exp_cat[r] = exp_cat.get(r, 0) + abs(v)
    return {
        "inc": inc, "exp": exp, "net": inc - exp,
        "inc_cat": inc_cat, "exp_cat": exp_cat,
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
    detail = {"inc_cat": ag["inc_cat"], "exp_cat": ag["exp_cat"]}
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
    flights = db_all_flights()
    if not flights:
        return "📊 <b>No flight data available.</b>"
    rates = [f["landing_rate"] for f in flights]
    avg = round(sum(rates) / len(rates))
    return (
        f"📊 <b>VA UP! OPERATIONS</b>\n\n"
        f"🛬 Flights: <b>{len(flights)}</b>\n"
        f"📐 Average Landing: <b>{avg} fpm</b>"
    )


def fmt_last(limit: int = 5) -> str:
    flights = db_last_flights(limit)
    if not flights:
        return "✈️ No flights recorded yet."
    lines = []
    for f in flights:
        rating, emoji = landing_rating(f["landing_rate"])
        lines.append(
            f"{emoji} <b>{f['flight_no']}</b>\n"
            f"👨‍✈️ {f['pilot']}\n"
            f"🗺 {f['departure']} → {f['arrival']}\n"
            f"📊 {f['landing_rate']} fpm"
        )
    return "✈️ <b>LATEST FLIGHTS</b>\n\n" + "\n\n".join(lines)


def fmt_top_landings(limit: int = 10) -> str:
    rows = db_top_landings(limit)
    if not rows:
        return "🏆 No landing data available."
    medals = ["🥇", "🥈", "🥉"]
    lines = []
    for i, row in enumerate(rows, 1):
        prefix = medals[i - 1] if i <= 3 else f"{i}."
        lines.append(f"{prefix} <b>{row['pilot']}</b> — {row['landing_rate']} fpm")
    return "🏆 <b>TOP LANDINGS</b>\n\n" + "\n".join(lines)


def fmt_top_pilots() -> str:
    flights = db_all_flights()
    if not flights:
        return "🏆 No flight data available."
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
        return "🏆 No flights in the last 7 days."
    sorted_pilots = sorted(counts.items(), key=lambda x: x[1], reverse=True)[:10]
    medals = {1: "🥇", 2: "🥈", 3: "🥉"}
    lines = [
        f"{medals.get(i, f'{i}.')} <b>{pilot}</b> — {n} flights"
        for i, (pilot, n) in enumerate(sorted_pilots, 1)
    ]
    return "🏆 <b>TOP PILOTS (7 days)</b>\n\n" + "\n".join(lines)


def fmt_daily_economy() -> str:
    row = db_get_today_economy()
    if row:
        inc = row["income"]
        exp = row["expense"]
        net = row["net"]
        detail = row["detail"] or {}
        inc_cat = detail.get("inc_cat", {})
        exp_cat = detail.get("exp_cat", {})
    else:
        txs = fsa_daily_transactions()
        if not txs:
            return "📊 No financial data for today."
        ag = _aggregate(txs)
        inc, exp, net = ag["inc"], ag["exp"], ag["net"]
        inc_cat, exp_cat = ag["inc_cat"], ag["exp_cat"]

    em = _nem(net)
    msg = (
        f"📊 <b>DAILY FINANCIAL REPORT</b>\n\n"
        f"💰 Revenue: <b>+{inc:,.0f} v$</b>\n"
        f"📉 Expenses: <b>-{exp:,.0f} v$</b>\n"
        f"{em} <b>Net: {net:+,.0f} v$</b>\n"
    )
    if inc_cat:
        msg += "\n🔝 <b>TOP REVENUE:</b>\n"
        for reason, amount in _top(inc_cat):
            msg += f"   • {reason}: <b>+{amount:,.0f} v$</b>\n"
    if exp_cat:
        msg += "\n⚠️ <b>TOP EXPENSES:</b>\n"
        for reason, amount in _top(exp_cat):
            msg += f"   • {reason}: <b>-{amount:,.0f} v$</b>\n"
    return msg


def fmt_monthly_economy() -> str:
    rows = db_get_monthly_economy(days=30)
    if not rows:
        return (
            "📊 No monthly data yet.\n\n"
            "ℹ️ History is collected daily at 23:50 UTC. "
            "Data will appear from tomorrow."
        )
    total_inc = sum(r["income"] for r in rows)
    total_exp = sum(r["expense"] for r in rows)
    net = total_inc - total_exp
    best_row = max(rows, key=lambda r: r["net"])
    worst_row = min(rows, key=lambda r: r["net"])
    return (
        f"🏆 <b>MONTHLY FINANCIAL DIGEST</b>\n"
        f"📅 {datetime.now().strftime('%B %Y')}\n\n"
        f"💰 Revenue: <b>+{total_inc:,.0f} v$</b>\n"
        f"📉 Expenses: <b>-{total_exp:,.0f} v$</b>\n"
        f"{_nem(net)} <b>Net: {net:+,.0f} v$</b>\n\n"
        f"🌟 Best Day: <b>{best_row['day']}</b> (+{best_row['net']:,.0f} v$)\n"
        f"⚠️ Worst Day: <b>{worst_row['day']}</b> ({worst_row['net']:+,.0f} v$)\n\n"
        f"📊 Days with data: <b>{len(rows)}</b>"
    )


def fmt_active_flights() -> str:
    flights = fsa_active_flights()
    if not flights:
        return "✈️ No airborne aircraft at this time."
    lines = [
        f"✈️ <b>{f.get('number', 'N/A')}</b> "
        f"{f.get('departure', '???')} → {f.get('arrival', '???')} "
        f"[{f.get('passengers', 0)} pax]"
        for f in flights[:10]
    ]
    return f"🛫 <b>AIRBORNE AIRCRAFT ({len(flights)})</b>\n\n" + "\n".join(lines)


def fmt_va_info() -> str:
    data = fsa_airline_data()
    if not data:
        return "📊 VA data unavailable."
    return (
        f"🏢 <b>VA UP! INFO</b>\n\n"
        f"📛 Name: <b>{data.get('name', 'N/A')}</b>\n"
        f"💰 Budget: <b>{data.get('budget', 0):,.0f} v$</b>\n"
        f"⭐ Reputation: <b>{data.get('reputation', 0)}</b>\n"
        f"📍 Base: <b>{data.get('base', 'N/A')}</b>\n"
        f"✈️ Code: <b>{data.get('code', 'N/A')}</b>"
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


def fmt_runway(icao: str) -> str:
    """Возвращает сообщение со ссылкой на RunwayApp для указанного ICAO."""
    icao = icao.upper()
    url = f"https://runway.airportdb.io/airport/{icao.lower()}"
    return (
        f"🛫 <b>РЕКОМЕНДАЦИИ ПО ПОЛОСАМ — {icao}</b>\n\n"
        f"🔗 <a href='{url}'>Открыть на RunwayApp</a>\n\n"
        f"💡 <b>Как читать результат:</b>\n"
        f"• 🟢 Зелёные стрелки — встречный ветер (лучший выбор)\n"
        f"• 🟡 Жёлтые стрелки — боковой ветер\n"
        f"• 🔴 Красные — попутный ветер (не рекомендуется)\n"
        f"• Первая полоса в списке = оптимальный вариант\n\n"
        f"📡 <i>Данные обновляются по текущему METAR</i>"
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
    tg_send(
        f"🛫 <b>ВЫЛЕТ ПОДТВЕРЖДЁН — CLEARED FOR TAKEOFF</b>\n\n"
        f"👨‍✈️ Captain: <b>{user.get('name', 'Unknown')}</b>\n"
        f"🆔 Flight: <b>{plan.get('flight_no', 'N/A')}</b>\n"
        f"🗺 Route: <b>{plan.get('departure', '????')} → {plan.get('arrival', '????')}</b>\n"
        f"✈️ Aircraft: <b>{aircraft.get('icao_name', 'N/A')}</b>\n\n"
        f"✈️ <i>Желаем попутного ветра и мягкой посадки!</i>"
    )


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

    tg_send(
        f"🛬 <b>ПОСАДКА ВЫПОЛНЕНА — TOUCHDOWN</b> {emoji}\n\n"
        f"👨‍✈️ Captain: <b>{user.get('name', 'Unknown')}</b>\n"
        f"🆔 Flight: <b>{flight_no}</b>\n"
        f"🗺 Route: <b>{dep} → {arr}</b>\n"
        f"📍 Airport: <b>{airport.get('name', 'Unknown')}</b>\n"
        f"✈️ Aircraft: <b>{aircraft.get('icao_name', 'Unknown')}</b>\n"
        f"📊 Landing Rate: <b>{rate} fpm</b> — {rating}"
        f"{extras}"
        f"{flight_link}"
    )

    if rate < -600:
        tg_send(
            f"⚠️ <b>HARD LANDING ALERT</b>\n\n"
            f"👨‍✈️ Pilot: <b>{user.get('name', 'Unknown')}</b>\n"
            f"📊 Landing Rate: <b>{rate} fpm</b>\n"
            f"✈️ Aircraft inspection recommended."
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
    "/stats": fmt_stats,
    "/last": fmt_last,
    "/top_landing": fmt_top_landings,
    "/top": fmt_top_pilots,
    "/economy": fmt_daily_economy,
    "/monthly": fmt_monthly_economy,
    "/live": fmt_active_flights,
    "/va": fmt_va_info,
}

HELP_TEXT = (
    "<b>VA UP! Operations Panel</b>\n\n"
    "📊 <b>Flight Operations:</b>\n"
    "/stats — operations statistics\n"
    "/last — latest flights\n"
    "/top — top pilots (7 days)\n"
    "/top_landing — best landings\n\n"
    "🛫 <b>Pre-flight Tools:</b>\n"
    "/runway ICAO — runway recommendations\n"
    "   Example: /runway UHWW\n\n"
    "💰 <b>Financial Operations:</b>\n"
    "/economy — daily financial report\n"
    "/monthly — monthly financial digest\n"
    "/live — active flights\n"
    "/va — VA information"
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

    if text.startswith("/start") or text.startswith("/help") or text.startswith("/menu"):
        tg_send_menu(chat_id)
        return

    # ─── Обработка /contest [YYYY-MM] ───────────────────────────
    cmd_parts = text.split()
    base_cmd = cmd_parts[0].split("@")[0]

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

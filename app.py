"""
VA UP! VATSIM Bot — PostgreSQL Edition
FSHub webhook + FSAirlines API + Telegram + Scheduler
"""

import os
import sys
import time
import json
import logging
import threading
from logging.handlers import RotatingFileHandler
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from collections import deque

import psycopg2
import psycopg2.extras
import psycopg2.pool
import requests
from flask import Flask, request, jsonify
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ThreadPoolExecutor
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ═══════════════════════════════════════════════════════════════
# КОНФИГУРАЦИЯ
# ═══════════════════════════════════════════════════════════════

BOT_TOKEN     = os.environ.get("TG_BOT_TOKEN", "")
CHAT_ID       = os.environ.get("TG_CHAT_ID", "")
FSA_KEY       = os.environ.get("FSA_API_KEY", "")
FSA_VA_ID     = os.environ.get("FSA_VA_ID", "56177")
PORT          = int(os.environ.get("PORT", 10000))
HOSTNAME      = os.environ.get("RENDER_EXTERNAL_HOSTNAME", "fshub-bot.onrender.com")
ADMIN_CHAT_ID = os.environ.get("ADMIN_CHAT_ID", "")
MAX_DB_FLIGHTS = int(os.environ.get("MAX_DB_FLIGHTS", "5000"))
DATABASE_URL   = os.environ.get("DATABASE_URL", "")

FSA_URL = "https://www.fsairlines.net/va_interface2.php"
TG_BASE = f"https://api.telegram.org/bot{BOT_TOKEN}"

if not BOT_TOKEN or not CHAT_ID:
    print("❌ TG_BOT_TOKEN или TG_CHAT_ID не заданы")
    sys.exit(1)

if not DATABASE_URL:
    print("❌ DATABASE_URL не задан")
    sys.exit(1)

# ═══════════════════════════════════════════════════════════════
# ЛОГИРОВАНИЕ
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
    logger.warning("Не удалось создать файл журнала, используется только консоль")

logger.info("Запуск VA UP! PostgreSQL Edition")

# ═══════════════════════════════════════════════════════════════
# ЗАЩИТА ОТ ДУБЛИРОВАНИЯ
# ═══════════════════════════════════════════════════════════════

# Для arrivals — дедупликация по flight_id
processed_flight_ids: deque = deque(maxlen=100)
processed_lock = threading.Lock()

# Для departures — дедупликация по flight_id И по пилот+маршрут
processed_departures: deque = deque(maxlen=200)
_recent_dep_keys: deque = deque(maxlen=100)   # хранит (ключ, timestamp)
departures_lock = threading.Lock()

# ═══════════════════════════════════════════════════════════════
# HTTP-СЕССИЯ
# ═══════════════════════════════════════════════════════════════

session = requests.Session()
_retry = Retry(
    total=3, read=3, connect=3, backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET", "POST"],
)
_adapter = HTTPAdapter(max_retries=_retry)
session.mount("https://", _adapter)
session.mount("http://",  _adapter)

# ═══════════════════════════════════════════════════════════════
# БАЗА ДАННЫХ
# ═══════════════════════════════════════════════════════════════

_pool: Optional[psycopg2.pool.ThreadedConnectionPool] = None
_pool_lock = threading.Lock()


def _create_pool() -> None:
    global _pool
    with _pool_lock:
        if _pool is not None:
            return
        _pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=5,
            dsn=DATABASE_URL,
            cursor_factory=psycopg2.extras.RealDictCursor,
            keepalives=1,
            keepalives_idle=10,     # уменьшено с 30 до 10 сек
            keepalives_interval=5,
            keepalives_count=5,
        )
        logger.info("PostgreSQL pool создан (maxconn=5, keepalives_idle=10s)")


def _get_conn():
    if _pool is None:
        _create_pool()
    conn = _pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        conn.rollback()
        return conn
    except Exception:
        try:
            _pool.putconn(conn, close=True)
        except Exception:
            pass
        logger.info("Переподключение к PostgreSQL")
        return _pool.getconn()


def _put_conn(conn, close: bool = False) -> None:
    try:
        if _pool:
            _pool.putconn(conn, close=close)
    except Exception as e:
        logger.warning(f"Ошибка возврата соединения: {e}")


def db_execute(query: str, params=None, fetch: str = "none"):
    max_retries = 3
    last_error  = None
    for attempt in range(max_retries):
        conn = None
        try:
            conn = _get_conn()
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
            logger.warning(f"DB OperationalError (попытка {attempt+1}/{max_retries}): {e}")
            if conn:
                try:
                    conn.rollback()
                except Exception:
                    pass
                _put_conn(conn, close=True)
                conn = None
            if attempt < max_retries - 1:
                time.sleep(0.5 * (attempt + 1))
        except Exception as e:
            last_error = e
            logger.warning(f"DB ошибка (попытка {attempt+1}/{max_retries}): {e}")
            if conn:
                try:
                    conn.rollback()
                except Exception:
                    pass
            if attempt == max_retries - 1:
                raise
        finally:
            if conn is not None:
                _put_conn(conn)
    raise last_error


def _init_db() -> None:
    db_execute("""
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
    """)
    db_execute("""
        CREATE TABLE IF NOT EXISTS daily_economy (
            id      SERIAL PRIMARY KEY,
            day     DATE UNIQUE,
            income  BIGINT DEFAULT 0,
            expense BIGINT DEFAULT 0,
            net     BIGINT DEFAULT 0,
            detail  JSONB  DEFAULT '{}'
        )
    """)
    logger.info("Таблицы БД готовы")

# ═══════════════════════════════════════════════════════════════
# DB HELPERS
# ═══════════════════════════════════════════════════════════════

def db_add_flight(
    flight_id: Optional[str], pilot: str, flight_no: str,
    departure: str, arrival: str, aircraft: str,
    landing_rate: int, profit: Optional[int],
) -> None:
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
        "DELETE FROM flights WHERE id NOT IN "
        "(SELECT id FROM flights ORDER BY id DESC LIMIT %s)",
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


def db_save_daily_economy(day: str, income: int, expense: int, detail: Dict) -> None:
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
        WHERE day >= CURRENT_DATE - (%s || ' days')::INTERVAL
        ORDER BY day ASC
        """,
        (str(days),), fetch="all",
    ) or []


def db_get_today_economy() -> Optional[Dict]:
    return db_execute(
        "SELECT * FROM daily_economy WHERE day = CURRENT_DATE",
        fetch="one",
    )

# ═══════════════════════════════════════════════════════════════
# TELEGRAM
# ═══════════════════════════════════════════════════════════════

def tg_send(text: str, chat_id=None) -> bool:
    target = str(chat_id) if chat_id else CHAT_ID
    try:
        r = session.post(
            f"{TG_BASE}/sendMessage",
            json={
                "chat_id":                  target,
                "text":                     text[:4096],
                "parse_mode":               "HTML",
                "disable_web_page_preview": True,
            },
            timeout=20,
        )
        if r.status_code == 200:
            logger.info(f"TG → {target}: {text[:60]}…")
            return True
        logger.warning(f"TG ошибка {r.status_code}: {r.text[:120]}")
        return False
    except Exception as e:
        logger.exception(f"TG send error: {e}")
        return False


def tg_photo(url: str, caption: str) -> bool:
    try:
        r = session.post(
            f"{TG_BASE}/sendPhoto",
            json={
                "chat_id":    CHAT_ID,
                "photo":      url,
                "caption":    caption[:1024],
                "parse_mode": "HTML",
            },
            timeout=30,
        )
        if r.status_code == 200:
            return True
        logger.warning(f"Фото ошибка: {r.text[:120]}")
    except Exception as e:
        logger.exception(f"Фото error: {e}")
    return tg_send(f'📸 <b>Скриншот рейса</b>\n<a href="{url}">Открыть медиа</a>')


def tg_setup_webhook() -> None:
    url = f"https://{HOSTNAME}/bot/{BOT_TOKEN}"
    try:
        r = session.post(f"{TG_BASE}/setWebhook", json={"url": url}, timeout=20)
        logger.info(f"Webhook: {r.text}")
    except Exception as e:
        logger.exception(f"Webhook error: {e}")

# ═══════════════════════════════════════════════════════════════
# FSA API
# ═══════════════════════════════════════════════════════════════

def fsa_call(function: str, extra: Optional[Dict] = None):
    if not FSA_KEY:
        return None
    params = {
        "function": function,
        "va_id":    FSA_VA_ID,
        "apikey":   FSA_KEY,
        "format":   "json",
    }
    if extra:
        params.update(extra)
    try:
        r = session.get(FSA_URL, params=params, timeout=20)
        r.raise_for_status()
        body = r.json()
        if body.get("status") == "SUCCESS":
            return body.get("data")
        if body.get("status") == "NOT FOUND":
            logger.debug(f"FSA {function}: NOT FOUND")
        else:
            logger.warning(f"FSA {function} ошибка: {body}")
        return None
    except Exception as e:
        logger.exception(f"FSA error: {e}")
        return None


def fsa_daily_transactions() -> List[Dict]:
    """getDailyTransactions — только va_id, API сам отдаёт только сегодня."""
    data = fsa_call("getDailyTransactions")
    if not isinstance(data, list):
        return []
    logger.info(f"fsa_daily_transactions: {len(data)} транзакций")
    return data


def fsa_active_flights() -> List[Dict]:
    data = fsa_call("getActiveFlights")
    return data if isinstance(data, list) else []


def fsa_airline_data() -> Optional[Dict]:
    data = fsa_call("getAirlineData")
    if isinstance(data, list) and data:
        return data[0]
    return data if isinstance(data, dict) else None


def _fetch_profit_and_notify(flight_id: str, pilot: str, flight_no: str) -> None:
    """
    Запрашивает прибыль рейса из FSAirlines в фоновом потоке.
    FSAirlines обрабатывает PIREP 5-15 минут — поэтому delays увеличены.
    Прибыль отправляется отдельным сообщением когда данные появятся.
    """
    # ИСПРАВЛЕНО: было [5, 10, 20] сек — FSAirlines не успевал обработать.
    # Теперь ждём 2 мин, затем 5 мин, затем 10 мин.
    delays = [120, 300, 600]
    for i, delay in enumerate(delays):
        time.sleep(delay)
        data = fsa_call("getReportDetail", {"report_id": flight_id})
        if data:
            try:
                profit = data.get("profit")
                if profit is not None:
                    p = int(float(profit))
                    tg_send(
                        f"💎 <b>ФИНАНСОВЫЙ ОТЧЁТ</b>\n\n"
                        f"👨‍✈️ {pilot}\n"
                        f"🆔 {flight_no}\n"
                        f"💰 Прибыль: <b>{p:+,.0f} v$</b>\n"
                        f"🔗 <a href='https://fshub.io/flight/{flight_id}'>Отчёт о рейсе</a>"
                    )
                    try:
                        db_execute(
                            "UPDATE flights SET profit = %s WHERE flight_id = %s",
                            (p, flight_id),
                        )
                    except Exception as e:
                        logger.warning(f"Не удалось обновить прибыль в БД: {e}")
                    return
            except (ValueError, TypeError) as e:
                logger.warning(f"Ошибка разбора прибыли: {e}")
        logger.debug(f"Попытка {i+1}/{len(delays)} прибыль рейса {flight_id}: ещё не готова")
    logger.info(f"Прибыль рейса {flight_id} недоступна после всех попыток (~17 мин)")

# ═══════════════════════════════════════════════════════════════
# ФИНАНСОВАЯ АНАЛИТИКА
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
        r = t.get("reason") or "Прочее"
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


def snapshot_daily_economy() -> None:
    if not FSA_KEY:
        return
    txs = fsa_daily_transactions()
    if not txs:
        logger.info("snapshot_daily_economy: транзакций нет")
        return
    ag  = _aggregate(txs)
    day = datetime.now().strftime("%Y-%m-%d")
    db_save_daily_economy(
        day=day,
        income=int(ag["inc"]),
        expense=int(ag["exp"]),
        detail={"inc_cat": ag["inc_cat"], "exp_cat": ag["exp_cat"]},
    )
    logger.info(f"Снимок экономики сохранён: {day} net={ag['net']:,.0f}")

# ═══════════════════════════════════════════════════════════════
# ОЦЕНКА ПОСАДКИ
# ═══════════════════════════════════════════════════════════════

def landing_rating(rate: int) -> Tuple[str, str]:
    if rate < -1000: return "ОПАСНАЯ ПОСАДКА",  "🔴"
    if rate < -600:  return "HARD LANDING",      "🟠"
    if rate < -500:  return "FIRM LANDING",      "🟡"
    if rate < -350:  return "STABLE LANDING",    "🟢"
    if rate < -50:   return "SMOOTH LANDING",    "✅"
    return                  "BUTTER LANDING",    "⭐⭐⭐"

# ═══════════════════════════════════════════════════════════════
# ФОРМАТИРОВАНИЕ КОМАНД
# ═══════════════════════════════════════════════════════════════

def fmt_stats() -> str:
    flights = db_all_flights()
    if not flights:
        return "📊 <b>Данные о рейсах отсутствуют.</b>"
    rates = [f["landing_rate"] for f in flights]
    avg   = round(sum(rates) / len(rates))
    return (
        f"📊 <b>ОПЕРАТИВНАЯ СВОДКА VA UP!</b>\n\n"
        f"🛬 Рейсов выполнено: <b>{len(flights)}</b>\n"
        f"📐 Средний landing rate: <b>{avg} fpm</b>"
    )


def fmt_last(limit: int = 5) -> str:
    flights = db_last_flights(limit)
    if not flights:
        return "✈️ Рейсы ещё не зафиксированы."
    lines = []
    for f in flights:
        rating, emoji = landing_rating(f["landing_rate"])
        profit_str = (
            f"\n   💎 Прибыль: {f['profit']:+,.0f} v$"
            if f.get("profit") is not None else ""
        )
        lines.append(
            f"{emoji} <b>{f['flight_no']}</b>\n"
            f"   👨‍✈️ {f['pilot']}\n"
            f"   🗺 {f['departure']} → {f['arrival']}\n"
            f"   📊 {f['landing_rate']} fpm — {rating}{profit_str}"
        )
    return "✈️ <b>ПОСЛЕДНИЕ РЕЙСЫ</b>\n\n" + "\n\n".join(lines)


def fmt_top_landings(limit: int = 10) -> str:
    rows = db_top_landings(limit)
    if not rows:
        return "🏆 Данные о посадках отсутствуют."
    medals = ["🥇", "🥈", "🥉"]
    lines  = []
    for i, row in enumerate(rows, 1):
        prefix = medals[i - 1] if i <= 3 else f"{i}."
        rating, _ = landing_rating(row["landing_rate"])
        lines.append(
            f"{prefix} <b>{row['pilot']}</b> — "
            f"{row['landing_rate']} fpm  <i>({rating})</i>"
        )
    return "🏆 <b>ЛУЧШИЕ ПОСАДКИ</b>\n\n" + "\n".join(lines)


def fmt_top_pilots() -> str:
    flights  = db_all_flights()
    if not flights:
        return "🏆 Данные о рейсах отсутствуют."
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
    top    = sorted(counts.items(), key=lambda x: x[1], reverse=True)[:10]
    medals = {1: "🥇", 2: "🥈", 3: "🥉"}
    lines  = [
        f"{medals.get(i, f'{i}.')} <b>{pilot}</b> — {n} рейсов"
        for i, (pilot, n) in enumerate(top, 1)
    ]
    return "🏆 <b>ЛУЧШИЕ ПИЛОТЫ (7 дней)</b>\n\n" + "\n".join(lines)


def fmt_daily_economy() -> str:
    row = db_get_today_economy()
    if row:
        inc     = row["income"]
        exp     = row["expense"]
        net     = row["net"]
        detail  = row["detail"] or {}
        inc_cat = detail.get("inc_cat", {})
        exp_cat = detail.get("exp_cat", {})
    else:
        txs = fsa_daily_transactions()
        if not txs:
            return "📊 Финансовые данные за сегодня отсутствуют."
        ag = _aggregate(txs)
        inc, exp, net = ag["inc"], ag["exp"], ag["net"]
        inc_cat, exp_cat = ag["inc_cat"], ag["exp_cat"]
    em  = _nem(net)
    msg = (
        f"📊 <b>СУТОЧНЫЙ ФИНАНСОВЫЙ ОТЧЁТ</b>\n\n"
        f"💰 Выручка:  <b>+{inc:,.0f} v$</b>\n"
        f"📉 Расходы:  <b>-{exp:,.0f} v$</b>\n"
        f"{em} <b>Итого: {net:+,.0f} v$</b>\n"
    )
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
    rows = db_get_monthly_economy(days=30)
    if not rows:
        return (
            "📊 Данные за месяц ещё не накоплены.\n\n"
            "ℹ️ История формируется ежедневно в 23:50 UTC."
        )
    total_inc = sum(r["income"]  for r in rows)
    total_exp = sum(r["expense"] for r in rows)
    net       = total_inc - total_exp
    best_row  = max(rows, key=lambda r: r["net"])
    worst_row = min(rows, key=lambda r: r["net"])
    return (
        f"🏆 <b>ЕЖЕМЕСЯЧНЫЙ ФИНАНСОВЫЙ ДАЙДЖЕСТ</b>\n"
        f"📅 {datetime.now().strftime('%B %Y')}\n\n"
        f"💰 Выручка: <b>+{total_inc:,.0f} v$</b>\n"
        f"📉 Расходы: <b>-{total_exp:,.0f} v$</b>\n"
        f"{_nem(net)} <b>Итого: {net:+,.0f} v$</b>\n\n"
        f"🌟 Лучший день:  <b>{best_row['day']}</b> (+{best_row['net']:,.0f} v$)\n"
        f"⚠️ Худший день: <b>{worst_row['day']}</b> ({worst_row['net']:+,.0f} v$)\n\n"
        f"📊 Дней в базе: <b>{len(rows)}</b>"
    )


def fmt_active_flights() -> str:
    flights = fsa_active_flights()
    if not flights:
        return "✈️ Сейчас нет рейсов в воздухе."
    lines = [
        f"✈️ <b>{f.get('number','N/A')}</b>  "
        f"{f.get('departure','???')} → {f.get('arrival','???')}  "
        f"[{f.get('passengers',0)} PAX]"
        for f in flights[:10]
    ]
    return f"🛫 <b>РЕЙСЫ В ВОЗДУХЕ ({len(flights)})</b>\n\n" + "\n".join(lines)


def fmt_va_info() -> str:
    data = fsa_airline_data()
    if not data:
        return "📊 Данные авиакомпании недоступны."
    return (
        f"🏢 <b>ВИРТУАЛЬНАЯ АВИАКОМПАНИЯ UP!</b>\n\n"
        f"📛 Название: <b>{data.get('name', 'N/A')}</b>\n"
        f"💰 Бюджет: <b>{data.get('budget', 0):,.0f} v$</b>\n"
        f"⭐ Репутация: <b>{data.get('reputation', 0)}</b>\n"
        f"📍 База: <b>{data.get('base', 'N/A')}</b>\n"
        f"✈️ ICAO: <b>{data.get('code', 'N/A')}</b>"
    )

# ═══════════════════════════════════════════════════════════════
# ОБРАБОТЧИКИ СОБЫТИЙ FSHUB
# ═══════════════════════════════════════════════════════════════

def handle_departure(data: Dict) -> None:
    d         = data.get("_data", {}) or {}
    user      = d.get("user") or {}
    plan      = d.get("plan") or {}
    aircraft  = d.get("aircraft") or {}
    flight_id = str(d.get("id", ""))
    pilot     = user.get("name", "Unknown")

    # ИСПРАВЛЕНО: если план не загружен — используем FREE-ID вместо N/A
    flight_no = plan.get("flight_no") or f"FREE-{flight_id}"
    dep       = plan.get("departure") or "????"
    arr       = plan.get("arrival")   or "????"

    # Защита 1: по flight_id (FSHub retry)
    if flight_id:
        with departures_lock:
            if flight_id in processed_departures:
                logger.info(f"Дубль departure {flight_id} пропущен")
                return
            processed_departures.append(flight_id)

    # Защита 2: по пилот+маршрут в 10-минутном окне (двойной старт)
    dedup_key = f"{pilot}:{dep}:{arr}"
    now = datetime.utcnow()
    with departures_lock:
        for key, ts in list(_recent_dep_keys):
            if key == dedup_key and (now - ts).seconds < 600:
                logger.info(f"Дубль departure в 10 мин: {dedup_key}")
                return
        _recent_dep_keys.append((dedup_key, now))

    tg_send(
        f"🛫 <b>ВЫЛЕТ ПОДТВЕРЖДЁН</b>\n\n"
        f"👨‍✈️ Captain: <b>{pilot}</b>\n"
        f"🆔 Flight: <b>{flight_no}</b>\n"
        f"🗺 Route: <b>{dep} → {arr}</b>\n"
        f"✈️ Aircraft: <b>{aircraft.get('icao_name', 'N/A')}</b>\n\n"
        f"<i>Попутного ветра и мягкой посадки!</i>"
    )


def handle_arrival(data: Dict) -> None:
    d         = data.get("_data", {}) or {}
    flight_id = str(d.get("id", ""))

    # Дедупликация arrivals
    if flight_id:
        with processed_lock:
            if flight_id in processed_flight_ids:
                logger.info(f"Дубль arrival {flight_id} пропущен")
                return
            processed_flight_ids.append(flight_id)

    user      = d.get("user") or {}
    plan      = d.get("plan") or {}
    aircraft  = d.get("aircraft") or {}
    airport   = d.get("airport") or {}
    rate      = int(d.get("landing_rate", 0))
    pilot     = user.get("name", "Unknown")

    # ИСПРАВЛЕНО: если план не загружен — arrival берём из airport.icao
    flight_no = plan.get("flight_no") or f"FREE-{flight_id}"
    departure = plan.get("departure") or "????"
    arrival   = plan.get("arrival")   or airport.get("icao") or "????"

    rating, emoji = landing_rating(rate)

    db_add_flight(
        flight_id    = flight_id or None,
        pilot        = pilot,
        flight_no    = flight_no,
        departure    = departure,
        arrival      = arrival,
        aircraft     = aircraft.get("icao_name", "Unknown"),
        landing_rate = rate,
        profit       = None,
    )

    tg_send(
        f"🛬 <b>ПОСАДКА ВЫПОЛНЕНА</b> {emoji}\n\n"
        f"👨‍✈️ Captain: <b>{pilot}</b>\n"
        f"🆔 Flight: <b>{flight_no}</b>\n"
        f"🗺 Route: <b>{departure} → {arrival}</b>\n"
        f"📍 Airport: <b>{airport.get('name', arrival)}</b>\n"
        f"✈️ Aircraft: <b>{aircraft.get('icao_name', 'Unknown')}</b>\n"
        f"📊 Landing Rate: <b>{rate} fpm</b>\n"
        f"🎯 Оценка: <b>{rating}</b>\n"
        f"🔗 <a href='https://fshub.io/flight/{flight_id}'>Отчёт о рейсе</a>"
    )

    if rate < -600:
        tg_send(
            f"⚠️ <b>HARD LANDING ALERT</b>\n\n"
            f"👨‍✈️ {pilot}\n"
            f"📊 {rate} fpm\n"
            f"✈️ Рекомендуется инспекция ВС."
        )

    # Прибыль запрашиваем в фоне — не блокируем Flask
    if FSA_KEY and flight_id and flight_id.isdigit():
        threading.Thread(
            target=_fetch_profit_and_notify,
            args=(flight_id, pilot, flight_no),
            daemon=True,
        ).start()


def _send_screenshots_async(screenshots: List) -> None:
    for scr in screenshots[:3]:
        url = scr.get("screenshot_url")
        if url:
            tg_photo(url, "📸 <b>Скриншот рейса</b>")
            time.sleep(1)


def handle_screenshots(data: Dict) -> None:
    screenshots = data.get("_data", [])
    if screenshots:
        threading.Thread(
            target=_send_screenshots_async,
            args=(screenshots,),
            daemon=True,
        ).start()


def handle_achievement(data: Dict) -> None:
    d           = data.get("_data", {}) or {}
    achievement = d.get("achievement") or {}
    flight      = d.get("flight") or {}
    user        = flight.get("user") or {}
    tg_send(
        f"🏆 <b>ДОСТИЖЕНИЕ РАЗБЛОКИРОВАНО</b>\n\n"
        f"👨‍✈️ {user.get('name', 'Unknown')}\n"
        f"🎯 {achievement.get('title', 'Новое достижение')}\n\n"
        f"Поздравляем! 🎉"
    )


FSHUB_HANDLERS = {
    "flight.departed":      handle_departure,
    "flight.arrived":       handle_arrival,
    "screenshots.uploaded": handle_screenshots,
    "airline.achievement":  handle_achievement,
}

# ═══════════════════════════════════════════════════════════════
# TELEGRAM КОМАНДЫ
# ═══════════════════════════════════════════════════════════════

COMMANDS = {
    "/stats":       fmt_stats,
    "/last":        fmt_last,
    "/top_landing": fmt_top_landings,
    "/top":         fmt_top_pilots,
    "/economy":     fmt_daily_economy,
    "/monthly":     fmt_monthly_economy,
    "/live":        fmt_active_flights,
    "/va":          fmt_va_info,
}

HELP_TEXT = (
    "✈️ <b>VA UP! — Панель управления</b>\n\n"
    "📋 <b>Полётные операции:</b>\n"
    "/stats — общая сводка\n"
    "/last — последние рейсы\n"
    "/top — лучшие пилоты (7 дней)\n"
    "/top_landing — лучшие посадки\n"
    "/live — рейсы в воздухе\n\n"
    "💰 <b>Финансы:</b>\n"
    "/economy — отчёт за сегодня\n"
    "/monthly — дайджест за месяц\n\n"
    "🏢 <b>Информация:</b>\n"
    "/va — данные авиакомпании"
)


def handle_tg_command(message: Dict) -> None:
    chat_id = message.get("chat", {}).get("id")
    text    = (message.get("text") or "").strip()

    if not chat_id or not text:
        return
   
    logger.info(f"Команда от {chat_id}: {text}")
    cmd = text.split("@")[0]

    if cmd == "/force_economy" and ADMIN_CHAT_ID and str(chat_id) == ADMIN_CHAT_ID:
        snapshot_daily_economy()
        tg_send("✅ Снимок экономики сохранён.", chat_id)
        return

    if cmd.startswith("/start"):
        tg_send(
            "✈️ <b>Добро пожаловать в VA UP!</b>\n\n"
            "Используйте /help для просмотра команд.",
            chat_id,
        )
        return
    if cmd.startswith("/help"):
        tg_send(HELP_TEXT, chat_id)
        return
    if cmd in COMMANDS:
        try:
            tg_send(COMMANDS[cmd](), chat_id)
        except Exception as e:
            logger.exception(f"Ошибка команды {cmd}: {e}")
            tg_send("⚠️ Ошибка. Попробуйте позже.", chat_id)
        return

    tg_send("Неизвестная команда. /help — список команд.", chat_id)

# ═══════════════════════════════════════════════════════════════
# FLASK
# ═══════════════════════════════════════════════════════════════

app = Flask(__name__)


@app.route("/")
def home():
    return jsonify({"status": "running", "service": "VA UP!", "fsa": bool(FSA_KEY)})


@app.route("/health")
def health():
    try:
        db_execute("SELECT 1", fetch="one")
        return jsonify({"ok": True, "db": "ok"}), 200
    except Exception as e:
        return jsonify({"ok": False, "db": str(e)}), 500


@app.route("/webhook", methods=["GET", "POST"])
def fshub_webhook():
    if request.method == "GET":
        return jsonify({"status": "ok"})
    try:
        data = request.get_json(force=True)
        if not data:
            return jsonify({"error": "no json"}), 400
        event = data.get("_type", "")
        logger.info(f"FSHub: {event}")
        handler = FSHUB_HANDLERS.get(event)
        if handler:
            handler(data)
        return jsonify({"ok": True})
    except Exception as e:
        logger.exception(f"Webhook ошибка: {e}")
        return jsonify({"error": str(e)}), 500


@app.route(f"/bot/{BOT_TOKEN}", methods=["POST"])
def tg_webhook():
    try:
        data    = request.get_json(force=True) or {}
        message = data.get("message") or data.get("channel_post") or {}
        handle_tg_command(message)
    except Exception as e:
        logger.exception(f"TG webhook ошибка: {e}")
    return jsonify({"ok": True})

# ═══════════════════════════════════════════════════════════════
# ПЛАНИРОВЩИК
# ═══════════════════════════════════════════════════════════════

scheduler = BackgroundScheduler(
    executors={"default": ThreadPoolExecutor(max_workers=2)},
    timezone="UTC",
)

# Keepalive БД каждые 5 минут — предотвращает разрыв соединения
scheduler.add_job(
    lambda: db_execute("SELECT 1", fetch="one"),
    "interval", minutes=5, id="db_keepalive",
    replace_existing=True,
)

# Снимок экономики в 23:50 UTC
scheduler.add_job(
    snapshot_daily_economy, "cron",
    hour=23, minute=50, id="daily_economy_snapshot",
    replace_existing=True, misfire_grace_time=300,
)

# Ежедневная сводка в 00:30 UTC
scheduler.add_job(
    lambda: tg_send(fmt_stats()), "cron",
    hour=0, minute=30, id="daily_stats",
    replace_existing=True, misfire_grace_time=300,
)

# Топ посадок — воскресенье 12:00 UTC
scheduler.add_job(
    lambda: tg_send(fmt_top_landings()), "cron",
    day_of_week="sun", hour=12, minute=0, id="weekly_landing_ranking",
    replace_existing=True, misfire_grace_time=300,
)

# Топ пилотов — воскресенье 10:00 UTC
scheduler.add_job(
    lambda: tg_send(fmt_top_pilots()), "cron",
    day_of_week="sun", hour=10, minute=0, id="weekly_top_pilots",
    replace_existing=True, misfire_grace_time=300,
)

# Приглашение на субботний рейс — суббота 06:00 UTC
scheduler.add_job(
    lambda: tg_send(
        "🛫 <b>СУББОТНИЙ СОВМЕСТНЫЙ ВЫЛЕТ!</b>\n\n"
        "⏰ Москва: 09:00 ☀️  |  Камчатка: 18:00 🌙\n\n"
        "✈️ Предлагайте маршруты в комментариях!\n"
        "Кто в экипаже? 👇"
    ),
    "cron", day_of_week="sat", hour=6, minute=0, id="saturday_inv",
    replace_existing=True, misfire_grace_time=300,
)

# Еженедельный вызов — понедельник 08:00 UTC
scheduler.add_job(
    lambda: tg_send(
        "🏆 <b>ЕЖЕНЕДЕЛЬНЫЙ ВЫЗОВ!</b>\n\n"
        "🔹 Цель: 3 рейса за 7 дней\n"
        "🔹 Бонус: лучший landing rate\n\n"
        "Принимаете вызов? 💪"
    ),
    "cron", day_of_week="mon", hour=8, minute=0, id="monday_challenge",
    replace_existing=True, misfire_grace_time=300,
)

# Ежемесячный дайджест — 1-е число 09:00 UTC
scheduler.add_job(
    lambda: tg_send(fmt_monthly_economy()), "cron",
    day=1, hour=9, minute=0, id="monthly_digest",
    replace_existing=True, misfire_grace_time=600,
)

scheduler.start()
logger.info("Планировщик запущен")

# ═══════════════════════════════════════════════════════════════
# СТАРТ
# ═══════════════════════════════════════════════════════════════

try:
    _create_pool()
    _init_db()
    tg_setup_webhook()
except Exception as e:
    logger.exception(f"Ошибка запуска: {e}")
    sys.exit(1)

logger.info(f"Сервис запущен на порту {PORT}")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT, threaded=True)

import os
import sys
import time
import sqlite3
import logging
import threading
from logging.handlers import RotatingFileHandler
from contextlib import closing
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import requests
from flask import Flask, request, jsonify
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ThreadPoolExecutor
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
DB_PATH        = os.environ.get("DB_PATH", "va_up.db")
MAX_DB_FLIGHTS = int(os.environ.get("MAX_DB_FLIGHTS", "5000"))

FSA_URL = "https://www.fsairlines.net/va_interface2.php"
TG_BASE = f"https://api.telegram.org/bot{BOT_TOKEN}"

if not BOT_TOKEN or not CHAT_ID:
    print("❌ TG_BOT_TOKEN or TG_CHAT_ID missing")
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

logger.info("Starting VA UP! Stable Edition")

# ═══════════════════════════════════════════════════════════════
# REQUEST SESSION
# ═══════════════════════════════════════════════════════════════

session = requests.Session()

retry = Retry(
    total=3,
    read=3,
    connect=3,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET", "POST"],
)

adapter = HTTPAdapter(max_retries=retry)
session.mount("https://", adapter)
session.mount("http://", adapter)

# ═══════════════════════════════════════════════════════════════
# DATABASE
# Per-thread connections + WAL mode for safe concurrent access
# ═══════════════════════════════════════════════════════════════

_db_local = threading.local()
_db_lock  = threading.Lock()


def get_conn() -> sqlite3.Connection:
    """Return a per-thread SQLite connection."""
    if not hasattr(_db_local, "conn") or _db_local.conn is None:
        conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        _db_local.conn = conn
    return _db_local.conn


def _init_db():
    conn = get_conn()
    with closing(conn.cursor()) as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS flights (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                flight_id    TEXT UNIQUE,
                pilot        TEXT,
                flight_no    TEXT,
                departure    TEXT,
                arrival      TEXT,
                aircraft     TEXT,
                landing_rate INTEGER,
                profit       INTEGER,
                created_at   TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS achievements (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                pilot       TEXT,
                achievement TEXT,
                created_at  TEXT
            )
            """
        )
        conn.commit()


_init_db()
logger.info("Database initialized")

# ═══════════════════════════════════════════════════════════════
# DATABASE HELPERS
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
    conn = get_conn()
    with _db_lock:
        with closing(conn.cursor()) as cur:
            cur.execute(
                """
                INSERT OR IGNORE INTO flights
                    (flight_id, pilot, flight_no, departure, arrival,
                     aircraft, landing_rate, profit, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    flight_id, pilot, flight_no, departure, arrival,
                    aircraft, landing_rate, profit,
                    datetime.utcnow().isoformat(),
                ),
            )
            conn.commit()

            cur.execute(
                """
                DELETE FROM flights
                WHERE id NOT IN (
                    SELECT id FROM flights ORDER BY id DESC LIMIT ?
                )
                """,
                (MAX_DB_FLIGHTS,),
            )
            conn.commit()


def db_last_flights(limit: int = 5) -> List:
    conn = get_conn()
    with _db_lock:
        with closing(conn.cursor()) as cur:
            cur.execute(
                "SELECT * FROM flights ORDER BY id DESC LIMIT ?", (limit,)
            )
            return cur.fetchall()


def db_all_flights() -> List:
    conn = get_conn()
    with _db_lock:
        with closing(conn.cursor()) as cur:
            cur.execute("SELECT * FROM flights")
            return cur.fetchall()


def db_top_landings(limit: int = 10) -> List:
    conn = get_conn()
    with _db_lock:
        with closing(conn.cursor()) as cur:
            cur.execute(
                "SELECT * FROM flights ORDER BY ABS(landing_rate) ASC LIMIT ?",
                (limit,),
            )
            return cur.fetchall()

# ═══════════════════════════════════════════════════════════════
# TELEGRAM
# ═══════════════════════════════════════════════════════════════

def tg_send(text: str, chat_id: Optional[str] = None) -> bool:
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
        r = session.get(FSA_URL, params=params, timeout=20)
        r.raise_for_status()
        body = r.json()
        if body.get("status") == "SUCCESS":
            return body.get("data")
        logger.warning(f"FSA failure: {body}")
        return None
    except Exception as e:
        logger.exception(f"FSA error: {e}")
        return None


def _safe_timestamp(ts) -> Optional[datetime]:
    """Convert a Unix timestamp to datetime safely."""
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
        dt = _safe_timestamp(t.get("ts"))
        if dt and dt.date() == today:
            result.append(t)
    return result


def fsa_monthly_transactions() -> List[Dict]:
    data = fsa_call("getDailyTransactions")
    if not isinstance(data, list):
        return []
    threshold = datetime.now() - timedelta(days=30)
    result = []
    for t in data:
        dt = _safe_timestamp(t.get("ts"))
        if dt and dt >= threshold:
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
    data = fsa_call("getReportDetail", {"report_id": report_id})
    if not data:
        return None
    # FIX: guard against non-numeric profit value (e.g. "N/A", "")
    try:
        return int(float(data.get("profit", 0)))
    except (ValueError, TypeError):
        logger.warning(f"Non-numeric profit value: {data.get('profit')}")
        return None

# ═══════════════════════════════════════════════════════════════
# STATISTICS & FORMATTING HELPERS
# ═══════════════════════════════════════════════════════════════

def landing_rating(rate: int) -> Tuple[str, str]:
    if rate < -600:
        return "UNSAFE LANDING", "🔴"
    if rate < -400:
        return "HARD LANDING", "🟠"
    if rate < -250:
        return "FIRM LANDING", "🟡"
    if rate < -100:
        return "STABLE LANDING", "🟢"
    return "SMOOTH LANDING", "✅"


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
        "inc": inc,
        "exp": exp,
        "net": inc - exp,
        "inc_cat": inc_cat,
        "exp_cat": exp_cat,
    }


def _top(d: Dict, n: int = 3) -> List[Tuple]:
    return sorted(d.items(), key=lambda x: x[1], reverse=True)[:n]


def _nem(net: float) -> str:
    return "📈" if net > 0 else "📉" if net < 0 else "➡️"

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
        lines.append(
            f"{prefix} <b>{row['pilot']}</b> — {row['landing_rate']} fpm"
        )
    return "🏆 <b>TOP LANDINGS</b>\n\n" + "\n".join(lines)


def fmt_top_pilots() -> str:
    flights = db_all_flights()
    if not flights:
        return "🏆 No flight data available."

    week_ago = datetime.now() - timedelta(days=7)
    counts: Dict[str, int] = {}

    for f in flights:
        try:
            # FIX: explicit except Exception instead of bare except
            date = datetime.fromisoformat(f["created_at"])
            if date >= week_ago:
                counts[f["pilot"]] = counts.get(f["pilot"], 0) + 1
        except Exception:
            pass

    if not counts:
        return "🏆 No flights in the last 7 days."

    sorted_pilots = sorted(counts.items(), key=lambda x: x[1], reverse=True)[:10]
    medals = {1: "🥇", 2: "🥈", 3: "🥉"}
    lines = []
    for i, (pilot, n) in enumerate(sorted_pilots, 1):
        lines.append(f"{medals.get(i, f'{i}.')} <b>{pilot}</b> — {n} flights")

    return "🏆 <b>TOP PILOTS (7 days)</b>\n\n" + "\n".join(lines)


def fmt_daily_economy() -> str:
    txs = fsa_daily_transactions()
    if not txs:
        return "📊 No financial data for today."

    ag = _aggregate(txs)
    em = _nem(ag["net"])

    msg = (
        f"📊 <b>DAILY FINANCIAL REPORT</b>\n\n"
        f"💰 Revenue: <b>+{ag['inc']:,.0f} v$</b>\n"
        f"📉 Expenses: <b>-{ag['exp']:,.0f} v$</b>\n"
        f"{em} <b>Net: {ag['net']:+,.0f} v$</b>\n"
    )
    if ag["inc_cat"]:
        msg += "\n🔝 <b>TOP REVENUE:</b>\n"
        for reason, amount in _top(ag["inc_cat"]):
            msg += f"   • {reason}: <b>+{amount:,.0f} v$</b>\n"
    if ag["exp_cat"]:
        msg += "\n⚠️ <b>TOP EXPENSES:</b>\n"
        for reason, amount in _top(ag["exp_cat"]):
            msg += f"   • {reason}: <b>-{amount:,.0f} v$</b>\n"
    return msg


def fmt_monthly_economy() -> str:
    txs = fsa_monthly_transactions()
    if not txs:
        return "📊 No financial data for the last 30 days."

    total_inc, total_exp = 0.0, 0.0
    daily_net: Dict[str, float] = {}

    for t in txs:
        try:
            value = float(t.get("value", 0))
        except (ValueError, TypeError):
            continue

        dt = _safe_timestamp(t.get("ts"))
        if dt:
            day = dt.strftime("%Y-%m-%d")
            daily_net[day] = daily_net.get(day, 0.0) + value

        if value >= 0:
            total_inc += value
        else:
            total_exp += abs(value)

    net = total_inc - total_exp
    best_day  = max(daily_net.items(), key=lambda x: x[1]) if daily_net else (None, 0)
    worst_day = min(daily_net.items(), key=lambda x: x[1]) if daily_net else (None, 0)

    msg = (
        f"🏆 <b>MONTHLY FINANCIAL DIGEST</b>\n"
        f"📅 {datetime.now().strftime('%B %Y')}\n\n"
        f"💰 Revenue: <b>+{total_inc:,.0f} v$</b>\n"
        f"📉 Expenses: <b>-{total_exp:,.0f} v$</b>\n"
        f"{_nem(net)} <b>Net: {net:+,.0f} v$</b>\n"
    )
    if best_day[0]:
        msg += f"\n🌟 Best Day: <b>{best_day[0]}</b> (+{best_day[1]:,.0f} v$)\n"
    if worst_day[0]:
        msg += f"⚠️ Worst Day: <b>{worst_day[0]}</b> ({worst_day[1]:+,.0f} v$)\n"
    msg += f"\n📊 Days with activity: <b>{len(daily_net)}</b>"
    return msg


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

# ═══════════════════════════════════════════════════════════════
# FSHUB EVENTS
# ═══════════════════════════════════════════════════════════════

def handle_departure(data: Dict):
    d        = data.get("_data", {})
    user     = d.get("user", {})
    plan     = d.get("plan", {})
    aircraft = d.get("aircraft", {})

    tg_send(
        f"🛫 <b>DEPARTURE CONFIRMED</b>\n\n"
        f"👨‍✈️ Captain: <b>{user.get('name', 'Unknown')}</b>\n"
        f"🆔 Flight: <b>{plan.get('flight_no', 'N/A')}</b>\n"
        f"🗺 Route: <b>{plan.get('departure')} → {plan.get('arrival')}</b>\n"
        f"✈️ Aircraft: <b>{aircraft.get('icao_name', 'N/A')}</b>"
    )


def handle_arrival(data: Dict):
    d        = data.get("_data", {})
    user     = d.get("user", {})
    plan     = d.get("plan", {})
    aircraft = d.get("aircraft", {})
    airport  = d.get("airport", {})

    flight_id     = str(d.get("id", ""))
    rate          = int(d.get("landing_rate", 0))
    rating, emoji = landing_rating(rate)

    profit = None
    if FSA_KEY and flight_id and flight_id.isdigit():
        try:
            profit = get_flight_profit(int(flight_id))
        except Exception:
            logger.exception("Failed to get flight profit")

    db_add_flight(
        flight_id    = flight_id or None,
        pilot        = user.get("name", "Unknown"),
        flight_no    = plan.get("flight_no", "N/A"),
        departure    = plan.get("departure", "????"),
        arrival      = plan.get("arrival", "????"),
        aircraft     = aircraft.get("icao_name", "Unknown"),
        landing_rate = rate,
        profit       = profit,
    )

    profit_text = (
        f"\n💎 Profit: <b>{profit:,.0f} v$</b>" if profit is not None else ""
    )
    flight_link = (
        f"\n🔗 <a href='https://fshub.io/flight/{flight_id}'>Open Flight Report</a>"
        if flight_id else ""
    )

    tg_send(
        f"🛬 <b>ARRIVAL CONFIRMED</b> {emoji}\n\n"
        f"👨‍✈️ Captain: <b>{user.get('name', 'Unknown')}</b>\n"
        f"🆔 Flight: <b>{plan.get('flight_no', 'N/A')}</b>\n"
        f"🗺 Route: <b>{plan.get('departure')} → {plan.get('arrival')}</b>\n"
        f"📍 Airport: <b>{airport.get('name', 'Unknown')}</b>\n"
        f"✈️ Aircraft: <b>{aircraft.get('icao_name', 'Unknown')}</b>\n"
        f"📊 Landing Rate: <b>{rate} fpm</b> — {rating}"
        f"{profit_text}"
        f"{flight_link}"
    )

    if rate < -400:
        tg_send(
            f"⚠️ <b>HARD LANDING ALERT</b>\n\n"
            f"👨‍✈️ Pilot: <b>{user.get('name', 'Unknown')}</b>\n"
            f"📊 Landing Rate: <b>{rate} fpm</b>\n"
            f"✈️ Aircraft inspection recommended."
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
    d           = data.get("_data", {})
    achievement = d.get("achievement", {})
    flight      = d.get("flight", {})
    user        = flight.get("user", {})

    tg_send(
        f"🏆 <b>ACHIEVEMENT UNLOCKED</b>\n\n"
        f"👨‍✈️ {user.get('name', 'Unknown')}\n"
        f"🎯 {achievement.get('title', 'Achievement')}"
    )


FSHUB_HANDLERS = {
    "flight.departed":      handle_departure,
    "flight.arrived":       handle_arrival,
    "screenshots.uploaded": handle_screenshots,
    "airline.achievement":  handle_achievement,
}

# ═══════════════════════════════════════════════════════════════
# TELEGRAM COMMANDS
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
    "<b>VA UP! Operations Panel</b>\n\n"
    "📊 <b>Flight Operations:</b>\n"
    "/stats — operations statistics\n"
    "/last — latest flights\n"
    "/top — top pilots (7 days)\n"
    "/top_landing — best landings\n\n"
    "💰 <b>Financial Operations:</b>\n"
    "/economy — daily financial report\n"
    "/monthly — monthly financial digest\n"
    "/live — active flights\n"
    "/va — VA information"
)


def handle_tg_command(message: Dict):
    chat_id = message.get("chat", {}).get("id")
    text    = (message.get("text") or "").strip()

    if not chat_id or not text:
        return

    logger.info(f"Command from {chat_id}: {text}")

    if text.startswith("/start"):
        tg_send("✈️ <b>VA UP! Stable Edition</b>\n\nUse /help for commands.", chat_id)
        return

    if text.startswith("/help"):
        tg_send(HELP_TEXT, chat_id)
        return

    # Strip bot-username suffix, e.g. /stats@MyBot → /stats
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
# FLASK
# ═══════════════════════════════════════════════════════════════

app = Flask(__name__)


@app.route("/")
def home():
    return jsonify({
        "status": "running",
        "service": "VA UP! Stable Edition",
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
        if WEBHOOK_SECRET:
            if request.headers.get("X-Webhook-Secret") != WEBHOOK_SECRET:
                logger.warning("Invalid webhook secret")
                return jsonify({"error": "forbidden"}), 403

        data = request.get_json(force=True)
        if not data:
            return jsonify({"error": "no json"}), 400

        event = data.get("_type", "")
        logger.info(f"FSHub event: {event}")

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
        data    = request.get_json(force=True) or {}
        message = data.get("message") or data.get("channel_post") or {}
        handle_tg_command(message)
    except Exception as e:
        logger.exception(f"Telegram webhook failure: {e}")
    return jsonify({"ok": True})

# ═══════════════════════════════════════════════════════════════
# SCHEDULER
# ═══════════════════════════════════════════════════════════════

scheduler = BackgroundScheduler(
    executors={"default": ThreadPoolExecutor(max_workers=2)},
    timezone="UTC",
)

scheduler.add_job(
    lambda: tg_send(fmt_stats()),
    "cron",
    hour=21, minute=0,
    id="daily_stats",
    replace_existing=True,
    misfire_grace_time=300,
)

scheduler.add_job(
    lambda: tg_send(fmt_top_landings()),
    "cron",
    day_of_week="sun", hour=12, minute=0,
    id="weekly_landing_ranking",
    replace_existing=True,
    misfire_grace_time=300,
)

scheduler.add_job(
    lambda: tg_send(fmt_top_pilots()),
    "cron",
    day_of_week="sun", hour=10, minute=0,
    id="weekly_top_pilots",
    replace_existing=True,
    misfire_grace_time=300,
)

scheduler.start()
logger.info("Scheduler started")

# ═══════════════════════════════════════════════════════════════
# STARTUP
# ═══════════════════════════════════════════════════════════════

try:
    tg_setup_webhook()
    tg_send("🟢 <b>VA UP! Stable Edition online</b>")
except Exception as e:
    logger.exception(f"Startup failed: {e}")

logger.info(f"Running on port {PORT}")

# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT, threaded=True)

"""
VA UP! VATSIM Bot — Unified Operations Bot
FSHub webhook + FSAirlines API + Telegram + Scheduler
"""

import os
import sys
import time
import threading
import traceback
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import Optional, Dict, List, Tuple

import requests
from flask import Flask, request, jsonify
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ThreadPoolExecutor

# ═══════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════

BOT_TOKEN = os.environ.get("TG_BOT_TOKEN", "")
CHAT_ID   = os.environ.get("TG_CHAT_ID", "")
FSA_KEY   = os.environ.get("FSA_API_KEY", "")
FSA_VA_ID = os.environ.get("FSA_VA_ID", "56177")
PORT      = int(os.environ.get("PORT", 10000))
HOSTNAME  = os.environ.get("RENDER_EXTERNAL_HOSTNAME", "fshub-bot.onrender.com")

FSA_URL   = "https://www.fsairlines.net/va_interface2.php"
TG_BASE   = f"https://api.telegram.org/bot{BOT_TOKEN}"

MAX_FLIGHTS = 500

if not BOT_TOKEN or not CHAT_ID:
    print("❌ TG_BOT_TOKEN or TG_CHAT_ID not set")
    sys.exit(1)

print(f"[CONFIG] BOT_TOKEN : {BOT_TOKEN[:10]}… (hidden)")
print(f"[CONFIG] CHAT_ID   : {CHAT_ID}")
if FSA_KEY:
    print(f"[CONFIG] FSA_KEY   : {FSA_KEY[:10]}… (hidden)")
    print(f"[CONFIG] FSA_VA_ID : {FSA_VA_ID}")
else:
    print("[CONFIG] FSA_API_KEY not set — financial features unavailable")

# ═══════════════════════════════════════════════════════════════
# FLIGHT STORAGE (in-memory)
# ═══════════════════════════════════════════════════════════════

@dataclass
class FlightRecord:
    pilot:        str
    flight_no:    str
    departure:    str
    arrival:      str
    landing_rate: int
    date:         datetime
    flight_id:    Optional[str] = None

_flights: List[FlightRecord] = []
_flights_lock = threading.Lock()

def _add_flight(record: FlightRecord) -> None:
    global _flights
    with _flights_lock:
        _flights.append(record)
        if len(_flights) > MAX_FLIGHTS:
            _flights = _flights[-MAX_FLIGHTS:]

def _get_flights() -> List[FlightRecord]:
    with _flights_lock:
        return list(_flights)

def _find_flight(flight_id: str) -> Optional[FlightRecord]:
    with _flights_lock:
        for f in reversed(_flights):
            if str(f.flight_id) == str(flight_id):
                return f
    return None

# ═══════════════════════════════════════════════════════════════
# TELEGRAM
# ═══════════════════════════════════════════════════════════════

def tg_send(text: str, chat_id: Optional[str] = None) -> bool:
    target = str(chat_id) if chat_id else CHAT_ID
    try:
        r = requests.post(f"{TG_BASE}/sendMessage", json={
            "chat_id": target, "text": text, "parse_mode": "HTML",
        }, timeout=15)
        return r.status_code == 200
    except Exception as e:
        print(f"[TG] send error: {e}")
        return False

def tg_photo(url: str, caption: str, retries: int = 2) -> bool:
    for attempt in range(retries + 1):
        try:
            r = requests.post(f"{TG_BASE}/sendPhoto", json={
                "chat_id": CHAT_ID, "photo": url,
                "caption": caption, "parse_mode": "HTML",
            }, timeout=30)
            if r.status_code == 200:
                return True
            print(f"[TG] photo attempt {attempt+1} failed: {r.status_code}")
        except Exception as e:
            print(f"[TG] photo error: {e}")
        if attempt < retries:
            time.sleep(1)
    tg_send(f'📸 <b>Media attachment</b>\n<a href="{url}">View screenshot</a>')
    return False

def tg_setup_webhook() -> None:
    url = f"https://{HOSTNAME}/bot/{BOT_TOKEN}"
    try:
        r = requests.get(f"{TG_BASE}/setWebhook", params={"url": url}, timeout=10)
        result = r.json()
        if result.get("ok"):
            print(f"[TG] Webhook → {url}")
        else:
            print(f"[TG] Webhook failed: {result}")
    except Exception as e:
        print(f"[TG] Webhook error: {e}")

# ═══════════════════════════════════════════════════════════════
# FSAIRLINES API
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
        r = requests.get(FSA_URL, params=params, timeout=15)
        r.raise_for_status()
        body = r.json()
        if body.get("status") == "SUCCESS":
            return body.get("data")
        print(f"[FSA] {function} → {body.get('status')}: {body.get('message','')}")
        return None
    except Exception as e:
        print(f"[FSA] {function} error: {e}")
        return None

def fsa_daily_transactions() -> List[Dict]:
    data = fsa_call("getDailyTransactions")
    if not isinstance(data, list):
        return []
    today = datetime.now().date()
    result = []
    for t in data:
        ts = t.get("ts")
        if ts:
            date = datetime.fromtimestamp(ts).date()
            if date == today:
                result.append(t)
    return result

def fsa_pos_sums() -> List[Dict]:
    data = fsa_call("getPosTransactionSums")
    return data if isinstance(data, list) else []

def fsa_neg_sums() -> List[Dict]:
    data = fsa_call("getNegTransactionSums")
    return data if isinstance(data, list) else []

def fsa_airline_stats() -> Optional[Dict]:
    data = fsa_call("getAirlineStats")
    if isinstance(data, list) and data:
        return data[0]
    return data if isinstance(data, dict) else None

def fsa_active_flights() -> List[Dict]:
    data = fsa_call("getActiveFlights")
    return data if isinstance(data, list) else []

def _aggregate(transactions: List[Dict]) -> Dict:
    inc, exp = 0.0, 0.0
    inc_cat: Dict[str, float] = {}
    exp_cat: Dict[str, float] = {}
    for t in transactions:
        v = float(t.get("value", 0))
        r = t.get("reason") or "Other"
        if v >= 0:
            inc += v
            inc_cat[r] = inc_cat.get(r, 0) + v
        else:
            exp += abs(v)
            exp_cat[r] = exp_cat.get(r, 0) + abs(v)
    return {"inc": inc, "exp": exp, "net": inc - exp,
            "inc_cat": inc_cat, "exp_cat": exp_cat}

def _top(d: Dict, n: int = 3) -> List[Tuple]:
    return sorted(d.items(), key=lambda x: x[1], reverse=True)[:n]

def _nem(net: float) -> str:
    return "📈" if net > 0 else "📉" if net < 0 else "➡️"

# ═══════════════════════════════════════════════════════════════
# STATISTICS (ENGLISH VERSION)
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

def fmt_full_stats() -> str:
    flights    = _get_flights()
    week_ago   = datetime.now() - timedelta(days=7)
    week_count = sum(1 for f in flights if f.date >= week_ago)
    rates      = [f.landing_rate for f in flights]
    avg        = round(sum(rates) / len(rates)) if rates else 0

    return (
        "📊 <b>VA UP! OPERATIONS SUMMARY</b>\n\n"
        f"🛬 Total Flights: <b>{len(flights)}</b>\n"
        f"📅 Last 7 Days: <b>{week_count}</b>\n"
        f"📐 Average Landing Rate: <b>{avg} fpm</b>"
    )

def fmt_top_pilots() -> str:
    week_ago = datetime.now() - timedelta(days=7)
    flights  = [f for f in _get_flights() if f.date >= week_ago]

    if not flights:
        return (
            "🏆 <b>CREW ACTIVITY RANKING</b>\n\n"
            "No flight activity recorded during the last 7 days."
        )

    counts: Dict[str, int] = {}
    for f in flights:
        counts[f.pilot] = counts.get(f.pilot, 0) + 1

    top    = sorted(counts.items(), key=lambda x: x[1], reverse=True)[:10]
    medals = {1: "🥇", 2: "🥈", 3: "🥉"}
    lines  = []

    for i, (pilot, n) in enumerate(top, 1):
        lines.append(f"{medals.get(i, f'{i}.')} <b>{pilot}</b> — {n} flights")

    return "🏆 <b>CREW ACTIVITY RANKING</b>\n\n" + "\n".join(lines)

def fmt_last_flights(limit: int = 5) -> str:
    recent = _get_flights()[-limit:][::-1]

    if not recent:
        return (
            "✈️ <b>LATEST FLIGHT REPORTS</b>\n\n"
            "No completed flights available."
        )

    lines = []
    for f in recent:
        rating, emoji = landing_rating(f.landing_rate)
        lines.append(
            f"{emoji} <b>{f.flight_no}</b> | {f.pilot}\n"
            f"   🛫 {f.departure} → {f.arrival}\n"
            f"   📊 {f.landing_rate} fpm — {rating}"
        )

    return "✈️ <b>LATEST FLIGHT REPORTS</b>\n\n" + "\n\n".join(lines)

def fmt_daily_report() -> str:
    today   = datetime.now().date()
    flights = [f for f in _get_flights() if f.date.date() == today]

    if not flights:
        return (
            "📊 <b>DAILY OPERATIONS SUMMARY</b>\n\n"
            "✈️ No flight operations recorded today."
        )

    counts: Dict[str, int] = {}
    for f in flights:
        counts[f.pilot] = counts.get(f.pilot, 0) + 1

    top_pilot, top_n = max(counts.items(), key=lambda x: x[1])
    avg = round(sum(f.landing_rate for f in flights) / len(flights))

    return (
        f"📊 <b>DAILY OPERATIONS SUMMARY</b>\n\n"
        f"✈️ Flights Completed: <b>{len(flights)}</b>\n"
        f"👨‍✈️ Most Active Pilot: <b>{top_pilot}</b> ({top_n} flights)\n"
        f"📐 Average Landing Rate: <b>{avg} fpm</b>"
    )

# ═══════════════════════════════════════════════════════════════
# FINANCIAL FORMATTING (ENGLISH VERSION)
# ═══════════════════════════════════════════════════════════════

def fmt_daily_economy() -> str:
    txs = fsa_daily_transactions()

    if not txs:
        return (
            "📊 <b>DAILY FINANCIAL REPORT</b>\n\n"
            "No financial transactions available.\n"
            "<i>Rate limit: 500 requests/hour (Gold)</i>"
        )

    ag = _aggregate(txs)
    em = _nem(ag["net"])

    msg = (
        f"📊 <b>DAILY FINANCIAL REPORT</b>\n\n"
        f"💰 Revenue:  <b>+{ag['inc']:,.0f} v$</b>\n"
        f"📉 Expenses: <b>-{ag['exp']:,.0f} v$</b>\n"
        f"{em} <b>Net Result: {ag['net']:+,.0f} v$</b>\n"
    )

    if ag["inc_cat"]:
        msg += "\n🔝 <b>PRIMARY REVENUE SOURCES:</b>\n"
        for reason, amount in _top(ag["inc_cat"]):
            msg += f"   • {reason}: <b>+{amount:,.0f} v$</b>\n"

    if ag["exp_cat"]:
        msg += "\n⚠️ <b>PRIMARY EXPENSE SOURCES:</b>\n"
        for reason, amount in _top(ag["exp_cat"]):
            msg += f"   • {reason}: <b>-{amount:,.0f} v$</b>\n"

    return msg

def fmt_monthly_economy() -> str:
    pos   = fsa_pos_sums()
    neg   = fsa_neg_sums()
    stats = fsa_airline_stats()

    if not pos and not neg:
        return (
            "📊 <b>MONTHLY FINANCIAL SUMMARY</b>\n\n"
            "Financial data unavailable."
        )

    inc = sum(float(t.get("value", 0)) for t in pos)
    exp = sum(abs(float(t.get("value", 0))) for t in neg)
    net = inc - exp
    em  = _nem(net)

    msg = (
        f"📈 <b>MONTHLY FINANCIAL SUMMARY</b>\n"
        f"📅 {datetime.now().strftime('%B %Y')}\n\n"
        f"💰 Total Revenue: <b>+{inc:,.0f} v$</b>\n"
        f"📉 Total Expenses: <b>-{exp:,.0f} v$</b>\n"
        f"{em} <b>Net Balance: {net:+,.0f} v$</b>\n"
    )

    if pos:
        msg += "\n🔝 <b>TOP REVENUE SOURCES:</b>\n"
        for t in sorted(pos, key=lambda x: float(x.get("value", 0)), reverse=True)[:3]:
            msg += f"   • {t.get('reason','?')}: <b>+{float(t.get('value',0)):,.0f} v$</b>\n"

    if stats:
        msg += (
            f"\n📊 <b>FLEET OPERATIONS DATA:</b>\n"
            f"   ✈️ Flights: <b>{stats.get('flights', 0)}</b>\n"
            f"   ⏱ Hours: <b>{float(stats.get('hours', 0)):.0f}</b>\n"
            f"   👥 Passengers: <b>{stats.get('pax', 0)}</b>\n"
            f"   ⭐ Rating: <b>{float(stats.get('rating', 0)):.1f}</b>"
        )

    return msg

def fmt_active_flights() -> str:
    flights = fsa_active_flights()

    if not flights:
        return (
            "✈️ <b>ACTIVE FLIGHT OPERATIONS</b>\n\n"
            "No airborne aircraft at this time."
        )

    lines = []
    for f in flights[:10]:
        lines.append(
            f"✈️ <b>{f.get('number','N/A')}</b>  "
            f"{f.get('departure','???')} → {f.get('arrival','???')}  "
            f"[{f.get('passengers',0)} pax]"
        )

    return f"🛫 <b>AIRBORNE AIRCRAFT ({len(flights)})</b>\n\n" + "\n".join(lines)

# ═══════════════════════════════════════════════════════════════
# FSHUB EVENT HANDLERS (ENGLISH VERSION)
# ═══════════════════════════════════════════════════════════════

def handle_departure(data: Dict) -> None:
    d  = data.get("_data", {})
    pl = d.get("plan", {})

    tg_send(
        f"🛫 <b>DEPARTURE CONFIRMED</b>\n\n"
        f"👨‍✈️ Captain: <b>{d.get('user',{}).get('name','Unknown')}</b>\n"
        f"🆔 Flight: <b>{pl.get('flight_no','N/A')}</b>\n"
        f"🗺 Route: <b>{pl.get('departure','????')} → {pl.get('arrival','????')}</b>\n"
        f"✈️ Aircraft: <b>{d.get('aircraft',{}).get('icao_name','N/A')}</b>\n\n"
        f"🕒 OFF BLOCK: {datetime.utcnow().strftime('%H:%M UTC')}"
    )

def handle_arrival(data: Dict) -> None:
    d    = data.get("_data", {})
    pl   = d.get("plan", {})
    rate = int(d.get("landing_rate", 0))

    rating, emoji = landing_rating(rate)

    _add_flight(FlightRecord(
        pilot        = d.get("user", {}).get("name", "Unknown"),
        flight_no    = pl.get("flight_no", "N/A"),
        departure    = pl.get("departure", "????"),
        arrival      = pl.get("arrival",   "????"),
        landing_rate = rate,
        date         = datetime.now(),
        flight_id    = str(d.get("id")) if d.get("id") else None,
    ))

    tg_send(
        f"🛬 <b>ARRIVAL CONFIRMED</b> {emoji}\n\n"
        f"👨‍✈️ Captain: <b>{d.get('user',{}).get('name','Unknown')}</b>\n"
        f"🆔 Flight: <b>{pl.get('flight_no','N/A')}</b>\n"
        f"🗺 Route: <b>{pl.get('departure','????')} → {pl.get('arrival','????')}</b>\n"
        f"✈️ Aircraft: <b>{d.get('aircraft',{}).get('icao_name','N/A')}</b>\n"
        f"📍 Destination: <b>{d.get('airport',{}).get('name', pl.get('arrival','????'))}</b>\n"
        f"📊 Landing Rate: <b>{rate} fpm</b> — {rating}\n\n"
        f"🕒 ON BLOCK: {datetime.utcnow().strftime('%H:%M UTC')}"
    )

def handle_screenshots(data: Dict) -> None:
    screenshots = data.get("_data", [])

    if not screenshots:
        return

    flight_id = screenshots[0].get("flight_id")
    flight    = _find_flight(str(flight_id)) if flight_id else None

    if flight:
        caption = (
            f"📸 <b>POST-FLIGHT MEDIA</b>\n"
            f"✈️ {flight.departure} → {flight.arrival}\n"
            f"👨‍✈️ {flight.pilot}"
        )
    else:
        caption = f"📸 <b>FLIGHT MEDIA ATTACHMENT</b>\nFlight ID: #{flight_id}"

    for scr in screenshots[:3]:
        if url := scr.get("screenshot_url"):
            tg_photo(url, caption)
            time.sleep(1)

    extra = len(screenshots) - 3
    if extra > 0:
        tg_send(f"📸 <b>{extra} additional media attachment(s)</b>\nFlight ID: #{flight_id}")

def handle_achievement(data: Dict) -> None:
    d = data.get("_data", {})

    tg_send(
        f"🏆 <b>CREW ACHIEVEMENT UNLOCKED</b>\n\n"
        f"👨‍✈️ {d.get('flight',{}).get('user',{}).get('name','Unknown')}\n"
        f"🎯 {d.get('achievement',{}).get('title','Achievement')}\n\n"
        f"Operations Control congratulates the crew."
    )

FSHUB_HANDLERS = {
    "flight.departed":      handle_departure,
    "flight.arrived":       handle_arrival,
    "screenshots.uploaded": handle_screenshots,
    "airline.achievement":  handle_achievement,
}

# ═══════════════════════════════════════════════════════════════
# TELEGRAM COMMANDS (ENGLISH VERSION)
# ═══════════════════════════════════════════════════════════════

_HELP = (
    "<b>VA UP! OPERATIONS PANEL</b>\n\n"
    "📊 <b>Flight Operations:</b>\n"
    "/stats   — fleet activity statistics\n"
    "/top     — crew activity ranking\n"
    "/last    — latest flight reports\n\n"
    "💰 <b>Financial Operations:</b>\n"
    "/economy — daily financial report\n"
    "/monthly — monthly financial summary\n"
    "/live    — active flight operations"
)

TG_COMMANDS = {
    "/stats":   fmt_full_stats,
    "/top":     fmt_top_pilots,
    "/last":    fmt_last_flights,
    "/economy": fmt_daily_economy,
    "/monthly": fmt_monthly_economy,
    "/live":    fmt_active_flights,
}

def handle_tg_command(message: Dict) -> None:
    chat_id = message.get("chat", {}).get("id")
    text    = (message.get("text") or "").strip()
    if not chat_id or not text:
        return
    if str(chat_id) == str(CHAT_ID):
        return

    print(f"[CMD] chat={chat_id} text={text!r}")

    if text == "/start":
        tg_send("✈️ <b>VA UP! Operations Bot</b>\n\nUse /help for command list.", chat_id)
    elif text == "/help":
        tg_send(_HELP, chat_id)
    elif text in TG_COMMANDS:
        tg_send(TG_COMMANDS[text](), chat_id)
    else:
        tg_send("Unknown command. /help — list of commands.", chat_id)

# ═══════════════════════════════════════════════════════════════
# FLASK ROUTES
# ═══════════════════════════════════════════════════════════════

app = Flask(__name__)

@app.route("/webhook", methods=["GET", "POST"])
def fshub_webhook():
    if request.method == "GET":
        return jsonify({"status": "ok"}), 200
    try:
        data = request.get_json(force=True)
        if not data:
            return jsonify({"error": "no JSON"}), 400
        event = data.get("_type", "")
        print(f"[FSHUB] event={event}")
        if handler := FSHUB_HANDLERS.get(event):
            handler(data)
        else:
            print(f"[FSHUB] Unknown event: {event!r}")
        return jsonify({"ok": True}), 200
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@app.route(f"/bot/{BOT_TOKEN}", methods=["POST"])
def tg_webhook():
    try:
        data    = request.get_json(force=True) or {}
        message = data.get("message") or data.get("channel_post") or {}
        handle_tg_command(message)
    except Exception:
        traceback.print_exc()
    return jsonify({"ok": True}), 200

@app.route("/")
def home():
    with _flights_lock:
        n = len(_flights)
    return jsonify({"status": "running", "flights_in_memory": n, "fsa": bool(FSA_KEY)})

# ═══════════════════════════════════════════════════════════════
# SCHEDULER (All times UTC)
# ═══════════════════════════════════════════════════════════════

scheduler = BackgroundScheduler(
    executors={"default": ThreadPoolExecutor(max_workers=2)},
    timezone="UTC",
)

# Daily FSHub stats at 21:00 UTC
scheduler.add_job(lambda: tg_send(fmt_daily_report()),
    "cron", hour=21, minute=0, id="daily_stats")

# Weekly crew ranking on Sunday at 12:00 UTC
scheduler.add_job(lambda: tg_send(fmt_top_pilots()),
    "cron", day_of_week="sun", hour=12, minute=0, id="weekly_top")

# Saturday joint flight invitation at 06:00 UTC (09:00 MSK / 18:00 Kamchatka)
scheduler.add_job(lambda: tg_send(
    "🛫 <b>SATURDAY JOINT FLIGHT OPERATION!</b>\n\n"
    "⏰ Moscow: 09:00 ☀️  |  Kamchatka: 18:00 🌙\n\n"
    "✈️ Suggest your route in the comments!\nWho is joining? 👇"
), "cron", day_of_week="sat", hour=6, minute=0, id="saturday_inv")

# Weekly challenge on Monday at 08:00 UTC (11:00 MSK)
scheduler.add_job(lambda: tg_send(
    "🏆 <b>WEEKLY CREW CHALLENGE!</b>\n\n"
    "🔹 Goal: 3 flights in 7 days\n"
    "🔹 Bonus: Best landing rate of the week\n\nReady to accept? 💪"
), "cron", day_of_week="mon", hour=8, minute=0, id="monday_challenge")

# Daily financial report at 07:00 UTC (10:00 MSK)
if FSA_KEY:
    scheduler.add_job(lambda: tg_send(fmt_daily_economy()),
        "cron", hour=7, minute=0, id="daily_economy")

# Monthly financial digest on the 1st at 21:00 UTC (00:00 MSK on the 2nd)
if FSA_KEY:
    scheduler.add_job(lambda: tg_send(fmt_monthly_economy()),
        "cron", day=1, hour=21, minute=0, id="monthly_digest")

scheduler.start()
print("[SCHED] Scheduler started:")
for job in scheduler.get_jobs():
    print(f"  · {job.id:20s} next={job.next_run_time}")

# ═══════════════════════════════════════════════════════════════
# START
# ═══════════════════════════════════════════════════════════════

tg_setup_webhook()
print(f"[BOT] Started on port {PORT}")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT)

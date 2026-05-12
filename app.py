from flask import Flask, request, jsonify
import requests
import os
import threading
import time
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler

app = Flask(__name__)

BOT_TOKEN = os.environ.get('TG_BOT_TOKEN')
CHAT_ID = os.environ.get('TG_CHAT_ID')

# ───────────────────────────────────────────
# STATE
# ───────────────────────────────────────────

stats = {'flights': []}
stats_lock = threading.Lock()

last_update_id = 0
update_lock = threading.Lock()

processed_updates = set()
processed_lock = threading.Lock()

# ───────────────────────────────────────────
# TELEGRAM SENDING
# ───────────────────────────────────────────

def send_to_telegram(message):
    if not BOT_TOKEN or not CHAT_ID:
        return

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

    try:
        requests.post(url, json={
            "chat_id": CHAT_ID,
            "text": message,
            "parse_mode": "HTML"
        }, timeout=10)
    except Exception as e:
        print(f"[TG ERROR] {e}")


def send_photo(image_url, caption):
    if not BOT_TOKEN or not CHAT_ID:
        return

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto"

    try:
        requests.post(url, json={
            "chat_id": CHAT_ID,
            "photo": image_url,
            "caption": caption,
            "parse_mode": "HTML"
        }, timeout=15)
    except Exception as e:
        print(f"[PHOTO ERROR] {e}")


def send_to_user(chat_id, text):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

    try:
        requests.post(url, json={
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML"
        }, timeout=10)
    except Exception as e:
        print(f"[USER MSG ERROR] {e}")

# ───────────────────────────────────────────
# STATS
# ───────────────────────────────────────────

def get_top_pilots():
    week_ago = datetime.now() - timedelta(days=7)

    with stats_lock:
        week_flights = [f for f in stats['flights'] if f['date'] > week_ago]

    pilot_counts = {}
    for f in week_flights:
        pilot_counts[f['pilot']] = pilot_counts.get(f['pilot'], 0) + 1

    sorted_pilots = sorted(pilot_counts.items(), key=lambda x: x[1], reverse=True)[:10]

    if not sorted_pilots:
        return "🏆 <b>Топ пилотов недели</b>\n\nПока нет рейсов."

    medals = {1: "🥇", 2: "🥈", 3: "🥉"}

    lines = []
    for i, (pilot, count) in enumerate(sorted_pilots, 1):
        medal = medals.get(i, f"{i}.")
        lines.append(f"{medal} <b>{pilot}</b> — {count}")

    return "🏆 <b>Топ пилотов недели</b>\n\n" + "\n".join(lines)


def get_stats():
    with stats_lock:
        total = len(stats['flights'])

    return f"📊 Всего рейсов: <b>{total}</b>"

# ───────────────────────────────────────────
# WEBHOOK EVENTS
# ───────────────────────────────────────────

@app.route('/webhook', methods=['POST'])
def webhook():
    data = request.get_json()
    if not data:
        return jsonify({"error": "no data"}), 400

    event_type = data.get('_type')

    if event_type == 'flight.arrived':
        handle_arrival(data)

    elif event_type == 'flight.departed':
        send_to_telegram("🛫 Рейс начался")

    return jsonify({"ok": True})


def handle_arrival(data):
    d = data.get('_data', {})

    pilot = d.get('user', {}).get('name', 'Unknown')
    plan = d.get('plan', {})
    flight_no = plan.get('flight_no', 'N/A')

    landing_rate = d.get('landing_rate', 0)

    with stats_lock:
        stats['flights'].append({
            "pilot": pilot,
            "flight_no": flight_no,
            "landing_rate": landing_rate,
            "date": datetime.now()
        })

    send_to_telegram(
        f"🛬 <b>Посадка</b>\n"
        f"👨‍✈️ {pilot}\n"
        f"✈️ {flight_no}\n"
        f"📊 {landing_rate} fpm"
    )

# ───────────────────────────────────────────
# TELEGRAM POLLING (FIXED)
# ───────────────────────────────────────────

def poll_telegram():
    global last_update_id

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates"

    try:
        with update_lock:
            offset = last_update_id + 1

        r = requests.get(url, params={"offset": offset, "timeout": 10}, timeout=15)
        data = r.json()

        if not data.get("ok"):
            return

        updates = data.get("result", [])

        max_update_id = last_update_id

        for upd in updates:
            uid = upd["update_id"]

            # дедуп
            with processed_lock:
                if uid in processed_updates:
                    continue
                processed_updates.add(uid)

            max_update_id = max(max_update_id, uid)

            msg = upd.get("message", {})
            chat_id = msg.get("chat", {}).get("id")
            text = msg.get("text", "")

            if not chat_id or str(chat_id) == str(CHAT_ID):
                continue

            if text == "/start":
                send_to_user(chat_id, "✈️ Бот работает")
            elif text == "/stats":
                send_to_user(chat_id, get_stats())
            elif text == "/top":
                send_to_user(chat_id, get_top_pilots())

        # ОБНОВЛЯЕМ ТОЛЬКО ПОСЛЕ ПАЧКИ
        with update_lock:
            last_update_id = max_update_id

    except Exception as e:
        print(f"[POLL ERROR] {e}")


def start_polling():
    while True:
        poll_telegram()
        time.sleep(2)

# ───────────────────────────────────────────
# SCHEDULER
# ───────────────────────────────────────────

scheduler = BackgroundScheduler()

def daily():
    send_to_telegram("📊 Ежедневный отчёт")

scheduler.add_job(daily, "cron", hour=21, minute=0)
scheduler.start()

# ───────────────────────────────────────────
# START
# ───────────────────────────────────────────

threading.Thread(target=start_polling, daemon=True).start()

@app.route("/")
def home():
    return "OK"

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)

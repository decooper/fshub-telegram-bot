from flask import Flask, request, jsonify
import requests
import os
import threading
import time
from datetime import datetime, timedelta

app = Flask(__name__)

BOT_TOKEN = os.environ.get('TG_BOT_TOKEN')
CHAT_ID = os.environ.get('TG_CHAT_ID')

stats = {'flights': []}
last_update_id = 0
stats_lock = threading.Lock()
update_id_lock = threading.Lock()

# ───────────────────────────────────────────
#  ОТПРАВКА
# ───────────────────────────────────────────

def send_to_telegram(message):
    if not BOT_TOKEN or not CHAT_ID:
        return False
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    try:
        requests.post(url, json={
            "chat_id": CHAT_ID,
            "text": message,
            "parse_mode": "HTML"
        }, timeout=10)
        return True
    except Exception as e:
        print(f"[TG ERROR] {e}")
        return False

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
    if not BOT_TOKEN:
        return
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
#  КОМАНДЫ БОТА
# ───────────────────────────────────────────

def get_top_pilots():
    week_ago = datetime.now() - timedelta(days=7)
    with stats_lock:
        week_flights = [f for f in stats['flights'] if f['date'] > week_ago]

    pilot_counts = {}
    for flight in week_flights:
        pilot = flight['pilot']
        pilot_counts[pilot] = pilot_counts.get(pilot, 0) + 1

    sorted_pilots = sorted(pilot_counts.items(), key=lambda x: x[1], reverse=True)[:10]

    if not sorted_pilots:
        return (
            "╔══════════════════════════╗\n"
            "║   🏆 ТОП ПИЛОТОВ НЕДЕЛИ   ║\n"
            "╚══════════════════════════╝\n\n"
            "📭 За эту неделю рейсов пока нет."
        )

    medals = {1: "🥇", 2: "🥈", 3: "🥉"}
    lines = []
    for i, (pilot, count) in enumerate(sorted_pilots, 1):
        medal = medals.get(i, f"  {i}.")
        word = "рейс" if count == 1 else "рейса" if count in (2, 3, 4) else "рейсов"
        lines.append(f"{medal} <b>{pilot}</b> — {count} {word}")

    return (
        "╔══════════════════════════╗\n"
        "║   🏆 ТОП ПИЛОТОВ НЕДЕЛИ   ║\n"
        "╚══════════════════════════╝\n\n"
        + "\n".join(lines)
    )

def get_last_flights(limit=5):
    with stats_lock:
        recent = stats['flights'][-limit:][::-1]

    if not recent:
        return (
            "╔══════════════════════════╗\n"
            "║    ✈️  ПОСЛЕДНИЕ РЕЙСЫ    ║\n"
            "╚══════════════════════════╝\n\n"
            "📭 Завершённых рейсов пока нет."
        )

    lines = []
    for flight in recent:
        rate = flight['landing_rate']
        if rate < -300:
            rating = "⚠️ Жёсткая"
        elif rate < -200:
            rating = "🟡 Средняя"
        elif rate > -50:
            rating = "🌟 Идеальная"
        else:
            rating = "👍 Хорошая"

        lines.append(
            f"┌ ✈️ <b>{flight['flight_no']}</b> · {flight['pilot']}\n"
            f"├ 🛫 {flight['departure']} → {flight['arrival']}\n"
            f"└ 📊 {rate} fpm — {rating}"
        )

    return (
        "╔══════════════════════════╗\n"
        "║    ✈️  ПОСЛЕДНИЕ РЕЙСЫ    ║\n"
        "╚══════════════════════════╝\n\n"
        + "\n\n".join(lines)
    )

def get_stats():
    with stats_lock:
        total = len(stats['flights'])
        week_ago = datetime.now() - timedelta(days=7)
        week_count = sum(1 for f in stats['flights'] if f['date'] > week_ago)
        rates = [f['landing_rate'] for f in stats['flights']]

    avg_rate = round(sum(rates) / len(rates)) if rates else 0

    return (
        "╔══════════════════════════╗\n"
        "║    📊 СТАТИСТИКА VA UP!   ║\n"
        "╚══════════════════════════╝\n\n"
        f"🛬 Всего рейсов: <b>{total}</b>\n"
        f"📅 За неделю: <b>{week_count}</b>\n"
        f"📐 Средняя посадка: <b>{avg_rate} fpm</b>"
    )

# ───────────────────────────────────────────
#  ФОРМАТИРОВАНИЕ СОБЫТИЙ FSHUB
# ───────────────────────────────────────────

def format_flight_departed(data):
    d = data.get('_data', {})
    pilot_name = d.get('user', {}).get('name', 'Неизвестный пилот')
    plan       = d.get('plan', {})
    aircraft   = d.get('aircraft', {})
    airport    = d.get('airport', {})

    flight_no    = plan.get('flight_no', 'N/A')
    departure    = plan.get('departure', '????')
    arrival      = plan.get('arrival', '????')
    aircraft_name = aircraft.get('icao_name') or aircraft.get('icao', 'N/A')
    airport_name = airport.get('name', departure)

    return (
        "╔══════════════════════════╗\n"
        "║     🛫  РЕЙС НАЧАЛСЯ     ║\n"
        "╚══════════════════════════╝\n\n"
        f"👨‍✈️ Пилот:    <b>{pilot_name}</b>\n"
        f"🆔 Рейс:     <b>{flight_no}</b>\n"
        f"🗺 Маршрут:  <b>{departure} → {arrival}</b>\n"
        f"✈️ Борт:     <b>{aircraft_name}</b>\n"
        f"📍 Вылет из: <b>{airport_name}</b>\n\n"
        f"🕒 {datetime.utcnow().strftime('%H:%M UTC')}"
    )

def format_flight_arrived(data):
    d = data.get('_data', {})
    pilot_name   = d.get('user', {}).get('name', 'Неизвестный пилот')
    plan         = d.get('plan', {})
    aircraft     = d.get('aircraft', {})
    airport      = d.get('airport', {})
    landing_rate = d.get('landing_rate', 0)

    flight_no     = plan.get('flight_no', 'N/A')
    departure     = plan.get('departure', '????')
    arrival       = plan.get('arrival', '????')
    aircraft_name = aircraft.get('icao_name') or aircraft.get('icao', 'N/A')
    airport_name  = airport.get('name', arrival)

    with stats_lock:
        stats['flights'].append({
            'flight_no':    flight_no,
            'pilot':        pilot_name,
            'departure':    departure,
            'arrival':      arrival,
            'landing_rate': landing_rate,
            'date':         datetime.now()
        })
        if len(stats['flights']) > 200:
            stats['flights'] = stats['flights'][-200:]

    if landing_rate < -500:
        rating = "💥 Катастрофа!"
    elif landing_rate < -300:
        rating = "⚠️ Жёсткая"
    elif landing_rate < -200:
        rating = "🟡 Средняя"
    elif landing_rate < -50:
        rating = "👍 Хорошая"
    else:
        rating = "🌟 Идеальная!"

    return (
        "╔══════════════════════════╗\n"
        "║    🛬  РЕЙС ЗАВЕРШЁН     ║\n"
        "╚══════════════════════════╝\n\n"
        f"👨‍✈️ Пилот:    <b>{pilot_name}</b>\n"
        f"🆔 Рейс:     <b>{flight_no}</b>\n"
        f"🗺 Маршрут:  <b>{departure} → {arrival}</b>\n"
        f"✈️ Борт:     <b>{aircraft_name}</b>\n"
        f"📍 Прилёт в: <b>{airport_name}</b>\n"
        f"📊 Посадка:  <b>{landing_rate} fpm</b> — {rating}\n\n"
        f"🕒 {datetime.utcnow().strftime('%H:%M UTC')}"
    )

def format_screenshots(data):
    screenshots = data.get('_data', [])
    if not screenshots:
        return
    flight_id = screenshots[0].get('flight_id', 'N/A')
    for i, scr in enumerate(screenshots, 1):
        image_url = scr.get('screenshot_url')
        if image_url:
            caption = (
                f"📸 <b>Скриншот рейса #{flight_id}</b>\n"
                f"🖼 Фото {i} из {len(screenshots)}\n"
                f"🕒 {datetime.utcnow().strftime('%H:%M UTC')}"
            )
            send_photo(image_url, caption)
            time.sleep(0.5)

# ───────────────────────────────────────────
#  POLLING TELEGRAM
# ───────────────────────────────────────────

def poll_telegram():
    global last_update_id
    if not BOT_TOKEN:
        return
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates"
    try:
        with update_id_lock:
            offset = last_update_id + 1

        response = requests.get(url, params={"timeout": 10, "offset": offset}, timeout=15)
        data = response.json()

        if not data.get('ok'):
            return

        for update in data.get('result', []):
            with update_id_lock:
                last_update_id = update['update_id']

            message = update.get('message', {})
            chat_id = message.get('chat', {}).get('id')
            text    = message.get('text', '')

            if not chat_id or str(chat_id) == str(CHAT_ID):
                continue

            if text == '/start':
                send_to_user(chat_id,
                    "╔══════════════════════════╗\n"
                    "║    ✈️  VA UP! BOT         ║\n"
                    "╚══════════════════════════╝\n\n"
                    "Привет! Я бот виртуальной авиакомпании <b>UP!</b>\n\n"
                    "Используй /help для списка команд."
                )
            elif text == '/help':
                send_to_user(chat_id,
                    "╔══════════════════════════╗\n"
                    "║      🤖 КОМАНДЫ БОТА      ║\n"
                    "╚══════════════════════════╝\n\n"
                    "/top — 🏆 топ пилотов недели\n"
                    "/last — ✈️ последние 5 рейсов\n"
                    "/stats — 📊 общая статистика\n"
                    "/help — 🆘 эта справка"
                )
            elif text == '/top':
                send_to_user(chat_id, get_top_pilots())
            elif text == '/last':
                send_to_user(chat_id, get_last_flights())
            elif text == '/stats':
                send_to_user(chat_id, get_stats())

    except Exception as e:
        print(f"[POLL ERROR] {e}")

def start_polling():
    while True:
        poll_telegram()
        time.sleep(3)

# ───────────────────────────────────────────
#  FLASK ENDPOINTS
# ───────────────────────────────────────────

@app.route('/')
def home():
    return '✅ VA UP! Telegram Bot is running'

@app.route('/webhook', methods=['POST'])
def fshub_webhook():
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "No JSON data"}), 400

        event_type = data.get('_type')
        print(f"[EVENT] {event_type}")

        if event_type == 'flight.departed':
            send_to_telegram(format_flight_departed(data))
        elif event_type == 'flight.arrived':
            send_to_telegram(format_flight_arrived(data))
        elif event_type == 'screenshots.uploaded':
            format_screenshots(data)

        return jsonify({"status": "ok"}), 200

    except Exception as e:
        print(f"[WEBHOOK ERROR] {e}")
        return jsonify({"error": str(e)}), 500

# ───────────────────────────────────────────
#  СТАРТ
# ───────────────────────────────────────────

polling_thread = threading.Thread(target=start_polling, daemon=True)
polling_thread.start()

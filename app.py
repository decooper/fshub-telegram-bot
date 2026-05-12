from flask import Flask, request, jsonify
import requests
import os
import threading
import time
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler

app = Flask(__name__)

# ───────────────────────────────────────────
# CONFIG
# ───────────────────────────────────────────

PORT = int(os.environ.get("PORT", 10000))
BOT_TOKEN = os.environ.get('TG_BOT_TOKEN')
CHAT_ID = os.environ.get('TG_CHAT_ID')

if not BOT_TOKEN or not CHAT_ID:
    print("❌ Ошибка: не заданы BOT_TOKEN или CHAT_ID")
    exit(1)

# ───────────────────────────────────────────
# STATE (в памяти, для одного процесса)
# ───────────────────────────────────────────

stats = {'flights': []}
stats_lock = threading.Lock()

# ───────────────────────────────────────────
# TELEGRAM SENDING
# ───────────────────────────────────────────

def send_to_telegram(message):
    if not BOT_TOKEN:
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

def send_photo(image_url, caption, retry=2):
    """Отправляет фото в Telegram с повторными попытками"""
    if not BOT_TOKEN or not image_url:
        return False
    
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto"
    
    for attempt in range(retry + 1):
        try:
            response = requests.post(
                url, 
                json={
                    "chat_id": CHAT_ID,
                    "photo": image_url,
                    "caption": caption,
                    "parse_mode": "HTML"
                }, 
                timeout=30
            )
            if response.status_code == 200:
                print(f"[PHOTO SENT] {caption[:50]}...")
                return True
            else:
                print(f"[PHOTO ERROR] Status {response.status_code}")
        except requests.exceptions.Timeout:
            print(f"[PHOTO TIMEOUT] Попытка {attempt + 1} из {retry + 1}")
            if attempt == retry:
                send_to_telegram(f"📸 <b>Скриншот рейса</b>\n\nНе удалось загрузить фото.\nСсылка: {image_url}")
        except Exception as e:
            print(f"[PHOTO ERROR] {e}")
        
        if attempt < retry:
            time.sleep(1)
    
    return False

def send_to_user(chat_id, text):
    if not BOT_TOKEN:
        return False
    
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    try:
        requests.post(url, json={
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML"
        }, timeout=10)
        return True
    except Exception as e:
        print(f"[USER MSG ERROR] {e}")
        return False

# ───────────────────────────────────────────
# STATISTICS
# ───────────────────────────────────────────

def get_landing_rating(rate):
    if rate < -1000:
        return "💥 Катастрофа", "💀"
    elif rate < -600:
        return "⚠️ Жёсткая", "😬"
    elif rate < -300:
        return "🟡 Средняя", "🤔"
    elif rate < -50:
        return "👍 Хорошая", "😊"
    else:
        return "🌟 Идеальная", "👌"

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
        word = "рейс" if count == 1 else "рейса" if count in (2, 3, 4) else "рейсов"
        lines.append(f"{medal} <b>{pilot}</b> — {count} {word}")

    return "🏆 <b>Топ пилотов недели</b>\n\n" + "\n".join(lines)

def get_last_flights(limit=5):
    with stats_lock:
        recent = stats['flights'][-limit:][::-1]

    if not recent:
        return "✈️ <b>Последние рейсы</b>\n\nЗавершённых рейсов пока нет."

    lines = []
    for flight in recent:
        rating, emoji = get_landing_rating(flight['landing_rate'])
        lines.append(
            f"{emoji} <b>{flight['flight_no']}</b> | {flight['pilot']}\n"
            f"   🛫 {flight['departure']} → {flight['arrival']}\n"
            f"   📊 {flight['landing_rate']} fpm — {rating}"
        )

    return "✈️ <b>Последние рейсы</b>\n\n" + "\n\n".join(lines)

def get_full_stats():
    with stats_lock:
        total = len(stats['flights'])
        week_ago = datetime.now() - timedelta(days=7)
        week_count = sum(1 for f in stats['flights'] if f['date'] > week_ago)
        rates = [f['landing_rate'] for f in stats['flights']] if stats['flights'] else [0]
        avg_rate = round(sum(rates) / len(rates)) if rates else 0

    return (
        "📊 <b>Статистика VA UP!</b>\n\n"
        f"🛬 Всего рейсов: <b>{total}</b>\n"
        f"📅 За неделю: <b>{week_count}</b>\n"
        f"📐 Средняя посадка: <b>{avg_rate} fpm</b>"
    )

def get_daily_report():
    today = datetime.now().date()
    with stats_lock:
        today_flights = [f for f in stats['flights'] if f['date'].date() == today]
    
    if not today_flights:
        return "📊 <b>Статистика за сегодня</b>\n\n✈️ Сегодня рейсов пока нет."
    
    pilot_counts = {}
    for f in today_flights:
        pilot_counts[f['pilot']] = pilot_counts.get(f['pilot'], 0) + 1
    top_pilot = max(pilot_counts.items(), key=lambda x: x[1])
    
    rates = [f['landing_rate'] for f in today_flights]
    avg_rate = round(sum(rates) / len(rates))
    
    return (
        f"📊 <b>Статистика за сегодня</b>\n\n"
        f"✈️ Рейсов: <b>{len(today_flights)}</b>\n"
        f"👨‍✈️ Пилот дня: <b>{top_pilot[0]}</b> ({top_pilot[1]} рейсов)\n"
        f"📊 Средняя посадка: <b>{avg_rate} fpm</b>"
    )

# ───────────────────────────────────────────
# SCHEDULER MESSAGES
# ───────────────────────────────────────────

def send_daily_stats():
    message = get_daily_report()
    send_to_telegram(message)
    print("[SCHEDULER] Daily stats sent")

def send_weekly_top():
    message = get_top_pilots()
    send_to_telegram(message)
    print("[SCHEDULER] Weekly top sent")

def send_flight_invitation():
    message = (
        "🛫 <b>СУББОТНЯЯ БАМБАЛЕЙЛА!</b>\n\n"
        "⏰ <b>Время:</b> то что надо\n"
        "🌍 <b>Отличное время для полёта:</b>\n"
        "   • Москва — 10:00 утра ☀️\n"
        "   • Камчатка — 19:00 вечера 🌙\n\n"
        "✈️ <b>Куда полетим?</b>\n"
        "Предлагайте маршрут!\n\n"
        "Кто полетит? 👇"
    )
    send_to_telegram(message)
    print("[SCHEDULER] Saturday flight invitation sent")

def send_challenge():
    message = (
        "🏆 <b>НЕДЕЛЬНЫЙ ЧЕЛЛЕНДЖ VA UP!</b>\n\n"
        "🔹 <b>Цель:</b> выполнить 3 рейса за 7 дней\n"
        "🔹 <b>Бонус:</b> лучшая посадка недели\n\n"
        "Кто готов? 💪"
    )
    send_to_telegram(message)
    print("[SCHEDULER] Challenge sent")

# ───────────────────────────────────────────
# WEBHOOK EVENTS
# ───────────────────────────────────────────

@app.route('/webhook', methods=['POST'])
def fshub_webhook():
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "no data"}), 400

        event_type = data.get('_type')
        print(f"[FSHUB EVENT] {event_type}")

        if event_type == 'flight.departed':
            handle_departure(data)
        elif event_type == 'flight.arrived':
            handle_arrival(data)
        elif event_type == 'screenshots.uploaded':
            handle_screenshots(data)
        elif event_type == 'airline.achievement':
            handle_achievement(data)

        return jsonify({"ok": True}), 200
    except Exception as e:
        print(f"[WEBHOOK ERROR] {e}")
        return jsonify({"error": str(e)}), 500

def handle_departure(data):
    d = data.get('_data', {})
    pilot = d.get('user', {}).get('name', 'Пилот')
    plan = d.get('plan', {})
    flight_no = plan.get('flight_no', 'N/A')
    departure = plan.get('departure', '????')
    arrival = plan.get('arrival', '????')
    aircraft = d.get('aircraft', {}).get('icao_name', 'N/A')
    
    message = (
        f"🛫 <b>DEPARTURE</b>\n\n"
        f"👨‍✈️ Пилот: <b>{pilot}</b>\n"
        f"🆔 Рейс: <b>{flight_no}</b>\n"
        f"🗺 Маршрут: <b>{departure} → {arrival}</b>\n"
        f"✈️ Борт: <b>{aircraft}</b>\n\n"
        f"🕒 {datetime.utcnow().strftime('%H:%M UTC')}"
    )
    send_to_telegram(message)

def handle_arrival(data):
    d = data.get('_data', {})
    pilot = d.get('user', {}).get('name', 'Пилот')
    plan = d.get('plan', {})
    flight_no = plan.get('flight_no', 'N/A')
    departure = plan.get('departure', '????')
    arrival = plan.get('arrival', '????')
    landing_rate = d.get('landing_rate', 0)
    aircraft = d.get('aircraft', {}).get('icao_name', 'N/A')
    airport = d.get('airport', {}).get('name', arrival)
    flight_id = d.get('id')
    
    rating, emoji = get_landing_rating(landing_rate)
    
    with stats_lock:
        stats['flights'].append({
            'pilot': pilot,
            'flight_no': flight_no,
            'departure': departure,
            'arrival': arrival,
            'landing_rate': landing_rate,
            'date': datetime.now(),
            'flight_id': flight_id
        })
        if len(stats['flights']) > 500:
            stats['flights'] = stats['flights'][-500:]
    
    message = (
        f"🛬 <b>ARRIVAL</b> {emoji}\n\n"
        f"👨‍✈️ Пилот: <b>{pilot}</b>\n"
        f"🆔 Рейс: <b>{flight_no}</b>\n"
        f"🗺 Маршрут: <b>{departure} → {arrival}</b>\n"
        f"✈️ Борт: <b>{aircraft}</b>\n"
        f"📍 Прилёт: <b>{airport}</b>\n"
        f"📊 Посадка: <b>{landing_rate} fpm</b> — {rating}\n\n"
        f"🕒 {datetime.utcnow().strftime('%H:%M UTC')}"
    )
    send_to_telegram(message)

def handle_screenshots(data):
    screenshots = data.get('_data', [])
    if not screenshots:
        return
    
    first = screenshots[0]
    flight_id = first.get('flight_id')
    
    # Пытаемся найти рейс в сохранённой статистике
    flight_info = None
    with stats_lock:
        for f in stats['flights']:
            if str(f.get('flight_id')) == str(flight_id):
                flight_info = f
                break
    
    if flight_info:
        pilot = flight_info.get('pilot', 'Пилот')
        dep = flight_info.get('departure', '???')
        arr = flight_info.get('arrival', '???')
        flight_no = flight_info.get('flight_no', flight_id)
        caption = f"📸 <b>Скриншот рейса {flight_no}</b>\n✈️ {dep} → {arr}\n👨‍✈️ {pilot}"
    else:
        caption = f"📸 <b>Скриншот рейса #{flight_id}</b>"
    
    for scr in screenshots[:3]:
        image_url = scr.get('screenshot_url')
        if image_url:
            send_photo(image_url, caption)
            time.sleep(1)
    
    if len(screenshots) > 3:
        send_to_telegram(f"📸 <b>И ещё {len(screenshots) - 3} скриншотов</b> к рейсу #{flight_id}")

def handle_achievement(data):
    d = data.get('_data', {})
    achievement = d.get('achievement', {})
    flight = d.get('flight', {})
    pilot = flight.get('user', {}).get('name', 'Пилот')
    title = achievement.get('title', 'Достижение')
    send_to_telegram(f"🏆 <b>АЧИВКА!</b>\n\n👨‍✈️ {pilot}\n🎯 {title}\n\nПоздравляем!")

# ───────────────────────────────────────────
# TELEGRAM WEBHOOK (COMMANDS)
# ───────────────────────────────────────────

@app.route(f'/bot/{BOT_TOKEN}', methods=['POST'])
def telegram_webhook():
    try:
        data = request.get_json()
        if not data or 'message' not in data:
            return jsonify({"ok": True}), 200
        
        message = data['message']
        chat_id = message.get('chat', {}).get('id')
        text = message.get('text', '')
        
        if not chat_id or str(chat_id) == str(CHAT_ID):
            return jsonify({"ok": True}), 200
        
        if text == '/start':
            send_to_user(chat_id, "✈️ <b>VA UP! Bot</b>\n\nИспользуй /help для списка команд")
        elif text == '/help':
            send_to_user(chat_id, "/stats — статистика\n/top — топ пилотов\n/last — последние рейсы")
        elif text == '/stats':
            send_to_user(chat_id, get_full_stats())
        elif text == '/top':
            send_to_user(chat_id, get_top_pilots())
        elif text == '/last':
            send_to_user(chat_id, get_last_flights())
        
        return jsonify({"ok": True}), 200
    except Exception as e:
        print(f"[TG WEBHOOK ERROR] {e}")
        return jsonify({"ok": True}), 200

@app.route('/')
def home():
    return '✅ VA UP! Bot is running'

# ───────────────────────────────────────────
# SETUP TELEGRAM WEBHOOK
# ───────────────────────────────────────────

def setup_telegram_webhook():
    hostname = os.environ.get('RENDER_EXTERNAL_HOSTNAME', 'fshub-bot.onrender.com')
    webhook_url = f"https://{hostname}/bot/{BOT_TOKEN}"
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/setWebhook?url={webhook_url}"
    try:
        response = requests.get(url, timeout=10)
        print(f"[TG WEBHOOK] {response.json()}")
    except Exception as e:
        print(f"[TG WEBHOOK ERROR] {e}")

# ───────────────────────────────────────────
# SCHEDULER
# ───────────────────────────────────────────

scheduler = BackgroundScheduler()

# Ежедневная статистика в 21:00 UTC
scheduler.add_job(func=send_daily_stats, trigger="cron", hour=21, minute=0)

# Еженедельный топ пилотов в воскресенье в 12:00 UTC
scheduler.add_job(func=send_weekly_top, trigger="cron", day_of_week="sun", hour=12, minute=0)

# Приглашение на совместный полёт — ТОЛЬКО ПО СУББОТАМ в 07:00 UTC
scheduler.add_job(func=send_flight_invitation, trigger="cron", day_of_week="sat", hour=7, minute=0)

# Челлендж на неделю (каждый понедельник в 08:00 UTC)
scheduler.add_job(func=send_challenge, trigger="cron", day_of_week="mon", hour=8, minute=0)

scheduler.start()

# ───────────────────────────────────────────
# START
# ───────────────────────────────────────────

setup_telegram_webhook()

print(f"[BOT] Started on port {PORT}")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT)

from flask import Flask, request, jsonify
import requests
import os
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
# STATE (в памяти, но для одного worker'a)
# ───────────────────────────────────────────

stats = {'flights': []}
stats_lock = None  # не нужен при одном процессе

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

def send_photo(image_url, caption):
    if not BOT_TOKEN or not image_url:
        return False
    
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto"
    try:
        requests.post(url, json={
            "chat_id": CHAT_ID,
            "photo": image_url,
            "caption": caption,
            "parse_mode": "HTML"
        }, timeout=15)
        return True
    except Exception as e:
        print(f"[PHOTO ERROR] {e}")
        send_to_telegram(f"📸 Скриншот: {image_url}")
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
    if rate < -600:
        return "💥 Катастрофа", "💀"
    elif rate < -400:
        return "⚠️ Жёсткая", "😬"
    elif rate < -300:
        return "🟡 Средняя", "🤔"
    elif rate < -50:
        return "👍 Хорошая", "😊"
    else:
        return "🌟 Идеальная", "👌"

def get_top_pilots():
    week_ago = datetime.now() - timedelta(days=7)
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
# SCHEDULER
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
        "🛫 <b>СОВМЕСТНЫЙ ПОЛЁТ</b>\n\n"
        "⏰ <b>Время:</b> отличное время для полёта \n"
        "   • Москва — 10:00 утра ☀️\n"
        "   • Камчатка — 19:00 вечера 🌙\n\n"
        "✈️ <b>Куда полетим?</b>\n"
        "Предлагайте маршрут!\n\n"
        "Кто присоединится? 👇"
    )
    send_to_telegram(message)
    print("[SCHEDULER] Flight invitation sent")

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
# WEBHOOKS
# ───────────────────────────────────────────

@app.route('/webhook', methods=['POST'])
def fshub_webhook():
    """Принимает события от FSHub"""
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

@app.route(f'/bot/{BOT_TOKEN}', methods=['POST'])
def telegram_webhook():
    """Принимает команды от Telegram (вместо polling)"""
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

def handle_departure(data):
    d = data.get('_data', {})
    pilot = d.get('user', {}).get('name', 'Пилот')
    plan = d.get('plan', {})
    flight_no = plan.get('flight_no', 'N/A')
    departure = plan.get('departure', '????')
    arrival = plan.get('arrival', '????')
    
    message = f"🛫 <b>DEPARTURE</b>\n\n👨‍✈️ {pilot}\n🆔 {flight_no}\n🗺 {departure} → {arrival}"
    send_to_telegram(message)

def handle_arrival(data):
    d = data.get('_data', {})
    pilot = d.get('user', {}).get('name', 'Пилот')
    plan = d.get('plan', {})
    flight_no = plan.get('flight_no', 'N/A')
    departure = plan.get('departure', '????')
    arrival = plan.get('arrival', '????')
    landing_rate = d.get('landing_rate', 0)
    
    rating, emoji = get_landing_rating(landing_rate)
    
    stats['flights'].append({
        'pilot': pilot, 'flight_no': flight_no,
        'departure': departure, 'arrival': arrival,
        'landing_rate': landing_rate, 'date': datetime.now()
    })
    if len(stats['flights']) > 500:
        stats['flights'] = stats['flights'][-500:]
    
    message = f"🛬 <b>ARRIVAL</b> {emoji}\n\n👨‍✈️ {pilot}\n🆔 {flight_no}\n🗺 {departure} → {arrival}\n📊 {landing_rate} fpm — {rating}"
    send_to_telegram(message)

def handle_screenshots(data):
    screenshots = data.get('_data', [])
    if not screenshots:
        return
    flight_id = screenshots[0].get('flight_id', 'N/A')
    for scr in screenshots[:3]:
        url = scr.get('screenshot_url')
        if url:
            send_photo(url, f"📸 <b>Рейс #{flight_id}</b>")

def handle_achievement(data):
    d = data.get('_data', {})
    achievement = d.get('achievement', {})
    flight = d.get('flight', {})
    pilot = flight.get('user', {}).get('name', 'Пилот')
    title = achievement.get('title', 'Достижение')
    send_to_telegram(f"🏆 <b>ДОСТИЖЕНИЕ!</b>\n\n👨‍✈️ {pilot}\n🎯 {title}\n\nПоздравляем!")

@app.route('/')
def home():
    return '✅ VA UP! Bot is running'

# ───────────────────────────────────────────
# SETUP TELEGRAM WEBHOOK
# ───────────────────────────────────────────

def setup_telegram_webhook():
    """Устанавливает webhook для Telegram (один раз при запуске)"""
    webhook_url = f"https://{os.environ.get('RENDER_EXTERNAL_HOSTNAME', 'fshub-bot.onrender.com')}/bot/{BOT_TOKEN}"
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/setWebhook?url={webhook_url}"
    try:
        response = requests.get(url, timeout=10)
        print(f"[TG WEBHOOK] {response.json()}")
    except Exception as e:
        print(f"[TG WEBHOOK ERROR] {e}")

# ───────────────────────────────────────────
# START
# ───────────────────────────────────────────

# Запускаем планировщик
scheduler = BackgroundScheduler()
scheduler.add_job(func=send_daily_stats, trigger="cron", hour=21, minute=0)
scheduler.add_job(func=send_weekly_top, trigger="cron", day_of_week="sun", hour=12, minute=0)
scheduler.add_job(func=send_flight_invitation, trigger="cron", hour=7, minute=0)
scheduler.add_job(func=send_challenge, trigger="cron", day_of_week="mon", hour=8, minute=0)
scheduler.start()

# Устанавливаем webhook для Telegram
setup_telegram_webhook()

print(f"[BOT] Started on port {PORT}")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT)

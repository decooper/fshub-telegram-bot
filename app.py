from flask import Flask, request, jsonify
import requests
import os
import threading
import time
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
import json

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
# STATE (in-memory с защитой)
# ───────────────────────────────────────────

stats = {
    'flights': [],
    'daily': {}
}
stats_lock = threading.Lock()

# Для совместных полётов
group_flights = {
    'active': False,
    'participants': [],
    'route': None,
    'datetime': None
}
group_lock = threading.Lock()

# Telegram polling state
last_update_id = 0
update_lock = threading.Lock()
processed_updates = set()
processed_lock = threading.Lock()

# ───────────────────────────────────────────
# TELEGRAM SENDING (с защитой)
# ───────────────────────────────────────────

def send_to_telegram(message, parse_mode='HTML'):
    if not BOT_TOKEN or not CHAT_ID:
        return False
    
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    try:
        response = requests.post(url, json={
            "chat_id": CHAT_ID,
            "text": message,
            "parse_mode": parse_mode
        }, timeout=10)
        return response.status_code == 200
    except Exception as e:
        print(f"[TG ERROR] {e}")
        return False

def send_photo(image_url, caption):
    if not BOT_TOKEN or not CHAT_ID or not image_url:
        return False
    
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto"
    try:
        response = requests.post(url, json={
            "chat_id": CHAT_ID,
            "photo": image_url,
            "caption": caption,
            "parse_mode": "HTML"
        }, timeout=15)
        return response.status_code == 200
    except Exception as e:
        print(f"[PHOTO ERROR] {e}")
        # Fallback: отправляем ссылку текстом
        send_to_telegram(f"📸 Скриншот (не удалось отправить картинку): {image_url}")
        return False

def send_to_user(chat_id, text):
    if not BOT_TOKEN:
        return False
    
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    try:
        response = requests.post(url, json={
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML"
        }, timeout=10)
        return response.status_code == 200
    except Exception as e:
        print(f"[USER MSG ERROR] {e}")
        return False

def send_keyboard(chat_id, text, buttons):
    """Отправляет сообщение с кнопками"""
    if not BOT_TOKEN:
        return False
    
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    keyboard = {
        "inline_keyboard": [[{"text": btn, "callback_data": f"join_{btn}"}] for btn in buttons]
    }
    try:
        response = requests.post(url, json={
            "chat_id": chat_id,
            "text": text,
            "reply_markup": keyboard,
            "parse_mode": "HTML"
        }, timeout=10)
        return response.status_code == 200
    except Exception as e:
        print(f"[KEYBOARD ERROR] {e}")
        return False

# ───────────────────────────────────────────
# STATISTICS (умная аналитика)
# ───────────────────────────────────────────

def get_landing_rating(rate):
    if rate < -600:
        return ("💥 Катастрофа", "💀")
    elif rate < -400:
        return ("⚠️ Жёсткая", "😬")
    elif rate < -300:
        return ("🟡 Средняя", "🤔")
    elif rate < -50:
        return ("👍 Хорошая", "😊")
    else:
        return ("🌟 Идеальная", "👌")

def get_top_pilots():
    week_ago = datetime.now() - timedelta(days=7)
    with stats_lock:
        week_flights = [f for f in stats['flights'] if f['date'] > week_ago]

    pilot_counts = {}
    for f in week_flights:
        pilot_counts[f['pilot']] = pilot_counts.get(f['pilot'], 0) + 1

    sorted_pilots = sorted(pilot_counts.items(), key=lambda x: x[1], reverse=True)[:10]

    if not sorted_pilots:
        return "🏆 <b>Топ пилотов недели</b>\n\nПока нет рейсов за эту неделю."

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

def get_daily_report():
    today = datetime.now().date()
    yesterday = today - timedelta(days=1)
    
    with stats_lock:
        today_flights = [f for f in stats['flights'] if f['date'].date() == today]
        yesterday_flights = [f for f in stats['flights'] if f['date'].date() == yesterday]
    
    today_count = len(today_flights)
    yesterday_count = len(yesterday_flights)
    
    if today_count == 0:
        return "📊 <b>Статистика за сегодня</b>\n\n✈️ Сегодня рейсов пока нет. Самое время поднять шасси в небо!"
    
    # Топ пилота дня
    pilot_counts = {}
    for f in today_flights:
        pilot_counts[f['pilot']] = pilot_counts.get(f['pilot'], 0) + 1
    top_pilot = max(pilot_counts.items(), key=lambda x: x[1]) if pilot_counts else (None, 0)
    
    # Средняя посадка
    rates = [f['landing_rate'] for f in today_flights]
    avg_rate = round(sum(rates) / len(rates))
    
    # Динамика
    trend = "📈" if today_count > yesterday_count else "📉" if today_count < yesterday_count else "➡️"
    
    message = (
        f"📊 <b>Ежедневный отчёт VA UP!</b>\n\n"
        f"✈️ Рейсов сегодня: <b>{today_count}</b> {trend}\n"
        f"👨‍✈️ Пилот дня: <b>{top_pilot[0]}</b> ({top_pilot[1]} рейсов)\n"
        f"📊 Средняя посадка: <b>{avg_rate} fpm</b>\n"
    )
    
    if yesterday_count > 0:
        change = today_count - yesterday_count
        message += f"📅 По сравнению со вчера: {'+' if change > 0 else ''}{change} рейсов\n"
    
    message += f"\n🙏 Спасибо пилотам за отличную работу!"
    return message

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

# ───────────────────────────────────────────
# GROUP FLIGHT SYSTEM
# ───────────────────────────────────────────

def create_group_flight(route, time_str):
    global group_flights
    with group_lock:
        group_flights = {
            'active': True,
            'participants': [],
            'route': route,
            'datetime': time_str,
            'created_at': datetime.now()
        }
    
    message = (
        f"🛫 <b>СОВМЕСТНЫЙ ПОЛЁТ</b>\n\n"
        f"🗺 Маршрут: <b>{route}</b>\n"
        f"⏰ Время: <b>{time_str}</b>\n\n"
        f"Кто присоединяется? Нажмите кнопку!"
    )
    send_to_telegram(message)
    
    # Отправляем с кнопкой в личку админу для управления
    # (пока просто пост)

def join_group_flight(username):
    with group_lock:
        if not group_flights['active']:
            return False
        if username not in group_flights['participants']:
            group_flights['participants'].append(username)
    return True

def get_group_flight_status():
    with group_lock:
        if not group_flights['active']:
            return None
        return {
            'route': group_flights['route'],
            'datetime': group_flights['datetime'],
            'count': len(group_flights['participants']),
            'participants': group_flights['participants'][:10]
        }

# ───────────────────────────────────────────
# SCHEDULER MESSAGES
# ───────────────────────────────────────────

def send_morning_invitation():
    message = (
        "🛫 <b>СОВМЕСТНЫЙ ПОЛЁТ</b>\n\n"
        "⏰ <b>Время:</b> сегодня, договариваемся в комментариях\n"
        "🌍 <b>Отличное время для всех:</b>\n"
        "   • Москва — 10:00 утра ☀️\n"
        "   • Камчатка — 19:00 вечера 🌙\n"
        "   • Калининград — 09:00 утра ☀️\n\n"
        "✈️ <b>Куда полетим?</b>\n"
        "Предлагайте маршруты в комментариях!\n\n"
        "Кто присоединится — отмечайтесь 👇"
    )
    send_to_telegram(message)
    print("[SCHEDULER] Morning invitation sent")

def send_daily_stats():
    message = get_daily_report()
    send_to_telegram(message)
    print("[SCHEDULER] Daily stats sent")

def send_weekly_top():
    message = get_top_pilots()
    send_to_telegram(message)
    print("[SCHEDULER] Weekly top sent")

def send_challenge():
    message = (
        "🏆 <b>НЕДЕЛЬНЫЙ ЧЕЛЛЕНДЖ VA UP!</b>\n\n"
        "🔹 <b>Цель:</b> выполнить 3 рейса за 7 дней\n"
        "🔹 <b>Бонус:</b> лучшая посадка недели\n"
        "🔹 <b>Приз:</b> упоминание в топе пилотов\n\n"
        "Кто готов принять вызов? Отмечайтесь! 💪"
    )
    send_to_telegram(message)
    print("[SCHEDULER] Challenge sent")

# ───────────────────────────────────────────
# WEBHOOK EVENTS
# ───────────────────────────────────────────

@app.route('/webhook', methods=['POST'])
def webhook():
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "no data"}), 400

        event_type = data.get('_type')
        print(f"[EVENT] {event_type}")

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
    pilot = d.get('user', {}).get('name', 'Неизвестный пилот')
    plan = d.get('plan', {})
    flight_no = plan.get('flight_no', 'N/A')
    departure = plan.get('departure', '????')
    arrival = plan.get('arrival', '????')
    aircraft = d.get('aircraft', {}).get('icao_name', 'N/A')
    
    message = (
        f"🛫 <b>РЕЙС НАЧАЛСЯ</b>\n\n"
        f"👨‍✈️ Пилот: <b>{pilot}</b>\n"
        f"🆔 Рейс: <b>{flight_no}</b>\n"
        f"🗺 Маршрут: <b>{departure} → {arrival}</b>\n"
        f"✈️ Борт: <b>{aircraft}</b>\n\n"
        f"🕒 {datetime.utcnow().strftime('%H:%M UTC')}"
    )
    send_to_telegram(message)

def handle_arrival(data):
    d = data.get('_data', {})
    pilot = d.get('user', {}).get('name', 'Неизвестный пилот')
    plan = d.get('plan', {})
    flight_no = plan.get('flight_no', 'N/A')
    departure = plan.get('departure', '????')
    arrival = plan.get('arrival', '????')
    landing_rate = d.get('landing_rate', 0)
    aircraft = d.get('aircraft', {}).get('icao_name', 'N/A')
    airport = d.get('airport', {}).get('name', arrival)
    
    rating, emoji = get_landing_rating(landing_rate)
    
    with stats_lock:
        stats['flights'].append({
            'pilot': pilot,
            'flight_no': flight_no,
            'departure': departure,
            'arrival': arrival,
            'landing_rate': landing_rate,
            'date': datetime.now()
        })
        # Оставляем только последние 500 записей
        if len(stats['flights']) > 500:
            stats['flights'] = stats['flights'][-500:]
    
    message = (
        f"🛬 <b>РЕЙС ЗАВЕРШЁН</b> {emoji}\n\n"
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
    
    flight_id = screenshots[0].get('flight_id', 'N/A')
    count = len(screenshots)
    
    # Отправляем первые 3 скриншота
    for i, scr in enumerate(screenshots[:3], 1):
        image_url = scr.get('screenshot_url')
        if image_url:
            caption = f"📸 <b>Скриншот рейса #{flight_id}</b>\n🖼 {i}/{count}\n🕒 {datetime.utcnow().strftime('%H:%M UTC')}"
            send_photo(image_url, caption)
            time.sleep(0.5)
    
    if count > 3:
        send_to_telegram(f"📸 <b>Ещё скриншоты</b>\n\nВсего загружено: {count} шт.\nСмотреть все: https://fshub.io/flight/{flight_id}")

def handle_achievement(data):
    d = data.get('_data', {})
    achievement = d.get('achievement', {})
    flight = d.get('flight', {})
    user = flight.get('user', {})
    pilot = user.get('name', 'Пилот')
    title = achievement.get('title', 'Достижение')
    description = achievement.get('description', '')
    
    message = (
        f"🏆 <b>ДОСТИЖЕНИЕ ПОЛУЧЕНО!</b>\n\n"
        f"👨‍✈️ <b>{pilot}</b>\n"
        f"🎯 {title}\n"
        f"📖 {description}\n\n"
        f"🎉 Поздравляем!"
    )
    send_to_telegram(message)

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

            # Очищаем старые processed_updates (не больше 1000)
            with processed_lock:
                if len(processed_updates) > 1000:
                    processed_updates.clear()

            if text == "/start":
                send_to_user(chat_id, 
                    "✈️ <b>VA UP! Bot</b>\n\n"
                    "Привет! Я бот виртуальной авиакомпании UP!\n\n"
                    "📋 Доступные команды:\n"
                    "/stats — общая статистика\n"
                    "/top — топ пилотов недели\n"
                    "/last — последние 5 рейсов\n"
                    "/help — эта справка"
                )
            elif text == "/help":
                send_to_user(chat_id,
                    "🤖 <b>Команды бота</b>\n\n"
                    "/stats 📊 общая статистика\n"
                    "/top 🏆 топ пилотов недели\n"
                    "/last ✈️ последние 5 рейсов\n"
                    "/help — эта справка"
                )
            elif text == "/stats":
                send_to_user(chat_id, get_full_stats())
            elif text == "/top":
                send_to_user(chat_id, get_top_pilots())
            elif text == "/last":
                send_to_user(chat_id, get_last_flights())

        with update_lock:
            last_update_id = max_update_id

    except Exception as e:
        print(f"[POLL ERROR] {e}")

def start_polling():
    while True:
        poll_telegram()
        time.sleep(2)

# ───────────────────────────────────────────
# INIT SCHEDULER
# ───────────────────────────────────────────

scheduler = BackgroundScheduler()

# Ежедневная статистика в 21:00 UTC
scheduler.add_job(func=send_daily_stats, trigger="cron", hour=21, minute=0)

# Еженедельный топ в воскресенье 12:00 UTC
scheduler.add_job(func=send_weekly_top, trigger="cron", day_of_week="sun", hour=12, minute=0)

# Приглашение к совместному полёту (07:00 UTC = 10:00 Москва / 19:00 Камчатка)
scheduler.add_job(func=send_morning_invitation, trigger="cron", hour=7, minute=0)

# Челлендж по понедельникам в 08:00 UTC
scheduler.add_job(func=send_challenge, trigger="cron", day_of_week="mon", hour=8, minute=0)

scheduler.start()

print("[SCHEDULER] Запущен:")
print("  📊 Статистика за день: 21:00 UTC")
print("  🏆 Топ пилотов недели: воскресенье 12:00 UTC")
print("  🛫 Совместный полёт: ежедневно 07:00 UTC")
print("  🏆 Челлендж: понедельник 08:00 UTC")

# ───────────────────────────────────────────
# START
# ───────────────────────────────────────────

threading.Thread(target=start_polling, daemon=True).start()
print("[BOT] Polling thread started")

@app.route("/")
def home():
    return "✅ VA UP! Telegram Bot is running"

if __name__ == "__main__":
    print(f"[BOT] Starting on port {PORT}")
    app.run(host="0.0.0.0", port=PORT)

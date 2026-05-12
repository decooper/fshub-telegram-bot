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
        rate = flight['landing_rate']
        if rate < -600:
            rating = "💥 Катастрофа"
        elif rate < -400:
            rating = "⚠️ Жёсткая"
        elif rate < -300:
            rating = "🟡 Средняя"
        elif rate < -50:
            rating = "👍 Хорошая"
        else:
            rating = "🌟 Идеальная"

        lines.append(
            f"✈️ <b>{flight['flight_no']}</b> · {flight['pilot']}\n"
            f"🛫 {flight['departure']} → {flight['arrival']}\n"
            f"📊 {rate} fpm — {rating}"
        )

    return "✈️ <b>Последние рейсы</b>\n\n" + "\n\n".join(lines)


def get_stats():
    with stats_lock:
        total = len(stats['flights'])
        week_ago = datetime.now() - timedelta(days=7)
        week_count = sum(1 for f in stats['flights'] if f['date'] > week_ago)
        rates = [f['landing_rate'] for f in stats['flights']]

    avg_rate = round(sum(rates) / len(rates)) if rates else 0

    return (
        "📊 <b>Статистика VA UP!</b>\n\n"
        f"🛬 Всего рейсов: <b>{total}</b>\n"
        f"📅 За неделю: <b>{week_count}</b>\n"
        f"📐 Средняя посадка: <b>{avg_rate} fpm</b>"
    )

# ───────────────────────────────────────────
#  АВТОМАТИЧЕСКИЕ СООБЩЕНИЯ ПО РАСПИСАНИЮ
# ───────────────────────────────────────────

def send_daily_stats():
    """Отправляет статистику за сегодня в канал"""
    today = datetime.now().date()
    with stats_lock:
        today_flights = [f for f in stats['flights'] if f['date'].date() == today]
    
    count = len(today_flights)
    if count == 0:
        message = "📊 <b>Статистика за сегодня</b>\n\n✈️ Сегодня рейсов пока нет. Самое время поднять шасси в небо!"
    elif count == 1:
        message = f"📊 <b>Статистика за сегодня</b>\n\n✈️ Выполнен <b>1 рейс</b>\n🛬 Посадок: 1\n\nСпасибо пилоту! 👏"
    else:
        message = f"📊 <b>Статистика за сегодня</b>\n\n✈️ Выполнено рейсов: <b>{count}</b>\n🛬 Посадок: {count}\n\nСпасибо пилотам за отличную работу! 👏"
    
    send_to_telegram(message)
    print(f"[SCHEDULER] Daily stats sent: {count} flights")

def send_weekly_top():
    """Отправляет топ пилотов недели в канал"""
    message = get_top_pilots()
    send_to_telegram(message)
    print("[SCHEDULER] Weekly top sent")

def send_flight_invitation():
    """Отправляет приглашение к совместному полёту в 07:00 UTC (10:00 Москва / 19:00 Камчатка)"""
    message = (
        "🛫 <b>СОВМЕСТНЫЙ ПОЛЁТ</b>\n\n"
        "⏰ <b>Время:</b> сегодня, договариваемся в комментариях\n"
        "🌍 <b>Отличное время для всех:</b>\n"
        "   • Москва — 10:00 утра ☀️\n"
        "   • Камчатка — 19:00 вечера 🌙\n"
        "   • Калининград — 09:00 утра ☀️\n\n"
        "✈️ <b>Куда полетим?</b>\n"
        "Предлагайте маршруты в комментариях!\n\n"
        "Кто присоединится — отмечайтесь 👇\n"
        "⬇️ Пишите, кто с нами!"
    )
    send_to_telegram(message)
    print("[SCHEDULER] Flight invitation sent")

def send_challenge():
    """Отправляет челлендж на неделю"""
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
#  ФОРМАТИРОВАНИЕ СОБЫТИЙ FSHUB
# ───────────────────────────────────────────

def format_flight_departed(data):
    d = data.get('_data', {})
    pilot_name    = d.get('user', {}).get('name', 'Неизвестный пилот')
    plan          = d.get('plan', {})
    aircraft      = d.get('aircraft', {})
    airport       = d.get('airport', {})
    flight_no     = plan.get('flight_no', 'N/A')
    departure     = plan.get('departure', '????')
    arrival       = plan.get('arrival', '????')
    aircraft_name = aircraft.get('icao_name') or aircraft.get('icao', 'N/A')
    airport_name  = airport.get('name', departure)

    return (
        "🛫 <b>РЕЙС НАЧАЛСЯ</b>\n\n"
        f"👨‍✈️ Пилот: <b>{pilot_name}</b>\n"
        f"🆔 Рейс: <b>{flight_no}</b>\n"
        f"🗺 Маршрут: <b>{departure} → {arrival}</b>\n"
        f"✈️ Борт: <b>{aircraft_name}</b>\n"
        f"📍 Вылет из: <b>{airport_name}</b>\n\n"
        f"🕒 {datetime.utcnow().strftime('%H:%M UTC')}"
    )


def format_flight_arrived(data):
    d = data.get('_data', {})
    pilot_name    = d.get('user', {}).get('name', 'Неизвестный пилот')
    plan          = d.get('plan', {})
    aircraft      = d.get('aircraft', {})
    airport       = d.get('airport', {})
    landing_rate  = d.get('landing_rate', 0)
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

    if landing_rate < -600:
        rating = "💥 Катастрофа"
    elif landing_rate < -400:
        rating = "⚠️ Жёсткая"
    elif landing_rate < -300:
        rating = "🟡 Средняя"
    elif landing_rate < -50:
        rating = "👍 Хорошая"
    else:
        rating = "🌟 Идеальная"

    return (
        "🛬 <b>РЕЙС ЗАВЕРШЁН</b>\n\n"
        f"👨‍✈️ Пилот: <b>{pilot_name}</b>\n"
        f"🆔 Рейс: <b>{flight_no}</b>\n"
        f"🗺 Маршрут: <b>{departure} → {arrival}</b>\n"
        f"✈️ Борт: <b>{aircraft_name}</b>\n"
        f"📍 Прилёт в: <b>{airport_name}</b>\n"
        f"📊 Посадка: <b>{landing_rate} fpm</b> — {rating}\n\n"
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
#  POLLING TELEGRAM (ИСПРАВЛЕНАЯ ВЕРСИЯ)
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
            # Сразу обновляем ID, чтобы не обработать повторно
            with update_id_lock:
                if update['update_id'] <= last_update_id:
                    continue
                last_update_id = update['update_id']

            message = update.get('message', {})
            chat_id = message.get('chat', {}).get('id')
            text    = message.get('text', '')

            if not chat_id or str(chat_id) == str(CHAT_ID):
                continue

            if text == '/start':
                send_to_user(chat_id,
                    "✈️ <b>VA UP! Bot</b>\n\n"
                    "Привет! Я бот виртуальной авиакомпании UP!\n\n"
                    "Используй /help для списка команд."
                )
            elif text == '/help':
                send_to_user(chat_id,
                    "🤖 <b>Команды бота</b>\n\n"
                    "/top — 🏆 топ пилотов недели\n"
                    "/last — ✈️ последние 5 рейсов\n"
                    "/stats — 📊 общая статистика\n"
                    "/help — эта справка"
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

# Запускаем планировщик
scheduler = BackgroundScheduler()

# Ежедневная статистика в 21:00 UTC
scheduler.add_job(func=send_daily_stats, trigger="cron", hour=21, minute=0)

# Еженедельный топ пилотов в воскресенье в 12:00 UTC
scheduler.add_job(func=send_weekly_top, trigger="cron", day_of_week="sun", hour=12, minute=0)

# Приглашение к совместному полёту (каждый день в 07:00 UTC)
# Москва 10:00, Камчатка 19:00, Калининград 09:00
scheduler.add_job(func=send_flight_invitation, trigger="cron", hour=7, minute=0)

# Челлендж на неделю (каждый понедельник в 08:00 UTC)
scheduler.add_job(func=send_challenge, trigger="cron", day_of_week="mon", hour=8, minute=0)

scheduler.start()
print("[SCHEDULER] Запущен:")
print("  - Статистика за день: каждый день в 21:00 UTC")
print("  - Топ пилотов недели: каждое воскресенье в 12:00 UTC")
print("  - Приглашение к полёту: каждый день в 07:00 UTC (Москва 10:00, Камчатка 19:00)")
print("  - Челлендж недели: каждый понедельник в 08:00 UTC")

# Запускаем polling для команд
polling_thread = threading.Thread(target=start_polling, daemon=True)
polling_thread.start()
print("[BOT] Polling thread started")

from flask import Flask, request, jsonify
import requests
import os
import json
from datetime import datetime, timedelta
import time
import threading

app = Flask(__name__)

# Telegram настройки
BOT_TOKEN = os.environ.get('TG_BOT_TOKEN')
CHAT_ID = os.environ.get('TG_CHAT_ID')

# Хранилище для статистики
stats = {
    'flights': [],  # список рейсов за неделю
    'last_flights': {}
}

# Переменная для хранения последнего update_id для polling
last_update_id = 0

def send_photo_to_telegram(image_url, caption):
    """Отправляет фото в Telegram"""
    if not BOT_TOKEN or not CHAT_ID:
        return False
    
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto"
    payload = {
        "chat_id": CHAT_ID,
        "photo": image_url,
        "caption": caption,
        "parse_mode": "HTML"
    }
    
    try:
        response = requests.post(url, json=payload, timeout=15)
        return response.status_code == 200
    except Exception as e:
        print(f"Ошибка отправки фото: {e}")
        return False

def send_to_telegram(message):
    """Отправляет текст в Telegram канал"""
    if not BOT_TOKEN or not CHAT_ID:
        return False
    
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": message,
        "parse_mode": "HTML"
    }
    
    try:
        response = requests.post(url, json=payload, timeout=10)
        return response.status_code == 200
    except Exception as e:
        print(f"Ошибка отправки в Telegram: {e}")
        return False

def send_to_user(chat_id, message):
    """Отправляет текст конкретному пользователю (для команд)"""
    if not BOT_TOKEN:
        return False
    
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "HTML"
    }
    
    try:
        response = requests.post(url, json=payload, timeout=10)
        return response.status_code == 200
    except Exception as e:
        print(f"Ошибка отправки пользователю: {e}")
        return False

def format_flight_departed(data):
    """Форматирует сообщение о вылете"""
    d = data.get('_data', {})
    user = d.get('user', {})
    plan = d.get('plan', {})
    aircraft = d.get('aircraft', {})
    airport = d.get('airport', {})
    
    pilot_name = user.get('name', 'Неизвестный пилот')
    flight_no = plan.get('flight_no', 'N/A')
    departure = plan.get('departure', '????')
    arrival = plan.get('arrival', '????')
    aircraft_name = aircraft.get('icao_name', aircraft.get('icao', 'N/A'))
    airport_name = airport.get('name', departure)
    
    return (
        f"✈️ <b>РЕЙС НАЧАЛСЯ</b>\n\n"
        f"👨‍✈️ Пилот: {pilot_name}\n"
        f"🆔 Рейс: {flight_no}\n"
        f"🛫 Маршрут: {departure} → {arrival}\n"
        f"✈️ ВС: {aircraft_name}\n"
        f"📍 Аэропорт вылета: {airport_name} ({departure})\n\n"
        f"🕒 {datetime.now().strftime('%H:%M UTC')}"
    )

def format_flight_arrived(data):
    """Форматирует сообщение о посадке"""
    d = data.get('_data', {})
    user = d.get('user', {})
    plan = d.get('plan', {})
    aircraft = d.get('aircraft', {})
    airport = d.get('airport', {})
    landing_rate = d.get('landing_rate', 0)
    
    pilot_name = user.get('name', 'Неизвестный пилот')
    flight_no = plan.get('flight_no', 'N/A')
    departure = plan.get('departure', '????')
    arrival = plan.get('arrival', '????')
    aircraft_name = aircraft.get('icao_name', aircraft.get('icao', 'N/A'))
    airport_name = airport.get('name', arrival)
    
    # Сохраняем рейс для статистики
    try:
        stats['flights'].append({
            'flight_no': flight_no,
            'pilot': pilot_name,
            'departure': departure,
            'arrival': arrival,
            'landing_rate': landing_rate,
            'date': datetime.now()
        })
        if len(stats['flights']) > 100:
            stats['flights'] = stats['flights'][-100:]
    except:
        pass
    
    # Оценка посадки
    if landing_rate < -300:
        rating = "⚠️ Жёсткая!"
    elif landing_rate < -200:
        rating = "🟡 Средняя"
    elif landing_rate > -50:
        rating = "🌟 Идеальная!"
    else:
        rating = "👍 Хорошая"
    
    return (
        f"🛬 <b>РЕЙС ЗАВЕРШЁН</b>\n\n"
        f"👨‍✈️ Пилот: {pilot_name}\n"
        f"🆔 Рейс: {flight_no}\n"
        f"🛬 Маршрут: {departure} → {arrival}\n"
        f"✈️ ВС: {aircraft_name}\n"
        f"📍 Аэропорт прибытия: {airport_name} ({arrival})\n"
        f"📊 Посадка: {landing_rate} fpm — {rating}\n\n"
        f"🕒 {datetime.now().strftime('%H:%M UTC')}"
    )

def format_screenshots(data):
    """Обрабатывает скриншоты"""
    screenshots = data.get('_data', [])
    if not screenshots:
        return
    
    flight_id = screenshots[0].get('flight_id', 'N/A') if screenshots else 'N/A'
    
    for scr in screenshots:
        image_url = scr.get('screenshot_url')
        if image_url:
            caption = f"📸 <b>Скриншот рейса #{flight_id}</b>\n🕒 {datetime.now().strftime('%H:%M UTC')}"
            send_photo_to_telegram(image_url, caption)
            time.sleep(0.5)

def get_top_pilots():
    """Формирует топ пилотов"""
    week_ago = datetime.now() - timedelta(days=7)
    week_flights = [f for f in stats['flights'] if f['date'] > week_ago]
    
    pilot_counts = {}
    for flight in week_flights:
        pilot = flight['pilot']
        pilot_counts[pilot] = pilot_counts.get(pilot, 0) + 1
    
    sorted_pilots = sorted(pilot_counts.items(), key=lambda x: x[1], reverse=True)[:10]
    
    if not sorted_pilots:
        return "📊 За эту неделю пока нет рейсов."
    
    message = "<b>🏆 ТОП ПИЛОТОВ НЕДЕЛИ</b>\n\n"
    for i, (pilot, count) in enumerate(sorted_pilots, 1):
        medal = {1: "🥇", 2: "🥈", 3: "🥉"}.get(i, f"{i}.")
        message += f"{medal} {pilot} — {count} рейс(ов)\n"
    
    return message

def get_last_flights(limit=5):
    """Формирует список последних рейсов"""
    recent = stats['flights'][-limit:][::-1]
    
    if not recent:
        return "📭 Нет завершённых рейсов."
    
    message = f"<b>✈️ ПОСЛЕДНИЕ {len(recent)} РЕЙСОВ</b>\n\n"
    for flight in recent:
        rating = "👍" if flight['landing_rate'] > -200 else "⚠️"
        message += f"{rating} {flight['flight_no']} | {flight['pilot']}\n"
        message += f"   {flight['departure']} → {flight['arrival']}\n"
        message += f"   Посадка: {flight['landing_rate']} fpm\n\n"
    
    return message

def get_stats():
    """Общая статистика"""
    total = len(stats['flights'])
    week_ago = datetime.now() - timedelta(days=7)
    week_flights = [f for f in stats['flights'] if f['date'] > week_ago]
    
    return (
        f"<b>📊 Статистика VA UP!</b>\n\n"
        f"Всего рейсов: {total}\n"
        f"За неделю: {len(week_flights)}"
    )

def poll_telegram():
    """Polling для получения команд от пользователей"""
    global last_update_id
    
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates"
    
    while True:
        try:
            params = {"timeout": 30, "offset": last_update_id + 1}
            response = requests.get(url, params=params, timeout=35)
            data = response.json()
            
            if data.get('ok') and data.get('result'):
                for update in data['result']:
                    last_update_id = update['update_id']
                    
                    if 'message' in update:
                        msg = update['message']
                        chat_id = msg['chat']['id']
                        text = msg.get('text', '')
                        
                        # Игнорируем сообщения из канала
                        if str(chat_id) == str(CHAT_ID):
                            continue
                        
                        # Обрабатываем команды
                        if text == '/top':
                            send_to_user(chat_id, get_top_pilots())
                        elif text == '/last':
                            send_to_user(chat_id, get_last_flights())
                        elif text == '/stats':
                            send_to_user(chat_id, get_stats())
                        elif text == '/help':
                            help_text = (
                                "<b>🤖 Команды бота VA UP!</b>\n\n"
                                "/top — топ пилотов недели\n"
                                "/last — последние 5 рейсов\n"
                                "/stats — общая статистика\n"
                                "/help — эта справка"
                            )
                            send_to_user(chat_id, help_text)
                        elif text == '/start':
                            send_to_user(chat_id, "✈️ Привет! Я бот виртуальной авиакомпании UP!\n\nИспользуй /help для списка команд.")
        
        except Exception as e:
            print(f"Polling error: {e}")
        
        time.sleep(1)

@app.route('/')
def home():
    return '✅ UP! VA Telegram Bot is running'

@app.route('/webhook', methods=['POST'])
def fshub_webhook():
    """Обработчик вебхуков от FSHub"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "No JSON data"}), 400
        
        event_type = data.get('_type')
        print(f"Получено событие: {event_type}")
        
        if event_type == 'flight.departed':
            send_to_telegram(format_flight_departed(data))
        elif event_type == 'flight.arrived':
            send_to_telegram(format_flight_arrived(data))
        elif event_type == 'screenshots.uploaded':
            format_screenshots(data)
        elif event_type == 'airline.achievement':
            send_to_telegram(format_achievement(data))
        
        return jsonify({"status": "ok"}), 200
    except Exception as e:
        print(f"Ошибка: {e}")
        return jsonify({"error": str(e)}), 500

def format_achievement(data):
    """Форматирует достижение"""
    d = data.get('_data', {})
    achievement = d.get('achievement', {})
    flight = d.get('flight', {})
    user = flight.get('user', {})
    pilot_name = user.get('name', 'Пилот')
    title = achievement.get('title', 'Достижение')
    
    return (
        f"🏆 <b>ДОСТИЖЕНИЕ ПОЛУЧЕНО!</b>\n\n"
        f"👨‍✈️ {pilot_name}\n"
        f"🎯 {title}\n\n"
        f"🎉 Поздравляем!"
    )

# Запускаем polling в отдельном потоке
def start_polling():
    thread = threading.Thread(target=poll_telegram, daemon=True)
    thread.start()

if __name__ == '__main__':
    start_polling()
    app.run(host='0.0.0.0', port=10000)

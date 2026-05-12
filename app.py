from flask import Flask, request, jsonify
import requests
import os
import json
from datetime import datetime

app = Flask(__name__)

# Telegram настройки (из переменных окружения Render)
BOT_TOKEN = os.environ.get('TG_BOT_TOKEN')
CHAT_ID = os.environ.get('TG_CHAT_ID')

def send_to_telegram(message):
    """Отправляет сообщение в Telegram канал"""
    if not BOT_TOKEN or not CHAT_ID:
        print("Ошибка: не заданы TG_BOT_TOKEN или TG_CHAT_ID")
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
    
    message = (
        f"✈️ <b>Departure</b>\n\n"
        f"👨‍✈️ Пилот: {pilot_name}\n"
        f"🆔 Рейс: {flight_no}\n"
        f"🛫 Маршрут: {departure} → {arrival}\n"
        f"✈️ ВС: {aircraft_name}\n"
        f"📍 Аэропорт вылета: {airport_name} ({departure})\n\n"
        f"🕒 Время: {datetime.now().strftime('%H:%M UTC')}"
    )
    return message

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
    arrival = plan.get('arrival', '????')
    aircraft_name = aircraft.get('icao_name', aircraft.get('icao', 'N/A'))
    airport_name = airport.get('name', arrival)
    
    # Оценка качества посадки
    landing_rating = "👍 Хорошая"
    if landing_rate < -300:
        landing_rating = "⚠️ Жёсткая!"
    elif landing_rate < -200:
        landing_rating = "🟡 Средняя"
    elif landing_rate > -50:
        landing_rating = "🌟 Идеальная!"
    
    message = (
        f"🛬 <b>Arrival</b>\n\n"
        f"👨‍✈️ Пилот: {pilot_name}\n"
        f"🆔 Рейс: {flight_no}\n"
        f"🛬 Маршрут: → {arrival}\n"
        f"✈️ ВС: {aircraft_name}\n"
        f"📍 Аэропорт прибытия: {airport_name} ({arrival})\n"
        f"📊 Посадка: {landing_rate} fpm — {landing_rating}\n\n"
        f"🕒 Время: {datetime.now().strftime('%H:%M UTC')}"
    )
    return message

def format_flight_completed(data):
    """Форматирует полный отчёт о рейсе"""
    d = data.get('_data', {})
    user = d.get('user', {})
    plan = d.get('plan', {})
    aircraft = d.get('aircraft', {})
    distance = d.get('distance', {})
    fuel_burnt = d.get('fuel_burnt', 0)
    
    pilot_name = user.get('name', 'Неизвестный пилот')
    flight_no = plan.get('callsign', plan.get('flight_no', 'N/A'))
    departure = plan.get('icao_dep', '????')
    arrival = plan.get('icao_arr', '????')
    aircraft_name = aircraft.get('icao_name', aircraft.get('icao', 'N/A'))
    distance_nm = distance.get('nm', 0)
    
    message = (
        f"📋 <b>ОТЧЁТ О РЕЙСЕ</b>\n\n"
        f"👨‍✈️ Пилот: {pilot_name}\n"
        f"🆔 Рейс: {flight_no}\n"
        f"🛫 Маршрут: {departure} → {arrival}\n"
        f"✈️ ВС: {aircraft_name}\n"
        f"📏 Дистанция: {distance_nm} nm ({distance.get('km', 0)} km)\n"
        f"⛽ Топлива сожжено: {fuel_burnt} кг\n\n"
        f"✅ Рейс успешно завершён!"
    )
    return message

def format_screenshots(data):
    """Форматирует сообщение о скриншотах (может быть несколько)"""
    screenshots = data.get('_data', [])
    if not screenshots:
        return None
    
    count = len(screenshots)
    # Берём первый скриншот для примера
    first = screenshots[0]
    flight_id = first.get('flight_id', 'N/A')
    user_id = first.get('user_id', 'N/A')
    
    message = (
        f"📸 <b>НОВЫЕ СКРИНШОТЫ</b>\n\n"
        f"🆔 Рейс ID: {flight_id}\n"
        f"👤 Пилот ID: {user_id}\n"
        f"🖼 Количество: {count} шт.\n\n"
    )
    
    # Добавляем ссылки на первые 3 скриншота
    for i, scr in enumerate(screenshots[:3]):
        url = scr.get('screenshot_url')
        if url:
            message += f"📷 <a href='{url}'>Скриншот {i+1}</a>\n"
    
    if count > 3:
        message += f"\n... и ещё {count - 3} скриншотов"
    
    return message

def format_achievement(data):
    """Форматирует сообщение о достижении"""
    d = data.get('_data', {})
    achievement = d.get('achievement', {})
    airline = d.get('airline', {})
    flight = d.get('flight', {})
    
    title = achievement.get('title', 'Новое достижение')
    description = achievement.get('description', '')
    airline_name = airline.get('name', 'VA')
    user = flight.get('user', {})
    pilot_name = user.get('name', 'Пилот')
    
    message = (
        f"🏆 <b>ДОСТИЖЕНИЕ ПОЛУЧЕНО!</b>\n\n"
        f"👨‍✈️ Пилот: {pilot_name}\n"
        f"🎯 Достижение: {title}\n"
        f"📖 Описание: {description}\n"
        f"✈️ Авиакомпания: {airline_name}\n\n"
        f"Поздравляем! 🎉"
    )
    return message

@app.route('/')
def home():
    return '✅ FSHub to Telegram bridge is running. Webhook URL: /webhook'

@app.route('/webhook', methods=['POST'])
def webhook():
    """Основной обработчик вебхуков от FSHub"""
    try:
        # Получаем данные от FSHub
        data = request.get_json()
        
        if not data:
            return jsonify({"error": "No JSON data"}), 400
        
        event_type = data.get('_type')
        
        print(f"Получено событие: {event_type}")
        
        # Формируем сообщение в зависимости от типа
        message = None
        
        if event_type == 'flight.departed':
            message = format_flight_departed(data)
        elif event_type == 'flight.arrived':
            message = format_flight_arrived(data)
        elif event_type == 'flight.completed':
            message = format_flight_completed(data)
        elif event_type == 'screenshots.uploaded':
            message = format_screenshots(data)
        elif event_type == 'airline.achievement':
            message = format_achievement(data)
        else:
            # Если тип не распознан — отправляем сырой JSON
            message = f"📨 <b>Новое событие FSHub</b>\n\n<code>{json.dumps(data, indent=2, ensure_ascii=False)[:500]}</code>"
        
        # Отправляем в Telegram
        if message:
            send_to_telegram(message)
            print(f"Сообщение отправлено в Telegram")
        else:
            print(f"Не удалось сформировать сообщение для типа {event_type}")
        
        return jsonify({"status": "ok"}), 200
        
    except Exception as e:
        print(f"Ошибка обработки вебхука: {e}")
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)

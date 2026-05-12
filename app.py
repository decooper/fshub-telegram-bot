from flask import Flask, request, jsonify
import requests
import os
import json
from datetime import datetime, timedelta
import time

app = Flask(__name__)

# Telegram настройки
BOT_TOKEN = os.environ.get('TG_BOT_TOKEN')
CHAT_ID = os.environ.get('TG_CHAT_ID')

# Хранилище для статистики (в реальном проекте лучше использовать базу данных)
# Но для начала хватит и этого
stats = {
    'flights': [],  # список рейсов за неделю
    'last_flights': {}  # последний рейс каждого пилота
}

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
    """Отправляет текст в Telegram"""
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
        f"✈️ <b>DEPARTURE</b>\n\n"
        f"👨‍✈️ Пилот: {pilot_name}\n"
        f"🆔 Рейс: {flight_no}\n"
        f"🛫 Маршрут: {departure} → {arrival}\n"
        f"✈️ ВС: {aircraft_name}\n"
        f"📍 Аэропорт вылета: {airport_name} ({departure})\n\n"
        f"🕒 {datetime.now().strftime('%H:%M UTC')}"
    )
    return message

def format_flight_arrived(data):
    """Форматирует сообщение о посадке с картой"""
    d = data.get('_data', {})
    user = d.get('user', {})
    plan = d.get('plan', {})
    aircraft = d.get('aircraft', {})
    airport = d.get('airport', {})
    landing_rate = d.get('landing_rate', 0)
    flight_id = d.get('id', '')
    
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
            'date': datetime.now(),
            'flight_id': flight_id
        })
        # Оставляем только последние 100 рейсов
        if len(stats['flights']) > 100:
            stats['flights'] = stats['flights'][-100:]
    except:
        pass
    
    # Оценка качества посадки
    landing_rating = "👍 Хорошая"
    if landing_rate < -300:
        landing_rating = "⚠️ Жёсткая!"
    elif landing_rate < -200:
        landing_rating = "🟡 Средняя"
    elif landing_rate > -50:
        landing_rating = "🌟 Идеальная!"
    
    # Ссылка на карту полёта (если есть flight_id)
    map_link = f"https://fshub.io/flight/{flight_id}" if flight_id else ""
    map_text = f"\n🗺 <a href='{map_link}'>Карта полёта</a>" if map_link else ""
    
    message = (
        f"🛬 <b>ARRIVAL</b>\n\n"
        f"👨‍✈️ Пилот: {pilot_name}\n"
        f"🆔 Рейс: {flight_no}\n"
        f"🛬 Маршрут: {departure} → {arrival}\n"
        f"✈️ ВС: {aircraft_name}\n"
        f"📍 Аэропорт прибытия: {airport_name} ({arrival})\n"
        f"📊 Посадка: {landing_rate} fpm — {landing_rating}{map_text}\n\n"
        f"🕒 {datetime.now().strftime('%H:%M UTC')}"
    )
    return message

def format_screenshots(data):
    """Форматирует и отправляет скриншоты как фото"""
    screenshots = data.get('_data', [])
    if not screenshots:
        return None
    
    count = len(screenshots)
    flight_id = screenshots[0].get('flight_id', 'N/A') if screenshots else 'N/A'
    
    # Отправляем каждый скриншот как отдельное фото
    for scr in screenshots:
        image_url = scr.get('screenshot_url')
        if image_url:
            caption = f"📸 <b>Скриншот рейса #{flight_id}</b>\n🕒 {datetime.now().strftime('%H:%M UTC')}"
            send_photo_to_telegram(image_url, caption)
            time.sleep(0.5)  # небольшая задержка между отправками
    
    return f"📸 Отправлено {count} скриншотов"

def get_top_pilots():
    """Формирует топ пилотов по количеству рейсов за неделю"""
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
    recent = stats['flights'][-limit:][::-1]  # последние в обратном порядке
    
    if not recent:
        return "📭 Нет завершённых рейсов."
    
    message = f"<b>✈️ ПОСЛЕДНИЕ {len(recent)} РЕЙСОВ</b>\n\n"
    for flight in recent:
        rating = "👍" if flight['landing_rate'] > -200 else "⚠️"
        message += f"{rating} {flight['flight_no']} | {flight['pilot']}\n"
        message += f"   {flight['departure']} → {flight['arrival']}\n"
        message += f"   Посадка: {flight['landing_rate']} fpm\n\n"
    
    return message

def get_weekly_digest():
    """Формирует еженедельный дайджест"""
    week_ago = datetime.now() - timedelta(days=7)
    week_flights = [f for f in stats['flights'] if f['date'] > week_ago]
    
    if not week_flights:
        return "📊 За эту неделю рейсов не было."
    
    total_flights = len(week_flights)
    avg_landing = sum(f['landing_rate'] for f in week_flights) // total_flights
    
    # Самый дальний рейс (по ID можно было бы, но пока просто по маршруту)
    best_landing = min(week_flights, key=lambda x: x['landing_rate']) if week_flights else None
    worst_landing = max(week_flights, key=lambda x: x['landing_rate']) if week_flights else None
    
    message = (
        f"<b>📊 ЕЖЕНЕДЕЛЬНЫЙ ДАЙДЖЕСТ VA UP!</b>\n\n"
        f"✈️ Рейсов за неделю: <b>{total_flights}</b>\n"
        f"📊 Средняя посадка: <b>{avg_landing} fpm</b>\n\n"
    )
    
    if best_landing:
        message += f"🏆 Лучшая посадка: <b>{best_landing['landing_rate']} fpm</b>\n"
        message += f"   ({best_landing['pilot']}, {best_landing['flight_no']})\n\n"
    
    if worst_landing and worst_landing['landing_rate'] > -300:
        message += f"📈 Требует внимания: {worst_landing['landing_rate']} fpm\n"
    
    message += f"\n#{datetime.now().strftime('%Y%m%d')}"
    
    return message

@app.route('/')
def home():
    return '✅ UP! VA Telegram Bot is running'

@app.route(f'/bot{BOT_TOKEN}', methods=['POST'])
def telegram_webhook():
    """Обрабатывает команды от Telegram бота"""
    try:
        data = request.get_json()
        if not data or 'message' not in data:
            return jsonify({"status": "ok"}), 200
        
        message = data['message']
        text = message.get('text', '')
        chat_id = message.get('chat', {}).get('id')
        
        # Отвечаем только в личку бота (не в канал)
        if chat_id and str(chat_id) != str(CHAT_ID):
            # Это личный чат с ботом
            response_text = None
            if text == '/top':
                response_text = get_top_pilots()
            elif text == '/last':
                response_text = get_last_flights()
            elif text == '/stats':
                total = len(stats['flights'])
                week_ago = datetime.now() - timedelta(days=7)
                week_flights = [f for f in stats['flights'] if f['date'] > week_ago]
                response_text = f"<b>📊 Статистика VA UP!</b>\n\n"
                response_text += f"Всего рейсов: {total}\n"
                response_text += f"За неделю: {len(week_flights)}"
            elif text == '/help':
                response_text = "<b>🤖 Команды бота:</b>\n\n"
                response_text += "/top — топ пилотов недели\n"
                response_text += "/last — последние 5 рейсов\n"
                response_text += "/stats — общая статистика\n"
                response_text += "/help — эта справка"
            
            if response_text:
                url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
                payload = {"chat_id": chat_id, "text": response_text, "parse_mode": "HTML"}
                requests.post(url, json=payload, timeout=10)
        
        return jsonify({"status": "ok"}), 200
    except Exception as e:
        print(f"Ошибка обработки команды: {e}")
        return jsonify({"status": "error"}), 200

@app.route('/webhook', methods=['POST'])
def fshub_webhook():
    """Основной обработчик вебхуков от FSHub"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "No JSON data"}), 400
        
        event_type = data.get('_type')
        print(f"Получено событие: {event_type}")
        
        if event_type == 'flight.departed':
            message = format_flight_departed(data)
            if message:
                send_to_telegram(message)
        
        elif event_type == 'flight.arrived':
            message = format_flight_arrived(data)
            if message:
                send_to_telegram(message)
        
        elif event_type == 'flight.completed':
            # Полные отчёты пока пропускаем, чтобы не дублировать
            pass
        
        elif event_type == 'screenshots.uploaded':
            format_screenshots(data)
        
        elif event_type == 'airline.achievement':
            message = format_achievement(data)
            if message:
                send_to_telegram(message)
        
        else:
            message = f"📨 <b>Новое событие FSHub</b>\n\n<code>{json.dumps(data, indent=2, ensure_ascii=False)[:300]}</code>"
            send_to_telegram(message)
        
        return jsonify({"status": "ok"}), 200
        
    except Exception as e:
        print(f"Ошибка обработки вебхука: {e}")
        return jsonify({"error": str(e)}), 500

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
        f"📖 {description}\n"
        f"✈️ {airline_name}\n\n"
        f"🎉 Поздравляем!"
    )
    return message

# Функция для еженедельного дайджеста (запускается по расписанию)
def send_weekly_digest():
    """Отправляет еженедельный дайджест"""
    digest = get_weekly_digest()
    send_to_telegram(digest)

# Добавляем простую статистику для тестовых команд
@app.route('/test_stats', methods=['GET'])
def test_stats():
    """Тестовый эндпоинт для добавления демо-статистики"""
    # Добавляем тестовые рейсы для демонстрации команд
    for i in range(5):
        stats['flights'].append({
            'flight_no': f'UP00{i}',
            'pilot': f'Тест{i} Пилот',
            'departure': 'UUEE',
            'arrival': 'ULLI',
            'landing_rate': -100 - i*20,
            'date': datetime.now() - timedelta(days=i),
            'flight_id': str(100000 + i)
        })
    return "Тестовые рейсы добавлены"

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)

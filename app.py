from flask import Flask, request, jsonify
import requests
import os
import threading
import time
import traceback
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler

app = Flask(__name__)

# ───────────────────────────────────────────
# CONFIG
# ───────────────────────────────────────────

PORT = int(os.environ.get("PORT", 10000))
BOT_TOKEN = os.environ.get('TG_BOT_TOKEN')
CHAT_ID = os.environ.get('TG_CHAT_ID')

# FSAirlines настройки (добавь эти переменные в Render)
FSA_API_KEY = os.environ.get('FSA_API_KEY')
FSA_VA_ID = os.environ.get('FSA_VA_ID', '56177')  # твой VA ID в FSAirlines

if not BOT_TOKEN or not CHAT_ID:
    print("❌ Ошибка: не заданы BOT_TOKEN или CHAT_ID")
    exit(1)

print(f"[CONFIG] BOT_TOKEN: {BOT_TOKEN[:10]}... (скрыто)")
print(f"[CONFIG] CHAT_ID: {CHAT_ID}")
if FSA_API_KEY:
    print(f"[CONFIG] FSA_API_KEY: {FSA_API_KEY[:10]}... (скрыто)")
    print(f"[CONFIG] FSA_VA_ID: {FSA_VA_ID}")
else:
    print("[CONFIG] FSA_API_KEY не задан (экономические функции будут недоступны)")

# ───────────────────────────────────────────
# STATE (в памяти, для одного процесса)
# ───────────────────────────────────────────

stats = {'flights': []}
stats_lock = threading.Lock()

# ───────────────────────────────────────────
# FSAirlines API HELPER
# ───────────────────────────────────────────

def call_fsa_api(function, extra_params=None):
    """Универсальная функция вызова FSAirlines API"""
    if not FSA_API_KEY:
        print("[FSA] API ключ не задан")
        return None
    
    url = "https://www.fsairlines.net/va_interface2.php"
    params = {
        "function": function,
        "va_id": FSA_VA_ID,
        "apikey": FSA_API_KEY,
        "format": "json"
    }
    if extra_params:
        params.update(extra_params)
    
    try:
        response = requests.get(url, params=params, timeout=15)
        data = response.json()
        if data.get('status') == 'SUCCESS':
            return data.get('data')
        else:
            print(f"[FSA API] Ошибка: {data.get('status')}")
            return None
    except Exception as e:
        print(f"[FSA API ERROR] {e}")
        return None

# ───────────────────────────────────────────
# FSAirlines ECONOMIC FUNCTIONS
# ───────────────────────────────────────────

def get_daily_economy():
    """Получает экономику за сегодня (доходы/расходы)"""
    today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    today_ts = int(today_start.timestamp())
    
    transactions = call_fsa_api("getDailyTransactions", {"from_ts": today_ts})
    if not transactions:
        return None
    
    total_income = 0
    total_expense = 0
    income_by_reason = {}
    expense_by_reason = {}
    
    for t in transactions:
        value = t.get('value', 0)
        reason = t.get('reason', 'Other')
        if value > 0:
            total_income += value
            income_by_reason[reason] = income_by_reason.get(reason, 0) + value
        else:
            total_expense += abs(value)
            expense_by_reason[reason] = expense_by_reason.get(reason, 0) + abs(value)
    
    return {
        'total_income': total_income,
        'total_expense': total_expense,
        'net': total_income - total_expense,
        'income_by_reason': income_by_reason,
        'expense_by_reason': expense_by_reason
    }

def get_monthly_economy():
    """Получает экономику за последние 30 дней"""
    month_ago = int((datetime.now() - timedelta(days=30)).timestamp())
    
    transactions = call_fsa_api("getDailyTransactions", {"from_ts": month_ago})
    if not transactions:
        return None
    
    total_income = 0
    total_expense = 0
    daily_net = {}
    
    for t in transactions:
        value = t.get('value', 0)
        ts = t.get('ts', 0)
        day = datetime.fromtimestamp(ts).strftime('%Y-%m-%d')
        
        if value > 0:
            total_income += value
        else:
            total_expense += abs(value)
        
        daily_net[day] = daily_net.get(day, 0) + value
    
    best_day = max(daily_net.items(), key=lambda x: x[1]) if daily_net else (None, 0)
    worst_day = min(daily_net.items(), key=lambda x: x[1]) if daily_net else (None, 0)
    
    return {
        'total_income': total_income,
        'total_expense': total_expense,
        'net': total_income - total_expense,
        'best_day': best_day,
        'worst_day': worst_day,
        'days_count': len(daily_net)
    }

def get_flight_profit(report_id):
    """Получает прибыль от конкретного рейса"""
    report = call_fsa_api("getReportDetail", {"report_id": report_id})
    if not report:
        return None
    return {
        'profit': report.get('profit', 0),
        'salary': report.get('salary', 0),
        'bonus': report.get('bonus', 0),
        'fuel_used': report.get('fuel_used', 0),
        'pax': report.get('pax', 0)
    }

def format_daily_economy():
    """Форматирует ежедневную экономику для отправки в Telegram"""
    data = get_daily_economy()
    if not data:
        return "📊 <b>Экономика VA UP! (FSAirlines)</b>\n\nДанные временно недоступны. Проверьте API ключ."
    
    net_emoji = "📈" if data['net'] > 0 else "📉" if data['net'] < 0 else "➡️"
    
    message = f"📊 <b>Экономика VA UP! за сегодня</b>\n\n"
    message += f"💰 Доходы: <b>+{data['total_income']:,.0f} v$</b>\n"
    message += f"📉 Расходы: <b>-{data['total_expense']:,.0f} v$</b>\n"
    message += f"{net_emoji} <b>ИТОГО: {net_emoji} {data['net']:+,.0f} v$</b>\n\n"
    
    if data['income_by_reason']:
        top_income = sorted(data['income_by_reason'].items(), key=lambda x: x[1], reverse=True)[:3]
        message += "🔝 <b>Основные доходы:</b>\n"
        for reason, amount in top_income:
            message += f"   • {reason}: <b>+{amount:,.0f} v$</b>\n"
    
    if data['expense_by_reason']:
        top_expense = sorted(data['expense_by_reason'].items(), key=lambda x: x[1], reverse=True)[:3]
        message += "\n⚠️ <b>Основные расходы:</b>\n"
        for reason, amount in top_expense:
            message += f"   • {reason}: <b>-{amount:,.0f} v$</b>\n"
    
    return message

def format_monthly_economy():
    """Форматирует ежемесячную экономику для отправки в Telegram"""
    data = get_monthly_economy()
    if not data:
        return "📊 <b>Экономика VA UP! за месяц</b>\n\nДанные временно недоступны."
    
    net_emoji = "📈" if data['net'] > 0 else "📉" if data['net'] < 0 else "➡️"
    
    message = f"🏆 <b>ЭКОНОМИЧЕСКИЙ ДАЙДЖЕСТ VA UP!</b>\n"
    message += f"📅 {datetime.now().strftime('%B %Y')}\n\n"
    message += f"💰 Доходы за месяц: <b>+{data['total_income']:,.0f} v$</b>\n"
    message += f"📉 Расходы: <b>-{data['total_expense']:,.0f} v$</b>\n"
    message += f"{net_emoji} <b>ИТОГО: {net_emoji} {data['net']:+,.0f} v$</b>\n\n"
    
    if data['best_day'][0]:
        message += f"🌟 Лучший день: <b>{data['best_day'][0]}</b> (+{data['best_day'][1]:,.0f} v$)\n"
    if data['worst_day'][0]:
        message += f"⚠️ Худший день: <b>{data['worst_day'][0]}</b> ({data['worst_day'][1]:+,.0f} v$)\n"
    
    message += f"\n📊 Всего дней с операциями: <b>{data['days_count']}</b>"
    
    return message

# ───────────────────────────────────────────
# TELEGRAM SENDING
# ───────────────────────────────────────────

def send_to_telegram(message):
    if not BOT_TOKEN:
        return False
    
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    try:
        response = requests.post(url, json={
            "chat_id": CHAT_ID,
            "text": message,
            "parse_mode": "HTML"
        }, timeout=10)
        print(f"[TG SENT] Status: {response.status_code}")
        return response.status_code == 200
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
        response = requests.post(url, json={
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML"
        }, timeout=10)
        print(f"[USER MSG SENT] Status: {response.status_code}")
        return response.status_code == 200
    except Exception as e:
        print(f"[USER MSG ERROR] {e}")
        return False

# ───────────────────────────────────────────
# STATISTICS (FSHub)
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
        "📊 <b>Статистика VA UP! (FSHub)</b>\n\n"
        f"🛬 Всего рейсов: <b>{total}</b>\n"
        f"📅 За неделю: <b>{week_count}</b>\n"
        f"📐 Средняя посадка: <b>{avg_rate} fpm</b>"
    )

def get_daily_report():
    today = datetime.now().date()
    with stats_lock:
        today_flights = [f for f in stats['flights'] if f['date'].date() == today]
    
    if not today_flights:
        return "📊 <b>Статистика за сегодня (FSHub)</b>\n\n✈️ Сегодня рейсов пока нет."
    
    pilot_counts = {}
    for f in today_flights:
        pilot_counts[f['pilot']] = pilot_counts.get(f['pilot'], 0) + 1
    top_pilot = max(pilot_counts.items(), key=lambda x: x[1])
    
    rates = [f['landing_rate'] for f in today_flights]
    avg_rate = round(sum(rates) / len(rates))
    
    return (
        f"📊 <b>Статистика за сегодня (FSHub)</b>\n\n"
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
        "🛫 <b>СОВМЕСТНЫЙ СУББОТНИЙ ПОЛЕТ!</b>\n\n"
        "⏰ <b>Время:</b> что надо\n"
        "   • Москва — 10:00 утра ☀️\n"
        "   • Камчатка — 19:00 вечера 🌙\n\n"
        "✈️ <b>Куда сегодня полетим?</b>\n"
        "Предлагайте маршрут!\n\n"
        "Кто присоединится? 👇"
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

def send_daily_economy_report():
    """Отправляет ежедневный экономический отчёт за сегодня"""
    message = format_daily_economy()
    send_to_telegram(message)
    print("[SCHEDULER] Daily economy report sent")

def send_monthly_economic_digest():
    """Отправляет ежемесячный экономический дайджест в канал"""
    message = format_monthly_economy()
    send_to_telegram(message)
    print("[SCHEDULER] Monthly economic digest sent")

# ───────────────────────────────────────────
# WEBHOOK EVENTS (FSHub)
# ───────────────────────────────────────────

@app.route('/webhook', methods=['GET', 'POST'])
def fshub_webhook():
    print(f"[WEBHOOK] === НОВЫЙ ЗАПРОС ===")
    print(f"[WEBHOOK] Method: {request.method}")
    
    if request.method == 'GET':
        print("[WEBHOOK] GET request received, returning OK")
        return jsonify({"status": "ok", "message": "Webhook is active"}), 200
    
    try:
        data = request.get_json()
        if not data:
            print("[WEBHOOK] No JSON data, returning 400")
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
        else:
            print(f"[WEBHOOK] Unknown event type: {event_type}")

        return jsonify({"ok": True}), 200
        
    except Exception as e:
        print(f"[WEBHOOK CRITICAL ERROR] {type(e).__name__}: {e}")
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

def handle_departure(data):
    try:
        d = data.get('_data', {})
        pilot = d.get('user', {}).get('name', 'Пилот')
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
    except Exception as e:
        print(f"[DEPARTURE ERROR] {e}")
        traceback.print_exc()

def handle_arrival(data):
    try:
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
    except Exception as e:
        print(f"[ARRIVAL ERROR] {e}")
        traceback.print_exc()

def handle_screenshots(data):
    try:
        screenshots = data.get('_data', [])
        if not screenshots:
            return
        
        first = screenshots[0]
        flight_id = first.get('flight_id')
        
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
    except Exception as e:
        print(f"[SCREENSHOTS ERROR] {e}")
        traceback.print_exc()

def handle_achievement(data):
    try:
        d = data.get('_data', {})
        achievement = d.get('achievement', {})
        flight = d.get('flight', {})
        pilot = flight.get('user', {}).get('name', 'Пилот')
        title = achievement.get('title', 'Достижение')
        send_to_telegram(f"🏆 <b>ДОСТИЖЕНИЕ!</b>\n\n👨‍✈️ {pilot}\n🎯 {title}\n\nПоздравляем!")
    except Exception as e:
        print(f"[ACHIEVEMENT ERROR] {e}")
        traceback.print_exc()

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
        
        print(f"[TG_COMMAND] Chat: {chat_id}, Text: {text}")
        
        if not chat_id or str(chat_id) == str(CHAT_ID):
            return jsonify({"ok": True}), 200
        
        if text == '/start':
            send_to_user(chat_id, "✈️ <b>VA UP! Bot</b>\n\nИспользуй /help для списка команд")
        elif text == '/help':
            help_text = (
                "<b>🤖 Команды бота VA UP!</b>\n\n"
                "📊 <b>FSHub статистика:</b>\n"
                "/stats — общая статистика рейсов\n"
                "/top — топ пилотов недели\n"
                "/last — последние 5 рейсов\n\n"
                "💰 <b>FSAirlines экономика:</b>\n"
                "/economy — экономика за сегодня\n"
                "/monthly — экономический дайджест за месяц"
            )
            send_to_user(chat_id, help_text)
        elif text == '/stats':
            send_to_user(chat_id, get_full_stats())
        elif text == '/top':
            send_to_user(chat_id, get_top_pilots())
        elif text == '/last':
            send_to_user(chat_id, get_last_flights())
        elif text == '/economy':
            send_to_user(chat_id, format_daily_economy())
        elif text == '/monthly':
            send_to_user(chat_id, format_monthly_economy())
        
        return jsonify({"ok": True}), 200
    except Exception as e:
        print(f"[TG COMMAND ERROR] {e}")
        traceback.print_exc()
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
        print(f"[TG WEBHOOK SETUP] {response.json()}")
    except Exception as e:
        print(f"[TG WEBHOOK ERROR] {e}")

# ───────────────────────────────────────────
# SCHEDULER
# ───────────────────────────────────────────

scheduler = BackgroundScheduler()

# Ежедневная статистика FSHub в 21:00 UTC
scheduler.add_job(func=send_daily_stats, trigger="cron", hour=21, minute=0)

# Еженедельный топ пилотов в воскресенье в 12:00 UTC
scheduler.add_job(func=send_weekly_top, trigger="cron", day_of_week="sun", hour=12, minute=0)

# Приглашение на совместный полёт — по субботам в 06:00 UTC
scheduler.add_job(func=send_flight_invitation, trigger="cron", day_of_week="sat", hour=6, minute=0)

# Челлендж на неделю — по понедельникам в 08:00 UTC
scheduler.add_job(func=send_challenge, trigger="cron", day_of_week="mon", hour=8, minute=0)

# ЕЖЕДНЕВНЫЙ ЭКОНОМИЧЕСКИЙ ОТЧЁТ — каждый день в 07:00 UTC (10:00 МСК)
scheduler.add_job(func=send_daily_economy_report, trigger="cron", hour=7, minute=0)

# ЕЖЕМЕСЯЧНЫЙ ЭКОНОМИЧЕСКИЙ ОТЧЁТ — 1-го числа в 21:00 UTC (00:00 МСК)
if FSA_API_KEY:
    scheduler.add_job(func=send_monthly_economic_digest, trigger="cron", day=1, hour=21, minute=0)

scheduler.start()

print("[SCHEDULER] Запущен:")
print("  - Ежедневная статистика FSHub: 21:00 UTC")
print("  - Топ пилотов: воскресенье 12:00 UTC")
print("  - Совместный полёт: суббота 06:00 UTC")
print("  - Челлендж: понедельник 08:00 UTC")
print("  - Ежедневный экономический отчёт: 07:00 UTC (10:00 МСК)")
print("  - Ежемесячный экономический отчёт: 1-го числа 21:00 UTC (00:00 МСК)")

# ───────────────────────────────────────────
# START
# ───────────────────────────────────────────

setup_telegram_webhook()

print(f"[BOT] Started on port {PORT}")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT)

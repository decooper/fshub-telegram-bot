from flask import Flask, request
import requests
import os
import json

app = Flask(__name__)

# Здесь переменные читаются из окружения
BOT_TOKEN = os.environ.get('TG_BOT_TOKEN')
CHAT_ID = os.environ.get('TG_CHAT_ID')

@app.route('/')
def home():
    return '✅ FSHub to Telegram bridge is running'

@app.route('/webhook', methods=['POST'])
def webhook():
    try:
        data = request.get_json()
        
        message = f"✈️ FSHub Event:\n\n```json\n{json.dumps(data, indent=2, ensure_ascii=False)}\n```"
        
        # ПРАВИЛЬНО — используем переменную BOT_TOKEN
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        
        # ПРАВИЛЬНО — используем переменную CHAT_ID
        payload = {
            "chat_id": CHAT_ID,
            "text": message,
            "parse_mode": "Markdown"
        }
        
        requests.post(url, json=payload)
        return "OK", 200
        
    except Exception as e:
        print(f"Error: {e}")
        return f"Error: {e}", 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)
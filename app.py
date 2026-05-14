@app.route("/webhook", methods=["GET", "POST"])
def fshub_webhook():
    if request.method == "GET":
        return jsonify({"status": "ok"})
    try:
        # Проверка секрета временно отключена для приёма вебхуков от FSHub
        # if WEBHOOK_SECRET:
        #     if request.headers.get("X-Webhook-Secret") != WEBHOOK_SECRET:
        #         logger.warning("Invalid webhook secret")
        #         return jsonify({"error": "forbidden"}), 403

        data = request.get_json(force=True)
        if not data:
            return jsonify({"error": "no json"}), 400

        event = data.get("_type", "")
        logger.info(f"FSHub event: {event}")

        handler = FSHUB_HANDLERS.get(event)
        if handler:
            handler(data)

        return jsonify({"ok": True})
    except Exception as e:
        logger.exception(f"Webhook failure: {e}")
        return jsonify({"error": str(e)}), 500

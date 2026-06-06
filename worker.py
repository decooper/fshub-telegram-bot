"""
worker.py — VA UP! Background Worker

Запускается как отдельный сервис на Render (Background Worker).
НЕ содержит Flask, Gunicorn или HTTP-обработчиков.

Изменения v2:
- Добавлена отправка в Discord через Webhook
- discord_send() — аналог tg_send(), но для Discord
- HTML-теги из Telegram автоматически очищаются для Discord
- Все задачи отправляют уведомления в оба канала
"""

import os
import sys
import re
import time
import logging
import requests
from datetime import datetime

# ───── Импорт общих модулей из app.py ─────
try:
    from app import (
        tg_send,
        fmt_stats,
        fmt_top_landings,
        fmt_top_pilots,
        fmt_monthly_economy,
        snapshot_daily_economy,
        _create_pool,
        _init_db,
        logger,
    )
except ImportError as e:
    print(f"❌ Failed to import from app.py: {e}")
    sys.exit(1)

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED

# ═══════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════

DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "")

# ═══════════════════════════════════════════════════════════════
# LOGGING
# ═══════════════════════════════════════════════════════════════

worker_logger = logging.getLogger("va_up_worker")
worker_logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s | WORKER | %(levelname)s | %(message)s"))
worker_logger.addHandler(handler)

# ═══════════════════════════════════════════════════════════════
# DISCORD
# ═══════════════════════════════════════════════════════════════

def _tg_to_discord(text: str) -> str:
    """
    Конвертирует Telegram HTML-разметку в читаемый текст для Discord.
    Discord поддерживает Markdown, поэтому <b> → **bold**, <i> → *italic*.
    """
    text = re.sub(r"<b>(.*?)</b>", r"**\1**", text, flags=re.DOTALL)
    text = re.sub(r"<i>(.*?)</i>", r"*\1*", text, flags=re.DOTALL)
    text = re.sub(r"<code>(.*?)</code>", r"`\1`", text, flags=re.DOTALL)
    text = re.sub(r'<a href="(.*?)">(.*?)</a>', r"[\2](\1)", text, flags=re.DOTALL)
    # Убираем оставшиеся HTML-теги
    text = re.sub(r"<[^>]+>", "", text)
    return text.strip()


def discord_send(text: str) -> bool:
    """Отправляет сообщение в Discord через Webhook."""
    if not DISCORD_WEBHOOK_URL:
        worker_logger.warning("DISCORD_WEBHOOK_URL не задан, пропускаем Discord")
        return False

    clean = _tg_to_discord(text)

    # Discord лимит — 2000 символов
    if len(clean) > 2000:
        clean = clean[:1997] + "..."

    try:
        r = requests.post(
            DISCORD_WEBHOOK_URL,
            json={"content": clean},
            timeout=10,
        )
        if r.status_code in (200, 204):
            return True
        worker_logger.warning(f"Discord webhook failed: {r.status_code} — {r.text}")
        return False
    except Exception as e:
        worker_logger.error(f"❌ Ошибка Discord webhook: {e}")
        return False


def broadcast(text: str) -> None:
    """Отправляет сообщение в Telegram и Discord одновременно."""
    tg_send(text)
    discord_send(text)


# ═══════════════════════════════════════════════════════════════
# JOB WRAPPERS — с обработкой ошибок
# ═══════════════════════════════════════════════════════════════

def safe_job(name: str, fn):
    """Обёртка для безопасного выполнения задачи с логированием."""
    def wrapper():
        worker_logger.info(f"▶ Запуск задачи: {name}")
        try:
            fn()
            worker_logger.info(f"✅ Задача выполнена: {name}")
        except Exception as e:
            worker_logger.error(f"❌ Ошибка в задаче {name}: {e}", exc_info=True)
    wrapper.__name__ = name
    return wrapper


# ═══════════════════════════════════════════════════════════════
# JOBS
# ═══════════════════════════════════════════════════════════════

def job_daily_economy():
    snapshot_daily_economy()


def job_daily_stats():
    msg = fmt_stats()
    tg_send(msg)
    discord_send(msg)


def job_night_stats():
    msg = fmt_stats()
    tg_send(msg)
    discord_send(msg)


def job_weekly_landing_ranking():
    msg = fmt_top_landings()
    tg_send(msg)
    discord_send(msg)


def job_weekly_top_pilots():
    msg = fmt_top_pilots()
    tg_send(msg)
    discord_send(msg)


def job_saturday_inv():
    msg = (
        "🛫 **СОВМЕСТНАЯ СУББОТНЯЯ ОПЕРАЦИЯ!**\n\n"
        "⏰ Москва: 09:00 ☀️ | Камчатка: 18:00 🌙\n\n"
        "✈️ Предлагайте маршрут в комментариях!\nКто присоединяется? 👇"
    )
    # Для Telegram отправляем с HTML-тегами
    tg_send(
        "🛫 <b>СОВМЕСТНАЯ СУББОТНЯЯ ОПЕРАЦИЯ!</b>\n\n"
        "⏰ Москва: 09:00 ☀️ | Камчатка: 18:00 🌙\n\n"
        "✈️ Предлагайте маршрут в комментариях!\nКто присоединяется? 👇"
    )
    # Для Discord — с Markdown
    discord_send(msg)


def job_monday_challenge():
    tg_send(
        "🏆 <b>ЕЖЕНЕДЕЛЬНЫЙ ВЫЗОВ ЭКИПАЖУ!</b>\n\n"
        "🔹 Цель: 3 рейса за 7 дней\n"
        "🔹 Бонус: лучшая посадка недели\n\nГотов принять вызов? 💪"
    )
    discord_send(
        "🏆 **ЕЖЕНЕДЕЛЬНЫЙ ВЫЗОВ ЭКИПАЖУ!**\n\n"
        "🔹 Цель: 3 рейса за 7 дней\n"
        "🔹 Бонус: лучшая посадка недели\n\nГотов принять вызов? 💪"
    )


def job_monthly_digest():
    msg = fmt_monthly_economy()
    tg_send(msg)
    discord_send(msg)


# ═══════════════════════════════════════════════════════════════
# SCHEDULER
# ═══════════════════════════════════════════════════════════════

def job_listener(event):
    if event.exception:
        worker_logger.error(f"💥 Job {event.job_id} завершился с ошибкой: {event.exception}")
    else:
        worker_logger.info(f"✅ Job {event.job_id} выполнен успешно")


def build_scheduler() -> BlockingScheduler:
    """Создаёт и настраивает планировщик."""
    scheduler = BlockingScheduler(
        executors={"default": ThreadPoolExecutor(max_workers=2)},
        job_defaults={
            "coalesce": True,        # Не накапливать пропущенные запуски
            "max_instances": 1,      # Одновременно только 1 экземпляр задачи
            "misfire_grace_time": 3600,  # Терпеть опоздание до 1 часа
        },
        timezone="UTC",
    )

    # ── Ежедневные задачи ──────────────────────────────────────
    scheduler.add_job(
        safe_job("daily_economy_snapshot", job_daily_economy),
        "cron", hour=23, minute=50,
        id="daily_economy_snapshot",
    )
    scheduler.add_job(
        safe_job("daily_stats", job_daily_stats),
        "cron", hour=21, minute=0,
        id="daily_stats",
    )
    scheduler.add_job(
        safe_job("night_stats", job_night_stats),
        "cron", hour=1, minute=25,
        id="night_stats",
    )

    # ── Еженедельные задачи ────────────────────────────────────
    scheduler.add_job(
        safe_job("weekly_landing_ranking", job_weekly_landing_ranking),
        "cron", day_of_week="sun", hour=12, minute=0,
        id="weekly_landing_ranking",
    )
    scheduler.add_job(
        safe_job("weekly_top_pilots", job_weekly_top_pilots),
        "cron", day_of_week="sun", hour=10, minute=0,
        id="weekly_top_pilots",
    )
    scheduler.add_job(
        safe_job("saturday_inv", job_saturday_inv),
        "cron", day_of_week="sat", hour=6, minute=0,
        id="saturday_inv",
    )
    scheduler.add_job(
        safe_job("monday_challenge", job_monday_challenge),
        "cron", day_of_week="mon", hour=8, minute=0,
        id="monday_challenge",
    )

    # ── Ежемесячные задачи ─────────────────────────────────────
    scheduler.add_job(
        safe_job("monthly_digest", job_monthly_digest),
        "cron", day=1, hour=9, minute=0,
        id="monthly_digest",
    )


    scheduler.add_listener(job_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
    return scheduler


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════

def main():
    worker_logger.info("═" * 60)
    worker_logger.info(" VA UP! Background Worker v2 — запуск")
    worker_logger.info("═" * 60)

    if DISCORD_WEBHOOK_URL:
        worker_logger.info("✅ Discord webhook настроен")
    else:
        worker_logger.warning("⚠️  DISCORD_WEBHOOK_URL не задан — уведомления только в Telegram")

    # Инициализация БД
    try:
        _create_pool()
        _init_db()
        worker_logger.info("✅ Подключение к PostgreSQL установлено")
    except Exception as e:
        worker_logger.error(f"❌ Не удалось подключиться к БД: {e}")
        sys.exit(1)

    scheduler = build_scheduler()
    jobs = scheduler.get_jobs()
    worker_logger.info(f"📋 Зарегистрировано задач: {len(jobs)}")
    for job in jobs:
        worker_logger.info(f"  • {job.id} → следующий запуск: {job.next_run_time}")

    worker_logger.info("🚀 Планировщик запущен. Ожидание задач...")

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        worker_logger.info("🛑 Worker остановлен")
        scheduler.shutdown(wait=False)


if __name__ == "__main__":
    main()

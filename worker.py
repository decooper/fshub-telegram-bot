"""
worker.py — VA UP! Background Worker
Запускается как отдельный сервис на Render (Background Worker).
НЕ содержит Flask, Gunicorn или HTTP-обработчиков.
"""

import os
import sys
import time
import logging
from datetime import datetime

# ───── Импорт общих модулей из app.py ─────
# Предполагается, что shared.py содержит:
# tg_send, fmt_stats, fmt_top_landings, fmt_top_pilots,
# fmt_monthly_economy, snapshot_daily_economy, _create_pool, _init_db

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
# LOGGING
# ═══════════════════════════════════════════════════════════════

worker_logger = logging.getLogger("va_up_worker")
worker_logger.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s | WORKER | %(levelname)s | %(message)s"))
worker_logger.addHandler(handler)


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


def job_daily_economy():
    snapshot_daily_economy()


def job_daily_stats():
    tg_send(fmt_stats())


def job_night_stats():
    tg_send(fmt_stats())


def job_weekly_landing_ranking():
    tg_send(fmt_top_landings())


def job_weekly_top_pilots():
    tg_send(fmt_top_pilots())


def job_saturday_inv():
    tg_send(
        "🛫 <b>СОВМЕСТНАЯ СУББОТНЯЯ ОПЕРАЦИЯ!</b>\n\n"
        "⏰ Москва: 09:00 ☀️  |  Камчатка: 18:00 🌙\n\n"
        "✈️ Предлагайте маршрут в комментариях!\nКто присоединяется? 👇"
    )


def job_monday_challenge():
    tg_send(
        "🏆 <b>ЕЖЕНЕДЕЛЬНЫЙ ВЫЗОВ ЭКИПАЖУ!</b>\n\n"
        "🔹 Цель: 3 рейса за 7 дней\n"
        "🔹 Бонус: лучшая посадка недели\n\nГотов принять вызов? 💪"
    )


def job_monthly_digest():
    tg_send(fmt_monthly_economy())


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
            "coalesce": True,         # Не накапливать пропущенные запуски
            "max_instances": 1,       # Одновременно только 1 экземпляр задачи
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
    worker_logger.info("  VA UP! Background Worker — запуск")
    worker_logger.info("═" * 60)

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
        next_run = job.next_run_time
        worker_logger.info(f"   • {job.id} → следующий запуск: {next_run}")

    worker_logger.info("🚀 Планировщик запущен. Ожидание задач...")

    try:
        scheduler.start()  # BlockingScheduler — блокирует основной поток
    except (KeyboardInterrupt, SystemExit):
        worker_logger.info("🛑 Worker остановлен")
        scheduler.shutdown(wait=False)


if __name__ == "__main__":
    main()

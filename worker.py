"""
worker.py — VA UP! Background Worker v3

Запускается как отдельный сервис на Render (Background Worker).
НЕ содержит Flask, Gunicorn или HTTP-обработчиков.

Импортирует напрямую из core.py — без побочных эффектов
(нет лишнего планировщика, нет перерегистрации вебхука Telegram).

Изменения v3:
- Импорт из core.py вместо app.py (устранён баг с двойным запуском планировщика)
- discord_send() использует сессию с retry (HTTPAdapter) как в core.py
- job_night_stats переименован в job_weekly_digest (был дублем daily_stats)
- safe_job() корректно пробрасывает __name__ для APScheduler
"""

import os
import re
import sys
import logging

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED

# ── Импорт из core.py — без side effects ──────────────────────
try:
    from core import (
        _create_pool, _init_db,
        tg_send, logger,
        fmt_stats, fmt_top_landings, fmt_top_pilots,
        fmt_monthly_economy, fmt_operation_digest, fmt_operation,
        snapshot_daily_economy,
        operation_is_active,
        FSA_KEY,
    )
except ImportError as e:
    print(f"❌ Failed to import from core.py: {e}")
    sys.exit(1)

# ═══════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════

DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "")

# ═══════════════════════════════════════════════════════════════
# LOGGING
# ═══════════════════════════════════════════════════════════════

worker_logger = logging.getLogger("va_up_worker")
worker_logger.setLevel(logging.INFO)
_handler = logging.StreamHandler(sys.stdout)
_handler.setFormatter(
    logging.Formatter("%(asctime)s | WORKER | %(levelname)s | %(message)s")
)
worker_logger.addHandler(_handler)

# ═══════════════════════════════════════════════════════════════
# HTTP SESSION с retry — для Discord webhook
# ═══════════════════════════════════════════════════════════════

_discord_session = requests.Session()
_discord_retry   = Retry(
    total=3, read=3, connect=3, backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["POST"],
)
_discord_session.mount("https://", HTTPAdapter(max_retries=_discord_retry))

# ═══════════════════════════════════════════════════════════════
# DISCORD
# ═══════════════════════════════════════════════════════════════

_RE_TG_BOLD  = re.compile(r"<b>(.*?)</b>",            re.DOTALL)
_RE_TG_ITAL  = re.compile(r"<i>(.*?)</i>",            re.DOTALL)
_RE_TG_CODE  = re.compile(r"<code>(.*?)</code>",      re.DOTALL)
_RE_TG_LINK  = re.compile(r'<a href="(.*?)">(.*?)</a>', re.DOTALL)
_RE_TG_TAGS  = re.compile(r"<[^>]+>")


def _tg_to_discord(text: str) -> str:
    """Конвертирует Telegram HTML → Discord Markdown."""
    text = _RE_TG_BOLD.sub(r"**\1**", text)
    text = _RE_TG_ITAL.sub(r"*\1*",   text)
    text = _RE_TG_CODE.sub(r"`\1`",   text)
    text = _RE_TG_LINK.sub(r"[\2](\1)", text)
    text = _RE_TG_TAGS.sub("", text)
    return text.strip()


def discord_send(text: str) -> bool:
    """Отправляет сообщение в Discord через Webhook (с retry)."""
    if not DISCORD_WEBHOOK_URL:
        worker_logger.warning("DISCORD_WEBHOOK_URL не задан, пропускаем Discord")
        return False

    clean = _tg_to_discord(text)
    if len(clean) > 2000:
        clean = clean[:1997] + "..."

    try:
        r = _discord_session.post(
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
    """Отправляет сообщение в Telegram и Discord."""
    tg_send(text)
    discord_send(text)


# ═══════════════════════════════════════════════════════════════
# JOB WRAPPERS
# ═══════════════════════════════════════════════════════════════

def safe_job(name: str, fn):
    """Обёртка: перехватывает исключения, не роняет планировщик."""
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
    broadcast(fmt_stats())


def job_weekly_digest():
    """Воскресный дайджест операции (если активна)."""
    if operation_is_active():
        broadcast(fmt_operation_digest())


def job_weekly_landing_ranking():
    broadcast(fmt_top_landings())


def job_weekly_top_pilots():
    broadcast(fmt_top_pilots())


def job_weekly_operation_standings():
    """Пятничная таблица лидеров операции (если активна)."""
    if operation_is_active():
        broadcast(fmt_operation())


def job_saturday_inv():
    tg_send(
        "🛫 <b>СОВМЕСТНАЯ СУББОТНЯЯ ОПЕРАЦИЯ!</b>\n\n"
        "⏰ Москва: 09:00 ☀️ | Камчатка: 18:00 🌙\n\n"
        "✈️ Предлагайте маршрут в комментариях!\nКто присоединяется? 👇"
    )
    discord_send(
        "🛫 **СОВМЕСТНАЯ СУББОТНЯЯ ОПЕРАЦИЯ!**\n\n"
        "⏰ Москва: 09:00 ☀️ | Камчатка: 18:00 🌙\n\n"
        "✈️ Предлагайте маршрут в комментариях!\nКто присоединяется? 👇"
    )


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
    broadcast(fmt_monthly_economy())


# ═══════════════════════════════════════════════════════════════
# SCHEDULER
# ═══════════════════════════════════════════════════════════════

def _job_listener(event):
    if event.exception:
        worker_logger.error(
            f"💥 Job {event.job_id} завершился с ошибкой: {event.exception}"
        )
    else:
        worker_logger.info(f"✅ Job {event.job_id} выполнен успешно")


def build_scheduler() -> BlockingScheduler:
    scheduler = BlockingScheduler(
        executors={"default": ThreadPoolExecutor(max_workers=2)},
        job_defaults={
            "coalesce": True,
            "max_instances": 1,
            "misfire_grace_time": 3600,
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

    # ── Еженедельные задачи ────────────────────────────────────
    scheduler.add_job(
        safe_job("weekly_landing_ranking", job_weekly_landing_ranking),
        "cron", day_of_week="sun", hour=12, minute=0,
        id="weekly_landing_ranking",
    )
    scheduler.add_job(
        safe_job("weekly_operation_digest", job_weekly_digest),
        "cron", day_of_week="sun", hour=11, minute=0,
        id="weekly_operation_digest",
    )
    scheduler.add_job(
        safe_job("weekly_operation_standings", job_weekly_operation_standings),
        "cron", day_of_week="fri", hour=0, minute=0,
        id="weekly_operation_standings",
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

    scheduler.add_listener(_job_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
    return scheduler


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════

def main():
    worker_logger.info("═" * 60)
    worker_logger.info(" VA UP! Background Worker v3 — запуск")
    worker_logger.info("═" * 60)

    if DISCORD_WEBHOOK_URL:
        worker_logger.info("✅ Discord webhook настроен")
    else:
        worker_logger.warning(
            "⚠️  DISCORD_WEBHOOK_URL не задан — уведомления только в Telegram"
        )

    try:
        _create_pool()
        _init_db()
        worker_logger.info("✅ Подключение к PostgreSQL установлено")
    except Exception as e:
        worker_logger.error(f"❌ Не удалось подключиться к БД: {e}")
        sys.exit(1)

    scheduler = build_scheduler()
    jobs      = scheduler.get_jobs()
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

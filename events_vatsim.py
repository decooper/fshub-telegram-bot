# -*- coding: utf-8 -*-
"""
events_vatsim.py — публикация событий VATSIM российского дивизиона (RUS).

Что делает:
  • раз в 10 минут (задача в планировщике app.py) опрашивает VATSIM Events API
    по дивизиону: GET https://my.vatsim.net/api/v2/events/view/division/RUS
  • для каждого нового события шлёт объявление в три канала:
      – Telegram: тема (топик) в супергруппе @virtual_avia, message_thread_id=5
      – VK:       беседа UNICOM (core.vk_send)
      – Discord:  отдельный вебхук (только если задан EVENTS_DISCORD_WEBHOOK)
  • дедуп ведётся ПОКАНАЛЬНО в таблице sent_events, поэтому сбой одного канала
    не плодит дубли в остальных — на следующем цикле дошлётся только то, что
    ещё не ушло.

Важно про VK: события НЕ проходят через core.tg_send, поэтому автозеркало
core.vk_send_if_channel не срабатывает. VK вызывается здесь ровно один раз явно —
дублей в VK быть не может по построению.

Никаких новых обязательных env-переменных: значения по умолчанию рассчитаны на
текущую конфигурацию. Discord включается позже — достаточно задать в Render
переменную EVENTS_DISCORD_WEBHOOK; пока она пустая, Discord просто пропускается.
"""

import os
from datetime import datetime, timezone, timedelta

# Транспорт и инфраструктура берём из основного модуля — core.py не меняется.
from core import session, logger, TG_BASE, db_execute, vk_send, discord_send

# ── Конфигурация (env с дефолтами — запуск без новых переменных) ─────────────
VATSIM_BASE = "https://my.vatsim.net/api/v2/events"
DIVISION    = (os.environ.get("EVENTS_VATSIM_DIVISION", "RUS").strip() or "RUS")

EVENTS_TG_CHAT = os.environ.get("EVENTS_TG_CHAT", "@virtual_avia").strip()
try:
    EVENTS_TG_THREAD_ID = int(os.environ.get("EVENTS_TG_THREAD_ID", "5"))
except ValueError:
    EVENTS_TG_THREAD_ID = 5

EVENTS_DISCORD_WEBHOOK = os.environ.get("EVENTS_DISCORD_WEBHOOK", "").strip()

_TABLE_READY = False


# ── Таблица дедупа ──────────────────────────────────────────────────────────
def ensure_events_table():
    """Создаёт sent_events при первом обращении (идемпотентно)."""
    global _TABLE_READY
    if _TABLE_READY:
        return
    db_execute(
        """
        CREATE TABLE IF NOT EXISTS sent_events (
            id           SERIAL PRIMARY KEY,
            network      VARCHAR(16) NOT NULL,
            event_id     VARCHAR(64) NOT NULL,
            tg_sent      BOOLEAN DEFAULT FALSE,
            vk_sent      BOOLEAN DEFAULT FALSE,
            discord_sent BOOLEAN DEFAULT FALSE,
            created_at   TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE (network, event_id)
        )
        """
    )
    _TABLE_READY = True


# ── Утилиты ─────────────────────────────────────────────────────────────────
def _esc(s) -> str:
    """Экранирование под parse_mode=HTML."""
    return (
        str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    )


def _parse_iso(iso):
    """ISO8601 (в т.ч. с 'Z') → aware datetime в UTC, либо None."""
    if not iso:
        return None
    try:
        dt = datetime.fromisoformat(str(iso).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _fmt_start(iso) -> str:
    dt_utc = _parse_iso(iso)
    if dt_utc is None:
        return str(iso)[:16] if iso else "—"
    dt_msk = dt_utc + timedelta(hours=3)  # Москва = UTC+3
    return f"{dt_utc:%d.%m.%Y %H:%M}z ({dt_msk:%H:%M} МСК)"


def _is_relevant(ev) -> bool:
    """Отбрасываем уже завершившиеся события (защита от старья)."""
    end = _parse_iso(ev.get("end_time"))
    if end is None:
        return True
    return end >= datetime.now(timezone.utc)


def _is_division(ev, div) -> bool:
    """Подстраховка: подтверждаем принадлежность к дивизиону по организаторам."""
    for o in (ev.get("organisers") or []):
        if (o.get("division") or "").upper() == div.upper():
            return True
    return False


def _fmt_event(ev) -> str:
    name = ev.get("name") or "—"
    link = ev.get("link") or "https://my.vatsim.net/events"
    airports = ev.get("airports") or []
    icaos = ", ".join(a.get("icao", "") for a in airports if a.get("icao")) or "—"
    start = _fmt_start(ev.get("start_time"))
    return (
        "📅 <b>СОБЫТИЕ VATSIM (RUS)</b>\n\n"
        f"<b>{_esc(name)}</b>\n"
        f"🛫 Аэропорты: {_esc(icaos)}\n"
        f"🕒 Начало: {start}\n"
        f"🔗 {_esc(link)}"
    )


# ── Отправка в топик Telegram (собственный отправитель, не core.tg_send) ─────
def _tg_send_topic(text: str, banner: str = "") -> bool:
    if not EVENTS_TG_CHAT:
        return False

    # Есть баннер — шлём фото с подписью (лимит подписи Telegram — 1024 символа).
    # Если фото не ушло (нет картинки/ошибка) — откатываемся на обычный текст.
    if banner:
        photo_payload = {
            "chat_id": EVENTS_TG_CHAT,
            "photo": banner,
            "caption": text[:1024],
            "parse_mode": "HTML",
        }
        if EVENTS_TG_THREAD_ID:
            photo_payload["message_thread_id"] = EVENTS_TG_THREAD_ID
        try:
            r = session.post(f"{TG_BASE}/sendPhoto", json=photo_payload, timeout=20)
            if r.status_code == 200:
                return True
            logger.warning(f"[events] TG sendPhoto failed, fallback to text: {r.text[:300]}")
        except Exception as e:
            logger.warning(f"[events] TG sendPhoto error, fallback to text: {e}")

    payload = {
        "chat_id": EVENTS_TG_CHAT,
        "text": text[:4096],
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    if EVENTS_TG_THREAD_ID:
        payload["message_thread_id"] = EVENTS_TG_THREAD_ID
    try:
        r = session.post(f"{TG_BASE}/sendMessage", json=payload, timeout=20)
        if r.status_code != 200:
            logger.warning(f"[events] TG topic send failed: {r.text[:300]}")
            return False
        return True
    except Exception as e:
        logger.warning(f"[events] TG topic send error: {e}")
        return False


def _banner(ev) -> str:
    """URL баннера события, если это валидная http-ссылка, иначе пусто."""
    b = ev.get("banner") or ""
    return b if isinstance(b, str) and b.startswith("http") else ""


# ── Отправка карточки события в Discord (embed с картинкой) ──────────────────
_DC_EVENT_COLOR = 0x5865F2  # синий

def _discord_send_event(ev) -> bool:
    if not EVENTS_DISCORD_WEBHOOK:
        return False
    name = ev.get("name") or "СОБЫТИЕ VATSIM (RUS)"
    link = ev.get("link") or "https://my.vatsim.net/events"
    airports = ev.get("airports") or []
    icaos = ", ".join(a.get("icao", "") for a in airports if a.get("icao")) or "—"
    start = _fmt_start(ev.get("start_time"))
    embed = {
        "title": name[:256],
        "url": link,
        "color": _DC_EVENT_COLOR,
        "description": f"🛫 Аэропорты: {icaos}\n🕒 Начало: {start}",
        "footer": {"text": "VA UP! · VATSIM RUS"},
    }
    banner = _banner(ev)
    if banner:
        embed["image"] = {"url": banner}
    try:
        r = session.post(EVENTS_DISCORD_WEBHOOK, json={"embeds": [embed]}, timeout=15)
        if r.status_code in (200, 204):
            return True
        logger.warning(f"[events] Discord embed failed ({r.status_code}): {r.text[:200]}")
        return False
    except Exception as e:
        logger.warning(f"[events] Discord send error: {e}")
        return False


# ── Получение событий дивизиона ─────────────────────────────────────────────
def _fetch_division_events():
    url = f"{VATSIM_BASE}/view/division/{DIVISION}"
    try:
        r = session.get(url, timeout=15)
        if r.status_code != 200:
            logger.warning(f"[events] VATSIM API {r.status_code}: {r.text[:200]}")
            return []
        data = r.json()
        evs = data.get("data") if isinstance(data, dict) else data
        return evs or []
    except Exception as e:
        logger.warning(f"[events] VATSIM fetch error: {e}")
        return []


# ── Основная задача планировщика ────────────────────────────────────────────
def poll_vatsim_events():
    """
    Вызывается APScheduler раз в 10 минут. Никогда не бросает исключение —
    любые сбои логируются и опрос повторится в следующем цикле.
    """
    try:
        ensure_events_table()
        events = _fetch_division_events()
        if not events:
            return

        touched = 0
        for ev in events:
            try:
                eid = str(ev.get("id") or "").strip()
                if not eid:
                    continue
                if not _is_division(ev, DIVISION):
                    continue
                if not _is_relevant(ev):
                    continue

                row = db_execute(
                    "SELECT tg_sent, vk_sent, discord_sent "
                    "FROM sent_events WHERE network=%s AND event_id=%s",
                    ("vatsim", eid), fetch="one",
                )
                if row is None:
                    db_execute(
                        "INSERT INTO sent_events (network, event_id) "
                        "VALUES (%s, %s) ON CONFLICT DO NOTHING",
                        ("vatsim", eid),
                    )
                    tg_done, vk_done, dc_done = False, False, False
                else:
                    # Пул создан с RealDictCursor → row это словарь, читаем по именам.
                    tg_done = bool(row["tg_sent"])
                    vk_done = bool(row["vk_sent"])
                    dc_done = bool(row["discord_sent"])

                # Нечего досылать?
                dc_satisfied = dc_done or not EVENTS_DISCORD_WEBHOOK
                if tg_done and vk_done and dc_satisfied:
                    continue

                text = _fmt_event(ev)
                banner = _banner(ev)

                if not tg_done and _tg_send_topic(text, banner):
                    tg_done = True
                if not vk_done and vk_send(text):
                    vk_done = True
                if EVENTS_DISCORD_WEBHOOK and not dc_done and _discord_send_event(ev):
                    dc_done = True

                db_execute(
                    "UPDATE sent_events SET tg_sent=%s, vk_sent=%s, discord_sent=%s "
                    "WHERE network=%s AND event_id=%s",
                    (tg_done, vk_done, dc_done, "vatsim", eid),
                )
                touched += 1
            except Exception as e:
                logger.warning(f"[events] event handling error: {e}")

        if touched:
            logger.info(f"[events] VATSIM(RUS): событий с отправкой в этом цикле — {touched}")
    except Exception as e:
        logger.warning(f"[events] poll skipped: {e}")

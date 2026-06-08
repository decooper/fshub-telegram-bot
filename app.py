"""
app.py — VA UP! Flask Web Service

Отвечает за:
 - приём вебхуков FSHub (flight.departed / flight.completed / ...)
 - приём команд Telegram
 - публичный API для сайта va-up.ru
 - фоновый планировщик (APScheduler BackgroundScheduler)

Вся бизнес-логика, DB-хелперы и форматтеры — в core.py.

Start command на Render:
    gunicorn app:app --workers 1 --threads 4 --timeout 120 --preload
"""

import re
import os
import sys
import time
import hashlib
import hmac
import threading
import logging
from collections import Counter
from datetime import datetime
from typing import Dict, List, Optional

from flask import Flask, request, jsonify
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED

# ── Вся бизнес-логика из core.py ──────────────────────────────
from core import (
    # config
    BOT_TOKEN, CHAT_ID, FSA_KEY, ADMIN_ID,
    WEBHOOK_SECRET, DATABASE_URL,
    TG_BASE, FSA_ENRICH_DEPARTURE_DELAY, FSA_ENRICH_ARRIVAL_DELAY,
    # logging
    logger,
    # db init
    _create_pool, _init_db, db_execute,
    # db flights
    db_add_flight, db_update_flight_route,
    db_last_flights, db_all_flights,
    db_flights_this_month, db_top_landings,
    # db economy
    db_save_daily_economy, db_get_monthly_economy, db_get_today_economy,
    # db contest
    CONTEST_POINTS_PER_LANDING, CONTEST_MONTHLY_LIMIT,
    CONTEST_RATE_MIN, CONTEST_RATE_MAX,
    is_contest_landing, db_contest_add, db_contest_month,
    # db operation
    OPERATION_NAME, OPERATION_START, OPERATION_END,
    OPERATION_LEGS, OPERATION_LEG_MAP,
    OPERATION_HARD_CRASH, OPERATION_FAIL_RATE,
    OPERATION_VATSIM_BONUS, OPERATION_MAX_POINTS,
    operation_is_active, op_calc_points,
    db_op_get_pilot, db_op_all_pilots, db_op_register_pilot,
    db_op_add_leg, db_op_update_pilot, db_op_start_new_ferry,
    db_op_admin_set, db_op_reset_pilot, db_op_check_daily_limit,
    # telegram
    session, tg_send, tg_photo, tg_setup_webhook, tg_edit_message,
    # discord
    discord_send, discord_send_flights, discord_send_event,
    discord_send_departure, discord_send_landing, discord_send_hard_landing,
    discord_send_operation, discord_send_screenshots,
    DISCORD_WEBHOOK_SCREENSHOTS,
    # fsa
    fsa_airline_data, fsa_active_flights, fsa_daily_transactions,
    fsa_get_pilot_id, fsa_get_pilot_status, fsa_get_recent_report,
    fsa_refresh_pilot_cache, fsa_refresh_pilot_cache2,
    _departure_cache, _departure_cache_lock,
    # helpers
    _is_plan_empty, _is_valid_icao, _aggregate,
    # formatters
    MONTH_NAMES,
    fmt_stats, fmt_last, fmt_top_landings, fmt_top_pilots,
    fmt_daily_economy, fmt_monthly_economy,
    fmt_active_flights, fmt_va_info,
    fmt_contest, fmt_operation, fmt_operation_digest,
    fmt_runway,
    # economy
    snapshot_daily_economy,
    # landing rating
    landing_rating,
)

if not BOT_TOKEN or not CHAT_ID:
    print("❌ TG_BOT_TOKEN or TG_CHAT_ID missing")
    sys.exit(1)

if not DATABASE_URL:
    print("❌ DATABASE_URL missing")
    sys.exit(1)

logger.info("Starting VA UP! PostgreSQL Edition")

# ═══════════════════════════════════════════════════════════════
# ДЕДУПЛИКАЦИЯ СОБЫТИЙ FSHUB
# Храним события с timestamp — удаляем старше 1 часа,
# а не весь set целиком (защита от race condition при .clear()).
# ═══════════════════════════════════════════════════════════════

_processed_events: Dict[str, float] = {}   # key → timestamp добавления
_processed_lock = threading.Lock()
_DEDUP_TTL = 3600  # секунд


def is_duplicate_event(event_id: str, event_type: str) -> bool:
    key = f"{event_type}:{event_id}"
    now = time.time()
    with _processed_lock:
        # Удаляем протухшие записи (TTL 1 час)
        expired = [k for k, ts in _processed_events.items() if now - ts > _DEDUP_TTL]
        for k in expired:
            del _processed_events[k]
        # Проверяем дубль
        if key in _processed_events:
            return True
        _processed_events[key] = now
        return False


# ═══════════════════════════════════════════════════════════════
# СОСТОЯНИЕ ДИАЛОГА /runway
# ═══════════════════════════════════════════════════════════════

_awaiting_icao: Dict[str, bool] = {}
_awaiting_lock = threading.Lock()

# ═══════════════════════════════════════════════════════════════
# TELEGRAM — вспомогательные функции (специфичные для app.py)
# ═══════════════════════════════════════════════════════════════

def tg_send_with_cancel(text: str, chat_id) -> bool:
    """Отправляет сообщение с inline-кнопкой Отмена."""
    try:
        r = session.post(
            f"{TG_BASE}/sendMessage",
            json={
                "chat_id": str(chat_id),
                "text": text[:4096],
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
                "reply_markup": {
                    "inline_keyboard": [[
                        {"text": "❌ Отмена", "callback_data": "runway_cancel"}
                    ]]
                },
            },
            timeout=20,
        )
        if r.status_code != 200:
            logger.warning(f"tg_send_with_cancel failed: {r.text}")
            return False
        return True
    except Exception as e:
        logger.exception(f"tg_send_with_cancel error: {e}")
        return False


def tg_answer_callback(callback_query_id: str, text: str = "") -> None:
    try:
        session.post(
            f"{TG_BASE}/answerCallbackQuery",
            json={"callback_query_id": callback_query_id, "text": text},
            timeout=10,
        )
    except Exception as e:
        logger.exception(f"answerCallbackQuery error: {e}")


def tg_send_menu(chat_id) -> bool:
    try:
        r = session.post(
            f"{TG_BASE}/sendMessage",
            json={
                "chat_id": str(chat_id),
                "text": "✈️ <b>VA UP! Operations Panel</b>\n\nВыберите раздел:",
                "parse_mode": "HTML",
                "reply_markup": {
                    "inline_keyboard": [
                        [
                            {"text": "📊 Статистика",      "callback_data": "cmd_stats"},
                            {"text": "✈️ Последние рейсы", "callback_data": "cmd_last"},
                        ],
                        [
                            {"text": "🏆 Топ пилоты",    "callback_data": "cmd_top"},
                            {"text": "🛬 Топ посадки",   "callback_data": "cmd_top_landing"},
                        ],
                        [
                            {"text": "💰 Финансы",       "callback_data": "cmd_economy"},
                            {"text": "📅 За месяц",      "callback_data": "cmd_monthly"},
                        ],
                        [
                            {"text": "📡 Онлайн",        "callback_data": "cmd_live"},
                            {"text": "🏢 О компании",    "callback_data": "cmd_va"},
                        ],
                        [
                            {"text": "🛫 Полосы (Runway)",  "callback_data": "cmd_runway"},
                            {"text": "🎯 Мастер Посадки",   "callback_data": "cmd_contest"},
                        ],
                        [
                            {"text": "✈️ Операция «Тихий Вжух»", "callback_data": "cmd_operation"},
                        ],
                    ]
                },
            },
            timeout=20,
        )
        if r.status_code != 200:
            logger.warning(f"tg_send_menu failed: {r.text}")
            return False
        return True
    except Exception as e:
        logger.exception(f"tg_send_menu error: {e}")
        return False


# Маппинг callback_data → форматтер (заполняется после определения всех функций)
MENU_CALLBACKS: Dict[str, callable] = {
    "cmd_stats":       fmt_stats,
    "cmd_last":        fmt_last,
    "cmd_top":         fmt_top_pilots,
    "cmd_top_landing": fmt_top_landings,
    "cmd_economy":     fmt_daily_economy,
    "cmd_monthly":     fmt_monthly_economy,
    "cmd_live":        fmt_active_flights,
    "cmd_va":          fmt_va_info,
    "cmd_contest":     fmt_contest,
    "cmd_operation":   fmt_operation,
}


def handle_callback_query(cq: Dict) -> None:
    cq_id      = cq.get("id", "")
    data       = cq.get("data", "")
    chat_id    = str(cq.get("message", {}).get("chat", {}).get("id", ""))
    message_id = cq.get("message", {}).get("message_id")

    if data == "runway_cancel":
        with _awaiting_lock:
            _awaiting_icao.pop(chat_id, None)
        tg_answer_callback(cq_id, "Отменено")
        if message_id:
            tg_edit_message(chat_id, message_id, "✅ Запрос полосы отменён.")
        logger.info(f"Runway dialog cancelled by chat={chat_id}")
        return

    if data == "cmd_runway":
        tg_answer_callback(cq_id)
        with _awaiting_lock:
            _awaiting_icao[chat_id] = True
        tg_send_with_cancel(
            "✈️ Введите ICAO-код аэропорта:\n<i>Например: UHWW, UUEE, EGLL</i>",
            chat_id,
        )
        return

    if data in MENU_CALLBACKS:
        tg_answer_callback(cq_id)
        try:
            result = MENU_CALLBACKS[data]()
            tg_send(result, chat_id)
        except Exception as e:
            logger.exception(f"Menu callback {data} failed: {e}")
            tg_send("⚠️ Ошибка при выполнении команды.", chat_id)
        return

    tg_answer_callback(cq_id)


# ═══════════════════════════════════════════════════════════════
# FSA — обогащение сообщений в фоне
# ═══════════════════════════════════════════════════════════════

def _enrich_departure_from_fsa(
    message_id: int,
    chat_id_str: str,
    pilot_name: str,
    aircraft_name: str,
    delay: int = 90,
) -> None:
    time.sleep(delay)
    logger.info(f"[Enrich] Запрос FSAirlines для пилота '{pilot_name}' (delay={delay}s)")

    pilot_id = fsa_get_pilot_id(pilot_name)
    if not pilot_id:
        logger.warning(f"[Enrich] Пилот '{pilot_name}' не найден в FSAirlines")
        tg_edit_message(
            chat_id_str, message_id,
            f"🛫 <b>ВЫЛЕТ ПОДТВЕРЖДЁН — CLEARED FOR TAKEOFF</b>\n\n"
            f"👨‍✈️ Captain: <b>{pilot_name}</b>\n"
            f"✈️ Aircraft: <b>{aircraft_name}</b>\n\n"
            f"⚠️ <i>Маршрут не найден — пилот не активировал план в RLM Client</i>\n"
            f"✈️ <i>Желаем попутного ветра и мягкой посадки!</i>"
        )
        return

    status = fsa_get_pilot_status(pilot_id)
    if not status:
        logger.warning(f"[Enrich] Статус пилота {pilot_id} не получен из FSAirlines")
        return

    dep       = status.get("departure", "????")
    arr       = status.get("arrival", "????")

    active  = fsa_active_flights()
    flt_no  = "N/A"
    for f in active:
        if str(f.get("user_id")) == str(pilot_id):
            flt_no = f.get("number", "N/A")
            break

    logger.info(f"[Enrich] Получены данные FSA: {dep}→{arr} flight={flt_no}")

    if _is_valid_icao(dep) and _is_valid_icao(arr):
        with _departure_cache_lock:
            _departure_cache[pilot_name] = {
                "dep":       dep,
                "arr":       arr,
                "flight_no": flt_no,
                "ts":        time.time(),
                "from_fsa":  True,
            }

    tg_edit_message(
        chat_id_str, message_id,
        f"🛫 <b>ВЫЛЕТ ПОДТВЕРЖДЁН — CLEARED FOR TAKEOFF</b>\n\n"
        f"👨‍✈️ Captain: <b>{pilot_name}</b>\n"
        f"🆔 Flight: <b>{flt_no}</b>\n"
        f"🗺 Route: <b>{dep} → {arr}</b>\n"
        f"✈️ Aircraft: <b>{aircraft_name}</b>\n\n"
        f"✈️ <i>Желаем попутного ветра и мягкой посадки!</i>"
    )


def _enrich_completed_from_fsa(
    message_id: int,
    chat_id_str: str,
    pilot_name: str,
    aircraft_name: str,
    airport_name: str,
    rate: int,
    rating: str,
    emoji: str,
    extras: str,
    flight_link: str,
    arrival_time: str,
    delay: int = 60,
    flight_id_for_db: Optional[str] = None,
) -> None:
    time.sleep(delay)
    logger.info(f"[Enrich arrival] Запрос FSAirlines для пилота '{pilot_name}'")

    pilot_id = fsa_get_pilot_id(pilot_name)
    dep, arr, flight_no = "????", "????", "N/A"

    if pilot_id:
        report = fsa_get_recent_report(pilot_id, arrival_time)
        if report:
            dep       = report.get("dep", "????") or "????"
            arr       = report.get("arr", "????") or "????"
            flight_no = report.get("number", "N/A") or "N/A"
            logger.info(f"[Enrich arrival] Найден маршрут FSA: {dep}→{arr} {flight_no}")
            if _is_valid_icao(dep) and _is_valid_icao(arr) and flight_id_for_db:
                db_update_flight_route(flight_id_for_db, flight_no, dep, arr)
                logger.info(f"[Enrich arrival] БД обновлена для flight_id={flight_id_for_db}")
        else:
            logger.warning(f"[Enrich arrival] Отчёт FSA не найден для пилота {pilot_id}")
    else:
        logger.warning(f"[Enrich arrival] Пилот '{pilot_name}' не найден в FSAirlines")

    route_line = (
        f"🗺 Route: <b>{dep} → {arr}</b>\n"
        if dep != "????" else
        f"🗺 Route: <i>не определён</i>\n"
    )

    tg_edit_message(
        chat_id_str, message_id,
        f"🛬 <b>ПОСАДКА ВЫПОЛНЕНА — TOUCHDOWN</b> {emoji}\n\n"
        f"👨‍✈️ Captain: <b>{pilot_name}</b>\n"
        f"🆔 Flight: <b>{flight_no}</b>\n"
        f"{route_line}"
        f"📍 Airport: <b>{airport_name}</b>\n"
        f"✈️ Aircraft: <b>{aircraft_name}</b>\n"
        f"📊 Landing Rate: <b>{rate} fpm</b> — {rating}"
        f"{extras}"
        f"{flight_link}"
    )


# ═══════════════════════════════════════════════════════════════
# FSHUB EVENT HANDLERS
# ═══════════════════════════════════════════════════════════════

def handle_departure(data: Dict):
    d         = data.get("_data") or {}
    flight_id = str(d.get("id", ""))

    if flight_id and is_duplicate_event(flight_id, "departure"):
        logger.info(f"Пропуск дублирующего departure для рейса {flight_id}")
        return

    user     = d.get("user") or {}
    plan     = d.get("plan") or {}
    aircraft = d.get("aircraft") or {}

    pilot_name    = user.get("name", "Unknown")
    aircraft_name = aircraft.get("icao_name", "N/A")

    if not _is_plan_empty(plan):
        logger.info(f"[Departure] Полный план от FSHub для '{pilot_name}'")
        with _departure_cache_lock:
            _departure_cache[pilot_name] = {
                "dep":       plan.get("departure", "????"),
                "arr":       plan.get("arrival", "????"),
                "flight_no": plan.get("flight_no", "N/A"),
                "ts":        time.time(),
            }
        _dep_msg = (
            f"🛫 <b>ВЫЛЕТ ПОДТВЕРЖДЁН — CLEARED FOR TAKEOFF</b>\n\n"
            f"👨‍✈️ Captain: <b>{pilot_name}</b>\n"
            f"🆔 Flight: <b>{plan.get('flight_no')}</b>\n"
            f"🗺 Route: <b>{plan.get('departure')} → {plan.get('arrival')}</b>\n"
            f"✈️ Aircraft: <b>{aircraft_name}</b>\n\n"
            f"✈️ <i>Желаем попутного ветра и мягкой посадки!</i>"
        )
        tg_send(_dep_msg)
        discord_send_departure(
            pilot=pilot_name,
            flight_no=plan.get("flight_no", "N/A"),
            dep=plan.get("departure", "????"),
            arr=plan.get("arrival", "????"),
            aircraft=aircraft_name,
        )
    else:
        logger.info(
            f"[Departure] Пустой план от FSHub для '{pilot_name}', "
            f"запускаю обогащение через {FSA_ENRICH_DEPARTURE_DELAY}с"
        )
        _dep_placeholder = (
            f"🛫 <b>ВЫЛЕТ ПОДТВЕРЖДЁН — CLEARED FOR TAKEOFF</b>\n\n"
            f"👨‍✈️ Captain: <b>{pilot_name}</b>\n"
            f"✈️ Aircraft: <b>{aircraft_name}</b>\n"
            f"🗺 Route: <b>⏳ Загружаю маршрут...</b>\n\n"
            f"✈️ <i>Желаем попутного ветра и мягкой посадки!</i>"
        )
        # В Discord сразу шлём placeholder — без редактирования
        discord_send_departure(
            pilot=pilot_name,
            flight_no="N/A",
            dep="????",
            arr="????",
            aircraft=aircraft_name,
            is_loading=True,
        )
        r = session.post(
            f"{TG_BASE}/sendMessage",
            json={
                "chat_id": CHAT_ID,
                "text": _dep_placeholder,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
            timeout=20,
        )
        if r.status_code == 200:
            message_id = r.json().get("result", {}).get("message_id")
            if message_id:
                threading.Thread(
                    target=_enrich_departure_from_fsa,
                    args=(message_id, CHAT_ID, pilot_name, aircraft_name,
                          FSA_ENRICH_DEPARTURE_DELAY),
                    daemon=True,
                ).start()
        else:
            logger.warning(f"[Departure] Не удалось отправить сообщение: {r.text}")


def handle_completed(data: Dict):
    d         = data.get("_data") or {}
    report_id = str(d.get("id", ""))

    if report_id and is_duplicate_event(report_id, "completed"):
        logger.info(f"Пропуск дублирующего completed для рейса {report_id}")
        return

    arrival  = d.get("arrival") or {}
    plan     = d.get("plan") or {}
    user     = arrival.get("user") or d.get("user") or {}
    aircraft = arrival.get("aircraft") or d.get("aircraft") or {}
    airport  = arrival.get("airport") or {}

    flight_no = plan.get("callsign") or plan.get("flight_no", "N/A")
    dep = plan.get("icao_dep") or plan.get("departure", "????")
    arr = plan.get("icao_arr") or plan.get("arrival", "????")

    rate           = int(arrival.get("landing_rate", 0))
    rating, emoji  = landing_rating(rate)

    distance_nm = (d.get("distance") or {}).get("nm")
    fuel_burnt  = d.get("fuel_burnt")
    max_alt     = (d.get("max") or {}).get("alt")

    db_add_flight(
        flight_id=report_id or None,
        pilot=user.get("name", "Unknown"),
        flight_no=flight_no,
        departure=dep,
        arrival=arr,
        aircraft=aircraft.get("icao_name", "Unknown"),
        landing_rate=rate,
        profit=None,
    )

    flight_link = (
        f"\n🔗 <a href='https://fshub.io/flight/{report_id}/report'>Открыть отчёт о рейсе</a>"
        if report_id else ""
    )

    extras = ""
    if distance_nm:
        extras += f"\n📏 Distance: <b>{distance_nm} nm</b>"
    if fuel_burnt:
        extras += f"\n⛽ Fuel burnt: <b>{fuel_burnt} kg</b>"
    if max_alt:
        extras += f"\n🏔 Max altitude: <b>{max_alt:,} ft</b>"

    pilot_name    = user.get("name", "Unknown")
    aircraft_name = aircraft.get("icao_name", "Unknown")
    airport_name  = airport.get("name", "Unknown")
    arrival_time  = d.get("arrival_at", "") or ""
    plan_empty    = _is_plan_empty({"flight_no": flight_no, "departure": dep, "arrival": arr})

    msg_text = (
        f"🛬 <b>ПОСАДКА ВЫПОЛНЕНА — TOUCHDOWN</b> {emoji}\n\n"
        f"👨‍✈️ Captain: <b>{pilot_name}</b>\n"
        f"🆔 Flight: <b>{flight_no}</b>\n"
        f"🗺 Route: <b>{'⏳ Загружаю маршрут...' if plan_empty else f'{dep} → {arr}'}</b>\n"
        f"📍 Airport: <b>{airport_name}</b>\n"
        f"✈️ Aircraft: <b>{aircraft_name}</b>\n"
        f"📊 Landing Rate: <b>{rate} fpm</b> — {rating}"
        f"{extras}"
        f"{flight_link}"
    )

    if plan_empty and FSA_KEY:
        airport_icao = airport.get("icao", "").upper()
        cached = None
        with _departure_cache_lock:
            entry = _departure_cache.get(pilot_name)
            if entry and (time.time() - entry.get("ts", 0)) < 86400:
                cached = entry

        if cached:
            cached_arr = cached.get("arr", "????").upper()
            cached_dep = cached.get("dep", "????")
            cached_fno = cached.get("flight_no", "N/A")
            if not _is_valid_icao(cached_dep) or not _is_valid_icao(cached_arr):
                logger.warning(
                    f"[Completed] Невалидные ICAO в кэше: {cached_dep}→{cached_arr}, игнорируем"
                )
                cached = None

        if cached:
            if airport_icao and cached_arr != airport_icao and _is_valid_icao(airport_icao):
                logger.info(
                    f"[Completed] Запасной аэропорт для '{pilot_name}': "
                    f"план={cached_arr}, факт={airport_icao} — жду 15 мин"
                )
                r = session.post(
                    f"{TG_BASE}/sendMessage",
                    json={
                        "chat_id": CHAT_ID,
                        "text": msg_text,
                        "parse_mode": "HTML",
                        "disable_web_page_preview": True,
                    },
                    timeout=20,
                )
                if r.status_code == 200:
                    message_id = r.json().get("result", {}).get("message_id")
                    if message_id:
                        threading.Thread(
                            target=_enrich_completed_from_fsa,
                            args=(
                                message_id, CHAT_ID, pilot_name, aircraft_name,
                                airport_name, rate, rating, emoji,
                                extras, flight_link, arrival_time, 900,
                            ),
                            kwargs={"flight_id_for_db": report_id or None},
                            daemon=True,
                        ).start()
            else:
                logger.info(
                    f"[Completed] Используем кэш вылета для '{pilot_name}': "
                    f"{cached_dep}→{cached_arr}"
                )
                final_msg = (
                    f"🛬 <b>ПОСАДКА ВЫПОЛНЕНА — TOUCHDOWN</b> {emoji}\n\n"
                    f"👨‍✈️ Captain: <b>{pilot_name}</b>\n"
                    f"🆔 Flight: <b>{cached_fno}</b>\n"
                    f"🗺 Route: <b>{cached_dep} → {cached_arr}</b>\n"
                    f"📍 Airport: <b>{airport_name}</b>\n"
                    f"✈️ Aircraft: <b>{aircraft_name}</b>\n"
                    f"📊 Landing Rate: <b>{rate} fpm</b> — {rating}"
                    f"{extras}"
                    f"{flight_link}"
                )
                tg_send(final_msg)
                discord_send_landing(
                    pilot=pilot_name, flight_no=cached_fno,
                    dep=cached_dep, arr=cached_arr,
                    aircraft=aircraft_name, airport=airport_name,
                    rate=rate, rating=rating,
                    distance_nm=distance_nm, fuel_burnt=fuel_burnt, max_alt=max_alt,
                    report_url=f"https://fshub.io/flight/{report_id}/report" if report_id else "",
                )
                if _is_valid_icao(cached_dep) and _is_valid_icao(cached_arr) and report_id:
                    db_update_flight_route(report_id, cached_fno, cached_dep, cached_arr)
                    logger.info(f"[Completed] БД обновлена из кэша для flight_id={report_id}")
                with _departure_cache_lock:
                    _departure_cache.pop(pilot_name, None)
        else:
            logger.info(
                f"[Completed] Нет кэша вылета для '{pilot_name}', "
                f"запускаю обогащение через {FSA_ENRICH_ARRIVAL_DELAY}с"
            )
            r = session.post(
                f"{TG_BASE}/sendMessage",
                json={
                    "chat_id": CHAT_ID,
                    "text": msg_text,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True,
                },
                timeout=20,
            )
            if r.status_code == 200:
                message_id = r.json().get("result", {}).get("message_id")
                if message_id:
                    threading.Thread(
                        target=_enrich_completed_from_fsa,
                        args=(
                            message_id, CHAT_ID, pilot_name, aircraft_name,
                            airport_name, rate, rating, emoji,
                            extras, flight_link, arrival_time, FSA_ENRICH_ARRIVAL_DELAY,
                        ),
                        kwargs={"flight_id_for_db": report_id or None},
                        daemon=True,
                    ).start()
            else:
                logger.warning(f"[Completed] Не удалось отправить: {r.text}")
            # В Discord шлём сразу embed — маршрут будет "загружается..."
            discord_send_landing(
                pilot=pilot_name, flight_no=flight_no,
                dep=dep, arr=arr,
                aircraft=aircraft_name, airport=airport_name,
                rate=rate, rating=rating,
                distance_nm=distance_nm, fuel_burnt=fuel_burnt, max_alt=max_alt,
                report_url=f"https://fshub.io/flight/{report_id}/report" if report_id else "",
                is_loading=plan_empty,
            )
    else:
        tg_send(msg_text)
        discord_send_landing(
            pilot=pilot_name, flight_no=flight_no,
            dep=dep, arr=arr,
            aircraft=aircraft_name, airport=airport_name,
            rate=rate, rating=rating,
            distance_nm=distance_nm, fuel_burnt=fuel_burnt, max_alt=max_alt,
            report_url=f"https://fshub.io/flight/{report_id}/report" if report_id else "",
        )

    if rate < -600:
        _hard_msg = (
            f"⚠️ <b>HARD LANDING ALERT</b>\n\n"
            f"👨‍✈️ Pilot: <b>{user.get('name', 'Unknown')}</b>\n"
            f"📊 Landing Rate: <b>{rate} fpm</b>\n"
            f"✈️ Aircraft inspection recommended."
        )
        tg_send(_hard_msg)
        discord_send_hard_landing(pilot=user.get("name", "Unknown"), rate=rate)

    # ─── Проверка ивента «Тихий Вжух» ──────────────────────────
    if operation_is_active() and _is_valid_icao(dep) and _is_valid_icao(arr):
        leg_key = (dep.upper(), arr.upper())
        if leg_key in OPERATION_LEG_MAP:
            leg_num, leg_pts = OPERATION_LEG_MAP[leg_key]
            op_pilot = db_op_get_pilot(pilot_name)

            if not op_pilot:
                aircraft_name_op = aircraft.get("icao_name", "")
                db_op_register_pilot(pilot_name, aircraft_name_op)
                op_pilot = db_op_get_pilot(pilot_name)
                logger.info(f"[Operation] Авторегистрация пилота '{pilot_name}'")

            if op_pilot and op_pilot["status"] == "finished" and leg_num == 1:
                aircraft_name_op = aircraft.get("icao_name", "")
                new_ferry = db_op_start_new_ferry(pilot_name, aircraft_name_op)
                if new_ferry:
                    op_pilot = db_op_get_pilot(pilot_name)
                    logger.info(f"[Operation] Автостарт перегона #{new_ferry} для '{pilot_name}'")
                    _ferry_msg = (
                        f"✈️ <b>НОВЫЙ ПЕРЕГОН #{new_ferry} — «{OPERATION_NAME}»</b>\n\n"
                        f"👨‍✈️ <b>{pilot_name}</b> начинает новый перегон!\n"
                        f"✈️ Самолёт: <b>{aircraft_name_op}</b>\n"
                        f"🗺 Маршрут: VTBS → SBGL"
                    )
                    tg_send(_ferry_msg)
                    discord_send_operation(
                        title=f"✈️  НОВЫЙ ПЕРЕГОН #{new_ferry} — «{OPERATION_NAME}»",
                        color=0x5865F2,
                        fields=[
                            {"name": "Пилот",    "value": pilot_name,      "inline": True},
                            {"name": "Самолёт",  "value": aircraft_name_op or "N/A", "inline": True},
                            {"name": "Маршрут",  "value": "VTBS → SBGL",   "inline": False},
                        ],
                    )

            if op_pilot and op_pilot["status"] == "active":
                report_url_op = (
                    f"https://fshub.io/flight/{report_id}/report" if report_id else ""
                )

                if leg_num != op_pilot["current_leg"]:
                    logger.info(
                        f"[Operation] {pilot_name} Leg {leg_num} ignored, "
                        f"expected Leg {op_pilot['current_leg']}"
                    )
                else:
                    allowed, legs_today = db_op_check_daily_limit(pilot_name)
                    if not allowed:
                        _limit_msg = (
                            f"⏳ <b>ЛИМИТ ЛЕГОВ — ОПЕРАЦИЯ «{OPERATION_NAME}»</b>\n\n"
                            f"👨‍✈️ <b>{pilot_name}</b>\n"
                            f"✈️ Leg {leg_num}: {dep} → {arr}\n\n"
                            f"❌ Сегодня уже выполнено <b>2 лега</b>.\n"
                            f"🕛 Счётчик сбросится в <b>00:00 UTC (03:00 МСК)</b>"
                        )
                        tg_send(_limit_msg)
                        discord_send_operation(
                            title=f"⏳  ЛИМИТ ЛЕГОВ — «{OPERATION_NAME}»",
                            color=0xF0A332,
                            fields=[
                                {"name": "Пилот", "value": pilot_name, "inline": True},
                                {"name": "Лег",   "value": f"Leg {leg_num}: {dep} → {arr}", "inline": True},
                                {"name": "Статус", "value": "Сегодня уже 2 лега. Сброс в 00:00 UTC (03:00 МСК)", "inline": False},
                            ],
                        )
                        logger.info(f"[Operation] {pilot_name} leg={leg_num} — дневной лимит")
                    else:
                        aircraft_icao_type = (
                            d.get("aircraft") or {}
                        ).get("icao") or aircraft.get("icao_name", "")
                        user_handles = user.get("handles") or {}
                        on_network   = bool(
                            user_handles.get("vatsim") or user_handles.get("ivao")
                        )

                        if rate <= -OPERATION_HARD_CRASH:
                            db_op_add_leg(
                                pilot_name, leg_num, dep, arr, rate,
                                0, report_id or "", report_url_op,
                            )
                            db_op_reset_pilot(pilot_name)
                            _crash_msg = (
                                f"💥 <b>КРУШЕНИЕ — ОПЕРАЦИЯ «{OPERATION_NAME}»</b>\n\n"
                                f"👨‍✈️ <b>{pilot_name}</b>\n"
                                f"✈️ Leg {leg_num}: {dep} → {arr}\n"
                                f"📊 Посадка: <b>{rate} fpm</b>\n\n"
                                f"⚠️ Борт утерян. Весь прогресс сброшен.\n"
                                f"Пилот начинает с Leg 1."
                            )
                            tg_send(_crash_msg)
                            discord_send_operation(
                                title=f"💥  КРУШЕНИЕ — «{OPERATION_NAME}»",
                                color=0xED4245,
                                fields=[
                                    {"name": "Пилот",    "value": pilot_name,   "inline": True},
                                    {"name": "Этап",     "value": f"Leg {leg_num}: {dep} → {arr}", "inline": True},
                                    {"name": "Посадка",  "value": f"{rate} fpm", "inline": True},
                                    {"name": "Статус",   "value": "Борт утерян. Прогресс сброшен. Начинает с Leg 1.", "inline": False},
                                ],
                            )
                        elif rate <= -OPERATION_FAIL_RATE:
                            db_op_add_leg(
                                pilot_name, leg_num, dep, arr, rate,
                                0, report_id or "", report_url_op,
                            )
                            _fail_msg = (
                                f"🔴 <b>ЭТАП ПРОВАЛЕН — ОПЕРАЦИЯ «{OPERATION_NAME}»</b>\n\n"
                                f"👨‍✈️ <b>{pilot_name}</b>\n"
                                f"✈️ Leg {leg_num}: {dep} → {arr}\n"
                                f"📊 Посадка: <b>{rate} fpm</b> — слишком жёстко!\n\n"
                                f"❌ Очки не начислены. Повторите Leg {leg_num}."
                            )
                            tg_send(_fail_msg)
                            discord_send_operation(
                                title=f"🔴  ЭТАП ПРОВАЛЕН — «{OPERATION_NAME}»",
                                color=0xED4245,
                                fields=[
                                    {"name": "Пилот",   "value": pilot_name,   "inline": True},
                                    {"name": "Этап",    "value": f"Leg {leg_num}: {dep} → {arr}", "inline": True},
                                    {"name": "Посадка", "value": f"{rate} fpm — слишком жёстко!", "inline": True},
                                    {"name": "Статус",  "value": f"Очки не начислены. Повторите Leg {leg_num}.", "inline": False},
                                ],
                            )
                        else:
                            earned, coeff, net_bonus = op_calc_points(
                                leg_pts, aircraft_icao_type, on_network
                            )
                            next_leg    = leg_num + 1
                            is_finished = next_leg > len(OPERATION_LEGS)
                            new_status  = "finished" if is_finished else "active"
                            new_points  = op_pilot["total_points"] + earned

                            db_op_add_leg(
                                pilot_name, leg_num, dep, arr, rate,
                                earned, report_id or "", report_url_op,
                                base_points=leg_pts, coeff=coeff,
                                net_bonus=net_bonus, on_network=on_network,
                            )
                            db_op_update_pilot(
                                pilot_name,
                                next_leg if not is_finished else leg_num,
                                new_points, new_status,
                            )

                            coeff_str  = f"x{coeff}" if coeff != 1.0 else ""
                            bonus_str  = f" +{net_bonus} (VATSIM/IVAO)" if net_bonus else ""
                            detail_str = f"{leg_pts}{coeff_str}{bonus_str} = <b>{earned}</b>"
                            ferry_num  = op_pilot.get("ferry_num", 1)

                            if is_finished:
                                _finish_msg = (
                                    f"🏁 <b>ФИНИШ! ОПЕРАЦИЯ «{OPERATION_NAME}»</b>\n\n"
                                    f"👨‍✈️ <b>{pilot_name}</b> завершил перегон #{ferry_num}!\n"
                                    f"✈️ Последний этап: {dep} → {arr}\n"
                                    f"📊 Посадка: <b>{rate} fpm</b>\n"
                                    f"⭐ Очки: {detail_str} | Итого: <b>{new_points:,}</b>\n\n"
                                    f"🎉 Борт успешно перегнан в SBGL!\n"
                                    f"✈️ Готов к следующему перегону — "
                                    f"/operation_admin add {pilot_name} | самолёт"
                                )
                                tg_send(_finish_msg)
                                discord_send_operation(
                                    title=f"🏁  ФИНИШ! — «{OPERATION_NAME}»",
                                    color=0x23A55A,
                                    fields=[
                                        {"name": "Пилот",    "value": pilot_name,  "inline": True},
                                        {"name": "Перегон",  "value": f"#{ferry_num}", "inline": True},
                                        {"name": "Этап",     "value": f"{dep} → {arr}", "inline": True},
                                        {"name": "Посадка",  "value": f"{rate} fpm", "inline": True},
                                        {"name": "Очки",     "value": detail_str,  "inline": True},
                                        {"name": "Итого",    "value": f"{new_points:,}", "inline": True},
                                    ],
                                    footer_extra="Борт успешно перегнан в SBGL!",
                                )
                            else:
                                next_info = next(
                                    (
                                        f"{d2}→{a2}"
                                        for n2, d2, a2, _ in OPERATION_LEGS
                                        if n2 == next_leg
                                    ),
                                    "",
                                )
                                legs_after = legs_today + 1
                                limit_str  = (
                                    f"\n🕛 На сегодня лимит исчерпан. "
                                    f"Следующий лег — после <b>00:00 UTC (03:00 МСК)</b>"
                                    if legs_after >= 2 else
                                    f"\n✅ Сегодня можно выполнить ещё <b>1 лег</b>"
                                )
                                _leg_msg = (
                                    f"✅ <b>LEG {leg_num} ВЫПОЛНЕН — «{OPERATION_NAME}»</b>\n\n"
                                    f"👨‍✈️ <b>{pilot_name}</b>\n"
                                    f"✈️ {dep} → {arr}\n"
                                    f"📊 Посадка: <b>{rate} fpm</b>\n"
                                    f"⭐ Очки: {detail_str} | Итого: <b>{new_points:,}</b>\n"
                                    f"➡️ Следующий: Leg {next_leg} {next_info}"
                                    f"{limit_str}"
                                )
                                tg_send(_leg_msg)
                                discord_send_operation(
                                    title=f"✅  LEG {leg_num} ВЫПОЛНЕН — «{OPERATION_NAME}»",
                                    color=0xFEE75C,
                                    fields=[
                                        {"name": "Пилот",      "value": pilot_name, "inline": True},
                                        {"name": "Этап",       "value": f"{dep} → {arr}", "inline": True},
                                        {"name": "Посадка",    "value": f"{rate} fpm", "inline": True},
                                        {"name": "Очки",       "value": detail_str, "inline": True},
                                        {"name": "Итого",      "value": f"{new_points:,}", "inline": True},
                                        {"name": "Следующий",  "value": f"Leg {next_leg} {next_info}".strip(), "inline": True},
                                    ],
                                )
                        logger.info(
                            f"[Operation] {pilot_name} leg={leg_num} rate={rate} "
                            f"network={on_network}"
                        )

    # ─── Проверка конкурса Мастер Посадки ───────────────────────
    if is_contest_landing(rate):
        report_url = f"https://fshub.io/flight/{report_id}/report" if report_id else ""
        db_contest_add(
            flight_id=report_id,
            pilot=user.get("name", "Unknown"),
            flight_no=flight_no,
            departure=dep,
            arrival=arr,
            aircraft=aircraft.get("icao_name", "Unknown"),
            landing_rate=rate,
            report_url=report_url,
        )
        entries  = db_contest_month()
        position = len(entries)
        slots    = CONTEST_MONTHLY_LIMIT // CONTEST_POINTS_PER_LANDING
        if position <= slots:
            tg_send(
                f"🎯 <b>КАНДИДАТ — МАСТЕР ПОСАДКИ!</b>\n\n"
                f"👨‍✈️ <b>{user.get('name', 'Unknown')}</b>\n"
                f"📊 Landing Rate: <b>{rate} fpm</b>\n"
                f"⭐ Позиция в этом месяце: <b>#{position}</b>\n"
                f"🏅 Начислено: <b>{CONTEST_POINTS_PER_LANDING} баллов</b>\n\n"
                f"🔍 Находится на проверке — результат в конце месяца"
            )
        else:
            tg_send(
                f"🎯 <b>СНАЙПЕРСКАЯ ПОСАДКА!</b>\n\n"
                f"👨‍✈️ <b>{user.get('name', 'Unknown')}</b>\n"
                f"📊 Landing Rate: <b>{rate} fpm</b>\n"
                f"📅 Фонд {CONTEST_MONTHLY_LIMIT} баллов на этот месяц исчерпан — ждём следующего!"
            )


def _send_screenshots_async(screenshots: List, pilot: str = "", flight_no: str = ""):
    for scr in screenshots[:3]:
        url = scr.get("screenshot_url")
        if url:
            tg_photo(url, "📸 <b>Flight Screenshot</b>")
            time.sleep(1)
    # Discord: все скриншоты разом (с паузами внутри функции)
    discord_send_screenshots(screenshots, pilot=pilot, flight_no=flight_no)


def handle_screenshots(data: Dict):
    screenshots = data.get("_data", [])
    if not screenshots:
        return
    # Пробуем извлечь пилота и рейс из первого скриншота если FSHub их передаёт
    first = screenshots[0] if screenshots else {}
    pilot    = (first.get("flight") or {}).get("user", {}).get("name", "")
    flight_no = (first.get("flight") or {}).get("plan", {}).get("callsign", "")
    threading.Thread(
        target=_send_screenshots_async,
        args=(screenshots,),
        kwargs={"pilot": pilot, "flight_no": flight_no},
        daemon=True,
    ).start()


def handle_achievement(data: Dict):
    d           = data.get("_data", {})
    achievement = d.get("achievement", {})
    flight      = d.get("flight", {})
    user        = flight.get("user", {})
    tg_send(
        f"🏆 <b>ACHIEVEMENT UNLOCKED</b>\n\n"
        f"👨‍✈️ {user.get('name', 'Unknown')}\n"
        f"🎯 {achievement.get('title', 'Achievement')}"
    )


FSHUB_HANDLERS = {
    "flight.departed":      handle_departure,
    "flight.completed":     handle_completed,
    "screenshots.uploaded": handle_screenshots,
    "airline.achievement":  handle_achievement,
}

# ═══════════════════════════════════════════════════════════════
# TELEGRAM COMMANDS
# ═══════════════════════════════════════════════════════════════

COMMANDS = {
    "/stats":       fmt_stats,
    "/last":        fmt_last,
    "/top_landing": fmt_top_landings,
    "/top":         fmt_top_pilots,
    "/economy":     fmt_daily_economy,
    "/monthly":     fmt_monthly_economy,
    "/live":        fmt_active_flights,
    "/va":          fmt_va_info,
    "/operation":   fmt_operation,
}

# Скомпилированный паттерн для валидации YYYY-MM (один раз на уровне модуля)
_RE_MONTH = re.compile(r"^\d{4}-\d{2}$")


def handle_tg_command(message: Dict):
    chat_id = message.get("chat", {}).get("id")
    text    = (message.get("text") or "").strip()

    if not chat_id or not text:
        return

    if str(chat_id) == str(CHAT_ID):
        with _awaiting_lock:
            user_is_awaiting = str(chat_id) in _awaiting_icao
        if not user_is_awaiting:
            first_word       = text.split()[0] if text.split() else ""
            addressed_to_bot = "@" in first_word and first_word.startswith("/")
            if not addressed_to_bot:
                return

    logger.info(f"Command from chat={chat_id}: {text}")

    cmd_parts = text.split()
    base_cmd  = cmd_parts[0].split("@")[0] if cmd_parts else ""

    if text.startswith("/start") or text.startswith("/help") or text.startswith("/menu"):
        tg_send_menu(chat_id)
        return

    # ─── /operation_admin ───────────────────────────────────────
    if base_cmd == "/operation_admin":
        if str(chat_id) != str(ADMIN_ID):
            tg_send("⛔ Нет доступа.", chat_id)
            return

        parts = text.split(None, 3)
        sub   = parts[1] if len(parts) > 1 else ""

        if sub == "add":
            rest = text.split(None, 2)[2] if len(text.split(None, 2)) > 2 else ""
            if "|" in rest:
                pilot_part, aircraft_part = rest.split("|", 1)
                pilot    = pilot_part.strip()
                aircraft = aircraft_part.strip()
            else:
                pilot    = rest.strip()
                aircraft = ""
            if not pilot:
                tg_send("Использование: /operation_admin add Имя Фамилия | B738", chat_id)
                return
            ok = db_op_register_pilot(pilot, aircraft)
            if ok:
                p     = db_op_get_pilot(pilot)
                ferry = p["ferry_num"] if p else 1
                msg   = (
                    f"✅ Пилот <b>{pilot}</b> — перегон #{ferry} начат"
                    f"{' на ' + aircraft if aircraft else ''}."
                )
            else:
                msg = f"⚠️ Пилот <b>{pilot}</b> уже ведёт активный перегон."
            tg_send(msg, chat_id)

        elif sub == "set":
            rest = text.split(None, 2)[2] if len(text.split(None, 2)) > 2 else ""
            if "|" not in rest:
                tg_send("Использование: /operation_admin set Имя Фамилия | +500", chat_id)
                return
            pilot_part, delta_part = rest.split("|", 1)
            pilot = pilot_part.strip()
            try:
                delta = int(delta_part.strip().replace("+", ""))
            except ValueError:
                tg_send("Дельта должна быть числом (например +500 или -200)", chat_id)
                return
            p = db_op_get_pilot(pilot)
            if not p:
                tg_send(f"Пилот <b>{pilot}</b> не найден.", chat_id)
                return
            db_op_admin_set(pilot, 0, delta)
            p2 = db_op_get_pilot(pilot)
            tg_send(
                f"✅ <b>{pilot}</b>: {p['total_points']:,} → <b>{p2['total_points']:,}</b> очков.",
                chat_id,
            )

        elif sub == "leg":
            rest = text.split(None, 2)[2] if len(text.split(None, 2)) > 2 else ""
            if "|" not in rest:
                tg_send("Использование: /operation_admin leg Имя Фамилия | 3", chat_id)
                return
            pilot_part, leg_part = rest.split("|", 1)
            pilot = pilot_part.strip()
            try:
                leg = int(leg_part.strip())
            except ValueError:
                tg_send("Номер лега должен быть числом.", chat_id)
                return
            if not 1 <= leg <= len(OPERATION_LEGS) + 1:
                tg_send(f"Номер лега: 1–{len(OPERATION_LEGS)}.", chat_id)
                return
            p = db_op_get_pilot(pilot)
            if not p:
                tg_send(f"Пилот <b>{pilot}</b> не найден.", chat_id)
                return
            db_execute(
                "UPDATE operation_pilots SET current_leg = %s WHERE pilot_name = %s",
                (leg, pilot),
            )
            tg_send(f"✅ <b>{pilot}</b>: текущий лег установлен на <b>{leg}</b>.", chat_id)

        elif sub == "reset":
            rest  = text.split(None, 2)[2] if len(text.split(None, 2)) > 2 else ""
            pilot = rest.strip()
            if not pilot:
                tg_send("Использование: /operation_admin reset Имя Фамилия", chat_id)
                return
            p = db_op_get_pilot(pilot)
            if not p:
                tg_send(f"Пилот <b>{pilot}</b> не найден.", chat_id)
                return
            db_op_reset_pilot(pilot)
            tg_send(f"✅ Прогресс <b>{pilot}</b> сброшен.", chat_id)

        elif sub == "list":
            pilots = db_op_all_pilots()
            if not pilots:
                tg_send("Участников нет.", chat_id)
                return
            lines = [
                f"• <b>{p['pilot_name']}</b> ({p['aircraft'] or '—'}) "
                f"Leg {p['current_leg']} | {p['total_points']:,} очк. [{p['status']}]"
                for p in pilots
            ]
            tg_send("📋 <b>Участники операции:</b>\n\n" + "\n".join(lines), chat_id)

        else:
            tg_send(
                "📋 <b>Команды администратора:</b>\n\n"
                "/operation_admin add Имя Фамилия | B738\n"
                "/operation_admin set Имя Фамилия | +500\n"
                "/operation_admin leg Имя Фамилия | 3\n"
                "/operation_admin reset Имя Фамилия\n"
                "/operation_admin list",
                chat_id,
            )
        return

    # ─── /contest [YYYY-MM] ─────────────────────────────────────
    if base_cmd == "/contest":
        month_arg = None
        if len(cmd_parts) >= 2:
            raw = cmd_parts[1].strip()
            if _RE_MONTH.match(raw):
                month_arg = raw
            else:
                tg_send(
                    "❌ Неверный формат месяца.\n"
                    "Пример: <code>/contest 2026-05</code>",
                    chat_id,
                )
                return
        tg_send(fmt_contest(month_arg), chat_id)
        return

    # ─── /runway [ICAO] ─────────────────────────────────────────
    if base_cmd == "/runway":
        if len(cmd_parts) < 2:
            with _awaiting_lock:
                _awaiting_icao[str(chat_id)] = True
            tg_send_with_cancel(
                "✈️ Введите ICAO-код аэропорта:\n<i>Например: UHWW, UUEE, EGLL</i>",
                chat_id,
            )
        else:
            icao_raw = cmd_parts[1].upper()
            if len(icao_raw) != 4 or not icao_raw.isalnum():
                tg_send(
                    "❌ Некорректный ICAO-код. Должен содержать 4 символа (буквы и цифры).\n"
                    "Пример: <code>/runway UHWW</code>",
                    chat_id,
                )
            else:
                logger.info(f"Runway request: {icao_raw} from {chat_id}")
                tg_send(fmt_runway(icao_raw), chat_id)
        return

    # ─── Ответ на диалог /runway ─────────────────────────────────
    with _awaiting_lock:
        is_awaiting = _awaiting_icao.pop(str(chat_id), False)

    if is_awaiting:
        icao_raw = text.upper().strip()
        if len(icao_raw) != 4 or not icao_raw.isalnum():
            tg_send(
                "❌ Некорректный ICAO-код. Должен содержать 4 символа (буквы и цифры).\n"
                "Попробуйте снова: <code>/runway UHWW</code>",
                chat_id,
            )
        else:
            logger.info(f"Runway request (dialog): {icao_raw} from {chat_id}")
            tg_send(fmt_runway(icao_raw), chat_id)
        return

    # ─── Стандартные команды ─────────────────────────────────────
    cmd = text.split("@")[0]
    if cmd in COMMANDS:
        try:
            tg_send(COMMANDS[cmd](), chat_id)
        except Exception as e:
            logger.exception(f"Command failed: {e}")
            tg_send("⚠️ Command error.", chat_id)
        return

    tg_send("Unknown command. Use /help", chat_id)


# ═══════════════════════════════════════════════════════════════
# FLASK APP
# ═══════════════════════════════════════════════════════════════

app = Flask(__name__)


def _job_listener(event):
    if event.exception:
        logger.error(f"Job {event.job_id} crashed: {event.exception}")
    else:
        logger.info(f"Job {event.job_id} executed successfully")


def init_scheduler():
    scheduler = BackgroundScheduler(
        daemon=True,
        executors={"default": ThreadPoolExecutor(max_workers=2)},
        job_defaults={
            "coalesce": True,
            "max_instances": 1,
            "misfire_grace_time": 3600,
        },
        timezone="UTC",
    )

    # ─── Ежедневные задачи ──────────────────────────────────────
    scheduler.add_job(
        snapshot_daily_economy,
        "cron", hour=23, minute=50,
        id="daily_economy_snapshot",
    )
    scheduler.add_job(
        lambda: tg_send(fmt_daily_economy()),
        "cron", hour=20, minute=0,
        id="daily_economy_report",
    )
    scheduler.add_job(
        lambda: tg_send(fmt_stats()),
        "cron", hour=21, minute=0,
        id="daily_stats",
    )

    # ─── Еженедельные задачи ────────────────────────────────────
    scheduler.add_job(
        lambda: tg_send(fmt_top_landings()),
        "cron", day_of_week="sun", hour=12, minute=0,
        id="weekly_landing_ranking",
    )
    scheduler.add_job(
        lambda: tg_send(fmt_operation_digest()) if operation_is_active() else None,
        "cron", day_of_week="sun", hour=11, minute=0,
        id="weekly_operation_digest",
    )
    scheduler.add_job(
        lambda: tg_send(fmt_operation()) if operation_is_active() else None,
        "cron", day_of_week="fri", hour=0, minute=0,
        id="weekly_operation_standings",
    )
    scheduler.add_job(
        lambda: tg_send(fmt_top_pilots()),
        "cron", day_of_week="sun", hour=10, minute=0,
        id="weekly_top_pilots",
    )
    scheduler.add_job(
        lambda: tg_send(
            "🛫 <b>СОВМЕСТНАЯ СУББОТНЯЯ ОПЕРАЦИЯ!</b>\n\n"
            "⏰ Москва: 09:00 ☀️  |  Камчатка: 18:00 🌙\n\n"
            "✈️ Предлагайте маршрут в комментариях!\nКто присоединяется? 👇"
        ),
        "cron", day_of_week="sat", hour=6, minute=0,
        id="saturday_inv",
    )
    scheduler.add_job(
        lambda: tg_send(
            "🏆 <b>ЕЖЕНЕДЕЛЬНЫЙ ВЫЗОВ ЭКИПАЖУ!</b>\n\n"
            "🔹 Цель: 3 рейса за 7 дней\n"
            "🔹 Бонус: лучшая посадка недели\n\nГотов принять вызов? 💪"
        ),
        "cron", day_of_week="mon", hour=8, minute=0,
        id="monday_challenge",
    )

    # ─── Ежемесячные задачи ─────────────────────────────────────
    scheduler.add_job(
        lambda: tg_send(fmt_monthly_economy()),
        "cron", day=1, hour=9, minute=0,
        id="monthly_digest",
    )

    # ─── Фоновое обновление кэша пилотов FSAirlines (раз в час) ─
    scheduler.add_job(
        _refresh_fsa_pilot_cache_bg,
        "interval", hours=1,
        id="fsa_pilot_cache_refresh",
        next_run_time=datetime.now(),  # сразу при старте
    )

    scheduler.add_listener(_job_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
    scheduler.start()

    jobs = scheduler.get_jobs()
    logger.info(f"Планировщик запущен. Активных задач: {len(jobs)}")
    for job in jobs:
        logger.info(f"  • {job.id} → следующий запуск: {job.next_run_time}")

    return scheduler


def _refresh_fsa_pilot_cache_bg():
    """Фоновое обновление кэша пилотов — не блокирует вебхуки."""
    try:
        fsa_refresh_pilot_cache()
        if FSA_KEY:  # FSA_KEY2 проверяется внутри fsa_refresh_pilot_cache2
            fsa_refresh_pilot_cache2()
    except Exception as e:
        logger.warning(f"fsa_pilot_cache refresh error: {e}")


# ═══════════════════════════════════════════════════════════════
# FLASK ROUTES
# ═══════════════════════════════════════════════════════════════

@app.route("/")
def home():
    return jsonify({
        "status":      "running",
        "service":     "VA UP! PostgreSQL Edition",
        "fsa_enabled": bool(FSA_KEY),
    })


@app.route("/health")
def health():
    return jsonify({"ok": True}), 200


def _verify_fshub_signature(payload: bytes, signature_header: str) -> bool:
    """
    Проверяет HMAC-SHA256 подпись FSHub.
    Если WEBHOOK_SECRET не задан — пропускаем проверку (совместимость).
    """
    if not WEBHOOK_SECRET:
        return True
    if not signature_header:
        return False
    try:
        expected = hmac.new(
            WEBHOOK_SECRET.encode(), payload, hashlib.sha256
        ).hexdigest()
        # FSHub присылает подпись в формате "sha256=<hex>"
        parts = signature_header.split("=", 1)
        actual = parts[1] if len(parts) == 2 else signature_header
        return hmac.compare_digest(expected, actual)
    except Exception as e:
        logger.warning(f"Signature verification error: {e}")
        return False


@app.route("/webhook", methods=["GET", "POST"])
def fshub_webhook():
    if request.method == "GET":
        return jsonify({"status": "ok"})

    # Проверка подписи FSHub
    sig = request.headers.get("X-FSHub-Signature", "")
    if not _verify_fshub_signature(request.get_data(), sig):
        logger.warning("FSHub webhook: invalid signature")
        return jsonify({"error": "invalid signature"}), 403

    try:
        data = request.get_json(force=True)
        if not data:
            return jsonify({"error": "no json"}), 400

        event = data.get("_type", "")
        logger.info(f"FSHub событие: {event}")

        handler = FSHUB_HANDLERS.get(event)
        if handler:
            handler(data)

        return jsonify({"ok": True})
    except Exception as e:
        logger.exception(f"Webhook failure: {e}")
        return jsonify({"error": str(e)}), 500


@app.route(f"/bot/{BOT_TOKEN}", methods=["POST"])
def tg_webhook():
    try:
        data = request.get_json(force=True) or {}
        if "callback_query" in data:
            handle_callback_query(data["callback_query"])
        else:
            message = data.get("message") or data.get("channel_post") or {}
            handle_tg_command(message)
    except Exception as e:
        logger.exception(f"Telegram webhook failure: {e}")
    return jsonify({"ok": True})


# ═══════════════════════════════════════════════════════════════
# PUBLIC API — для сайта va-up.ru
# ═══════════════════════════════════════════════════════════════

def _cors_headers(response):
    response.headers["Access-Control-Allow-Origin"] = "https://va-up.ru"
    response.headers["Access-Control-Allow-Methods"] = "GET"
    response.headers["Cache-Control"] = "public, max-age=60"
    return response


@app.route("/api/operation")
def api_operation():
    try:
        pilots     = db_op_all_pilots()
        total_legs = len(OPERATION_LEGS)
        result     = []
        for i, p in enumerate(pilots, 1):
            legs_done = max(0, p["current_leg"] - 1)
            if p["status"] == "finished":
                legs_done = total_legs
            result.append({
                "rank":         i,
                "pilot_name":   p["pilot_name"],
                "aircraft":     p["aircraft"] or "",
                "current_leg":  p["current_leg"],
                "total_legs":   total_legs,
                "legs_done":    legs_done,
                "total_points": p["total_points"],
                "ferry_num":    p.get("ferry_num", 1),
                "status":       p["status"],
            })
        resp = jsonify({
            "ok": True,
            "operation": {
                "name":       OPERATION_NAME,
                "start":      OPERATION_START,
                "end":        OPERATION_END,
                "active":     operation_is_active(),
                "max_points": OPERATION_MAX_POINTS,
            },
            "pilots": result,
            "total":  len(result),
        })
        return _cors_headers(resp)
    except Exception as e:
        logger.exception(f"API /api/operation error: {e}")
        return _cors_headers(jsonify({"ok": False, "error": str(e)})), 500


@app.route("/api/flights")
def api_flights():
    try:
        limit      = min(int(request.args.get("limit", 10)), 50)
        month_only = request.args.get("month", "0") == "1"
        flights    = db_flights_this_month()[:limit] if month_only else db_last_flights(limit)
        result     = []
        for f in flights:
            dep      = f["departure"] or "????"
            arr      = f["arrival"]   or "????"
            fno      = f["flight_no"] or "N/A"
            no_plan  = (
                fno in ("N/A", "", "None") or
                dep in ("????", "", "None") or
                arr in ("????", "", "None")
            )
            rating, _ = landing_rating(f["landing_rate"])
            result.append({
                "flight_no":    fno if not no_plan else None,
                "pilot":        f["pilot"],
                "departure":    dep if not no_plan else None,
                "arrival":      arr if not no_plan else None,
                "aircraft":     f["aircraft"],
                "landing_rate": f["landing_rate"],
                "rating":       rating,
                "no_plan":      no_plan,
                "report_url":   (
                    f"https://fshub.io/flight/{f['flight_id']}/report"
                    if f.get("flight_id") else None
                ),
                "created_at":   (
                    f["created_at"].isoformat() if f.get("created_at") else None
                ),
            })
        resp = jsonify({"ok": True, "flights": result, "total": len(result)})
        return _cors_headers(resp)
    except Exception as e:
        logger.exception(f"API /api/flights error: {e}")
        return _cors_headers(jsonify({"ok": False, "error": str(e)})), 500


@app.route("/api/stats")
def api_stats():
    try:
        flights_month = db_flights_this_month()
        flights_all   = db_all_flights()
        rates_month   = [f["landing_rate"] for f in flights_month]
        rates_all     = [f["landing_rate"] for f in flights_all]
        avg_month = round(sum(rates_month) / len(rates_month)) if rates_month else 0
        avg_all   = round(sum(rates_all)   / len(rates_all))   if rates_all   else 0
        pilot_counts = Counter(f["pilot"] for f in flights_month)
        top_pilots   = [{"pilot": n, "flights": c} for n, c in pilot_counts.most_common(5)]
        now = datetime.now()
        resp = jsonify({
            "ok": True,
            "month": {
                "label":       f"{MONTH_NAMES[now.month]} {now.year}",
                "flights":     len(flights_month),
                "avg_landing": avg_month,
                "top_pilots":  top_pilots,
            },
            "total": {"flights": len(flights_all), "avg_landing": avg_all},
        })
        return _cors_headers(resp)
    except Exception as e:
        logger.exception(f"API /api/stats error: {e}")
        return _cors_headers(jsonify({"ok": False, "error": str(e)})), 500


@app.route("/api/contest")
def api_contest():
    try:
        now     = datetime.now()
        month   = now.strftime("%Y-%m")
        entries = db_contest_month(month)
        slots   = CONTEST_MONTHLY_LIMIT // CONTEST_POINTS_PER_LANDING
        earned  = min(len(entries) * CONTEST_POINTS_PER_LANDING, CONTEST_MONTHLY_LIMIT)
        result  = []
        for i, e in enumerate(entries, 1):
            result.append({
                "rank":         i,
                "pilot":        e["pilot"],
                "flight_no":    e["flight_no"],
                "departure":    e["departure"],
                "arrival":      e["arrival"],
                "landing_rate": e["landing_rate"],
                "points":       CONTEST_POINTS_PER_LANDING if i <= slots else 0,
                "report_url":   e.get("report_url", ""),
            })
        resp = jsonify({
            "ok":            True,
            "month":         month,
            "month_label":   f"{MONTH_NAMES[now.month]} {now.year}",
            "slots_total":   slots,
            "slots_used":    min(len(entries), slots),
            "points_limit":  CONTEST_MONTHLY_LIMIT,
            "points_earned": earned,
            "entries":       result,
        })
        return _cors_headers(resp)
    except Exception as e:
        logger.exception(f"API /api/contest error: {e}")
        return _cors_headers(jsonify({"ok": False, "error": str(e)})), 500


# ═══════════════════════════════════════════════════════════════
# STARTUP
# Выполняется один раз при загрузке модуля.
# Gunicorn с --preload загружает ДО форка воркеров.
# ═══════════════════════════════════════════════════════════════

try:
    _create_pool()
    _init_db()
    tg_setup_webhook()
    init_scheduler()
except Exception as e:
    logger.exception(f"Startup failed: {e}")
    sys.exit(1)

logger.info(f"Сервис запущен на порту {os.environ.get('PORT', 10000)} — VA UP! готова к полётам")

# ═══════════════════════════════════════════════════════════════
# MAIN (локальный запуск)
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)), threaded=True)

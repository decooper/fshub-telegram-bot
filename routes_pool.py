"""
routes_pool.py — пул маршрутов компании + ежедневный челлендж.

Челлендж дня = 3 рейса разной длительности:
  🟢 Короткий 1–2 ч  → 40 очков
  🟡 Средний  3–5 ч  → 60 очков
  🔴 Дальний  5 ч+   → 100 очков
Можно выполнить любой или все три (40+60+100). Очки идут в месячный зачёт.

Зависит от core.py (db_execute, tg_send, discord_send, logger) — без side effects.
routes.txt лежит рядом с этим файлом. Формат строки:
    flight_no,dep,arr,dep_time,arr_time,price,category
"""

import os
import random
from datetime import datetime, timezone, timedelta
from collections import namedtuple

from flask import Blueprint, jsonify, request

from core import db_execute, tg_send, discord_send, logger, MONTH_NAMES
import airports as A

# ─── Константы ──────────────────────────────────────────────────
ROUTES_FILE         = os.path.join(os.path.dirname(__file__), "routes.txt")
ANNOUNCE_COMPLETION = True
SITE_ORIGIN         = "https://va-up.ru"

# Санитарные границы реального рейса (мин): отсекаем мусор в расписании
SANE_MIN, SANE_MAX = 40, 960   # 40 мин .. 16 ч

# Тиры челленджа: (ключ, подпись, длит_от, длит_до, очки)
CHALLENGE_TIERS = [
    ("short",  "🟢 Короткий", 60,  120,  40),
    ("medium", "🟡 Средний",  180, 299,  60),
    ("long",   "🔴 Дальний",  300, 960, 100),
]
_TIER_LABEL  = {k: lbl for k, lbl, *_ in CHALLENGE_TIERS}
_TIER_POINTS = {k: pts for k, *_, pts in CHALLENGE_TIERS}

Route = namedtuple("Route", "flight_no dep arr dep_time arr_time price category")

_routes_cache = None
_tier_pools_cache = None


# ═══════════════════════════════════════════════════════════════
# ЗАГРУЗКА МАРШРУТОВ + ДЛИТЕЛЬНОСТЬ
# ═══════════════════════════════════════════════════════════════

def _load_routes():
    global _routes_cache
    if _routes_cache is not None:
        return _routes_cache
    routes = []
    try:
        with open(ROUTES_FILE, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                p = line.split(",")
                if len(p) < 7:
                    continue
                try:
                    price = int(p[5])
                except ValueError:
                    price = 0
                routes.append(Route(p[0], p[1].upper(), p[2].upper(),
                                    p[3], p[4], price, p[6]))
    except FileNotFoundError:
        logger.warning(f"[routes] {ROUTES_FILE} не найден — пул пуст")
    _routes_cache = routes
    logger.info(f"[routes] Загружено маршрутов: {len(routes)}")
    return routes


def base_routes():
    return [r for r in _load_routes() if r.category == "base"]


def route_duration(r) -> int:
    """Длительность рейса в минутах из времён расписания (Zulu). 0 — если не разобрать."""
    try:
        d = int(r.dep_time[:2]) * 60 + int(r.dep_time[2:])
        a = int(r.arr_time[:2]) * 60 + int(r.arr_time[2:])
    except (ValueError, IndexError):
        return 0
    m = a - d
    if m <= 0:
        m += 1440   # рейс через полночь
    return m


def _fmt_dur(m: int) -> str:
    h, mm = divmod(m, 60)
    return f"{h}ч{mm:02d}м" if mm else f"{h}ч"


def _tier_pools():
    """{tier_key: [Route, ...]} — рейсы каждого тира в санитарных границах. Кэш на процесс."""
    global _tier_pools_cache
    if _tier_pools_cache is not None:
        return _tier_pools_cache
    pools = {k: [] for k, *_ in CHALLENGE_TIERS}
    for r in base_routes():
        m = route_duration(r)
        if not (SANE_MIN <= m <= SANE_MAX):
            continue
        for key, _lbl, lo, hi, _pts in CHALLENGE_TIERS:
            if lo <= m <= hi:
                pools[key].append(r)
                break
    _tier_pools_cache = pools
    return pools


# ═══════════════════════════════════════════════════════════════
# ВРЕМЯ (UTC — как лимит легов ивента)
# ═══════════════════════════════════════════════════════════════

def _now_utc():
    return datetime.now(timezone.utc)

def _today_utc():
    return _now_utc().date()

def _month_utc():
    return _now_utc().strftime("%Y-%m")

def _date_seed():
    return int(_today_utc().strftime("%Y%m%d"))


# ═══════════════════════════════════════════════════════════════
# /route — случайный маршрут
# ═══════════════════════════════════════════════════════════════

def fmt_route(hub=None):
    pool = [r for r in base_routes()
            if hub is None or r.dep == hub or r.arr == hub]
    if not pool:
        return "Маршруты не загружены."
    r = random.choice(pool)
    dur = route_duration(r)
    dur_str = f" · {_fmt_dur(dur)}" if dur else ""
    return (
        "🗺 <b>Случайный маршрут</b>\n\n"
        f"✈️ <b>{r.flight_no}</b>: {r.dep} → {r.arr}{dur_str}\n"
        f"💰 {r.price}v$\n\n"
        f'<a href="https://metar-taf.com/{r.dep}">METAR {r.dep}</a> · '
        f'<a href="https://metar-taf.com/{r.arr}">METAR {r.arr}</a>'
    )


# ═══════════════════════════════════════════════════════════════
# /challenge — 3 рейса дня (короткий/средний/длинный)
# ═══════════════════════════════════════════════════════════════

def daily_challenge():
    """
    Возвращает список пиков на сегодня:
      [{tier, label, points, duration, route}, ...]
    Детерминировано по дате (UTC) — стабильно при рестартах.
    """
    pools = _tier_pools()
    rng = random.Random(_date_seed())
    picks = []
    for key, label, _lo, _hi, pts in CHALLENGE_TIERS:
        pool = pools.get(key) or []
        if not pool:
            continue
        r = rng.choice(pool)
        picks.append({
            "tier":     key,
            "label":    label,
            "points":   pts,
            "duration": route_duration(r),
            "route":    r,
        })
    return picks


def fmt_daily_challenge():
    picks = daily_challenge()
    if not picks:
        return "Маршруты не загружены."
    today = _now_utc().strftime("%d.%m.%Y")
    lines = [
        f"🎯 <b>ЧЕЛЛЕНДЖ ДНЯ</b> · {today}",
        "━━━━━━━━━━━━━━",
        "Чем дальше рейс — тем больше очков. Можно все три 👇",
        "",
    ]
    for pk in picks:
        r = pk["route"]
        lines.append(f"{pk['label']} · <b>+{pk['points']}</b> очков")
        lines.append(f"<b>{r.flight_no}</b> · {A.place_full(r.dep)} → {A.place_full(r.arr)}")
        hl = A.highlight(r.arr)
        if hl:
            lines.append(f"⏱ ~{_fmt_dur(pk['duration'])} · 📍 <i>{hl}</i>")
        else:
            lines.append(f"⏱ ~{_fmt_dur(pk['duration'])}")
        lines.append("")
    lines.append("━━━━━━━━━━━━━━")
    lines.append("🏅 Лидеры месяца: /challenge_top")
    return "\n".join(lines)


def post_daily_challenge():
    """Плановая публикация челленджа дня (00:00 UTC) — в канал + Discord."""
    text = fmt_daily_challenge()
    tg_send(text)
    try:
        discord_send(text)
    except Exception as e:
        logger.warning(f"[Challenge] Discord post failed: {e}")


# ═══════════════════════════════════════════════════════════════
# ЗАЧЁТ + ЛИДЕРБОРД (БД)
# ═══════════════════════════════════════════════════════════════

def init_challenge_db():
    """Создаёт/мигрирует таблицу зачёта. Зачёт по (пилот, день, тир)."""
    db_execute(
        """
        CREATE TABLE IF NOT EXISTS challenge_completions (
            id         SERIAL PRIMARY KEY,
            pilot      TEXT NOT NULL,
            day        DATE NOT NULL,
            tier       TEXT NOT NULL DEFAULT 'short',
            dep        TEXT,
            arr        TEXT,
            points     INTEGER NOT NULL DEFAULT 0,
            created_at TIMESTAMP DEFAULT NOW()
        )
        """
    )
    # Миграция со старой схемы (UNIQUE(pilot,day), без tier/points)
    for stmt in (
        "ALTER TABLE challenge_completions ADD COLUMN IF NOT EXISTS tier TEXT NOT NULL DEFAULT 'short'",
        "ALTER TABLE challenge_completions ADD COLUMN IF NOT EXISTS points INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE challenge_completions DROP CONSTRAINT IF EXISTS challenge_completions_pilot_day_key",
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_challenge_pilot_day_tier "
        "ON challenge_completions (pilot, day, tier)",
    ):
        try:
            db_execute(stmt)
        except Exception as e:
            logger.warning(f"[Challenge] миграция пропущена: {e}")
    logger.info("[Challenge] Таблица challenge_completions готова")


def record_challenge_if_match(pilot_name, dep, arr):
    """
    Вызывается из handle_completed. Если маршрут совпал с одним из 3 рейсов дня —
    пилоту начисляются очки этого тира (раз в сутки на каждый тир по UTC).
    Совпадение строго по направлению DEP→ARR.
    """
    try:
        dep = (dep or "").upper()
        arr = (arr or "").upper()
        if len(dep) != 4 or len(arr) != 4:
            return
        match = None
        for pk in daily_challenge():
            r = pk["route"]
            if (dep, arr) == (r.dep, r.arr):
                match = pk
                break
        if not match:
            return
        row = db_execute(
            """
            INSERT INTO challenge_completions (pilot, day, tier, dep, arr, points)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (pilot, day, tier) DO NOTHING
            RETURNING id
            """,
            (pilot_name, _today_utc(), match["tier"], dep, arr, match["points"]),
            fetch="one",
        )
        if row:
            logger.info(
                f"[Challenge] {pilot_name} выполнил {match['tier']} челлендж: "
                f"{dep}->{arr} +{match['points']}"
            )
            if ANNOUNCE_COMPLETION:
                tg_send(
                    f"🎯 <b>{pilot_name}</b> выполнил челлендж дня "
                    f"({match['label'].split()[1].lower()}): {dep} → {arr} ✅ "
                    f"+{match['points']} очков"
                )
    except Exception as e:
        logger.exception(f"[Challenge] record_challenge_if_match error: {e}")


def challenge_leaders(month=None, limit=20):
    """Лидеры месяца: [{pilot, completed, points}], сорт по очкам."""
    month = month or _month_utc()
    rows = db_execute(
        """
        SELECT pilot,
               COUNT(*)                  AS done,
               COALESCE(SUM(points), 0)  AS pts,
               MIN(created_at)           AS first_at
        FROM challenge_completions
        WHERE to_char(day, 'YYYY-MM') = %s
        GROUP BY pilot
        ORDER BY pts DESC, first_at ASC
        LIMIT %s
        """,
        (month, limit),
        fetch="all",
    ) or []
    return [
        {"pilot": r["pilot"], "completed": int(r["done"]), "points": int(r["pts"])}
        for r in rows
    ]


def fmt_challenge_leaders(month=None):
    month   = month or _month_utc()
    leaders = challenge_leaders(month)
    if not leaders:
        return (f"🏆 <b>Челлендж — лидеры {month}</b>\n\n"
                "В этом месяце ещё никто не выполнял челлендж.")
    medals = ["🥇", "🥈", "🥉"]
    lines  = [f"🏆 <b>Челлендж — лидеры {month} (UTC)</b>\n"]
    for i, l in enumerate(leaders):
        mark = medals[i] if i < 3 else f"{i + 1}."
        lines.append(f"{mark} <b>{l['pilot']}</b> — {l['points']} очк. ({l['completed']} рей.)")
    return "\n".join(lines)


def _month_label(m: str) -> str:
    """'2026-06' → 'Июнь 2026'."""
    try:
        y, mo = m.split("-")
        return f"{MONTH_NAMES[int(mo)]} {y}"
    except Exception:
        return m


def _prev_month_utc() -> str:
    """Предыдущий календарный месяц (UTC) в формате YYYY-MM."""
    first = _now_utc().replace(day=1)
    return (first - timedelta(days=1)).strftime("%Y-%m")


def fmt_challenge_results(month=None) -> str:
    """Итоги челленджа за месяц: 3 призёра + остальные участники."""
    month   = month or _prev_month_utc()
    leaders = challenge_leaders(month, limit=100)
    label   = _month_label(month)
    if not leaders:
        return (f"🏁 <b>ИТОГИ ЧЕЛЛЕНДЖА — {label}</b>\n"
                "━━━━━━━━━━━━━━\n"
                "В этом месяце челлендж никто не выполнял.\n\n"
                "🆕 Новый цикл стартовал — /challenge")
    medals = ["🥇", "🥈", "🥉"]
    lines  = [f"🏁 <b>ИТОГИ ЧЕЛЛЕНДЖА — {label}</b>", "━━━━━━━━━━━━━━"]
    top = leaders[:3]
    for i, l in enumerate(top):
        lines.append(
            f"{medals[i]} <b>{l['pilot']}</b> — "
            f"{l['points']} очк. ({l['completed']} рей.)"
        )
    rest = leaders[3:]
    if rest:
        lines.append("")
        lines.append("✈️ <b>Также участвовали:</b>")
        for l in rest:
            lines.append(f"• {l['pilot']} — {l['points']} очк.")
    lines.append("━━━━━━━━━━━━━━")
    lines.append("🎉 Поздравляем призёров! 🆕 Новый цикл стартовал — /challenge")
    return "\n".join(lines)


def post_challenge_results():
    """Публикует итоги прошедшего месяца (вызывается 1-го числа в 00:01 UTC)."""
    text = fmt_challenge_results()
    tg_send(text)
    try:
        discord_send(text)
    except Exception as e:
        logger.warning(f"[Challenge] Discord итоги месяца failed: {e}")


# ═══════════════════════════════════════════════════════════════
# API ДЛЯ САЙТА (Flask Blueprint)
# ═══════════════════════════════════════════════════════════════

challenge_bp = Blueprint("challenge", __name__)


def _cors(resp):
    resp.headers["Access-Control-Allow-Origin"]  = SITE_ORIGIN
    resp.headers["Access-Control-Allow-Methods"] = "GET"
    resp.headers["Cache-Control"]                = "public, max-age=60"
    return resp


@challenge_bp.route("/api/challenge")
def api_challenge():
    try:
        routes = []
        for pk in daily_challenge():
            r = pk["route"]
            routes.append({
                "tier":          pk["tier"],
                "points":        pk["points"],
                "duration_min":  pk["duration"],
                "duration_str":  _fmt_dur(pk["duration"]),
                "flight_no":     r.flight_no,
                "departure":     r.dep,
                "arrival":       r.arr,
                "price":         r.price,
            })
        resp = jsonify({
            "ok":     True,
            "date":   _today_utc().isoformat(),
            "routes": routes,
        })
        return _cors(resp)
    except Exception as e:
        logger.exception(f"API /api/challenge error: {e}")
        return _cors(jsonify({"ok": False, "error": str(e)})), 500


@challenge_bp.route("/api/challenge/leaders")
def api_challenge_leaders():
    try:
        month   = request.args.get("month") or _month_utc()
        leaders = challenge_leaders(month)
        result  = [{"rank": i + 1, **l} for i, l in enumerate(leaders)]
        resp = jsonify({
            "ok":      True,
            "month":   month,
            "leaders": result,
            "total":   len(result),
        })
        return _cors(resp)
    except Exception as e:
        logger.exception(f"API /api/challenge/leaders error: {e}")
        return _cors(jsonify({"ok": False, "error": str(e)})), 500

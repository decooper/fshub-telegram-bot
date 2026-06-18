"""
routes_pool.py — пул маршрутов компании + ежедневный челлендж.

Что внутри:
  • загрузка routes.txt (чистое расписание рейсов компании);
  • /route       — случайный маршрут;
  • /challenge   — 3 рейса дня (короткий/средний/длинный), детерминировано по дате (МСК);
  • зачёт челленджа: бот ловит flight.completed → сверяет с маршрутами дня →
    начисляет 1 очко-день пилоту (раз в сутки, как у FSHub);
  • месячный лидерборд: кто больше выполнил челленджей за месяц (МСК);
  • Flask-блюпринт с API для сайта: /api/challenge и /api/challenge/leaders;
  • публикация челленджа в Telegram + Discord (для планировщика).

Зависит от core.py (db_execute, tg_send, discord_send, logger) — без side effects.
routes.txt должен лежать рядом с этим файлом (в корне репозитория).

Формат строки routes.txt:
    flight_no,dep,arr,dep_time,arr_time,price,category
"""

import os
import random
from datetime import datetime, timezone, timedelta
from collections import namedtuple

from flask import Blueprint, jsonify, request

from core import db_execute, tg_send, discord_send, logger

# ─── Константы ──────────────────────────────────────────────────
ROUTES_FILE              = os.path.join(os.path.dirname(__file__), "routes.txt")
LOCAL_TZ                 = timezone(timedelta(hours=3))   # МСК (как в боте)
CHALLENGE_POINTS_PER_DAY = 50      # очков за один выполненный челлендж-день
ANNOUNCE_COMPLETION      = True    # слать ли в канал «X выполнил челлендж дня»
SITE_ORIGIN              = "https://va-up.ru"

Route = namedtuple("Route", "flight_no dep arr dep_time arr_time price category")

_routes_cache = None  # ленивый кэш на процесс


# ═══════════════════════════════════════════════════════════════
# ЗАГРУЗКА МАРШРУТОВ
# ═══════════════════════════════════════════════════════════════

def _load_routes():
    """Парсит routes.txt один раз и кэширует. Возвращает list[Route]."""
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
        routes = []
    _routes_cache = routes
    logger.info(f"[routes] Загружено маршрутов: {len(routes)}")
    return routes


def base_routes():
    """Только базовые рейсы (без ивентовых/туровых)."""
    return [r for r in _load_routes() if r.category == "base"]


# ═══════════════════════════════════════════════════════════════
# ВРЕМЯ (МСК)
# ═══════════════════════════════════════════════════════════════

def _now_msk():
    return datetime.now(LOCAL_TZ)

def _today_msk():
    return _now_msk().date()

def _month_msk():
    return _now_msk().strftime("%Y-%m")

def _date_seed():
    """Стабильный seed на весь календарный день по МСК (одинаков при рестартах)."""
    return int(_today_msk().strftime("%Y%m%d"))


# ═══════════════════════════════════════════════════════════════
# /route — случайный маршрут
# ═══════════════════════════════════════════════════════════════

def pick_route(hub=None, category="base"):
    pool = [r for r in _load_routes()
            if (category is None or r.category == category)
            and (hub is None or r.dep == hub or r.arr == hub)]
    return random.choice(pool) if pool else None


def fmt_route(hub=None):
    r = pick_route(hub)
    if not r:
        return "Маршруты не загружены."
    return (
        "🗺 <b>Случайный маршрут</b>\n\n"
        f"✈️ <b>{r.flight_no}</b>: {r.dep} → {r.arr}\n"
        f"🕑 {r.dep_time[:2]}:{r.dep_time[2:]} – {r.arr_time[:2]}:{r.arr_time[2:]}\n"
        f"💰 {r.price}v$\n\n"
        f'<a href="https://metar-taf.com/{r.dep}">METAR {r.dep}</a> · '
        f'<a href="https://metar-taf.com/{r.arr}">METAR {r.arr}</a>'
    )


# ═══════════════════════════════════════════════════════════════
# /challenge — 3 рейса дня
# ═══════════════════════════════════════════════════════════════

def daily_challenge():
    """3 рейса дня: короткий / средний / длинный (цена как прокси дистанции)."""
    pool = base_routes()
    if len(pool) < 3:
        return []
    rng = random.Random(_date_seed())
    by_price = sorted(pool, key=lambda r: r.price)
    n = len(by_price)
    tiers = [by_price[:n // 3],
             by_price[n // 3:2 * n // 3],
             by_price[2 * n // 3:]]
    return [rng.choice(t) for t in tiers if t]


def fmt_daily_challenge():
    picks = daily_challenge()
    if not picks:
        return "Маршруты не загружены."
    today  = _now_msk().strftime("%d.%m.%Y")
    labels = ["🟢 Короткий", "🟡 Средний", "🔴 Длинный"]
    lines  = [f"🎯 <b>Челлендж дня — {today}</b>",
              "Выполни любой из трёх (или все) и получи очко в зачёт месяца:\n"]
    for lbl, r in zip(labels, picks):
        lines.append(
            f"{lbl} — <b>{r.flight_no}</b>: {r.dep} → {r.arr}  "
            f"({r.dep_time[:2]}:{r.dep_time[2:]}, {r.price}v$)"
        )
    lines.append(f"\n🏆 +{CHALLENGE_POINTS_PER_DAY} очков за день. Таблица лидеров: /challenge_top")
    return "\n".join(lines)


def post_daily_challenge():
    """Публикация челленджа дня: Telegram + Discord (для планировщика)."""
    text = fmt_daily_challenge()
    tg_send(text)
    try:
        discord_send(text)          # канал плановых задач (DISCORD_WEBHOOK_URL)
    except Exception as e:
        logger.warning(f"[Challenge] Discord post failed: {e}")


# ═══════════════════════════════════════════════════════════════
# ЗАЧЁТ ЧЕЛЛЕНДЖА + ЛИДЕРБОРД (БД)
# ═══════════════════════════════════════════════════════════════

def init_challenge_db():
    """Создаёт таблицу зачёта челленджа. Вызывать в startup (после _init_db)."""
    db_execute(
        """
        CREATE TABLE IF NOT EXISTS challenge_completions (
            id         SERIAL PRIMARY KEY,
            pilot      TEXT NOT NULL,
            day        DATE NOT NULL,
            dep        TEXT,
            arr        TEXT,
            created_at TIMESTAMP DEFAULT NOW(),
            UNIQUE (pilot, day)
        )
        """
    )
    logger.info("[Challenge] Таблица challenge_completions готова")


def record_challenge_if_match(pilot_name, dep, arr):
    """
    Вызывается из handle_completed после записи рейса.
    Если маршрут совпал с одним из 3 рейсов дня — пилоту начисляется
    1 очко-день (раз в сутки по МСК). Совпадение — строго по направлению DEP→ARR.
    """
    try:
        dep = (dep or "").upper()
        arr = (arr or "").upper()
        if len(dep) != 4 or len(arr) != 4:
            return
        pairs = {(r.dep, r.arr) for r in daily_challenge()}
        if (dep, arr) not in pairs:
            return
        row = db_execute(
            """
            INSERT INTO challenge_completions (pilot, day, dep, arr)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (pilot, day) DO NOTHING
            RETURNING id
            """,
            (pilot_name, _today_msk(), dep, arr),
            fetch="one",
        )
        if row:  # первая засчитанная попытка за день
            logger.info(f"[Challenge] {pilot_name} выполнил челлендж дня: {dep}->{arr}")
            if ANNOUNCE_COMPLETION:
                tg_send(f"🎯 <b>{pilot_name}</b> выполнил челлендж дня! "
                        f"({dep} → {arr}) ✅  +{CHALLENGE_POINTS_PER_DAY} очков")
    except Exception as e:
        logger.exception(f"[Challenge] record_challenge_if_match error: {e}")


def challenge_leaders(month=None, limit=20):
    """Список лидеров месяца: [{pilot, completed, points}], сорт по выполненным."""
    month = month or _month_msk()
    rows = db_execute(
        """
        SELECT pilot, COUNT(*) AS done, MIN(created_at) AS first_at
        FROM challenge_completions
        WHERE to_char(day, 'YYYY-MM') = %s
        GROUP BY pilot
        ORDER BY done DESC, first_at ASC
        LIMIT %s
        """,
        (month, limit),
        fetch="all",
    ) or []
    return [
        {
            "pilot":     r["pilot"],
            "completed": int(r["done"]),
            "points":    int(r["done"]) * CHALLENGE_POINTS_PER_DAY,
        }
        for r in rows
    ]


def fmt_challenge_leaders(month=None):
    month   = month or _month_msk()
    leaders = challenge_leaders(month)
    if not leaders:
        return (f"🏆 <b>Челлендж — лидеры {month}</b>\n\n"
                "В этом месяце ещё никто не выполнял челлендж.")
    medals = ["🥇", "🥈", "🥉"]
    lines  = [f"🏆 <b>Челлендж — лидеры {month} (МСК)</b>\n"]
    for i, l in enumerate(leaders):
        mark = medals[i] if i < 3 else f"{i + 1}."
        lines.append(f"{mark} <b>{l['pilot']}</b> — {l['completed']} дн. · {l['points']} очк.")
    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════
# API ДЛЯ САЙТА (Flask Blueprint)
# Регистрируется в app.py:  app.register_blueprint(challenge_bp)
# ═══════════════════════════════════════════════════════════════

challenge_bp = Blueprint("challenge", __name__)


def _cors(resp):
    resp.headers["Access-Control-Allow-Origin"]  = SITE_ORIGIN
    resp.headers["Access-Control-Allow-Methods"] = "GET"
    resp.headers["Cache-Control"]                = "public, max-age=60"
    return resp


@challenge_bp.route("/api/challenge")
def api_challenge():
    """3 маршрута дня + текущая дата (МСК)."""
    try:
        picks  = daily_challenge()
        tiers  = ["short", "medium", "long"]
        routes = [
            {
                "tier":      t,
                "flight_no": r.flight_no,
                "departure": r.dep,
                "arrival":   r.arr,
                "dep_time":  f"{r.dep_time[:2]}:{r.dep_time[2:]}",
                "arr_time":  f"{r.arr_time[:2]}:{r.arr_time[2:]}",
                "price":     r.price,
            }
            for t, r in zip(tiers, picks)
        ]
        resp = jsonify({
            "ok":              True,
            "date":            _today_msk().isoformat(),
            "points_per_day":  CHALLENGE_POINTS_PER_DAY,
            "routes":          routes,
        })
        return _cors(resp)
    except Exception as e:
        logger.exception(f"API /api/challenge error: {e}")
        return _cors(jsonify({"ok": False, "error": str(e)})), 500


@challenge_bp.route("/api/challenge/leaders")
def api_challenge_leaders():
    """Месячный лидерборд челленджа. ?month=YYYY-MM (по умолч. текущий, МСК)."""
    try:
        month   = request.args.get("month") or _month_msk()
        leaders = challenge_leaders(month)
        result  = [{"rank": i + 1, **l} for i, l in enumerate(leaders)]
        resp = jsonify({
            "ok":              True,
            "month":           month,
            "points_per_day":  CHALLENGE_POINTS_PER_DAY,
            "leaders":         result,
            "total":           len(result),
        })
        return _cors(resp)
    except Exception as e:
        logger.exception(f"API /api/challenge/leaders error: {e}")
        return _cors(jsonify({"ok": False, "error": str(e)})), 500

"""
routes_pool.py — пул маршрутов компании + ежедневный челлендж.

Челлендж дня = 3 рейса разной ДАЛЬНОСТИ (великий круг, км):
  🟢 Короткий   300–1200 км → 40 очков
  🟡 Средний   1200–3000 км → 60 очков
  🔴 Дальний   3000–9000 км → 100 очков
Можно выполнить любой или все три (40+60+100). Очки идут в месячный зачёт.

ВАЖНО: времена в routes.txt — МЕСТНЫЕ, а не Zulu. Вычитать их друг из друга
нельзя: UHBB(UTC+9)→UUEE(UTC+3) 1050→1220 давало «1ч30м» вместо ~7ч20м, и рейс
через полстраны попадал в «короткие». Поэтому и тир, и показываемое время
считаются ТОЛЬКО от расстояния (airports_geo.py). См. route_duration() ниже.

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
import airports_geo as G

# ─── Константы ──────────────────────────────────────────────────
ROUTES_FILE         = os.path.join(os.path.dirname(__file__), "routes.txt")
ANNOUNCE_COMPLETION = True
SITE_ORIGIN         = "https://va-up.ru"

# Санитарные границы реального рейса (мин): отсекаем мусор в расписании
SANE_MIN, SANE_MAX = 40, 960   # 40 мин .. 16 ч

# Тиры челленджа: (ключ, подпись, дист_от_км, дист_до_км, очки)
# Границы подобраны по фактическому распределению пула:
#   короткий 975 маршрутов, средний 1416, дальний 597.
# Ниже 300 км — «прыжки» вроде RJOO→RJBE (26 км), выше 9000 км — нелетабельно за сессию.
CHALLENGE_TIERS = [
    ("short",  "🟢 Короткий",  300, 1200,  40),
    ("medium", "🟡 Средний",  1200, 3000,  60),
    ("long",   "🔴 Дальний",  3000, 9000, 100),
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
    """
    УСТАРЕЛО, НЕ ИСПОЛЬЗОВАТЬ ДЛЯ ТИРОВ И ПОКАЗА.
    Наивная разница времён расписания. Времена в routes.txt — МЕСТНЫЕ, поэтому
    результат врёт на величину разницы часовых поясов (UHBB→UUEE = «1ч30м»).
    Оставлено только для обратной совместимости. Реальную оценку даёт route_eta().
    """
    try:
        d = int(r.dep_time[:2]) * 60 + int(r.dep_time[2:])
        a = int(r.arr_time[:2]) * 60 + int(r.arr_time[2:])
    except (ValueError, IndexError):
        return 0
    m = a - d
    if m <= 0:
        m += 1440   # рейс через полночь
    return m


def route_distance_km(r):
    """Расстояние рейса по большому кругу, км. None — если нет координат."""
    return G.distance_km(r.dep, r.arr)


def route_eta(r) -> int:
    """Оценка времени в пути, мин (от расстояния). 0 — если координат нет."""
    km = route_distance_km(r)
    return G.eta_minutes(km) if km else 0


def _fmt_dur(m: int) -> str:
    h, mm = divmod(m, 60)
    return f"{h}ч{mm:02d}м" if mm else f"{h}ч"


def _tier_pools():
    """{tier_key: [Route, ...]} — рейсы каждого тира по РАССТОЯНИЮ. Кэш на процесс."""
    global _tier_pools_cache
    if _tier_pools_cache is not None:
        return _tier_pools_cache
    pools = {k: [] for k, *_ in CHALLENGE_TIERS}
    no_coords = 0
    for r in base_routes():
        km = route_distance_km(r)
        if km is None:
            no_coords += 1
            continue
        for key, _lbl, lo, hi, _pts in CHALLENGE_TIERS:
            if lo <= km < hi:
                pools[key].append(r)
                break
    _tier_pools_cache = pools
    logger.info(
        "[Challenge] Пулы по дальности: "
        + ", ".join(f"{k}={len(v)}" for k, v in pools.items())
        + f" (без координат пропущено: {no_coords})"
    )
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
    km  = route_distance_km(r)
    eta = route_eta(r)
    meta = f" · {G.fmt_km(km)} · ~{_fmt_dur(eta)}" if km else ""
    return (
        "🗺 <b>Случайный маршрут</b>\n\n"
        f"✈️ <b>{r.flight_no}</b>: {r.dep} → {r.arr}{meta}\n"
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
      [{tier, label, points, distance_km, duration, route}, ...]
    duration — оценка ОТ РАССТОЯНИЯ (не из расписания).
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
        km = route_distance_km(r) or 0
        picks.append({
            "tier":        key,
            "label":       label,
            "points":      pts,
            "distance_km": int(round(km)),
            "duration":    route_eta(r),
            "route":       r,
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
        lines.append(f"📏 <b>{G.fmt_km(pk['distance_km'])}</b> · ⏱ ~{_fmt_dur(pk['duration'])}")
        hl = A.highlight(r.arr)
        if hl:
            lines.append(f"📍 <i>{hl}</i>")
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
# 💎 ЗОЛОТОЙ МАРШРУТ
#
# Раз в месяц, в случайный день, объявляется один дальний маршрут.
# Действует РОВНО ЭТИ СУТКИ (UTC). Первому, кто пройдёт — 1000 очков,
# каждому следующему в тот же день — 200. Один зачёт на пилота.
#
# Пул: рейсы на 2–6 часов (1973 маршрута). Верхняя граница стоит затем, чтобы
# маршрут реально укладывался в один вечер: он действует всего сутки. Дальние
# и сверхдальние (8ч+, до 20ч) сюда не попадают — их за день не пролететь.
#
# ВАЖНО: маршрут и его дата ФИКСИРУЮТСЯ в БД в момент объявления и больше
# не пересчитываются. Даже если пул маршрутов изменится — приз останется тем,
# который увидели пилоты. Зачёты пишутся в challenge_completions с
# tier='golden', поэтому очки автоматически идут в месячный лидерборд.
# ═══════════════════════════════════════════════════════════════

# Пул: 2–6 часов (1973 маршрута, ~1060–4460 км). Границы заданы по ВРЕМЕНИ,
# а не по километрам: маршрут действует одни сутки, и он должен реально
# укладываться в вечер. Всё длиннее 6ч (и сверхдальние на 12–20ч) отсекается.
GOLDEN_MIN_MIN      = 120      # 2 часа
GOLDEN_MAX_MIN      = 360      # 6 часов
GOLDEN_POINTS_FIRST = 1000     # первому, кто пройдёт
GOLDEN_POINTS_REST  = 200      # каждому следующему в тот же день
GOLDEN_TIER         = "golden"

# День объявления — случайный, но детерминированный для месяца.
# Не 1-е (там итоги месяца) и не позже 26-го.
GOLDEN_DROP_FROM = 2
GOLDEN_DROP_TO   = 26


def golden_drop_day(month=None) -> int:
    """
    День месяца, когда падает Золотой Маршрут. Случайный, но одинаковый при
    любом числе перезапусков: сид — сам месяц (202607 → 17).
    """
    month = month or _month_utc()
    return random.Random(int(month.replace("-", ""))).randint(
        GOLDEN_DROP_FROM, GOLDEN_DROP_TO
    )


def init_golden_db():
    """Таблица Золотого Маршрута."""
    db_execute(
        """
        CREATE TABLE IF NOT EXISTS golden_route (
            month       TEXT PRIMARY KEY,
            day         DATE NOT NULL,
            flight_no   TEXT NOT NULL,
            dep         TEXT NOT NULL,
            arr         TEXT NOT NULL,
            distance_km INTEGER NOT NULL,
            eta_min     INTEGER NOT NULL,
            price       INTEGER NOT NULL DEFAULT 0,
            created_at  TIMESTAMP DEFAULT NOW()
        )
        """
    )
    logger.info("[Golden] Таблица golden_route готова")


def _golden_pool():
    """[(Route, км), ...] — маршруты, укладывающиеся в GOLDEN_MIN_MIN..GOLDEN_MAX_MIN."""
    out = []
    for r in base_routes():
        km = route_distance_km(r)
        if km is None:
            continue
        if GOLDEN_MIN_MIN <= G.eta_minutes(km) <= GOLDEN_MAX_MIN:
            out.append((r, km))
    return out


def _golden_used_pairs():
    """{(dep, arr)} всех прошлых Золотых — чтобы не повторяться."""
    rows = db_execute("SELECT dep, arr FROM golden_route", fetch="all") or []
    used = set()
    for r in rows:
        used.add((r["dep"], r["arr"]))
        used.add((r["arr"], r["dep"]))   # обратное направление тоже «уже было»
    return used


def db_golden_get(month=None):
    month = month or _month_utc()
    return db_execute(
        "SELECT * FROM golden_route WHERE month = %s", (month,), fetch="one"
    )


def golden_get_or_create(month=None):
    """
    Выбирает Золотой Маршрут месяца и СОХРАНЯЕТ его вместе с датой действия.
    Если уже выбран — возвращает как есть, ничего не меняя.
    Вызывается ТОЛЬКО при объявлении (не при чтении), иначе сюрприза не будет.
    """
    month = month or _month_utc()
    row = db_golden_get(month)
    if row:
        return row

    pool = _golden_pool()
    if not pool:
        logger.warning("[Golden] Пул пуст — маршрут не выбран")
        return None

    used  = _golden_used_pairs()
    fresh = [(r, km) for r, km in pool if (r.dep, r.arr) not in used]
    if not fresh:
        logger.info("[Golden] Все направления уже были — начинаем круг заново")
        fresh = pool

    r, km = random.choice(fresh)
    db_execute(
        """
        INSERT INTO golden_route
            (month, day, flight_no, dep, arr, distance_km, eta_min, price)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (month) DO NOTHING
        """,
        (month, _today_utc(), r.flight_no, r.dep, r.arr,
         int(round(km)), G.eta_minutes(km), r.price),
    )
    # Перечитываем: если два воркера объявили одновременно — победит один.
    row = db_golden_get(month)
    if row:
        logger.info(
            f"[Golden] {month}: {row['flight_no']} {row['dep']}→{row['arr']} "
            f"({row['distance_km']} км), действует {row['day']}"
        )
    return row


def golden_claims(row):
    """Кто прошёл Золотой в его день — в порядке выполнения."""
    if not row:
        return []
    return db_execute(
        """
        SELECT pilot, points, created_at
        FROM challenge_completions
        WHERE tier = %s AND day = %s
        ORDER BY created_at ASC
        """,
        (GOLDEN_TIER, row["day"]), fetch="all",
    ) or []


def fmt_golden_route(month=None) -> str:
    """
    ТОЛЬКО ЧТЕНИЕ — маршрут здесь не создаётся, иначе /golden выдал бы
    сюрприз раньше времени.
    """
    month = month or _month_utc()
    row   = db_golden_get(month)
    label = _month_label(month)

    if not row:
        if month != _month_utc():
            return f"💎 В {label} Золотой Маршрут не объявлялся."
        return (
            f"💎 <b>ЗОЛОТОЙ МАРШРУТ</b> · {label}\n"
            "━━━━━━━━━━━━━━\n"
            "В этом месяце он ещё не объявлен.\n\n"
            "Маршрут появится <b>внезапно, в один из дней месяца</b>, "
            "и будет действовать <b>только эти сутки</b>.\n\n"
            f"🥇 Первому, кто пройдёт — <b>+{GOLDEN_POINTS_FIRST}</b> очков\n"
            f"✈️ Каждому следующему в тот же день — <b>+{GOLDEN_POINTS_REST}</b>\n\n"
            "Следи за каналом 👀\n"
            "━━━━━━━━━━━━━━"
        )

    today  = _today_utc()
    active = row["day"] == today
    claims = golden_claims(row)

    lines = [
        f"💎 <b>ЗОЛОТОЙ МАРШРУТ</b> · {label}",
        "━━━━━━━━━━━━━━",
        f"<b>{row['flight_no']}</b> · {A.place_full(row['dep'])} → {A.place_full(row['arr'])}",
        f"📏 <b>{G.fmt_km(row['distance_km'])}</b> · ⏱ ~{_fmt_dur(row['eta_min'])}",
    ]
    hl = A.highlight(row["arr"])
    if hl:
        lines.append(f"📍 <i>{hl}</i>")
    lines.append("")

    if active:
        lines.append("⚡️ <b>ДЕЙСТВУЕТ ТОЛЬКО СЕГОДНЯ</b>")
        lines.append("")
        if not claims:
            lines.append(f"🥇 Первому, кто пройдёт — <b>+{GOLDEN_POINTS_FIRST}</b> очков")
            lines.append(f"✈️ Каждому следующему — <b>+{GOLDEN_POINTS_REST}</b> очков")
            lines.append("")
            lines.append("Его ещё никто не взял. Успеешь? 👀")
        else:
            lines.append(f"🥇 <b>{claims[0]['pilot']}</b> — взял первым, +{claims[0]['points']}")
            for c in claims[1:]:
                lines.append(f"✈️ {c['pilot']} — +{c['points']}")
            lines.append("")
            lines.append(
                f"Ещё не поздно: <b>+{GOLDEN_POINTS_REST}</b> очков всем, "
                f"кто пройдёт до конца суток."
            )
    else:
        lines.append(f"🔒 <b>Закрыт</b> · действовал {row['day'].strftime('%d.%m.%Y')}")
        lines.append("")
        if not claims:
            lines.append("Никто так и не взял его 😔")
        else:
            lines.append(f"🥇 <b>{claims[0]['pilot']}</b> — взял первым, +{claims[0]['points']}")
            for c in claims[1:]:
                lines.append(f"✈️ {c['pilot']} — +{c['points']}")
        lines.append("")
        lines.append("Следующий — в случайный день следующего месяца.")
    lines.append("━━━━━━━━━━━━━━")
    return "\n".join(lines)


def post_golden_route():
    """Объявление Золотого Маршрута: выбирает (если ещё нет) и публикует."""
    row = golden_get_or_create()
    if not row:
        logger.warning("[Golden] Маршрут не выбран — публикация отменена")
        return
    text = fmt_golden_route()
    tg_send(text)
    try:
        discord_send(text)
    except Exception as e:
        logger.warning(f"[Golden] Discord post failed: {e}")


def golden_daily_check():
    """
    Ежедневно в 00:05 UTC: не сегодня ли падает Золотой Маршрут?
    Условие `>=`, а не `==`: если бот в тот день лежал — объявим при первом
    же запуске после, а не потеряем месяц. Дважды не объявит: маршрут в БД.
    """
    try:
        month = _month_utc()
        if db_golden_get(month):
            return                       # уже объявлен в этом месяце
        drop  = golden_drop_day(month)
        today = _now_utc().day
        if today < drop:
            return                       # ещё рано
        logger.info(f"[Golden] День {drop} (сегодня {today}) — объявляем")
        post_golden_route()
    except Exception as e:
        logger.exception(f"[Golden] golden_daily_check error: {e}")


def record_golden_if_match(pilot_name, dep, arr):
    """
    Вызывается из handle_completed. Засчитывается ТОЛЬКО в день действия
    маршрута. Первому — GOLDEN_POINTS_FIRST, остальным — GOLDEN_POINTS_REST.
    Один зачёт на пилота.
    """
    try:
        dep = (dep or "").upper()
        arr = (arr or "").upper()
        if len(dep) != 4 or len(arr) != 4:
            return

        row = db_golden_get()
        if not row:
            return
        if (dep, arr) != (row["dep"], row["arr"]):
            return

        today = _today_utc()
        if row["day"] != today:
            logger.info(f"[Golden] {pilot_name}: маршрут совпал, но день закрыт ({row['day']})")
            return

        stat = db_execute(
            """
            SELECT COUNT(*)                           AS total,
                   COUNT(*) FILTER (WHERE pilot = %s) AS mine
            FROM challenge_completions
            WHERE tier = %s AND day = %s
            """,
            (pilot_name, GOLDEN_TIER, today), fetch="one",
        )
        if stat and int(stat["mine"]) > 0:
            return   # этот пилот уже получил Золотой

        is_first = (not stat) or int(stat["total"]) == 0
        points   = GOLDEN_POINTS_FIRST if is_first else GOLDEN_POINTS_REST

        db_execute(
            """
            INSERT INTO challenge_completions (pilot, day, tier, dep, arr, points)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (pilot, day, tier) DO NOTHING
            """,
            (pilot_name, today, GOLDEN_TIER, dep, arr, points),
        )
        logger.info(
            f"[Golden] {pilot_name} прошёл {dep}->{arr} +{points} (первый={is_first})"
        )

        if not ANNOUNCE_COMPLETION:
            return
        if is_first:
            tg_send(
                "💎🥇 <b>ЗОЛОТОЙ МАРШРУТ ВЗЯТ!</b>\n\n"
                f"<b>{pilot_name}</b> первым прошёл\n"
                f"{A.place_full(dep)} → {A.place_full(arr)}\n"
                f"📏 {G.fmt_km(row['distance_km'])}\n\n"
                f"🏆 <b>+{points} очков</b>\n\n"
                f"Маршрут открыт до конца суток — всем остальным "
                f"+{GOLDEN_POINTS_REST} очков."
            )
        else:
            tg_send(
                f"💎 <b>{pilot_name}</b> тоже прошёл Золотой Маршрут: "
                f"{dep} → {arr} ✅ +{points} очков"
            )
    except Exception as e:
        logger.exception(f"[Golden] record_golden_if_match error: {e}")


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
                "distance_km":   pk["distance_km"],
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


@challenge_bp.route("/api/golden")
def api_golden():
    try:
        month = request.args.get("month") or _month_utc()
        row   = db_golden_get(month)          # только чтение: не создаём заранее
        if not row:
            return _cors(jsonify({
                "ok": True, "month": month, "announced": False,
                "route": None, "claims": [],
            }))
        claims = [
            {"rank": i + 1, "pilot": c["pilot"], "points": int(c["points"])}
            for i, c in enumerate(golden_claims(row))
        ]
        resp = jsonify({
            "ok":        True,
            "month":     month,
            "announced": True,
            "day":       row["day"].isoformat(),
            "active":    row["day"] == _today_utc(),
            "route": {
                "flight_no":    row["flight_no"],
                "departure":    row["dep"],
                "arrival":      row["arr"],
                "distance_km":  int(row["distance_km"]),
                "duration_min": int(row["eta_min"]),
                "duration_str": _fmt_dur(int(row["eta_min"])),
                "price":        int(row["price"]),
            },
            "points_first": GOLDEN_POINTS_FIRST,
            "points_rest":  GOLDEN_POINTS_REST,
            "claims":       claims,
        })
        return _cors(resp)
    except Exception as e:
        logger.exception(f"API /api/golden error: {e}")
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

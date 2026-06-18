"""
routes_pool.py — пул маршрутов компании для /route и ежедневного челленджа.

Кладётся рядом с core.py/app.py. routes.txt должен лежать в той же папке
(или укажи путь в ROUTES_FILE). Формат строки routes.txt:
    flight_no,dep,arr,dep_time,arr_time,price,category

Использование в app.py:
    from routes_pool import fmt_route, fmt_daily_challenge
    COMMANDS["/route"]     = fmt_route
    COMMANDS["/challenge"] = fmt_daily_challenge
И кнопку в меню при желании.
"""

import os
import random
from datetime import datetime, timezone, timedelta
from collections import namedtuple

ROUTES_FILE = os.path.join(os.path.dirname(__file__), "routes.txt")
LOCAL_TZ    = timezone(timedelta(hours=3))   # МСК, как в боте

Route = namedtuple("Route", "flight_no dep arr dep_time arr_time price category")

_routes_cache = None  # ленивая загрузка, кэш на процесс


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
                routes.append(Route(p[0], p[1], p[2], p[3], p[4], price, p[6]))
    except FileNotFoundError:
        routes = []
    _routes_cache = routes
    return routes


def base_routes():
    """Только базовые рейсы (без ивентовых/туровых)."""
    return [r for r in _load_routes() if r.category == "base"]


# --- /route: случайный маршрут (опц. фильтр по хабу/категории) ---

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


# --- /challenge: 3 маршрута на день, детерминированно по дате (МСК) ---

def _date_seed():
    """Один и тот же seed весь календарный день по МСК → стабильно при рестартах."""
    today = datetime.now(LOCAL_TZ).strftime("%Y-%m-%d")
    return int(today.replace("-", ""))


def daily_challenge():
    """3 рейса дня: короткий / средний / длинный (по цене как прокси дистанции)."""
    pool = base_routes()
    if len(pool) < 3:
        return []
    rng = random.Random(_date_seed())
    by_price = sorted(pool, key=lambda r: r.price)
    n = len(by_price)
    tiers = [by_price[:n // 3],            # короткие
             by_price[n // 3:2 * n // 3],  # средние
             by_price[2 * n // 3:]]        # длинные
    return [rng.choice(t) for t in tiers if t]


def fmt_daily_challenge():
    picks = daily_challenge()
    if not picks:
        return "Маршруты не загружены."
    today = datetime.now(LOCAL_TZ).strftime("%d.%m.%Y")
    labels = ["🟢 Короткий", "🟡 Средний", "🔴 Длинный"]
    lines = [f"🎯 <b>Челлендж дня — {today}</b>",
             "Выполни любой из трёх (или все):\n"]
    for lbl, r in zip(labels, picks):
        lines.append(
            f"{lbl} — <b>{r.flight_no}</b>: {r.dep} → {r.arr}  "
            f"({r.dep_time[:2]}:{r.dep_time[2:]}, {r.price}v$)"
        )
    return "\n".join(lines)

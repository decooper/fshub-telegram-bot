"""
airports.py — справочник аэропортов для челленджа.

Даёт по ICAO: страну (ISO2) + флаг-эмодзи, город, пометку «международный»
и опциональную заметку-«особенность».

Источники (по приоритету):
  1. airports.csv рядом с файлом — формат OurAirports (колонки ident, municipality,
     iso_country). Если положить полный файл OurAirports — города будут у ВСЕХ.
  2. Встроенный справочник: страна по ICAO-префиксу (стандарт ИКАО, надёжно),
     город — для крупных аэропортов, иначе показывается ICAO-код.

Расширять CITY и NOTES можно прямо здесь — это обычные словари.
"""

import os
import csv

# ── Страна по ICAO-префиксу (2 буквы; ниже — уточнения по 3 буквам) ──
_PREFIX2 = {
    # Россия
    "UU": "RU", "UW": "RU", "UE": "RU", "UH": "RU", "UI": "RU", "UL": "RU",
    "UN": "RU", "UO": "RU", "UR": "RU", "US": "RU",
    # СНГ / бывший СССР
    "UA": "KZ", "UC": "KG", "UD": "AM", "UB": "AZ", "UG": "GE", "UK": "UA",
    "UM": "BY", "UT": "UZ",
    # Европа
    "AY": "PG", "BG": "GL", "BI": "IS", "CY": "CA", "EB": "BE", "ED": "DE",
    "EF": "FI", "EG": "GB", "EH": "NL", "EI": "IE", "EK": "DK", "EL": "LU",
    "EP": "PL", "EY": "LT", "LD": "HR", "LE": "ES", "LF": "FR", "LG": "GR",
    "LI": "IT", "LJ": "SI", "LK": "CZ", "LL": "IL", "LM": "MT", "LP": "PT",
    "LQ": "BA", "LR": "RO", "LS": "CH", "LT": "TR", "LX": "GI",
    # Африка / Ближний Восток
    "FA": "ZA", "FM": "MG", "GM": "MA", "HE": "EG", "OI": "IR", "OM": "AE",
    "OR": "IQ",
    # Америка
    "KA": "US", "KB": "US", "KD": "US", "KF": "US", "KG": "US", "KJ": "US",
    "KL": "US", "KM": "US", "KO": "US", "KS": "US", "PA": "US", "TJ": "PR",
    "MH": "HN", "MM": "MX", "MR": "CR", "MU": "CU", "MY": "BS",
    "SA": "AR", "SB": "BR", "SC": "CL", "SE": "EC", "SG": "PY", "SK": "CO",
    "SL": "BO", "SM": "SR", "SO": "GF", "SP": "PE", "SU": "UY", "SV": "VE",
    "SY": "GY",
    # Азия / Океания
    "RJ": "JP", "RO": "JP", "RK": "KR", "RP": "PH", "VH": "HK", "VI": "IN",
    "VO": "IN", "VR": "MV", "VT": "TH", "VV": "VN", "WA": "ID", "WI": "ID",
    "WM": "MY", "WS": "SG",
    "NF": "FJ", "NS": "WS", "NT": "PF", "NW": "NC",
    "YB": "AU", "YM": "AU", "YP": "AU", "YS": "AU",
    "ZB": "CN", "ZG": "CN", "ZJ": "CN", "ZS": "CN", "ZW": "CN", "ZY": "CN",
    "ZK": "KP",
}
# Уточнения по 3-буквенному префиксу (перекрывают 2-буквенные)
_PREFIX3 = {
    "UMK": "RU",   # Калининград (UMKK/UMKD) — Россия, хотя UM=Беларусь
    "UTA": "TM",   # Туркменистан
    "UTD": "TJ",   # Таджикистан
}

# ── Города (крупные/ключевые). Остальные → показывается ICAO. ──
CITY = {
    "UUEE": "Москва", "UUDD": "Москва", "UUWW": "Москва", "UUBW": "Москва",
    "ULLI": "Санкт-Петербург", "ULMM": "Мурманск", "ULAA": "Архангельск",
    "USSS": "Екатеринбург", "UNNT": "Новосибирск", "UWWW": "Самара",
    "URSS": "Сочи", "URML": "Махачкала", "URMM": "Минеральные Воды",
    "URRR": "Ростов-на-Дону", "UWGG": "Нижний Новгород", "UWKD": "Казань",
    "USCC": "Челябинск", "UNKL": "Красноярск", "UIII": "Иркутск",
    "UHWW": "Владивосток", "UHHH": "Хабаровск", "UHPP": "Петропавловск-Камч.",
    "UNBB": "Барнаул", "UUYY": "Сыктывкар", "UWUU": "Уфа", "USTR": "Тюмень",
    "UMKK": "Калининград", "UMMS": "Минск", "UKBB": "Киев", "UDYZ": "Ереван",
    "UBBB": "Баку", "UGTB": "Тбилиси", "UAAA": "Алматы", "UACC": "Астана",
    "UTSS": "Самарканд", "UTTT": "Ташкент", "UTAA": "Ашхабад",
    # Международные направления
    "HESH": "Шарм-эль-Шейх", "HECA": "Каир", "HEGN": "Хургада",
    "LTAI": "Анталья", "LTBA": "Стамбул", "LTFM": "Стамбул",
    "OMDB": "Дубай", "LEPA": "Пальма", "LIRF": "Рим", "LFPG": "Париж",
    "EDDF": "Франкфурт", "EGLL": "Лондон", "EFHK": "Хельсинки",
    "VTBS": "Бангкок", "VVPQ": "Фукуок", "RPVP": "Пуэрто-Принсеса",
    "WAMM": "Манадо", "WAJJ": "Джаяпура", "NWWW": "Нумеа", "NFFN": "Нади",
    "NSFA": "Апиа", "NTAA": "Папеэте", "NTGJ": "Тотегеги", "SCIP": "о. Пасхи",
    "SCFA": "Антофагаста", "SGAS": "Асунсьон", "SBGL": "Рио-де-Жанейро",
}

# ── Заметки-«особенности» по ключевым аэропортам ──
NOTES = {
    "HESH": "🏖 курорт у Красного моря, жара, осторожно со сдвигом ветра",
    "URSS": "🏖 горы у моря, частый сдвиг ветра на глиссаде",
    "UHPP": "🌋 вулканы и переменчивая погода Камчатки",
    "SCIP": "🗿 остров Пасхи посреди океана — запасных рядом нет",
    "LTAI": "🏖 курортная Анталья, летом интенсивный трафик",
    "OMDB": "🌆 один из крупнейших хабов мира, плотный трафик",
    "NTAA": "🏝 Таити, короткая полоса у лагуны",
    "VTBS": "🛬 Бангкок, тропические грозы во второй половине дня",
    "ULMM": "❄️ Заполярье, зимой метели и низкая видимость",
    "UIII": "🏔 рядом Байкал, горный рельеф на подходе",
}

_RU = "RU"
_loaded = False
_csv_city = {}
_csv_iso = {}


def _load_csv():
    global _loaded
    _loaded = True
    path = os.path.join(os.path.dirname(__file__), "airports.csv")
    if not os.path.exists(path):
        return
    try:
        with open(path, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                ident = (row.get("ident") or "").strip().upper()
                if len(ident) != 4:
                    continue
                muni = (row.get("municipality") or "").strip()
                iso  = (row.get("iso_country") or "").strip().upper()
                if muni:
                    _csv_city[ident] = muni
                if iso:
                    _csv_iso[ident] = iso
    except Exception:
        pass


def country_iso(icao: str) -> str:
    if not _loaded:
        _load_csv()
    icao = (icao or "").upper()
    if icao in _csv_iso:
        return _csv_iso[icao]
    if icao[:3] in _PREFIX3:
        return _PREFIX3[icao[:3]]
    return _PREFIX2.get(icao[:2], "")


def flag(iso: str) -> str:
    """ISO2 → эмодзи-флаг через regional indicator symbols."""
    if not iso or len(iso) != 2 or not iso.isalpha():
        return ""
    return chr(0x1F1E6 + ord(iso[0].upper()) - 65) + chr(0x1F1E6 + ord(iso[1].upper()) - 65)


def city(icao: str):
    if not _loaded:
        _load_csv()
    icao = (icao or "").upper()
    return _csv_city.get(icao) or CITY.get(icao)


def place(icao: str) -> str:
    """'Москва 🇷🇺' если город известен, иначе 'UCFM 🇰🇬'."""
    icao = (icao or "").upper()
    fl = flag(country_iso(icao))
    name = city(icao) or icao
    return f"{name} {fl}".strip()


def is_international(dep: str, arr: str) -> bool:
    return country_iso(dep) != _RU or country_iso(arr) != _RU


def note(icao: str):
    return NOTES.get((icao or "").upper())

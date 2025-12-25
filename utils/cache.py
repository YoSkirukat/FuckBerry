# -*- coding: utf-8 -*-
"""Функции кэширования"""
import os
import json
from typing import Dict, Any
from flask import session
from flask_login import current_user
from utils.constants import CACHE_DIR, MOSCOW_TZ
from utils.helpers import _get_session_id
from datetime import datetime, timedelta


def _cache_path_for_user() -> str:
    """Путь к кэшу заказов для текущего пользователя"""
    if current_user.is_authenticated:
        return os.path.join(CACHE_DIR, f"orders_user_{current_user.id}.json")
    return os.path.join(CACHE_DIR, f"orders_{_get_session_id()}.json")


def _cache_path_for_user_id(user_id: int) -> str:
    """Путь к кэшу заказов для конкретного пользователя"""
    return os.path.join(CACHE_DIR, f"orders_user_{user_id}.json")


# Products cache helpers (per user)
def _products_cache_path_for_user() -> str:
    """Путь к кэшу товаров для текущего пользователя"""
    if current_user.is_authenticated:
        return os.path.join(CACHE_DIR, f"products_user_{current_user.id}.json")
    return os.path.join(CACHE_DIR, "products_anon.json")


def load_products_cache() -> Dict[str, Any] | None:
    """Загружает кэш товаров"""
    path = _products_cache_path_for_user()
    if not os.path.isfile(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def save_products_cache(payload: Dict[str, Any]) -> None:
    """Сохраняет кэш товаров"""
    path = _products_cache_path_for_user()
    try:
        enriched = dict(payload)
        if current_user.is_authenticated:
            enriched["_user_id"] = current_user.id
        with open(path, "w", encoding="utf-8") as f:
            json.dump(enriched, f, ensure_ascii=False)
    except Exception:
        pass


def _articles_cache_path_for_user() -> str:
    """Возвращает путь к файлу кэша артикулов для текущего пользователя"""
    if current_user.is_authenticated:
        return f"articles_cache_user_{current_user.id}.json"
    return "articles_cache.json"


def load_articles_cache() -> Dict[str, Any] | None:
    """Загружает кэш артикулов для текущего пользователя"""
    path = _articles_cache_path_for_user()
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return None
    except Exception:
        return None


def save_articles_cache(payload: Dict[str, Any]) -> None:
    """Сохраняет кэш артикулов для текущего пользователя"""
    path = _articles_cache_path_for_user()
    try:
        enriched = dict(payload)
        if current_user.is_authenticated:
            enriched["_user_id"] = current_user.id
        with open(path, "w", encoding="utf-8") as f:
            json.dump(enriched, f, ensure_ascii=False)
    except Exception:
        pass


# Stocks cache helpers (per user)
def _stocks_cache_path_for_user() -> str:
    """Путь к кэшу остатков для текущего пользователя"""
    if current_user.is_authenticated:
        return os.path.join(CACHE_DIR, f"stocks_user_{current_user.id}.json")
    return os.path.join(CACHE_DIR, "stocks_anon.json")


def load_stocks_cache() -> Dict[str, Any] | None:
    """Загружает кэш остатков"""
    path = _stocks_cache_path_for_user()
    if not os.path.isfile(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def save_stocks_cache(payload: Dict[str, Any]) -> None:
    """Сохраняет кэш остатков"""
    path = _stocks_cache_path_for_user()
    try:
        enriched = dict(payload)
        if current_user.is_authenticated:
            enriched["_user_id"] = current_user.id
        with open(path, "w", encoding="utf-8") as f:
            json.dump(enriched, f, ensure_ascii=False)
    except Exception:
        pass


def load_stocks_cache_for_user(user_id: int) -> Dict[str, Any] | None:
    """Загружает кэш остатков для конкретного пользователя"""
    path = os.path.join(CACHE_DIR, f"stocks_user_{user_id}.json")
    try:
        if os.path.isfile(path):
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        pass
    return None


def save_stocks_cache_for_user(user_id: int, payload: Dict[str, Any]) -> None:
    """Сохраняет кэш остатков для конкретного пользователя"""
    path = os.path.join(CACHE_DIR, f"stocks_user_{user_id}.json")
    try:
        enriched = dict(payload)
        enriched["_user_id"] = user_id
        with open(path, "w", encoding="utf-8") as f:
            json.dump(enriched, f, ensure_ascii=False)
    except Exception:
        pass


def clear_stocks_cache_for_user(user_id: int) -> None:
    """Очищает кэш остатков для конкретного пользователя"""
    path = os.path.join(CACHE_DIR, f"stocks_user_{user_id}.json")
    try:
        if os.path.exists(path):
            os.remove(path)
            print(f"Cleared stocks cache for user {user_id}")
    except Exception as e:
        print(f"Error clearing stocks cache for user {user_id}: {e}")


# FBS supplies cache helpers (per user)
def _fbs_supplies_cache_path_for_user() -> str:
    """Путь к кэшу поставок FBS для текущего пользователя"""
    if current_user.is_authenticated:
        return os.path.join(CACHE_DIR, f"fbs_supplies_user_{current_user.id}.json")
    return os.path.join(CACHE_DIR, "fbs_supplies_anon.json")


def load_fbs_supplies_cache() -> Dict[str, Any] | None:
    """Загружает кэш поставок FBS"""
    path = _fbs_supplies_cache_path_for_user()
    if not os.path.isfile(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def save_fbs_supplies_cache(payload: Dict[str, Any]) -> None:
    """Сохраняет кэш поставок FBS"""
    path = _fbs_supplies_cache_path_for_user()
    try:
        enriched = dict(payload)
        if current_user.is_authenticated:
            enriched["_user_id"] = current_user.id
        with open(path, "w", encoding="utf-8") as f:
            json.dump(enriched, f, ensure_ascii=False)
    except Exception:
        pass


# FBW supplies cache helpers (per user)
def _fbw_supplies_cache_path_for_user() -> str:
    """Путь к кэшу поставок FBW для текущего пользователя"""
    if current_user.is_authenticated:
        return os.path.join(CACHE_DIR, f"fbw_supplies_user_{current_user.id}.json")
    return os.path.join(CACHE_DIR, "fbw_supplies_anon.json")


def load_fbw_supplies_cache() -> Dict[str, Any] | None:
    """Загружает кэш поставок FBW"""
    path = _fbw_supplies_cache_path_for_user()
    if not os.path.isfile(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def save_fbw_supplies_cache(payload: Dict[str, Any]) -> None:
    """Сохраняет кэш поставок FBW"""
    path = _fbw_supplies_cache_path_for_user()
    try:
        enriched = dict(payload)
        if current_user.is_authenticated:
            enriched["_user_id"] = current_user.id
        with open(path, "w", encoding="utf-8") as f:
            json.dump(enriched, f, ensure_ascii=False)
    except Exception:
        pass


# Расширенный кэш поставок с товарами (для быстрой аналитики)
def _fbw_supplies_detailed_cache_path_for_user(user_id: int | None = None) -> str:
    """Путь к детальному кэшу поставок FBW"""
    if user_id is not None:
        return os.path.join(CACHE_DIR, f"fbw_supplies_detailed_user_{user_id}.json")
    # fallback без current_user в фоновых задачах
    try:
        if current_user and getattr(current_user, "is_authenticated", False):
            return os.path.join(CACHE_DIR, f"fbw_supplies_detailed_user_{current_user.id}.json")
    except Exception:
        pass
    return os.path.join(CACHE_DIR, "fbw_supplies_detailed_anon.json")


def load_fbw_supplies_detailed_cache(user_id: int | None = None) -> Dict[str, Any] | None:
    """Безопасно загружает кэш поставок. Не зависит от current_user в фоновом потоке."""
    path = _fbw_supplies_detailed_cache_path_for_user(user_id)
    if not os.path.isfile(path):
        return None
    
    # Проверяем размер файла - если больше 50MB, не загружаем
    try:
        file_size = os.path.getsize(path)
        if file_size > 50 * 1024 * 1024:  # 50MB
            print(f"Файл кэша поставок слишком большой ({file_size / 1024 / 1024:.1f}MB), пропускаем загрузку")
            return None
    except Exception:
        pass
    
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"Ошибка загрузки кэша поставок: {e}")
        return None


def save_fbw_supplies_detailed_cache(payload: Dict[str, Any], user_id: int = None) -> None:
    """Сохраняет детальный кэш поставок FBW"""
    path = _fbw_supplies_detailed_cache_path_for_user(user_id)
    try:
        enriched = dict(payload)
        if user_id:
            enriched["_user_id"] = user_id
        elif current_user.is_authenticated:
            enriched["_user_id"] = current_user.id
        with open(path, "w", encoding="utf-8") as f:
            json.dump(enriched, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def is_supplies_cache_fresh() -> bool:
    """Проверяет, свежий ли кэш поставок (обновлялся ли за последние 24 часа)"""
    cached = load_fbw_supplies_detailed_cache()
    if not cached:
        return False
    
    last_update = cached.get("last_updated")
    if not last_update:
        return False
    
    try:
        last_update_dt = datetime.fromisoformat(last_update)
        now = datetime.now(MOSCOW_TZ)
        return (now - last_update_dt).total_seconds() < 24 * 3600  # 24 часа
    except Exception:
        return False


# Orders cache helpers
def _orders_cache_meta_path_for_user(user_id: int = None) -> str:
    """Путь к метаданным кэша заказов"""
    if user_id:
        return os.path.join(CACHE_DIR, f"orders_warm_meta_user_{user_id}.json")
    elif current_user.is_authenticated:
        return os.path.join(CACHE_DIR, f"orders_warm_meta_user_{current_user.id}.json")
    return os.path.join(CACHE_DIR, "orders_warm_meta_anon.json")


def load_orders_cache_meta() -> Dict[str, Any] | None:
    """Загружает метаданные кэша заказов"""
    path = _orders_cache_meta_path_for_user()
    if not os.path.isfile(path):
        return None
    
    # Проверяем размер файла - если больше 10MB, не загружаем
    try:
        file_size = os.path.getsize(path)
        if file_size > 10 * 1024 * 1024:  # 10MB
            print(f"Файл метаданных кэша заказов слишком большой ({file_size / 1024 / 1024:.1f}MB), пропускаем загрузку")
            return None
    except Exception:
        pass
    
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"Ошибка загрузки метаданных кэша заказов: {e}")
        return None


def save_orders_cache_meta(payload: Dict[str, Any], user_id: int = None) -> None:
    """Сохраняет метаданные кэша заказов"""
    path = _orders_cache_meta_path_for_user(user_id)
    try:
        enriched = dict(payload)
        if user_id:
            enriched["_user_id"] = user_id
        elif current_user.is_authenticated:
            enriched["_user_id"] = current_user.id
        with open(path, "w", encoding="utf-8") as f:
            json.dump(enriched, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def is_orders_cache_fresh() -> bool:
    """Проверяет, свежий ли кэш заказов (обновлялся ли за последние 24 часа)"""
    meta = load_orders_cache_meta()
    if not meta:
        return False
    last_updated = meta.get("last_updated")
    if not last_updated:
        return False
    try:
        last_dt = datetime.fromisoformat(last_updated)
        now = datetime.now(MOSCOW_TZ)
        return (now - last_dt).total_seconds() < 24 * 3600
    except Exception:
        return False


def _orders_period_cache_path_for_user(user_id: int = None) -> str:
    """Путь к периодическому кэшу заказов"""
    if user_id:
        return os.path.join(CACHE_DIR, f"orders_period_user_{user_id}.json")
    elif current_user.is_authenticated:
        return os.path.join(CACHE_DIR, f"orders_period_user_{current_user.id}.json")
    return os.path.join(CACHE_DIR, f"orders_period_{_get_session_id()}.json")


def load_orders_period_cache(user_id: int = None) -> Dict[str, Any] | None:
    """Загружает периодический кэш заказов"""
    path = _orders_period_cache_path_for_user(user_id)
    print(f"Загружаем кэш из файла: {path}")
    if not os.path.isfile(path):
        print("Файл кэша не найден")
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            days_count = len(data.get('days', {}))
            print(f"Кэш загружен, дней в кэше: {days_count}")
            return data
    except Exception as e:
        print(f"Ошибка загрузки кэша: {e}")
        return None


def save_orders_period_cache(payload: Dict[str, Any], user_id: int = None) -> None:
    """Сохраняет периодический кэш заказов"""
    path = _orders_period_cache_path_for_user(user_id)
    try:
        enriched = dict(payload)
        if user_id:
            enriched["_user_id"] = user_id
        elif current_user.is_authenticated:
            enriched["_user_id"] = current_user.id
        print(f"Сохраняем кэш в файл: {path}")
        print(f"Количество дней для сохранения: {len(enriched.get('days', {}))}")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(enriched, f, ensure_ascii=False)
        print("Кэш успешно сохранен")
    except Exception as e:
        print(f"Ошибка сохранения кэша: {e}")


# FBS tasks cache helpers
def _fbs_tasks_cache_path_for_user() -> str:
    """Путь к кэшу задач FBS для текущего пользователя"""
    if current_user.is_authenticated:
        return os.path.join(CACHE_DIR, f"fbs_tasks_user_{current_user.id}.json")
    return os.path.join(CACHE_DIR, "fbs_tasks_anon.json")


def load_fbs_tasks_cache() -> Dict[str, Any] | None:
    """Загружает кэш задач FBS"""
    path = _fbs_tasks_cache_path_for_user()
    if not os.path.isfile(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def load_fbs_tasks_cache_by_user_id(user_id: int) -> Dict[str, Any] | None:
    """Load FBS tasks cache by user ID (for background threads)"""
    path = os.path.join(CACHE_DIR, f"fbs_tasks_user_{user_id}.json")
    print(f"Loading FBS tasks cache from: {path}")
    if not os.path.isfile(path):
        print(f"FBS tasks cache file not found: {path}")
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            print(f"FBS tasks cache loaded successfully, {len(data.get('rows', []))} tasks found")
            return data
    except Exception as e:
        print(f"Error loading FBS tasks cache: {e}")
        return None


def save_fbs_tasks_cache(payload: Dict[str, Any]) -> None:
    """Сохраняет кэш задач FBS"""
    path = _fbs_tasks_cache_path_for_user()
    try:
        enriched = dict(payload)
        if current_user.is_authenticated:
            enriched["_user_id"] = current_user.id
        with open(path, "w", encoding="utf-8") as f:
            json.dump(enriched, f, ensure_ascii=False)
    except Exception:
        pass


def load_last_results() -> Dict[str, Any] | None:
    """Загружает последние результаты из кэша"""
    path = _cache_path_for_user()
    if not os.path.isfile(path):
        return None
    
    # Проверяем размер файла - если больше 10MB, не загружаем
    try:
        file_size = os.path.getsize(path)
        if file_size > 10 * 1024 * 1024:  # 10MB
            print(f"Файл кэша слишком большой ({file_size / 1024 / 1024:.1f}MB), пропускаем загрузку")
            return None
    except Exception:
        pass
    
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"Ошибка загрузки кэша: {e}")
        return None


def save_last_results(payload: Dict[str, Any]) -> None:
    """
    Сохраняет результаты в кэш, объединяя с существующими данными.
    Это позволяет сохранять данные для разных страниц (заказы, финансовый отчет) без перезаписи друг друга.
    """
    path = _cache_path_for_user()
    try:
        # Загружаем существующий кэш, если он есть
        existing_cache = load_last_results() or {}
        
        # Объединяем существующий кэш с новыми данными (новые данные имеют приоритет)
        enriched = dict(existing_cache)
        enriched.update(payload)
        
        try:
            if current_user.is_authenticated:
                enriched["_user_id"] = current_user.id
                enriched["_username"] = getattr(current_user, "username", None)
        except Exception:
            # If current_user unavailable outside request context, ignore
            pass
        with open(path, "w", encoding="utf-8") as f:
            json.dump(enriched, f, ensure_ascii=False)
    except Exception:
        pass


# --- Orders cache processing functions ---
def _normalize_date_str(date_str: str) -> str:
    """Нормализует строку даты в формат YYYY-MM-DD"""
    from utils.helpers import parse_date
    try:
        dt = parse_date(date_str)
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return date_str


def _daterange_inclusive(start_date: str, end_date: str) -> list[str]:
    """Генерирует список дат от start_date до end_date включительно"""
    from utils.helpers import parse_date
    print(f"_daterange_inclusive: start_date='{start_date}', end_date='{end_date}'")
    start_dt = parse_date(start_date)
    end_dt = parse_date(end_date)
    print(f"_daterange_inclusive: start_dt={start_dt}, end_dt={end_dt}")
    days: list[str] = []
    cur = start_dt
    while cur <= end_dt:
        days.append(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)
    print(f"_daterange_inclusive: generated days={days}")
    return days


def get_orders_with_period_cache(
    token: str,
    date_from: str,
    date_to: str,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Возвращает (orders, cache_meta). Использует кэш по дням и загружает только отсутствующие дни."""
    from collections import defaultdict
    from utils.api import fetch_orders_range
    from utils.orders_processing import to_rows
    from utils.progress import set_orders_progress, clear_orders_progress
    import time
    from flask_login import current_user
    
    # Load existing cache structure
    cache = load_orders_period_cache() or {}
    days_map: Dict[str, Any] = cache.get("days") or {}

    requested_days = _daterange_inclusive(date_from, date_to)
    print(f"Запрошенные дни: {requested_days}")
    print(f"Дни в кэше: {list(days_map.keys())}")

    # Identify days to fetch: missing in cache or today (always refetch)
    today_iso = datetime.now(MOSCOW_TZ).date().strftime("%Y-%m-%d")
    print(f"Сегодня: {today_iso}")
    days_to_fetch: list[str] = []
    for day in requested_days:
        entry = days_map.get(day)
        print(f"День {day}: {'есть в кэше' if entry else 'НЕТ в кэше'}")
        if day == today_iso:
            print(f"День {day} - сегодня, загружаем принудительно")
            days_to_fetch.append(day)
            continue
        if entry is None:
            print(f"День {day} - отсутствует в кэше, загружаем")
            days_to_fetch.append(day)
        else:
            print(f"День {day} - используем из кэша")
    
    print(f"Дни для загрузки: {days_to_fetch}")

    collected_orders: list[dict[str, Any]] = []

    # Collect from cache first
    def _cached_orders(entry: Dict[str, Any]) -> list[dict[str, Any]]:
        """Извлекает список заказов из записи кэша дня"""
        if not isinstance(entry, dict):
            return []
        val = (
            entry.get("orders")
            or entry.get("orders_rows")
            or entry.get("rows")
            or entry.get("data")
        )
        return val if isinstance(val, list) else []
    for day in requested_days:
        entry = days_map.get(day)
        if entry and day not in days_to_fetch:
            collected_orders.extend(_cached_orders(entry))

    # Fetch missing days in one period request and split per day
    total_days = len(days_to_fetch)
    done_days = 0
    progress_key = f"{date_from}:{date_to}:{int(time.time())}"
    if current_user and current_user.is_authenticated:
        set_orders_progress(current_user.id, total_days, done_days, key=progress_key)
    if days_to_fetch:
        try:
            print(f"Единая загрузка заказов за период {date_from}..{date_to} для {len(days_to_fetch)} незакэшированных дней")
            raw = fetch_orders_range(token, date_from, date_to)
            all_rows = to_rows(raw, date_from, date_to)
            # Group by day
            by_day: Dict[str, list[dict[str, Any]]] = defaultdict(list)
            for r in all_rows:
                d = str(r.get("Дата") or "")[:10]
                if d:
                    by_day[d].append(r)
            # For each missing day, update cache and progress
            for day in days_to_fetch:
                fetched_orders = by_day.get(day, [])
                days_map[day] = {
                    "orders": fetched_orders,
                    "updated_at": datetime.now(MOSCOW_TZ).strftime("%d.%m.%Y %H:%M:%S"),
                }
                collected_orders.extend(fetched_orders)
                done_days += 1
                if current_user and current_user.is_authenticated:
                    set_orders_progress(current_user.id, total_days, done_days, key=progress_key)
        except Exception as e:
            print(f"Ошибка единой загрузки заказов: {e}")

    # Persist cache file if any changes were made
    if days_to_fetch:
        print(f"Сохраняем кэш для дней: {days_to_fetch}")
        cache["days"] = days_map
        save_orders_period_cache(cache)
        print(f"Кэш сохранен. Всего дней в кэше: {len(days_map)}")
    else:
        print("Нет изменений в кэше, не сохраняем")

    if current_user and current_user.is_authenticated:
        clear_orders_progress(current_user.id, key=progress_key)

    meta = {"used_cache_days": len(requested_days) - len(days_to_fetch), "fetched_days": len(days_to_fetch)}
    return collected_orders, meta


def update_period_cache_with_data(
    token: str,
    date_from: str,
    date_to: str,
    orders: list[dict[str, Any]],
    user_id: int = None,
) -> None:
    """Принудительно обновляет кэш по дням с предоставленными данными"""
    cache = load_orders_period_cache(user_id) or {}
    days_map: Dict[str, Any] = cache.get("days") or {}
    
    requested_days = _daterange_inclusive(date_from, date_to)
    
    # Группируем данные по дням
    orders_by_day: Dict[str, list[dict[str, Any]]] = {}
    
    for order in orders:
        # Rows produced by to_rows use key 'Дата'. Keep fallback for legacy 'Дата заказа'.
        order_date = (order.get("Дата") or order.get("Дата заказа") or "")
        if order_date:
            day_key = _normalize_date_str(order_date)
            if day_key not in orders_by_day:
                orders_by_day[day_key] = []
            orders_by_day[day_key].append(order)
    
    # Обновляем кэш для каждого дня
    for day in requested_days:
        days_map[day] = {
            "orders": orders_by_day.get(day, []),
            "updated_at": datetime.now(MOSCOW_TZ).strftime("%d.%m.%Y %H:%M:%S"),
        }
    
    # Сохраняем обновленный кэш
    cache["days"] = days_map
    save_orders_period_cache(cache, user_id)


def build_orders_warm_cache(token: str, user_id: int = None) -> Dict[str, Any]:
    """Подогревает кэш заказов за последние 6 месяцев"""
    from utils.api import fetch_orders_range
    from utils.orders_processing import to_rows
    
    from_date = (datetime.now(MOSCOW_TZ).date() - timedelta(days=180)).strftime("%Y-%m-%d")
    to_date = datetime.now(MOSCOW_TZ).date().strftime("%Y-%m-%d")
    # Fetch all rows in range and persist into per-day cache
    raw = fetch_orders_range(token, from_date, to_date)
    rows = to_rows(raw, from_date, to_date)
    # Используем функцию из этого же модуля
    update_period_cache_with_data(token, from_date, to_date, rows, user_id)
    meta = {
        "last_updated": datetime.now(MOSCOW_TZ).isoformat(),
        "date_from": from_date,
        "date_to": to_date,
        "total_orders_cached": len(rows),
        "cache_version": "1.0"
    }
    return meta


# --- DBS cache functions ---
def _ensure_dbs_cache_dir() -> None:
    """Создает директорию для кэша DBS"""
    try:
        os.makedirs(CACHE_DIR, exist_ok=True)
    except Exception:
        pass


def load_dbs_active_ids() -> Dict[str, Any]:
    """Загружает активные ID заказов DBS"""
    _ensure_dbs_cache_dir()
    path = os.path.join(CACHE_DIR, "dbs_active_ids.json")
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"ids": [], "updated_at": None}


def save_dbs_active_ids(data: Dict[str, Any]) -> None:
    """Сохраняет активные ID заказов DBS"""
    _ensure_dbs_cache_dir()
    path = os.path.join(CACHE_DIR, "dbs_active_ids.json")
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
    except Exception:
        pass


def add_dbs_active_ids(ids: list[int]) -> None:
    """Добавляет ID заказов DBS в кэш"""
    if not ids:
        return
    cache = load_dbs_active_ids() or {"ids": [], "updated_at": None}
    cur_ids = set(int(x) for x in (cache.get("ids") or []))
    for i in ids:
        try:
            cur_ids.add(int(i))
        except Exception:
            continue
    cache["ids"] = sorted(cur_ids)
    cache["updated_at"] = datetime.now(MOSCOW_TZ).strftime("%d.%m.%Y %H:%M:%S")
    save_dbs_active_ids(cache)


def load_dbs_known_orders() -> Dict[str, Any]:
    """Загружает известные заказы DBS"""
    _ensure_dbs_cache_dir()
    path = os.path.join(CACHE_DIR, "dbs_known_orders.json")
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"orders": {}, "updated_at": None}


def save_dbs_known_orders(data: Dict[str, Any]) -> None:
    """Сохраняет известные заказы DBS"""
    _ensure_dbs_cache_dir()
    path = os.path.join(CACHE_DIR, "dbs_known_orders.json")
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
    except Exception:
        pass


def add_dbs_known_orders(orders: list[dict[str, Any]]) -> None:
    """Добавляет заказы DBS в кэш"""
    if not orders:
        return
    cache = load_dbs_known_orders() or {"orders": {}, "updated_at": None}
    known: Dict[str, Any] = cache.get("orders") or {}
    for it in orders:
        oid = it.get("id") or it.get("orderId") or it.get("ID")
        if oid is None:
            continue
        try:
            key = str(int(oid))
        except Exception:
            continue
        known[key] = {"item": it, "seen_at": datetime.now(MOSCOW_TZ).strftime("%Y-%m-%d %H:%M:%S")}
    cache["orders"] = known
    cache["updated_at"] = datetime.now(MOSCOW_TZ).strftime("%d.%m.%Y %H:%M:%S")
    save_dbs_known_orders(cache)


# --- FBS Stock cache functions ---
def _fbs_stock_cache_path_for_user() -> str:
    """Путь к кэшу остатков FBS для текущего пользователя"""
    if current_user.is_authenticated:
        return os.path.join(CACHE_DIR, f"fbs_stock_user_{current_user.id}.json")
    return os.path.join(CACHE_DIR, f"fbs_stock_{_get_session_id()}.json")


def load_fbs_stock_cache() -> Dict[str, Any] | None:
    """Загружает кэш остатков FBS"""
    path = _fbs_stock_cache_path_for_user()
    if not os.path.isfile(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def save_fbs_stock_cache(payload: Dict[str, Any]) -> None:
    """Сохраняет кэш остатков FBS"""
    path = _fbs_stock_cache_path_for_user()
    try:
        enriched = dict(payload)
        if current_user.is_authenticated:
            enriched["_user_id"] = current_user.id
        with open(path, "w", encoding="utf-8") as f:
            json.dump(enriched, f, ensure_ascii=False)
    except Exception:
        pass


# -*- coding: utf-8 -*-
"""Вспомогательные функции"""
import os
import re
import uuid
import json
from datetime import datetime
from typing import Any, List, Dict
from flask import session
from flask_login import current_user
from utils.constants import MOSCOW_TZ, APP_VERSION


def to_moscow(dt: datetime | None) -> datetime | None:
    """Конвертирует datetime в московское время"""
    if dt is None:
        return None
    try:
        # If datetime is naive, consider it already in Moscow time (many WB fields come without TZ but are MSK)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=MOSCOW_TZ)
        # If it has timezone (e.g., Z/UTC), convert to Moscow
        return dt.astimezone(MOSCOW_TZ)
    except Exception:
        return dt


def format_int_thousands(value: Any) -> str:
    """Форматирует число с пробелами в качестве разделителей тысяч"""
    try:
        return f"{int(value):,}".replace(",", " ")
    except Exception:
        return str(value)


def format_money_ru(value: Any) -> str:
    """Форматирует денежную сумму в русском формате"""
    try:
        return f"{float(value):,.2f}".replace(",", " ").replace(".", ",")
    except Exception:
        return str(value)


def format_dmy(date_str: str) -> str:
    """Форматирует дату из YYYY-MM-DD в DD.MM.YYYY"""
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").strftime("%d.%m.%Y")
    except Exception:
        return date_str or ""


def parse_date(date_str: str) -> datetime:
    """Парсит дату в формате YYYY-MM-DD или DD.MM.YYYY"""
    try:
        # Try YYYY-MM-DD format first
        return datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        try:
            # Try DD.MM.YYYY format
            return datetime.strptime(date_str, "%d.%m.%Y")
        except ValueError:
            raise ValueError(f"Unable to parse date '{date_str}'. Expected formats: YYYY-MM-DD or DD.MM.YYYY")


def parse_wb_datetime(value: str) -> datetime | None:
    """Парсит datetime из формата Wildberries"""
    if not value:
        return None
    s = str(value)
    # Trim subseconds if present to 6 digits max for fromisoformat
    try:
        # Normalize Z to +00:00
        s_norm = s.replace("Z", "+00:00")
        dt = datetime.fromisoformat(s_norm[:26] + s_norm[26:])  # be forgiving on microseconds length
        return dt
    except Exception:
        try:
            return datetime.strptime(s[:19], "%Y-%m-%dT%H:%M:%S")
        except Exception:
            return None


def _parse_iso_datetime(value: str | None) -> datetime | None:
    """Парсит ISO datetime"""
    if not value:
        return None
    try:
        # Support trailing 'Z'
        s = value.replace("Z", "+00:00")
        return datetime.fromisoformat(s)
    except Exception:
        try:
            # Fallback common formats
            return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S%z")
        except Exception:
            try:
                return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S")
            except Exception:
                return None


def _fmt_dt_moscow(value: str | None, with_time: bool = True) -> str:
    """Форматирует datetime в московском времени"""
    dt = _parse_iso_datetime(value)
    if not dt:
        return ""
    msk = to_moscow(dt) or dt
    return msk.strftime("%d.%m.%Y %H:%M") if with_time else msk.strftime("%d.%m.%Y")


def _fbw_status_from_id(status_id: Any) -> str | None:
    """
    Преобразует statusID из API Wildberries в текстовый статус согласно документации:
    1 — Не запланировано
    2 — Запланировано
    3 — Отгрузка разрешена
    4 — Идёт приёмка
    5 — Принято
    6 — Отгружено на воротах
    """
    if status_id is None:
        return None
    try:
        sid = int(status_id)
        status_map = {
            1: "Не запланировано",
            2: "Запланировано",
            3: "Отгрузка разрешена",
            4: "Идёт приёмка",
            5: "Принято",
            6: "Отгружено на воротах",
        }
        return status_map.get(sid)
    except (ValueError, TypeError):
        return None


def extract_nm(value: Any) -> str:
    """Извлекает nm_id из строки"""
    try:
        s = str(value)
        m = re.search(r"(\d{7,12})", s)
        return m.group(1) if m else ""
    except Exception:
        return ""


def days_left_from_str(date_str: str | None) -> int | None:
    """Вычисляет количество дней до даты (формат ДД.ММ.ГГГГ)"""
    try:
        if not date_str:
            return None
        # входящие значения формата "ДД.ММ.ГГГГ" (мы так форматируем planned_date)
        dt = datetime.strptime(date_str.strip(), "%d.%m.%Y")
        today = datetime.now(MOSCOW_TZ).date()
        diff = (dt.date() - today).days
        return diff
    except Exception:
        return None


def time_ago_ru(dt_val: Any) -> str:
    """Возвращает относительное время на русском языке"""
    try:
        if dt_val is None:
            return ""
        if isinstance(dt_val, str):
            s = dt_val.strip()
            dt = parse_wb_datetime(s)
            if dt is None:
                # Try ISO first
                try:
                    dt = datetime.fromisoformat(s)
                except Exception:
                    dt = None
            if dt is None:
                # Try common RU formats
                for fmt in ("%d.%m.%Y %H:%M:%S", "%d.%m.%Y %H:%M", "%d.%m.%Y"):
                    try:
                        dt = datetime.strptime(s, fmt)
                        break
                    except Exception:
                        dt = None
            if dt is None:
                return ""
        elif isinstance(dt_val, datetime):
            dt = dt_val
        else:
            return ""
        # Convert both to Moscow time for consistent human delta
        dt = to_moscow(dt)
        now = datetime.now(MOSCOW_TZ)
        if dt > now:
            return "только что"
        diff = now - dt
        days = diff.days
        seconds = diff.seconds
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        if days > 0:
            return f"{days} д {hours} ч назад" if hours > 0 else f"{days} д назад"
        if hours > 0:
            return f"{hours} ч {minutes} м назад" if minutes > 0 else f"{hours} ч назад"
        if minutes > 0:
            return f"{minutes} м назад"
        return "только что"
    except Exception:
        return ""


def _get_session_id() -> str:
    """Получает ID сессии (для анонимных пользователей)"""
    # For anonymous sessions only; with auth we key cache by user id
    sid = session.get("SID")
    if not sid:
        sid = uuid.uuid4().hex
        session["SID"] = sid
    return sid


def _merge_package_counts(items: list[dict[str, Any]], cached_items: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    """Объединяет количество коробок из кэша с текущими данными"""
    try:
        cache_map: dict[str, int] = {}
        for it in (cached_items or []):
            sid = str(it.get("supply_id") or it.get("supplyID") or it.get("supplyId") or it.get("id") or "")
            pc = it.get("package_count")
            try:
                pc_int = int(pc)
            except Exception:
                pc_int = 0
            if sid and pc_int > 0:
                cache_map[sid] = pc_int
        merged: list[dict[str, Any]] = []
        for it in items:
            sid = str(it.get("supply_id") or it.get("supplyID") or it.get("supplyId") or it.get("id") or "")
            if sid in cache_map and (not it.get("package_count") or int(it.get("package_count") or 0) == 0):
                # Copy to avoid mutating original
                new_it = dict(it)
                new_it["package_count"] = cache_map[sid]
                merged.append(new_it)
            else:
                merged.append(it)
        return merged
    except Exception:
        return items


# --- Version and Changelog helpers ---
def read_version() -> str:
    """Читает версию из файла VERSION"""
    try:
        path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "VERSION")
        if os.path.isfile(path):
            with open(path, "r", encoding="utf-8") as f:
                v = f.read().strip()
                return v or APP_VERSION
    except Exception:
        pass
    return APP_VERSION


def write_version(version: str) -> None:
    """Записывает версию в файл VERSION"""
    try:
        path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "VERSION")
        with open(path, "w", encoding="utf-8") as f:
            f.write((version or "").strip() or "0.0.0")
    except Exception:
        pass


def read_changelog_md() -> str:
    """Читает changelog из файла CHANGELOG.md"""
    md_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "CHANGELOG.md")
    if os.path.isfile(md_path):
        try:
            with open(md_path, "r", encoding="utf-8") as f:
                return f.read()
        except Exception:
            return ""
    # Fallback: convert from old changelog.json if exists
    try:
        json_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "changelog.json")
        if os.path.isfile(json_path):
            with open(json_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list) and data:
                parts: List[str] = [f"# Обновления и изменения\n\nТекущая версия: {read_version()}\n"]
                for e in data:
                    ver = str(e.get("version") or "").strip()
                    date = str(e.get("date") or "").strip()
                    parts.append(f"\n## Версия {ver} — {date}\n")
                    if e.get("html"):
                        parts.append(e["html"])
                        parts.append("\n")
                    else:
                        notes = e.get("notes") or []
                        for n in notes:
                            parts.append(f"- {n}")
                        parts.append("\n")
                return "\n".join(parts)
    except Exception:
        pass
    # Default stub
    return f"# Обновления и изменения\n\nТекущая версия: {read_version()}\n\n## Версия {read_version()} — {datetime.now(MOSCOW_TZ).strftime('%d.%m.%Y')}\n- Первоначальная версия\n"


def write_changelog_md(content: str) -> None:
    """Записывает changelog в файл CHANGELOG.md"""
    try:
        path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "CHANGELOG.md")
        with open(path, "w", encoding="utf-8") as f:
            f.write(content or "")
    except Exception:
        pass


def build_acceptance_grid(items: List[Dict[str, Any]], days: int = 14):
    """Строит сетку коэффициентов приёмки"""
    from datetime import timedelta
    from collections import defaultdict
    
    # Prepare date list: today + next N days
    today = datetime.now(MOSCOW_TZ).date()
    date_objs = [today + timedelta(days=i) for i in range(days + 1)]
    date_keys = [d.strftime("%Y-%m-%d") for d in date_objs]
    date_labels = [d.strftime("%d-%m") for d in date_objs]

    # Filter only box type 'Короба' (boxTypeID == 2) for robustness also match by name
    filtered: List[Dict[str, Any]] = []
    for it in items or []:
        try:
            bt_id = it.get("boxTypeID")
            bt_name = str(it.get("boxTypeName") or "").lower()
            if (bt_id == 2) or ("короб" in bt_name):
                filtered.append(it)
        except Exception:
            continue

    # Unique warehouses from filtered
    warehouses: List[str] = sorted({str(it.get("warehouseName") or "") for it in filtered if it})

    # Map: (warehouse, date_key) -> record
    grid: Dict[str, Dict[str, Dict[str, Any]]] = {w: {} for w in warehouses}

    for it in filtered:
        try:
            wname = str(it.get("warehouseName") or "")
            dkey = str(it.get("date") or "")[:10]
            if wname not in grid or dkey not in date_keys:
                continue
            raw_coef = it.get("coefficient")
            try:
                coef_val = float(raw_coef)
            except Exception:
                coef_val = None
            grid[wname][dkey] = {
                "coef": coef_val,
                "allow": bool(it.get("allowUnload")),
            }
        except Exception:
            continue

    # Fill empty cells
    for w in warehouses:
        for dkey in date_keys:
            if dkey not in grid[w]:
                grid[w][dkey] = {"coef": None, "allow": None}

    # Sort warehouses by number of non-negative coefficients (>=0) across the horizon
    def count_non_negative(w: str) -> int:
        count = 0
        for dkey in date_keys:
            coef = grid[w][dkey].get("coef")
            try:
                if coef is not None and float(coef) >= 0:
                    count += 1
            except Exception:
                continue
        return count

    warehouses.sort(key=lambda w: count_non_negative(w), reverse=True)

    return warehouses, date_keys, date_labels, grid


def normalize_cards_response(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Нормализует ответ API карточек товаров"""
    items: List[Dict[str, Any]] = []
    try:
        payload = data.get("data") or data
        cards = payload.get("cards") or []
        for c in cards:
            nm_id = c.get("nmID") or c.get("nmId") or c.get("nm")
            supplier_article = c.get("supplierArticle") or c.get("vendorCode") or c.get("article")
            photo = None
            try:
                photos = c.get("mediaFiles") or c.get("photos") or []
                if isinstance(photos, list) and photos:
                    p0 = photos[0]
                    if isinstance(p0, str):
                        photo = p0
                    elif isinstance(p0, dict):
                        photo = p0.get("small") or p0.get("preview") or p0.get("url") or p0.get("big")
                if isinstance(photo, str) and photo.startswith("//"):
                    photo = "https:" + photo
                if isinstance(photo, str) and not (photo.startswith("http://") or photo.startswith("https://")):
                    photo = "https://" + photo.lstrip("/")
            except Exception:
                photo = None
            barcode = None
            size_info = None
            try:
                sizes = c.get("sizes") or []
                for s in sizes:
                    chrt_id = s.get("chrtID")
                    skus = s.get("skus") or s.get("barcodes") or []
                    if skus and not barcode:
                        barcode = str(skus[0])
                    if chrt_id:
                        size_info = {
                            "chrtID": chrt_id,
                            "skus": [str(x) for x in (s.get("skus") or [])]
                        }
                        break
            except Exception:
                barcode = None
            name = c.get("name") or c.get("title") or c.get("subject") or "Без названия"
            subject_id = c.get("subjectID") or c.get("subjectId") or c.get("subject_id")
            dimensions = c.get("dimensions") or {}
            length = dimensions.get("length", 0)
            width = dimensions.get("width", 0)
            height = dimensions.get("height", 0)
            weight = dimensions.get("weightBrutto", 0)
            volume = (length * width * height) / 1000 if all([length, width, height]) else 0
            
            items.append({
                "photo": photo,
                "supplier_article": supplier_article,
                "nm_id": nm_id,
                "barcode": barcode,
                "chrt_id": size_info.get("chrtID") if size_info else None,
                "name": name,
                "subject_id": subject_id,
                "dimensions": {
                    "length": length,
                    "width": width,
                    "height": height,
                    "weight": weight,
                    "volume": round(volume, 2)
                },
                "size_info": size_info
            })
    except Exception:
        pass
    return items


def normalize_stocks(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Нормализует данные остатков на складах"""
    items: List[Dict[str, Any]] = []
    for r in rows or []:
        qty_val = r.get("quantity") or r.get("qty") or 0
        try:
            qty_int = int(qty_val)
        except Exception:
            try:
                qty_int = int(float(qty_val))
            except Exception:
                qty_int = 0
        in_way_to_client = r.get("inWayToClient") or 0
        in_way_from_client = r.get("inWayFromClient") or 0
        try:
            in_way_to_client = int(in_way_to_client)
        except Exception:
            try:
                in_way_to_client = int(float(in_way_to_client))
            except Exception:
                in_way_to_client = 0
        try:
            in_way_from_client = int(in_way_from_client)
        except Exception:
            try:
                in_way_from_client = int(float(in_way_from_client))
            except Exception:
                in_way_from_client = 0
        in_transit_total = max(0, in_way_to_client + in_way_from_client)
        items.append({
            "vendor_code": r.get("supplierArticle") or r.get("vendorCode") or r.get("article"),
            "barcode": r.get("barcode") or r.get("skus") or r.get("sku"),
            "nm_id": r.get("nmId") or r.get("nmID") or r.get("nm") or None,
            "qty": qty_int,
            "in_transit": in_transit_total,
            "warehouse": r.get("warehouseName") or r.get("warehouse") or r.get("warehouse_name"),
        })
    return items


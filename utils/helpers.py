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


ACCEPTANCE_CARGO_LABELS = {
    1: "МГТ (малогабаритный)",
    2: "СГТ (сверхгабаритный)",
    3: "КГТ+ (крупногабаритный)",
}
ACCEPTANCE_DELIVERY_LABELS = {
    1: "FBS",
    2: "DBS",
    3: "DBW",
    5: "C&C",
    6: "EDBS",
}


def _acceptance_box_type_label(item: Dict[str, Any]) -> str:
    name = str(item.get("boxTypeName") or item.get("box_type") or "").strip()
    if name:
        return name
    raw_id = item.get("boxTypeID") or item.get("box_type_id")
    try:
        box_type_map = {
            1: "Без коробов",
            2: "Короба",
            3: "Монопаллета",
            4: "Суперсейф",
            5: "Паллета",
        }
        return box_type_map.get(int(raw_id), f"Тип {raw_id}")
    except Exception:
        return ""


def _acceptance_cargo_type_label(item: Dict[str, Any], warehouse_meta: Dict[str, Any] | None = None) -> str:
    for key in ("virtualTypeName", "cargoTypeName", "cargo_type", "cargoType"):
        value = str(item.get(key) or "").strip()
        if value:
            return value
    if warehouse_meta:
        meta_value = str((warehouse_meta or {}).get("cargo_type") or "").strip()
        if meta_value:
            return meta_value
    raw_id = item.get("virtualTypeID") or item.get("cargoTypeID") or item.get("cargo_type_id")
    try:
        return ACCEPTANCE_CARGO_LABELS.get(int(raw_id), f"Тип груза {raw_id}")
    except Exception:
        return ""


def _normalize_wh_key(name: str) -> str:
    s = str(name or "").strip().lower().replace("ё", "е")
    return re.sub(r"\s+", " ", s)


def _base_wh_name(name: str) -> str:
    s = _normalize_wh_key(name)
    for suffix in (" сгт", " мгт", " кгт+", " кгт"):
        if s.endswith(suffix):
            s = s[: -len(suffix)].strip()
    if "(" in s:
        s = s.split("(", 1)[0].strip()
    return s


def _cargo_labels_from_name(name: str) -> list[str]:
    labels: list[str] = []
    norm = _normalize_wh_key(name)
    if re.search(r"\bсгт\b", norm) or norm.endswith(" сгт"):
        labels.append(ACCEPTANCE_CARGO_LABELS[2])
    if re.search(r"\bкгт\+?\b", norm):
        labels.append(ACCEPTANCE_CARGO_LABELS[3])
    if re.search(r"\bмгт\b", norm) or norm.endswith(" мгт"):
        labels.append(ACCEPTANCE_CARGO_LABELS[1])
    return labels


def _merge_meta_dict(entry: Dict[str, Any], patch: Dict[str, Any]) -> Dict[str, Any]:
    entry.setdefault("cargo_types", [])
    entry.setdefault("delivery_types", [])
    for field in ("address", "city"):
        val = str(patch.get(field) or "").strip()
        if val and not str(entry.get(field) or "").strip():
            entry[field] = val
    for field in ("warehouse_id", "office_id"):
        if patch.get(field) is not None and entry.get(field) is None:
            entry[field] = patch.get(field)
    if patch.get("is_fbw"):
        entry["is_fbw"] = True
    if patch.get("is_fbs"):
        entry["is_fbs"] = True
    for list_field in ("cargo_types", "delivery_types"):
        for value in patch.get(list_field) or []:
            label = str(value or "").strip()
            if label and label not in entry[list_field]:
                entry[list_field].append(label)
    return entry


def _finalize_meta_entry(entry: Dict[str, Any]) -> Dict[str, Any]:
    if entry.get("cargo_types"):
        entry["cargo_type"] = ", ".join(sorted(entry["cargo_types"]))
    if entry.get("delivery_types"):
        entry["delivery_type"] = ", ".join(sorted(entry["delivery_types"]))
    return entry


def _unique_meta_values(meta_map: Dict[str, Dict[str, Any]] | None) -> list[Dict[str, Any]]:
    seen: set[int] = set()
    result: list[Dict[str, Any]] = []
    for key, meta in (meta_map or {}).items():
        if str(key).startswith("id:"):
            continue
        oid = id(meta)
        if oid in seen:
            continue
        seen.add(oid)
        result.append(meta)
    return result


def _lookup_warehouse_meta(
    warehouse_meta_map: Dict[str, Dict[str, Any]] | None,
    warehouse_name: str,
    warehouse_id: Any | None = None,
) -> Dict[str, Any]:
    if not warehouse_meta_map:
        return {}
    if warehouse_id is not None:
        try:
            entry = warehouse_meta_map.get(f"id:{int(warehouse_id)}")
            if entry:
                return entry
        except Exception:
            pass
    name = str(warehouse_name or "").strip()
    if not name:
        return {}
    for key in (name, _normalize_wh_key(name), _base_wh_name(name)):
        if key and key in warehouse_meta_map:
            return warehouse_meta_map[key]
    base = _base_wh_name(name)
    norm = _normalize_wh_key(name)
    for key, value in warehouse_meta_map.items():
        if str(key).startswith("id:"):
            continue
        if base and _base_wh_name(str(key)) == base:
            return value
        if _normalize_wh_key(str(key)) == norm:
            return value
    return {}


def normalize_acceptance_items(
    items: List[Dict[str, Any]] | None,
    warehouse_meta_map: Dict[str, Dict[str, Any]] | None = None,
) -> List[Dict[str, Any]]:
    """Нормализует ответ WB по коэффициентам приёмки для UI/фильтров."""
    normalized: List[Dict[str, Any]] = []
    for it in items or []:
        if not isinstance(it, dict):
            continue
        warehouse_name = str(it.get("warehouseName") or it.get("warehouse") or "").strip()
        warehouse_id = it.get("warehouseID") or it.get("warehouseId") or it.get("warehouse_id")
        date_key = str(it.get("date") or "")[:10]
        if not warehouse_name or not date_key:
            continue
        warehouse_meta = _lookup_warehouse_meta(warehouse_meta_map, warehouse_name, warehouse_id)
        raw_coef = it.get("coefficient")
        try:
            coef_val = float(raw_coef)
        except Exception:
            coef_val = None
        cargo_types = list(warehouse_meta.get("cargo_types") or [])
        if not cargo_types and warehouse_meta.get("cargo_type"):
            cargo_types = [str(warehouse_meta.get("cargo_type") or "").strip()]
        delivery_types = list(warehouse_meta.get("delivery_types") or [])
        if not delivery_types and warehouse_meta.get("delivery_type"):
            delivery_types = [
                part.strip()
                for part in str(warehouse_meta.get("delivery_type") or "").split(",")
                if part.strip()
            ]
        normalized.append({
            "warehouseName": warehouse_name,
            "date": date_key,
            "coefficient": coef_val,
            "allowUnload": it.get("allowUnload"),
            "box_type": _acceptance_box_type_label(it),
            "cargo_type": _acceptance_cargo_type_label(it, warehouse_meta),
            "cargo_types": cargo_types,
            "delivery_type": str(warehouse_meta.get("delivery_type") or "").strip(),
            "delivery_types": delivery_types,
            "address": str(warehouse_meta.get("address") or "").strip(),
            "boxTypeID": it.get("boxTypeID"),
            "virtualTypeID": it.get("virtualTypeID"),
        })
    return normalized


def enrich_warehouse_meta_for_names(
    warehouse_meta_map: Dict[str, Dict[str, Any]] | None,
    warehouse_names: List[str] | None,
    coefficient_items: List[Dict[str, Any]] | None = None,
) -> Dict[str, Dict[str, Any]]:
    """Возвращает метаданные, проиндексированные по именам складов из tariffs."""
    enriched: Dict[str, Dict[str, Any]] = {}
    names_with_ids: dict[str, Any] = {}
    for name in warehouse_names or []:
        clean = str(name or "").strip()
        if clean:
            names_with_ids.setdefault(clean, None)
    for it in coefficient_items or []:
        if not isinstance(it, dict):
            continue
        clean = str(it.get("warehouseName") or it.get("warehouse") or "").strip()
        if not clean:
            continue
        wid = it.get("warehouseID") or it.get("warehouseId") or it.get("warehouse_id")
        if names_with_ids.get(clean) is None and wid is not None:
            names_with_ids[clean] = wid
        elif clean not in names_with_ids:
            names_with_ids[clean] = wid
    for name, wid in names_with_ids.items():
        matched = _lookup_warehouse_meta(warehouse_meta_map, name, wid)
        if matched:
            enriched[name] = matched
    return enriched


def extract_acceptance_filter_options(
    items: List[Dict[str, Any]] | None,
    warehouse_meta_map: Dict[str, Dict[str, Any]] | None = None,
) -> tuple[List[str], List[str], List[str]]:
    normalized = normalize_acceptance_items(items, warehouse_meta_map)
    box_types = sorted({str(it.get("box_type") or "").strip() for it in normalized if str(it.get("box_type") or "").strip()})
    cargo_set: set[str] = set()
    delivery_set: set[str] = set()
    for meta in _unique_meta_values(warehouse_meta_map):
        for value in meta.get("cargo_types") or []:
            label = str(value or "").strip()
            if label:
                cargo_set.add(label)
        cargo_single = str(meta.get("cargo_type") or "").strip()
        if cargo_single:
            for part in cargo_single.split(","):
                part = part.strip()
                if part:
                    cargo_set.add(part)
        for value in meta.get("delivery_types") or []:
            label = str(value or "").strip()
            if label:
                delivery_set.add(label)
        delivery_single = str(meta.get("delivery_type") or "").strip()
        if delivery_single:
            for part in delivery_single.split(","):
                part = part.strip()
                if part:
                    delivery_set.add(part)
    cargo_types = sorted(cargo_set) or sorted(ACCEPTANCE_CARGO_LABELS.values())
    delivery_types = sorted(delivery_set) or sorted(ACCEPTANCE_DELIVERY_LABELS.values())
    return box_types, cargo_types, delivery_types


def build_acceptance_grid(
    items: List[Dict[str, Any]],
    days: int = 14,
    selected_box_types: List[str] | None = None,
    selected_cargo_types: List[str] | None = None,
):
    """Строит сетку коэффициентов приёмки."""
    from datetime import timedelta
    
    # Prepare date list: today + next N days
    today = datetime.now(MOSCOW_TZ).date()
    date_objs = [today + timedelta(days=i) for i in range(days + 1)]
    date_keys = [d.strftime("%Y-%m-%d") for d in date_objs]
    date_labels = [d.strftime("%d-%m") for d in date_objs]

    normalized = normalize_acceptance_items(items)
    selected_box_types_set = {str(x).strip() for x in (selected_box_types or []) if str(x).strip()}
    selected_cargo_types_set = {str(x).strip() for x in (selected_cargo_types or []) if str(x).strip()}

    filtered: List[Dict[str, Any]] = []
    for it in normalized:
        box_type = str(it.get("box_type") or "").strip()
        cargo_type = str(it.get("cargo_type") or "").strip()
        if selected_box_types_set and box_type not in selected_box_types_set:
            continue
        if selected_cargo_types_set and cargo_type not in selected_cargo_types_set:
            continue
        filtered.append(it)

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
            coef_val = it.get("coefficient")
            candidate = {
                "coef": coef_val,
                "allow": it.get("allowUnload"),
                "box_type": it.get("box_type"),
                "cargo_type": it.get("cargo_type"),
            }
            prev = grid[wname].get(dkey)
            if not prev:
                grid[wname][dkey] = candidate
                continue

            prev_coef = prev.get("coef")
            # Prefer available non-negative coefficients over blocked/empty ones.
            if prev_coef is None:
                grid[wname][dkey] = candidate
            elif coef_val is not None and prev_coef is not None:
                if float(prev_coef) < 0 <= float(coef_val):
                    grid[wname][dkey] = candidate
                elif float(coef_val) >= 0 and float(prev_coef) >= 0 and float(coef_val) < float(prev_coef):
                    grid[wname][dkey] = candidate
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


def _pick_box_tariff_value(box_tariff: list[dict[str, Any]] | None, *, small: bool) -> float | None:
    """Возвращает тариф за литр для короба <1500 л (small) или >1500 л (large)."""
    tiers: list[dict[str, Any]] = [t for t in (box_tariff or []) if isinstance(t, dict)]
    if not tiers:
        return None
    if small:
        for tier in tiers:
            try:
                to_val = tier.get("to")
                if to_val is not None and int(to_val) <= 1500:
                    return float(tier.get("value"))
            except Exception:
                continue
        try:
            tiers_sorted = sorted(tiers, key=lambda t: int(t.get("from") or 0))
            if tiers_sorted:
                first = tiers_sorted[0]
                to_val = first.get("to")
                if to_val is None or int(to_val) <= 1500:
                    return float(first.get("value"))
        except Exception:
            pass
        return None
    for tier in tiers:
        try:
            from_val = int(tier.get("from") or 0)
            if from_val >= 1500:
                return float(tier.get("value"))
        except Exception:
            continue
    try:
        tiers_sorted = sorted(tiers, key=lambda t: int(t.get("from") or 0))
        if len(tiers_sorted) >= 2:
            return float(tiers_sorted[1].get("value"))
        if len(tiers_sorted) == 1:
            to_val = tiers_sorted[0].get("to")
            if to_val is not None and int(to_val) > 1500:
                return float(tiers_sorted[0].get("value"))
    except Exception:
        pass
    return None


def normalize_transit_tariff_items(items: List[Dict[str, Any]] | None) -> List[Dict[str, Any]]:
    """Нормализует ответ WB /api/v1/transit-tariffs для UI."""
    normalized: List[Dict[str, Any]] = []
    for it in items or []:
        if not isinstance(it, dict):
            continue
        transit = str(it.get("transitWarehouseName") or "").strip()
        destination = str(it.get("destinationWarehouseName") or "").strip()
        if not transit or not destination:
            continue
        box_tariff = it.get("boxTariff") if isinstance(it.get("boxTariff"), list) else []
        pallet_raw = it.get("palletTariff")
        pallet_val = None
        pallet_available = False
        try:
            if pallet_raw is not None:
                pallet_val = int(pallet_raw)
                pallet_available = True
        except Exception:
            pallet_available = False
        normalized.append({
            "transit_warehouse": transit,
            "destination_warehouse": destination,
            "active_from": str(it.get("activeFrom") or "").strip(),
            "pallet_tariff": pallet_val,
            "pallet_available": pallet_available,
            "box_available": bool(box_tariff),
            "box_tariff_small": _pick_box_tariff_value(box_tariff, small=True),
            "box_tariff_large": _pick_box_tariff_value(box_tariff, small=False),
        })
    normalized.sort(key=lambda row: (row["transit_warehouse"].lower(), row["destination_warehouse"].lower()))
    return normalized


def build_transit_table_rows(items: List[Dict[str, Any]] | None) -> List[Dict[str, Any]]:
    """Группирует строки по транзитному складу для rowspan в таблице."""
    normalized = normalize_transit_tariff_items(items)
    groups: Dict[str, List[Dict[str, Any]]] = {}
    for row in normalized:
        groups.setdefault(row["transit_warehouse"], []).append(row)
    table_rows: List[Dict[str, Any]] = []
    for transit_name in sorted(groups.keys(), key=str.lower):
        group_rows = groups[transit_name]
        for idx, row in enumerate(group_rows):
            table_rows.append({
                **row,
                "show_transit": idx == 0,
                "transit_rowspan": len(group_rows) if idx == 0 else 0,
            })
    return table_rows


def extract_transit_filter_options(items: List[Dict[str, Any]] | None) -> tuple[List[str], List[str]]:
    normalized = normalize_transit_tariff_items(items)
    transit_names = sorted({row["transit_warehouse"] for row in normalized})
    destination_names = sorted({row["destination_warehouse"] for row in normalized})
    return transit_names, destination_names


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
    """Нормализует данные остатков на складах WB (старый Statistics и новый Analytics API)."""
    items: List[Dict[str, Any]] = []
    for r in rows or []:
        if not isinstance(r, dict):
            continue
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
        nm_raw = r.get("nmId") or r.get("nmID") or r.get("nm") or None
        try:
            nm_id = int(nm_raw) if nm_raw is not None else None
        except Exception:
            nm_id = nm_raw
        chrt_raw = r.get("chrtId") or r.get("chrtID") or r.get("chrt_id")
        try:
            chrt_id = int(chrt_raw) if chrt_raw is not None else None
        except Exception:
            chrt_id = chrt_raw
        items.append({
            "vendor_code": r.get("supplierArticle") or r.get("vendorCode") or r.get("article") or r.get("vendor_code"),
            "barcode": r.get("barcode") or r.get("skus") or r.get("sku"),
            "nm_id": nm_id,
            "chrt_id": chrt_id,
            "qty": qty_int,
            "in_transit": in_transit_total,
            "warehouse": r.get("warehouseName") or r.get("warehouse") or r.get("warehouse_name"),
            "warehouse_id": r.get("warehouseId") or r.get("warehouseID"),
            "region": r.get("regionName") or r.get("region"),
        })
    return items


def stock_row_product_key(it: Dict[str, Any]) -> tuple:
    """Ключ агрегации товара для страницы /stocks."""
    vc = str(it.get("vendor_code") or "").strip()
    bc = str(it.get("barcode") or "").strip()
    if vc or bc:
        return (vc, bc)
    nm = it.get("nm_id")
    chrt = it.get("chrt_id")
    return (f"nm:{nm if nm is not None else ''}", str(chrt if chrt is not None else ""))


def enrich_stocks_from_products(
    items: List[Dict[str, Any]] | None,
    products: List[Dict[str, Any]] | None = None,
) -> List[Dict[str, Any]]:
    """Подставляет vendor_code/barcode из кэша товаров (новый API остатков их не отдаёт)."""
    rows = list(items or [])
    if not rows:
        return rows
    if products is None:
        try:
            from utils.cache import load_products_cache
            products = (load_products_cache() or {}).get("items") or []
        except Exception:
            products = []
    by_nm: Dict[int, Dict[str, Any]] = {}
    for p in products or []:
        if not isinstance(p, dict):
            continue
        nmv = p.get("nm_id") or p.get("nmId") or p.get("nmID")
        if nmv is None:
            continue
        try:
            by_nm[int(nmv)] = p
        except Exception:
            continue
    for it in rows:
        nm = it.get("nm_id")
        if nm is None:
            continue
        try:
            meta = by_nm.get(int(nm))
        except Exception:
            meta = None
        if not meta:
            continue
        if not str(it.get("vendor_code") or "").strip():
            it["vendor_code"] = (
                meta.get("vendor_code")
                or meta.get("supplierArticle")
                or meta.get("vendorCode")
                or meta.get("article")
            )
        if not str(it.get("barcode") or "").strip():
            it["barcode"] = meta.get("barcode") or meta.get("sku")
    return rows


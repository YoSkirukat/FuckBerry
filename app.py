# FBS warehouses/stocks
FBS_WAREHOUSES_URL = "https://marketplace-api.wildberries.ru/api/v3/warehouses"
FBS_STOCKS_BY_WAREHOUSE_URL = "https://marketplace-api.wildberries.ru/api/v3/stocks/{warehouseId}"
# Supplies API warehouses (for labels tool)
SUPPLIES_WAREHOUSES_URL = "https://supplies-api.wildberries.ru/api/v1/warehouses"
import io
import os
import json
import uuid
import time
import random
import threading
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Tuple

import requests
from flask import Flask, render_template, request, send_file, redirect, url_for, session, flash, jsonify, send_from_directory
import xlwt
from io import BytesIO
from openpyxl import Workbook
from docx import Document
from docx.shared import Pt, Cm
from docx.enum.text import WD_ALIGN_PARAGRAPH, WD_BREAK
from docx.oxml import OxmlElement
from docx.oxml.ns import qn
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import text
from flask_login import (
    LoginManager,
    UserMixin,
    login_required,
    login_user,
    logout_user,
    current_user,
)

APP_VERSION = "1.0.1"

def _read_version() -> str:
    try:
        path = os.path.join(os.path.dirname(__file__), "VERSION")
        if os.path.isfile(path):
            with open(path, "r", encoding="utf-8") as f:
                v = f.read().strip()
                return v or APP_VERSION
    except Exception:
        pass
    return APP_VERSION

def _read_changelog_md() -> str:
    """Read Markdown changelog (CHANGELOG.md). If missing, try convert from JSON fallback."""
    md_path = os.path.join(os.path.dirname(__file__), "CHANGELOG.md")
    if os.path.isfile(md_path):
        try:
            with open(md_path, "r", encoding="utf-8") as f:
                return f.read()
        except Exception:
            return ""
    # Fallback: convert from old changelog.json if exists
    try:
        json_path = os.path.join(os.path.dirname(__file__), "changelog.json")
        if os.path.isfile(json_path):
            with open(json_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list) and data:
                parts: List[str] = [f"# Обновления и изменения\n\nТекущая версия: {_read_version()}\n"]
                for e in data:
                    ver = str(e.get("version") or "").strip()
                    date = str(e.get("date") or "").strip()
                    parts.append(f"\n## Версия {ver} — {date}\n")
                    if e.get("html"):
                        parts.append(e["html"])  # embed existing html as-is
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
    return f"# Обновления и изменения\n\nТекущая версия: {_read_version()}\n\n## Версия {_read_version()} — {datetime.now(MOSCOW_TZ).strftime('%d.%m.%Y')}\n- Первоначальная версия\n"

def _write_version(version: str) -> None:
    try:
        path = os.path.join(os.path.dirname(__file__), "VERSION")
        with open(path, "w", encoding="utf-8") as f:
            f.write((version or "").strip() or "0.0.0")
    except Exception:
        pass

def _write_changelog_md(content: str) -> None:
    try:
        path = os.path.join(os.path.dirname(__file__), "CHANGELOG.md")
        with open(path, "w", encoding="utf-8") as f:
            f.write(content or "")
    except Exception:
        pass

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET", "dev-secret-change-me")
app.config["SQLALCHEMY_DATABASE_URI"] = os.getenv("DATABASE_URL", f"sqlite:///{os.path.join(os.path.dirname(__file__), 'app.db')}")
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
db = SQLAlchemy(app)
login_manager = LoginManager(app)
login_manager.login_view = "login"
# --- DB init helpers (portable across common DBs) ---
def _ensure_schema_users_validity_columns() -> None:
    try:
        engine = db.engine
        dialect = getattr(engine, "name", getattr(engine.dialect, "name", "")) or getattr(engine.dialect, "name", "")
        # Use transactional context so DDL is committed on PG/MySQL
        with engine.begin() as conn:
            if dialect == "sqlite":
                try:
                    rows = conn.execute(text("PRAGMA table_info(users)")).fetchall()
                    cols = {r[1] for r in rows}
                    if "valid_from" not in cols:
                        conn.execute(text("ALTER TABLE users ADD COLUMN valid_from DATE"))
                    if "valid_to" not in cols:
                        conn.execute(text("ALTER TABLE users ADD COLUMN valid_to DATE"))
                    if "phone" not in cols:
                        conn.execute(text("ALTER TABLE users ADD COLUMN phone VARCHAR(64)"))
                    if "email" not in cols:
                        conn.execute(text("ALTER TABLE users ADD COLUMN email VARCHAR(120)"))
                    if "shipper_name" not in cols:
                        conn.execute(text("ALTER TABLE users ADD COLUMN shipper_name VARCHAR(255)"))
                    if "shipper_address" not in cols:
                        conn.execute(text("ALTER TABLE users ADD COLUMN shipper_address VARCHAR(255)"))
                    if "contact_person" not in cols:
                        conn.execute(text("ALTER TABLE users ADD COLUMN contact_person VARCHAR(255)"))
                except Exception:
                    pass
            elif dialect in ("postgresql", "postgres"):
                # IF NOT EXISTS works on Postgres
                try:
                    conn.execute(text("ALTER TABLE users ADD COLUMN IF NOT EXISTS valid_from DATE"))
                except Exception:
                    pass
                try:
                    conn.execute(text("ALTER TABLE users ADD COLUMN IF NOT EXISTS valid_to DATE"))
                except Exception:
                    pass
                try:
                    conn.execute(text("ALTER TABLE users ADD COLUMN IF NOT EXISTS phone VARCHAR(64)"))
                except Exception:
                    pass
                try:
                    conn.execute(text("ALTER TABLE users ADD COLUMN IF NOT EXISTS email VARCHAR(120)"))
                except Exception:
                    pass
                try:
                    conn.execute(text("ALTER TABLE users ADD COLUMN IF NOT EXISTS shipper_name VARCHAR(255)"))
                except Exception:
                    pass
                try:
                    conn.execute(text("ALTER TABLE users ADD COLUMN IF NOT EXISTS shipper_address VARCHAR(255)"))
                except Exception:
                    pass
                try:
                    conn.execute(text("ALTER TABLE users ADD COLUMN IF NOT EXISTS contact_person VARCHAR(255)"))
                except Exception:
                    pass
            elif dialect in ("mysql", "mariadb"):
                # MySQL 8.0+ supports IF NOT EXISTS
                try:
                    conn.execute(text("ALTER TABLE users ADD COLUMN IF NOT EXISTS valid_from DATE"))
                except Exception:
                    pass
                try:
                    conn.execute(text("ALTER TABLE users ADD COLUMN IF NOT EXISTS valid_to DATE"))
                except Exception:
                    pass
                try:
                    conn.execute(text("ALTER TABLE users ADD COLUMN IF NOT EXISTS phone VARCHAR(64)"))
                except Exception:
                    pass
                try:
                    conn.execute(text("ALTER TABLE users ADD COLUMN IF NOT EXISTS email VARCHAR(120)"))
                except Exception:
                    pass
                try:
                    conn.execute(text("ALTER TABLE users ADD COLUMN IF NOT EXISTS shipper_name VARCHAR(255)"))
                except Exception:
                    pass
                try:
                    conn.execute(text("ALTER TABLE users ADD COLUMN IF NOT EXISTS shipper_address VARCHAR(255)"))
                except Exception:
                    pass
            else:
                # Best-effort generic
                try:
                    conn.execute(text("ALTER TABLE users ADD COLUMN valid_from DATE"))
                except Exception:
                    pass
                try:
                    conn.execute(text("ALTER TABLE users ADD COLUMN valid_to DATE"))
                except Exception:
                    pass
                try:
                    conn.execute(text("ALTER TABLE users ADD COLUMN phone VARCHAR(64)"))
                except Exception:
                    pass
                try:
                    conn.execute(text("ALTER TABLE users ADD COLUMN email VARCHAR(120)"))
                except Exception:
                    pass
                try:
                    conn.execute(text("ALTER TABLE users ADD COLUMN shipper_name VARCHAR(255)"))
                except Exception:
                    pass
                try:
                    conn.execute(text("ALTER TABLE users ADD COLUMN shipper_address VARCHAR(255)"))
                except Exception:
                    pass
                try:
                    conn.execute(text("ALTER TABLE users ADD COLUMN contact_person VARCHAR(255)"))
                except Exception:
                    pass
    except Exception:
        pass


# Note: Flask 3.x removed before_first_request. We init DB in __main__ when run as a script.

API_URL = "https://statistics-api.wildberries.ru/api/v1/supplier/orders"
SALES_API_URL = "https://statistics-api.wildberries.ru/api/v1/supplier/sales"
CACHE_DIR = os.path.join(os.path.dirname(__file__), "cache")
if not os.path.isdir(CACHE_DIR):
    os.makedirs(CACHE_DIR, exist_ok=True)

FBS_NEW_URL = "https://marketplace-api.wildberries.ru/api/v3/orders/new"
FBS_ORDERS_URL = "https://marketplace-api.wildberries.ru/api/v3/orders"
FBS_ORDERS_STATUS_URL = "https://marketplace-api.wildberries.ru/api/v3/orders/status"
FBS_SUPPLIES_LIST_URL = "https://marketplace-api.wildberries.ru/api/v3/supplies"
FBS_SUPPLY_INFO_URL = "https://marketplace-api.wildberries.ru/api/v3/supplies/{supplyId}"
FBS_SUPPLY_ORDERS_URL = "https://marketplace-api.wildberries.ru/api/v3/supplies/{supplyId}/orders"

SELLER_INFO_URL = "https://common-api.wildberries.ru/api/v1/seller-info"

ACCEPT_COEFS_URL = "https://supplies-api.wildberries.ru/api/v1/acceptance/coefficients"
# FBW supplies API
FBW_SUPPLIES_LIST_URL = "https://supplies-api.wildberries.ru/api/v1/supplies"
FBW_SUPPLY_DETAILS_URL = "https://supplies-api.wildberries.ru/api/v1/supplies/{id}"
FBW_SUPPLY_GOODS_URL = "https://supplies-api.wildberries.ru/api/v1/supplies/{id}/goods"
FBW_SUPPLY_PACKAGE_URL = "https://supplies-api.wildberries.ru/api/v1/supplies/{id}/package"
# Wildberries Content API: cards list
WB_CARDS_LIST_URL = "https://content-api.wildberries.ru/content/v2/get/cards/list"
STOCKS_API_URL = "https://statistics-api.wildberries.ru/api/v1/supplier/stocks"
FIN_REPORT_URL = "https://statistics-api.wildberries.ru/api/v5/supplier/reportDetailByPeriod"

# Wildberries Content API: cards list
WB_CARDS_LIST_URL = "https://content-api.wildberries.ru/content/v2/get/cards/list"


# Timezone helpers (Moscow)
try:
    from zoneinfo import ZoneInfo  # Python 3.9+
    MOSCOW_TZ = ZoneInfo("Europe/Moscow")
except Exception:  # Fallback to fixed UTC+3 if zoneinfo unavailable
    MOSCOW_TZ = timezone(timedelta(hours=3))

def to_moscow(dt: datetime | None) -> datetime | None:
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
    try:
        return f"{int(value):,}".replace(",", " ")
    except Exception:
        return str(value)


def _parse_iso_datetime(value: str | None) -> datetime | None:
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
    dt = _parse_iso_datetime(value)
    if not dt:
        return ""
    msk = to_moscow(dt) or dt
    return msk.strftime("%d.%m.%Y %H:%M") if with_time else msk.strftime("%d.%m.%Y")


def fetch_fbw_supplies_list(token: str, days_back: int = 90) -> list[dict[str, Any]]:
    if not token:
        return []
    date_to = datetime.now(MOSCOW_TZ).date()
    date_from = date_to - timedelta(days=days_back)
    # Some WB endpoints treat 'till' as exclusive. Add +1 day to include entire current day.
    date_till = date_to + timedelta(days=1)
    body = {
        "dates": [
            {
                "from": date_from.strftime("%Y-%m-%d"),
                "till": date_till.strftime("%Y-%m-%d"),
                "type": "createDate",
            }
        ]
        # statusIDs optional; omit to include all
    }
    # Try Bearer first
    headers1 = {"Authorization": f"Bearer {token}"}
    try:
        resp = post_with_retry(FBW_SUPPLIES_LIST_URL, headers1, body)
        items = resp.json() or []
    except Exception:
        headers2 = {"Authorization": f"{token}"}
        resp = post_with_retry(FBW_SUPPLIES_LIST_URL, headers2, body)
        items = resp.json() or []
    # Sort by createDate desc
    def _key(it: dict[str, Any]):
        return _parse_iso_datetime(str(it.get("createDate") or "")) or datetime.min.replace(tzinfo=MOSCOW_TZ)
    items.sort(key=_key, reverse=True)
    return items


def fetch_fbw_supply_details(token: str, supply_id: int | str) -> dict[str, Any] | None:
    if not token or not supply_id:
        return None
    url = FBW_SUPPLY_DETAILS_URL.format(id=supply_id)
    headers1 = {"Authorization": f"Bearer {token}"}
    try:
        resp = get_with_retry(url, headers1, params={})
        return resp.json()
    except Exception:
        headers2 = {"Authorization": f"{token}"}
        try:
            resp = get_with_retry(url, headers2, params={})
            return resp.json()
        except Exception:
            return None


def fetch_fbw_supply_goods(token: str, supply_id: int | str, limit: int = 200, offset: int = 0) -> list[dict[str, Any]]:
    if not token or not supply_id:
        return []
    url = FBW_SUPPLY_GOODS_URL.format(id=supply_id)
    headers1 = {"Authorization": f"Bearer {token}"}
    params = {"limit": limit, "offset": offset}
    try:
        resp = get_with_retry(url, headers1, params=params)
        return resp.json() or []
    except Exception:
        headers2 = {"Authorization": f"{token}"}
        try:
            resp = get_with_retry(url, headers2, params=params)
            return resp.json() or []
        except Exception:
            return []


def fetch_fbw_supply_packages(token: str, supply_id: int | str) -> list[dict[str, Any]]:
    if not token or not supply_id:
        return []
    url = FBW_SUPPLY_PACKAGE_URL.format(id=supply_id)
    headers1 = {"Authorization": f"Bearer {token}"}
    try:
        resp = get_with_retry(url, headers1, params={})
        return resp.json() or []
    except Exception:
        headers2 = {"Authorization": f"{token}"}
        try:
            resp = get_with_retry(url, headers2, params={})
            return resp.json() or []
        except Exception:
            return []


def fetch_fbw_last_supplies(token: str, limit: int = 15) -> list[dict[str, Any]]:
    base_list = fetch_fbw_supplies_list(token)
    supplies: list[dict[str, Any]] = []
    
    # Загружаем кэшированные данные для оптимизации
    cached = load_fbw_supplies_cache() or {}
    cached_items = cached.get("items") or []
    cached_map = {}
    for item in cached_items:
        sid = str(item.get("supply_id") or item.get("supplyID") or item.get("supplyId") or item.get("id") or "")
        if sid:
            cached_map[sid] = item
    
    for it in base_list[: max(0, int(limit))]:
        supply_id = it.get("supplyID") or it.get("supplyId") or it.get("id")
        supply_id_str = str(supply_id or "")
        
        # Проверяем кэш для оптимизации
        cached_item = cached_map.get(supply_id_str)
        
        # Если поставка в кэше и статус "Принято", используем кэшированные данные
        if cached_item and "Принято" in str(cached_item.get("status", "")):
            supplies.append(cached_item)
            continue
            
        # Для остальных поставок получаем актуальные данные
        details = fetch_fbw_supply_details(token, supply_id)
        # Normalize fields; prefer details when available, fallback to list fields
        create_date = (details or {}).get("createDate") or it.get("createDate")
        supply_date = (details or {}).get("supplyDate") or it.get("supplyDate")
        fact_date = (details or {}).get("factDate") or it.get("factDate")
        status_name = (details or {}).get("statusName") or it.get("statusName")
        warehouse_name = (details or {}).get("warehouseName") or it.get("warehouseName") or ""
        box_type = (details or {}).get("boxTypeName") or (details or {}).get("boxTypeID") or ""
        total_qty = (details or {}).get("quantity")
        accepted_qty = (details or {}).get("acceptedQuantity")
        acceptance_cost = (details or {}).get("acceptanceCost")
        paid_coef = (details or {}).get("paidAcceptanceCoefficient")
        
        # Если есть кэшированное количество коробок, сохраняем его
        package_count = None
        if cached_item and "package_count" in cached_item:
            package_count = cached_item["package_count"]
        
        supply_data = {
            "supply_id": supply_id_str,
            "type": str(box_type) if box_type is not None else "",
            "created_at": _fmt_dt_moscow(create_date, with_time=False),
            "total_goods": int(total_qty) if isinstance(total_qty, (int, float)) and total_qty is not None else None,
            "accepted_goods": int(accepted_qty) if isinstance(accepted_qty, (int, float)) and accepted_qty is not None else None,
            "warehouse": warehouse_name or "",
            "acceptance_coefficient": paid_coef,
            "acceptance_cost": acceptance_cost,
            "planned_date": _fmt_dt_moscow(supply_date, with_time=False),
            "fact_date": _fmt_dt_moscow(fact_date, with_time=True),
            "status": status_name or "",
        }
        
        if package_count is not None:
            supply_data["package_count"] = package_count
            
        supplies.append(supply_data)

    return supplies


def fetch_fbw_supplies_range(token: str, offset: int, limit: int) -> list[dict[str, Any]]:
    base_list = fetch_fbw_supplies_list(token)
    if offset < 0:
        offset = 0
    end = offset + max(0, int(limit))
    slice_ids = base_list[offset:end]
    supplies: list[dict[str, Any]] = []
    
    # Загружаем кэшированные данные для оптимизации
    cached = load_fbw_supplies_cache() or {}
    cached_items = cached.get("items") or []
    cached_map = {}
    for item in cached_items:
        sid = str(item.get("supply_id") or item.get("supplyID") or item.get("supplyId") or item.get("id") or "")
        if sid:
            cached_map[sid] = item
    
    for it in slice_ids:
        supply_id = it.get("supplyID") or it.get("supplyId") or it.get("id")
        supply_id_str = str(supply_id or "")
        
        # Проверяем кэш для оптимизации
        cached_item = cached_map.get(supply_id_str)
        
        # Если поставка в кэше и статус "Принято", используем кэшированные данные
        if cached_item and "Принято" in str(cached_item.get("status", "")):
            supplies.append(cached_item)
            continue
            
        # Для остальных поставок получаем актуальные данные
        details = fetch_fbw_supply_details(token, supply_id)
        create_date = (details or {}).get("createDate") or it.get("createDate")
        supply_date = (details or {}).get("supplyDate") or it.get("supplyDate")
        fact_date = (details or {}).get("factDate") or it.get("factDate")
        status_name = (details or {}).get("statusName") or it.get("statusName")
        warehouse_name = (details or {}).get("warehouseName") or it.get("warehouseName") or ""
        box_type = (details or {}).get("boxTypeName") or (details or {}).get("boxTypeID") or ""
        total_qty = (details or {}).get("quantity")
        accepted_qty = (details or {}).get("acceptedQuantity")
        acceptance_cost = (details or {}).get("acceptanceCost")
        paid_coef = (details or {}).get("paidAcceptanceCoefficient")
        
        # Если есть кэшированное количество коробок, сохраняем его
        package_count = None
        if cached_item and "package_count" in cached_item:
            package_count = cached_item["package_count"]
        
        supply_data = {
            "supply_id": supply_id_str,
            "type": str(box_type) if box_type is not None else "",
            "created_at": _fmt_dt_moscow(create_date, with_time=False),
            "total_goods": int(total_qty) if isinstance(total_qty, (int, float)) and total_qty is not None else None,
            "accepted_goods": int(accepted_qty) if isinstance(accepted_qty, (int, float)) and accepted_qty is not None else None,
            "warehouse": warehouse_name or "",
            "acceptance_coefficient": paid_coef,
            "acceptance_cost": acceptance_cost,
            "planned_date": _fmt_dt_moscow(supply_date, with_time=False),
            "fact_date": _fmt_dt_moscow(fact_date, with_time=True),
            "status": status_name or "",
        }
        
        if package_count is not None:
            supply_data["package_count"] = package_count
            
        supplies.append(supply_data)
    return supplies


def format_money_ru(value: Any) -> str:
    try:
        return f"{float(value):,.2f}".replace(",", " ").replace(".", ",")
    except Exception:
        return str(value)


app.jinja_env.filters["num_space"] = format_int_thousands
app.jinja_env.filters["money_ru"] = format_money_ru


def format_dmy(date_str: str) -> str:
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").strftime("%d.%m.%Y")
    except Exception:
        return date_str or ""


def extract_nm(value: Any) -> str:
    try:
        import re
        s = str(value)
        m = re.search(r"(\d{7,12})", s)
        return m.group(1) if m else ""
    except Exception:
        return ""


app.jinja_env.filters["extract_nm"] = extract_nm


def days_left_from_str(date_str: str | None) -> int | None:
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


app.jinja_env.filters["days_left"] = days_left_from_str

def _merge_package_counts(items: list[dict[str, Any]], cached_items: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
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


def _preload_package_counts(token: str, supplies: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Предварительно загружает количество коробок для поставок, которые еще не имеют этой информации.
    Это позволяет кэшировать данные о коробках при обновлении списка поставок.
    """
    if not token or not supplies:
        return supplies
    
    # Находим поставки без информации о количестве коробок
    supplies_to_update = []
    for supply in supplies:
        if not supply.get("package_count") or int(supply.get("package_count") or 0) == 0:
            supplies_to_update.append(supply)
    
    if not supplies_to_update:
        return supplies
    
    # Обновляем количество коробок для найденных поставок
    updated_supplies = []
    for supply in supplies:
        if supply in supplies_to_update:
            try:
                supply_id = supply.get("supply_id") or supply.get("supplyID") or supply.get("supplyId") or supply.get("id")
                if supply_id:
                    packages = fetch_fbw_supply_packages(token, supply_id)
                    package_count = len(packages) if isinstance(packages, list) else 0
                    
                    # Создаем копию с обновленным количеством коробок
                    updated_supply = dict(supply)
                    updated_supply["package_count"] = package_count
                    updated_supplies.append(updated_supply)
                else:
                    updated_supplies.append(supply)
            except Exception:
                # В случае ошибки оставляем поставку без изменений
                updated_supplies.append(supply)
        else:
            updated_supplies.append(supply)
    
    return updated_supplies


def time_ago_ru(dt_val: Any) -> str:
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


app.jinja_env.filters["time_ago_ru"] = time_ago_ru

def _get_session_id() -> str:
    # For anonymous sessions only; with auth we key cache by user id
    sid = session.get("SID")
    if not sid:
        sid = uuid.uuid4().hex
        session["SID"] = sid
    return sid


def _cache_path_for_user() -> str:
    if current_user.is_authenticated:
        return os.path.join(CACHE_DIR, f"orders_user_{current_user.id}.json")
    return os.path.join(CACHE_DIR, f"orders_{_get_session_id()}.json")


def _cache_path_for_user_id(user_id: int) -> str:
    return os.path.join(CACHE_DIR, f"orders_user_{user_id}.json")


# Products cache helpers (per user)
def _products_cache_path_for_user() -> str:
    if current_user.is_authenticated:
        return os.path.join(CACHE_DIR, f"products_user_{current_user.id}.json")
    return os.path.join(CACHE_DIR, "products_anon.json")


def load_products_cache() -> Dict[str, Any] | None:
    path = _products_cache_path_for_user()
    if not os.path.isfile(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def save_products_cache(payload: Dict[str, Any]) -> None:
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
    if current_user.is_authenticated:
        return os.path.join(CACHE_DIR, f"stocks_user_{current_user.id}.json")
    return os.path.join(CACHE_DIR, "stocks_anon.json")


def load_stocks_cache() -> Dict[str, Any] | None:
    path = _stocks_cache_path_for_user()
    if not os.path.isfile(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def save_stocks_cache(payload: Dict[str, Any]) -> None:
    path = _stocks_cache_path_for_user()
    try:
        enriched = dict(payload)
        if current_user.is_authenticated:
            enriched["_user_id"] = current_user.id
        with open(path, "w", encoding="utf-8") as f:
            json.dump(enriched, f, ensure_ascii=False)
    except Exception:
        pass


# FBS supplies cache helpers (per user)
def _fbs_supplies_cache_path_for_user() -> str:
    if current_user.is_authenticated:
        return os.path.join(CACHE_DIR, f"fbs_supplies_user_{current_user.id}.json")
    return os.path.join(CACHE_DIR, "fbs_supplies_anon.json")


def load_fbs_supplies_cache() -> Dict[str, Any] | None:
    path = _fbs_supplies_cache_path_for_user()
    if not os.path.isfile(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def save_fbs_supplies_cache(payload: Dict[str, Any]) -> None:
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
    if current_user.is_authenticated:
        return os.path.join(CACHE_DIR, f"fbw_supplies_user_{current_user.id}.json")
    return os.path.join(CACHE_DIR, "fbw_supplies_anon.json")


def load_fbw_supplies_cache() -> Dict[str, Any] | None:
    path = _fbw_supplies_cache_path_for_user()
    if not os.path.isfile(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def save_fbw_supplies_cache(payload: Dict[str, Any]) -> None:
    path = _fbw_supplies_cache_path_for_user()
    try:
        enriched = dict(payload)
        if current_user.is_authenticated:
            enriched["_user_id"] = current_user.id
        with open(path, "w", encoding="utf-8") as f:
            json.dump(enriched, f, ensure_ascii=False)
    except Exception:
        pass


def _fbs_tasks_cache_path_for_user() -> str:
    if current_user.is_authenticated:
        return os.path.join(CACHE_DIR, f"fbs_tasks_user_{current_user.id}.json")
    return os.path.join(CACHE_DIR, "fbs_tasks_anon.json")


def load_fbs_tasks_cache() -> Dict[str, Any] | None:
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
    path = _cache_path_for_user()
    if not os.path.isfile(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def save_last_results(payload: Dict[str, Any]) -> None:
    path = _cache_path_for_user()
    try:
        enriched = dict(payload)
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


# -------------------------
# Models & Auth
# -------------------------

class User(UserMixin, db.Model):
    __tablename__ = "users"
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(120), unique=True, nullable=False)
    password = db.Column(db.String(255), nullable=False)  # store hashed in production
    is_admin = db.Column(db.Boolean, default=False)
    is_active = db.Column(db.Boolean, default=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    wb_token = db.Column(db.Text, nullable=True)
    valid_from = db.Column(db.Date, nullable=True)
    valid_to = db.Column(db.Date, nullable=True)
    phone = db.Column(db.String(64), nullable=True)
    email = db.Column(db.String(120), nullable=True)
    shipper_name = db.Column(db.String(255), nullable=True)
    shipper_address = db.Column(db.String(255), nullable=True)
    contact_person = db.Column(db.String(255), nullable=True)


class Notification(db.Model):
    __tablename__ = "notifications"
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False)
    title = db.Column(db.String(255), nullable=False)
    message = db.Column(db.Text, nullable=False)
    notification_type = db.Column(db.String(50), nullable=False)  # 'fbs_new_order', 'system', etc.
    is_read = db.Column(db.Boolean, default=False)
    created_at = db.Column(db.DateTime, default=datetime.now(MOSCOW_TZ))
    data = db.Column(db.Text, nullable=True)  # JSON data for additional info
    
    user = db.relationship('User', backref=db.backref('notifications', lazy=True))

    def get_id(self):  # type: ignore[override]
        return str(self.id)


@login_manager.user_loader
def load_user(user_id: str):
    try:
        return db.session.get(User, int(user_id))
    except Exception:
        return None


def _is_user_valid_now(u: "User") -> bool:
    if not u.is_active:
        return False
    today = datetime.now(MOSCOW_TZ).date()
    if u.valid_from and today < u.valid_from:
        return False
    if u.valid_to and today > u.valid_to:
        return False
    return True


@app.before_request
def _enforce_account_validity():
    # Allow unauthenticated pages and static assets
    endpoint = (request.endpoint or "")
    if endpoint in {"login", "logout", "favicon", "logo"} or endpoint.startswith("static"):
        return None
    if current_user.is_authenticated:
        if not _is_user_valid_now(current_user):
            logout_user()
            # For API requests return JSON 401 to avoid HTML redirect in fetch()
            if request.path.startswith("/api/"):
                return jsonify({"error": "expired"}), 401
            flash("Срок действия учётной записи истёк")
            return redirect(url_for("login"))


def parse_date(date_str: str) -> datetime:
    return datetime.strptime(date_str, "%Y-%m-%d")


def parse_wb_datetime(value: str) -> datetime | None:
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


def get_with_retry(url: str, headers: Dict[str, str], params: Dict[str, Any], max_retries: int = 8, timeout_s: int = 60) -> requests.Response:
    last_exc: Exception | None = None
    last_resp: requests.Response | None = None
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=timeout_s)
            last_resp = resp
            if resp.status_code in (429, 500, 502, 503, 504):
                retry_after = resp.headers.get("Retry-After")
                if retry_after is not None:
                    try:
                        sleep_s = float(retry_after)
                    except ValueError:
                        sleep_s = 1.0
                else:
                    sleep_s = min(15, 0.8 * (2 ** attempt) + random.uniform(0, 0.7))
                time.sleep(sleep_s)
                continue
            resp.raise_for_status()
            return resp
        except requests.RequestException as exc:  # network or HTTP error
            last_exc = exc
            time.sleep(min(8, 0.5 * (2 ** attempt) + random.uniform(0, 0.5)))
            continue
    if last_exc:
        raise last_exc
    if last_resp is not None:
        raise requests.HTTPError(f"HTTP {last_resp.status_code} after {max_retries} retries", response=last_resp)
    raise RuntimeError("Request failed after retries")


def get_with_retry_json(url: str, headers: Dict[str, str], params: Dict[str, Any], max_retries: int = 8, timeout_s: int = 60) -> Any:
    resp = get_with_retry(url, headers, params, max_retries=max_retries, timeout_s=timeout_s)
    try:
        return resp.json()
    except Exception:
        raise RuntimeError("Invalid JSON from API")


def fetch_orders_page(token: str, date_from_iso: str, flag: int = 0) -> List[Dict[str, Any]]:
    headers = {"Authorization": f"Bearer {token}"}
    params = {"dateFrom": date_from_iso, "flag": flag}
    response = get_with_retry(API_URL, headers, params)
    return response.json()


def fetch_orders_range(token: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
    """Fetch orders from WB by paginating with lastChangeDate, but filter by actual order date."""
    start_dt = parse_date(start_date)
    end_dt = parse_date(end_date)

    # Загружаем данные с запасом: начинаем за 7 дней до start_date
    # чтобы захватить заказы, которые могли быть обновлены позже
    extended_start = start_dt - timedelta(days=7)
    cursor_dt = datetime.combine(extended_start.date(), datetime.min.time())

    collected: List[Dict[str, Any]] = []
    seen_srid: set[str] = set()

    max_pages = 2000
    pages = 0

    while pages < max_pages:
        pages += 1
        page = fetch_orders_page(token, cursor_dt.strftime("%Y-%m-%dT%H:%M:%S"), flag=0)
        if not page:
            break
        try:
            page.sort(key=lambda x: parse_wb_datetime(x.get("lastChangeDate")) or datetime.min)
        except Exception:
            pass

        last_page_lcd: datetime | None = parse_wb_datetime(page[-1].get("lastChangeDate"))
        # Останавливаемся, когда lastChangeDate превышает end_date + 1 день
        page_exceeds = last_page_lcd and last_page_lcd.date() > (end_dt.date() + timedelta(days=1))

        for item in page:
            srid = str(item.get("srid", ""))
            if srid and srid in seen_srid:
                continue
            # Убираем фильтрацию по lastChangeDate здесь - будем фильтровать по date в to_rows
            if srid:
                seen_srid.add(srid)
            collected.append(item)

        if last_page_lcd is None:
            break
        cursor_dt = last_page_lcd
        if page_exceeds:
            break
        # Gentle delay between pages to avoid throttling
        time.sleep(0.2)

    return collected


def fetch_sales_page(token: str, date_from_iso: str, flag: int = 0) -> List[Dict[str, Any]]:
    headers = {"Authorization": f"Bearer {token}"}
    params = {"dateFrom": date_from_iso, "flag": flag}
    response = get_with_retry(SALES_API_URL, headers, params)
    return response.json()


def fetch_sales_range(token: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
    start_dt = parse_date(start_date)
    end_dt = parse_date(end_date)
    cursor_dt = datetime.combine(start_dt.date(), datetime.min.time())

    collected: List[Dict[str, Any]] = []
    seen_id: set[str] = set()

    max_pages = 2000
    pages = 0
    while pages < max_pages:
        pages += 1
        page = fetch_sales_page(token, cursor_dt.strftime("%Y-%m-%dT%H:%M:%S"), flag=0)
        if not page:
            break
        try:
            page.sort(key=lambda x: parse_wb_datetime(x.get("lastChangeDate")) or datetime.min)
        except Exception:
            pass

        last_page_lcd: datetime | None = parse_wb_datetime(page[-1].get("lastChangeDate"))
        page_exceeds = last_page_lcd and last_page_lcd.date() > end_dt.date()

        for item in page:
            key = str(item.get("srid")) or f"{item.get('gNumber','')}_{item.get('barcode','')}_{item.get('date','')}"
            if key and key in seen_id:
                continue
            lcd = parse_wb_datetime(item.get("lastChangeDate"))
            if lcd and lcd.date() > end_dt.date():
                continue
            if key:
                seen_id.add(key)
            collected.append(item)

        if last_page_lcd is None:
            break
        cursor_dt = last_page_lcd
        if page_exceeds:
            break
        # Gentle delay between pages
        time.sleep(0.2)

    return collected


def fetch_finance_report(token: str, date_from: str, date_to: str, limit: int = 100000) -> List[Dict[str, Any]]:
    """Fetch financial report details v5 with rrdid pagination.

    According to docs, start with rrdid=0 and then pass last row's rrd_id until empty list is returned.
    date_from must be RFC3339 in MSK; we'll accept YYYY-MM-DD and convert to T00:00:00.
    date_to is YYYY-MM-DD (end date).
    """
    headers = {"Authorization": f"Bearer {token}"}
    # Compose RFC3339-like dateFrom in MSK start of day
    try:
        df_iso = datetime.strptime(date_from, "%Y-%m-%d").strftime("%Y-%m-%dT00:00:00")
    except Exception:
        df_iso = f"{date_from}T00:00:00"
    params_base: Dict[str, Any] = {"dateFrom": df_iso, "dateTo": date_to, "limit": max(1, min(100000, int(limit)))}
    all_rows: List[Dict[str, Any]] = []
    rrdid = 0
    while True:
        params = dict(params_base)
        params["rrdid"] = rrdid
        data = get_with_retry_json(FIN_REPORT_URL, headers, params, max_retries=8, timeout_s=60)
        if not isinstance(data, list) or not data:
            break
        all_rows.extend(data)
        try:
            last = data[-1]
            rrdid = int(last.get("rrd_id") or last.get("rrdid") or last.get("rrdId") or 0)
        except Exception:
            break
        # If received less than limit rows, it's the last page — stop without extra call (avoid long 429 waits)
        try:
            if len(data) < params_base.get("limit", 100000):
                break
        except Exception:
            pass
        # Небольшая пауза между страницами (обычно не требуется, т.к. limit=100000 закрывает весь период одной страницей)
        time.sleep(1.1)
    return all_rows


def aggregate_finance_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Aggregate rows by product (nm_id + supplierArticle) to qty and revenue.
    Fields per docs: realize/prices/quantities. We'll use 'quantity' and 'retail_amount' if present.
    """
    by_key: Dict[tuple[Any, Any], Dict[str, Any]] = {}
    for r in rows:
        nm_id = r.get("nm_id") or r.get("nmId") or r.get("nm")
        prod = r.get("supplier_article") or r.get("supplierArticle") or r.get("supplierArticleName") or ""
        key = (nm_id, prod)
        item = by_key.get(key)
        qty = 0
        rev = 0.0
        # common fields in WB v5: quantity, retail_amount
        try:
            qty = int(r.get("quantity") or r.get("sale_qty") or r.get("qty") or 0)
        except Exception:
            qty = 0
        try:
            rev = float(r.get("retail_amount") or r.get("retailAmount") or r.get("sum_price") or 0)
        except Exception:
            rev = 0.0
        if item is None:
            by_key[key] = {"nm_id": nm_id, "product": prod, "qty": max(qty, 0), "revenue": max(rev, 0.0)}
        else:
            item["qty"] = int(item.get("qty", 0)) + max(qty, 0)
            item["revenue"] = float(item.get("revenue", 0.0)) + max(rev, 0.0)
    items = list(by_key.values())
    items.sort(key=lambda x: (x.get("revenue") or 0.0), reverse=True)
    return items


def to_rows(data: List[Dict[str, Any]], start_date: str, end_date: str) -> List[Dict[str, Any]]:
    """Преобразует данные заказов и фильтрует по реальной дате заказа (date), исключая отмененные."""
    start = parse_date(start_date).date()
    end = parse_date(end_date).date()

    rows: List[Dict[str, Any]] = []
    for sale in data:
        # Фильтруем по реальной дате заказа (date), а не по lastChangeDate
        date_str = str(sale.get("date", ""))[:10]
        try:
            d = datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            continue
        if not (start <= d <= end):
            continue
        # Фильтруем отмененные заказы
        is_cancelled = sale.get("isCancel")
        if is_cancelled is True or str(is_cancelled).lower() in ('true', '1', 'истина'):
            continue
        rows.append({
            "Дата": date_str,
            "Дата и время обновления информации в сервисе": sale.get("lastChangeDate"),
            "Склад отгрузки": sale.get("warehouseName"),
            "Тип склада хранения товаров": sale.get("warehouseType"),
            "Страна": sale.get("countryName"),
            "Округ": sale.get("oblastOkrugName"),
            "Регион": sale.get("regionName"),
            "Артикул продавца": sale.get("supplierArticle"),
            "Артикул WB": sale.get("nmId"),
            "Баркод": sale.get("barcode"),
            "Категория": sale.get("category"),
            "Предмет": sale.get("subject"),
            "Бренд": sale.get("brand"),
            "Размер товара": sale.get("techSize"),
            "Номер поставки": sale.get("incomeID"),
            "Договор поставки": sale.get("isSupply"),
            "Договор реализации": sale.get("isRealization"),
            "Цена без скидок": sale.get("totalPrice"),
            "Скидка продавца": sale.get("discountPercent"),
            "Скидка WB": sale.get("spp"),
            "Цена с учетом всех скидок": sale.get("finishedPrice"),
            "Цена со скидкой продавца": sale.get("priceWithDisc"),
            "Отмена заказа": sale.get("isCancel"),
            "Дата и время отмены заказа": sale.get("cancelDate"),
            "ID стикера": sale.get("sticker"),
            "Номер заказа": sale.get("gNumber"),
            "Уникальный ID заказа": sale.get("srid"),
        })
    return rows


def to_sales_rows(data: List[Dict[str, Any]], start_date: str, end_date: str) -> List[Dict[str, Any]]:
    start = parse_date(start_date).date()
    end = parse_date(end_date).date()
    rows: List[Dict[str, Any]] = []
    for sale in data:
        date_str = str(sale.get("date", ""))[:10]
        try:
            d = datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            continue
        if not (start <= d <= end):
            continue
        rows.append({
            "Дата": date_str,
            "Дата и время обновления информации в сервисе": sale.get("lastChangeDate"),
            "Склад отгрузки": sale.get("warehouseName"),
            "Артикул продавца": sale.get("supplierArticle"),
            "Артикул WB": sale.get("nmId"),
            "Баркод": sale.get("barcode"),
            "Цена с учетом всех скидок": sale.get("finishedPrice"),
        })
    return rows


def aggregate_daily(rows: List[Dict[str, Any]]):
    count_by_day: Dict[str, int] = defaultdict(int)
    revenue_by_day: Dict[str, float] = defaultdict(float)

    for r in rows:
        day = r.get("Дата")
        try:
            price = float(r.get("Цена с учетом всех скидок") or 0)
        except (TypeError, ValueError):
            price = 0.0
        count_by_day[day] += 1
        revenue_by_day[day] += price

    labels = sorted(count_by_day.keys())
    counts = [count_by_day[d] for d in labels]
    revenues = [round(revenue_by_day[d], 2) for d in labels]
    return labels, counts, revenues


def aggregate_daily_counts_and_revenue(rows: List[Dict[str, Any]]):
    count_by_day: Dict[str, int] = defaultdict(int)
    revenue_by_day: Dict[str, float] = defaultdict(float)
    for r in rows:
        day = r.get("Дата")
        try:
            price = float(r.get("Цена с учетом всех скидок") or 0)
        except (TypeError, ValueError):
            price = 0.0
        count_by_day[day] += 1
        revenue_by_day[day] += price
    return count_by_day, revenue_by_day


def build_union_series(orders_counts: Dict[str, int], sales_counts: Dict[str, int],
                       orders_rev: Dict[str, float], sales_rev: Dict[str, float]):
    labels = sorted(set(orders_counts.keys()) | set(sales_counts.keys()))
    o_counts = [orders_counts.get(d, 0) for d in labels]
    s_counts = [sales_counts.get(d, 0) for d in labels]
    o_rev = [round(orders_rev.get(d, 0.0), 2) for d in labels]
    s_rev = [round(sales_rev.get(d, 0.0), 2) for d in labels]
    return labels, o_counts, s_counts, o_rev, s_rev


def aggregate_by_warehouse(rows: List[Dict[str, Any]]) -> List[Tuple[str, int]]:
    counts: Dict[str, int] = defaultdict(int)
    for r in rows:
        warehouse = r.get("Склад отгрузки") or "Не указан"
        counts[warehouse] += 1
    return sorted(counts.items(), key=lambda kv: kv[1], reverse=True)


def aggregate_by_warehouse_dual(orders_rows: List[Dict[str, Any]], sales_rows: List[Dict[str, Any]]):
    orders_map: Dict[str, int] = defaultdict(int)
    sales_map: Dict[str, int] = defaultdict(int)
    for r in orders_rows:
        warehouse = r.get("Склад отгрузки") or "Не указан"
        orders_map[warehouse] += 1
    for r in sales_rows:
        warehouse = r.get("Склад отгрузки") or "Не указан"
        sales_map[warehouse] += 1
    all_wh = sorted(set(orders_map.keys()) | set(sales_map.keys()))
    summary = []
    for w in all_wh:
        summary.append({"warehouse": w, "orders": orders_map.get(w, 0), "sales": sales_map.get(w, 0)})
    # сортируем по заказам
    summary.sort(key=lambda x: x["orders"], reverse=True)
    return summary


def aggregate_top_products(rows: List[Dict[str, Any]], limit: int = 15) -> List[Dict[str, Any]]:
    counts: Dict[str, int] = defaultdict(int)
    revenue_by_product: Dict[str, float] = defaultdict(float)
    nm_by_product: Dict[str, Any] = {}
    barcode_by_product: Dict[str, Any] = {}
    supplier_article_by_product: Dict[str, Any] = {}
    for r in rows:
        product = r.get("Артикул продавца") or r.get("Артикул WB") or r.get("Баркод") or "Не указан"
        product = str(product)
        counts[product] += 1
        try:
            price = float(r.get("Цена с учетом всех скидок") or 0)
        except (TypeError, ValueError):
            price = 0.0
        revenue_by_product[product] += price
        nm = r.get("Артикул WB") or r.get("nmId") or r.get("nmID")
        if product not in nm_by_product and nm:
            nm_by_product[product] = nm
        barcode = r.get("Баркод")
        if product not in barcode_by_product and barcode:
            barcode_by_product[product] = barcode
        supplier_article = r.get("Артикул продавца")
        if product not in supplier_article_by_product and supplier_article:
            supplier_article_by_product[product] = supplier_article
    # photo map
    nm_to_photo: Dict[Any, Any] = {}
    try:
        prod_cached = load_products_cache() or {}
        for it in (prod_cached.get("items") or []):
            nmv = it.get("nm_id") or it.get("nmId") or it.get("nmID")
            photo = it.get("photo") or it.get("img")
            if nmv is not None and nmv not in nm_to_photo:
                nm_to_photo[nmv] = photo
    except Exception:
        nm_to_photo = {}

    items = [{
        "product": p,
        "qty": c,
        "nm_id": nm_by_product.get(p),
        "barcode": barcode_by_product.get(p),
        "supplier_article": supplier_article_by_product.get(p),
        "sum": round(revenue_by_product.get(p, 0.0), 2),
        "photo": nm_to_photo.get(nm_by_product.get(p))
    } for p, c in counts.items()]
    items.sort(key=lambda x: x["qty"], reverse=True)
    return items[:limit]


def aggregate_top_products_sales(rows: List[Dict[str, Any]], warehouse: str | None = None, limit: int = 50) -> List[Tuple[str, int]]:
    counts: Dict[str, int] = defaultdict(int)
    for r in rows:
        if warehouse and (r.get("Склад отгрузки") or "Не указан") != warehouse:
            continue
        product = r.get("Артикул продавца") or r.get("Артикул WB") or r.get("Баркод") or "Не указан"
        counts[str(product)] += 1
    return sorted(counts.items(), key=lambda kv: kv[1], reverse=True)[:limit]


def aggregate_top_products_orders(rows: List[Dict[str, Any]], warehouse: str | None = None, limit: int = 50) -> List[Dict[str, Any]]:
    counts: Dict[str, int] = defaultdict(int)
    revenue_by_product: Dict[str, float] = defaultdict(float)
    nm_by_product: Dict[str, Any] = {}
    barcode_by_product: Dict[str, Any] = {}
    supplier_article_by_product: Dict[str, Any] = {}
    for r in rows:
        if warehouse and (r.get("Склад отгрузки") or "Не указан") != warehouse:
            continue
        product = r.get("Артикул продавца") or r.get("Артикул WB") or r.get("Баркод") or "Не указан"
        product = str(product)
        counts[product] += 1
        try:
            price = float(r.get("Цена с учетом всех скидок") or 0)
        except (TypeError, ValueError):
            price = 0.0
        revenue_by_product[product] += price
        nm = r.get("Артикул WB") or r.get("nmId") or r.get("nmID")
        if product not in nm_by_product and nm:
            nm_by_product[product] = nm
        barcode = r.get("Баркод")
        if product not in barcode_by_product and barcode:
            barcode_by_product[product] = barcode
        supplier_article = r.get("Артикул продавца")
        if product not in supplier_article_by_product and supplier_article:
            supplier_article_by_product[product] = supplier_article
    # Enrich with product photos from cache
    nm_to_photo: Dict[Any, Any] = {}
    try:
        prod_cached = load_products_cache() or {}
        for it in (prod_cached.get("items") or []):
            nmv = it.get("nm_id") or it.get("nmId") or it.get("nmID")
            photo = it.get("photo") or it.get("img")
            if nmv is not None and nmv not in nm_to_photo:
                nm_to_photo[nmv] = photo
    except Exception:
        nm_to_photo = {}

    # Load stocks data for current user
    stocks_data = {}
    try:
        stocks_cached = load_stocks_cache()
        if stocks_cached and stocks_cached.get("_user_id"):
            for stock_item in stocks_cached.get("items", []):
                barcode = stock_item.get("barcode")
                stock_warehouse = stock_item.get("warehouse", "")
                qty = int(stock_item.get("qty", 0) or 0)
                
                if barcode:
                    if warehouse:
                        # Если выбран конкретный склад, суммируем только по этому складу
                        if (stock_warehouse == warehouse or 
                            (warehouse in stock_warehouse) or 
                            (stock_warehouse in warehouse)):
                            stocks_data[barcode] = stocks_data.get(barcode, 0) + qty
                    else:
                        # Если не выбран склад, суммируем по всем складам
                        stocks_data[barcode] = stocks_data.get(barcode, 0) + qty
    except Exception:
        stocks_data = {}

    items = [{
        "product": p,
        "qty": c,
        "nm_id": nm_by_product.get(p),
        "barcode": barcode_by_product.get(p),
        "supplier_article": supplier_article_by_product.get(p),
        "sum": round(revenue_by_product.get(p, 0.0), 2),
        "photo": nm_to_photo.get(nm_by_product.get(p)),
        "stock_qty": stocks_data.get(barcode_by_product.get(p), 0)
    } for p, c in counts.items()]
    items.sort(key=lambda x: x["qty"], reverse=True)
    return items[:limit]


def _extract_created_at(obj: Any) -> datetime:
    if not isinstance(obj, dict):
        return datetime.min
    val = (
        obj.get("createdAt")
        or obj.get("createAt")
        or obj.get("created")
        or obj.get("createDt")
        or obj.get("createdDt")
        or obj.get("createdDate")
        or obj.get("dateCreated")
        or obj.get("orderCreateDate")
        or obj.get("date")
        or obj.get("created_at")
        or obj.get("time")
        or ""
    )
    # Numeric timestamp support (ms or s)
    try:
        if isinstance(val, (int, float)):
            ts = float(val)
            if ts > 1e12:  # milliseconds
                return datetime.fromtimestamp(ts / 1000)
            if ts > 1e9:   # seconds
                return datetime.fromtimestamp(ts)
        s = str(val).strip()
        if s.isdigit():
            ts = float(s)
            if ts > 1e12:
                return datetime.fromtimestamp(ts / 1000)
            if ts > 1e9:
                return datetime.fromtimestamp(ts)
        dt = parse_wb_datetime(s)
        return dt or datetime.min
    except Exception:
        return datetime.min


def fetch_fbs_new_orders(token: str) -> List[Dict[str, Any]]:
    # Marketplace API expects the token without 'Bearer'
    headers = {"Authorization": f"{token}"}
    resp = get_with_retry(FBS_NEW_URL, headers, params={})
    data = resp.json()
    # Normalize to list of orders from various shapes
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        if isinstance(data.get("orders"), list):
            return data["orders"]
        inner = data.get("data")
        if isinstance(inner, list):
            return inner
        if isinstance(inner, dict) and isinstance(inner.get("orders"), list):
            return inner["orders"]
    return []


def fetch_fbs_orders(token: str, limit: int = 100, next_cursor: str | None = None) -> Dict[str, Any]:
    params: Dict[str, Any] = {"limit": limit, "next": 0 if next_cursor is None else next_cursor}
    # Try both auth styles. Some WB tenants expect bare token, другие — Bearer
    headers_list = [
        {"Authorization": f"{token}"},
        {"Authorization": f"Bearer {token}"},
    ]
    last_err: Exception | None = None
    for hdrs in headers_list:
        try:
            resp = get_with_retry(FBS_ORDERS_URL, hdrs, params=params)
            data = resp.json()
            # consider non-empty result a success
            arr = (data.get("orders") if isinstance(data, dict) else None) or []
            if isinstance(arr, list) and arr:
                return data
            # even if empty, return once tried both
            last_data = data
        except Exception as e:
            last_err = e
            continue
    if last_err:
        raise last_err
    return last_data  # type: ignore[name-defined]


def fetch_fbs_statuses(token: str, order_ids: List[int]) -> Dict[str, Any]:
    headers_list = [
        {"Authorization": f"{token}"},
        {"Authorization": f"Bearer {token}"},
    ]
    bodies = [
        {"orders": order_ids},
        {"orders": [{"id": oid} for oid in order_ids]},
    ]
    last_err: Exception | None = None
    for hdrs in headers_list:
        for body in bodies:
            try:
                resp = post_with_retry(FBS_ORDERS_STATUS_URL, hdrs, json_body=body)
                return resp.json()
            except Exception as e:
                last_err = e
                continue
    if last_err:
        raise last_err
    return {}


def fetch_fbs_latest_orders(token: str, want_count: int = 30, page_limit: int = 200, max_pages: int = 20) -> tuple[List[Dict[str, Any]], Any]:
    """Fetch multiple pages and return most recent `want_count` items by created time.

    WB API выдаёт страницы по параметру next. Первая страница (next=0) может содержать старые записи,
    поэтому идём по страницам, собираем и затем берём последние по дате.
    """
    collected: List[Dict[str, Any]] = []
    cursor: Any = 0
    pages = 0
    last_next: Any = None
    while pages < max_pages:
        page = fetch_fbs_orders(token, limit=page_limit, next_cursor=cursor)
        items, next_cursor = _normalize_fbs_orders_page(page)
        if not items:
            break
        collected.extend(items)
        pages += 1
        if not next_cursor or next_cursor == cursor:
            break
        cursor = next_cursor
        last_next = next_cursor
    try:
        collected.sort(key=_extract_created_at, reverse=True)
    except Exception:
        pass
    return collected[:want_count], last_next


def _merge_statuses_for_items(token: str, items: List[Dict[str, Any]]):
    ids: List[int] = []
    for it in items:
        oid = it.get("id") or it.get("orderId") or it.get("ID")
        try:
            if oid is not None:
                ids.append(int(oid))
        except Exception:
            continue
    if not ids:
        return
    st = fetch_fbs_statuses(token, ids[:1000])
    arr = st.get("orders") if isinstance(st, dict) else None
    if arr is None:
        arr = st.get("data") if isinstance(st, dict) else None
    if arr is None:
        arr = st if isinstance(st, list) else []
    map_st: Dict[int, Any] = {}
    if isinstance(arr, list):
        for x in arr:
            try:
                map_st[int(x.get("id") or x.get("orderId") or 0)] = x
            except Exception:
                continue
    for it in items:
        try:
            oid = int(it.get("id") or it.get("orderId") or it.get("ID") or 0)
            stx = map_st.get(oid) or {}
            # WB может возвращать разные поля для статусов
            status_val = (
                stx.get("status")
                or stx.get("supplierStatus")
                or stx.get("wbStatus")
                or stx.get("state")
            )
            status_name_val = (
                stx.get("statusName")
                or stx.get("supplierStatusName")
                or stx.get("wbStatusName")
                or stx.get("stateName")
                or status_val
            )
            if status_name_val:
                it["statusName"] = status_name_val
            if status_val:
                it["status"] = status_val
        except Exception:
            continue


def get_orders_with_status(token: str, need_count: int = 30, start_next: Any = None) -> tuple[List[Dict[str, Any]], Any]:
    collected: List[Dict[str, Any]] = []
    cursor: Any = 0 if (start_next is None or start_next == "" or start_next == "null") else start_next
    last_next: Any = None
    safety_pages = 0
    while len(collected) < need_count and safety_pages < 5:
        page = fetch_fbs_orders(token, limit=200, next_cursor=cursor)
        items, next_cursor = _normalize_fbs_orders_page(page)
        if not items:
            break
        collected.extend(items)
        last_next = next_cursor
        safety_pages += 1
        if not next_cursor or next_cursor == cursor:
            break
        cursor = next_cursor
    try:
        collected.sort(key=_extract_created_at, reverse=True)
    except Exception:
        pass
    result = collected[:need_count]
    _merge_statuses_for_items(token, result)
    return result, last_next


def _normalize_fbs_orders_page(page: Any) -> tuple[list[dict], str | None]:
    try:
        if isinstance(page, list):
            return page, None
        if isinstance(page, dict):
            items = page.get("orders") or page.get("data") or []
            if not isinstance(items, list):
                items = []
            next_cursor = page.get("next") or page.get("cursor") or None
            return items, next_cursor
    except Exception:
        pass
    return [], None

def to_fbs_rows(orders: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for o in orders:
        if not isinstance(o, dict):
            order_num = str(o)
            rows.append({
                "Номер и дата задания": f"{order_num}",
                "Наименование товара": "",
                "Стоимость товара": 0,
                "Склад": "",
            })
            continue
        # Номер задания — ID
        order_id = o.get("ID") or o.get("id") or o.get("orderId") or o.get("gNumber") or ""
        # Дата — createdAt (форматируем с временем если доступно)
        ca_raw = o.get("createdAt") or o.get("dateCreated") or o.get("date")
        ca_dt = parse_wb_datetime(str(ca_raw))
        ca_dt_msk = to_moscow(ca_dt) if ca_dt else None
        created_at = ca_dt_msk.strftime("%d.%m.%Y %H:%M") if ca_dt_msk else str(ca_raw or "")[:10]
        # Наименование — article
        article = o.get("article") or ""
        # Цена — price без двух последних нулей
        raw_price = o.get("convertedPrice")
        try:
            price_value = int(raw_price) // 100
        except Exception:
            try:
                price_value = int(float(raw_price)) // 100
            except Exception:
                price_value = 0
        # Склад — offices
        warehouse = ""
        offices = o.get("offices")
        if isinstance(offices, list) and offices:
            first = offices[0]
            if isinstance(first, dict):
                warehouse = first.get("name") or first.get("officeName") or first.get("address") or ""
            else:
                warehouse = str(first)
        elif isinstance(offices, dict):
            warehouse = offices.get("name") or offices.get("officeName") or offices.get("address") or ""
        elif isinstance(offices, str):
            warehouse = offices
        if not warehouse:
            warehouse = o.get("warehouseName") or o.get("warehouse") or o.get("warehouseId") or ""

        rows.append({
            "Номер и дата задания": f"{order_id} | {created_at}".strip(" |"),
            "Наименование товара": article,
            "Стоимость товара": price_value,
            "Цена": price_value,
            "Склад": warehouse,
            # placeholders for enrichment from products cache
            "nm_id": o.get("nmID") or o.get("nmId") or None,
            "barcode": None,
            "photo": None,
            "orderId": order_id,  # Добавляем orderId для JavaScript
        })
    return rows


def fetch_seller_info(token: str) -> Dict[str, Any] | None:
    if not token:
        return None
    # Try Bearer first
    headers1 = {"Authorization": f"Bearer {token}"}
    try:
        resp = get_with_retry(SELLER_INFO_URL, headers1, params={})
        return resp.json()
    except Exception:
        # Fallback: raw token (some WB endpoints expect without Bearer)
        headers2 = {"Authorization": f"{token}"}
        try:
            resp = get_with_retry(SELLER_INFO_URL, headers2, params={})
            return resp.json()
        except Exception:
            return None


def fetch_acceptance_coefficients(token: str) -> List[Dict[str, Any]] | None:
    if not token:
        return None
    # Try Bearer first
    headers1 = {"Authorization": f"Bearer {token}"}
    try:
        resp = get_with_retry(ACCEPT_COEFS_URL, headers1, params={})
        return resp.json()
    except Exception:
        # Fallback raw token
        headers2 = {"Authorization": f"{token}"}
        try:
            resp = get_with_retry(ACCEPT_COEFS_URL, headers2, params={})
            return resp.json()
        except Exception:
            return None


def build_acceptance_grid(items: List[Dict[str, Any]], days: int = 14):
    # Prepare date list: today + next N days
    today = datetime.now().date()
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


def fetch_fbs_warehouses(token: str) -> list[dict[str, Any]]:
    if not token:
        return []
    headers1 = {"Authorization": f"Bearer {token}"}
    try:
        resp = get_with_retry(FBS_WAREHOUSES_URL, headers1, params={})
        return resp.json() or []
    except Exception:
        headers2 = {"Authorization": f"{token}"}
        try:
            resp = get_with_retry(FBS_WAREHOUSES_URL, headers2, params={})
            return resp.json() or []
        except Exception:
            return []


def fetch_fbs_stocks_by_warehouse(token: str, warehouse_id: int, skus: list[str]) -> list[dict[str, Any]]:
    if not token or not warehouse_id or not skus:
        return []
    url = FBS_STOCKS_BY_WAREHOUSE_URL.format(warehouseId=warehouse_id)
    headers1 = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    body = {"skus": skus[:1000]}
    try:
        resp = post_with_retry(url, headers1, body)
        data = resp.json() or {}
        return data.get("stocks") or []
    except Exception:
        headers2 = {"Authorization": f"{token}", "Content-Type": "application/json"}
        resp = post_with_retry(url, headers2, body)
        data = resp.json() or {}
        return data.get("stocks") or []


def update_fbs_stocks_by_warehouse(token: str, warehouse_id: int, items: list[dict[str, Any]]) -> None:
    # items: {"sku": str, "amount": int}
    if not token or not warehouse_id or not items:
        raise ValueError("bad_args")
    url = FBS_STOCKS_BY_WAREHOUSE_URL.format(warehouseId=warehouse_id)
    
    # Try different body formats for WB API
    body_formats = [
        items[:1000],  # Direct array without wrapper
        {"stocks": items[:1000]},  # With "stocks" wrapper
        {"data": items[:1000]},  # With "data" wrapper
    ]
    
    headers1 = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    headers2 = {"Authorization": f"{token}", "Content-Type": "application/json"}
    
    print(f"Updating stocks for warehouse {warehouse_id}, items: {len(items)}")
    
    # Validate items before sending
    valid_items = []
    for item in items[:1000]:
        if isinstance(item, dict) and 'sku' in item and 'amount' in item:
            sku = str(item['sku']).strip()
            amount = int(item['amount'])
            if sku and amount >= 0:  # Only include valid items with non-negative amounts
                valid_items.append({"sku": sku, "amount": amount})
            else:
                print(f"Skipping invalid item: {item}")
        else:
            print(f"Skipping malformed item: {item}")
    
    if not valid_items:
        print("No valid items to update")
        return
    
    print(f"Valid items to update: {len(valid_items)}")
    
    # Update body formats with valid items
    body_formats = [
        valid_items,  # Direct array without wrapper
        {"stocks": valid_items},  # With "stocks" wrapper
        {"data": valid_items},  # With "data" wrapper
    ]
    
    for i, body in enumerate(body_formats):
        print(f"Trying body format {i+1}: {body}")
        
        try:
            # Try with Bearer token first
            print(f"Making PUT request to: {url}")
            print(f"Headers: {headers1}")
            resp = requests.put(url, headers=headers1, json=body, timeout=60)
            print(f"Response status: {resp.status_code}")
            if resp.status_code == 204:
                print("Successfully updated with Bearer token")
                return
            else:
                print(f"Response text: {resp.text}")
                print(f"Response headers: {dict(resp.headers)}")
                
                # Try without Bearer token
                print(f"Trying without Bearer token...")
                resp2 = requests.put(url, headers=headers2, json=body, timeout=60)
                print(f"Response status (no Bearer): {resp2.status_code}")
                if resp2.status_code == 204:
                    print("Successfully updated without Bearer token")
                    return
                else:
                    print(f"Response text (no Bearer): {resp2.text}")
                    print(f"Response headers (no Bearer): {dict(resp2.headers)}")
                    
        except Exception as e:
            print(f"Error with body format {i+1}: {e}")
            continue
    
    # If all formats failed, raise the last error
    raise requests.HTTPError("All body formats failed")


def _fbs_stock_cache_path_for_user() -> str:
    if current_user.is_authenticated:
        return os.path.join(CACHE_DIR, f"fbs_stock_user_{current_user.id}.json")
    return os.path.join(CACHE_DIR, "fbs_stock_anon.json")


def load_fbs_stock_cache() -> Dict[str, Any] | None:
    path = _fbs_stock_cache_path_for_user()
    if not os.path.isfile(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def save_fbs_stock_cache(payload: Dict[str, Any]) -> None:
    path = _fbs_stock_cache_path_for_user()
    try:
        enriched = dict(payload)
        if current_user.is_authenticated:
            enriched["_user_id"] = current_user.id
        with open(path, "w", encoding="utf-8") as f:
            json.dump(enriched, f, ensure_ascii=False)
    except Exception:
        pass


def _auto_update_settings_path_for_user() -> str:
    if current_user.is_authenticated:
        return os.path.join(CACHE_DIR, f"auto_update_settings_user_{current_user.id}.json")
    return os.path.join(CACHE_DIR, "auto_update_settings_anon.json")


# Notification system functions
def create_notification(user_id: int, title: str, message: str, notification_type: str, data: dict = None, created_at: datetime = None) -> Notification:
    """Create a new notification for a user"""
    # Используем переданное время или текущее московское время
    notification_time = created_at if created_at else datetime.now(MOSCOW_TZ)
    
    notification = Notification(
        user_id=user_id,
        title=title,
        message=message,
        notification_type=notification_type,
        data=json.dumps(data) if data else None,
        created_at=notification_time
    )
    db.session.add(notification)
    db.session.commit()
    return notification


def get_unread_notifications_count(user_id: int) -> int:
    """Get count of unread notifications for a user"""
    return Notification.query.filter_by(user_id=user_id, is_read=False).count()


def get_user_notifications(user_id: int, limit: int = 20) -> List[Notification]:
    """Get recent notifications for a user"""
    return Notification.query.filter_by(user_id=user_id)\
        .order_by(Notification.created_at.desc())\
        .limit(limit).all()


def mark_notification_as_read(notification_id: int, user_id: int) -> bool:
    """Mark a notification as read"""
    notification = Notification.query.filter_by(id=notification_id, user_id=user_id).first()
    if notification:
        notification.is_read = True
        db.session.commit()
        return True
    return False


def mark_all_notifications_as_read(user_id: int) -> int:
    """Mark all notifications as read for a user"""
    count = Notification.query.filter_by(user_id=user_id, is_read=False).update({'is_read': True})
    db.session.commit()
    return count


def cleanup_old_notifications(days: int = 30) -> int:
    """Clean up notifications older than specified days"""
    cutoff_date = datetime.now(MOSCOW_TZ) - timedelta(days=days)
    count = Notification.query.filter(Notification.created_at < cutoff_date).delete()
    db.session.commit()
    return count


def check_fbs_new_orders_for_notifications():
    """Check for new FBS orders and create notifications for all active users"""
    with app.app_context():
        try:
            # Get all active users with WB tokens
            users = User.query.filter_by(is_active=True).filter(User.wb_token.isnot(None)).all()
            
            for user in users:
                try:
                    # Get last check time from cache
                    cache_path = os.path.join(CACHE_DIR, f"fbs_notifications_user_{user.id}.json")
                    last_check = None
                    if os.path.exists(cache_path):
                        with open(cache_path, 'r', encoding='utf-8') as f:
                            cache_data = json.load(f)
                            last_check_str = cache_data.get('last_check')
                            if last_check_str:
                                last_check = datetime.fromisoformat(last_check_str.replace('Z', '+00:00'))
                    
                    # If no previous check, check last 5 minutes
                    if not last_check:
                        last_check = datetime.now(MOSCOW_TZ) - timedelta(minutes=5)
                    
                    # Fetch new orders
                    new_orders, _ = fetch_fbs_latest_orders(user.wb_token, want_count=50)
                    
                    # Filter orders created after last check
                    new_orders_since_check = []
                    for order in new_orders:
                        order_time = _parse_iso_datetime(str(order.get('createdAt', '')))
                        if order_time and order_time > last_check:
                            new_orders_since_check.append(order)
                    
                    # Create notifications for new orders
                    if new_orders_since_check:
                        for order in new_orders_since_check:
                            order_id = order.get('id', 'Unknown')
                            order_time = _parse_iso_datetime(str(order.get('createdAt', '')))
                            # Конвертируем время в московское время для корректного отображения
                            moscow_time = to_moscow(order_time) if order_time else None
                            time_str = moscow_time.strftime('%H:%M') if moscow_time else 'Unknown'
                            
                            create_notification(
                                user_id=user.id,
                                title="Новый заказ FBS",
                                message=f"Поступил новый заказ #{order_id}",
                                notification_type="fbs_new_order",
                                data={
                                    'order_id': order_id,
                                    'order_data': order,
                                    'created_at': order.get('createdAt')
                                },
                                created_at=datetime.now(MOSCOW_TZ)
                            )
                    
                    # Update last check time
                    current_time = datetime.now(MOSCOW_TZ)
                    with open(cache_path, 'w', encoding='utf-8') as f:
                        json.dump({
                            'last_check': current_time.isoformat(),
                            'checked_orders_count': len(new_orders_since_check)
                        }, f, ensure_ascii=False)
                        
                except Exception as e:
                    print(f"Error checking FBS orders for user {user.id}: {e}")
                    continue
                    
        except Exception as e:
            print(f"Error in FBS notifications check: {e}")


def check_version_updates():
    """Check for version updates and create notifications"""
    with app.app_context():
        try:
            version_file = "VERSION"
            version_cache_file = os.path.join(CACHE_DIR, "version_cache.json")
            
            # Read current version
            current_version = None
            if os.path.exists(version_file):
                try:
                    with open(version_file, 'r', encoding='utf-8') as f:
                        current_version = f.read().strip()
                except Exception as e:
                    return
            
            if not current_version:
                return
            
            # Read cached version
            cached_version = None
            if os.path.exists(version_cache_file):
                try:
                    with open(version_cache_file, 'r', encoding='utf-8') as f:
                        cache_data = json.load(f)
                        cached_version = cache_data.get('version')
                except Exception as e:
                    pass
            
            print(f"Version check: current={current_version}, cached={cached_version}")
        
            # If version changed, create notifications for all active users
            if cached_version and current_version != cached_version:
                print(f"Version changed from {cached_version} to {current_version}")
                active_users = User.query.filter_by(is_active=True).all()
                print(f"Found {len(active_users)} active users")
                
                for user in active_users:
                    # Check if user already has notification for this version
                    existing_notification = Notification.query.filter_by(
                        user_id=user.id,
                        notification_type="version_update",
                        data=json.dumps({"version": current_version})
                    ).first()
                    
                    if not existing_notification:
                        print(f"Creating version notification for user {user.id}")
                        # Создаем уведомление с точным временем обнаружения изменения версии
                        create_notification(
                            user_id=user.id,
                            title="Обновление сервиса",
                            message=f"Вышло обновление {current_version}",
                            notification_type="version_update",
                            data={"version": current_version, "previous_version": cached_version},
                            created_at=datetime.now(MOSCOW_TZ)
                        )
                    else:
                        print(f"User {user.id} already has notification for version {current_version}")
            
            # Update version cache
            try:
                cache_data = {"version": current_version, "last_check": datetime.now(MOSCOW_TZ).isoformat()}
                with open(version_cache_file, 'w', encoding='utf-8') as f:
                    json.dump(cache_data, f, ensure_ascii=False, indent=2)
            except Exception as e:
                print(f"Error updating version cache: {e}")
                
        except Exception as e:
            print(f"Error in check_version_updates: {e}")


# Global variable to track monitoring state
_monitoring_started = False

def start_notification_monitoring():
    """Start background monitoring for notifications"""
    global _monitoring_started
    
    if _monitoring_started:
        return
    
    def monitor_loop():
        while True:
            try:
                current_time = datetime.now()
                print(f"Running notification checks at {current_time.strftime('%H:%M:%S')}")
                
                check_fbs_new_orders_for_notifications()
                check_version_updates()
                
                # Clean up old notifications every hour
                if current_time.minute == 0:
                    cleanup_old_notifications()
                    
            except Exception as e:
                print(f"Error in monitoring loop: {e}")
            time.sleep(30)  # Check every 30 seconds for faster testing
    
    # Start monitoring in a separate thread
    monitor_thread = threading.Thread(target=monitor_loop, daemon=True)
    monitor_thread.start()
    _monitoring_started = True
    print("Notification monitoring started")


def load_auto_update_settings() -> Dict[str, Any]:
    path = _auto_update_settings_path_for_user()
    if not os.path.isfile(path):
        return {
            "global": {
                "url": "",
                "interval": 60,
                "enabled": False,
                "lastCheck": None,
                "history": []
            },
            "warehouses": {}
        }
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            # Migrate old format to new format
            if "url" in data and "warehouses" not in data:
                return {
                    "global": {
                        "url": data.get("url", ""),
                        "interval": data.get("interval", 60),
                        "enabled": data.get("enabled", False),
                        "lastCheck": data.get("lastCheck"),
                        "history": data.get("history", [])
                    },
                    "warehouses": {}
                }
            return data
    except Exception:
        return {
            "global": {
                "url": "",
                "interval": 60,
                "enabled": False,
                "lastCheck": None,
                "history": []
            },
            "warehouses": {}
        }


def save_auto_update_settings(settings: Dict[str, Any], user_id: int = None) -> None:
    if user_id is None:
        path = _auto_update_settings_path_for_user()
    else:
        path = os.path.join(CACHE_DIR, f"auto_update_settings_user_{user_id}.json")
    
    try:
        enriched = dict(settings)
        if user_id is not None:
            enriched["_user_id"] = user_id
        elif hasattr(current_user, 'is_authenticated') and current_user.is_authenticated:
            enriched["_user_id"] = current_user.id
        with open(path, "w", encoding="utf-8") as f:
            json.dump(enriched, f, ensure_ascii=False)
    except Exception:
        pass


def test_remote_file(url: str) -> Dict[str, Any]:
    """Test connection to remote file and return file info"""
    try:
        response = requests.head(url, timeout=30)
        response.raise_for_status()
        
        size = int(response.headers.get('content-length', 0))
        last_modified = response.headers.get('last-modified', '')
        
        return {
            "size": size,
            "lastModified": last_modified,
            "accessible": True
        }
    except Exception as e:
        return {
            "error": str(e),
            "accessible": False
        }


def download_and_process_remote_file(url: str, user_id: int) -> Dict[str, Any]:
    """Download remote file and process it for stock updates"""
    try:
        # Download file
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        
        # Parse Excel file
        import xlrd
        workbook = xlrd.open_workbook(file_contents=response.content)
        sheet = workbook.sheet_by_index(0)
        
        # Find column indices
        header_row = sheet.row_values(0)
        barcode_col = -1
        quantity_col = -1
        
        for i, cell in enumerate(header_row):
            cell_value = str(cell).strip().lower()
            if cell_value in ['баркод', 'barcode']:
                barcode_col = i
            elif cell_value in ['кол-во', 'количество', 'quantity']:
                quantity_col = i
        
        if barcode_col == -1 or quantity_col == -1:
            return {"success": False, "error": "Не найдены необходимые колонки"}
        
        # Parse data
        file_data = {}
        for row_idx in range(1, sheet.nrows):
            row = sheet.row_values(row_idx)
            if len(row) > max(barcode_col, quantity_col):
                barcode = str(row[barcode_col]).strip()
                # Remove .0 from barcode if it exists (Excel sometimes adds .0 to numbers)
                if barcode.endswith('.0'):
                    barcode = barcode[:-2]
                
                try:
                    quantity = int(float(row[quantity_col]))
                    if barcode:
                        file_data[barcode] = quantity
                except (ValueError, TypeError):
                    continue
        
        return {"success": True, "data": file_data}
        
    except Exception as e:
        return {"success": False, "error": str(e)}


def get_reserved_quantities_from_fbs_tasks(user_id: int) -> Dict[str, int]:
    """Get reserved quantities from FBS tasks for each barcode"""
    print(f"=== GET_RESERVED_QUANTITIES_FROM_FBS_TASKS CALLED ===")
    print(f"User ID: {user_id}")
    
    try:
        # Load FBS tasks cache using user_id instead of current_user
        cached_tasks = load_fbs_tasks_cache_by_user_id(user_id) or {}
        tasks_rows = cached_tasks.get("rows") or []
        
        print(f"Found {len(tasks_rows)} FBS tasks")
        
        # Debug: print first few tasks to see structure
        if tasks_rows:
            print(f"First task structure: {tasks_rows[0]}")
            print(f"All task keys: {list(tasks_rows[0].keys()) if tasks_rows else 'No tasks'}")
        
        # Aggregate reserved quantities by barcode
        reserved_quantities = {}
        
        for i, task in enumerate(tasks_rows):
            print(f"Task {i}: {task}")
            
            barcode = task.get("barcode")
            if not barcode:
                print(f"Task {i}: No barcode found, skipping")
                continue
                
            print(f"Task {i}: Found barcode {barcode}")
                
            # Get quantity from task (usually 1 per task, but could be more)
            quantity = 1  # Default quantity per task
            if "Количество" in task:
                try:
                    quantity = int(task["Количество"])
                except (ValueError, TypeError):
                    quantity = 1
            
            # Add to reserved quantities
            if barcode in reserved_quantities:
                reserved_quantities[barcode] += quantity
            else:
                reserved_quantities[barcode] = quantity
            
            print(f"Task {i}: Added {quantity} to reserved for barcode {barcode}")
        
        print(f"Reserved quantities: {reserved_quantities}")
        return reserved_quantities
        
    except Exception as e:
        print(f"Error getting reserved quantities from FBS tasks: {e}")
        import traceback
        traceback.print_exc()
        return {}


def adjust_stock_quantities_for_reserved(file_data: Dict[str, int], user_id: int) -> Dict[str, int]:
    """Adjust stock quantities by subtracting reserved quantities from FBS tasks"""
    print(f"=== ADJUST_STOCK_QUANTITIES_FOR_RESERVED CALLED ===")
    print(f"Original file data: {file_data}")
    print(f"User ID: {user_id}")
    
    # Get reserved quantities from FBS tasks
    reserved_quantities = get_reserved_quantities_from_fbs_tasks(user_id)
    print(f"Reserved quantities returned: {reserved_quantities}")
    
    # Check if we have any reserved quantities
    if not reserved_quantities:
        print("WARNING: No reserved quantities found from FBS tasks!")
        return file_data
    
    # Adjust quantities
    adjusted_data = {}
    for barcode, quantity in file_data.items():
        reserved = reserved_quantities.get(barcode, 0)
        adjusted_quantity = max(0, quantity - reserved)  # Don't go below 0
        
        if reserved > 0:
            print(f"Barcode {barcode}: original={quantity}, reserved={reserved}, adjusted={adjusted_quantity}")
        else:
            print(f"Barcode {barcode}: original={quantity}, no reservations")
        
        adjusted_data[barcode] = adjusted_quantity
    
    print(f"Adjusted data: {adjusted_data}")
    return adjusted_data


def update_stocks_from_remote_data(file_data: Dict[str, int], user_id: int, enabled_warehouse_ids: list = None) -> int:
    """Update stocks in the cache based on remote file data"""
    print(f"=== UPDATE_STOCKS_FROM_REMOTE_DATA CALLED ===")
    print(f"File data: {file_data}")
    print(f"User ID: {user_id}")
    print(f"Enabled warehouse IDs: {enabled_warehouse_ids}")
    
    # Adjust stock quantities by subtracting reserved quantities from FBS tasks
    adjusted_file_data = adjust_stock_quantities_for_reserved(file_data, user_id)
    print(f"Using adjusted file data: {adjusted_file_data}")
    
    try:
        # Get user token from database
        from app import app, User
        
        with app.app_context():
            print("Getting user from database...")
            user = User.query.get(user_id)
            print(f"User found: {user}")
            
            if not user or not user.wb_token:
                print(f"No token found for user {user_id}")
                return 0
            
            token = user.wb_token
            print(f"Token found: {token[:10]}...")
            
            # Get warehouses
            print("Fetching warehouses...")
            warehouses = fetch_fbs_warehouses(token)
            print(f"Warehouses found: {warehouses}")
            
            if not warehouses:
                print("No warehouses found")
                return 0
            
            total_updated = 0
            
            # Update stocks for each warehouse
            for i, warehouse in enumerate(warehouses):
                warehouse_id = warehouse.get("id") or warehouse.get("warehouseId") or warehouse.get("warehouseID")
                
                if not warehouse_id:
                    print(f"No warehouse ID found for warehouse: {warehouse}")
                    continue
                
                # Skip warehouse if not in enabled list
                if enabled_warehouse_ids and warehouse_id not in enabled_warehouse_ids:
                    print(f"Warehouse {warehouse_id} not enabled for auto-update, skipping")
                    continue
                
                print(f"Processing warehouse {i+1}/{len(warehouses)}: {warehouse}")
                print(f"Warehouse ID: {warehouse_id}")
                
                # Get current stocks for this warehouse to check which SKUs are valid
                try:
                    print(f"Getting current stocks for warehouse {warehouse_id}...")
                    current_stocks = fetch_fbs_stocks_by_warehouse(token, int(warehouse_id), list(adjusted_file_data.keys()))
                    print(f"Current stocks: {current_stocks}")
                    
                    # Filter file data to only include SKUs that exist in this warehouse
                    valid_skus = {stock.get('sku') for stock in current_stocks if stock.get('sku')}
                    print(f"Valid SKUs for warehouse {warehouse_id}: {valid_skus}")
                    
                    # Prepare stock updates only for valid SKUs
                    stock_updates = []
                    for barcode, quantity in adjusted_file_data.items():
                        if barcode in valid_skus:
                            # Filter out negative quantities as WB API doesn't accept them
                            if quantity >= 0:
                                stock_updates.append({"sku": barcode, "amount": quantity})
                            else:
                                print(f"SKU {barcode} has negative quantity {quantity}, setting to 0")
                                stock_updates.append({"sku": barcode, "amount": 0})
                        else:
                            print(f"SKU {barcode} not found in warehouse {warehouse_id}, skipping")
                    
                    print(f"Prepared {len(stock_updates)} stock updates for warehouse {warehouse_id}")
                    
                    if not stock_updates:
                        print(f"No valid stock updates for warehouse {warehouse_id}")
                        continue
                    
                    # Update stocks via WB API for this warehouse
                    print(f"Updating {len(stock_updates)} stocks for warehouse {warehouse_id}")
                    update_fbs_stocks_by_warehouse(token, int(warehouse_id), stock_updates)
                    total_updated += len(stock_updates)
                    print(f"Successfully updated {len(stock_updates)} stocks for warehouse {warehouse_id}")
                    
                    # Add delay between requests to avoid rate limiting
                    if i < len(warehouses) - 1:  # Don't delay after the last warehouse
                        print("Waiting 2 seconds before next warehouse...")
                        time.sleep(2)
                        
                except Exception as e:
                    print(f"Error updating stocks for warehouse {warehouse_id}: {e}")
                    import traceback
                    traceback.print_exc()
                    continue
            
            print(f"Total updated {total_updated} stocks for user {user_id}")
            return total_updated
        
    except Exception as e:
        print(f"Error updating stocks from remote data: {e}")
        import traceback
        traceback.print_exc()
        return 0


def auto_update_worker():
    """Background worker for automatic stock updates"""
    while True:
        try:
            # Get all user settings
            settings_files = [f for f in os.listdir(CACHE_DIR) if f.startswith('auto_update_settings_user_')]
            
            for settings_file in settings_files:
                try:
                    user_id = int(settings_file.replace('auto_update_settings_user_', '').replace('.json', ''))
                    settings_path = os.path.join(CACHE_DIR, settings_file)
                    
                    with open(settings_path, 'r', encoding='utf-8') as f:
                        settings = json.load(f)
                    
                    global_settings = settings.get('global', settings)  # Support old format
                    if not global_settings.get('enabled'):
                        continue
                    
                    # Check if it's time to check
                    last_check = global_settings.get('lastCheck')
                    interval_minutes = global_settings.get('interval', 60)
                    
                    if last_check:
                        last_check_time = datetime.fromisoformat(last_check)
                        if datetime.now() - last_check_time < timedelta(minutes=interval_minutes):
                            continue
                    
                    # Get enabled warehouses with their URLs
                    enabled_warehouses = []
                    for warehouse_id, warehouse_settings in settings.get('warehouses', {}).items():
                        if warehouse_settings.get('enabled') and warehouse_settings.get('url'):
                            enabled_warehouses.append({
                                'warehouseId': warehouse_id,
                                'url': warehouse_settings['url']
                            })
                    
                    if not enabled_warehouses:
                        print(f"No enabled warehouses with URLs for user {user_id}")
                        continue
                    
                    print(f"Auto update for user {user_id}, processing {len(enabled_warehouses)} warehouses")
                    
                    total_processed = 0
                    total_updated = 0
                    all_success = True
                    
                    # Process each warehouse individually
                    for warehouse in enabled_warehouses:
                        warehouse_id = warehouse['warehouseId']
                        url = warehouse['url']
                        
                        print(f"Processing warehouse {warehouse_id} with URL: {url}")
                        
                        # Test file
                        file_info = test_remote_file(url)
                        if not file_info.get('accessible'):
                            print(f"File not accessible for warehouse {warehouse_id}: {file_info.get('error', 'Unknown error')}")
                            all_success = False
                            continue
                        
                        current_size = file_info.get('size', 0)
                        current_modified = file_info.get('lastModified', '')
                        last_size = settings.get('warehouses', {}).get(warehouse_id, {}).get('lastFileSize', 0)
                        last_modified = settings.get('warehouses', {}).get(warehouse_id, {}).get('lastFileModified', '')
                        
                        print(f"File check for warehouse {warehouse_id}:")
                        print(f"  Current: size={current_size}, modified={current_modified}")
                        print(f"  Last: size={last_size}, modified={last_modified}")
                        
                        # Check if file has changed
                        if current_size == last_size and current_modified == last_modified:
                            print(f"File unchanged for warehouse {warehouse_id}, skipping update")
                            continue
                        
                        print(f"File changed for warehouse {warehouse_id}, processing...")
                        
                        # Download and process file
                        result = download_and_process_remote_file(url, user_id)
                        
                        if result['success']:
                            print(f"File data for warehouse {warehouse_id}: {len(result['data'])} items")
                            
                            # Update stocks for this specific warehouse
                            updated_count = update_stocks_from_remote_data(result['data'], user_id, [int(warehouse_id)])
                            print(f"Updated count for warehouse {warehouse_id}: {updated_count}")
                            
                            total_processed += len(result['data'])
                            total_updated += updated_count
                            
                            # Update warehouse-specific file info
                            if 'warehouses' not in settings:
                                settings['warehouses'] = {}
                            if warehouse_id not in settings['warehouses']:
                                settings['warehouses'][warehouse_id] = {}
                            settings['warehouses'][warehouse_id]['lastFileSize'] = current_size
                            settings['warehouses'][warehouse_id]['lastFileModified'] = current_modified
                        else:
                            print(f"File processing failed for warehouse {warehouse_id}: {result['error']}")
                            all_success = False
                    
                    # Update settings
                    global_settings['lastCheck'] = datetime.now().isoformat()
                    global_settings['history'].insert(0, {
                        "timestamp": datetime.now().isoformat(),
                        "success": all_success,
                        "message": f"Автообновление: обработано {total_processed} товаров, обновлено {total_updated} остатков (с учетом FBS заданий)"
                    })
                    global_settings['history'] = global_settings['history'][:50]  # Keep last 50 entries
                    save_auto_update_settings(settings, user_id)
                    
                except Exception as e:
                    print(f"Error processing auto-update for user {user_id}: {e}")
                    continue
            
            # Sleep for 1 minute before next check
            time.sleep(60)
            
        except Exception as e:
            print(f"Error in auto-update worker: {e}")
            time.sleep(60)


# Start auto-update worker in background
auto_update_thread = threading.Thread(target=auto_update_worker, daemon=True)
auto_update_thread.start()

@app.route("/", methods=["GET", "POST"]) 
def root():
    if request.method == "POST":
        return redirect(url_for("index"), code=307)
    return redirect(url_for("index"))


@app.route("/orders", methods=["GET", "POST"]) 
@login_required
def index():
    error = None
    # Orders
    orders = []
    total_orders = 0
    total_revenue = 0.0
    # Sales
    sales_rows = []
    total_sales = 0
    total_sales_revenue = 0.0

    # Chart series
    daily_labels: List[str] = []
    daily_orders_counts: List[int] = []
    daily_sales_counts: List[int] = []
    daily_orders_revenue: List[float] = []
    daily_sales_revenue: List[float] = []

    # Warehouses combined
    warehouse_summary_dual: List[Dict[str, Any]] = []

    # TOPs
    top_products: List[Tuple[str, int]] = []  # by orders (existing)
    top_products_orders_filtered: List[Tuple[str, int]] = []  # by orders and warehouse filter
    warehouses: List[str] = []
    selected_warehouse: str = request.args.get("warehouse", "")

    updated_at: str = ""
    date_from = request.form.get("date_from", "")
    date_to = request.form.get("date_to", "")
    include_orders = True
    include_sales = True
    if request.method == "POST":
        include_orders = request.form.get("include_orders") is not None
        include_sales = request.form.get("include_sales") is not None
    date_from_fmt = format_dmy(date_from)
    date_to_fmt = format_dmy(date_to)

    # Токен: берём из формы, иначе из профиля пользователя
    token = (request.form.get("token", "").strip() or (current_user.wb_token if current_user.is_authenticated else ""))

    # Если GET — пробуем показать последние результаты из кэша
    top_mode = "orders"
    if request.method == "GET":
        cached = load_last_results()
        # Use cache only if it belongs to this user (by user_id) and user has token
        if cached and current_user.is_authenticated and cached.get("_user_id") == current_user.id and (current_user.wb_token or ""):
            date_from = cached.get("date_from", date_from)
            date_to = cached.get("date_to", date_to)
            date_from_fmt = format_dmy(date_from)
            date_to_fmt = format_dmy(date_to)
            orders = cached.get("orders", [])
            total_orders = cached.get("total_orders", 0)
            total_revenue = cached.get("total_revenue", 0.0)
            top_products = cached.get("top_products", [])
            # sales & charts
            sales_rows = cached.get("sales_rows", [])
            total_sales = cached.get("total_sales", 0)
            total_sales_revenue = cached.get("total_sales_revenue", 0.0)
            daily_labels = cached.get("daily_labels", [])
            daily_orders_counts = cached.get("daily_orders_counts", [])
            daily_sales_counts = cached.get("daily_sales_counts", [])
            daily_orders_revenue = cached.get("daily_orders_revenue", [])
            daily_sales_revenue = cached.get("daily_sales_revenue", [])
            warehouse_summary_dual = cached.get("warehouse_summary_dual", [])
            updated_at = cached.get("updated_at", "")
            # default mode when loading cache
            top_mode = cached.get("top_mode", "orders")
            # restore include flags if present
            include_orders = cached.get("include_orders", include_orders)
            include_sales = cached.get("include_sales", include_sales)

    if request.method == "POST":
        if not token:
            error = "Укажите токен API"
        elif not date_from or not date_to:
            error = "Выберите даты"
        else:
            try:
                df = parse_date(date_from)
                dt = parse_date(date_to)
                # normalize inverted range
                if df > dt:
                    date_from, date_to = date_to, date_from
            except ValueError:
                error = "Неверный формат дат"

        if not error:
            try:
                # Orders
                if include_orders:
                    raw_orders = fetch_orders_range(token, date_from, date_to)
                    orders = to_rows(raw_orders, date_from, date_to)
                    total_orders = len(orders)
                    total_revenue = round(sum(float(o.get("Цена с учетом всех скидок") or 0) for o in orders), 2)
                else:
                    orders = []
                    total_orders = 0
                    total_revenue = 0.0
                # Sales
                if include_sales:
                    raw_sales = fetch_sales_range(token, date_from, date_to)
                    sales_rows = to_sales_rows(raw_sales, date_from, date_to)
                    total_sales = len(sales_rows)
                    total_sales_revenue = round(sum(float(o.get("Цена с учетом всех скидок") or 0) for o in sales_rows), 2)
                else:
                    sales_rows = []
                    total_sales = 0
                    total_sales_revenue = 0.0

                # Aggregates for charts
                o_counts_map, o_rev_map = aggregate_daily_counts_and_revenue(orders) if include_orders else ({}, {})
                s_counts_map, s_rev_map = aggregate_daily_counts_and_revenue(sales_rows) if include_sales else ({}, {})
                daily_labels, daily_orders_counts, daily_sales_counts, daily_orders_revenue, daily_sales_revenue = build_union_series(
                    o_counts_map, s_counts_map, o_rev_map, s_rev_map
                )

                # Warehouses combined summary
                warehouse_summary_dual = aggregate_by_warehouse_dual(orders, sales_rows)

                # Top products (by orders)
                # Top block uses orders by default; if orders disabled — use sales rows
                top_mode = "orders"
                if include_orders:
                    top_products = aggregate_top_products(orders, limit=15)
                elif include_sales:
                    top_products = aggregate_top_products(sales_rows, limit=15)
                    top_mode = "sales"
                else:
                    top_products = []
                    top_mode = "orders"

                # Сохраняем токен в профиле пользователя при наличии
                if current_user.is_authenticated and token:
                    try:
                        current_user.wb_token = token
                        db.session.commit()
                    except Exception:
                        db.session.rollback()
                updated_at = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
                date_from_fmt = format_dmy(date_from)
                date_to_fmt = format_dmy(date_to)
                save_last_results({
                    "date_from": date_from,
                    "date_to": date_to,
                    "orders": orders,
                    "total_orders": total_orders,
                    "total_revenue": total_revenue,
                    "sales_rows": sales_rows,
                    "total_sales": total_sales,
                    "total_sales_revenue": total_sales_revenue,
                    "daily_labels": daily_labels,
                    "daily_orders_counts": daily_orders_counts,
                    "daily_sales_counts": daily_sales_counts,
                    "daily_orders_revenue": daily_orders_revenue,
                    "daily_sales_revenue": daily_sales_revenue,
                    "warehouse_summary_dual": warehouse_summary_dual,
                    "top_products": top_products,
                    "top_mode": top_mode,
                    "include_orders": include_orders,
                    "include_sales": include_sales,
                    "updated_at": updated_at,
                })
            except requests.HTTPError as http_err:
                error = f"Ошибка API: {http_err.response.status_code}"
            except Exception as exc:  # noqa: BLE001
                error = f"Ошибка: {exc}"

    # Build warehouses list and filtered ORDERS TOP from current orders
    warehouses = sorted({(r.get("Склад отгрузки") or "Не указан") for r in orders})
    top_products_orders_filtered = aggregate_top_products_orders(
        orders, selected_warehouse or None, limit=50
    )

    return render_template(
        "index.html",
        error=error,
        token=token,
        date_from=date_from,
        date_to=date_to,
        date_from_fmt=date_from_fmt,
        date_to_fmt=date_to_fmt,
        # Orders table remains orders-only
        orders=orders,
        # KPIs
        total_orders=total_orders,
        total_revenue=total_revenue,
        updated_at=updated_at,
        # Sales KPIs
        total_sales=total_sales,
        total_sales_revenue=total_sales_revenue,
        # Charts
        daily_labels=daily_labels,
        daily_orders_counts=daily_orders_counts,
        daily_sales_counts=daily_sales_counts,
        daily_orders_revenue=daily_orders_revenue,
        daily_sales_revenue=daily_sales_revenue,
        # Warehouses dual
        warehouse_summary_dual=warehouse_summary_dual,
        # TOPs
        top_products=top_products,
        warehouses=warehouses,
        selected_warehouse=selected_warehouse,
        top_products_orders_filtered=top_products_orders_filtered,
        include_orders=include_orders,
        include_sales=include_sales,
        top_mode=top_mode,
    )


@app.route("/fbw", methods=["GET"]) 
@login_required
def fbw_supplies_page():
    token = (current_user.wb_token or "") if current_user.is_authenticated else ""
    error = None
    supplies: list[dict[str, Any]] = []
    generated_at = ""
    # Load from cache first; only refresh by button
    cached = load_fbw_supplies_cache() or {}
    if cached and cached.get("_user_id") == (current_user.id if current_user.is_authenticated else None):
        supplies = cached.get("items", [])
        generated_at = cached.get("updated_at", "")
    if not supplies:
        if token:
            try:
                # On first ever load: fetch first 15 and cache them
                supplies = fetch_fbw_last_supplies(token, limit=15)
                generated_at = datetime.now(MOSCOW_TZ).strftime("%d.%m.%Y %H:%M")
                save_fbw_supplies_cache({"items": supplies, "updated_at": generated_at, "next_offset": 15})
            except requests.HTTPError as http_err:
                error = f"Ошибка API: {http_err.response.status_code}"
            except Exception as exc:  # noqa: BLE001
                error = f"Ошибка: {exc}"
        else:
            error = "Укажите API токен в профиле"

    return render_template(
        "fbw_supplies.html",
        error=error,
        supplies=supplies,
        generated_at=generated_at,
    )


@app.route("/fbw/planning", methods=["GET"])
@login_required
def fbw_planning_page():
    """Страница планирования поставки FBW"""
    token = (current_user.wb_token or "") if current_user.is_authenticated else ""
    error = None
    
    if not token:
        error = "Укажите API токен в профиле"
    
    return render_template(
        "fbw_planning.html",
        error=error,
        token=token
    )


@app.route("/api/fbw/planning/products", methods=["GET"])
@login_required
def api_fbw_planning_products():
    """API для получения списка товаров с баркодами для планирования поставки"""
    token = (current_user.wb_token or "") if current_user.is_authenticated else ""
    if not token:
        return jsonify({"error": "no_token"}), 401
    
    try:
        # Всегда загружаем свежие данные для планирования
        # Загружаем все товары
        raw_cards = fetch_all_cards(token, page_limit=100)
        products = normalize_cards_response({"cards": raw_cards})
        save_products_cache({"items": products, "_user_id": current_user.id})
        
        # Формируем список товаров с баркодами - используем ту же логику что и на странице /products
        products_with_barcodes = []
        for product in products:
            # Получаем баркод точно так же как на странице /products
            barcode = product.get("barcode")
            
            # Если есть баркод, добавляем товар
            if barcode:
                products_with_barcodes.append({
                    "barcode": str(barcode),
                    "name": product.get("supplier_article") or "Без артикула",  # Используем supplier_article как название
                    "nm_id": product.get("nm_id"),
                    "supplier_article": product.get("supplier_article") or "Без артикула",
                    "photo": product.get("photo")
                })
        
        return jsonify({
            "success": True,
            "products": products_with_barcodes,
            "count": len(products_with_barcodes)
        })
        
    except requests.HTTPError as http_err:
        return jsonify({"error": "api_error", "message": f"Ошибка API: {http_err.response.status_code}"}), 502
    except Exception as exc:
        return jsonify({"error": "server_error", "message": f"Ошибка: {str(exc)}"}), 500


@app.route("/api/fbw/warehouses", methods=["GET"])
@login_required
def api_fbw_warehouses():
    """API для получения списка складов FBW"""
    token = (current_user.wb_token or "") if current_user.is_authenticated else ""
    if not token:
        return jsonify({"error": "no_token"}), 401
    
    try:
        # Запрос к API Wildberries для получения списка складов
        headers = {
            "Authorization": token,
            "Content-Type": "application/json"
        }
        
        response = requests.get(
            "https://supplies-api.wildberries.ru/api/v1/warehouses",
            headers=headers,
            timeout=30
        )
        response.raise_for_status()
        
        warehouses_data = response.json()
        
        # Обрабатываем данные складов
        warehouses = []
        print(f"DEBUG: Получены данные складов: {len(warehouses_data)} элементов")
        
        for warehouse in warehouses_data:
            warehouse_id = warehouse.get("ID")
            warehouse_name = warehouse.get("name")
            
            # Проверяем, что у нас есть основные данные
            if warehouse_id and warehouse_name:
                warehouses.append({
                    "id": warehouse_id,
                    "name": warehouse_name,
                    "city": warehouse.get("city", ""),
                    "address": warehouse.get("address", ""),
                    "is_sorting_center": warehouse.get("isSortingCenter", False)
                })
            else:
                print(f"DEBUG: Пропущен склад - ID: {warehouse_id}, Name: {warehouse_name}")
        
        print(f"DEBUG: Обработано складов: {len(warehouses)}")
        
        return jsonify({
            "success": True,
            "warehouses": warehouses,
            "count": len(warehouses)
        })
        
    except requests.HTTPError as http_err:
        return jsonify({"error": "api_error", "message": f"Ошибка API: {http_err.response.status_code}"}), 502
    except Exception as exc:
        return jsonify({"error": "server_error", "message": f"Ошибка: {str(exc)}"}), 500

@app.route("/api/fbw/planning/stocks", methods=["GET"])
@login_required
def api_fbw_planning_stocks():
    """API для получения остатков товаров по выбранному складу для планирования поставки"""
    token = (current_user.wb_token or "") if current_user.is_authenticated else ""
    warehouse_id = request.args.get("warehouse_id")
    
    if not token:
        return jsonify({"error": "no_token"}), 401
    if not warehouse_id:
        return jsonify({"error": "no_warehouse", "message": "Не указан ID склада"}), 400
    
    try:
        from datetime import datetime
        # Проверяем кэш остатков перед принудительным обновлением
        cached = load_stocks_cache()
        should_refresh = True
        
        if cached and cached.get("_user_id") == current_user.id:
            # Проверяем, когда последний раз обновлялись остатки
            updated_at = cached.get("updated_at")
            if updated_at:
                try:
                    # Парсим время обновления из кэша
                    cache_time = datetime.strptime(updated_at, "%d.%m.%Y %H:%M:%S")
                    # Если остатки обновлялись менее 5 минут назад, используем кэш
                    if (datetime.now() - cache_time).total_seconds() < 300:  # 5 минут
                        should_refresh = False
                        print(f"=== ПЛАНИРОВАНИЕ ПОСТАВКИ: Используем кэшированные остатки ===")
                        print(f"Кэш обновлен: {updated_at}")
                except Exception as e:
                    print(f"Ошибка парсинга времени кэша: {e}")
        
        if should_refresh:
            print("=== ПЛАНИРОВАНИЕ ПОСТАВКИ: Принудительное обновление остатков ===")
            print(f"Пользователь: {current_user.id}, Склад: {warehouse_id}")
            try:
                raw_stocks = fetch_stocks_resilient(token)
                stocks = normalize_stocks(raw_stocks)
                now_str = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
                save_stocks_cache({"items": stocks, "_user_id": current_user.id, "updated_at": now_str})
                print(f"Остатки обновлены для планирования поставки: {len(stocks)} товаров в {now_str}")
            except requests.HTTPError as e:
                if e.response and e.response.status_code == 429:
                    print("=== ПЛАНИРОВАНИЕ ПОСТАВКИ: Ошибка 429, используем кэш ===")
                    if cached and cached.get("_user_id") == current_user.id:
                        stocks = cached.get("items", [])
                        print(f"Используем кэшированные остатки: {len(stocks)} товаров")
                    else:
                        return jsonify({"error": "rate_limit", "message": "Превышен лимит запросов к API. Попробуйте позже."}), 429
                else:
                    raise
        else:
            stocks = cached.get("items", []) if cached else []
            print(f"Используем кэшированные остатки: {len(stocks)} товаров")
            
        # Проверяем, что у нас есть остатки
        if not stocks:
            return jsonify({"error": "no_stocks", "message": "Нет данных об остатках. Попробуйте позже."}), 500
        
        # Получаем название склада из API складов
        warehouse_name = None
        try:
            # Загружаем список складов для получения названия
            warehouses_response = requests.get(
                "https://supplies-api.wildberries.ru/api/v1/warehouses",
                headers={"Authorization": token, "Content-Type": "application/json"},
                timeout=30
            )
            if warehouses_response.status_code == 200:
                warehouses_data = warehouses_response.json()
                for warehouse in warehouses_data:
                    if str(warehouse.get("ID")) == str(warehouse_id):
                        warehouse_name = warehouse.get("name")
                        break
        except Exception as e:
            print(f"Ошибка получения названия склада: {e}")
        
        # Fallback на ID если название не найдено
        if not warehouse_name:
            warehouse_name = f"Склад {warehouse_id}"
        
        # Отладочная информация - посмотрим, какие склады есть в данных
        unique_warehouses = set()
        for stock in stocks:
            warehouse = stock.get("warehouse", "")
            if warehouse:
                unique_warehouses.add(warehouse)
        
        print(f"=== DEBUG: Доступные склады в данных остатков ===")
        for wh in sorted(unique_warehouses):
            print(f"  - '{wh}'")
        print(f"Ищем склад: '{warehouse_name}' (ID: {warehouse_id})")
        
        # Фильтруем остатки по выбранному складу или по всем складам
        warehouse_stocks = {}
        for stock in stocks:
            stock_warehouse = stock.get("warehouse", "")
            barcode = stock.get("barcode")
            
            if barcode:
                # Если запрашиваем остатки по всем складам
                if warehouse_id == "all":
                    # Суммируем остатки по баркоду на всех складах
                    if barcode in warehouse_stocks:
                        warehouse_stocks[barcode] += int(stock.get("qty", 0) or 0)
                    else:
                        warehouse_stocks[barcode] = int(stock.get("qty", 0) or 0)
                else:
                    # Сравниваем по названию склада (точное совпадение или частичное)
                    if (stock_warehouse == warehouse_name or 
                        (warehouse_name in stock_warehouse) or 
                        (stock_warehouse in warehouse_name)):
                        # Суммируем остатки по баркоду на этом складе
                        if barcode in warehouse_stocks:
                            warehouse_stocks[barcode] += int(stock.get("qty", 0) or 0)
                        else:
                            warehouse_stocks[barcode] = int(stock.get("qty", 0) or 0)
        
        # Получаем время обновления из кэша или используем текущее время
        now_str = cached.get("updated_at") if cached else datetime.now().strftime("%d.%m.%Y %H:%M:%S")
        
        if warehouse_id == "all":
            print(f"Найдено остатков по всем складам: {len(warehouse_stocks)}")
            return jsonify({
                "success": True,
                "stocks": warehouse_stocks,
                "warehouse_id": "all",
                "warehouse_name": "Все склады",
                "updated_at": now_str
            })
        else:
            print(f"Найдено остатков для склада '{warehouse_name}': {len(warehouse_stocks)}")
            return jsonify({
                "success": True,
                "stocks": warehouse_stocks,
                "warehouse_id": warehouse_id,
                "warehouse_name": warehouse_name,
                "updated_at": now_str
            })
        
    except requests.HTTPError as http_err:
        print(f"=== ОШИБКА API в api_fbw_planning_stocks ===")
        print(f"HTTP Error: {http_err.response.status_code}")
        print(f"Response: {http_err.response.text}")
        return jsonify({"error": "api_error", "message": f"Ошибка API: {http_err.response.status_code}"}), 502
    except Exception as exc:
        print(f"=== ОШИБКА в api_fbw_planning_stocks ===")
        print(f"Exception: {str(exc)}")
        print(f"Exception type: {type(exc)}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": "server_error", "message": f"Ошибка: {str(exc)}"}), 500

@app.route("/api/fbw/planning/orders", methods=["GET"])
@login_required
def api_fbw_planning_orders():
    """API для получения заказов по складу за период для планирования"""
    warehouse_id = request.args.get('warehouse_id')
    date_from = request.args.get('date_from')
    date_to = request.args.get('date_to')
    
    if not warehouse_id or not date_from or not date_to:
        return jsonify({"error": "missing_parameters"}), 400
    
    token = (current_user.wb_token or "") if current_user.is_authenticated else ""
    if not token:
        return jsonify({"error": "no_token"}), 401
    
    try:
        # Получаем название склада из API складов
        warehouse_name = None
        try:
            warehouses_response = requests.get(
                "https://supplies-api.wildberries.ru/api/v1/warehouses",
                headers={"Authorization": token, "Content-Type": "application/json"},
                timeout=30
            )
            if warehouses_response.status_code == 200:
                warehouses_data = warehouses_response.json()
                for warehouse in warehouses_data:
                    if str(warehouse.get("ID")) == str(warehouse_id):
                        warehouse_name = warehouse.get("name")
                        break
        except Exception as e:
            print(f"Ошибка получения названия склада: {e}")
        
        if not warehouse_name:
            warehouse_name = f"Склад {warehouse_id}"
        
        print(f"Загружаем заказы для склада: '{warehouse_name}' за период {date_from} - {date_to}")
        
        # Загружаем заказы через API Wildberries
        headers = {
            "Authorization": token,
            "Content-Type": "application/json"
        }
        
        # Конвертируем даты в формат RFC3339 для API
        from datetime import datetime
        try:
            # Парсим даты из формата DD.MM.YYYY
            date_from_obj = datetime.strptime(date_from, "%d.%m.%Y")
            date_to_obj = datetime.strptime(date_to, "%d.%m.%Y")
            
            # Конвертируем в RFC3339 формат (с временем 00:00:00)
            date_from_rfc = date_from_obj.strftime("%Y-%m-%dT00:00:00")
            date_to_rfc = date_to_obj.strftime("%Y-%m-%dT23:59:59")
            
        except ValueError as e:
            return jsonify({"error": "invalid_date_format", "message": "Неверный формат даты. Используйте DD.MM.YYYY"}), 400
        
        # Загружаем заказы
        orders_url = "https://statistics-api.wildberries.ru/api/v1/supplier/orders"
        orders_params = {
            "dateFrom": date_from_rfc,
            "dateTo": date_to_rfc
        }
        
        print(f"Запрос к API заказов: {orders_url} с параметрами: {orders_params}")
        
        orders_response = requests.get(
            orders_url,
            headers=headers,
            params=orders_params,
            timeout=60
        )
        
        if orders_response.status_code != 200:
            print(f"Ошибка API заказов: {orders_response.status_code} - {orders_response.text}")
            return jsonify({
                "error": "orders_api_error", 
                "message": f"Ошибка API заказов: {orders_response.status_code}"
            }), 502
        
        orders_data = orders_response.json()
        print(f"Получено заказов: {len(orders_data)}")
        
        # Фильтруем заказы по складу
        filtered_orders = []
        
        # Собираем уникальные названия складов из заказов для отладки
        unique_warehouses = set()
        for order in orders_data:
            wh_name = order.get("warehouseName")
            if wh_name:
                unique_warehouses.add(wh_name)
        
        print(f"Уникальные склады в заказах: {sorted(unique_warehouses)}")
        print(f"Ищем заказы для склада: '{warehouse_name}'")
        
        for order in orders_data:
            order_warehouse = order.get("warehouseName")
            if order_warehouse:
                # Точное совпадение
                if order_warehouse == warehouse_name:
                    filtered_orders.append(order)
                # Частичное совпадение - проверяем содержит ли название склада из заказов название из API складов
                elif warehouse_name in order_warehouse or order_warehouse in warehouse_name:
                    print(f"Найдено частичное совпадение: '{order_warehouse}' <-> '{warehouse_name}'")
                    filtered_orders.append(order)
        
        print(f"Найдено заказов для склада '{warehouse_name}': {len(filtered_orders)}")
        
        # Группируем заказы по баркодам для подсчета количества
        # В API заказов каждый заказ = 1 товар, поэтому считаем количество заказов
        # Исключаем отмененные заказы (isCancel = true)
        orders_by_barcode = {}
        cancelled_orders = 0
        
        for order in filtered_orders:
            # Проверяем, не отменен ли заказ
            is_cancelled = order.get("isCancel", False)
            if is_cancelled:
                cancelled_orders += 1
                continue  # Пропускаем отмененные заказы
            
            barcode = order.get("barcode")
            if barcode:
                if barcode not in orders_by_barcode:
                    orders_by_barcode[barcode] = 0
                # Каждый заказ = 1 товар
                orders_by_barcode[barcode] += 1
        
        print(f"Отмененных заказов исключено: {cancelled_orders}")
        
        # Отладочная информация о группировке заказов
        print(f"=== DEBUG: Группировка заказов ===")
        print(f"Всего отфильтрованных заказов: {len(filtered_orders)}")
        print(f"Уникальных баркодов: {len(orders_by_barcode)}")
        
        # Проверим первые несколько заказов
        if filtered_orders:
            print("Примеры заказов:")
            for i, order in enumerate(filtered_orders[:3]):
                print(f"  Заказ {i+1}: barcode={order.get('barcode')}, quantity={order.get('quantity')}, warehouseName={order.get('warehouseName')}")
                print(f"    Все поля заказа: {list(order.keys())}")
                print(f"    Полный заказ: {order}")
        
        # Проверим первые несколько сгруппированных заказов
        if orders_by_barcode:
            print("Примеры сгруппированных заказов:")
            for i, (barcode, qty) in enumerate(list(orders_by_barcode.items())[:5]):
                print(f"  {barcode}: {qty}")
        
        print(f"Заказы сгруппированы по {len(orders_by_barcode)} баркодам")
        
        return jsonify({
            "success": True,
            "warehouse_id": warehouse_id,
            "warehouse_name": warehouse_name,
            "date_from": date_from,
            "date_to": date_to,
            "orders": orders_by_barcode,
            "total_orders": len(filtered_orders),
            "cancelled_orders": cancelled_orders,
            "unique_products": len(orders_by_barcode)
        })
        
    except Exception as e:
        print(f"Ошибка получения заказов: {e}")
        return jsonify({"error": "server_error", "message": str(e)}), 500


@app.route("/api/fbw/planning/export-excel", methods=["POST"])
@login_required
def api_fbw_planning_export_excel():
    """Экспорт результатов планирования в Excel формат XLS"""
    try:
        data = request.get_json()
        if not data or 'products' not in data:
            return jsonify({"error": "Нет данных для экспорта"}), 400
        
        products = data['products']
        warehouse_name = data.get('warehouse_name', 'Неизвестный_склад')
        
        # Фильтруем товары - экспортируем только те, у которых количество для поставки больше 0
        products_to_export = [p for p in products if p.get('toSupply', 0) > 0]
        
        if not products_to_export:
            return jsonify({"error": "Нет товаров для поставки"}), 400
        
        # Создаем Excel файл в формате XLS (Excel 97-2003)
        workbook = xlwt.Workbook(encoding='utf-8')
        worksheet = workbook.add_sheet('Планирование поставки')
        
        # Стили
        header_style = xlwt.easyxf('font: bold on; align: horiz center;')
        number_style = xlwt.easyxf('align: horiz right;')
        
        # Заголовки
        headers = [
            '№', 'Штрихкод', 'Наименование', 'Текущий остаток', 
            'Остаток по всем складам', 'В пути на склад', 'Заказано за период',
            'Продаж в день', 'Необходимый остаток', 'Оборачиваемость', 'Поставить на склад'
        ]
        
        for col, header in enumerate(headers):
            worksheet.write(0, col, header, header_style)
        
        # Данные
        for row, product in enumerate(products_to_export, 1):
            worksheet.write(row, 0, row)  # №
            worksheet.write(row, 1, str(product.get('barcode', '')))  # Штрихкод
            worksheet.write(row, 2, str(product.get('name', '')))  # Наименование
            worksheet.write(row, 3, product.get('currentStock', 0), number_style)  # Текущий остаток
            worksheet.write(row, 4, product.get('allStocks', 0), number_style)  # Остаток по всем складам
            worksheet.write(row, 5, product.get('inTransit', 0), number_style)  # В пути на склад
            worksheet.write(row, 6, product.get('orderedInPeriod', 0), number_style)  # Заказано за период
            worksheet.write(row, 7, round(product.get('salesPerDay', 0), 2), number_style)  # Продаж в день
            worksheet.write(row, 8, round(product.get('requiredStock', 0)), number_style)  # Необходимый остаток
            worksheet.write(row, 9, round(product.get('turnover', 0), 1), number_style)  # Оборачиваемость
            worksheet.write(row, 10, round(product.get('toSupply', 0)), number_style)  # Поставить на склад
        
        # Автоподбор ширины колонок
        for col in range(len(headers)):
            worksheet.col(col).width = 3000  # Примерная ширина
        
        # Создаем файл в памяти
        output = BytesIO()
        workbook.save(output)
        output.seek(0)
        
        # Генерируем имя файла с общим количеством товаров
        now = datetime.now()
        day = now.strftime("%d.%m.%Y")
        time = now.strftime("%H_%M")
        total_quantity = sum(round(p.get('toSupply', 0)) for p in products_to_export)
        filename = f"{warehouse_name}_{day}_{time}_({total_quantity}).xls"
        
        return send_file(
            output,
            as_attachment=True,
            download_name=filename,
            mimetype='application/vnd.ms-excel'
        )
        
    except Exception as e:
        print(f"Ошибка экспорта в Excel: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": f"Ошибка экспорта: {str(e)}"}), 500

@app.route("/api/fbw/planning/supplies", methods=["GET"])
@login_required
def api_fbw_planning_supplies():
    """API для получения поставок со статусом 'Отгрузка разрешена' для планирования (оптимизированная версия)"""
    token = (current_user.wb_token or "") if current_user.is_authenticated else ""
    warehouse_name = request.args.get("warehouse_name")
    force_refresh = request.args.get("force_refresh", "false").lower() == "true"
    
    if not token:
        return jsonify({"error": "no_token"}), 401
    
    try:
        from datetime import datetime
        print(f"Запрос поставок для планирования: warehouse='{warehouse_name}', force_refresh={force_refresh}")
        
        # Проверяем кэш поставок
        cached = load_fbw_supplies_cache() or {}
        supplies_list = []
        
        # Для планирования используем отдельный кэш, чтобы не влиять на основной кэш страницы /fbw
        planning_cache_key = f"planning_supplies_{current_user.id}"
        cached_planning = cached.get(planning_cache_key, {})
        
        # Для планирования всегда загружаем свежие данные с API (как на странице FBS)
        # Это обеспечивает учет новых поставок, созданных недавно
        print("Планирование поставок: загружаем свежие данные с API для учета новых поставок")
        supplies_list = fetch_fbw_supplies_list(token, days_back=30)
        
        # Сохраняем в отдельный ключ для планирования, не трогая основной кэш
        cached[planning_cache_key] = {
            "supplies_list": supplies_list,
            "updated_at": datetime.now(MOSCOW_TZ).strftime("%d.%m.%Y %H:%M")
        }
        save_fbw_supplies_cache(cached)
        print(f"Загружен свежий список поставок для планирования: {len(supplies_list)} поставок")
        
        # Фильтруем поставки по подходящим статусам для планирования
        supplies_with_status = []
        print(f"Фильтруем подходящие поставки для планирования из {len(supplies_list)} поставок")
        
        # Собираем статистику по статусам для отладки
        status_counts = {}
        
        for supply in supplies_list[:20]:  # Берем только последние 20
            supply_id = supply.get("supplyID") or supply.get("supplyId") or supply.get("id")
            if not supply_id:
                continue
                
            status = supply.get("statusName", "").strip()
            status_counts[status] = status_counts.get(status, 0) + 1
            
            # Логируем каждую поставку для отладки
            print(f"Поставка {supply_id}: статус='{status}', склад='{supply.get('warehouseName', 'не указан')}'")
            
            # Включаем поставки с различными статусами, подходящими для планирования
            if status in ["Отгрузка разрешена", "Запланировано", "Создано", "В работе"]:
                supplies_with_status.append({
                    "supply_id": str(supply_id),
                    "create_date": supply.get("createDate"),
                    "supply_date": supply.get("supplyDate"),
                    "status": status
                })
        
        print(f"Найдено подходящих поставок для планирования: {len(supplies_with_status)}")
        print(f"Статистика по статусам: {status_counts}")
        
        # Если нет подходящих поставок, возвращаем пустой результат
        if not supplies_with_status:
            return jsonify({
                "success": True,
                "supplies": [],
                "warehouse_name": warehouse_name,
                "count": 0
            })
        
        # Загружаем детали только для поставок со статусом "Отгрузка разрешена" (медленно, но мало данных)
        print(f"Загружаем детали для {len(supplies_with_status)} поставок...")
        pending_supplies = []
        for supply_info in supplies_with_status:
            try:
                details = fetch_fbw_supply_details(token, supply_info["supply_id"])
                if details:
                    # Получаем название склада из деталей поставки
                    warehouse_from_details = details.get("warehouseName", "").strip()
                    if warehouse_from_details:
                        supply_info["warehouse"] = warehouse_from_details
                        print(f"Обновлено название склада для поставки {supply_info['supply_id']}: '{warehouse_from_details}'")
                    
                    total_qty = details.get("quantity", 0) or 0
                    supply_date = details.get("supplyDate")
                    
                    # Загружаем товары из поставки
                    supply_goods = []
                    try:
                        goods = fetch_fbw_supply_goods(token, supply_info["supply_id"])
                        for good in goods:
                            barcode = good.get("barcode", "").strip()
                            qty = int(good.get("quantity", 0) or 0)
                            if barcode and qty > 0:
                                supply_goods.append({
                                    "barcode": barcode,
                                    "quantity": qty,
                                    "name": good.get("name", ""),
                                    "article": good.get("article", "")
                                })
                        print(f"Загружено товаров из поставки {supply_info['supply_id']}: {len(supply_goods)}")
                    except Exception as e:
                        print(f"Ошибка загрузки товаров поставки {supply_info['supply_id']}: {e}")
                    
                    # Проверяем, что склад действительно совпадает
                    if warehouse_name and supply_info["warehouse"]:
                        if (supply_info["warehouse"] == warehouse_name or 
                            warehouse_name in supply_info["warehouse"] or
                            supply_info["warehouse"] in warehouse_name):
                            
                            pending_supplies.append({
                                "supply_id": supply_info["supply_id"],
                                "warehouse": supply_info["warehouse"],
                                "total_goods": int(total_qty) if total_qty else 0,
                                "goods": supply_goods,  # Добавляем детали товаров
                                "planned_date": _fmt_dt_moscow(supply_date, with_time=False) if supply_date else "",
                                "created_at": _fmt_dt_moscow(supply_info["create_date"], with_time=False) if supply_info["create_date"] else ""
                            })
                        else:
                            print(f"Поставка {supply_info['supply_id']} не подходит по складу: '{supply_info['warehouse']}' != '{warehouse_name}'")
                    else:
                        # Если нет названия склада, добавляем как есть
                        pending_supplies.append({
                            "supply_id": supply_info["supply_id"],
                            "warehouse": supply_info["warehouse"],
                            "total_goods": int(total_qty) if total_qty else 0,
                            "goods": supply_goods,  # Добавляем детали товаров
                            "planned_date": _fmt_dt_moscow(supply_date, with_time=False) if supply_date else "",
                            "created_at": _fmt_dt_moscow(supply_info["create_date"], with_time=False) if supply_info["create_date"] else ""
                        })
            except Exception as e:
                print(f"Ошибка загрузки деталей поставки {supply_info['supply_id']}: {e}")
                continue
        
        return jsonify({
            "success": True,
            "supplies": pending_supplies,
            "warehouse_name": warehouse_name,
            "count": len(pending_supplies)
        })
        
    except Exception as e:
        return jsonify({"error": "server_error", "message": str(e)}), 500

@app.route("/api/fbw/supplies", methods=["GET"]) 
@login_required
def api_fbw_supplies():
    # If cached=1, return cached items only (no API calls)
    if request.args.get("cached"):
        cached = load_fbw_supplies_cache() or {}
        items = cached.get("items") or []
        updated_at = cached.get("updated_at", "")
        return jsonify({"items": items, "updated_at": updated_at})

    token = (current_user.wb_token or "") if current_user.is_authenticated else ""
    if not token:
        return jsonify({"error": "no_token"}), 401
    try:
        # Refresh from WB: fetch first 15 or a subsequent page for load-more
        offset = int(request.args.get("offset", "0"))
        limit = int(request.args.get("limit", "15"))
        if offset <= 0:
            items = fetch_fbw_last_supplies(token, limit=limit)
            next_offset = limit
        else:
            # Always derive items from the same globally sorted list to avoid gaps
            items = fetch_fbw_supplies_range(token, offset=offset, limit=limit)
            next_offset = offset + limit
        # Merge cached package_count so они не теряются между обновлениями
        cached_for_user = load_fbw_supplies_cache() or {}
        cached_items = cached_for_user.get("items") or []
        items = _merge_package_counts(items, cached_items)
        
        # Предварительно загружаем количество коробок для поставок без этой информации
        # Это делаем только для первой страницы, чтобы не замедлять загрузку
        if offset <= 0:
            items = _preload_package_counts(token, items)
        
        updated_at = datetime.now(MOSCOW_TZ).strftime("%d.%m.%Y %H:%M")
        if offset <= 0:
            save_fbw_supplies_cache({"items": items, "updated_at": updated_at, "next_offset": next_offset})
        return jsonify({"items": items, "updated_at": updated_at, "next_offset": next_offset})
    except requests.HTTPError as http_err:
        return jsonify({"error": f"api_{http_err.response.status_code}"}), http_err.response.status_code
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": str(exc)}), 500


@app.route("/fbs-stock", methods=["GET"]) 
@login_required
def fbs_stock_page():
    token = (current_user.wb_token or "") if current_user.is_authenticated else ""
    error = None
    warehouses: list[dict[str, Any]] = []
    updated_at = ""
    # Show cache only; refresh by button
    cached = load_fbs_stock_cache() or {}
    if cached and cached.get("_user_id") == (current_user.id if current_user.is_authenticated else None):
        warehouses = cached.get("warehouses", []) or []
        updated_at = cached.get("updated_at", "")
    return render_template("fbs_stock.html", error=error, warehouses=warehouses, updated_at=updated_at)


@app.route("/api/fbs-stock/refresh", methods=["POST"]) 
@login_required
def api_fbs_stock_refresh():
    token = (current_user.wb_token or "") if current_user.is_authenticated else ""
    if not token:
        return jsonify({"error": "no_token"}), 401
    try:
        # 1) collect SKUs from products cache
        prod_cached = load_products_cache() or {}
        products = prod_cached.get("products") or prod_cached.get("items") or []
        # products expected structure should have barcode(s) and nm_id, vendor code, photo
        skus: list[str] = []
        barcode_to_info: dict[str, dict[str, Any]] = {}
        for p in products:
            bcs: list[str] = []
            if isinstance(p.get("barcodes"), list):
                bcs = [str(x) for x in p.get("barcodes") if x]
            elif p.get("barcode"):
                bcs = [str(p.get("barcode"))]
            for bc in bcs:
                skus.append(bc)
                barcode_to_info[bc] = {
                    "nm_id": p.get("nm_id") or p.get("nmId") or p.get("nmID"),
                    "vendor_code": p.get("Артикул продавца") or p.get("vendor_code") or p.get("vendorCode") or p.get("supplierArticle") or p.get("supplier_article"),
                    "photo": p.get("photo") or p.get("img") or None,
                }
        if not skus:
            return jsonify({"error": "no_skus_in_products_cache"}), 400
        # 2) fetch warehouses and build summary totals
        wlist = fetch_fbs_warehouses(token)
        warehouses: list[dict[str, Any]] = []
        # Maps for human-readable labels
        cargo_labels = {
            1: "МГТ (малогабаритный)",
            2: "СГТ (сверхгабаритный)",
            3: "КГТ+ (крупногабаритный)",
        }
        delivery_labels = {
            1: "FBS",
            2: "DBS",
            3: "DBW",
            5: "C&C",
            6: "EDBS",
        }
        for w in wlist:
            wid = w.get("id") or w.get("warehouseId") or w.get("warehouseID")
            wname = w.get("name") or w.get("warehouseName") or ""
            try:
                stocks = fetch_fbs_stocks_by_warehouse(token, int(wid), skus)
                total_amount = sum(int(s.get("amount") or 0) for s in stocks)
            except Exception:
                total_amount = 0
            warehouses.append({
                "id": wid,
                "name": wname,
                "cargoType": cargo_labels.get(int(w.get("cargoType") or 0), "-"),
                "deliveryType": delivery_labels.get(int(w.get("deliveryType") or 0), "-"),
                "total_amount": total_amount,
            })
        updated_at = datetime.now(MOSCOW_TZ).strftime("%d.%m.%Y %H:%M")
        save_fbs_stock_cache({"warehouses": warehouses, "updated_at": updated_at})
        return jsonify({"warehouses": warehouses, "updated_at": updated_at})
    except requests.HTTPError as http_err:
        return jsonify({"error": f"api_{http_err.response.status_code}"}), http_err.response.status_code
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": str(exc)}), 500


@app.route("/api/fbs-stock/update", methods=["POST"]) 
@login_required
def api_fbs_stock_update():
    token = (current_user.wb_token or "") if current_user.is_authenticated else ""
    if not token:
        return jsonify({"error": "no_token"}), 401
    try:
        payload = request.get_json(silent=True) or {}
        items = payload.get("stocks") or []
        warehouse_id_payload = payload.get("warehouseId")
        if not isinstance(items, list) or not items:
            return jsonify({"error": "no_items"}), 400
        # determine warehouse id
        if warehouse_id_payload:
            warehouse_id = warehouse_id_payload
        else:
            warehouses = fetch_fbs_warehouses(token)
            if not warehouses:
                return jsonify({"error": "warehouse_not_found"}), 404
            warehouse_id = warehouses[0].get("id") or warehouses[0].get("warehouseId") or warehouses[0].get("warehouseID")
        # update
        update_fbs_stocks_by_warehouse(token, int(warehouse_id), items)
        return jsonify({"ok": True})
    except requests.HTTPError as http_err:
        try:
            err_text = http_err.response.text
        except Exception:
            err_text = ""
        return jsonify({"error": f"api_{http_err.response.status_code}", "detail": err_text}), http_err.response.status_code
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": str(exc)}), 500


@app.route("/api/fbs-stock/warehouse/<int:warehouse_id>", methods=["GET"]) 
@login_required
def api_fbs_stock_by_warehouse(warehouse_id: int):
    token = (current_user.wb_token or "") if current_user.is_authenticated else ""
    if not token:
        return jsonify({"error": "no_token"}), 401
    try:
        # collect SKUs from products cache
        prod_cached = load_products_cache() or {}
        products = prod_cached.get("products") or prod_cached.get("items") or []
        skus: list[str] = []
        barcode_to_info: dict[str, dict[str, Any]] = {}
        for p in products:
            bcs: list[str] = []
            if isinstance(p.get("barcodes"), list):
                bcs = [str(x) for x in p.get("barcodes") if x]
            elif p.get("barcode"):
                bcs = [str(p.get("barcode"))]
            for bc in bcs:
                skus.append(bc)
                barcode_to_info[bc] = {
                    "nm_id": p.get("nm_id") or p.get("nmId") or p.get("nmID"),
                    "vendor_code": p.get("Артикул продавца") or p.get("vendor_code") or p.get("vendorCode") or p.get("supplierArticle") or p.get("supplier_article"),
                    "photo": p.get("photo") or p.get("img") or None,
                }
        if not skus:
            return jsonify({"error": "no_skus_in_products_cache"}), 400
        stocks = fetch_fbs_stocks_by_warehouse(token, int(warehouse_id), skus)
        rows: list[dict[str, Any]] = []
        for st in stocks:
            sku = str(st.get("sku") or "")
            amount = int(st.get("amount") or 0)
            info = barcode_to_info.get(sku, {})
            rows.append({
                "photo": info.get("photo"),
                "vendor_code": info.get("vendor_code") or "",
                "nm_id": info.get("nm_id") or "",
                "barcode": sku,
                "amount": amount,
            })
        total_amount = sum(r.get("amount", 0) for r in rows)
        return jsonify({"rows": rows, "total_amount": total_amount})
    except requests.HTTPError as http_err:
        return jsonify({"error": f"api_{http_err.response.status_code}"}), http_err.response.status_code
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": str(exc)}), 500


@app.route("/api/fbs-stock/auto-update/settings", methods=["GET", "POST"])
@login_required
def api_fbs_stock_auto_update_settings():
    if request.method == "GET":
        try:
            settings = load_auto_update_settings()
            return jsonify(settings)
        except Exception as exc:
            return jsonify({"error": str(exc)}), 500
    
    # POST - save settings
    try:
        payload = request.get_json(silent=True) or {}
        interval = int(payload.get("interval", 60))
        enabled = bool(payload.get("enabled", False))
        warehouse_settings = payload.get("warehouses", {})
        
        # Load current settings
        settings = load_auto_update_settings()
        
        # Update global settings
        settings["global"] = {
            "interval": interval,
            "enabled": enabled,
            "lastCheck": settings["global"].get("lastCheck"),
            "history": settings["global"].get("history", [])
        }
        
        # Update warehouse-specific settings
        if warehouse_settings:
            settings["warehouses"] = warehouse_settings
        
        save_auto_update_settings(settings)
        return jsonify({"ok": True})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/api/fbs-stock/auto-update/test", methods=["POST"])
@login_required
def api_fbs_stock_auto_update_test():
    try:
        payload = request.get_json(silent=True) or {}
        url = payload.get("url", "").strip()
        
        if not url:
            return jsonify({"error": "no_url"}), 400
        
        # Test connection and get file info
        file_info = test_remote_file(url)
        return jsonify(file_info)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/api/fbs-stock/auto-update/manual-update", methods=["POST"])
@login_required
def api_fbs_stock_auto_update_manual():
    print("=== MANUAL UPDATE ENDPOINT CALLED ===")
    try:
        payload = request.get_json(silent=True) or {}
        warehouses = payload.get("warehouses", [])
        
        print(f"Payload: {payload}")
        print(f"Warehouses: {warehouses}")
        
        if not warehouses:
            print("ERROR: No warehouses provided")
            return jsonify({"error": "no_warehouses"}), 400
        
        user_id = current_user.id if current_user.is_authenticated else None
        print(f"User ID: {user_id}")
        
        if not user_id:
            print("ERROR: No user ID")
            return jsonify({"error": "no_user"}), 401
        
        print(f"Manual update requested for user {user_id}, warehouses: {len(warehouses)}")
        
        total_processed = 0
        total_updated = 0
        results = []
        
        # Process each warehouse individually
        for warehouse in warehouses:
            warehouse_id = warehouse.get('warehouseId')
            url = warehouse.get('url', '').strip()
            
            if not url:
                print(f"ERROR: No URL provided for warehouse {warehouse_id}")
                results.append(f"Склад {warehouse_id}: не указан URL")
                continue
            
            print(f"Processing warehouse {warehouse_id} with URL: {url}")
            
            # Download and process file
            print(f"Starting file download and processing for warehouse {warehouse_id}...")
            result = download_and_process_remote_file(url, user_id)
            print(f"File processing result for warehouse {warehouse_id}: {result}")
            
            if result['success']:
                print(f"File data for warehouse {warehouse_id}: {len(result['data'])} items")
                
                # Update stocks for this specific warehouse
                print(f"Calling update_stocks_from_remote_data for warehouse {warehouse_id}...")
                updated_count = update_stocks_from_remote_data(result['data'], user_id, [int(warehouse_id)])
                print(f"Updated count for warehouse {warehouse_id}: {updated_count}")
                
                total_processed += len(result['data'])
                total_updated += updated_count
                results.append(f"Склад {warehouse_id}: обработано {len(result['data'])} товаров, обновлено {updated_count} остатков")
            else:
                print(f"File processing failed for warehouse {warehouse_id}: {result['error']}")
                results.append(f"Склад {warehouse_id}: ошибка - {result['error']}")
        
        # Add to history
        settings = load_auto_update_settings()
        history_entry = {
            "timestamp": datetime.now().isoformat(),
            "success": True,
            "message": f"Ручное обновление: обработано {total_processed} товаров, обновлено {total_updated} остатков (с учетом FBS заданий)"
        }
        settings['global']['history'].insert(0, history_entry)
        settings['global']['history'] = settings['global']['history'][:50]  # Keep last 50 entries
        settings['global']['lastCheck'] = datetime.now().isoformat()
        save_auto_update_settings(settings)
        
        print(f"Returning success: processed={total_processed}, updated={total_updated}")
        return jsonify({
            "processed": total_processed,
            "updated": total_updated,
            "details": results
        })

            
    except Exception as exc:
        print(f"Manual update error: {exc}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(exc)}), 500


@app.route("/api/fbw/supplies/<supply_id>/details", methods=["GET"]) 
@login_required
def api_fbw_supply_details(supply_id: str):
    token = (current_user.wb_token or "") if current_user.is_authenticated else ""
    if not token:
        return jsonify({"error": "no_token"}), 401
    try:
        details = fetch_fbw_supply_details(token, supply_id) or {}
        goods = fetch_fbw_supply_goods(token, supply_id, limit=200, offset=0)
        packages = fetch_fbw_supply_packages(token, supply_id)
        return jsonify({"details": details, "goods": goods, "packages": packages})
    except requests.HTTPError as http_err:
        return jsonify({"error": f"api_{http_err.response_status}"}), 500
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": str(exc)}), 500


@app.route("/api/fbw/supplies/<supply_id>/package-count", methods=["GET"]) 
@login_required
def api_fbw_supply_package_count(supply_id: str):
    token = (current_user.wb_token or "") if current_user.is_authenticated else ""
    if not token:
        return jsonify({"error": "no_token"}), 401
    try:
        packages = fetch_fbw_supply_packages(token, supply_id)
        count = len(packages) if isinstance(packages, list) else 0
        # Try update cache to persist the count
        try:
            cached = load_fbw_supplies_cache() or {}
            items = cached.get("items") or []
            changed = False
            for it in items:
                sid = str(it.get("supply_id") or it.get("supplyID") or it.get("supplyId") or it.get("id") or "")
                if sid == str(supply_id):
                    it["package_count"] = int(count)
                    changed = True
                    break
            if changed:
                # Keep updated_at and next_offset intact
                save_fbw_supplies_cache({
                    "items": items,
                    "updated_at": cached.get("updated_at", ""),
                    "next_offset": cached.get("next_offset", 0),
                })
        except Exception:
            pass
        return jsonify({"package_count": int(count)})
    except requests.HTTPError as http_err:
        return jsonify({"error": f"api_{http_err.response_status}"}), 500
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": str(exc)}), 500

@app.route("/api/orders-refresh", methods=["POST"]) 
@login_required
def api_orders_refresh():
    token = (current_user.wb_token or "") if current_user.is_authenticated else ""
    if not token:
        return jsonify({"error": "no_token"}), 401
    date_from = request.form.get("date_from", "")
    date_to = request.form.get("date_to", "")
    try:
        df = parse_date(date_from)
        dt = parse_date(date_to)
        if df > dt:
            date_from, date_to = date_to, date_from
    except ValueError:
        return jsonify({"error": "bad_dates"}), 400
    try:
        # Orders
        include_orders = request.form.get("include_orders") is not None
        include_sales = request.form.get("include_sales") is not None
        if include_orders:
            raw_orders = fetch_orders_range(token, date_from, date_to)
            orders = to_rows(raw_orders, date_from, date_to)
            total_orders = len(orders)
            total_revenue = round(sum(float(o.get("Цена с учетом всех скидок") or 0) for o in orders), 2)
        else:
            orders = []
            total_orders = 0
            total_revenue = 0.0
        # Sales
        if include_sales:
            raw_sales = fetch_sales_range(token, date_from, date_to)
            sales_rows = to_sales_rows(raw_sales, date_from, date_to)
        else:
            sales_rows = []
        # Aggregates
        o_counts_map, o_rev_map = aggregate_daily_counts_and_revenue(orders) if include_orders else ({}, {})
        s_counts_map, s_rev_map = aggregate_daily_counts_and_revenue(sales_rows) if include_sales else ({}, {})
        daily_labels, daily_orders_counts, daily_sales_counts, daily_orders_revenue, daily_sales_revenue = build_union_series(
            o_counts_map, s_counts_map, o_rev_map, s_rev_map
        )
        # Warehouses and TOPs
        warehouse_summary_dual = aggregate_by_warehouse_dual(orders, sales_rows)
        if include_orders:
            top_products = aggregate_top_products(orders, limit=15)
            top_mode = "orders"
        elif include_sales:
            top_products = aggregate_top_products(sales_rows, limit=15)
            top_mode = "sales"
        else:
            top_products = []
            top_mode = "orders"
        warehouses = sorted({(r.get("Склад отгрузки") or "Не указан") for r in orders})
        top_products_orders_filtered = aggregate_top_products_orders(orders, None, limit=50)
        updated_at = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
        # Save cache
        save_last_results({
            "date_from": date_from,
            "date_to": date_to,
            "orders": orders,
            "total_orders": total_orders,
            "total_revenue": total_revenue,
            "sales_rows": sales_rows,
            "total_sales": len(sales_rows),
            "total_sales_revenue": round(sum(float(o.get("Цена с учетом всех скидок") or 0) for o in sales_rows), 2),
            "daily_labels": daily_labels,
            "daily_orders_counts": daily_orders_counts,
            "daily_sales_counts": daily_sales_counts,
            "daily_orders_revenue": daily_orders_revenue,
            "daily_sales_revenue": daily_sales_revenue,
            "warehouse_summary_dual": warehouse_summary_dual,
            "top_products": top_products,
            "top_mode": top_mode,
            "include_orders": include_orders,
            "include_sales": include_sales,
            "updated_at": updated_at,
        })
        return jsonify({
            "total_orders": total_orders,
            "total_revenue": total_revenue,
            "daily_labels": daily_labels,
            "daily_orders_counts": daily_orders_counts,
            "daily_sales_counts": daily_sales_counts,
            "daily_orders_revenue": daily_orders_revenue,
            "daily_sales_revenue": daily_sales_revenue,
            "warehouse_summary_dual": warehouse_summary_dual,
            "top_products": top_products,
            "warehouses": list(warehouses),
            "top_products_orders_filtered": top_products_orders_filtered,
            "updated_at": updated_at,
            "date_from_fmt": format_dmy(date_from),
            "date_to_fmt": format_dmy(date_to),
            "top_mode": top_mode,
        })
    except requests.HTTPError as http_err:
        status = 502
        try:
            if http_err.response is not None and http_err.response.status_code:
                status = http_err.response.status_code
        except Exception:
            status = 502
        # Graceful fallback for 429: return cached data if dates match
        if status == 429:
            cached = load_last_results() or {}
            if (
                cached.get("date_from") == date_from
                and cached.get("date_to") == date_to
                and cached.get("_user_id") == (current_user.id if current_user.is_authenticated else None)
            ):
                return jsonify({
                    "total_orders": cached.get("total_orders", 0),
                    "total_revenue": cached.get("total_revenue", 0),
                    "daily_labels": cached.get("daily_labels", []),
                    "daily_orders_counts": cached.get("daily_orders_counts", []),
                    "daily_sales_counts": cached.get("daily_sales_counts", []),
                    "daily_orders_revenue": cached.get("daily_orders_revenue", []),
                    "daily_sales_revenue": cached.get("daily_sales_revenue", []),
                    "warehouse_summary_dual": cached.get("warehouse_summary_dual", []),
                    "top_products": cached.get("top_products", []),
                    "warehouses": cached.get("warehouses", []),
                    "top_products_orders_filtered": cached.get("top_products_orders_filtered", []),
                    "updated_at": cached.get("updated_at", ""),
                    "date_from_fmt": format_dmy(date_from),
                    "date_to_fmt": format_dmy(date_to),
                    "top_mode": cached.get("top_mode", "orders"),
                    "rate_limited": True,
                }), 200
        return jsonify({"error": "http", "status": status}), status
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": str(exc)}), 500


@app.route("/profile", methods=["GET"]) 
@login_required
def profile():
    seller_info: Dict[str, Any] | None = None
    token = current_user.wb_token or ""
    if token:
        try:
            seller_info = fetch_seller_info(token)
        except Exception:
            seller_info = None
    validity_status = None
    if current_user.valid_from or current_user.valid_to:
        today = datetime.now(MOSCOW_TZ).date()
        active = True
        if current_user.valid_from and today < current_user.valid_from:
            active = False
        if current_user.valid_to and today > current_user.valid_to:
            active = False
        validity_status = "active" if active and current_user.is_active else "inactive"
    return render_template(
        "profile.html",
        message=None,
        token=token,
        seller_info=seller_info,
        valid_from=current_user.valid_from.strftime("%d.%m.%Y") if current_user.valid_from else None,
        valid_to=current_user.valid_to.strftime("%d.%m.%Y") if current_user.valid_to else None,
        validity_status=validity_status,
    )


@app.route("/profile/token", methods=["POST"]) 
@login_required
def profile_token():
    new_token = request.form.get("token", "").strip()
    try:
        current_user.wb_token = new_token or None
        db.session.commit()
        if new_token:
            hint = []
            if not (current_user.phone and current_user.email and current_user.shipper_address):
                hint.append(" Заполните телефон, email и адрес склада для этикеток в профиле.")
            flash("Токен успешно добавлен." + (hint[0] if hint else ""))
        else:
            flash("Токен удален")
    except Exception:
        db.session.rollback()
        flash("Ошибка сохранения токена")
    return redirect(url_for("profile"))


@app.route("/profile/shipping", methods=["POST"]) 
@login_required
def profile_shipping():
    current_user.shipper_name = (request.form.get("shipper_name") or "").strip() or None
    current_user.contact_person = (request.form.get("contact_person") or "").strip() or None
    current_user.phone = (request.form.get("phone") or "").strip() or None
    current_user.email = (request.form.get("email") or "").strip() or None
    current_user.shipper_address = (request.form.get("shipper_address") or "").strip() or None
    try:
        db.session.commit()
        flash("Реквизиты сохранены")
    except Exception:
        db.session.rollback()
        flash("Ошибка сохранения реквизитов")
    return redirect(url_for("profile"))


@app.route("/profile/password", methods=["POST"]) 
@login_required
def profile_password():
    old_password = (request.form.get("old_password", "") or "").strip()
    new_password = (request.form.get("new_password", "") or "").strip()
    if not old_password or not new_password:
        flash("Заполните оба поля")
        return redirect(url_for("profile"))
    if current_user.password != old_password:
        flash("Текущий пароль неверен")
        return redirect(url_for("profile"))
    if len(new_password) < 4:
        flash("Новый пароль слишком короткий (мин. 4 символа)")
        return redirect(url_for("profile"))
    if new_password == old_password:
        flash("Новый пароль совпадает с текущим")
        return redirect(url_for("profile"))
    try:
        current_user.password = new_password
        db.session.commit()
        flash("Пароль обновлён")
    except Exception:
        db.session.rollback()
        flash("Ошибка обновления пароля")
    return redirect(url_for("profile"))


@app.route("/export", methods=["POST"]) 
@login_required
def export_excel():
    token = (request.form.get("token", "").strip() or (current_user.wb_token or ""))
    date_from = request.form.get("date_from", "")
    date_to = request.form.get("date_to", "")

    if not token or not date_from or not date_to:
        return render_template(
            "index.html",
            error="Для выгрузки укажите токен и даты",
            token=token,
            date_from=date_from,
            date_to=date_to,
            orders=[],
            total_orders=0,
            total_revenue=0,
            daily_labels=[],
            daily_counts=[],
            daily_revenue=[],
            warehouse_summary=[],
            top_products=[],
        )

    # Normalize and validate dates
    try:
        df = parse_date(date_from)
        dt = parse_date(date_to)
        if df > dt:
            date_from, date_to = date_to, date_from
    except ValueError:
        return render_template(
            "index.html",
            error="Неверный формат дат",
            token=token,
            date_from=date_from,
            date_to=date_to,
            orders=[],
            total_orders=0,
            total_revenue=0,
            daily_labels=[],
            daily_counts=[],
            daily_revenue=[],
            warehouse_summary=[],
            top_products=[],
        )

    raw_data = fetch_orders_range(token, date_from, date_to)
    rows = to_rows(raw_data, date_from, date_to)

    wb = Workbook()
    ws = wb.active
    ws.title = "orders"

    if rows:
        headers = list(rows[0].keys())
        ws.append(headers)
        for r in rows:
            ws.append([r.get(h, "") for h in headers])
    else:
        ws.append(["Нет данных"])

    output = io.BytesIO()
    wb.save(output)
    output.seek(0)

    filename = f"wb_orders_{date_from}_{date_to}.xlsx"
    return send_file(output, as_attachment=True, download_name=filename, mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


@app.route("/api/top-products-sales", methods=["GET"]) 
@login_required
def api_top_products_sales():
    warehouse = request.args.get("warehouse", "") or None
    cached = load_last_results()
    if not cached or not (current_user.is_authenticated and cached.get("_user_id") == current_user.id):
        return jsonify({"items": []})
    sales_rows = cached.get("sales_rows", [])
    items = aggregate_top_products_sales(sales_rows, warehouse, limit=50)
    return jsonify({"items": items})


@app.route("/api/top-products-orders", methods=["GET"]) 
@login_required
def api_top_products_orders():
    warehouse = request.args.get("warehouse", "") or None
    cached = load_last_results()
    if not cached or not (current_user.is_authenticated and cached.get("_user_id") == current_user.id):
        return jsonify({"items": [], "total_qty": 0, "total_sum": 0})
    orders = cached.get("orders", [])
    items = aggregate_top_products_orders(orders, warehouse, limit=50)
    
    # Рассчитываем общие суммы для выбранного склада
    total_qty = 0
    total_sum = 0.0
    for item in items:
        total_qty += item.get("qty", 0)
        total_sum += item.get("sum", 0.0)
    
    return jsonify({
        "items": items,
        "total_qty": total_qty,
        "total_sum": round(total_sum, 2)
    })


@app.route("/report/sales", methods=["GET"]) 
@login_required
def report_sales_page():
    cached = load_last_results()
    token = (current_user.wb_token or "") if current_user.is_authenticated else ""
    
    # Обновляем остатки если нужно (если кэш устарел)
    if token and current_user.is_authenticated:
        update_stocks_if_needed(current_user.id, token, force_update=False)
    
    # Страница по умолчанию открывается пустой: без данных, пока пользователь не задаст период и не нажмёт Загрузить
    if not request.args.get("date_from") and not request.args.get("date_to"):
        return render_template(
            "report_sales.html",
            error=None,
            items=[],
            date_from_fmt="",
            date_to_fmt="",
            warehouse=None,
            warehouses=[],
            date_from_val="",
            date_to_val="",
        ), 200

    # Accept date range from query params; fallback to cached
    req_from = (request.args.get("date_from") or "").strip()
    req_to = (request.args.get("date_to") or "").strip()
    warehouse = request.args.get("warehouse") or None

    if req_from and req_to and token:
        # Prefer cache if it matches the requested period and belongs to the user
        if (
            cached
            and current_user.is_authenticated
            and cached.get("_user_id") == current_user.id
            and cached.get("date_from") == req_from
            and cached.get("date_to") == req_to
        ):
            orders = cached.get("orders", [])
            try:
                date_from_fmt = datetime.strptime(req_from, "%Y-%m-%d").strftime("%d.%m.%Y")
                date_to_fmt = datetime.strptime(req_to, "%Y-%m-%d").strftime("%d.%m.%Y")
            except Exception:
                date_from_fmt = cached.get("date_from_fmt") or req_from
                date_to_fmt = cached.get("date_to_fmt") or req_to
        else:
            # Fetch fresh orders only if cache doesn't match
            try:
                raw_orders = fetch_orders_range(token, req_from, req_to)
                orders = to_rows(raw_orders, req_from, req_to)
                date_from_fmt = datetime.strptime(req_from, "%Y-%m-%d").strftime("%d.%m.%Y")
                date_to_fmt = datetime.strptime(req_to, "%Y-%m-%d").strftime("%d.%m.%Y")
            except Exception as exc:
                # Fallback to cache on error
                orders = (cached or {}).get("orders", [])
                date_from_fmt = (cached or {}).get("date_from_fmt") or (cached or {}).get("date_from")
                date_to_fmt = (cached or {}).get("date_to_fmt") or (cached or {}).get("date_to")
    else:
        # Без явного периода не подставляем кэш — страница остаётся пустой
        orders = []
        date_from_fmt = ""
        date_to_fmt = ""

    warehouses = sorted({(r.get("Склад отгрузки") or "Не указан") for r in orders}) if orders else []
    # Build matrix for client-side filtering (same as API)
    counts_total: Dict[str, int] = defaultdict(int)
    by_wh: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
    revenue_total: Dict[str, float] = defaultdict(float)
    by_wh_sum: Dict[str, Dict[str, float]] = defaultdict(lambda: defaultdict(float))
    nm_by_product: Dict[str, Any] = {}
    barcode_by_product: Dict[str, Any] = {}
    supplier_article_by_product: Dict[str, Any] = {}
    for r in (orders or []):
        prod = str(r.get("Артикул продавца") or r.get("Артикул WB") or r.get("Баркод") or "Не указан")
        wh = str(r.get("Склад отгрузки") or "Не указан")
        counts_total[prod] += 1
        by_wh[prod][wh] += 1
        try:
            price = float(r.get("Цена с учетом всех скидок") or 0)
        except (TypeError, ValueError):
            price = 0.0
        revenue_total[prod] += price
        by_wh_sum[prod][wh] += price
        nmv = r.get("Артикул WB") or r.get("nmId") or r.get("nmID")
        if prod not in nm_by_product and nmv:
            nm_by_product[prod] = nmv
        barcode = r.get("Баркод")
        if prod not in barcode_by_product and barcode:
            barcode_by_product[prod] = barcode
        supplier_article = r.get("Артикул продавца")
        if prod not in supplier_article_by_product and supplier_article:
            supplier_article_by_product[prod] = supplier_article
    # build photo map from products cache
    nm_to_photo: Dict[Any, Any] = {}
    try:
        prod_cached = load_products_cache() or {}
        for it in (prod_cached.get("items") or []):
            nmv = it.get("nm_id") or it.get("nmId") or it.get("nmID")
            photo = it.get("photo") or it.get("img")
            if nmv is not None and nmv not in nm_to_photo:
                nm_to_photo[nmv] = photo
    except Exception:
        nm_to_photo = {}

    # Load stocks data for current user - сохраняем остатки по складам
    stocks_by_warehouse = {}
    stocks_metadata = {}  # Дополнительная информация о товарах из остатков
    try:
        stocks_cached = load_stocks_cache()
        # print(f"DEBUG: stocks_cached loaded: {stocks_cached is not None}")
        if stocks_cached and stocks_cached.get("_user_id"):
            # print(f"DEBUG: stocks_cached user_id: {stocks_cached.get('_user_id')}, current_user.id: {current_user.id if current_user.is_authenticated else 'not authenticated'}")
            items = stocks_cached.get("items", [])
            # print(f"DEBUG: stocks items count: {len(items)}")
            for stock_item in items:
                barcode = stock_item.get("barcode")
                stock_warehouse = stock_item.get("warehouse", "")
                qty = int(stock_item.get("qty", 0) or 0)
                vendor_code = stock_item.get("vendor_code", "")
                nm_id = stock_item.get("nm_id")
                
                if barcode:
                    if barcode not in stocks_by_warehouse:
                        stocks_by_warehouse[barcode] = {}
                    if barcode not in stocks_metadata:
                        stocks_metadata[barcode] = {
                            "vendor_code": vendor_code,
                            "nm_id": nm_id,
                            "barcode": barcode
                        }
                    
                    if stock_warehouse:
                        stocks_by_warehouse[barcode][stock_warehouse] = stocks_by_warehouse[barcode].get(stock_warehouse, 0) + qty
            # print(f"DEBUG: stocks_by_warehouse loaded: {len(stocks_by_warehouse)} barcodes")
        # else:
            # print("DEBUG: No stocks cache or wrong user")
    except Exception as e:
        # print(f"DEBUG: Error loading stocks: {e}")
        stocks_by_warehouse = {}
        stocks_metadata = {}

    def _build_items(target_wh: str | None, show_all: bool = False) -> List[Dict[str, Any]]:
        items_local: List[Dict[str, Any]] = []
        
        # Get all products that have sales or stocks
        all_products = set(counts_total.keys())
        if show_all:
            # Add ALL products from stocks metadata (including those with zero stock)
            for barcode, metadata in stocks_metadata.items():
                # Find product by barcode
                found_in_sales = False
                for prod, prod_barcode in barcode_by_product.items():
                    if prod_barcode == barcode:
                        all_products.add(prod)
                        found_in_sales = True
                        break
                
                # If barcode not found in sales, create a virtual product entry
                if not found_in_sales:
                    # Use vendor_code from stocks metadata
                    virtual_prod = metadata["vendor_code"] or f"Товар с баркодом {barcode}"
                    # Add to mappings
                    barcode_by_product[virtual_prod] = barcode
                    if metadata["nm_id"]:
                        nm_by_product[virtual_prod] = metadata["nm_id"]
                    if metadata["vendor_code"]:
                        supplier_article_by_product[virtual_prod] = metadata["vendor_code"]
                    all_products.add(virtual_prod)
            # print(f"DEBUG: show_all=True, total products with sales: {len(counts_total)}, total products with stocks: {len(stocks_by_warehouse)}, all_products: {len(all_products)}")
        
        for prod in all_products:
            qty = (by_wh.get(prod, {}).get(target_wh, 0) if target_wh else counts_total.get(prod, 0))
            
            # Include items with sales OR (if show_all) items with stocks
            if qty > 0 or (show_all and prod in barcode_by_product):
                s = (by_wh_sum.get(prod, {}).get(target_wh, 0.0) if target_wh else revenue_total.get(prod, 0.0))
                # Calculate stock quantity for the target warehouse
                barcode = barcode_by_product.get(prod)
                stock_qty = 0
                if barcode and barcode in stocks_by_warehouse:
                    if target_wh:
                        # If specific warehouse selected, sum only for that warehouse
                        for wh_name, wh_qty in stocks_by_warehouse[barcode].items():
                            if (wh_name == target_wh or 
                                (target_wh in wh_name) or 
                                (wh_name in target_wh)):
                                stock_qty += wh_qty
                    else:
                        # If no warehouse selected, sum all warehouses
                        stock_qty = sum(stocks_by_warehouse[barcode].values())
                
                # Only include if has sales or (if show_all) has any stock data
                if qty > 0 or (show_all and prod in barcode_by_product):
                    items_local.append({
                        "product": prod,
                        "qty": qty,
                        "nm_id": nm_by_product.get(prod),
                        "barcode": barcode,
                        "supplier_article": supplier_article_by_product.get(prod),
                        "sum": round(float(s or 0.0), 2),
                        "photo": nm_to_photo.get(nm_by_product.get(prod)),
                        "stock_qty": stock_qty
                    })
        
        # Sort by quantity (descending), then by stock quantity (descending)
        items_local.sort(key=lambda x: (x["qty"], x["stock_qty"]), reverse=True)
        # print(f"DEBUG: _build_items returning {len(items_local)} items, show_all={show_all}")
        return items_local
    show_all = request.args.get("show_all_products") == "on"
    items = _build_items(warehouse, show_all) if orders else []
    matrix = [{
        "product": p,
        "nm_id": nm_by_product.get(p),
        "barcode": barcode_by_product.get(p),
        "supplier_article": supplier_article_by_product.get(p),
        "total": counts_total[p],
        "by_wh": by_wh[p],
        "total_sum": round(float(revenue_total.get(p, 0.0)), 2),
        "by_wh_sum": by_wh_sum[p],
        "photo": nm_to_photo.get(nm_by_product.get(p)),
        "by_wh_stock": stocks_by_warehouse.get(barcode_by_product.get(p), {})
    } for p in counts_total.keys()] if orders else []
    return render_template(
        "report_sales.html",
        error=None,
        items=items,
        date_from_fmt=date_from_fmt,
        date_to_fmt=date_to_fmt,
        warehouse=warehouse,
        warehouses=warehouses,
        date_from_val=(request.args.get("date_from") or ""),
        date_to_val=(request.args.get("date_to") or ""),
        matrix=matrix,
    )


@app.route("/api/report/sales", methods=["GET"]) 
@login_required
def api_report_sales():
    cached = load_last_results()
    token = (current_user.wb_token or "") if current_user.is_authenticated else ""
    
    # Обновляем остатки если нужно (если кэш устарел)
    if token and current_user.is_authenticated:
        update_stocks_if_needed(current_user.id, token, force_update=False)
    
    req_from = (request.args.get("date_from") or "").strip()
    req_to = (request.args.get("date_to") or "").strip()
    warehouse = (request.args.get("warehouse") or "").strip() or None
    try:
        if req_from and req_to and token:
            if (
                cached
                and current_user.is_authenticated
                and cached.get("_user_id") == current_user.id
                and cached.get("date_from") == req_from
                and cached.get("date_to") == req_to
            ):
                orders = cached.get("orders", [])
                date_from_fmt = datetime.strptime(req_from, "%Y-%m-%d").strftime("%d.%m.%Y")
                date_to_fmt = datetime.strptime(req_to, "%Y-%m-%d").strftime("%d.%m.%Y")
            else:
                raw_orders = fetch_orders_range(token, req_from, req_to)
                orders = to_rows(raw_orders, req_from, req_to)
                date_from_fmt = datetime.strptime(req_from, "%Y-%m-%d").strftime("%d.%m.%Y")
                date_to_fmt = datetime.strptime(req_to, "%Y-%m-%d").strftime("%d.%m.%Y")
        else:
            orders = (cached or {}).get("orders", [])
            date_from_fmt = (cached or {}).get("date_from_fmt") or (cached or {}).get("date_from")
            date_to_fmt = (cached or {}).get("date_to_fmt") or (cached or {}).get("date_to")
        # Build matrix for local filtering on frontend
        counts_total: Dict[str, int] = defaultdict(int)
        by_wh: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
        revenue_total: Dict[str, float] = defaultdict(float)
        by_wh_sum: Dict[str, Dict[str, float]] = defaultdict(lambda: defaultdict(float))
        nm_by_product: Dict[str, Any] = {}
        barcode_by_product: Dict[str, Any] = {}
        supplier_article_by_product: Dict[str, Any] = {}
        warehouses = set()
        for r in orders:
            prod = str(r.get("Артикул продавца") or r.get("Артикул WB") or r.get("Баркод") or "Не указан")
            wh = str(r.get("Склад отгрузки") or "Не указан")
            warehouses.add(wh)
            counts_total[prod] += 1
            by_wh[prod][wh] += 1
            try:
                price = float(r.get("Цена с учетом всех скидок") or 0)
            except (TypeError, ValueError):
                price = 0.0
            revenue_total[prod] += price
            by_wh_sum[prod][wh] += price
            nmv = r.get("Артикул WB") or r.get("nmId") or r.get("nmID")
            if prod not in nm_by_product and nmv:
                nm_by_product[prod] = nmv
            barcode = r.get("Баркод")
            if prod not in barcode_by_product and barcode:
                barcode_by_product[prod] = barcode
            supplier_article = r.get("Артикул продавца")
            if prod not in supplier_article_by_product and supplier_article:
                supplier_article_by_product[prod] = supplier_article
        # build photo map
        nm_to_photo: Dict[Any, Any] = {}
        try:
            prod_cached = load_products_cache() or {}
            for it in (prod_cached.get("items") or []):
                nmv = it.get("nm_id") or it.get("nmId") or it.get("nmID")
                photo = it.get("photo") or it.get("img")
                if nmv is not None and nmv not in nm_to_photo:
                    nm_to_photo[nmv] = photo
        except Exception:
            nm_to_photo = {}

        # Load stocks data for current user - сохраняем остатки по складам
        stocks_by_warehouse = {}
        stocks_metadata = {}  # Дополнительная информация о товарах из остатков
        try:
            stocks_cached = load_stocks_cache()
            if stocks_cached and stocks_cached.get("_user_id"):
                for stock_item in stocks_cached.get("items", []):
                    barcode = stock_item.get("barcode")
                    stock_warehouse = stock_item.get("warehouse", "")
                    qty = int(stock_item.get("qty", 0) or 0)
                    vendor_code = stock_item.get("vendor_code", "")
                    nm_id = stock_item.get("nm_id")
                    
                    if barcode:
                        if barcode not in stocks_by_warehouse:
                            stocks_by_warehouse[barcode] = {}
                        if barcode not in stocks_metadata:
                            stocks_metadata[barcode] = {
                                "vendor_code": vendor_code,
                                "nm_id": nm_id,
                                "barcode": barcode
                            }
                        
                        if stock_warehouse:
                            stocks_by_warehouse[barcode][stock_warehouse] = stocks_by_warehouse[barcode].get(stock_warehouse, 0) + qty
        except Exception:
            stocks_by_warehouse = {}
            stocks_metadata = {}

        def build_items_for_wh(target_wh: str | None, show_all: bool = False) -> List[Dict[str, Any]]:
            items_local: List[Dict[str, Any]] = []
            
            # Get all products that have sales or stocks
            all_products = set(counts_total.keys())
            if show_all:
                # Add ALL products from stocks metadata (including those with zero stock)
                for barcode, metadata in stocks_metadata.items():
                    # Find product by barcode
                    found_in_sales = False
                    for prod, prod_barcode in barcode_by_product.items():
                        if prod_barcode == barcode:
                            all_products.add(prod)
                            found_in_sales = True
                            break
                    
                    # If barcode not found in sales, create a virtual product entry
                    if not found_in_sales:
                        # Use vendor_code from stocks metadata
                        virtual_prod = metadata["vendor_code"] or f"Товар с баркодом {barcode}"
                        # Add to mappings
                        barcode_by_product[virtual_prod] = barcode
                        if metadata["nm_id"]:
                            nm_by_product[virtual_prod] = metadata["nm_id"]
                        if metadata["vendor_code"]:
                            supplier_article_by_product[virtual_prod] = metadata["vendor_code"]
                        all_products.add(virtual_prod)
            
            for prod in all_products:
                qty = (by_wh.get(prod, {}).get(target_wh, 0) if target_wh else counts_total.get(prod, 0))
                
                # Include items with sales OR (if show_all) items with stocks
                if qty > 0 or (show_all and prod in barcode_by_product):
                    s = (by_wh_sum.get(prod, {}).get(target_wh, 0.0) if target_wh else revenue_total.get(prod, 0.0))
                    # Calculate stock quantity for the target warehouse
                    barcode = barcode_by_product.get(prod)
                    stock_qty = 0
                    if barcode and barcode in stocks_by_warehouse:
                        if target_wh:
                            # If specific warehouse selected, sum only for that warehouse
                            for wh_name, wh_qty in stocks_by_warehouse[barcode].items():
                                if (wh_name == target_wh or 
                                    (target_wh in wh_name) or 
                                    (wh_name in target_wh)):
                                    stock_qty += wh_qty
                        else:
                            # If no warehouse selected, sum all warehouses
                            stock_qty = sum(stocks_by_warehouse[barcode].values())
                    
                    # Only include if has sales or (if show_all) has any stock data
                    if qty > 0 or (show_all and prod in barcode_by_product):
                        items_local.append({
                            "product": prod,
                            "qty": qty,
                            "nm_id": nm_by_product.get(prod),
                            "barcode": barcode,
                            "supplier_article": supplier_article_by_product.get(prod),
                            "sum": round(float(s or 0.0), 2),
                            "photo": nm_to_photo.get(nm_by_product.get(prod)),
                            "stock_qty": stock_qty
                        })
            
            # Sort by quantity (descending), then by stock quantity (descending)
            items_local.sort(key=lambda x: (x["qty"], x["stock_qty"]), reverse=True)
            return items_local
        show_all = request.args.get("show_all_products") == "on"
        items = build_items_for_wh(warehouse, show_all)
        total_qty = sum(int(it.get("qty") or 0) for it in items)
        matrix = [{
            "product": p,
            "nm_id": nm_by_product.get(p),
            "barcode": barcode_by_product.get(p),
            "supplier_article": supplier_article_by_product.get(p),
            "total": counts_total[p],
            "by_wh": by_wh[p],
            "total_sum": round(float(revenue_total.get(p, 0.0)), 2),
            "by_wh_sum": by_wh_sum[p],
            "photo": nm_to_photo.get(nm_by_product.get(p)),
            "by_wh_stock": stocks_by_warehouse.get(barcode_by_product.get(p), {})
        } for p in counts_total.keys()]
        return jsonify({
            "items": items,
            "total_qty": total_qty,
            "date_from_fmt": date_from_fmt,
            "date_to_fmt": date_to_fmt,
            "warehouses": sorted(list(warehouses)),
            "matrix": matrix,
        }), 200
    except Exception as exc:
        return jsonify({"items": [], "error": str(exc)}), 200


@app.route("/api/report/sales/export", methods=["GET"])
@login_required
def api_report_sales_export():
    """Экспорт отчета по продажам в Excel формат"""
    from io import BytesIO
    from openpyxl import Workbook
    from openpyxl.styles import Font, Alignment, PatternFill
    from openpyxl.utils import get_column_letter
    
    cached = load_last_results()
    token = (current_user.wb_token or "") if current_user.is_authenticated else ""
    req_from = (request.args.get("date_from") or "").strip()
    req_to = (request.args.get("date_to") or "").strip()
    warehouse = (request.args.get("warehouse") or "").strip() or None
    
    try:
        if req_from and req_to and token:
            if (
                cached
                and current_user.is_authenticated
                and cached.get("_user_id") == current_user.id
                and cached.get("date_from") == req_from
                and cached.get("date_to") == req_to
            ):
                orders = cached.get("orders", [])
                date_from_fmt = datetime.strptime(req_from, "%Y-%m-%d").strftime("%d.%m.%Y")
                date_to_fmt = datetime.strptime(req_to, "%Y-%m-%d").strftime("%d.%m.%Y")
            else:
                raw_orders = fetch_orders_range(token, req_from, req_to)
                orders = to_rows(raw_orders, req_from, req_to)
                date_from_fmt = datetime.strptime(req_from, "%Y-%m-%d").strftime("%d.%m.%Y")
                date_to_fmt = datetime.strptime(req_to, "%Y-%m-%d").strftime("%d.%m.%Y")
        else:
            orders = (cached or {}).get("orders", [])
            date_from_fmt = (cached or {}).get("date_from_fmt") or (cached or {}).get("date_from")
            date_to_fmt = (cached or {}).get("date_to_fmt") or (cached or {}).get("date_to")
        
        # Build matrix for filtering
        counts_total: Dict[str, int] = defaultdict(int)
        by_wh: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
        revenue_total: Dict[str, float] = defaultdict(float)
        by_wh_sum: Dict[str, Dict[str, float]] = defaultdict(lambda: defaultdict(float))
        nm_by_product: Dict[str, Any] = {}
        barcode_by_product: Dict[str, Any] = {}
        supplier_article_by_product: Dict[str, Any] = {}
        warehouses = set()
        
        for r in orders:
            prod = str(r.get("Артикул продавца") or r.get("Артикул WB") or r.get("Баркод") or "Не указан")
            wh = str(r.get("Склад отгрузки") or "Не указан")
            warehouses.add(wh)
            counts_total[prod] += 1
            by_wh[prod][wh] += 1
            try:
                price = float(r.get("Цена с учетом всех скидок") or 0)
            except (TypeError, ValueError):
                price = 0.0
            revenue_total[prod] += price
            by_wh_sum[prod][wh] += price
            nmv = r.get("Артикул WB") or r.get("nmId") or r.get("nmID")
            if prod not in nm_by_product and nmv:
                nm_by_product[prod] = nmv
            barcode = r.get("Баркод") or r.get("barcode")
            if prod not in barcode_by_product and barcode:
                barcode_by_product[prod] = barcode
            supplier_article = r.get("Артикул продавца") or r.get("supplier_article")
            if prod not in supplier_article_by_product and supplier_article:
                supplier_article_by_product[prod] = supplier_article
        
        # Build items for export
        def _build_items(target_wh: str | None) -> List[Dict[str, Any]]:
            items_local: List[Dict[str, Any]] = []
            for prod, total in counts_total.items():
                qty = (by_wh[prod].get(target_wh, 0) if target_wh else total)
                if qty > 0:
                    s = (by_wh_sum.get(prod, {}).get(target_wh, 0.0) if target_wh else revenue_total.get(prod, 0.0))
                    items_local.append({
                        "product": prod,
                        "qty": qty,
                        "nm_id": nm_by_product.get(prod),
                        "barcode": barcode_by_product.get(prod),
                        "supplier_article": supplier_article_by_product.get(prod),
                        "sum": round(float(s or 0.0), 2),
                    })
            items_local.sort(key=lambda x: x["qty"], reverse=True)
            return items_local
        
        items = _build_items(warehouse) if orders else []
        
        # Create Excel workbook
        wb = Workbook()
        ws = wb.active
        ws.title = "Отчет по заказам"
        
        # Headers
        headers = ["Артикул WB", "Баркод", "Товар", "Кол-во", "Сумма"]
        for col, header in enumerate(headers, 1):
            cell = ws.cell(row=1, column=col, value=header)
            cell.font = Font(bold=True)
            cell.alignment = Alignment(horizontal='center')
            cell.fill = PatternFill(start_color="CCCCCC", end_color="CCCCCC", fill_type="solid")
        
        # Data rows
        for row, item in enumerate(items, 2):
            ws.cell(row=row, column=1, value=item["nm_id"] or "")
            ws.cell(row=row, column=2, value=item["barcode"] or "")
            ws.cell(row=row, column=3, value=item["product"])
            ws.cell(row=row, column=4, value=item["qty"])
            ws.cell(row=row, column=5, value=item["sum"])
        
        # Auto-adjust column widths
        for column in ws.columns:
            max_length = 0
            column_letter = get_column_letter(column[0].column)
            for cell in column:
                try:
                    if len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except:
                    pass
            adjusted_width = min(max_length + 2, 50)
            ws.column_dimensions[column_letter].width = adjusted_width
        
        # Generate filename
        now = datetime.now()
        warehouse_name = warehouse if warehouse else "Все"
        filename = f"Отчёт по заказам со складов ({warehouse_name})_{now.strftime('%d.%m.%Y_%H_%M')}.xlsx"
        
        # Save to BytesIO
        output = BytesIO()
        wb.save(output)
        output.seek(0)
        
        # Encode filename for HTTP headers
        import urllib.parse
        encoded_filename = urllib.parse.quote(filename.encode('utf-8'))
        
        return output.getvalue(), 200, {
            'Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            'Content-Disposition': f'attachment; filename*=UTF-8\'\'{encoded_filename}'
        }
        
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/report/finance", methods=["GET"]) 
@login_required
def report_finance_page():
    # initial render without data
    if not request.args.get("date_from") and not request.args.get("date_to"):
        # Try restore last viewed period and data for this user
        cached = load_last_results() or {}
        dfv = cached.get("finance_date_from") or ""
        dtv = cached.get("finance_date_to") or ""
        metrics = cached.get("finance_metrics") or {}
        rows = cached.get("finance_rows") or []
        return render_template(
            "finance_report.html",
            error=None,
            rows=rows,
            date_from_fmt=(metrics.get("date_from_fmt") or ""),
            date_to_fmt=(metrics.get("date_to_fmt") or ""),
            date_from_val=dfv,
            date_to_val=dtv,
            finance_metrics=metrics,
        ), 200
    token = (current_user.wb_token or "") if current_user.is_authenticated else ""
    if not token:
        return render_template(
            "finance_report.html",
            error="Требуется API токен (Статистика)",
            items=[],
            date_from_fmt="",
            date_to_fmt="",
            date_from_val=(request.args.get("date_from") or ""),
            date_to_val=(request.args.get("date_to") or ""),
        ), 200
    req_from = (request.args.get("date_from") or "").strip()
    req_to = (request.args.get("date_to") or "").strip()
    try:
        raw = fetch_finance_report(token, req_from, req_to)
        date_from_fmt = datetime.strptime(req_from, "%Y-%m-%d").strftime("%d.%m.%Y")
        date_to_fmt = datetime.strptime(req_to, "%Y-%m-%d").strftime("%d.%m.%Y")
    except Exception as exc:
        return render_template(
            "finance_report.html",
            error=str(exc),
            rows=[],
            date_from_fmt="",
            date_to_fmt="",
            date_from_val=req_from,
            date_to_val=req_to,
        ), 200
    return render_template(
        "finance_report.html",
        error=None,
        rows=raw,
        date_from_fmt=date_from_fmt,
        date_to_fmt=date_to_fmt,
        date_from_val=req_from,
        date_to_val=req_to,
    ), 200


@app.route("/api/report/finance", methods=["GET"]) 
@login_required
def api_report_finance():
    token = (current_user.wb_token or "") if current_user.is_authenticated else ""
    req_from = (request.args.get("date_from") or "").strip()
    req_to = (request.args.get("date_to") or "").strip()
    if not (token and req_from and req_to):
        return jsonify({"items": [], "error": None}), 200
    try:
        # Always fetch fresh report for the period (не кэшируем данные отчёта)
        raw = fetch_finance_report(token, req_from, req_to)
        total_qty = 0
        total_sum = 0.0
        # WB реализовал (по retail_amount с фильтрами по основаниям оплаты)
        wbr_plus = 0.0
        wbr_minus = 0.0
        total_logistics = 0.0
        total_storage = 0.0
        total_acceptance = 0.0
        total_for_pay = 0.0
        total_buyouts = 0.0
        total_returns = 0.0
        total_acquiring = 0.0
        total_commission_wb = 0.0
        total_other_deductions = 0.0
        total_penalties = 0.0
        total_additional_payment = 0.0
        # Компенсация брака составная метрика по ppvz_for_pay
        x1 = x2 = x3 = x4 = x5 = x6 = x7 = x8 = 0.0
        # Комиссия компоненты K1..K9
        k1 = k2 = k3 = k4 = k5 = k6 = k7 = k8 = k9 = 0.0
        # Компенсация ущерба компоненты U1..U14
        u1 = u2 = u3 = u4 = u5 = u6 = u7 = u8 = u9 = u10 = u11 = u12 = u13 = u14 = 0.0
        # Выкупы и возвраты считаем по колонке "supplier_oper_name"
        # Выкупы: one of ["Продажа","Сторно возвратов","Корректная продажа","коррекция продаж"] -> sum(retail_price)
        buyout_oper_values_lower = {"продажа", "сторно возвратов", "корректная продажа", "коррекция продаж"}
        for r in raw:
            try:
                total_qty += int(r.get("quantity") or 0)
            except Exception:
                pass
            try:
                total_sum += float(r.get("retail_amount") or 0.0)
            except Exception:
                pass
            try:
                total_logistics += float(r.get("delivery_rub") or 0.0)
            except Exception:
                pass
            try:
                total_storage += float(r.get("storage_fee") or 0.0)
            except Exception:
                pass
            try:
                total_acceptance += float(r.get("acceptance") or 0.0)
            except Exception:
                pass
            try:
                total_for_pay += float(r.get("ppvz_for_pay") or 0.0)
            except Exception:
                pass
            # Выкупы: суммируем розничную цену для нужных оснований оплаты
            try:
                oper = (r.get("supplier_oper_name") or "").strip()
                if oper and oper.lower() in buyout_oper_values_lower:
                    total_buyouts += float(r.get("retail_price") or 0.0)
            except Exception:
                pass
            # WB реализовал: retail_amount с суммированием по основаниям оплаты
            try:
                oper_lc = (r.get("supplier_oper_name") or "").strip().lower()
                amt = float(r.get("retail_amount") or 0.0)
                if oper_lc in {"продажа","сторно возвратов","корректная продажа","коррекция продаж"}:
                    wbr_plus += amt
                elif oper_lc in {"возврат","сторно продаж","корректный возврат"}:
                    wbr_minus += amt
            except Exception:
                pass
            # Возвраты: supplier_oper_name == "Возврат"; суммируем retail_price
            try:
                oper = (r.get("supplier_oper_name") or "").strip()
                if oper == "Возврат":
                    total_returns += float(r.get("retail_price") or 0.0)
            except Exception:
                pass
            # Эквайринг по формуле: E1 - E2 + E3
            # E1: doc_type_name == "Продажа" AND acquiring_percent > 0 -> sum acquiring_fee
            # E2: doc_type_name == "Возврат" AND acquiring_percent > 0 -> sum acquiring_fee
            # E3: supplier_oper_name == "Корректировка эквайринга" -> sum ppvz_for_pay
            try:
                dt_name = (r.get("doc_type_name") or "").strip()
                acq_pct = float(r.get("acquiring_percent") or 0.0)
                afee = float(r.get("acquiring_fee") or 0.0)
                if dt_name == "Продажа" and acq_pct > 0:
                    total_acquiring += afee
                elif dt_name == "Возврат" and acq_pct > 0:
                    total_acquiring -= afee
                # Корректировка эквайринга не включается в "Эквайринг", 
                # а учитывается отдельно в формуле "К перечислению"
            except Exception:
                pass
            try:
                total_commission_wb += float(r.get("ppvz_vw") or 0.0)
            except Exception:
                pass
            try:
                total_other_deductions += float(r.get("deduction") or 0.0)
                total_other_deductions += float(r.get("additional_payment") or 0.0)
            except Exception:
                pass
            try:
                total_penalties += float(r.get("penalty") or 0.0)
            except Exception:
                pass
            try:
                total_additional_payment += float(r.get("additional_payment") or 0.0)
            except Exception:
                pass
            # Компенсация брака: считаем X1..X8 по supplier_oper_name + doc_type_name, суммируя ppvz_for_pay
            try:
                oper_l = (r.get("supplier_oper_name") or "").strip().lower()
                doc_l = (r.get("doc_type_name") or "").strip().lower()
                pay_val = float(r.get("ppvz_for_pay") or 0.0)
                if oper_l == "компенсация брака" and doc_l == "продажа":
                    x1 += pay_val
                if oper_l == "оплата брака" and doc_l == "продажа":
                    x2 += pay_val
                if oper_l == "компенсация брака" and doc_l == "возврат":
                    x3 += pay_val
                if oper_l == "оплата брака" and doc_l == "возврат":
                    x4 += pay_val
                if oper_l == "частичная компенсация брака" and doc_l == "продажа":
                    x5 += pay_val
                if oper_l == "частичная компенсация брака" and doc_l == "возврат":
                    x6 += pay_val
                if oper_l == "добровольная компенсация при возврате" and doc_l == "продажа":
                    x7 += pay_val
                if oper_l == "добровольная компенсация при возврате" and doc_l == "возврат":
                    x8 += pay_val
                # Комиссия: K1..K9 — также на основе ppvz_for_pay
                if oper_l == "продажа":
                    k1 += pay_val
                if oper_l == "сторно возвратов":
                    k2 += pay_val
                if oper_l == "корректная продажа":
                    k3 += pay_val
                if oper_l == "коррекция продаж" and doc_l == "продажа":
                    k4 += pay_val
                if oper_l == "возврат":
                    k5 += pay_val
                if oper_l == "сторно продаж":
                    k6 += pay_val
                if oper_l == "коррекция продаж" and doc_l == "возврат":
                    k7 += pay_val
                if oper_l == "корректный возврат":
                    k8 += pay_val
                if oper_l == "корректировка эквайринга":
                    k9 += pay_val
                # Компенсация ущерба: U1..U14 по условиям, суммируем ppvz_for_pay
                if oper_l == "оплата потерянного товара" and doc_l == "продажа":
                    u1 += pay_val
                if oper_l == "компенсация потерянного товара" and doc_l == "продажа":
                    u2 += pay_val
                if oper_l == "оплата потерянного товара" and doc_l == "возврат":
                    u3 += pay_val
                if oper_l == "компенсация потерянного товара" and doc_l == "возврат":
                    u4 += pay_val
                if oper_l == "авансовая оплата за товар без движения" and doc_l == "продажа":
                    u5 += pay_val
                if oper_l == "авансовая оплата за товар без движения" and doc_l == "возврат":
                    u6 += pay_val
                if oper_l == "компенсация подмененного товара" and doc_l == "продажа":
                    u7 += pay_val
                if oper_l == "компенсация подмен" and doc_l == "продажа":
                    u8 += pay_val
                if oper_l == "компенсация подмененного товара" and doc_l == "возврат":
                    u9 += pay_val
                if oper_l == "компенсация подмен" and doc_l == "возврат":
                    u10 += pay_val
                if oper_l == "компенсация ущерба" and doc_l == "продажа":
                    u11 += pay_val
                if oper_l == "компенсация ущерба" and doc_l == "возврат":
                    u12 += pay_val
                if oper_l == "компенсация подмена" and doc_l == "продажа":
                    u13 += pay_val
                if oper_l == "компенсация подмен" and doc_l == "возврат":
                    u14 += pay_val
            except Exception:
                pass
        date_from_fmt = datetime.strptime(req_from, "%Y-%m-%d").strftime("%d.%m.%Y")
        date_to_fmt = datetime.strptime(req_to, "%Y-%m-%d").strftime("%d.%m.%Y")
        # Save last viewed period and last computed metrics/rows to restore on page reload
        try:
            save_last_results({
                "finance_date_from": req_from,
                "finance_date_to": req_to,
                "finance_rows": raw,
                "finance_metrics": {
                    "total_qty": int(total_qty),
                    "total_sum": round(total_sum, 2),
                    "total_logistics": round(total_logistics, 2),
                    "total_storage": round(total_storage, 2),
                    "total_acceptance": round(total_acceptance, 2),
                    "total_for_pay": round(total_for_pay, 2),
                    "total_buyouts": round(total_buyouts, 2),
                    "total_returns": round(total_returns, 2),
                    "revenue": round(revenue_calc, 2),
                    "total_commission": round(commission_total, 2),
                    "total_acquiring": round(total_acquiring, 2),
                    "total_other_deductions": round(total_other_deductions, 2),
                    "total_penalties": round(total_penalties, 2),
                    "total_defect_compensation": round(defect_comp, 2),
                    "total_damage_compensation": round(damage_comp, 2),
                    "total_additional_payment": round(total_additional_payment, 2),
                    "total_deductions": round(total_deductions, 2),
                    "total_for_transfer": round(total_for_transfer, 2),
                    "date_from_fmt": date_from_fmt,
                    "date_to_fmt": date_to_fmt,
                }
            })
        except Exception:
            pass
        revenue_calc = total_buyouts - total_returns
        defect_comp = x1 + x2 - x3 - x4 + x5 - x6 + x7 - x8
        total_wb_realized = wbr_plus - wbr_minus
        # Комиссия = Выручка - (K1+K2+K3+K4 - (K5+K6+K7+K8)) - Эквайринг
        # (K9 - корректировка эквайринга учитывается отдельно в формуле "К перечислению")
        commission_total = (
            revenue_calc
            - (k1 + k2 + k3 + k4)
            + (k5 + k6 + k7 + k8)
            - total_acquiring
        )
        damage_comp = u1 + u2 - u3 - u4 + u5 - u6 + u7 + u8 - u9 - u10 + u11 - u12 + u13 - u14
        
        # Удержания и компенсации WB = Комиссия + Эквайринг + Логистика + Хранение + Прочие удержания + Приёмка - Компенсация брака - Компенсация ущерба + Штрафы + Доплаты
        total_deductions = (
            commission_total + 
            total_acquiring + 
            total_logistics + 
            total_storage + 
            total_other_deductions + 
            total_acceptance - 
            defect_comp - 
            damage_comp + 
            total_penalties + 
            total_additional_payment
        )
        
        # К перечислению = Выручка - Удержания и компенсации WB + E3
        # E3: supplier_oper_name == "Корректировка эквайринга" -> sum ppvz_for_pay
        e3_correction = 0
        for r in raw:
            try:
                oper_name = (r.get("supplier_oper_name") or "").strip()
                if oper_name == "Корректировка эквайринга":
                    e3_correction += float(r.get("ppvz_for_pay") or 0.0)
            except Exception:
                pass
        
        total_for_transfer = revenue_calc - total_deductions + e3_correction
        
        return jsonify({
            "rows": raw,
            "total_qty": int(total_qty),
            "total_sum": round(total_sum, 2),
            "total_logistics": round(total_logistics, 2),
            "total_storage": round(total_storage, 2),
            "total_acceptance": round(total_acceptance, 2),
            "total_for_pay": round(total_for_pay, 2),
            "total_buyouts": round(total_buyouts, 2),
            "total_returns": round(total_returns, 2),
            "revenue": round(revenue_calc, 2),
            "total_wb_realized": round(total_wb_realized, 2),
            "total_commission": round(commission_total, 2),
            "total_acquiring": round(total_acquiring, 2),
            "total_commission_wb": round(total_commission_wb, 2),
            "total_other_deductions": round(total_other_deductions, 2),
            "total_penalties": round(total_penalties, 2),
            "total_defect_compensation": round(defect_comp, 2),
                                "total_damage_compensation": round(damage_comp, 2),
                    "total_additional_payment": round(total_additional_payment, 2),
                    "total_deductions": round(total_deductions, 2),
                    "total_for_transfer": round(total_for_transfer, 2),
                    "date_from_fmt": date_from_fmt,
                    "date_to_fmt": date_to_fmt,
        }), 200
    except Exception as exc:
        return jsonify({"items": [], "error": str(exc)}), 200


@app.route("/report/finance/export", methods=["GET"]) 
@login_required
def export_finance_xls():
    token = (current_user.wb_token or "") if current_user.is_authenticated else ""
    req_from = (request.args.get("date_from") or "").strip()
    req_to = (request.args.get("date_to") or "").strip()
    if not (token and req_from and req_to):
        return ("Требуются даты и токен", 400)
    try:
        # Always fetch fresh for export
        rows = fetch_finance_report(token, req_from, req_to)
        # Build XLS (not XLSX) to match requirement "XLS"
        try:
            import xlwt  # type: ignore
        except Exception:
            return ("На сервере отсутствует зависимость xlwt (для .xls)", 500)
        wb = xlwt.Workbook()
        ws = wb.add_sheet("finance")
        header_style = xlwt.easyxf("font: bold on; align: horiz center")
        num_style = xlwt.easyxf("align: horiz right")
        cols = [
            'realizationreport_id','date_from','date_to','create_dt','currency_name','suppliercontract_code','rrd_id','gi_id','dlv_prc','fix_tariff_date_from','fix_tariff_date_to','subject_name','nm_id','brand_name','sa_name','ts_name','barcode','doc_type_name','quantity','retail_price','retail_amount','sale_percent','commission_percent','office_name','supplier_oper_name','order_dt','sale_dt','rr_dt','shk_id','retail_price_withdisc_rub','delivery_amount','return_amount','delivery_rub','gi_box_type_name','product_discount_for_report','supplier_promo','ppvz_spp_prc','ppvz_kvw_prc_base','ppvz_kvw_prc','sup_rating_prc_up','is_kgvp_v2','ppvz_sales_commission','ppvz_for_pay','ppvz_reward','acquiring_fee','acquiring_percent','payment_processing','acquiring_bank','ppvz_vw','ppvz_vw_nds','ppvz_office_name','ppvz_office_id','ppvz_supplier_id','ppvz_supplier_name','ppvz_inn','declaration_number','bonus_type_name','sticker_id','site_country','srv_dbs','penalty','additional_payment','rebill_logistic_cost','rebill_logistic_org','storage_fee','deduction','acceptance','assembly_id','kiz','srid','report_type','is_legal_entity','trbx_id','installment_cofinancing_amount','wibes_wb_discount_percent','cashback_amount','cashback_discount'
        ]
        headers_ru = [
            "Номер отчёта","Дата начала периода","Дата конца периода","Дата формирования","Валюта","Договор","Номер строки","Номер поставки","Фикс. коэф. склада","Начало фиксации","Конец фиксации","Предмет","Артикул WB","Бренд","Артикул продавца","Размер","Баркод","Тип документа","Количество","Цена розничная","Реализовано (Пр)","Скидка, %","кВВ, %","Склад","Обоснование оплаты","Дата заказа","Дата продажи","Дата операции","Штрихкод","Розничная с уч. скидки","Кол-во доставок","Кол-во возвратов","Доставка, руб","Тип коробов","Итог. продукт. скидка, %","Промокод, %","СПП, %","Базовый кВВ без НДС, %","Итоговый кВВ без НДС, %","Снижение кВВ (рейтинг), %","Снижение кВВ (акция), %","Вознаграждение с продаж","К перечислению продавцу","Возмещение ПВЗ","Эквайринг","Эквайринг, %","Тип платежа эквайринга","Банк-эквайер","Вознаграждение ВВ","НДС ВВ","Офис доставки","ID офиса","ID партнёра","Партнёр","ИНН партнёра","№ декларации","Тип логистики/штрафа","ID стикера","Страна продажи","Платная доставка","Штрафы","Корректировка ВВ","Возмещение логистики","Организатор перевозки","Хранение","Удержания","Платная приёмка","ID сборочного","Код маркировки","SRID","Тип отчёта","B2B","ID короба приёмки","Софинансирование","Скидка Wibes, %","Баллы (удержано)","Компенсация скидки"
        ]
        for ci, title in enumerate(headers_ru, start=1):
            ws.write(0, ci-1, title, header_style)
        row_idx = 1
        for r in rows:
            for ci, key in enumerate(cols, start=1):
                val = r.get(key)
                if isinstance(val, (dict, list)):
                    try:
                        import json as _json
                        val = _json.dumps(val, ensure_ascii=False)
                    except Exception:
                        val = str(val)
                ws.write(row_idx, ci-1, val if val is not None else "")
            row_idx += 1
        out = io.BytesIO()
        wb.save(out)
        out.seek(0)
        filename = f"finance_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xls"
        return send_file(out, mimetype="application/vnd.ms-excel", as_attachment=True, download_name=filename)
    except requests.HTTPError as http_err:
        return (f"Ошибка API: {http_err.response.status_code}", 502)
    except Exception as exc:
        return (f"Ошибка: {exc}", 500)

@app.route("/fbs", methods=["GET", "POST"]) 
@login_required
def fbs_page():
    error = None
    token = (current_user.wb_token or "") if current_user.is_authenticated else ""
    rows: List[Dict[str, Any]] = []
    # Load cached tasks to show immediately
    cached_tasks = load_fbs_tasks_cache() or {}
    cached_rows = cached_tasks.get("rows") or []
    rows = cached_rows

    if request.method == "POST":
        pass  # Раньше была ручная проверка; теперь обновляем через JS-кнопку в блоке

    # If enrichment impossible due to empty products cache
    products_hint = None
    prod_cached_now = load_products_cache()
    if not prod_cached_now or not ((prod_cached_now or {}).get("items")):
        products_hint = "Для отображения фото товара и баркода обновите данные на странице Товары"

    # Не блокируем рендер страницы: текущие задания подтянем AJAX-ом
    return render_template("fbs.html", error=error, rows=rows, products_hint=products_hint, current_orders=[])


@app.route("/fbs/export", methods=["POST"]) 
@login_required
def fbs_export():
    token = (current_user.wb_token or "") if current_user.is_authenticated else ""
    if not token:
        return ("Требуется API токен", 400)
    try:
        raw = fetch_fbs_new_orders(token)
        raw_sorted = sorted(raw, key=_extract_created_at)
        rows = to_fbs_rows(raw_sorted)
        # Enrich from products cache
        prod_cached = load_products_cache()
        items = (prod_cached or {}).get("items") or []
        by_article: Dict[str, Dict[str, Any]] = {}
        by_nm: Dict[int, Dict[str, Any]] = {}
        for it in items:
            art = (it.get("supplier_article") or it.get("vendorCode") or "").strip()
            if art:
                by_article.setdefault(art, it)
            nmv = it.get("nm_id") or it.get("nmID")
            try:
                if nmv:
                    by_nm[int(nmv)] = it
            except Exception:
                pass
        for r in rows:
            art = (r.get("Наименование товара") or "").strip()
            hit = by_article.get(art)
            if not hit and r.get("nm_id"):
                try:
                    hit = by_nm.get(int(r["nm_id"]))
                except Exception:
                    hit = None
            if hit:
                if hit.get("barcode"):
                    r["barcode"] = hit.get("barcode")
                elif isinstance(hit.get("barcodes"), list) and hit.get("barcodes"):
                    r["barcode"] = str(hit.get("barcodes")[0])
                else:
                    sizes = hit.get("sizes") or []
                    if isinstance(sizes, list):
                        for s in sizes:
                            bar_list = s.get("skus") or s.get("barcodes")
                            if isinstance(bar_list, list) and bar_list:
                                r["barcode"] = str(bar_list[0])
                                break

        # Aggregate: Наименование + Баркод -> Количество
        agg: Dict[tuple[str, str], int] = {}
        for r in rows:
            name = (r.get("Наименование товара") or "").strip()
            barcode = (r.get("barcode") or "").strip()
            key = (name, barcode)
            agg[key] = agg.get(key, 0) + 1

        # Build XLS (not XLSX)
        try:
            import xlwt  # type: ignore
        except Exception:
            return ("На сервере отсутствует зависимость xlwt (для .xls)", 500)
        wb = xlwt.Workbook()
        ws = wb.add_sheet("FBS")
        header_style = xlwt.easyxf("font: bold on; align: horiz center")
        num_style = xlwt.easyxf("align: horiz right")
        ws.write(0, 0, "Наименование", header_style)
        ws.write(0, 1, "Баркод", header_style)
        ws.write(0, 2, "Количество", header_style)
        row_idx = 1
        for (name, barcode), qty in sorted(agg.items(), key=lambda x: (-x[1], x[0][0])):
            ws.write(row_idx, 0, name)
            ws.write(row_idx, 1, barcode)
            ws.write(row_idx, 2, qty, num_style)
            row_idx += 1
        out = io.BytesIO()
        wb.save(out)
        out.seek(0)
        filename = f"fbs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xls"
        return send_file(out, mimetype="application/vnd.ms-excel", as_attachment=True, download_name=filename)
    except requests.HTTPError as http_err:
        return (f"Ошибка API: {http_err.response.status_code}", 502)
    except Exception as exc:
        return (f"Ошибка: {exc}", 500)


@app.route("/api/fbs/tasks", methods=["GET"]) 
@login_required
def api_fbs_tasks():
    token = (current_user.wb_token or "") if current_user.is_authenticated else ""
    refresh = request.args.get("refresh") in ("1", "true", "True")
    if not token and refresh:
        return jsonify({"items": [], "updated_at": None})
    try:
        if not refresh:
            cached = load_fbs_tasks_cache() or {}
            if cached.get("rows"):
                return jsonify({"items": cached.get("rows"), "updated_at": cached.get("updated_at")})
        # Fetch fresh
        print(f"=== FETCHING FRESH FBS TASKS ===")
        raw = fetch_fbs_new_orders(token)
        print(f"Raw tasks count: {len(raw)}")
        raw_sorted = sorted(raw, key=_extract_created_at)
        rows = to_fbs_rows(raw_sorted)
        print(f"Processed rows count: {len(rows)}")
        if rows:
            print(f"First row: {rows[0]}")
        # Enrich from products cache
        prod_cached = load_products_cache() or {}
        items = (prod_cached.get("items") or [])
        by_article: Dict[str, Dict[str, Any]] = {}
        by_nm: Dict[int, Dict[str, Any]] = {}
        for it in items:
            art = (it.get("supplier_article") or it.get("vendorCode") or "").strip()
            if art:
                by_article.setdefault(art, it)
            nm = it.get("nm_id") or it.get("nmID")
            if nm:
                try:
                    by_nm[int(nm)] = it
                except Exception:
                    pass
        for r in rows:
            art = (r.get("Наименование товара") or "").strip()
            hit = by_article.get(art)
            if not hit and r.get("nm_id"):
                try:
                    hit = by_nm.get(int(r["nm_id"]))
                except Exception:
                    hit = None
            if hit:
                if hit.get("barcode"):
                    r["barcode"] = hit.get("barcode")
                elif isinstance(hit.get("barcodes"), list) and hit.get("barcodes"):
                    r["barcode"] = str(hit.get("barcodes")[0])
                else:
                    sizes = hit.get("sizes") or []
                    if isinstance(sizes, list):
                        for s in sizes:
                            bar_list = s.get("skus") or s.get("barcodes")
                            if isinstance(bar_list, list) and bar_list:
                                r["barcode"] = str(bar_list[0])
                                break
                r["photo"] = hit.get("photo")
        now_str = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
        save_fbs_tasks_cache({"rows": rows, "updated_at": now_str})
        return jsonify({"items": rows, "updated_at": now_str})
    except Exception as exc:
        return jsonify({"items": [], "error": str(exc)}), 200
@app.route("/api/fbs/orders/load-more", methods=["POST"]) 
@login_required
def api_fbs_orders_load_more():
    token = (current_user.wb_token or "") if current_user.is_authenticated else ""
    if not token:
        return jsonify({"items": [], "next": None}), 200
    try:
        cursor = session.get("fbs_next_cursor")
        if not cursor:
            return jsonify({"items": [], "next": None}), 200
        page = fetch_fbs_orders(token, limit=30, next_cursor=cursor)
        items, next_cursor = _normalize_fbs_orders_page(page)
        try:
            items.sort(key=_extract_created_at, reverse=True)
        except Exception:
            pass
        # Merge statuses
        need_ids: List[int] = []
        for it in items:
            oid = it.get("id") or it.get("orderId") or it.get("ID")
            try:
                if oid is not None:
                    need_ids.append(int(oid))
            except Exception:
                pass
        if need_ids:
            st = fetch_fbs_statuses(token, need_ids[:1000])
            arr = st.get("orders") or st.get("data") or st
            m: Dict[int, Any] = {}
            if isinstance(arr, list):
                for x in arr:
                    try:
                        m[int(x.get("id") or x.get("orderId") or 0)] = x
                    except Exception:
                        continue
            for it in items:
                try:
                    oid = int(it.get("id") or it.get("orderId") or it.get("ID") or 0)
                    stx = m.get(oid) or {}
                    it["statusName"] = stx.get("statusName") or stx.get("status") or it.get("statusName") or it.get("status")
                    it["status"] = stx.get("status") or it.get("status")
                except Exception:
                    pass
        session["fbs_next_cursor"] = next_cursor
        # Вернём курсор для «Загрузить ещё»: если мы собрали несколько страниц, last_next уже сохранён в сессии,
        # но фронту нужен любой ненулевой next, чтобы показать кнопку. Используем тот из сессии.
        return jsonify({"items": items, "next": session.get("fbs_next_cursor")}), 200
    except Exception as exc:
        return jsonify({"items": [], "next": None, "error": str(exc)}), 200


@app.route("/api/fbs/orders", methods=["GET"]) 
@login_required
def api_fbs_orders_first():
    token = (current_user.wb_token or "") if current_user.is_authenticated else ""
    if not token:
        return jsonify({"items": [], "next": None}), 200
    try:
        items, last_next = get_orders_with_status(token, need_count=30, start_next=None)
        session["fbs_next_cursor"] = last_next
        return jsonify({"items": items, "next": last_next}), 200
    except Exception as exc:
        return jsonify({"items": [], "next": None, "error": str(exc)}), 200


@app.route("/api/fbs/orders/with-status", methods=["GET"]) 
@login_required
def api_fbs_orders_with_status():
    token = (current_user.wb_token or "") if current_user.is_authenticated else ""
    if not token:
        return jsonify({"items": [], "next": None}), 200
    try:
        limit = request.args.get("limit", default="30")
        try:
            limit_i = max(1, min(200, int(limit)))
        except Exception:
            limit_i = 30
        next_val = request.args.get("next")
        items, last_next = get_orders_with_status(token, need_count=limit_i, start_next=next_val)
        session["fbs_next_cursor"] = last_next
        return jsonify({"items": items, "next": last_next}), 200
    except Exception as exc:
        return jsonify({"items": [], "next": None, "error": str(exc)}), 200


def _collect_fbs_orders_for_supplies(token: str, max_pages: int = 5, limit: int = 200) -> List[Dict[str, Any]]:
    collected: List[Dict[str, Any]] = []
    seen: set[int] = set()
    cursor: Any = 0
    pages = 0
    while pages < max_pages:
        page = fetch_fbs_orders(token, limit=limit, next_cursor=cursor)
        items, next_cursor = _normalize_fbs_orders_page(page)
        if not items:
            break
        for it in items:
            oid = it.get("id") or it.get("orderId") or it.get("ID")
            try:
                if oid is not None:
                    oid_i = int(oid)
                    if oid_i in seen:
                        continue
                    seen.add(oid_i)
            except Exception:
                pass
            collected.append(it)
        if not next_cursor or next_cursor == cursor:
            break
        cursor = next_cursor
        pages += 1
    return collected


def _aggregate_fbs_supplies(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    groups: Dict[str, Dict[str, Any]] = {}
    for it in items:
        sid = it.get("supplyId") or it.get("supplyID") or it.get("supply_id")
        if not sid:
            continue
        d_raw = it.get("createdAt") or it.get("dateCreated") or it.get("date")
        dt = parse_wb_datetime(str(d_raw)) if d_raw else None
        dt_msk = to_moscow(dt) if dt else None
        status_name = (it.get("statusName") or it.get("status") or "").strip()
        g = groups.get(str(sid))
        if not g:
            g = {"supplyId": str(sid), "count": 0, "last_dt": dt_msk, "status_counts": {}}
            groups[str(sid)] = g
        g["count"] = int(g.get("count", 0)) + 1
        # last date in the supply
        if dt_msk and (g.get("last_dt") is None or dt_msk > g.get("last_dt")):
            g["last_dt"] = dt_msk
        # collect status counts
        if status_name:
            sc = g.get("status_counts") or {}
            sc[status_name] = int(sc.get(status_name, 0)) + 1
            g["status_counts"] = sc

    result: List[Dict[str, Any]] = []
    for g in groups.values():
        dt_val = g.get("last_dt")
        date_str = dt_val.strftime("%d.%m.%Y %H:%M") if dt_val else ""
        ts = int(dt_val.timestamp()) if dt_val else 0
        # determine supply status: priority rules, then most frequent
        status_counts: Dict[str, int] = g.get("status_counts") or {}
        status_final = ""
        if status_counts:
            # Priority: Отгрузите поставку -> Поставку приняли -> otherwise most frequent
            lowered = {k.lower(): k for k in status_counts.keys()}
            # Look for 'отгрузите поставку'
            for lk, orig in lowered.items():
                if "отгруз" in lk:
                    status_final = orig
                    break
            if not status_final:
                for lk, orig in lowered.items():
                    if "принял" in lk or "приняли" in lk:
                        status_final = orig
                        break
            if not status_final:
                status_final = max(status_counts.items(), key=lambda x: x[1])[0]
        result.append({
            "supplyId": g["supplyId"],
            "date": date_str,
            "ts": ts,
            "count": g["count"],
            "status": status_final,
        })
    result.sort(key=lambda x: x.get("ts", 0), reverse=True)
    return result


@app.route("/api/fbs/supplies", methods=["GET"]) 
@login_required
def api_fbs_supplies():
    token = (current_user.wb_token or "") if current_user.is_authenticated else ""
    if not token:
        return jsonify({"items": [], "lastUpdated": None}), 200

    # Support two modes: cached read (default) and refresh=1 to re-fetch
    refresh_flag = request.args.get("refresh") in ("1", "true", "True")
    limit_param = request.args.get("limit", default="5")
    offset_param = request.args.get("offset", default="0")
    try:
        limit_i = max(1, min(1000, int(limit_param)))
    except Exception:
        limit_i = 5
    try:
        offset_i = int(offset_param)
    except Exception:
        offset_i = 0

    # Always load from API for now (to ensure we get fresh data)
    # TODO: Implement proper caching later

    try:
        # Load ALL supplies at once (fast - 262ms as per user's test)
        headers_list = [
            {"Authorization": f"{token}"},
            {"Authorization": f"Bearer {token}"},
        ]
        all_supplies_raw = []
        
        for hdrs in headers_list:
            try:
                resp = get_with_retry(FBS_SUPPLIES_LIST_URL, hdrs, params={"limit": 1000, "next": 0})
                data = resp.json()
                
                # Handle both list and dict response formats
                if isinstance(data, list):
                    all_supplies_raw = data
                elif isinstance(data, dict):
                    all_supplies_raw = data.get("supplies", []) or data.get("data", []) or []
                else:
                    continue
                break
            except Exception as e:
                continue
        
        if not all_supplies_raw:
            orders = _collect_fbs_orders_for_supplies(token, max_pages=10, limit=200)
            all_supplies_raw = _aggregate_fbs_supplies(orders)
        
        # Sort all supplies by creation date (newest first)
        all_supplies_raw.sort(key=lambda x: x.get("createdAt", ""), reverse=True)
        
        # Get only the supplies we need to process (based on offset and limit)
        supplies_to_process = all_supplies_raw[offset_i:offset_i + limit_i]
        
        # Process only the supplies we need
        processed_supplies = []
        for s in supplies_to_process:
            supply_id = s.get("id")
            if supply_id:
                # Get count from orders for this supply (API doesn't provide count directly)
                count = 0
                try:
                    url = FBS_SUPPLY_ORDERS_URL.replace("{supplyId}", str(supply_id))
                    orders_resp = get_with_retry(url, headers_list[0], params={})
                    orders_data = orders_resp.json()
                    orders = orders_data.get("orders", []) if isinstance(orders_data, dict) else []
                    count = len(orders) if isinstance(orders, list) else 0
                except Exception as e:
                    count = 0
                
                # Enrich with supply info (createdAt, done, closedAt) and compute status
                created_at = s.get("createdAt")
                done = s.get("done")
                closed_at = s.get("closedAt")
                
            # Status per requirement
            if done:
                status_label = "Отгружено"
                try:
                    _sdt = parse_wb_datetime(str(closed_at))
                    _sdt_msk = to_moscow(_sdt) if _sdt else None
                    status_dt = _sdt_msk.strftime("%d.%m.%Y %H:%M") if _sdt_msk else str(closed_at)
                except Exception:
                    status_dt = str(closed_at)
            else:
                status_label = "Не отгружена"
                status_dt = None
                
            # Date column should use createdAt
            date_dt = parse_wb_datetime(str(created_at)) if created_at else None
            date_msk = to_moscow(date_dt) if date_dt else None
            date_str = date_msk.strftime("%d.%m.%Y %H:%M") if date_msk else ""
            
            processed_supplies.append({
                "supplyId": str(supply_id),
                "date": date_str,
                "count": count,
                "status": status_label,
                "statusDt": status_dt or "",
            })

        # Save raw supplies to cache (for future pagination)
        now_msk = datetime.now(MOSCOW_TZ)
        cache_payload = {
            "all_supplies_raw": all_supplies_raw,  # Store raw data for pagination
            "lastUpdated": now_msk.strftime("%d.%m.%Y %H:%M"),
            "ts": int(now_msk.timestamp())
        }
        save_fbs_supplies_cache(cache_payload)

        return jsonify({
            "items": processed_supplies,
            "lastUpdated": cache_payload["lastUpdated"],
            "total": len(all_supplies_raw),
            "hasMore": offset_i + limit_i < len(all_supplies_raw)
        }), 200
    except Exception as exc:
        return jsonify({"items": [], "error": str(exc), "lastUpdated": None}), 200


@app.route("/api/fbs/supplies/<supply_id>/orders", methods=["GET"]) 
@login_required
def api_fbs_supply_orders(supply_id: str):
    token = (current_user.wb_token or "") if current_user.is_authenticated else ""
    if not token:
        return jsonify({"items": []}), 200
    headers_list = [
        {"Authorization": f"{token}"},
        {"Authorization": f"Bearer {token}"},
    ]
    last_err = None
    try:
        for hdrs in headers_list:
            try:
                url = FBS_SUPPLY_ORDERS_URL.replace("{supplyId}", str(supply_id))
                resp = get_with_retry(url, hdrs, params={})
                data = resp.json()
                items = data.get("orders") if isinstance(data, dict) else (data if isinstance(data, list) else [])
                if not isinstance(items, list):
                    items = []
                # Minimal normalization for frontend: id, article, barcode, nmId, photo
                norm = []
                prod_cached = load_products_cache() or {}
                by_nm: Dict[int, Dict[str, Any]] = {}
                try:
                    for it in (prod_cached.get("items") or []):
                        nmv = it.get("nm_id") or it.get("nmID")
                        if nmv:
                            by_nm[int(nmv)] = it
                except Exception:
                    pass
                for it in items:
                    if not isinstance(it, dict):
                        continue
                    nm = it.get("nmId") or it.get("nmID")
                    photo = None
                    barcode = None
                    # format createdAt for item
                    created_raw = it.get("createdAt") or it.get("dateCreated") or it.get("date")
                    try:
                        _dt = parse_wb_datetime(str(created_raw)) if created_raw else None
                        _dt_msk = to_moscow(_dt) if _dt else None
                        created_str = _dt_msk.strftime("%d.%m.%Y %H:%M") if _dt_msk else (str(created_raw) if created_raw else "")
                    except Exception:
                        created_str = str(created_raw) if created_raw else ""
                    if nm:
                        try:
                            hit = by_nm.get(int(nm))
                        except Exception:
                            hit = None
                        if hit:
                            photo = hit.get("photo")
                            if hit.get("barcode"):
                                barcode = hit.get("barcode")
                            elif isinstance(hit.get("barcodes"), list) and hit.get("barcodes"):
                                barcode = str(hit.get("barcodes")[0])
                            else:
                                sizes = hit.get("sizes") or []
                                if isinstance(sizes, list):
                                    for s in sizes:
                                        bl = s.get("skus") or s.get("barcodes")
                                        if isinstance(bl, list) and bl:
                                            barcode = str(bl[0])
                                            break
                    norm.append({
                        "id": it.get("id") or it.get("orderId") or it.get("ID"),
                        "article": it.get("article") or it.get("vendorCode") or "",
                        "barcode": (it.get("skus")[0] if isinstance(it.get("skus"), list) and it.get("skus") else None) or barcode or "",
                        "nmId": nm,
                        "photo": photo,
                        "createdAt": created_str,
                    })
                return jsonify({"items": norm}), 200
            except Exception as e:
                last_err = e
                continue
        if last_err:
            raise last_err
        return jsonify({"items": []}), 200
    except Exception as exc:
        return jsonify({"items": [], "error": str(exc)}), 200


@app.route("/api/fbs/supplies/<supply_id>/orders/<order_id>", methods=["PATCH"])
@login_required
def api_fbs_add_order_to_supply(supply_id: str, order_id: str):
    """Добавить сборочное задание в поставку"""
    token = (current_user.wb_token or "") if current_user.is_authenticated else ""
    if not token:
        return jsonify({"error": "No token"}), 401
    
    headers_list = [
        {"Authorization": f"{token}"},
        {"Authorization": f"Bearer {token}"},
    ]
    
    # URL для добавления задания в поставку
    url = f"https://marketplace-api.wildberries.ru/api/v3/supplies/{supply_id}/orders/{order_id}"
    
    last_err = None
    for hdrs in headers_list:
        try:
            resp = requests.patch(url, headers=hdrs, timeout=30)
            if resp.status_code in [200, 204]:  # 204 = No Content (успешно)
                return jsonify({"success": True}), 200
            elif resp.status_code == 409:
                # Задание уже в поставке
                return jsonify({"error": "Order already in supply"}), 409
            else:
                last_err = f"HTTP {resp.status_code}: {resp.text}"
                continue
        except Exception as e:
            last_err = str(e)
            continue
    
    return jsonify({"error": last_err or "Unknown error"}), 500


@app.route("/api/fbs/supplies/create", methods=["POST"])
@login_required
def api_fbs_create_supply():
    """Создать новую поставку"""
    token = (current_user.wb_token or "") if current_user.is_authenticated else ""
    if not token:
        return jsonify({"error": "No token"}), 401
    
    headers_list = [
        {"Authorization": f"{token}"},
        {"Authorization": f"Bearer {token}"},
    ]
    
    # URL для создания поставки
    url = "https://marketplace-api.wildberries.ru/api/v3/supplies"
    
    last_err = None
    for hdrs in headers_list:
        try:
            # Добавляем Content-Type для JSON
            hdrs_with_content_type = hdrs.copy()
            hdrs_with_content_type["Content-Type"] = "application/json"
            
            resp = requests.post(url, headers=hdrs_with_content_type, json={}, timeout=30)
            if resp.status_code in [200, 201]:
                data = resp.json()
                supply_id = data.get("id") or data.get("supplyId") or "Неизвестно"
                return jsonify({"success": True, "supplyId": supply_id}), 200
            else:
                last_err = f"HTTP {resp.status_code}: {resp.text}"
                continue
        except Exception as e:
            last_err = str(e)
            continue
    
    return jsonify({"error": last_err or "Unknown error"}), 500


@app.route("/coefficients", methods=["GET", "POST"]) 
@login_required
def coefficients_page():
    error = None
    token = (current_user.wb_token or "") if current_user.is_authenticated else ""
    items: List[Dict[str, Any]] | None = None
    warehouses: List[str] = []
    date_keys: List[str] = []
    date_labels: List[str] = []
    grid: Dict[str, Dict[str, Dict[str, Any]]] = {}
    generated_at = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
    period_label = None

    if not token:
        error = "Укажите токен API на странице Настройки"
    else:
        try:
            items = fetch_acceptance_coefficients(token)
            if not isinstance(items, list):
                items = []
            warehouses, date_keys, date_labels, grid = build_acceptance_grid(items, days=14)
            if date_keys:
                try:
                    start = datetime.strptime(date_keys[0], "%Y-%m-%d").date()
                    end = datetime.strptime(date_keys[-1], "%Y-%m-%d").date()
                    period_label = f"{start.strftime('%d.%m')} по {end.strftime('%d.%m')}"
                except Exception:
                    period_label = None
        except requests.HTTPError as http_err:
            error = f"Ошибка API: {http_err.response.status_code}"
        except Exception as exc:
            error = f"Ошибка: {exc}"

    return render_template(
        "coefficients.html",
        error=error,
        warehouses=warehouses,
        date_labels=date_labels,
        date_keys=date_keys,
        grid=grid,
        generated_at=generated_at,
        period_label=period_label,
    )


@app.route("/api/acceptance-coefficients", methods=["GET"]) 
@login_required
def api_acceptance_coefficients():
    token = (current_user.wb_token or "") if current_user.is_authenticated else ""
    if not token:
        return jsonify({"error": "no_token"}), 401
    try:
        items = fetch_acceptance_coefficients(token) or []
        if not isinstance(items, list):
            items = []
        warehouses, date_keys, date_labels, grid = build_acceptance_grid(items, days=14)
        return jsonify({
            "warehouses": warehouses,
            "date_keys": date_keys,
            "date_labels": date_labels,
            "grid": grid,
            "lastUpdated": datetime.now().strftime("%d.%m.%Y %H:%M:%S"),
        })
    except requests.HTTPError as http_err:
        status = 502
        retry_after = None
        try:
            if http_err.response is not None:
                if http_err.response.status_code:
                    status = http_err.response.status_code
                retry_after = http_err.response.headers.get("Retry-After")
        except Exception:
            status = 502
        cached = load_stocks_cache()
        return jsonify({
            "error": "http",
            "status": status,
            "retry_after": retry_after,
            "updated_at": (cached.get("updated_at") if cached and cached.get("_user_id") == current_user.id else None)
        }), status
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


def post_with_retry(url: str, headers: Dict[str, str], json_body: Dict[str, Any], max_retries: int = 8) -> requests.Response:
    last_exc: Exception | None = None
    last_resp: requests.Response | None = None
    for attempt in range(max_retries):
        try:
            resp = requests.post(url, headers=headers, json=json_body, timeout=60)
            last_resp = resp
            if resp.status_code in (429, 500, 502, 503, 504):
                retry_after = resp.headers.get("Retry-After")
                if retry_after is not None:
                    try:
                        sleep_s = float(retry_after)
                    except ValueError:
                        sleep_s = 1.0
                else:
                    sleep_s = min(15, 0.8 * (2 ** attempt) + random.uniform(0, 0.7))
                time.sleep(sleep_s)
                continue
            resp.raise_for_status()
            return resp
        except requests.RequestException as exc:
            last_exc = exc
            time.sleep(min(8, 0.5 * (2 ** attempt) + random.uniform(0, 0.5)))
            continue
    if last_exc:
        raise last_exc
    if last_resp is not None:
        raise requests.HTTPError(f"HTTP {last_resp.status_code} after {max_retries} retries", response=last_resp)
    raise RuntimeError("Request failed after retries")


def fetch_cards_list(token: str, nm_ids: List[int] | None = None, cursor: Dict[str, Any] | None = None, limit: int = 100) -> Dict[str, Any]:
    # Build request body per WB docs: settings.cursor + settings.filter
    base_cursor = {"limit": limit, "nmID": 0}
    if cursor:
        base_cursor.update(cursor)
    body: Dict[str, Any] = {
        "settings": {
            "cursor": base_cursor,
            "filter": {
                "textSearch": "",
                "withPhoto": -1,  # -1 — не фильтровать по наличию фото
            },
        }
    }
    if nm_ids:
        body["nmID"] = nm_ids
    # Try with Bearer first, then raw token (Content API часто принимает без Bearer)
    headers1 = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    try:
        resp = post_with_retry(WB_CARDS_LIST_URL, headers1, body)
        return resp.json()
    except requests.HTTPError as err:
        if err.response is not None and err.response.status_code in (401, 403):
            headers2 = {"Authorization": f"{token}", "Content-Type": "application/json"}
            resp2 = post_with_retry(WB_CARDS_LIST_URL, headers2, body)
            return resp2.json()
        raise


def fetch_all_cards(token: str, page_limit: int = 1000) -> List[Dict[str, Any]]:
    all_cards: List[Dict[str, Any]] = []
    seen_keys: set[tuple] = set()
    cursor: Dict[str, Any] = {"limit": page_limit, "nmID": 0}
    safety = 0
    while True:
        safety += 1
        if safety > 5000:
            break
        data = fetch_cards_list(token, cursor=cursor, limit=page_limit)
        payload = data.get("data") or data
        cards = payload.get("cards") or []
        if not cards:
            break
        all_cards.extend(cards)
        cur = payload.get("cursor") or {}
        key = (cur.get("updatedAt"), cur.get("nmID"), cur.get("nmIDNext"))
        if key in seen_keys:
            break
        seen_keys.add(key)
        # Prepare next cursor
        next_nm = cur.get("nmIDNext") or cur.get("nmID")
        next_cursor: Dict[str, Any] = {"limit": page_limit}
        if cur.get("updatedAt"):
            next_cursor["updatedAt"] = cur.get("updatedAt")
        if next_nm is not None:
            next_cursor["nmID"] = next_nm
        cursor = next_cursor
        # If страница меньше лимита, вероятно, достигнут конец
        if len(cards) < page_limit:
            break
    return all_cards


def normalize_cards_response(data: Dict[str, Any]) -> List[Dict[str, Any]]:
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
            try:
                sizes = c.get("sizes") or []
                for s in sizes:
                    tech = s.get("skus") or s.get("barcodes") or []
                    if tech:
                        barcode = str(tech[0])
                        break
            except Exception:
                barcode = None
            # Получаем название товара
            name = c.get("name") or c.get("title") or c.get("subject") or "Без названия"
            
            items.append({
                "photo": photo,
                "supplier_article": supplier_article,
                "nm_id": nm_id,
                "barcode": barcode,
                "name": name,
            })
    except Exception:
        pass
    return items


@app.route("/products", methods=["GET"]) 
@login_required
def products_page():
    token = current_user.wb_token or ""
    error = None
    products: List[Dict[str, Any]] = []
    if not token:
        error = "Укажите токен API в профиле"
    else:
        try:
            cached = load_products_cache()
            if cached and cached.get("_user_id") == current_user.id:
                products = cached.get("items", [])
            else:
                # Load all pages
                raw_cards = fetch_all_cards(token, page_limit=100)
                products = normalize_cards_response({"cards": raw_cards})
                save_products_cache({"items": products})
        except requests.HTTPError as http_err:
            error = f"Ошибка API: {http_err.response.status_code}"
        except Exception as exc:
            error = f"Ошибка: {exc}"
    return render_template("products.html", error=error, items=products, items_count=len(products))


# -------------------------
# Stocks page
# -------------------------

def fetch_stocks_all(token: str) -> List[Dict[str, Any]]:
    """/supplier/stocks отдаёт текущие остатки одним снимком без пагинации. Берём полные данные за один запрос."""
    headers1 = {"Authorization": f"Bearer {token}"}
    # WB иногда отдаёт 502/504 — добавим несколько повторов и альтернативный заголовок
    try:
        # один запрос без агрессивных ретраев, чтобы не словить 429 по всплеску
        resp = get_with_retry(STOCKS_API_URL, headers1, params={}, max_retries=1, timeout_s=60)
        return resp.json()
    except requests.HTTPError as err:
        # если авторизация — попробуем без Bearer
        if err.response is not None and err.response.status_code in (401, 403):
            headers2 = {"Authorization": f"{token}"}
            resp2 = get_with_retry(STOCKS_API_URL, headers2, params={}, max_retries=1, timeout_s=60)
            return resp2.json()
        # 429 отдадим наверх без повторов — пусть фронт покажет таймер
        raise


def fetch_stocks_paginated(token: str, start_iso: str = "1970-01-01T00:00:00") -> List[Dict[str, Any]]:
    headers = {"Authorization": f"Bearer {token}"}
    cursor = start_iso
    collected: List[Dict[str, Any]] = []
    safety = 0
    while True:
        safety += 1
        if safety > 5000:
            break
        params = {"dateFrom": cursor, "flag": 0}
        try:
            resp = get_with_retry(STOCKS_API_URL, headers, params, max_retries=6, timeout_s=90)
        except requests.HTTPError as err:
            if err.response is not None and err.response.status_code in (401, 403):
                alt_headers = {"Authorization": f"{token}"}
                resp = get_with_retry(STOCKS_API_URL, alt_headers, params, max_retries=6, timeout_s=90)
            else:
                raise
        page = resp.json()
        if not isinstance(page, list) or not page:
            break
        try:
            page.sort(key=lambda x: parse_wb_datetime(str(x.get("lastChangeDate"))) or datetime.min)
        except Exception:
            pass
        collected.extend(page)
        last_lcd = None
        try:
            last_lcd = page[-1].get("lastChangeDate")
        except Exception:
            last_lcd = None
        if not last_lcd:
            break
        cursor = str(last_lcd)
        time.sleep(0.25)
    return collected


def fetch_stocks_resilient(token: str) -> List[Dict[str, Any]]:
    try:
        data = fetch_stocks_all(token)
        if isinstance(data, list) and data:
            return data
    except requests.HTTPError as e:
        # если 429 — не уходим в пагинацию, возвращаем 429
        try:
            if e.response is not None and e.response.status_code == 429:
                raise
        except Exception:
            pass
        # иначе попробуем постранично (редкие случаи нестабильности снапшота)
        return fetch_stocks_paginated(token)
    # Fallback to paginated flow
    return fetch_stocks_paginated(token)


def normalize_stocks(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    for r in rows or []:
        qty_val = r.get("quantity") or r.get("qty") or r.get("inWayToClient") or 0
        try:
            qty_int = int(qty_val)
        except Exception:
            try:
                qty_int = int(float(qty_val))
            except Exception:
                qty_int = 0
        items.append({
            "vendor_code": r.get("supplierArticle") or r.get("vendorCode") or r.get("article"),
            "barcode": r.get("barcode") or r.get("skus") or r.get("sku"),
            "nm_id": r.get("nmId") or r.get("nmID") or r.get("nm") or None,
            "qty": qty_int,
            "warehouse": r.get("warehouseName") or r.get("warehouse") or r.get("warehouse_name"),
        })
    return items


def update_stocks_if_needed(user_id: int, token: str, force_update: bool = False) -> bool:
    """
    Обновляет остатки если нужно (если кэш устарел или принудительно)
    Возвращает True если остатки были обновлены, False если использовался кэш
    """
    try:
        cached = load_stocks_cache()
        should_refresh = force_update
        
        if not should_refresh and cached and cached.get("_user_id") == user_id:
            # Проверяем, когда последний раз обновлялись остатки
            updated_at = cached.get("updated_at")
            if updated_at:
                try:
                    from datetime import datetime
                    # Парсим время обновления из кэша
                    cache_time = datetime.strptime(updated_at, "%d.%m.%Y %H:%M:%S")
                    # Если остатки обновлялись менее 10 минут назад, используем кэш
                    if (datetime.now() - cache_time).total_seconds() < 600:  # 10 минут
                        should_refresh = False
                        print(f"=== ОТЧЕТ ПО ЗАКАЗАМ: Используем кэшированные остатки ===")
                        print(f"Кэш обновлен: {updated_at}")
                    else:
                        should_refresh = True
                        print(f"=== ОТЧЕТ ПО ЗАКАЗАМ: Кэш устарел, обновляем остатки ===")
                except Exception as e:
                    print(f"Ошибка парсинга времени кэша: {e}")
                    should_refresh = True
        else:
            should_refresh = True
            print(f"=== ОТЧЕТ ПО ЗАКАЗАМ: Нет кэша или принудительное обновление ===")
        
        if should_refresh:
            print(f"Обновляем остатки для пользователя {user_id}")
            try:
                raw_stocks = fetch_stocks_resilient(token)
                stocks = normalize_stocks(raw_stocks)
                from datetime import datetime
                now_str = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
                save_stocks_cache({"items": stocks, "_user_id": user_id, "updated_at": now_str})
                print(f"Остатки обновлены для отчета по заказам: {len(stocks)} товаров в {now_str}")
                return True
            except requests.HTTPError as e:
                if e.response and e.response.status_code == 429:
                    print("=== ОТЧЕТ ПО ЗАКАЗАМ: Ошибка 429, используем кэш ===")
                    if cached and cached.get("_user_id") == user_id:
                        print(f"Используем кэшированные остатки: {len(cached.get('items', []))} товаров")
                        return False
                    else:
                        print("Нет кэша и ошибка 429 - не можем получить остатки")
                        return False
                else:
                    print(f"Ошибка при обновлении остатков: {e}")
                    return False
        else:
            return False
            
    except Exception as e:
        print(f"Ошибка в update_stocks_if_needed: {e}")
        return False


@app.route("/api/stocks/update-time", methods=["GET"])
@login_required
def api_stocks_update_time():
    """API для получения времени последнего обновления остатков"""
    try:
        cached = load_stocks_cache()
        if cached and cached.get("_user_id") == current_user.id:
            return jsonify({
                "updated_at": cached.get("updated_at", "Неизвестно")
            })
        else:
            return jsonify({
                "updated_at": "Остатки не загружены"
            })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/stocks", methods=["GET"]) 
@login_required
def stocks_page():
    token = current_user.wb_token or ""
    error = None
    items: List[Dict[str, Any]] = []
    if not token:
        error = "Укажите токен API в профиле"
    else:
        try:
            cached = load_stocks_cache()
            if cached and cached.get("_user_id") == current_user.id:
                items = cached.get("items", [])
            else:
                raw = fetch_stocks_resilient(token)
                items = normalize_stocks(raw)
                save_stocks_cache({"items": items, "updated_at": datetime.now().strftime("%d.%m.%Y %H:%M:%S")})
        except requests.HTTPError as http_err:
            error = f"Ошибка API: {http_err.response.status_code}"
        except Exception as exc:
            error = f"Ошибка: {exc}"
    # Aggregations
    total_qty_all = sum(int(it.get("qty", 0) or 0) for it in items)
    # by product
    prod_map: Dict[tuple, Dict[str, Any]] = {}
    for it in items:
        key = (it.get("vendor_code") or "", it.get("barcode") or "")
        rec = prod_map.get(key)
        if not rec:
            rec = {"vendor_code": key[0], "barcode": key[1], "nm_id": it.get("nm_id"), "total_qty": 0, "warehouses": []}
            prod_map[key] = rec
        rec["total_qty"] += int(it.get("qty", 0) or 0)
        rec["warehouses"].append({"warehouse": it.get("warehouse"), "qty": int(it.get("qty", 0) or 0)})
    # Collapse warehouses per product, filter zeroes, sort by qty desc
    for rec in prod_map.values():
        from collections import defaultdict as _dd
        acc = _dd(int)
        for w in rec["warehouses"]:
            name = w.get("warehouse") or ""
            acc[name] += int(w.get("qty", 0) or 0)
        wh_list = [{"warehouse": name, "qty": qty} for name, qty in acc.items() if qty > 0]
        wh_list.sort(key=lambda x: (-x["qty"], x["warehouse"]))
        rec["warehouses"] = wh_list
    products_agg = sorted(prod_map.values(), key=lambda x: (-x["total_qty"], x["vendor_code"] or ""))
    # by warehouse
    wh_map: Dict[str, Dict[str, Any]] = {}
    for it in items:
        w = it.get("warehouse") or ""
        rec = wh_map.get(w)
        if not rec:
            rec = {"warehouse": w, "total_qty": 0, "products": []}
            wh_map[w] = rec
        qty_i = int(it.get("qty", 0) or 0)
        rec["total_qty"] += qty_i
        rec["products"].append({
            "vendor_code": it.get("vendor_code"),
            "barcode": it.get("barcode"),
            "nm_id": it.get("nm_id"),
            "qty": qty_i,
        })
    for rec in wh_map.values():
        rec["products"].sort(key=lambda x: (-x["qty"], x["vendor_code"] or ""))
    warehouses_agg = sorted(wh_map.values(), key=lambda x: (-x["total_qty"], x["warehouse"] or ""))
    updated_at = None
    try:
        cached = load_stocks_cache()
        if cached and cached.get("_user_id") == current_user.id:
            updated_at = cached.get("updated_at")
    except Exception:
        updated_at = None
    return render_template(
        "stocks.html",
        error=error,
        items=items,
        items_count=len(items),
        total_qty_all=total_qty_all,
        updated_at=updated_at,
        products_agg=products_agg,
        warehouses_agg=warehouses_agg,
    )


@app.route("/api/stocks/refresh", methods=["POST"]) 
@login_required
def api_stocks_refresh():
    token = current_user.wb_token or ""
    if not token:
        return jsonify({"error": "no_token"}), 401
    try:
        raw = fetch_stocks_resilient(token)
        items = normalize_stocks(raw)
        now_str = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
        save_stocks_cache({"items": items, "updated_at": now_str})
        return jsonify({"ok": True, "count": len(items), "updated_at": now_str})
    except requests.HTTPError as http_err:
        return jsonify({"error": "http", "status": http_err.response.status_code}), 502
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/api/stocks/data", methods=["GET"]) 
@login_required
def api_stocks_data():
    cached = load_stocks_cache()
    if not cached or not (current_user.is_authenticated and cached.get("_user_id") == current_user.id):
        return jsonify({"products": [], "warehouses": [], "total_qty_all": 0, "updated_at": None})
    items = cached.get("items", [])
    # Total
    try:
        total_qty_all = sum(int((it.get("qty") or 0)) for it in items)
    except Exception:
        total_qty_all = 0
    # by product (same shape as on page)
    prod_map: Dict[tuple, Dict[str, Any]] = {}
    for it in items:
        key = (it.get("vendor_code") or "", it.get("barcode") or "")
        rec = prod_map.get(key)
        if not rec:
            rec = {"vendor_code": key[0], "barcode": key[1], "nm_id": it.get("nm_id"), "total_qty": 0, "warehouses": []}
            prod_map[key] = rec
        qty_i = int(it.get("qty", 0) or 0)
        rec["total_qty"] += qty_i
        rec["warehouses"].append({"warehouse": it.get("warehouse"), "qty": qty_i})
    from collections import defaultdict as _dd
    products_agg = []
    for rec in prod_map.values():
        acc = _dd(int)
        for w in rec["warehouses"]:
            acc[w.get("warehouse") or ""] += int(w.get("qty", 0) or 0)
        rec["warehouses"] = [{"warehouse": n, "qty": q} for n, q in acc.items() if q > 0]
        rec["warehouses"].sort(key=lambda x: (-x["qty"], x["warehouse"]))
        products_agg.append(rec)
    products_agg.sort(key=lambda x: (-x["total_qty"], x["vendor_code"] or ""))

    # by warehouse
    wh_map: Dict[str, Dict[str, Any]] = {}
    for it in items:
        w = it.get("warehouse") or ""
        rec = wh_map.get(w)
        if not rec:
            rec = {"warehouse": w, "total_qty": 0, "products": []}
            wh_map[w] = rec
        qty_i = int(it.get("qty", 0) or 0)
        rec["total_qty"] += qty_i
        rec["products"].append({
            "vendor_code": it.get("vendor_code"),
            "nm_id": it.get("nm_id"),
            "barcode": it.get("barcode"),
            "qty": qty_i,
        })
    for rec in wh_map.values():
        rec["products"].sort(key=lambda x: (-x["qty"], x["vendor_code"] or ""))
    warehouses_agg = sorted(wh_map.values(), key=lambda x: (-x["total_qty"], x["warehouse"] or ""))

    return jsonify({
        "products": products_agg,
        "warehouses": warehouses_agg,
        "total_qty_all": total_qty_all,
        "updated_at": cached.get("updated_at"),
    })

@app.route("/stocks/export", methods=["POST"]) 
@login_required
def stocks_export():
    token = current_user.wb_token or ""
    if not token:
        return redirect(url_for("stocks_page"))
    try:
        cached = load_stocks_cache()
        if cached and cached.get("_user_id") == current_user.id:
            items = cached.get("items", [])
        else:
            raw = fetch_stocks_all(token)
            items = normalize_stocks(raw)
            save_stocks_cache({"items": items})

        wb = Workbook()
        ws = wb.active
        ws.title = "stocks"
        headers = ["Артикул продавца", "Баркод", "Остаток", "Склад"]
        ws.append(headers)
        for it in items:
            ws.append([
                it.get("vendor_code", ""),
                it.get("barcode", ""),
                it.get("qty", 0),
                it.get("warehouse", ""),
            ])
        output = io.BytesIO()
        wb.save(output)
        output.seek(0)
        from datetime import datetime as _dt
        filename = f"wb_stocks_{_dt.now().strftime('%Y%m%d_%H%M')}.xlsx"
        return send_file(output, as_attachment=True, download_name=filename, mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    except Exception:
        return redirect(url_for("stocks_page"))


@app.route("/api/products/refresh", methods=["POST"]) 
@login_required
def api_products_refresh():
    token = current_user.wb_token or ""
    if not token:
        return jsonify({"error": "no_token"}), 401
    try:
        raw_cards = fetch_all_cards(token, page_limit=100)
        items = normalize_cards_response({"cards": raw_cards})
        save_products_cache({"items": items})
        return jsonify({"ok": True, "count": len(items)})
    except requests.HTTPError as http_err:
        return jsonify({"error": "http", "status": http_err.response.status_code}), 502
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500

@app.route('/favicon.ico')
def favicon():
    return send_from_directory(app.root_path, 'favicon.ico', mimetype='image/vnd.microsoft.icon')

@app.route('/logo.png')
def logo():
    return send_from_directory(os.path.join(app.root_path, 'templates'), 'logo.png', mimetype='image/png')


# -------------------------
# Auth routes
# -------------------------

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        user = User.query.filter_by(username=username).first()
        # Authentication and account state checks
        if not user or user.password != password:
            return render_template("login.html", error="Неверный логин или пароль")
        if not user.is_active:
            return render_template("login.html", error="Ваша учетная запись заблокирована, обратитесь в техподдержку")
        # Check validity dates
        today = datetime.now(MOSCOW_TZ).date()
        if user.valid_from and today < user.valid_from:
            return render_template("login.html", error="Учётная запись ещё не активна")
        if user.valid_to and today > user.valid_to:
            return render_template("login.html", error="Срок действия вашей подписки истек")
        login_user(user)
        return redirect(request.args.get("next") or url_for("index"))
    return render_template("login.html")


@app.route("/logout", methods=["GET"]) 
def logout():
    logout_user()
    return redirect(url_for("login"))


def admin_required():
    if not current_user.is_authenticated or not current_user.is_admin:
        return False
    return True


@app.route("/admin/users", methods=["GET"]) 
@login_required
def admin_users():
    # Проверяем права администратора
    if not current_user.is_authenticated or not current_user.is_admin:
        flash("У вас нет прав для доступа к этой странице", "error")
        return redirect(url_for("index"))
    
    users = User.query.order_by(User.id.asc()).all()
    return render_template("admin_users.html", users=users, message=None)


@app.route("/admin/users/create", methods=["POST"]) 
@login_required
def admin_users_create():
    # Проверяем права администратора
    if not current_user.is_authenticated or not current_user.is_admin:
        return jsonify({"success": False, "error": "У вас нет прав для выполнения этого действия"}), 403
    username = request.form.get("username", "").strip()
    password = request.form.get("password", "")
    is_admin = bool(request.form.get("is_admin"))
    vf = request.form.get("valid_from") or None
    vt = request.form.get("valid_to") or None
    if not username or not password:
        flash("Укажите логин и пароль")
        return redirect(url_for("admin_users"))
    if User.query.filter_by(username=username).first():
        flash("Такой логин уже существует")
        return redirect(url_for("admin_users"))
    try:
        from datetime import date
        vf_d = None
        vt_d = None
        if vf:
            try:
                vf_d = datetime.strptime(vf, "%Y-%m-%d").date()
            except Exception:
                vf_d = None
        if vt:
            try:
                vt_d = datetime.strptime(vt, "%Y-%m-%d").date()
            except Exception:
                vt_d = None
        u = User(username=username, password=password, is_admin=is_admin, is_active=True, valid_from=vf_d, valid_to=vt_d)
        db.session.add(u)
        db.session.commit()
        flash("Пользователь создан")
    except Exception as exc:
        try:
            db.session.rollback()
        except Exception:
            pass
        app.logger.exception("admin_users_create failed")
        flash(f"Ошибка создания пользователя: {exc}")
    return redirect(url_for("admin_users"))


@app.route("/admin/users/<int:user_id>/block", methods=["POST"]) 
@login_required
def admin_users_block(user_id: int):
    # Проверяем права администратора
    if not current_user.is_authenticated or not current_user.is_admin:
        return jsonify({"success": False, "error": "У вас нет прав для выполнения этого действия"}), 403
    u = db.session.get(User, user_id)
    if u:
        try:
            u.is_active = False
            db.session.commit()
            flash("Пользователь заблокирован")
        except Exception:
            db.session.rollback()
            flash("Ошибка")
    return redirect(url_for("admin_users"))


@app.route("/admin/users/<int:user_id>/unblock", methods=["POST"]) 
@login_required
def admin_users_unblock(user_id: int):
    # Проверяем права администратора
    if not current_user.is_authenticated or not current_user.is_admin:
        return jsonify({"success": False, "error": "У вас нет прав для выполнения этого действия"}), 403
    u = db.session.get(User, user_id)
    if u:
        try:
            u.is_active = True
            db.session.commit()
            flash("Пользователь разблокирован")
        except Exception:
            db.session.rollback()
            flash("Ошибка")
    return redirect(url_for("admin_users"))


@app.route("/admin/users/<int:user_id>/reset", methods=["POST"]) 
@login_required
def admin_users_reset(user_id: int):
    # Проверяем права администратора
    if not current_user.is_authenticated or not current_user.is_admin:
        return jsonify({"success": False, "error": "У вас нет прав для выполнения этого действия"}), 403
    new_pass = request.form.get("password", "")
    if not new_pass:
        flash("Укажите новый пароль")
        return redirect(url_for("admin_users"))
    u = db.session.get(User, user_id)
    if u:
        try:
            u.password = new_pass
            db.session.commit()
            flash("Пароль обновлён")
        except Exception:
            db.session.rollback()
            flash("Ошибка")
    return redirect(url_for("admin_users"))


@app.route("/admin/users/<int:user_id>/delete", methods=["POST"]) 
@login_required
def admin_users_delete(user_id: int):
    # Проверяем права администратора
    if not current_user.is_authenticated or not current_user.is_admin:
        return jsonify({"success": False, "error": "У вас нет прав для выполнения этого действия"}), 403
    u = db.session.get(User, user_id)
    if u:
        try:
            db.session.delete(u)
            db.session.commit()
            # Remove user's cache file if exists
            try:
                cache_path = _cache_path_for_user_id(user_id)
                if os.path.isfile(cache_path):
                    os.remove(cache_path)
            except Exception:
                pass
            flash("Пользователь удалён")
        except Exception:
            db.session.rollback()
            flash("Ошибка")
    return redirect(url_for("admin_users"))


@app.route("/admin/users/<int:user_id>/validity", methods=["POST"]) 
@login_required
def admin_users_validity(user_id: int):
    # Проверяем права администратора
    if not current_user.is_authenticated or not current_user.is_admin:
        return jsonify({"success": False, "error": "У вас нет прав для выполнения этого действия"}), 403
    vf = request.form.get("valid_from") or None
    vt = request.form.get("valid_to") or None
    u = db.session.get(User, user_id)
    if u:
        try:
            vf_d = None
            vt_d = None
            if vf:
                try:
                    vf_d = datetime.strptime(vf, "%Y-%m-%d").date()
                except Exception:
                    vf_d = None
            if vt:
                try:
                    vt_d = datetime.strptime(vt, "%Y-%m-%d").date()
                except Exception:
                    vt_d = None
            u.valid_from = vf_d
            u.valid_to = vt_d
            db.session.commit()
            flash("Срок действия обновлён")
        except Exception as exc:
            try:
                db.session.rollback()
            except Exception:
                pass
            app.logger.exception("admin_users_validity failed")
            flash(f"Ошибка обновления срока: {exc}")
    return redirect(url_for("admin_users"))


# -------------------------
# Template context: subscription banner
# -------------------------

@app.context_processor
def inject_subscription_banner():
    banner = {"show": False}
    try:
        if current_user.is_authenticated and current_user.valid_to:
            today = datetime.now(MOSCOW_TZ).date()
            days_left = (current_user.valid_to - today).days
            if 0 <= days_left <= 5:
                # Check cookie suppressing banner for a day
                hide_until = request.cookies.get("hide_sub_banner_until")
                hide_ok = False
                if hide_until:
                    try:
                        from datetime import date
                        hide_date = datetime.strptime(hide_until, "%Y-%m-%d").date()
                        if hide_date >= today:
                            hide_ok = True
                    except Exception:
                        hide_ok = False
                if not hide_ok:
                    banner = {
                        "show": True,
                        "days_left": days_left,
                        "end_date": current_user.valid_to.strftime("%d.%m.%Y"),
                    }
    except Exception:
        pass
    return {"subscription_banner": banner, "app_version": _read_version()}


@app.route("/changelog")
def changelog_page():
    md = _read_changelog_md()
    return render_template("changelog.html", app_version=_read_version(), md=md)


@app.route("/changelog/edit", methods=["GET", "POST"]) 
@login_required
def changelog_edit():
    if not current_user.is_admin:
        return redirect(url_for("changelog_page"))
    error = None
    message = None
    current_version = _read_version()
    md_content = _read_changelog_md()
    if request.method == "POST":
        try:
            new_version = (request.form.get("version") or "").strip()
            new_md = request.form.get("md_content")
            if new_md is not None:
                _write_changelog_md(new_md)
                md_content = new_md
            if new_version:
                _write_version(new_version)
                current_version = new_version
            message = "Сохранено"
        except Exception as exc:
            error = f"Ошибка: {exc}"
    return render_template(
        "changelog_edit.html",
        app_version=current_version,
        md_content=md_content,
        default_date=datetime.now(MOSCOW_TZ).strftime("%d.%m.%Y"),
        error=error,
        message=message,
    )


_DB_INIT_DONE = False

@app.before_request
def _init_db_once_per_process():
    global _DB_INIT_DONE
    if _DB_INIT_DONE:
        return
    try:
        db.create_all()
        _ensure_schema_users_validity_columns()
        if not User.query.filter_by(username="admin").first():
            db.session.add(User(username="admin", password="admin", is_admin=True, is_active=True))
            db.session.commit()
        
        # Start notification monitoring
        start_notification_monitoring()
        
    except Exception as e:
        try:
            db.session.rollback()
        except Exception:
            pass
    finally:
        _DB_INIT_DONE = True


# -------------------------
# Tools: Labels for boxes
# -------------------------

@app.route("/tools/labels", methods=["GET"]) 
@login_required
def tools_labels_page():
    token = (current_user.wb_token or "") if current_user.is_authenticated else ""
    warehouses = []
    if token:
        try:
            headers = {"Authorization": f"Bearer {token}"}
            resp = get_with_retry(SUPPLIES_WAREHOUSES_URL, headers, params={})
            warehouses = resp.json() or []
        except Exception:
            try:
                headers2 = {"Authorization": f"{token}"}
                resp = get_with_retry(SUPPLIES_WAREHOUSES_URL, headers2, params={})
                warehouses = resp.json() or []
            except Exception:
                warehouses = []
    return render_template(
        "tools_labels.html",
        warehouses=warehouses,
    )


@app.route("/tools/labels/download", methods=["POST"]) 
@login_required
def tools_labels_download():
    # Inputs
    warehouse_name = (request.form.get("warehouse") or "").strip()
    boxes = int(request.form.get("boxes") or 0)
    if boxes <= 0:
        return jsonify({"error": "bad_boxes"}), 400

    shipper_name = (request.form.get("shipper_name") or current_user.shipper_name or "").strip()
    contact_person = (request.form.get("contact_person") or getattr(current_user, 'contact_person', None) or "").strip()
    phone = (request.form.get("phone") or current_user.phone or "").strip()
    email = (request.form.get("email") or current_user.email or "").strip()
    address = (request.form.get("shipper_address") or current_user.shipper_address or "").strip()

    # Build DOCX
    doc = Document()
    section = doc.sections[0]
    section.left_margin = Cm(1.2)
    section.right_margin = Cm(1.2)
    section.top_margin = Cm(0.7)
    section.bottom_margin = Cm(0.7)

    # Flow labels as table rows; prevent row splitting so на страницу попадают только целые этикетки
    labels_table = doc.add_table(rows=0, cols=1)
    labels_table.autofit = True

    for n in range(1, boxes + 1):
        row = labels_table.add_row()
        # запрет разрыва строки таблицы между страницами
        tr = row._tr
        trPr = tr.get_or_add_trPr()
        cantSplit = OxmlElement('w:cantSplit')
        trPr.append(cantSplit)

        cell = row.cells[0]

        # Title line: increase font (approx 28pt)
        p1 = cell.add_paragraph()
        p1.paragraph_format.space_before = Pt(0)
        p1.paragraph_format.space_after = Pt(0)
        p1.paragraph_format.line_spacing = 1.0
        r1 = p1.add_run(f"Доставить на WB {warehouse_name}")
        r1.bold = True
        r1.font.size = Pt(28)
        p1.alignment = WD_ALIGN_PARAGRAPH.CENTER

        # Counter line: increase font (approx 32pt)
        p2 = cell.add_paragraph()
        p2.paragraph_format.space_before = Pt(0)
        p2.paragraph_format.space_after = Pt(2)
        p2.paragraph_format.line_spacing = 1.0
        r2 = p2.add_run(f"{n} из {boxes} КОРОБОК")
        r2.bold = True
        r2.font.size = Pt(32)
        p2.alignment = WD_ALIGN_PARAGRAPH.CENTER

        # Supplier line in one sentence
        supplier_line = ", ".join(filter(None, [shipper_name, f"Контактное лицо: {contact_person}" if contact_person else None, phone, email, address]))
        p3 = cell.add_paragraph(supplier_line)
        p3.paragraph_format.space_before = Pt(0)
        p3.paragraph_format.space_after = Pt(2)
        p3.paragraph_format.line_spacing = 1.0
        p3.alignment = WD_ALIGN_PARAGRAPH.LEFT
        for run in p3.runs:
            run.font.size = Pt(11)

        # Horizontal line separator in the cell
        p_sep = cell.add_paragraph()
        p_sep.paragraph_format.space_before = Pt(2)
        p_sep.paragraph_format.space_after = Pt(2)
        p_sep.paragraph_format.line_spacing = 1.0
        p_sep.alignment = WD_ALIGN_PARAGRAPH.CENTER
        p = p_sep._p
        pPr = p.get_or_add_pPr()
        pBdr = OxmlElement('w:pBdr')
        bottom = OxmlElement('w:bottom')
        bottom.set(qn('w:val'), 'single')
        bottom.set(qn('w:sz'), '8')
        bottom.set(qn('w:space'), '0')
        bottom.set(qn('w:color'), 'auto')
        pBdr.append(bottom)
        pPr.append(pBdr)

    out = io.BytesIO()
    doc.save(out)
    out.seek(0)
    fname = f"labels_{datetime.now().strftime('%Y%m%d_%H%M%S')}.docx"
    return send_file(out, as_attachment=True, download_name=fname, mimetype="application/vnd.openxmlformats-officedocument.wordprocessingml.document")


# Notification API endpoints
@app.route("/api/notifications/count", methods=["GET"])
@login_required
def api_notifications_count():
    """Get count of unread notifications"""
    count = get_unread_notifications_count(current_user.id)
    return jsonify({"count": count})


@app.route("/api/notifications", methods=["GET"])
@login_required
def api_notifications():
    """Get user notifications"""
    limit = int(request.args.get('limit', 20))
    notifications = get_user_notifications(current_user.id, limit)
    
    result = []
    for notif in notifications:
        data = None
        if notif.data:
            try:
                data = json.loads(notif.data)
            except:
                pass
                
        # Форматируем время создания уведомления в московском времени
        # Время уже создается в московском времени, но может быть naive
        if notif.created_at.tzinfo is None:
            # Если время naive, считаем его московским
            moscow_time = notif.created_at.replace(tzinfo=MOSCOW_TZ)
        else:
            # Если время уже с timezone, конвертируем в московское
            moscow_time = notif.created_at.astimezone(MOSCOW_TZ)
        
        formatted_time = moscow_time.strftime('%d.%m.%Y %H:%M')
        
        
        
        result.append({
            'id': notif.id,
            'title': notif.title,
            'message': notif.message,
            'type': notif.notification_type,
            'is_read': notif.is_read,
            'created_at': formatted_time,
            'data': data
        })
    
    return jsonify({"notifications": result})


@app.route("/api/notifications/<int:notification_id>/read", methods=["POST"])
@login_required
def api_mark_notification_read(notification_id: int):
    """Mark a notification as read"""
    success = mark_notification_as_read(notification_id, current_user.id)
    if success:
        return jsonify({"success": True})
    else:
        return jsonify({"error": "Notification not found"}), 404


@app.route("/api/notifications/read-all", methods=["POST"])
@login_required
def api_mark_all_notifications_read():
    """Mark all notifications as read"""
    count = mark_all_notifications_as_read(current_user.id)
    return jsonify({"success": True, "count": count})


@app.route("/api/notifications/<int:notification_id>/delete", methods=["DELETE"])
@login_required
def api_delete_notification(notification_id: int):
    """Delete a notification"""
    notification = Notification.query.filter_by(id=notification_id, user_id=current_user.id).first()
    if notification:
        db.session.delete(notification)
        db.session.commit()
        return jsonify({"success": True})
    else:
        return jsonify({"error": "Notification not found"}), 404



@app.route("/api/notifications/delete-all", methods=["DELETE"])
@login_required
def api_delete_all_notifications():
    """Delete all notifications for current user"""
    count = Notification.query.filter_by(user_id=current_user.id).delete()
    db.session.commit()
    return jsonify({"success": True, "count": count})


@app.route("/api/notifications/test", methods=["POST"])
@login_required
def test_notification():
    """Create a test notification for debugging"""
    try:
        create_notification(
            user_id=current_user.id,
            title="Тестовое уведомление",
            message="Это тестовое уведомление для проверки системы",
            notification_type="test",
            created_at=datetime.now(MOSCOW_TZ)
        )
        return jsonify({"success": True, "message": "Test notification created"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500




if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
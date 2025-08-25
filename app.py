# FBS warehouses/stocks
FBS_WAREHOUSES_URL = "https://marketplace-api.wildberries.ru/api/v3/warehouses"
FBS_STOCKS_BY_WAREHOUSE_URL = "https://marketplace-api.wildberries.ru/api/v3/stocks/{warehouseId}"
import io
import os
import json
import uuid
import time
import random
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Tuple

import requests
from flask import Flask, render_template, request, send_file, redirect, url_for, session, flash, jsonify, send_from_directory
from openpyxl import Workbook
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


def fetch_fbw_last_supplies(token: str, limit: int = 10) -> list[dict[str, Any]]:
    base_list = fetch_fbw_supplies_list(token)
    supplies: list[dict[str, Any]] = []
    for it in base_list[: max(0, int(limit))]:
        supply_id = it.get("supplyID") or it.get("supplyId") or it.get("id")
        details = fetch_fbw_supply_details(token, supply_id)
        # Normalize fields; prefer details when available, fallback to list fields
        create_date = (details or {}).get("createDate") or it.get("createDate")
        supply_date = (details or {}).get("supplyDate") or it.get("supplyDate")
        fact_date = (details or {}).get("factDate") or it.get("factDate")
        status_name = (details or {}).get("statusName") or it.get("statusName")
        warehouse_name = (details or {}).get("warehouseName") or it.get("warehouseName") or ""
        box_type = (details or {}).get("boxTypeName") or (details or {}).get("boxTypeID") or ""
        total_qty = (details or {}).get("quantity")
        acceptance_cost = (details or {}).get("acceptanceCost")
        paid_coef = (details or {}).get("paidAcceptanceCoefficient")
        supplies.append(
            {
                "supply_id": str(supply_id or ""),
                "type": str(box_type) if box_type is not None else "",
                "created_at": _fmt_dt_moscow(create_date, with_time=False),
                "total_goods": int(total_qty) if isinstance(total_qty, (int, float)) and total_qty is not None else None,
                "warehouse": warehouse_name or "",
                "acceptance_coefficient": paid_coef,
                "acceptance_cost": acceptance_cost,
                "planned_date": _fmt_dt_moscow(supply_date, with_time=False),
                "fact_date": _fmt_dt_moscow(fact_date, with_time=True),
                "status": status_name or "",
            }
        )
    return supplies


def fetch_fbw_supplies_range(token: str, offset: int, limit: int) -> list[dict[str, Any]]:
    base_list = fetch_fbw_supplies_list(token)
    if offset < 0:
        offset = 0
    end = offset + max(0, int(limit))
    slice_ids = base_list[offset:end]
    supplies: list[dict[str, Any]] = []
    for it in slice_ids:
        supply_id = it.get("supplyID") or it.get("supplyId") or it.get("id")
        details = fetch_fbw_supply_details(token, supply_id)
        create_date = (details or {}).get("createDate") or it.get("createDate")
        supply_date = (details or {}).get("supplyDate") or it.get("supplyDate")
        fact_date = (details or {}).get("factDate") or it.get("factDate")
        status_name = (details or {}).get("statusName") or it.get("statusName")
        warehouse_name = (details or {}).get("warehouseName") or it.get("warehouseName") or ""
        box_type = (details or {}).get("boxTypeName") or (details or {}).get("boxTypeID") or ""
        total_qty = (details or {}).get("quantity")
        acceptance_cost = (details or {}).get("acceptanceCost")
        paid_coef = (details or {}).get("paidAcceptanceCoefficient")
        supplies.append(
            {
                "supply_id": str(supply_id or ""),
                "type": str(box_type) if box_type is not None else "",
                "created_at": _fmt_dt_moscow(create_date, with_time=False),
                "total_goods": int(total_qty) if isinstance(total_qty, (int, float)) and total_qty is not None else None,
                "warehouse": warehouse_name or "",
                "acceptance_coefficient": paid_coef,
                "acceptance_cost": acceptance_cost,
                "planned_date": _fmt_dt_moscow(supply_date, with_time=False),
                "fact_date": _fmt_dt_moscow(fact_date, with_time=True),
                "status": status_name or "",
            }
        )
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


def fetch_orders_page(token: str, date_from_iso: str, flag: int = 0) -> List[Dict[str, Any]]:
    headers = {"Authorization": f"Bearer {token}"}
    params = {"dateFrom": date_from_iso, "flag": flag}
    response = get_with_retry(API_URL, headers, params)
    return response.json()


def fetch_orders_range(token: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
    """Fetch orders from WB by paginating with lastChangeDate until end_date inclusive."""
    start_dt = parse_date(start_date)
    end_dt = parse_date(end_date)

    cursor_dt = datetime.combine(start_dt.date(), datetime.min.time())

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
        page_exceeds = last_page_lcd and last_page_lcd.date() > end_dt.date()

        for item in page:
            srid = str(item.get("srid", ""))
            if srid and srid in seen_srid:
                continue
            lcd = parse_wb_datetime(item.get("lastChangeDate"))
            if lcd and lcd.date() > end_dt.date():
                continue
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


def to_rows(data: List[Dict[str, Any]], start_date: str, end_date: str) -> List[Dict[str, Any]]:
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

    items = [{
        "product": p,
        "qty": c,
        "nm_id": nm_by_product.get(p),
        "sum": round(revenue_by_product.get(p, 0.0), 2),
        "photo": nm_to_photo.get(nm_by_product.get(p))
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
    headers1 = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    body = {"stocks": items[:1000]}
    try:
        # WB expects 204 No Content
        resp = requests.put(url, headers=headers1, json=body, timeout=60)
        if resp.status_code != 204:
            raise requests.HTTPError(response=resp)
    except Exception:
        headers2 = {"Authorization": f"{token}", "Content-Type": "application/json"}
        resp = requests.put(url, headers=headers2, json=body, timeout=60)
        if resp.status_code != 204:
            raise requests.HTTPError(response=resp)


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
                # On first ever load: fetch only first 10 and cache them
                supplies = fetch_fbw_last_supplies(token, limit=10)
                generated_at = datetime.now(MOSCOW_TZ).strftime("%d.%m.%Y %H:%M")
                save_fbw_supplies_cache({"items": supplies, "updated_at": generated_at, "next_offset": 10})
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
        # Refresh from WB: fetch only first 10 or a subsequent page for load-more
        offset = int(request.args.get("offset", "0"))
        limit = int(request.args.get("limit", "10"))
        if offset <= 0:
            items = fetch_fbw_last_supplies(token, limit=limit)
            next_offset = limit
        else:
            # Always derive items from the same globally sorted list to avoid gaps
            items = fetch_fbw_supplies_range(token, offset=offset, limit=limit)
            next_offset = offset + limit
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
        flash("Токен успешно добавлен" if new_token else "Токен удален")
    except Exception:
        db.session.rollback()
        flash("Ошибка сохранения токена")
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
        return jsonify({"items": []})
    orders = cached.get("orders", [])
    items = aggregate_top_products_orders(orders, warehouse, limit=50)
    return jsonify({"items": items})


@app.route("/report/sales", methods=["GET"]) 
@login_required
def report_sales_page():
    cached = load_last_results()
    token = (current_user.wb_token or "") if current_user.is_authenticated else ""
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

    def _build_items(target_wh: str | None) -> List[Dict[str, Any]]:
        items_local: List[Dict[str, Any]] = []
        for prod, total in counts_total.items():
            qty = (by_wh[prod].get(target_wh, 0) if target_wh else total)
            if qty > 0:
                s = (by_wh_sum[prod].get(target_wh, 0.0) if target_wh else revenue_total.get(prod, 0.0))
                items_local.append({
                    "product": prod,
                    "qty": qty,
                    "nm_id": nm_by_product.get(prod),
                    "sum": round(float(s or 0.0), 2),
                    "photo": nm_to_photo.get(nm_by_product.get(prod))
                })
        items_local.sort(key=lambda x: x["qty"], reverse=True)
        return items_local
    items = _build_items(warehouse) if orders else []
    matrix = [{
        "product": p,
        "nm_id": nm_by_product.get(p),
        "total": counts_total[p],
        "by_wh": by_wh[p],
        "total_sum": round(float(revenue_total.get(p, 0.0)), 2),
        "by_wh_sum": by_wh_sum[p],
        "photo": nm_to_photo.get(nm_by_product.get(p))
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

        def build_items_for_wh(target_wh: str | None) -> List[Dict[str, Any]]:
            items_local: List[Dict[str, Any]] = []
            for prod, total in counts_total.items():
                qty = (by_wh[prod].get(target_wh, 0) if target_wh else total)
                if qty > 0:
                    s = (by_wh_sum[prod].get(target_wh, 0.0) if target_wh else revenue_total.get(prod, 0.0))
                    items_local.append({
                        "product": prod,
                        "qty": qty,
                        "nm_id": nm_by_product.get(prod),
                        "sum": round(float(s or 0.0), 2),
                        "photo": nm_to_photo.get(nm_by_product.get(prod))
                    })
            items_local.sort(key=lambda x: x["qty"], reverse=True)
            return items_local
        items = build_items_for_wh(warehouse)
        total_qty = sum(int(it.get("qty") or 0) for it in items)
        matrix = [{
            "product": p,
            "nm_id": nm_by_product.get(p),
            "total": counts_total[p],
            "by_wh": by_wh[p],
            "total_sum": round(float(revenue_total.get(p, 0.0)), 2),
            "by_wh_sum": by_wh_sum[p],
            "photo": nm_to_photo.get(nm_by_product.get(p))
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
        raw = fetch_fbs_new_orders(token)
        raw_sorted = sorted(raw, key=_extract_created_at)
        rows = to_fbs_rows(raw_sorted)
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
    try:
        limit_i = max(1, min(1000, int(limit_param)))
    except Exception:
        limit_i = 5

    if not refresh_flag:
        cached = load_fbs_supplies_cache() or {}
        if cached.get("items"):
            # Slice on server to reduce payload if requested
            items = cached.get("items", [])[:limit_i]
            return jsonify({
                "items": items,
                "lastUpdated": cached.get("lastUpdated"),
                "total": len(cached.get("items", []))
            }), 200

    try:
        pages_param = request.args.get("pages", default="5")
        try:
            pages_i = max(1, min(20, int(pages_param)))
        except Exception:
            pages_i = 5

        orders = _collect_fbs_orders_for_supplies(token, max_pages=pages_i, limit=200)
        supplies = _aggregate_fbs_supplies(orders)
        # Enrich supplies with supply info (createdAt, scanDt) and compute status
        headers_list = [
            {"Authorization": f"{token}"},
            {"Authorization": f"Bearer {token}"},
        ]
        enriched: List[Dict[str, Any]] = []
        for s in supplies:
            info = None
            last_err = None
            for hdrs in headers_list:
                try:
                    url = FBS_SUPPLY_INFO_URL.replace("{supplyId}", str(s["supplyId"]))
                    resp = get_with_retry(url, hdrs, params={})
                    info = resp.json()
                    break
                except Exception as e:
                    last_err = e
                    continue
            created_at = info.get("createdAt") if isinstance(info, dict) else None
            scan_dt = info.get("scanDt") if isinstance(info, dict) else None
            # Status per requirement
            if scan_dt:
                status_label = "Отгружено"
                try:
                    _sdt = parse_wb_datetime(str(scan_dt))
                    _sdt_msk = to_moscow(_sdt) if _sdt else None
                    status_dt = _sdt_msk.strftime("%d.%m.%Y %H:%M") if _sdt_msk else str(scan_dt)
                except Exception:
                    status_dt = str(scan_dt)
            else:
                status_label = "Не отгружена"
                status_dt = None
            # Date column should use createdAt
            date_dt = parse_wb_datetime(str(created_at)) if created_at else None
            date_msk = to_moscow(date_dt) if date_dt else None
            date_str = date_msk.strftime("%d.%m.%Y %H:%M") if date_msk else (s.get("date") or "")
            enriched.append({
                "supplyId": s["supplyId"],
                "date": date_str,
                "count": s["count"],
                "status": status_label,
                "statusDt": status_dt or "",
            })

        # Save cache with timestamp in Moscow TZ
        now_msk = datetime.now(MOSCOW_TZ)
        payload = {
            "items": enriched,
            "lastUpdated": now_msk.strftime("%d.%m.%Y %H:%M"),
            "ts": int(now_msk.timestamp())
        }
        save_fbs_supplies_cache(payload)

        # Return sliced view per limit
        return jsonify({
            "items": enriched[:limit_i],
            "lastUpdated": payload["lastUpdated"],
            "total": len(enriched)
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
            items.append({
                "photo": photo,
                "supplier_article": supplier_article,
                "nm_id": nm_id,
                "barcode": barcode,
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
    if not admin_required():
        return redirect(url_for("index"))
    users = User.query.order_by(User.id.asc()).all()
    return render_template("admin_users.html", users=users, message=None)


@app.route("/admin/users/create", methods=["POST"]) 
@login_required
def admin_users_create():
    if not admin_required():
        return redirect(url_for("index"))
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
    if not admin_required():
        return redirect(url_for("index"))
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
    if not admin_required():
        return redirect(url_for("index"))
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
    if not admin_required():
        return redirect(url_for("index"))
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
    if not admin_required():
        return redirect(url_for("index"))
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
    if not admin_required():
        return redirect(url_for("index"))
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
    except Exception:
        try:
            db.session.rollback()
        except Exception:
            pass
    _DB_INIT_DONE = True

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
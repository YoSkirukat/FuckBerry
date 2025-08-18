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

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET", "dev-secret-change-me")
app.config["SQLALCHEMY_DATABASE_URI"] = os.getenv("DATABASE_URL", f"sqlite:///{os.path.join(os.path.dirname(__file__), 'app.db')}")
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
db = SQLAlchemy(app)
login_manager = LoginManager(app)
login_manager.login_view = "login"

API_URL = "https://statistics-api.wildberries.ru/api/v1/supplier/orders"
SALES_API_URL = "https://statistics-api.wildberries.ru/api/v1/supplier/sales"
CACHE_DIR = os.path.join(os.path.dirname(__file__), "cache")
if not os.path.isdir(CACHE_DIR):
    os.makedirs(CACHE_DIR, exist_ok=True)

FBS_NEW_URL = "https://marketplace-api.wildberries.ru/api/v3/orders/new"

SELLER_INFO_URL = "https://common-api.wildberries.ru/api/v1/seller-info"

ACCEPT_COEFS_URL = "https://supplies-api.wildberries.ru/api/v1/acceptance/coefficients"
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
    today = datetime.utcnow().date()
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
    nm_by_product: Dict[str, Any] = {}
    for r in rows:
        product = r.get("Артикул продавца") or r.get("Артикул WB") or r.get("Баркод") or "Не указан"
        product = str(product)
        counts[product] += 1
        nm = r.get("Артикул WB") or r.get("nmId") or r.get("nmID")
        if product not in nm_by_product and nm:
            nm_by_product[product] = nm
    items = [{"product": p, "qty": c, "nm_id": nm_by_product.get(p)} for p, c in counts.items()]
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
    nm_by_product: Dict[str, Any] = {}
    for r in rows:
        if warehouse and (r.get("Склад отгрузки") or "Не указан") != warehouse:
            continue
        product = r.get("Артикул продавца") or r.get("Артикул WB") or r.get("Баркод") or "Не указан"
        product = str(product)
        counts[product] += 1
        nm = r.get("Артикул WB") or r.get("nmId") or r.get("nmID")
        if product not in nm_by_product and nm:
            nm_by_product[product] = nm
    items = [{"product": p, "qty": c, "nm_id": nm_by_product.get(p)} for p, c in counts.items()]
    items.sort(key=lambda x: x["qty"], reverse=True)
    return items[:limit]


def _extract_created_at(obj: Any) -> datetime:
    if not isinstance(obj, dict):
        return datetime.min
    val = obj.get("createdAt") or obj.get("dateCreated") or obj.get("date") or obj.get("created_at") or obj.get("time") or ""
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
        today = datetime.utcnow().date()
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
    old_password = request.form.get("old_password", "")
    new_password = request.form.get("new_password", "")
    if not old_password or not new_password:
        flash("Заполните оба поля")
        return redirect(url_for("profile"))
    if current_user.password != old_password:
        flash("Текущий пароль неверен")
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


@app.route("/fbs", methods=["GET", "POST"]) 
@login_required
def fbs_page():
    error = None
    token = (current_user.wb_token or "") if current_user.is_authenticated else ""
    rows: List[Dict[str, Any]] = []

    if request.method == "POST":
        if not token:
            error = "Укажите токен API на странице Настройки"
        else:
            try:
                raw = fetch_fbs_new_orders(token)
                # sort by createdAt asc (oldest first)
                raw_sorted = sorted(raw, key=_extract_created_at)
                rows = to_fbs_rows(raw_sorted)
                # Try enrich from products cache
                prod_cached = load_products_cache()
                items = (prod_cached or {}).get("items") or []
                # Build index by supplier_article and by nm_id
                by_article = {}
                by_nm = {}
                for it in items:
                    art = (it.get("supplier_article") or it.get("vendorCode") or "").strip()
                    if art:
                        by_article.setdefault(art, it)
                    nm = it.get("nm_id") or it.get("nmID")
                    if nm:
                        by_nm[int(nm)] = it
                # Enrich
                for r in rows:
                    art = (r.get("Наименование товара") or "").strip()
                    hit = by_article.get(art)
                    if not hit and r.get("nm_id"):
                        hit = by_nm.get(int(r["nm_id"]))
                    if hit:
                        # barcode normalization
                        if hit.get("barcode"):
                            r["barcode"] = hit.get("barcode")
                        elif isinstance(hit.get("barcodes"), list) and hit.get("barcodes"):
                            r["barcode"] = str(hit.get("barcodes")[0])
                        else:
                            # Try sizes -> skus
                            sizes = hit.get("sizes") or []
                            if isinstance(sizes, list):
                                for s in sizes:
                                    bar_list = s.get("skus") or s.get("barcodes")
                                    if isinstance(bar_list, list) and bar_list:
                                        r["barcode"] = str(bar_list[0])
                                        break
                        r["photo"] = hit.get("photo")
            except requests.HTTPError as http_err:
                error = f"Ошибка API: {http_err.response.status_code}"
            except Exception as exc:  # noqa: BLE001
                error = f"Ошибка: {exc}"

    # If enrichment impossible due to empty products cache
    products_hint = None
    prod_cached_now = load_products_cache()
    if not prod_cached_now or not ((prod_cached_now or {}).get("items")):
        products_hint = "Для отображения фото товара и баркода обновите данные на странице Товары"

    return render_template("fbs.html", error=error, rows=rows, products_hint=products_hint)


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
        today = datetime.utcnow().date()
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
    except Exception:
        db.session.rollback()
        flash("Ошибка создания пользователя")
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
        except Exception:
            db.session.rollback()
            flash("Ошибка обновления срока")
    return redirect(url_for("admin_users"))


# -------------------------
# Template context: subscription banner
# -------------------------

@app.context_processor
def inject_subscription_banner():
    banner = {"show": False}
    try:
        if current_user.is_authenticated and current_user.valid_to:
            today = datetime.utcnow().date()
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
    return {"subscription_banner": banner}


if __name__ == "__main__":
    # Initialize DB on first run and perform simple SQLite migrations
    def _ensure_sqlite_schema():
        try:
            with db.engine.connect() as conn:
                rows = conn.execute(text("PRAGMA table_info(users)")).fetchall()
                cols = {r[1] for r in rows}
                if "valid_from" not in cols:
                    conn.execute(text("ALTER TABLE users ADD COLUMN valid_from DATE"))
                if "valid_to" not in cols:
                    conn.execute(text("ALTER TABLE users ADD COLUMN valid_to DATE"))
        except Exception:
            # Ignore if users table doesn't exist yet; create_all will make it
            pass

    with app.app_context():
        db.create_all()
        _ensure_sqlite_schema()
        # Ensure there is at least one admin user (default creds: admin/admin) — change in prod!
        if not User.query.filter_by(username="admin").first():
            db.session.add(User(username="admin", password="admin", is_admin=True, is_active=True))
            db.session.commit()
    app.run(host="0.0.0.0", port=5000, debug=True) 
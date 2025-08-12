import io
import os
import json
import uuid
from collections import defaultdict
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple

import requests
from flask import Flask, render_template, request, send_file, redirect, url_for, session, flash
from openpyxl import Workbook

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET", "dev-secret-change-me")

API_URL = "https://statistics-api.wildberries.ru/api/v1/supplier/orders"
CACHE_DIR = os.path.join(os.path.dirname(__file__), "cache")
if not os.path.isdir(CACHE_DIR):
    os.makedirs(CACHE_DIR, exist_ok=True)


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


def _get_session_id() -> str:
    sid = session.get("SID")
    if not sid:
        sid = uuid.uuid4().hex
        session["SID"] = sid
    return sid


def _cache_path(sid: str) -> str:
    return os.path.join(CACHE_DIR, f"orders_{sid}.json")


def load_last_results() -> Dict[str, Any] | None:
    sid = _get_session_id()
    path = _cache_path(sid)
    if not os.path.isfile(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def save_last_results(payload: Dict[str, Any]) -> None:
    sid = _get_session_id()
    path = _cache_path(sid)
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)
    except Exception:
        pass


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
        return datetime.fromisoformat(s_norm[:26] + s_norm[26:])  # be forgiving on microseconds length
    except Exception:
        try:
            return datetime.strptime(s[:19], "%Y-%m-%dT%H:%M:%S")
        except Exception:
            return None


def fetch_orders_page(token: str, date_from_iso: str, flag: int = 0) -> List[Dict[str, Any]]:
    headers = {"Authorization": f"Bearer {token}"}
    params = {"dateFrom": date_from_iso, "flag": flag}
    response = requests.get(API_URL, headers=headers, params=params, timeout=60)
    response.raise_for_status()
    return response.json()


def fetch_orders_range(token: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
    """Fetch orders from WB by paginating with lastChangeDate until end_date inclusive."""
    start_dt = parse_date(start_date)
    end_dt = parse_date(end_date)

    # Start from 00:00:00 at start_dt
    cursor_dt = datetime.combine(start_dt.date(), datetime.min.time())

    collected: List[Dict[str, Any]] = []
    seen_srid: set[str] = set()

    max_pages = 2000  # safety
    pages = 0

    while pages < max_pages:
        pages += 1
        # WB accepts both date and datetime, but use ISO datetime for safety
        page = fetch_orders_page(token, cursor_dt.strftime("%Y-%m-%dT%H:%M:%S"), flag=0)
        if not page:
            break

        # Sort defensively by lastChangeDate ascending if not guaranteed
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
                # stop adding beyond requested end date
                continue
            if srid:
                seen_srid.add(srid)
            collected.append(item)

        # Advance cursor to the last lastChangeDate from this page
        if last_page_lcd is None:
            break
        cursor_dt = last_page_lcd

        if page_exceeds:
            # We reached beyond end date in this (or last) page
            break

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


def aggregate_by_warehouse(rows: List[Dict[str, Any]]) -> List[Tuple[str, int]]:
    counts: Dict[str, int] = defaultdict(int)
    for r in rows:
        warehouse = r.get("Склад отгрузки") or "Не указан"
        counts[warehouse] += 1
    return sorted(counts.items(), key=lambda kv: kv[1], reverse=True)


def aggregate_top_products(rows: List[Dict[str, Any]], limit: int = 15) -> List[Tuple[str, int]]:
    counts: Dict[str, int] = defaultdict(int)
    for r in rows:
        product = r.get("Артикул продавца") or r.get("Артикул WB") or r.get("Баркод") or "Не указан"
        counts[str(product)] += 1
    return sorted(counts.items(), key=lambda kv: kv[1], reverse=True)[:limit]


@app.route("/", methods=["GET", "POST"]) 
def root():
    if request.method == "POST":
        return redirect(url_for("index"), code=307)
    return redirect(url_for("index"))


@app.route("/orders", methods=["GET", "POST"]) 
def index():
    error = None
    orders = []
    total_orders = 0
    total_revenue = 0.0
    daily_labels: List[str] = []
    daily_counts: List[int] = []
    daily_revenue: List[float] = []
    warehouse_summary: List[Tuple[str, int]] = []
    top_products: List[Tuple[str, int]] = []

    # Токен: берём из формы, иначе из сессии
    token = (request.form.get("token", "").strip() or session.get("WB_API_TOKEN", ""))
    date_from = request.form.get("date_from", "")
    date_to = request.form.get("date_to", "")

    # Если GET — пробуем показать последние результаты из кэша
    if request.method == "GET":
        cached = load_last_results()
        if cached:
            date_from = cached.get("date_from", date_from)
            date_to = cached.get("date_to", date_to)
            orders = cached.get("orders", [])
            total_orders = cached.get("total_orders", 0)
            total_revenue = cached.get("total_revenue", 0.0)
            daily_labels = cached.get("daily_labels", [])
            daily_counts = cached.get("daily_counts", [])
            daily_revenue = cached.get("daily_revenue", [])
            warehouse_summary = cached.get("warehouse_summary", [])
            top_products = cached.get("top_products", [])

    if request.method == "POST":
        if not token:
            error = "Укажите токен API"
        elif not date_from or not date_to:
            error = "Выберите даты"
        else:
            try:
                parse_date(date_from)
                parse_date(date_to)
            except ValueError:
                error = "Неверный формат дат"

        if not error:
            try:
                raw_data = fetch_orders_range(token, date_from, date_to)
                orders = to_rows(raw_data, date_from, date_to)
                total_orders = len(orders)
                total_revenue = round(sum(float(o.get("Цена с учетом всех скидок") or 0) for o in orders), 2)
                daily_labels, daily_counts, daily_revenue = aggregate_daily(orders)
                warehouse_summary = aggregate_by_warehouse(orders)
                top_products = aggregate_top_products(orders, limit=15)
                # Сохраняем токен в сессию для последующих запросов
                session["WB_API_TOKEN"] = token
                # Сохраняем последние результаты для отображения на GET
                save_last_results({
                    "date_from": date_from,
                    "date_to": date_to,
                    "orders": orders,
                    "total_orders": total_orders,
                    "total_revenue": total_revenue,
                    "daily_labels": daily_labels,
                    "daily_counts": daily_counts,
                    "daily_revenue": daily_revenue,
                    "warehouse_summary": warehouse_summary,
                    "top_products": top_products,
                })
            except requests.HTTPError as http_err:
                error = f"Ошибка API: {http_err.response.status_code}"
            except Exception as exc:  # noqa: BLE001
                error = f"Ошибка: {exc}"

    return render_template(
        "index.html",
        error=error,
        token=token,
        date_from=date_from,
        date_to=date_to,
        orders=orders,
        total_orders=total_orders,
        total_revenue=total_revenue,
        daily_labels=daily_labels,
        daily_counts=daily_counts,
        daily_revenue=daily_revenue,
        warehouse_summary=warehouse_summary,
        top_products=top_products,
    )


@app.route("/settings", methods=["GET", "POST"]) 
def settings():
    message = None
    current_token = session.get("WB_API_TOKEN", "")

    if request.method == "POST":
        new_token = request.form.get("token", "").strip()
        if new_token:
            session["WB_API_TOKEN"] = new_token
            message = "Токен сохранен"
            return render_template("settings.html", message=message, token=new_token)
        else:
            # Очистка токена, если отправили пустым
            session.pop("WB_API_TOKEN", None)
            message = "Токен очищен"
            return render_template("settings.html", message=message, token="")

    return render_template("settings.html", message=message, token=current_token)


@app.route("/export", methods=["POST"]) 
def export_excel():
    token = (request.form.get("token", "").strip() or session.get("WB_API_TOKEN", ""))
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


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True) 
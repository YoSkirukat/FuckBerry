# -*- coding: utf-8 -*-
"""Сборка ленты заказов из кэша статистики WB + обогащение."""
from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from models import PurchasePrice
from utils.cache import CACHE_DIR, load_orders_period_cache
from utils.constants import MOSCOW_TZ

# Статусы ленты
STATUS_ORDERED = "ordered"
STATUS_SOLD = "sold"
STATUS_RETURNED = "returned"
STATUS_CANCELLED = "cancelled"

STATUS_LABELS = {
    STATUS_ORDERED: "Заказан",
    STATUS_SOLD: "Выкуплен",
    STATUS_RETURNED: "Возврат",
    STATUS_CANCELLED: "Отменён",
}

STATUS_CSS = {
    STATUS_ORDERED: "status-ordered",
    STATUS_SOLD: "status-sold",
    STATUS_RETURNED: "status-returned",
    STATUS_CANCELLED: "status-cancelled",
}


def _sales_period_cache_path(user_id: int) -> str:
    return os.path.join(CACHE_DIR, f"sales_period_user_{user_id}.json")


def _status_history_path(user_id: int) -> str:
    return os.path.join(CACHE_DIR, f"order_status_history_user_{user_id}.json")


def load_sales_period_cache(user_id: int) -> Dict[str, Any]:
    path = _sales_period_cache_path(user_id)
    if not os.path.isfile(path):
        return {"days": {}}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            if not isinstance(data, dict):
                return {"days": {}}
            data.setdefault("days", {})
            return data
    except Exception:
        return {"days": {}}


def save_sales_period_cache(user_id: int, payload: Dict[str, Any]) -> None:
    path = _sales_period_cache_path(user_id)
    try:
        enriched = dict(payload)
        enriched["_user_id"] = user_id
        with open(path, "w", encoding="utf-8") as f:
            json.dump(enriched, f, ensure_ascii=False)
    except Exception:
        pass


def load_status_history(user_id: int) -> Dict[str, List[Dict[str, Any]]]:
    path = _status_history_path(user_id)
    if not os.path.isfile(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def save_status_history(user_id: int, history: Dict[str, List[Dict[str, Any]]]) -> None:
    path = _status_history_path(user_id)
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(history, f, ensure_ascii=False)
    except Exception:
        pass


def _parse_wb_dt(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=None) if value.tzinfo else value
    s = str(value).strip()
    if not s or s.startswith("0001-01-01"):
        return None
    # Убираем таймзону для единообразия
    core = s.replace("Z", "")
    if "+" in core[10:]:
        core = core[: core.rfind("+", 10)]
    core = core.strip()
    for fmt, n in (
        ("%Y-%m-%dT%H:%M:%S", 19),
        ("%Y-%m-%d %H:%M:%S", 19),
        ("%d.%m.%Y %H:%M:%S", 19),
        ("%Y-%m-%d", 10),
        ("%d.%m.%Y", 10),
    ):
        try:
            return datetime.strptime(core[:n], fmt)
        except Exception:
            continue
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        return None


def format_dt_display(value: Any) -> str:
    dt = _parse_wb_dt(value)
    if not dt:
        return "—"
    if isinstance(value, str) and len(value.strip()) <= 10 and "T" not in value and " " not in value.strip():
        return dt.strftime("%d.%m.%Y")
    if dt.hour == 0 and dt.minute == 0 and dt.second == 0 and (
        not isinstance(value, str) or "T" not in value
    ):
        return dt.strftime("%d.%m.%Y")
    return dt.strftime("%d.%m.%Y %H:%M")


def format_money(value: Any) -> Optional[float]:
    try:
        if value is None or value == "":
            return None
        return round(float(value), 2)
    except (TypeError, ValueError):
        return None


def detect_scheme(warehouse_type: Any) -> str:
    wt = str(warehouse_type or "").strip().lower()
    if "wb" in wt or "склад wb" in wt:
        return "FBW"
    if not wt:
        return "—"
    return "FBS"


def _is_return_sale(item: Dict[str, Any]) -> bool:
    sale_id = str(item.get("saleID") or item.get("saleId") or "")
    if sale_id.upper().startswith("R"):
        return True
    try:
        price = float(item.get("priceWithDisc") or item.get("finishedPrice") or 0)
        if price < 0:
            return True
    except (TypeError, ValueError):
        pass
    return False


def index_sales_by_srid(sales: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """srid -> {sold_at, returned_at, sale, return}"""
    out: Dict[str, Dict[str, Any]] = {}
    for item in sales:
        srid = str(item.get("srid") or "").strip()
        if not srid:
            continue
        bucket = out.setdefault(srid, {})
        dt = _parse_wb_dt(item.get("date") or item.get("lastChangeDate"))
        if _is_return_sale(item):
            prev = bucket.get("returned_at")
            if dt and (not prev or dt > prev):
                bucket["returned_at"] = dt
                bucket["return"] = item
        else:
            prev = bucket.get("sold_at")
            if dt and (not prev or dt > prev):
                bucket["sold_at"] = dt
                bucket["sale"] = item
            elif "sale" not in bucket:
                bucket["sale"] = item
                if dt:
                    bucket["sold_at"] = dt
    return out


def resolve_status(
    order: Dict[str, Any],
    sale_info: Optional[Dict[str, Any]] = None,
) -> Tuple[str, str, str]:
    """Возвращает (code, label, css_class)."""
    if order.get("is_cancelled"):
        return STATUS_CANCELLED, STATUS_LABELS[STATUS_CANCELLED], STATUS_CSS[STATUS_CANCELLED]
    if sale_info:
        if sale_info.get("returned_at") or sale_info.get("return"):
            # возврат после выкупа
            if sale_info.get("sold_at") or sale_info.get("sale"):
                return STATUS_RETURNED, STATUS_LABELS[STATUS_RETURNED], STATUS_CSS[STATUS_RETURNED]
            return STATUS_RETURNED, STATUS_LABELS[STATUS_RETURNED], STATUS_CSS[STATUS_RETURNED]
        if sale_info.get("sold_at") or sale_info.get("sale"):
            return STATUS_SOLD, STATUS_LABELS[STATUS_SOLD], STATUS_CSS[STATUS_SOLD]
    return STATUS_ORDERED, STATUS_LABELS[STATUS_ORDERED], STATUS_CSS[STATUS_ORDERED]


def extend_iso_date(date_str: str, days: int) -> str:
    """Сдвигает YYYY-MM-DD на days дней вперёд."""
    try:
        dt = datetime.strptime(str(date_str)[:10], "%Y-%m-%d").date()
        return (dt + timedelta(days=int(days))).strftime("%Y-%m-%d")
    except Exception:
        return date_str


# Продажа часто позже даты заказа — ищем продажи с запасом
SALES_LOOKUP_EXTRA_DAYS = 21
# Заказ обычно раньше выкупа — при фильтре по выкупам смотрим заказы назад
ORDERS_LOOKUP_BEFORE_SALE_DAYS = 90


def iso_date_in_range(value: Any, date_from: str, date_to: str) -> bool:
    """Проверяет, попадает ли дата (datetime / ISO / display) в [date_from, date_to]."""
    if not date_from or not date_to:
        return False
    dt = value if isinstance(value, datetime) else _parse_wb_dt(value)
    if not dt:
        return False
    day = dt.strftime("%Y-%m-%d")
    return date_from <= day <= date_to


def build_timeline(
    order: Dict[str, Any],
    sale_info: Optional[Dict[str, Any]] = None,
    history_events: Optional[List[Dict[str, Any]]] = None,
    current_status: Optional[str] = None,
) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []

    created = order.get("ДатаВремя") or order.get("Дата")
    created_dt = _parse_wb_dt(created)
    if created_dt:
        events.append({
            "status": STATUS_ORDERED,
            "label": "Заказ оформлен",
            "at": created_dt.isoformat(sep=" "),
            "at_display": format_dt_display(created_dt),
            "source": "order",
        })

    if sale_info and sale_info.get("sold_at"):
        events.append({
            "status": STATUS_SOLD,
            "label": "Выкуплен",
            "at": sale_info["sold_at"].isoformat(sep=" "),
            "at_display": format_dt_display(sale_info["sold_at"]),
            "source": "sales",
        })

    if sale_info and sale_info.get("returned_at"):
        events.append({
            "status": STATUS_RETURNED,
            "label": "Возврат",
            "at": sale_info["returned_at"].isoformat(sep=" "),
            "at_display": format_dt_display(sale_info["returned_at"]),
            "source": "sales",
        })

    if order.get("is_cancelled"):
        cancel_raw = order.get("Дата и время отмены заказа")
        cancel_dt = _parse_wb_dt(cancel_raw) or created_dt
        if cancel_dt:
            events.append({
                "status": STATUS_CANCELLED,
                "label": "Отменён",
                "at": cancel_dt.isoformat(sep=" "),
                "at_display": format_dt_display(cancel_dt),
                "source": "order",
            })

    # История не должна показывать «Выкуплен», если сейчас заказ только оформлен
    allowed_hist = {
        STATUS_ORDERED: {STATUS_ORDERED},
        STATUS_SOLD: {STATUS_ORDERED, STATUS_SOLD},
        STATUS_RETURNED: {STATUS_ORDERED, STATUS_SOLD, STATUS_RETURNED},
        STATUS_CANCELLED: {STATUS_ORDERED, STATUS_CANCELLED},
    }.get(current_status or STATUS_ORDERED, {STATUS_ORDERED})

    primary_statuses = {e.get("status") for e in events}
    for ev in history_events or []:
        st = ev.get("status")
        if st not in allowed_hist:
            continue
        if st in primary_statuses:
            continue
        if st in (STATUS_SOLD, STATUS_RETURNED) and not sale_info:
            continue
        label = ev.get("label") or STATUS_LABELS.get(st, st)
        at = ev.get("at")
        events.append({
            "status": st,
            "label": label,
            "at": at,
            "at_display": format_dt_display(at),
            "source": ev.get("source") or "history",
        })
        primary_statuses.add(st)

    seen = set()
    unique: List[Dict[str, Any]] = []
    for ev in events:
        key = (ev.get("status"), ev.get("at_display"), ev.get("label"))
        if key in seen:
            continue
        seen.add(key)
        unique.append(ev)

    unique.sort(key=lambda e: e.get("at") or "")
    return unique


def record_status_observation(
    history: Dict[str, List[Dict[str, Any]]],
    srid: str,
    status: str,
    at: Optional[str] = None,
) -> None:
    if not srid:
        return
    now = datetime.now(MOSCOW_TZ).strftime("%Y-%m-%d %H:%M:%S")
    events = history.setdefault(srid, [])
    if events and events[-1].get("status") == status:
        return
    events.append({
        "status": status,
        "label": STATUS_LABELS.get(status, status),
        "at": at or now,
        "source": "observation",
        "seen_at": now,
    })
    # ограничиваем хвост
    if len(events) > 40:
        history[srid] = events[-40:]


def _products_index(user_id: int) -> Dict[int, Dict[str, Any]]:
    by_nm: Dict[int, Dict[str, Any]] = {}
    try:
        from utils.cache import load_products_cache_for_user
        cached = load_products_cache_for_user(user_id) or {}
    except Exception:
        cached = {}
    for it in cached.get("items") or []:
        nm = it.get("nm_id") or it.get("nmId") or it.get("nmID")
        try:
            if nm is not None:
                by_nm[int(nm)] = it
        except (TypeError, ValueError):
            continue
    return by_nm


def _purchase_index(user_id: int) -> Dict[str, float]:
    out: Dict[str, float] = {}
    try:
        rows = PurchasePrice.query.filter_by(user_id=user_id).all()
        for r in rows:
            if r.barcode:
                out[str(r.barcode)] = float(r.price)
    except Exception:
        pass
    return out


def normalize_order_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Приводит строки кэша заказов (в т.ч. старый формат с _order_date) к единым ключам."""
    if not isinstance(row, dict):
        return {}
    o = dict(row)

    if not o.get("Дата"):
        o["Дата"] = (
            o.get("ДатаВремя")
            or o.get("date")
            or o.get("Дата заказа")
            or o.get("orderDate")
            or o.get("_order_date")
            or ""
        )
    if not o.get("ДатаВремя"):
        o["ДатаВремя"] = o.get("date") or o.get("Дата") or o.get("_order_date") or ""

    if not o.get("Склад отгрузки"):
        o["Склад отгрузки"] = (
            o.get("warehouseName")
            or o.get("_warehouse")
            or o.get("_warehouse_label")
            or ""
        )

    if not o.get("Артикул продавца"):
        o["Артикул продавца"] = (
            o.get("supplierArticle")
            or o.get("_supplier_article")
            or ""
        )

    if o.get("Артикул WB") is None:
        o["Артикул WB"] = o.get("nmId") or o.get("nmID") or o.get("_nm_id")

    if not o.get("Баркод"):
        o["Баркод"] = o.get("barcode") or o.get("_barcode") or ""

    if o.get("Цена со скидкой продавца") is None:
        o["Цена со скидкой продавца"] = (
            o.get("priceWithDisc")
            or o.get("_price")
            or o.get("Цена")
            or 0
        )

    # Тип склада: в старом кэше поля нет — по имени склада WB-региона это обычно FBW
    if not o.get("Тип склада хранения товаров"):
        wt = o.get("warehouseType") or ""
        if not wt:
            warehouse = str(o.get("Склад отгрузки") or "").strip()
            if warehouse:
                # Имена вроде «Котовск», «Электросталь» — склады WB (FBW)
                wt = "Склад WB"
        o["Тип склада хранения товаров"] = wt

    if "is_cancelled" not in o:
        cancel_raw = o.get("isCancel") if o.get("isCancel") is not None else o.get("Отмена заказа")
        o["is_cancelled"] = cancel_raw is True or str(cancel_raw).lower() in ("true", "1", "истина")

    if not o.get("Уникальный ID заказа"):
        o["Уникальный ID заказа"] = o.get("srid") or ""
    if not o.get("Номер заказа"):
        o["Номер заказа"] = o.get("gNumber") or ""

    return o


def _finance_srid_cache_path(user_id: int) -> str:
    return os.path.join(CACHE_DIR, f"finance_srid_user_{user_id}.json")


def load_finance_srid_index(user_id: int) -> Dict[str, Dict[str, Any]]:
    """srid -> {acquiring, ppvz_for_pay, ...}"""
    path = _finance_srid_cache_path(user_id)
    if not os.path.isfile(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            by_srid = (data or {}).get("by_srid") or {}
            return by_srid if isinstance(by_srid, dict) else {}
    except Exception:
        return {}


def save_finance_srid_index(user_id: int, by_srid: Dict[str, Dict[str, Any]]) -> None:
    path = _finance_srid_cache_path(user_id)
    try:
        payload = {
            "_user_id": user_id,
            "updated_at": datetime.now(MOSCOW_TZ).strftime("%d.%m.%Y %H:%M:%S"),
            "by_srid": by_srid,
        }
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)
    except Exception:
        pass


def _merge_finance_srid_row(bucket: Dict[str, Any], row: Dict[str, Any]) -> None:
    try:
        fee = float(row.get("acquiring_fee") or 0)
    except (TypeError, ValueError):
        fee = 0.0
    try:
        pay = float(row.get("ppvz_for_pay") or 0)
    except (TypeError, ValueError):
        pay = 0.0
    bucket["acquiring"] = round(float(bucket.get("acquiring") or 0) + fee, 2)
    bucket["ppvz_for_pay"] = round(float(bucket.get("ppvz_for_pay") or 0) + pay, 2)
    oper = str(row.get("supplier_oper_name") or "")
    doc = str(row.get("doc_type_name") or "")
    if fee and not bucket.get("acquiring_percent"):
        try:
            bucket["acquiring_percent"] = float(row.get("acquiring_percent") or 0) or None
        except (TypeError, ValueError):
            pass
    if ("продажа" in oper.lower() or "продажа" in doc.lower()) and pay:
        bucket["sale_ppvz_for_pay"] = round(float(bucket.get("sale_ppvz_for_pay") or 0) + pay, 2)
        bucket["sale_acquiring"] = round(float(bucket.get("sale_acquiring") or 0) + fee, 2)


def update_finance_srid_index_from_api(
    user_id: int,
    token: str,
    date_from: str,
    date_to: str,
) -> Dict[str, Dict[str, Any]]:
    """Тянет фин. отчёт за период и дописывает индекс по srid (эквайринг / к перечислению)."""
    from utils.api import fetch_finance_report

    rows = fetch_finance_report(token, date_from, date_to)
    existing = load_finance_srid_index(user_id)
    # Пересобираем только затронутые srid из этого ответа: сначала сгруппируем строки периода,
    # затем заменим записи этих srid целиком (чтобы не задвоить при повторном refresh).
    period_srids: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        srid = str(row.get("srid") or "").strip()
        if not srid:
            continue
        bucket = period_srids.setdefault(srid, {"acquiring": 0.0, "ppvz_for_pay": 0.0})
        _merge_finance_srid_row(bucket, row)

    for srid, bucket in period_srids.items():
        # Для «К перечислению» в ЛК по продаже важны суммы операции Продажа
        if bucket.get("sale_ppvz_for_pay") is not None:
            bucket["ppvz_for_pay"] = bucket["sale_ppvz_for_pay"]
        if bucket.get("sale_acquiring") is not None:
            bucket["acquiring"] = bucket["sale_acquiring"]
        existing[srid] = {
            "acquiring": bucket.get("acquiring") or 0.0,
            "ppvz_for_pay": bucket.get("ppvz_for_pay") or 0.0,
            "acquiring_percent": bucket.get("acquiring_percent"),
        }

    save_finance_srid_index(user_id, existing)
    return existing


def collect_orders_from_period_cache(
    user_id: int,
    date_from: str,
    date_to: str,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    cache = load_orders_period_cache(user_id) or {}
    days_map = cache.get("days") or {}
    from utils.cache import _daterange_inclusive

    orders: List[Dict[str, Any]] = []
    missing_days: List[str] = []
    for day in _daterange_inclusive(date_from, date_to):
        entry = days_map.get(day)
        if not entry:
            missing_days.append(day)
            continue
        day_orders = entry.get("orders") or []
        if isinstance(day_orders, list):
            for raw in day_orders:
                if isinstance(raw, dict):
                    orders.append(normalize_order_row(raw))
    meta = {
        "cache_days": len(days_map),
        "missing_days": missing_days,
        "has_gaps": bool(missing_days),
    }
    return orders, meta


def collect_sales_from_period_cache(
    user_id: int,
    date_from: str,
    date_to: str,
) -> List[Dict[str, Any]]:
    cache = load_sales_period_cache(user_id)
    days_map = cache.get("days") or {}
    from utils.cache import _daterange_inclusive

    sales: List[Dict[str, Any]] = []
    for day in _daterange_inclusive(date_from, date_to):
        entry = days_map.get(day)
        if not entry:
            continue
        day_sales = entry.get("sales") or []
        if isinstance(day_sales, list):
            sales.extend(day_sales)
    return sales


def update_sales_period_cache_from_api(
    user_id: int,
    token: str,
    date_from: str,
    date_to: str,
) -> List[Dict[str, Any]]:
    """Тянет продажи за период и сохраняет по дням."""
    from utils.api import fetch_sales_range
    from utils.cache import _daterange_inclusive

    raw = fetch_sales_range(token, date_from, date_to)
    by_day: Dict[str, List[Dict[str, Any]]] = {}
    for item in raw:
        dt = _parse_wb_dt(item.get("date") or item.get("lastChangeDate"))
        if not dt:
            continue
        day = dt.strftime("%Y-%m-%d")
        by_day.setdefault(day, []).append(item)

    cache = load_sales_period_cache(user_id)
    days_map = cache.setdefault("days", {})
    now_s = datetime.now(MOSCOW_TZ).strftime("%d.%m.%Y %H:%M:%S")
    for day in _daterange_inclusive(date_from, date_to):
        days_map[day] = {
            "sales": by_day.get(day, []),
            "updated_at": now_s,
        }
    save_sales_period_cache(user_id, cache)
    return raw


def build_feed_items(
    user_id: int,
    orders: List[Dict[str, Any]],
    sales: Optional[List[Dict[str, Any]]] = None,
    *,
    record_history: bool = True,
) -> List[Dict[str, Any]]:
    products = _products_index(user_id)
    purchases = _purchase_index(user_id)
    sales_idx = index_sales_by_srid(sales or [])
    finance_idx = load_finance_srid_index(user_id)
    # Историю всегда читаем для таймлайна; пишем только при record_history=True
    history = load_status_history(user_id)

    items: List[Dict[str, Any]] = []
    for raw_order in orders:
        order = normalize_order_row(raw_order)
        srid = str(order.get("Уникальный ID заказа") or "").strip()
        gnumber = str(order.get("Номер заказа") or "").strip()
        nm_raw = order.get("Артикул WB")
        try:
            nm_id = int(nm_raw) if nm_raw is not None else None
        except (TypeError, ValueError):
            nm_id = None

        barcode = str(order.get("Баркод") or "").strip()
        prod = products.get(nm_id) if nm_id is not None else None
        name = None
        photo = None
        if prod:
            name = prod.get("name") or prod.get("title")
            photo = prod.get("photo")
        if not name:
            subject = order.get("Предмет") or ""
            article = order.get("Артикул продавца") or ""
            name = f"{subject} {article}".strip() or "Без названия"

        sale_info = sales_idx.get(srid) if srid else None
        status, status_label, status_css = resolve_status(order, sale_info)

        purchase_price = purchases.get(barcode) if barcode else None
        price_seller = format_money(order.get("Цена со скидкой продавца"))
        price_buyer = format_money(order.get("Цена с учетом всех скидок"))

        created_raw = order.get("ДатаВремя") or order.get("Дата")
        updated_raw = order.get("Дата и время обновления информации в сервисе")

        hist = history.get(srid) if srid else []
        timeline = build_timeline(order, sale_info, hist, current_status=status)

        if record_history and srid:
            at_hint = None
            if status == STATUS_CANCELLED:
                at_hint = str(order.get("Дата и время отмены заказа") or "")
            elif status == STATUS_SOLD and sale_info and sale_info.get("sold_at"):
                at_hint = sale_info["sold_at"].isoformat(sep=" ")
            elif status == STATUS_RETURNED and sale_info and sale_info.get("returned_at"):
                at_hint = sale_info["returned_at"].isoformat(sep=" ")
            else:
                at_hint = str(created_raw or "")
            record_status_observation(history, srid, status, at_hint)

        scheme = detect_scheme(order.get("Тип склада хранения товаров"))

        # Финансы / к перечислению / маржа — только при подтверждённой продаже
        for_pay_gross = None
        acquiring = None
        for_pay_net = None
        if sale_info and status in (STATUS_SOLD, STATUS_RETURNED):
            for_pay_gross = format_money((sale_info.get("sale") or {}).get("forPay"))
            for_pay_net = for_pay_gross
            fin = finance_idx.get(srid) if srid else None
            if fin:
                acquiring = format_money(fin.get("acquiring"))
                if fin.get("ppvz_for_pay") is not None:
                    for_pay_net = format_money(fin.get("ppvz_for_pay"))
                elif for_pay_gross is not None and acquiring is not None:
                    for_pay_net = round(float(for_pay_gross) - float(acquiring), 2)
                if acquiring == 0:
                    acquiring = 0.0

        margin_value = None
        margin_pct = None
        if (
            status in (STATUS_SOLD, STATUS_RETURNED)
            and for_pay_net is not None
            and purchase_price is not None
        ):
            try:
                margin_value = round(float(for_pay_net) - float(purchase_price), 2)
                if float(purchase_price) > 0:
                    margin_pct = round(margin_value / float(purchase_price) * 100.0, 1)
            except (TypeError, ValueError):
                margin_value = None
                margin_pct = None

        sold_at_dt = sale_info.get("sold_at") if sale_info else None

        items.append({
            "id": srid or gnumber,
            "srid": srid,
            "gnumber": gnumber,
            "sticker": order.get("ID стикера"),
            "datetime": created_raw,
            "datetime_display": format_dt_display(created_raw),
            "sold_at": sold_at_dt.strftime("%Y-%m-%d") if sold_at_dt else None,
            "sold_at_display": format_dt_display(sold_at_dt) if sold_at_dt else None,
            "updated_at": updated_raw,
            "updated_at_display": format_dt_display(updated_raw),
            "status": status,
            "status_label": status_label,
            "status_css": status_css,
            "scheme": scheme,
            "warehouse": order.get("Склад отгрузки"),
            "warehouse_type": order.get("Тип склада хранения товаров"),
            "country": order.get("Страна"),
            "region": order.get("Регион"),
            "oblast": order.get("Округ"),
            "nm_id": nm_id,
            "article": order.get("Артикул продавца"),
            "barcode": barcode,
            "brand": order.get("Бренд"),
            "subject": order.get("Предмет"),
            "category": order.get("Категория"),
            "size": order.get("Размер товара"),
            "name": name,
            "photo": photo,
            "qty": 1,
            "price": price_seller,
            "price_buyer": price_buyer,
            "price_total": format_money(order.get("Цена без скидок")),
            "discount_seller": order.get("Скидка продавца"),
            "discount_wb": order.get("Скидка WB"),
            "purchase_price": purchase_price,
            "margin": margin_value,
            "margin_pct": margin_pct,
            "income_id": order.get("Номер поставки"),
            "is_cancelled": bool(order.get("is_cancelled")),
            "cancel_date": order.get("Дата и время отмены заказа"),
            "cancel_date_display": format_dt_display(order.get("Дата и время отмены заказа")),
            "timeline": timeline,
            "sale": {
                "sold_at": format_dt_display(sale_info["sold_at"]) if sale_info.get("sold_at") else None,
                "returned_at": format_dt_display(sale_info["returned_at"]) if sale_info.get("returned_at") else None,
                "for_pay_gross": for_pay_gross,
                "acquiring": acquiring,
                "for_pay": for_pay_net,
            } if sale_info else None,
        })

    if record_history:
        save_status_history(user_id, history)

    # Свежие сверху
    def sort_key(it: Dict[str, Any]):
        dt = _parse_wb_dt(it.get("datetime")) or datetime.min
        return dt

    items.sort(key=sort_key, reverse=True)
    return items


def default_date_range(days: int = 7) -> Tuple[str, str]:
    today = datetime.now(MOSCOW_TZ).date()
    start = today - timedelta(days=max(days - 1, 0))
    return start.strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d")

# -*- coding: utf-8 -*-
"""Функции для обработки данных FBS и DBS"""
from typing import List, Dict, Any
from datetime import datetime, timezone
from utils.helpers import parse_wb_datetime, to_moscow


def _extract_created_at(obj: Any) -> datetime:
    """Извлекает дату создания; всегда naive (для корректной сортировки)."""
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
    try:
        if isinstance(val, (int, float)):
            ts = float(val)
            if ts > 1e12:
                return datetime.utcfromtimestamp(ts / 1000)
            if ts > 1e9:
                return datetime.utcfromtimestamp(ts)
        s = str(val).strip()
        if not s:
            return datetime.min
        if s.isdigit():
            ts = float(s)
            if ts > 1e12:
                return datetime.utcfromtimestamp(ts / 1000)
            if ts > 1e9:
                return datetime.utcfromtimestamp(ts)
        dt = parse_wb_datetime(s)
        if dt is None:
            for fmt, n in (("%d.%m.%Y %H:%M", 16), ("%d.%m.%Y", 10)):
                try:
                    return datetime.strptime(s[:n], fmt)
                except Exception:
                    pass
            return datetime.min
        if dt.tzinfo is not None:
            return dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt
    except Exception:
        return datetime.min


def to_fbs_rows(orders: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Преобразует заказы FBS в строки для таблицы"""
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
            "Склад": warehouse,
            "nm_id": o.get("nmId") or o.get("nmID"),
            "ID": order_id,
        })
    return rows


def to_dbs_rows(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Преобразует заказы DBS в строки для таблицы"""
    rows: List[Dict[str, Any]] = []
    for it in items:
        try:
            created_raw = (
                it.get("createdAt")
                or it.get("dateCreated")
                or it.get("date")
            )
            try:
                _dt = parse_wb_datetime(str(created_raw)) if created_raw else None
                _dt_msk = to_moscow(_dt) if _dt else None
                created_str = _dt_msk.strftime("%d.%m.%Y %H:%M") if _dt_msk else (str(created_raw) if created_raw else "")
                created_date = _dt_msk.strftime("%d.%m.%Y") if _dt_msk else ""
            except Exception:
                created_str = str(created_raw) if created_raw else ""
                created_date = ""
            nm_id = it.get("nmId") or it.get("nmID")
            article = it.get("article") or it.get("vendorCode") or ""
            price_raw = (
                it.get("convertedFinalPrice")
                or it.get("finalPrice")
                or it.get("convertedPrice")
                or it.get("salePrice")
                or it.get("price")
            )
            try:
                price_value = int(price_raw) // 100
            except Exception:
                try:
                    price_value = int(float(price_raw)) // 100
                except Exception:
                    price_value = price_raw
            addr = None
            adr = it.get("address") or {}
            if isinstance(adr, dict):
                addr = adr.get("fullAddress") or None
            status_val = (
                it.get("supplierStatus")
                or it.get("status")
                or it.get("wbStatus")
            )
            status_name_val = (
                it.get("statusName")
                or it.get("supplierStatusName")
                or it.get("wbStatusName")
                or status_val
            )
            skus = it.get("skus") if isinstance(it.get("skus"), list) else []
            barcode = str(skus[0]) if skus else (it.get("barcode") or "")
            d_date = it.get("dDate") or it.get("ddate") or it.get("deliveryDate") or ""
            # ISO date → ДД.ММ.ГГГГ
            if isinstance(d_date, str) and len(d_date) >= 10 and d_date[4] == "-":
                try:
                    d_date = datetime.fromisoformat(d_date[:10]).strftime("%d.%m.%Y")
                except Exception:
                    pass
            d_from = it.get("dTimeFrom") or it.get("dTimeFromOld") or ""
            d_to = it.get("dTimeTo") or it.get("dTimeToOld") or ""
            phone = it.get("phone") or it.get("buyerPhone") or ""
            phone_code = it.get("phoneCode") or ""
            full_name = it.get("fullName") or it.get("firstName") or ""
            warehouse = (
                it.get("warehouseName")
                or it.get("warehouse")
                or it.get("warehouseId")
                or ""
            )
            title = it.get("title") or ""
            photo = it.get("photo") or it.get("img") or ""
            rows.append({
                "orderId": it.get("id") or it.get("orderId") or it.get("ID"),
                "Номер и дата заказа": f"{it.get('id') or it.get('orderId') or ''} | {created_str}".strip(" |"),
                "Наименование товара": article,
                "title": title,
                "photo": photo,
                "Цена": price_value,
                "Адрес": addr or "",
                "nm_id": nm_id,
                "status": status_val,
                "statusName": status_name_val,
                "wbStatus": it.get("wbStatus") or "",
                "barcode": barcode,
                "requiredMeta": it.get("requiredMeta") or [],
                "createdAt": created_str,
                "createdDate": created_date,
                "dDate": d_date,
                "dTimeFrom": d_from,
                "dTimeTo": d_to,
                "phone": phone,
                "phoneCode": phone_code,
                "fullName": full_name,
                "warehouse": warehouse,
                "deliveryType": it.get("deliveryType") or "dbs",
            })
        except Exception:
            continue
    return rows



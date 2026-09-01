# -*- coding: utf-8 -*-
"""Blueprint для поставок FBS (список и состав поставок)."""

import io
import requests
from datetime import datetime
from flask import Blueprint, jsonify, request, send_file
from flask_login import login_required, current_user
from utils.wb_token import effective_wb_api_token
from typing import Dict, Any, List

from utils.api import get_with_retry
from utils.cache import load_fbs_supplies_cache, save_fbs_supplies_cache, load_products_cache
from utils.constants import (
    FBS_SUPPLIES_LIST_URL,
    FBS_SUPPLY_ORDERS_URL,
    FBS_SUPPLY_ORDERS_IDS_URL_V2,
    FBS_SUPPLY_ORDERS_IDS_URL_V3,
    FBS_SUPPLY_ADD_ORDERS_URL,
    FBS_ORDERS_URL,
    MOSCOW_TZ,
)
from utils.helpers import parse_wb_datetime, to_moscow

fbs_supplies_bp = Blueprint("fbs_supplies", __name__)


@fbs_supplies_bp.route("/api/fbs/supplies", methods=["GET"])
@login_required
def api_fbs_supplies():
    """Список поставок FBS (с пагинацией и возможностью обновления)."""
    token = effective_wb_api_token(current_user)
    if not token:
        return jsonify({"items": [], "lastUpdated": None}), 200

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

    # Всегда пробуем загрузить из API с fallback на кэш
    all_supplies_raw: List[Dict[str, Any]] = []
    try:
        headers_list = [
            {"Authorization": f"{token}"},
            {"Authorization": f"Bearer {token}"},
        ]
        for hdrs in headers_list:
            try:
                resp = get_with_retry(
                    FBS_SUPPLIES_LIST_URL,
                    hdrs,
                    params={"limit": 1000, "next": 0},
                    timeout_s=10,
                )
                data = resp.json()
                print(f"FBS supplies API response: type={type(data)}, keys={list(data.keys()) if isinstance(data, dict) else 'not dict'}")
                if isinstance(data, list):
                    all_supplies_raw = data
                    print(f"Got {len(all_supplies_raw)} supplies from list response")
                elif isinstance(data, dict):
                    all_supplies_raw = (
                        data.get("supplies", []) or data.get("data", []) or []
                    )
                    print(f"Got {len(all_supplies_raw)} supplies from dict response")
                    if all_supplies_raw and isinstance(all_supplies_raw[0], dict):
                        print(f"First supply sample keys: {list(all_supplies_raw[0].keys())}")
                        print(f"First supply sample: {all_supplies_raw[0]}")
                else:
                    continue
                break
            except requests.RequestException:
                continue
        if not all_supplies_raw:
            cached = load_fbs_supplies_cache() or {}
            all_supplies_raw = cached.get("all_supplies_raw", [])
    except Exception:
        cached = load_fbs_supplies_cache() or {}
        all_supplies_raw = cached.get("all_supplies_raw", [])

    # Сортируем по дате создания (новые сверху)
    try:
        all_supplies_raw.sort(key=lambda x: x.get("createdAt", ""), reverse=True)
    except Exception:
        pass

    # Получаем все заказы и считаем количество для каждой поставки
    supply_counts: Dict[str, int] = {}
    try:
        headers_list = [
            {"Authorization": f"{token}"},
            {"Authorization": f"Bearer {token}"},
        ]
        for hdrs in headers_list:
            try:
                orders_url = FBS_ORDERS_URL
                orders_params = {"limit": 1000, "next": 0}
                orders_resp = requests.get(orders_url, headers=hdrs, params=orders_params, timeout=30)
                if orders_resp.status_code == 200:
                    orders_data = orders_resp.json()
                    all_orders: List[Dict[str, Any]] = []
                    if isinstance(orders_data, dict):
                        if isinstance(orders_data.get("orders"), list):
                            all_orders = orders_data["orders"]
                    elif isinstance(orders_data, list):
                        all_orders = [it for it in orders_data if isinstance(it, dict)]
                    
                    # Группируем заказы по supplyId
                    for order in all_orders:
                        if not isinstance(order, dict):
                            continue
                        order_supply_id = None
                        for field in ["supplyId", "supply_id", "supplyID", "supply"]:
                            if field in order:
                                order_supply_id = str(order[field])
                                break
                        if order_supply_id:
                            supply_counts[order_supply_id] = supply_counts.get(order_supply_id, 0) + 1
                    break
            except Exception:
                continue
    except Exception:
        pass

    supplies_to_process = all_supplies_raw[offset_i : offset_i + limit_i]

    # Нормализуем для фронтенда
    norm_items: List[Dict[str, Any]] = []
    for it in supplies_to_process:
        if not isinstance(it, dict):
            continue
        supply_id = it.get("id") or it.get("supplyId") or it.get("supply_id")
        if not supply_id:
            continue

        # Количество товаров (из API или из подсчета заказов)
        count = it.get("orderCount") or it.get("ordersCount") or it.get("count")
        if count is None or count == 0:
            # Используем подсчитанное количество из заказов
            count = supply_counts.get(supply_id, 0)

        # Информация о датах и статусе
        created_raw = it.get("createdAt") or it.get("dateCreated") or it.get("date")
        closed_at = it.get("closedAt") or it.get("doneAt")
        raw_status = str(it.get("status") or "").upper()
        done_flag = bool(it.get("done")) or raw_status in ("DONE", "CLOSED", "COMPLETED", "FINISHED", "SHIPPED")

        # Форматируем даты
        def _fmt(raw):
            if not raw:
                return ""
            try:
                dt = parse_wb_datetime(str(raw))
                dt_msk = to_moscow(dt) if dt else None
                return (
                    dt_msk.strftime("%d.%m.%Y %H:%M")
                    if dt_msk
                    else (str(raw) if raw else "")
                )
            except Exception:
                return str(raw)

        created_str = _fmt(created_raw)
        
        # Status label for UI
        if done_flag:
            status_label = "Отгружено"
            try:
                status_dt_str = _fmt(closed_at) if closed_at else ""
            except Exception:
                status_dt_str = str(closed_at) if closed_at else ""
        else:
            status_label = "Не отгружена"
            status_dt_str = ""

        norm_items.append(
            {
                "supplyId": supply_id,
                "date": created_str,
                "count": count,
                "status": status_label,
                "statusDt": status_dt_str,
            }
        )

    # Сохраняем кэш
    try:
        save_fbs_supplies_cache({"all_supplies_raw": all_supplies_raw})
    except Exception:
        pass

    last_updated = (
        datetime.now(MOSCOW_TZ).strftime("%d.%m.%Y %H:%M") if norm_items else None
    )

    return jsonify(
        {
            "items": norm_items,
            "total": len(all_supplies_raw),
            "hasMore": offset_i + limit_i < len(all_supplies_raw),
            "lastUpdated": last_updated,
        }
    )


def _fetch_supply_orders_raw(token: str, supply_id: str) -> tuple[List[Dict[str, Any]], str | None]:
    """Загружает сырые заказы поставки FBS через /api/v3/orders."""
    headers_list = [
        {"Authorization": f"{token}"},
        {"Authorization": f"Bearer {token}"},
    ]
    last_err: str | None = None
    items: List[Dict[str, Any]] = []
    supply_id_fields = ["supplyId", "supply_id", "supplyID", "supply"]

    for idx, hdrs in enumerate(headers_list):
        try:
            orders_params = {"limit": 1000, "next": 0}
            orders_resp = requests.get(
                FBS_ORDERS_URL, headers=hdrs, params=orders_params, timeout=30
            )
            if orders_resp.status_code != 200:
                continue

            orders_data = orders_resp.json()
            all_orders: List[Dict[str, Any]] = []
            if isinstance(orders_data, dict):
                if isinstance(orders_data.get("orders"), list):
                    all_orders = orders_data["orders"]
                elif isinstance(orders_data.get("data"), list):
                    all_orders = orders_data["data"]
            elif isinstance(orders_data, list):
                all_orders = [it for it in orders_data if isinstance(it, dict)]

            filtered_items: List[Dict[str, Any]] = []
            for order in all_orders:
                if not isinstance(order, dict):
                    continue
                order_supply_id = None
                for field in supply_id_fields:
                    if field in order:
                        order_supply_id = order[field]
                        break
                if order_supply_id and str(order_supply_id) == str(supply_id):
                    filtered_items.append(order)

            if filtered_items:
                return filtered_items, None
            items = filtered_items
        except Exception as e:
            last_err = str(e)
            continue

    return items, last_err


def _normalize_supply_order_items(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Нормализует заказы поставки и обогащает из кэша товаров."""
    norm: List[Dict[str, Any]] = []
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
        created_raw = it.get("createdAt") or it.get("dateCreated") or it.get("date")
        try:
            _dt = parse_wb_datetime(str(created_raw)) if created_raw else None
            _dt_msk = to_moscow(_dt) if _dt else None
            created_str = (
                _dt_msk.strftime("%d.%m.%Y %H:%M")
                if _dt_msk
                else (str(created_raw) if created_raw else "")
            )
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
                            bar_list = s.get("skus") or s.get("barcodes")
                            if isinstance(bar_list, list) and bar_list:
                                barcode = str(bar_list[0])
                                break

        norm.append(
            {
                "id": it.get("id") or it.get("orderId"),
                "article": it.get("article") or it.get("supplierArticle"),
                "barcode": barcode or it.get("barcode") or "",
                "nm_id": nm,
                "photo": photo,
                "createdAt": created_str,
            }
        )
    return norm


def _group_supply_items_for_export(items: List[Dict[str, Any]]) -> List[tuple[str, str, str, int]]:
    """Группирует товары поставки для экспорта: (наименование, nm_id, баркод, кол-во)."""
    agg: Dict[tuple[str, str, str], int] = {}
    for it in items:
        name = str(it.get("article") or "").strip()
        nm_id = str(it.get("nm_id") or "").strip()
        barcode = str(it.get("barcode") or "").strip()
        key = (name, nm_id, barcode)
        agg[key] = agg.get(key, 0) + 1
    return [
        (name, nm_id, barcode, qty)
        for (name, nm_id, barcode), qty in sorted(
            agg.items(), key=lambda x: (-x[1], x[0][0])
        )
    ]


def _build_supply_xls(rows: List[tuple[str, str, str, int]], sheet_name: str = "Поставка") -> io.BytesIO:
    """Собирает .xls с группированными товарами."""
    try:
        import xlwt  # type: ignore
    except Exception as exc:
        raise RuntimeError("На сервере отсутствует зависимость xlwt (для .xls)") from exc

    wb = xlwt.Workbook()
    ws = wb.add_sheet(sheet_name[:31])
    header_style = xlwt.easyxf("font: bold on; align: horiz center")
    num_style = xlwt.easyxf("align: horiz right")
    ws.write(0, 0, "Наименование", header_style)
    ws.write(0, 1, "Артикул WB (nmId)", header_style)
    ws.write(0, 2, "Баркод", header_style)
    ws.write(0, 3, "Количество", header_style)
    for row_idx, (name, nm_id, barcode, qty) in enumerate(rows, start=1):
        ws.write(row_idx, 0, name)
        ws.write(row_idx, 1, nm_id)
        ws.write(row_idx, 2, barcode)
        ws.write(row_idx, 3, qty, num_style)
    out = io.BytesIO()
    wb.save(out)
    out.seek(0)
    return out


@fbs_supplies_bp.route("/api/fbs/supplies/<supply_id>/orders", methods=["GET"])
@login_required
def api_fbs_supply_orders(supply_id: str):
    """Состав (товары) конкретной поставки FBS."""
    token = effective_wb_api_token(current_user)
    if not token:
        return jsonify({"items": []}), 200

    try:
        items, last_err = _fetch_supply_orders_raw(token, supply_id)
        norm = _normalize_supply_order_items(items)
        return jsonify({"items": norm}), 200
    except Exception as e:
        return jsonify({"items": [], "error": str(e)}), 200


@fbs_supplies_bp.route("/api/fbs/supplies/<supply_id>/export", methods=["GET"])
@login_required
def api_fbs_supply_export(supply_id: str):
    """Экспорт состава поставки FBS в Excel (сгруппировано по товарам)."""
    token = effective_wb_api_token(current_user)
    if not token:
        return ("Требуется API токен", 400)

    try:
        items, _ = _fetch_supply_orders_raw(token, supply_id)
        norm = _normalize_supply_order_items(items)
        if not norm:
            return ("Нет товаров в поставке", 400)

        grouped = _group_supply_items_for_export(norm)
        out = _build_supply_xls(grouped, sheet_name=f"Поставка {supply_id}")
        safe_id = "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in str(supply_id))
        filename = f"fbs_supply_{safe_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xls"
        return send_file(
            out,
            mimetype="application/vnd.ms-excel",
            as_attachment=True,
            download_name=filename,
        )
    except RuntimeError as exc:
        return (str(exc), 500)
    except Exception as exc:
        return (f"Ошибка: {exc}", 500)


@fbs_supplies_bp.route("/api/fbs/supplies/<supply_id>/orders/<order_id>", methods=["PATCH", "POST"])
@login_required
def api_fbs_add_order_to_supply(supply_id: str, order_id: str):
    """Добавить сборочное задание в поставку.

    Использует новый метод WB:
    PATCH /api/marketplace/v3/supplies/{supplyId}/orders
    c телом { "orders": [orderId] }.
    """
    import logging

    logger = logging.getLogger(__name__)
    logger.info(f"=== Adding order {order_id} to supply {supply_id} ===")

    token = effective_wb_api_token(current_user)
    if not token:
        logger.warning("No token provided")
        return jsonify({"error": "No token"}), 401

    headers_list = [
        {"Authorization": f"{token}"},
        {"Authorization": f"Bearer {token}"},
    ]

    url = FBS_SUPPLY_ADD_ORDERS_URL.replace("{supplyId}", str(supply_id))
    payload = {"orders": [int(order_id)]}
    last_err: str | None = None

    for hdrs in headers_list:
        try:
            hdrs_with_content = dict(hdrs)
            hdrs_with_content["Content-Type"] = "application/json"
            logger.info(f"PATCH {url} with payload={payload}")
            # Уменьшаем таймаут до 10 секунд, чтобы UI не «висел» по 30 секунд
            resp = requests.patch(url, headers=hdrs_with_content, json=payload, timeout=10)
            logger.info(f"WB response status={resp.status_code}, body={resp.text[:300]}")

            if resp.status_code in (200, 201, 204):
                return jsonify({"success": True}), 200
            if resp.status_code == 409:
                # Задание уже привязано к поставке
                return jsonify({"error": "Order already in supply"}), 409

            # Прочие ошибки — сохраняем текст и пробуем со следующим вариантом заголовков
            last_err = f"HTTP {resp.status_code}: {resp.text[:300]}"
        except Exception as e:
            import traceback

            last_err = str(e)
            logger.error(f"Exception while calling WB add-orders endpoint: {e}")
            traceback.print_exc()

    return jsonify({"error": last_err or "Unknown error"}), 500



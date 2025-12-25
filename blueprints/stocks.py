# -*- coding: utf-8 -*-
"""Blueprint для остатков на складах"""

import io
from datetime import datetime
from typing import Any, Dict, List

import requests
from flask import Blueprint, jsonify, redirect, render_template, send_file, url_for
from flask_login import current_user, login_required
from openpyxl import Workbook

from utils.cache import (
    load_products_cache,
    load_stocks_cache,
    save_stocks_cache,
)
from utils.helpers import normalize_stocks
from utils.api import get_with_retry
from utils.constants import STOCKS_API_URL


stocks_bp = Blueprint("stocks", __name__)


def fetch_stocks_resilient(token: str) -> List[Dict[str, Any]]:
    """Получает остатки с повторными попытками (снепшот всех складов)."""
    headers1 = {"Authorization": f"Bearer {token}"}
    try:
        resp = get_with_retry(STOCKS_API_URL, headers1, params={}, max_retries=1, timeout_s=30)
        return resp.json()
    except requests.HTTPError as err:
        # Если авторизация не подошла — пробуем без Bearer
        if err.response is not None and err.response.status_code in (401, 403):
            headers2 = {"Authorization": f"{token}"}
            resp2 = get_with_retry(STOCKS_API_URL, headers2, params={}, max_retries=1, timeout_s=30)
            return resp2.json()
        raise


@stocks_bp.route("/stocks", methods=["GET"])
@login_required
def stocks_page():
    """Страница остатков на складах."""
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
                save_stocks_cache(
                    {
                        "items": items,
                        "updated_at": datetime.now().strftime("%d.%m.%Y %H:%M:%S"),
                    }
                )
        except requests.HTTPError as http_err:
            error = f"Ошибка API: {http_err.response.status_code}"
        except Exception as exc:
            error = f"Ошибка: {exc}"

    # Общие итоги
    total_qty_all = sum(int(it.get("qty", 0) or 0) for it in items)
    total_in_transit_all = sum(int(it.get("in_transit", 0) or 0) for it in items)

    # Агрегация по товарам
    prod_map: Dict[tuple, Dict[str, Any]] = {}
    for it in items:
        key = (it.get("vendor_code") or "", it.get("barcode") or "")
        rec = prod_map.get(key)
        if not rec:
            rec = {
                "vendor_code": key[0],
                "barcode": key[1],
                "nm_id": it.get("nm_id"),
                "total_qty": 0,
                "total_in_transit": 0,
                "warehouses": [],
            }
            prod_map[key] = rec
        rec["total_qty"] += int(it.get("qty", 0) or 0)
        rec["total_in_transit"] += int(it.get("in_transit", 0) or 0)
        rec["warehouses"].append(
            {
                "warehouse": it.get("warehouse"),
                "qty": int(it.get("qty", 0) or 0),
                "in_transit": int(it.get("in_transit", 0) or 0),
            }
        )

    from collections import defaultdict as _dd

    for rec in prod_map.values():
        qty_acc = _dd(int)
        transit_acc = _dd(int)
        for w in rec["warehouses"]:
            name = w.get("warehouse") or ""
            qty_acc[name] += int(w.get("qty", 0) or 0)
            transit_acc[name] += int(w.get("in_transit", 0) or 0)
        wh_list = [
            {"warehouse": name, "qty": qty, "in_transit": transit_acc.get(name, 0)}
            for name, qty in qty_acc.items()
            if qty > 0 or transit_acc.get(name, 0) > 0
        ]
        wh_list.sort(key=lambda x: (-x["qty"], x["warehouse"]))
        rec["warehouses"] = wh_list

    # Подтягиваем фото товаров из кэша товаров
    try:
        nm_to_photo: Dict[Any, Any] = {}
        prod_cached = load_products_cache() or {}
        for it_p in (prod_cached.get("items") or []):
            nmv = it_p.get("nm_id") or it_p.get("nmId") or it_p.get("nmID")
            if nmv is None:
                continue
            photo = it_p.get("photo") or it_p.get("img")
            if photo:
                nm_to_photo[int(nmv)] = photo
        for rec in prod_map.values():
            nm = rec.get("nm_id")
            if nm is not None:
                try:
                    rec["photo"] = nm_to_photo.get(int(nm))
                except Exception:
                    rec["photo"] = nm_to_photo.get(nm)
    except Exception:
        pass

    products_agg = sorted(
        prod_map.values(), key=lambda x: (-x["total_qty"], x["vendor_code"] or "")
    )

    # Агрегация по складам
    wh_map: Dict[str, Dict[str, Any]] = {}
    for it in items:
        w = it.get("warehouse") or ""
        rec = wh_map.get(w)
        if not rec:
            rec = {
                "warehouse": w,
                "total_qty": 0,
                "total_in_transit": 0,
                "products": [],
            }
            wh_map[w] = rec
        qty_i = int(it.get("qty", 0) or 0)
        in_transit_i = int(it.get("in_transit", 0) or 0)
        rec["total_qty"] += qty_i
        rec["total_in_transit"] += in_transit_i
        rec["products"].append(
            {
                "vendor_code": it.get("vendor_code"),
                "barcode": it.get("barcode"),
                "nm_id": it.get("nm_id"),
                "qty": qty_i,
                "in_transit": in_transit_i,
            }
        )

    for rec in wh_map.values():
        rec["products"].sort(key=lambda x: (-x["qty"], x["vendor_code"] or ""))

    # Фото для товаров внутри складов
    try:
        nm_to_photo: Dict[Any, Any] = {}
        prod_cached = load_products_cache() or {}
        for it_p in (prod_cached.get("items") or []):
            nmv = it_p.get("nm_id") or it_p.get("nmId") or it_p.get("nmID")
            if nmv is None:
                continue
            photo = it_p.get("photo") or it_p.get("img")
            if photo:
                nm_to_photo[int(nmv)] = photo
        for wh in wh_map.values():
            for p in wh.get("products", []):
                nm = p.get("nm_id")
                if nm is not None:
                    try:
                        p["photo"] = nm_to_photo.get(int(nm))
                    except Exception:
                        p["photo"] = nm_to_photo.get(nm)
    except Exception:
        pass

    warehouses_agg = sorted(
        wh_map.values(), key=lambda x: (-x["total_qty"], x["warehouse"] or "")
    )

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


@stocks_bp.route("/api/stocks/refresh", methods=["POST"])
@login_required
def api_stocks_refresh():
    """Обновляет кэш остатков по API и возвращает краткий статус."""
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


@stocks_bp.route("/api/stocks/data", methods=["GET"])
@login_required
def api_stocks_data():
    """Возвращает агрегированные данные остатков для перерисовки таблиц без перезагрузки страницы."""
    cached = load_stocks_cache()
    if not cached or not (
        current_user.is_authenticated and cached.get("_user_id") == current_user.id
    ):
        return jsonify(
            {
                "products": [],
                "warehouses": [],
                "total_qty_all": 0,
                "updated_at": None,
                "total_in_transit_all": 0,
            }
        )

    items = cached.get("items", [])

    # Общие итоги
    try:
        total_qty_all = sum(int((it.get("qty") or 0)) for it in items)
    except Exception:
        total_qty_all = 0
    try:
        total_in_transit_all = sum(int((it.get("in_transit") or 0)) for it in items)
    except Exception:
        total_in_transit_all = 0

    # Агрегация по товарам
    prod_map: Dict[tuple, Dict[str, Any]] = {}
    for it in items:
        key = (it.get("vendor_code") or "", it.get("barcode") or "")
        rec = prod_map.get(key)
        if not rec:
            rec = {
                "vendor_code": key[0],
                "barcode": key[1],
                "nm_id": it.get("nm_id"),
                "total_qty": 0,
                "total_in_transit": 0,
                "warehouses": [],
            }
            prod_map[key] = rec
        qty_i = int(it.get("qty", 0) or 0)
        rec["total_qty"] += qty_i
        rec["total_in_transit"] += int(it.get("in_transit", 0) or 0)
        rec["warehouses"].append(
            {
                "warehouse": it.get("warehouse"),
                "qty": qty_i,
                "in_transit": int(it.get("in_transit", 0) or 0),
            }
        )

    from collections import defaultdict as _dd

    products_agg: List[Dict[str, Any]] = []
    for rec in prod_map.values():
        qty_acc = _dd(int)
        transit_acc = _dd(int)
        for w in rec["warehouses"]:
            name = w.get("warehouse") or ""
            qty_acc[name] += int(w.get("qty", 0) or 0)
            transit_acc[name] += int(w.get("in_transit", 0) or 0)
        rec["total_in_transit"] = sum(transit_acc.values())
        rec["warehouses"] = [
            {"warehouse": name, "qty": qty, "in_transit": transit_acc.get(name, 0)}
            for name, qty in qty_acc.items()
            if qty > 0 or transit_acc.get(name, 0) > 0
        ]
        rec["warehouses"].sort(key=lambda x: (-x["qty"], x["warehouse"]))
        products_agg.append(rec)

    # Фото для товаров (по товарам)
    try:
        nm_to_photo: Dict[Any, Any] = {}
        prod_cached = load_products_cache() or {}
        for it_p in (prod_cached.get("items") or []):
            nmv = it_p.get("nm_id") or it_p.get("nmId") or it_p.get("nmID")
            if nmv is None:
                continue
            photo = it_p.get("photo") or it_p.get("img")
            if photo:
                nm_to_photo[int(nmv)] = photo
        for rec in products_agg:
            nm = rec.get("nm_id")
            if nm is not None:
                try:
                    rec["photo"] = nm_to_photo.get(int(nm))
                except Exception:
                    rec["photo"] = nm_to_photo.get(nm)
    except Exception:
        pass

    products_agg.sort(key=lambda x: (-x["total_qty"], x["vendor_code"] or ""))

    # Агрегация по складам
    wh_map: Dict[str, Dict[str, Any]] = {}
    for it in items:
        w = it.get("warehouse") or ""
        rec = wh_map.get(w)
        if not rec:
            rec = {
                "warehouse": w,
                "total_qty": 0,
                "total_in_transit": 0,
                "products": [],
            }
            wh_map[w] = rec
        qty_i = int(it.get("qty", 0) or 0)
        in_transit_i = int(it.get("in_transit", 0) or 0)
        rec["total_qty"] += qty_i
        rec["total_in_transit"] += in_transit_i
        rec["products"].append(
            {
                "vendor_code": it.get("vendor_code"),
                "nm_id": it.get("nm_id"),
                "barcode": it.get("barcode"),
                "qty": qty_i,
                "in_transit": in_transit_i,
            }
        )

    for rec in wh_map.values():
        rec["products"].sort(key=lambda x: (-x["qty"], x["vendor_code"] or ""))

    # Фото для вложенных товаров по складам
    try:
        nm_to_photo: Dict[Any, Any] = {}
        prod_cached = load_products_cache() or {}
        for it_p in (prod_cached.get("items") or []):
            nmv = it_p.get("nm_id") or it_p.get("nmId") or it_p.get("nmID")
            if nmv is None:
                continue
            photo = it_p.get("photo") or it_p.get("img")
            if photo:
                nm_to_photo[int(nmv)] = photo
        for wh in wh_map.values():
            for p in wh.get("products", []):
                nm = p.get("nm_id")
                if nm is not None:
                    try:
                        p["photo"] = nm_to_photo.get(int(nm))
                    except Exception:
                        p["photo"] = nm_to_photo.get(nm)
    except Exception:
        pass

    warehouses_agg = sorted(
        wh_map.values(), key=lambda x: (-x["total_qty"], x["warehouse"] or "")
    )

    return jsonify(
        {
            "products": products_agg,
            "warehouses": warehouses_agg,
            "total_qty_all": total_qty_all,
            "updated_at": cached.get("updated_at"),
            "total_in_transit_all": total_in_transit_all,
        }
    )


@stocks_bp.route("/stocks/export", methods=["POST"])
@login_required
def stocks_export():
    """Выгрузка остатков в Excel."""
    token = current_user.wb_token or ""
    if not token:
        return redirect(url_for("stocks.stocks_page"))
    try:
        cached = load_stocks_cache()
        if cached and cached.get("_user_id") == current_user.id:
            items = cached.get("items", [])
        else:
            raw = fetch_stocks_resilient(token)
            items = normalize_stocks(raw)
            save_stocks_cache({"items": items})

        wb = Workbook()
        ws = wb.active
        ws.title = "stocks"
        headers = ["Артикул продавца", "Баркод", "Остаток", "В пути", "Склад"]
        ws.append(headers)
        for it in items:
            ws.append(
                [
                    it.get("vendor_code", ""),
                    it.get("barcode", ""),
                    it.get("qty", 0),
                    it.get("in_transit", 0),
                    it.get("warehouse", ""),
                ]
            )
        output = io.BytesIO()
        wb.save(output)
        output.seek(0)
        from datetime import datetime as _dt

        filename = f"wb_stocks_{_dt.now().strftime('%Y%m%d_%H%M')}.xlsx"
        return send_file(
            output,
            as_attachment=True,
            download_name=filename,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    except Exception:
        return redirect(url_for("stocks.stocks_page"))



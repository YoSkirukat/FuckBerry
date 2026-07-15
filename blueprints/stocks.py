# -*- coding: utf-8 -*-
"""Blueprint для остатков на складах"""

import io
import logging
import threading
from datetime import datetime
from typing import Any, Dict, List

import requests
from flask import Blueprint, jsonify, redirect, render_template, send_file, url_for
from flask_login import current_user, login_required
from openpyxl import Workbook

from utils.api import fetch_wb_warehouse_stocks
from utils.cache import (
    load_products_cache,
    load_products_cache_for_user,
    load_stocks_cache,
    save_stocks_cache,
    save_stocks_cache_for_user,
)
from utils.constants import STOCKS_AUTO_REFRESH_INTERVAL_S, STOCKS_CACHE_STALE_S
from utils.helpers import enrich_stocks_from_products, normalize_stocks, stock_row_product_key
from utils.wb_token import effective_wb_api_token

logger = logging.getLogger(__name__)

stocks_bp = Blueprint("stocks", __name__)

_stocks_bg_lock = threading.Lock()
_stocks_bg_users: set[int] = set()


def fetch_stocks_resilient(token: str) -> List[Dict[str, Any]]:
    """Получает остатки на складах WB через Analytics API (с пагинацией)."""
    return fetch_wb_warehouse_stocks(token)


def _normalize_and_enrich_stocks(
    raw: List[Dict[str, Any]],
    user_id: int | None = None,
) -> List[Dict[str, Any]]:
    items = normalize_stocks(raw)
    try:
        if user_id is not None:
            products = (load_products_cache_for_user(user_id) or {}).get("items") or []
        else:
            products = (load_products_cache() or {}).get("items") or []
    except Exception:
        products = []
    return enrich_stocks_from_products(items, products)


def _stocks_cache_is_stale(cached: Dict[str, Any] | None) -> bool:
    if not cached or not cached.get("updated_at"):
        return True
    try:
        cache_time = datetime.strptime(str(cached.get("updated_at")), "%d.%m.%Y %H:%M:%S")
        return (datetime.now() - cache_time).total_seconds() >= float(STOCKS_CACHE_STALE_S)
    except Exception:
        return True


def _maybe_start_stocks_bg_refresh(user_id: int, token: str, cached: Dict[str, Any] | None) -> None:
    """Фоновое обновление остатков, если кэш устарел (не блокирует страницу)."""
    if not user_id or not token:
        return
    if not _stocks_cache_is_stale(cached):
        return
    with _stocks_bg_lock:
        if user_id in _stocks_bg_users:
            return
        _stocks_bg_users.add(user_id)

    def _worker() -> None:
        try:
            logger.info("Background stocks refresh started for user %s", user_id)
            raw = fetch_stocks_resilient(token)
            items = _normalize_and_enrich_stocks(raw, user_id=user_id)
            now_str = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
            save_stocks_cache_for_user(
                user_id,
                {"items": items, "updated_at": now_str, "_user_id": user_id},
            )
            logger.info("Background stocks refresh done for user %s: %s items", user_id, len(items))
        except Exception as exc:
            logger.warning("Background stocks refresh failed for user %s: %s", user_id, exc)
        finally:
            with _stocks_bg_lock:
                _stocks_bg_users.discard(user_id)

    threading.Thread(target=_worker, daemon=True).start()


@stocks_bp.route("/stocks", methods=["GET"])
@login_required
def stocks_page():
    """Страница остатков на складах."""
    token = effective_wb_api_token(current_user)
    error = None
    items: List[Dict[str, Any]] = []

    if not token:
        error = "Укажите токен API в профиле"
    else:
        try:
            cached = load_stocks_cache()
            if cached and cached.get("_user_id") == current_user.id:
                items = cached.get("items", [])
                # Показ из кэша; устаревший кэш обновляем в фоне
                _maybe_start_stocks_bg_refresh(current_user.id, token, cached)
            else:
                raw = fetch_stocks_resilient(token)
                items = _normalize_and_enrich_stocks(raw, user_id=current_user.id)
                save_stocks_cache(
                    {
                        "items": items,
                        "updated_at": datetime.now().strftime("%d.%m.%Y %H:%M:%S"),
                    }
                )
        except requests.HTTPError as http_err:
            status = http_err.response.status_code if http_err.response is not None else "?"
            error = f"Ошибка API: {status}"
        except Exception as exc:
            error = f"Ошибка: {exc}"

    # Общие итоги
    total_qty_all = sum(int(it.get("qty", 0) or 0) for it in items)
    total_in_transit_all = sum(int(it.get("in_transit", 0) or 0) for it in items)

    # Агрегация по товарам
    prod_map: Dict[tuple, Dict[str, Any]] = {}
    for it in items:
        key = stock_row_product_key(it)
        rec = prod_map.get(key)
        if not rec:
            rec = {
                "vendor_code": it.get("vendor_code") or "",
                "barcode": it.get("barcode") or "",
                "nm_id": it.get("nm_id"),
                "total_qty": 0,
                "total_in_transit": 0,
                "warehouses": [],
            }
            prod_map[key] = rec
        rec["total_qty"] += int(it.get("qty", 0) or 0)
        rec["total_in_transit"] += int(it.get("in_transit", 0) or 0)
        if not rec.get("vendor_code") and it.get("vendor_code"):
            rec["vendor_code"] = it.get("vendor_code")
        if not rec.get("barcode") and it.get("barcode"):
            rec["barcode"] = it.get("barcode")
        if rec.get("nm_id") is None and it.get("nm_id") is not None:
            rec["nm_id"] = it.get("nm_id")
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
        for rec in wh_map.values():
            for p in rec["products"]:
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

    cached = load_stocks_cache() or {}
    updated_at = cached.get("updated_at") if cached.get("_user_id") == current_user.id else None

    return render_template(
        "stocks.html",
        error=error,
        total_qty_all=total_qty_all,
        total_in_transit_all=total_in_transit_all,
        updated_at=updated_at,
        products_agg=products_agg,
        warehouses_agg=warehouses_agg,
        stocks_auto_refresh_ms=int(float(STOCKS_AUTO_REFRESH_INTERVAL_S or 1800) * 1000),
    )


@stocks_bp.route("/api/stocks/refresh", methods=["POST"])
@login_required
def api_stocks_refresh():
    """Обновляет кэш остатков по API и возвращает краткий статус."""
    token = effective_wb_api_token(current_user)
    if not token:
        return jsonify({"error": "no_token"}), 401

    try:
        logger.info("Starting stocks refresh for user %s", current_user.id)
        raw = fetch_stocks_resilient(token)

        if not isinstance(raw, list):
            logger.error("Expected list from fetch_stocks_resilient, got %s: %s", type(raw), raw)
            return jsonify({"error": "invalid_data", "detail": "API returned non-list data"}), 502

        logger.info("Got %s items from API, normalizing...", len(raw))
        items = _normalize_and_enrich_stocks(raw, user_id=current_user.id)
        logger.info("Normalized to %s items", len(items))

        now_str = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
        save_stocks_cache({"items": items, "updated_at": now_str, "_user_id": current_user.id})
        logger.info("Stocks cache saved successfully")

        return jsonify({"ok": True, "count": len(items), "updated_at": now_str})
    except requests.HTTPError as http_err:
        status_code = 500
        error_detail = ""
        response_obj = None

        if hasattr(http_err, "response") and http_err.response is not None:
            response_obj = http_err.response
            status_code = response_obj.status_code
            try:
                error_detail = response_obj.text[:500] if response_obj.text else ""
            except Exception:
                pass
        else:
            error_detail = str(http_err)
            import re

            match = re.search(r"status (\d+)", str(http_err), re.IGNORECASE)
            if match:
                try:
                    status_code = int(match.group(1))
                except (ValueError, TypeError):
                    pass

        logger.error("HTTP error %s while refreshing stocks: %s", status_code, error_detail)

        if status_code == 429:
            retry_after = None
            try:
                if response_obj is not None:
                    retry_after = response_obj.headers.get("Retry-After")
            except Exception:
                pass
            return jsonify({
                "error": "rate_limit",
                "status": 429,
                "retry_after": retry_after,
                "detail": error_detail,
            }), 429

        return jsonify({
            "error": "http",
            "status": status_code,
            "detail": error_detail,
        }), status_code if 400 <= status_code < 600 else 502
    except Exception as exc:
        import traceback

        logger.error("Unexpected error refreshing stocks: %s\n%s", exc, traceback.format_exc())
        return jsonify({"error": str(exc)}), 500


@stocks_bp.route("/api/stocks/update-time", methods=["GET"])
@login_required
def api_stocks_update_time():
    cached = load_stocks_cache() or {}
    if cached.get("_user_id") == current_user.id:
        return jsonify({"updated_at": cached.get("updated_at")})
    return jsonify({"updated_at": None})


@stocks_bp.route("/api/stocks/data", methods=["GET"])
@login_required
def api_stocks_data():
    cached = load_stocks_cache()
    if not cached or not (current_user.is_authenticated and cached.get("_user_id") == current_user.id):
        return jsonify({"products": [], "warehouses": [], "total_qty_all": 0, "updated_at": None})

    items = cached.get("items", [])
    total_qty_all = sum(int((it.get("qty") or 0)) for it in items)
    total_in_transit_all = sum(int((it.get("in_transit") or 0)) for it in items)

    from collections import defaultdict as _dd

    prod_map: Dict[tuple, Dict[str, Any]] = {}
    for it in items:
        key = stock_row_product_key(it)
        rec = prod_map.get(key)
        if not rec:
            rec = {
                "vendor_code": it.get("vendor_code") or "",
                "barcode": it.get("barcode") or "",
                "nm_id": it.get("nm_id"),
                "total_qty": 0,
                "total_in_transit": 0,
                "warehouses": [],
            }
            prod_map[key] = rec
        qty_i = int(it.get("qty", 0) or 0)
        rec["total_qty"] += qty_i
        rec["total_in_transit"] += int(it.get("in_transit", 0) or 0)
        if not rec.get("vendor_code") and it.get("vendor_code"):
            rec["vendor_code"] = it.get("vendor_code")
        if not rec.get("barcode") and it.get("barcode"):
            rec["barcode"] = it.get("barcode")
        if rec.get("nm_id") is None and it.get("nm_id") is not None:
            rec["nm_id"] = it.get("nm_id")
        rec["warehouses"].append({
            "warehouse": it.get("warehouse"),
            "qty": qty_i,
            "in_transit": int(it.get("in_transit", 0) or 0),
        })

    products_agg = []
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

    wh_map: Dict[str, Dict[str, Any]] = {}
    for it in items:
        w = it.get("warehouse") or ""
        rec = wh_map.get(w)
        if not rec:
            rec = {"warehouse": w, "total_qty": 0, "total_in_transit": 0, "products": []}
            wh_map[w] = rec
        qty_i = int(it.get("qty", 0) or 0)
        in_transit_i = int(it.get("in_transit", 0) or 0)
        rec["total_qty"] += qty_i
        rec["total_in_transit"] += in_transit_i
        rec["products"].append({
            "vendor_code": it.get("vendor_code"),
            "nm_id": it.get("nm_id"),
            "barcode": it.get("barcode"),
            "qty": qty_i,
            "in_transit": in_transit_i,
        })
    for rec in wh_map.values():
        rec["products"].sort(key=lambda x: (-x["qty"], x["vendor_code"] or ""))

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
        for rec in wh_map.values():
            for p in rec["products"]:
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

    return jsonify({
        "products": products_agg,
        "warehouses": warehouses_agg,
        "total_qty_all": total_qty_all,
        "total_in_transit_all": total_in_transit_all,
        "updated_at": cached.get("updated_at"),
    })


@stocks_bp.route("/stocks/export", methods=["POST"])
@login_required
def stocks_export():
    token = effective_wb_api_token(current_user)
    if not token:
        return redirect(url_for("stocks.stocks_page"))
    try:
        cached = load_stocks_cache()
        if cached and cached.get("_user_id") == current_user.id:
            items = cached.get("items", [])
        else:
            raw = fetch_stocks_resilient(token)
            items = _normalize_and_enrich_stocks(raw, user_id=current_user.id)
            save_stocks_cache({
                "items": items,
                "updated_at": datetime.now().strftime("%d.%m.%Y %H:%M:%S"),
            })
        wb = Workbook()
        ws = wb.active
        ws.title = "Остатки"
        ws.append(["Артикул", "Баркод", "NM ID", "Склад", "Остаток", "В пути"])
        for it in items:
            ws.append([
                it.get("vendor_code", ""),
                it.get("barcode", ""),
                it.get("nm_id", ""),
                it.get("warehouse", ""),
                it.get("qty", 0),
                it.get("in_transit", 0),
            ])
        buf = io.BytesIO()
        wb.save(buf)
        buf.seek(0)
        filename = f"stocks_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        return send_file(
            buf,
            as_attachment=True,
            download_name=filename,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    except Exception:
        return redirect(url_for("stocks.stocks_page"))

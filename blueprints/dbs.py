# -*- coding: utf-8 -*-
"""Blueprint для заказов DBS"""
import requests
from flask import Blueprint, render_template, request, jsonify
from flask_login import login_required, current_user
from datetime import datetime
from typing import List, Dict, Any
from utils.api import fetch_dbs_new_orders
from utils.cache import load_products_cache, add_dbs_known_orders, add_dbs_active_ids
from utils.fbs_dbs_processing import to_dbs_rows, _extract_created_at

dbs_bp = Blueprint('dbs', __name__)


@dbs_bp.route("/dbs", methods=["GET"]) 
@login_required
def dbs_page():
    """Страница заказов DBS"""
    error = None
    products_hint = None
    prod_cached_now = load_products_cache()
    if not prod_cached_now or not ((prod_cached_now or {}).get("items")):
        products_hint = "Для отображения фото товара и баркода обновите данные на странице Товары"
    return render_template("dbs.html", error=error, products_hint=products_hint)


@dbs_bp.route("/api/dbs/orders/new", methods=["GET"]) 
@login_required
def api_dbs_orders_new():
    """API для получения новых заказов DBS"""
    token = (current_user.wb_token or "") if current_user.is_authenticated else ""
    if not token:
        return jsonify({"items": [], "updated_at": None}), 200
    try:
        raw = fetch_dbs_new_orders(token)
        try:
            raw_sorted = sorted(raw, key=_extract_created_at)
        except Exception:
            raw_sorted = raw
        rows = to_dbs_rows(raw_sorted)
        # Cache known orders and IDs for tracking in-progress statuses later
        try:
            add_dbs_known_orders(raw_sorted)
            ids_to_add: list[int] = []
            for it in raw_sorted:
                oid = it.get("id") or it.get("orderId") or it.get("ID")
                try:
                    if oid is not None:
                        ids_to_add.append(int(oid))
                except Exception:
                    continue
            add_dbs_active_ids(ids_to_add)
        except Exception:
            pass
        prod_cached = load_products_cache() or {}
        items = (prod_cached.get("items") or [])
        by_nm: Dict[int, Dict[str, Any]] = {}
        for it in items:
            nmv = it.get("nm_id") or it.get("nmID")
            try:
                if nmv:
                    by_nm[int(nmv)] = it
            except Exception:
                pass
        for r in rows:
            nm = r.get("nm_id")
            try:
                nm_i = int(nm) if nm is not None else None
            except Exception:
                nm_i = None
            hit = by_nm.get(nm_i) if nm_i is not None else None
            if hit:
                r["photo"] = hit.get("photo")
                if hit.get("barcode"):
                    r["barcode"] = hit.get("barcode")
                elif isinstance(hit.get("barcodes"), list) and hit.get("barcodes"):
                    r["barcode"] = str(hit.get("barcodes")[0])
                else:
                    sizes = hit.get("sizes") or []
                    if isinstance(sizes, list):
                        for s in sizes:
                            bl = s.get("skus") or s.get("barcodes")
                            if isinstance(bl, list) and bl:
                                r["barcode"] = str(bl[0])
                                break
        now_str = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
        return jsonify({"items": rows, "updated_at": now_str}), 200
    except Exception as exc:
        return jsonify({"items": [], "error": str(exc)}), 200


@dbs_bp.route("/api/dbs/orders/<order_id>/deliver", methods=["PATCH"]) 
@login_required
def api_dbs_order_deliver(order_id: str):
    """API для доставки заказа DBS"""
    token = (current_user.wb_token or "") if current_user.is_authenticated else ""
    if not token:
        return jsonify({"error": "No token"}), 401
    headers_list = [
        {"Authorization": f"{token}"},
        {"Authorization": f"Bearer {token}"},
    ]
    url = f"https://marketplace-api.wildberries.ru/api/v3/dbs/orders/{order_id}/deliver"
    last_err = None
    for hdrs in headers_list:
        try:
            resp = requests.patch(url, headers=hdrs, timeout=30)
            if resp.status_code in [200, 204]:
                try:
                    add_dbs_active_ids([int(order_id)])
                except Exception:
                    pass
                return jsonify({"success": True}), 200
            else:
                last_err = f"HTTP {resp.status_code}: {resp.text}"
                continue
        except Exception as e:
            last_err = str(e)
            continue
    return jsonify({"error": last_err or "Unknown error"}), 500



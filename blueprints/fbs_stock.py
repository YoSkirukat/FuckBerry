# -*- coding: utf-8 -*-
"""Blueprint для остатков FBS"""
import requests
from flask import Blueprint, render_template, request, jsonify
from flask_login import login_required, current_user
from datetime import datetime
from typing import List, Dict, Any
from utils.constants import MOSCOW_TZ
from utils.api import fetch_fbs_warehouses, fetch_fbs_stocks_by_warehouse
from utils.cache import load_fbs_stock_cache, save_fbs_stock_cache, load_products_cache

fbs_stock_bp = Blueprint('fbs_stock', __name__)


@fbs_stock_bp.route("/fbs-stock", methods=["GET"]) 
@login_required
def fbs_stock_page():
    """Страница остатков FBS"""
    token = (current_user.wb_token or "") if current_user.is_authenticated else ""
    error = None
    warehouses: list[dict[str, Any]] = []
    updated_at = ""
    # Show cache only; refresh by button
    cached = load_fbs_stock_cache() or {}
    user_id = current_user.id if current_user.is_authenticated else None
    if cached and cached.get("_user_id") == user_id:
        warehouses = cached.get("warehouses", []) or []
        updated_at = cached.get("updated_at", "")
    else:
        # Если кэш не найден или для другого пользователя, пробуем загрузить склады без остатков
        if token:
            try:
                wlist = fetch_fbs_warehouses(token)
                warehouses = []
                for w in wlist:
                    if isinstance(w, dict):
                        wid = w.get("id") or w.get("warehouseId") or w.get("warehouseID")
                        if wid:
                            try:
                                warehouses.append({
                                    "id": int(wid),
                                    "name": w.get("name") or w.get("warehouseName") or "",
                                    "cargoType": "-",
                                    "deliveryType": "-",
                                    "total_amount": 0,
                                })
                            except (ValueError, TypeError):
                                pass
            except Exception as e:
                print(f"Ошибка при загрузке складов: {e}")
                warehouses = []
    return render_template("fbs_stock.html", error=error, warehouses=warehouses, updated_at=updated_at)


@fbs_stock_bp.route("/api/fbs-stock/refresh", methods=["POST"]) 
@login_required
def api_fbs_stock_refresh():
    """API для обновления остатков FBS"""
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
            # Проверяем, что w - это словарь
            if not isinstance(w, dict):
                continue
            wid = w.get("id") or w.get("warehouseId") or w.get("warehouseID")
            if not wid:
                continue
            try:
                wid_int = int(wid)
            except (ValueError, TypeError):
                continue
            wname = w.get("name") or w.get("warehouseName") or ""
            total_amount = 0
            try:
                stocks = fetch_fbs_stocks_by_warehouse(token, wid_int, skus)
                # Проверяем, что stocks - это список словарей
                if isinstance(stocks, list):
                    for s in stocks:
                        if isinstance(s, dict):
                            amount = s.get("amount") or s.get("qty") or 0
                            try:
                                total_amount += int(amount)
                            except (ValueError, TypeError):
                                pass
            except Exception as e:
                print(f"Ошибка при получении остатков для склада {wid_int}: {e}")
                import traceback
                traceback.print_exc()
                total_amount = 0
            warehouses.append({
                "id": wid_int,
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


@fbs_stock_bp.route("/api/fbs-stock/warehouse/<int:warehouse_id>", methods=["GET"]) 
@login_required
def api_fbs_stock_by_warehouse(warehouse_id: int):
    """API для получения остатков FBS по складу"""
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
        try:
            stocks = fetch_fbs_stocks_by_warehouse(token, int(warehouse_id), skus)
        except Exception as e:
            print(f"Ошибка при получении остатков для склада {warehouse_id}: {e}")
            import traceback
            traceback.print_exc()
            return jsonify({"error": f"Ошибка API: {str(e)}"}), 500
        
        rows: list[dict[str, Any]] = []
        if isinstance(stocks, list):
            for st in stocks:
                if not isinstance(st, dict):
                    continue
                sku = str(st.get("sku") or st.get("barcode") or "")
                if not sku:
                    continue
                amount = int(st.get("amount") or st.get("qty") or 0)
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



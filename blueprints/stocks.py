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
    import logging
    import time
    logger = logging.getLogger(__name__)
    
    headers1 = {"Authorization": f"Bearer {token}"}
    headers2 = {"Authorization": f"{token}"}
    
    # Пробуем сначала самый простой вариант (Bearer без параметров)
    # Это самый частый случай, который должен работать
    try:
        logger.info(f"Fetching stocks from {STOCKS_API_URL} with Bearer auth, no params")
        resp = requests.get(STOCKS_API_URL, headers=headers1, params={}, timeout=30)
        logger.info(f"Stocks API response status: {resp.status_code}")
        
        if resp.status_code == 200:
            # Успех - обрабатываем и возвращаем
            if not resp.text or not resp.text.strip():
                logger.warning("Empty response from Stocks API")
                raise requests.HTTPError("Empty response from API", response=resp)
            
            content_type = resp.headers.get('Content-Type', '').lower()
            if 'application/json' not in content_type and 'text/json' not in content_type:
                logger.warning(f"Unexpected Content-Type: {content_type}. Response preview: {resp.text[:200]}")
                raise requests.HTTPError(f"Invalid Content-Type: {content_type}. Response: {resp.text[:200]}", response=resp)
            
            try:
                data = resp.json()
                
                # API может вернуть словарь с данными внутри
                if isinstance(data, dict):
                    for key in ['data', 'stocks', 'items', 'result']:
                        if key in data and isinstance(data[key], list):
                            logger.info(f"Found list in key '{key}', extracted {len(data[key])} items")
                            data = data[key]
                            break
                    else:
                        for value in data.values():
                            if isinstance(value, list):
                                logger.info(f"Found list in dict values, extracted {len(value)} items")
                                data = value
                                break
                        else:
                            raise requests.HTTPError(f"API returned dict but no list found: {list(data.keys())}", response=resp)
                
                if not isinstance(data, list):
                    raise requests.HTTPError(f"API returned non-list data: {type(data)}", response=resp)
                
                logger.info(f"Successfully parsed stocks data, got {len(data)} items")
                return data
            except ValueError as json_err:
                raise requests.HTTPError(f"JSON parse error: {json_err}. Response: {resp.text[:500]}", response=resp)
        elif resp.status_code == 429:
            # Rate limit - сразу возвращаем ошибку, не пробуем другие варианты
            error_text = resp.text[:500] if resp.text else "No response body"
            logger.warning(f"Rate limit (429): {error_text}")
            http_err = requests.HTTPError(f"API returned status 429: {error_text}", response=resp)
            http_err.response = resp
            raise http_err
        elif resp.status_code in (401, 403):
            # Авторизация не подошла - пробуем без Bearer
            logger.info(f"Got {resp.status_code} with Bearer auth, trying without Bearer")
            # Продолжаем в блоке except ниже
            raise requests.HTTPError(f"API returned status {resp.status_code}", response=resp)
        else:
            # Другие ошибки (400, 500 и т.д.) - пробуем с параметрами
            error_text = resp.text[:500] if resp.text else "No response body"
            logger.warning(f"Stocks API returned status {resp.status_code}: {error_text}")
            # Пробуем с параметрами
            raise requests.HTTPError(f"API returned status {resp.status_code}: {error_text}", response=resp)
    except requests.HTTPError as first_err:
        # Если первый вариант не сработал, пробуем альтернативы
        if first_err.response and first_err.response.status_code == 429:
            # 429 - сразу пробрасываем, не пробуем другие варианты
            raise
        
        # Пробуем другие варианты только если это не 429
        params_list = [
            {"dateFrom": "1970-01-01T00:00:00", "flag": 0},  # С параметрами
        ]
        
        # Если была ошибка авторизации, пробуем без Bearer
        headers_to_try = [headers2] if (first_err.response and first_err.response.status_code in (401, 403)) else [headers1, headers2]
        
        last_error = first_err
        
        for headers in headers_to_try:
            for params in params_list:
                try:
                    auth_type = "Bearer" if headers == headers1 else "no Bearer"
                    logger.info(f"Fetching stocks from {STOCKS_API_URL} with {auth_type} auth, params: {params}")
                    
                    resp = requests.get(STOCKS_API_URL, headers=headers, params=params, timeout=30)
                    logger.info(f"Stocks API response status: {resp.status_code}")
                    
                    if resp.status_code == 200:
                        if not resp.text or not resp.text.strip():
                            logger.warning("Empty response from Stocks API")
                            last_error = requests.HTTPError("Empty response from API", response=resp)
                            continue
                        
                        content_type = resp.headers.get('Content-Type', '').lower()
                        if 'application/json' not in content_type and 'text/json' not in content_type:
                            logger.warning(f"Unexpected Content-Type: {content_type}. Response preview: {resp.text[:200]}")
                            last_error = requests.HTTPError(f"Invalid Content-Type: {content_type}. Response: {resp.text[:200]}", response=resp)
                            continue
                        
                        try:
                            data = resp.json()
                            
                            if isinstance(data, dict):
                                for key in ['data', 'stocks', 'items', 'result']:
                                    if key in data and isinstance(data[key], list):
                                        logger.info(f"Found list in key '{key}', extracted {len(data[key])} items")
                                        data = data[key]
                                        break
                                else:
                                    for value in data.values():
                                        if isinstance(value, list):
                                            logger.info(f"Found list in dict values, extracted {len(value)} items")
                                            data = value
                                            break
                                    else:
                                        logger.warning(f"API returned dict but no list found: {list(data.keys())}")
                                        last_error = requests.HTTPError(f"API returned dict but no list found: {list(data.keys())}", response=resp)
                                        continue
                            
                            if not isinstance(data, list):
                                logger.warning(f"API returned non-list data: {type(data)}")
                                last_error = requests.HTTPError(f"API returned non-list data: {type(data)}", response=resp)
                                continue
                            
                            logger.info(f"Successfully parsed stocks data, got {len(data)} items")
                            return data
                        except ValueError as json_err:
                            logger.warning(f"JSON parse error: {json_err}. Response preview: {resp.text[:500]}")
                            last_error = requests.HTTPError(f"JSON parse error: {json_err}. Response: {resp.text[:500]}", response=resp)
                            continue
                    elif resp.status_code == 429:
                        # 429 - сразу пробрасываем, не пробуем другие варианты
                        error_text = resp.text[:500] if resp.text else "No response body"
                        logger.warning(f"Rate limit (429) with {auth_type} auth, params {params}: {error_text}")
                        http_err = requests.HTTPError(f"API returned status 429: {error_text}", response=resp)
                        http_err.response = resp
                        raise http_err
                    elif resp.status_code in (401, 403):
                        logger.info(f"Got {resp.status_code} with {auth_type} auth, will try other auth method")
                        http_err = requests.HTTPError(f"API returned status {resp.status_code}", response=resp)
                        http_err.response = resp
                        last_error = http_err
                        break
                    else:
                        error_text = resp.text[:500] if resp.text else "No response body"
                        logger.warning(f"Stocks API returned status {resp.status_code} with {auth_type} auth, params {params}: {error_text}")
                        http_err = requests.HTTPError(f"API returned status {resp.status_code}: {error_text}", response=resp)
                        http_err.response = resp
                        last_error = http_err
                        continue
                        
                except requests.HTTPError as e:
                    # Если это 429, пробрасываем дальше
                    if e.response and e.response.status_code == 429:
                        raise
                    logger.warning(f"HTTP error with {auth_type} auth, params {params}: {e}")
                    last_error = e
                    continue
                except requests.RequestException as e:
                    logger.warning(f"Request exception with {auth_type} auth, params {params}: {e}")
                    last_error = e
                    continue
                except Exception as e:
                    logger.error(f"Unexpected error with {auth_type} auth, params {params}: {e}")
                    last_error = e
                    continue
    
    # Если все варианты не сработали, пробрасываем последнюю ошибку
    if last_error:
        if isinstance(last_error, requests.HTTPError):
            # Если это HTTPError с response, пробрасываем как есть
            if last_error.response is not None:
                raise last_error
            # Если response нет, создаем новый HTTPError с информацией об ошибке
            raise requests.HTTPError(f"Failed to fetch stocks: {last_error}", response=None)
        else:
            # Для других типов ошибок создаем HTTPError
            raise requests.HTTPError(f"Failed to fetch stocks: {last_error}", response=None)
    
    # Если вообще не было ошибок, но и данных нет
    raise RuntimeError("Failed to fetch stocks: all parameter combinations failed")


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
    import logging
    logger = logging.getLogger(__name__)
    
    token = current_user.wb_token or ""
    if not token:
        return jsonify({"error": "no_token"}), 401
    
    try:
        logger.info("Starting stocks refresh for user %s", current_user.id)
        raw = fetch_stocks_resilient(token)
        
        # Проверяем, что получили валидные данные
        if not isinstance(raw, list):
            logger.error(f"Expected list from fetch_stocks_resilient, got {type(raw)}: {raw}")
            return jsonify({"error": "invalid_data", "detail": "API returned non-list data"}), 502
        
        logger.info(f"Got {len(raw)} items from API, normalizing...")
        try:
            items = normalize_stocks(raw)
            logger.info(f"Normalized to {len(items)} items")
        except Exception as norm_err:
            import traceback
            error_trace = traceback.format_exc()
            logger.error(f"Error in normalize_stocks: {norm_err}\n{error_trace}")
            return jsonify({
                "error": "normalization_error",
                "detail": str(norm_err),
                "trace": error_trace
            }), 500
        
        now_str = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
        try:
            save_stocks_cache({"items": items, "updated_at": now_str, "_user_id": current_user.id})
            logger.info("Stocks cache saved successfully")
        except Exception as cache_err:
            import traceback
            error_trace = traceback.format_exc()
            logger.error(f"Error in save_stocks_cache: {cache_err}\n{error_trace}")
            # Кэш не сохранился, но данные нормализованы - возвращаем успех, но с предупреждением
            logger.warning("Failed to save cache, but data was normalized successfully")
            return jsonify({
                "ok": True,
                "count": len(items),
                "updated_at": now_str,
                "warning": "Cache save failed"
            })
        
        return jsonify({"ok": True, "count": len(items), "updated_at": now_str})
    except requests.HTTPError as http_err:
        # Получаем статус код из response, если он есть
        status_code = 500
        error_detail = ""
        response_obj = None
        
        if hasattr(http_err, 'response') and http_err.response is not None:
            response_obj = http_err.response
            status_code = response_obj.status_code
            try:
                error_detail = response_obj.text[:500] if response_obj.text else ""
            except Exception:
                pass
        else:
            # Если response нет, пытаемся извлечь информацию из сообщения об ошибке
            error_detail = str(http_err)
            # Пытаемся найти статус код в сообщении
            import re
            match = re.search(r'status (\d+)', str(http_err), re.IGNORECASE)
            if match:
                try:
                    status_code = int(match.group(1))
                except (ValueError, TypeError):
                    pass
        
        logger.error(f"HTTP error {status_code} while refreshing stocks: {error_detail}")
        
        # Для 429 возвращаем специальный статус с retry_after
        if status_code == 429:
            retry_after = 60
            if response_obj:
                retry_header = response_obj.headers.get('X-Ratelimit-Retry') or response_obj.headers.get('Retry-After')
                if retry_header:
                    try:
                        retry_after = int(float(retry_header))
                    except (ValueError, TypeError):
                        pass
            return jsonify({
                "error": "rate_limit",
                "status": 429,
                "retry_after": retry_after,
                "detail": error_detail
            }), 429
        
        # Для других HTTP ошибок возвращаем статус с деталями
        return jsonify({
            "error": "http",
            "status": status_code,
            "detail": error_detail
        }), status_code if 400 <= status_code < 600 else 502
    except Exception as exc:
        import traceback
        error_trace = traceback.format_exc()
        logger.error(f"Unexpected error while refreshing stocks: {exc}\n{error_trace}")
        return jsonify({"error": str(exc), "trace": error_trace}), 500


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



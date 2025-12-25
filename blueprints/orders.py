# -*- coding: utf-8 -*-
"""Blueprint для аналитики заказов"""
import threading
import requests
from typing import Any
from flask import Blueprint, render_template, request, jsonify
from flask_login import login_required, current_user
from models import db
from datetime import datetime
from utils.constants import MOSCOW_TZ, SUPPLIES_CACHE_AUTO
from utils.helpers import parse_date, format_dmy
from utils.api import fetch_orders_range
from utils.cache import (
    load_last_results, save_last_results,
    load_orders_cache_meta, save_orders_cache_meta,
    load_fbw_supplies_detailed_cache, save_fbw_supplies_detailed_cache,
    is_supplies_cache_fresh, is_orders_cache_fresh,
    build_orders_warm_cache,
    get_orders_with_period_cache, update_period_cache_with_data,
    _daterange_inclusive
)
from utils.orders_processing import (
    to_rows, aggregate_daily_counts_and_revenue,
    aggregate_by_warehouse_orders_only, aggregate_top_products,
    aggregate_top_products_orders, aggregate_top_products_sales
)
from utils.progress import ORDERS_PROGRESS, set_orders_progress, clear_orders_progress

orders_bp = Blueprint('orders', __name__)

# Глобальные переменные для отслеживания обновления кэша
_orders_cache_updating: dict[int, bool] = {}


@orders_bp.route("/orders", methods=["GET", "POST"]) 
@login_required
def index():
    """Главная страница аналитики заказов"""
    error = None
    # Orders
    orders = []
    total_orders = 0
    total_active_orders = 0
    total_cancelled_orders = 0
    total_revenue = 0.0

    # Chart series
    daily_labels: list[str] = []
    daily_orders_counts: list[int] = []
    daily_orders_cancelled_counts: list[int] = []
    daily_orders_revenue: list[float] = []

    # Warehouses combined
    warehouse_summary_dual: list[dict[str, Any]] = []

    # TOPs
    top_products: list[dict[str, Any]] = []
    top_products_orders_filtered: list[dict[str, Any]] = []
    warehouses: list[str] = []
    selected_warehouse: str = request.args.get("warehouse", "")

    updated_at: str = ""
    date_from = request.form.get("date_from", "")
    date_to = request.form.get("date_to", "")
    include_orders = True
    if request.method == "POST":
        force_refresh = request.form.get("force_refresh") is not None
    date_from_fmt = format_dmy(date_from)
    date_to_fmt = format_dmy(date_to)

    # Токен: берём из формы, иначе из профиля пользователя
    token = (request.form.get("token", "").strip() or (current_user.wb_token if current_user.is_authenticated else ""))

    # Если GET — пробуем показать последние результаты из кэша
    top_mode = "orders"
    cache_info = None
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
            total_active_orders = cached.get("total_active_orders", 0)
            total_cancelled_orders = cached.get("total_cancelled_orders", 0)
            total_revenue = cached.get("total_revenue", 0.0)
            top_products = cached.get("top_products", [])
            # charts
            daily_labels = cached.get("daily_labels", [])
            daily_orders_counts = cached.get("daily_orders_counts", [])
            daily_orders_cancelled_counts = cached.get("daily_orders_cancelled_counts", [])
            daily_orders_revenue = cached.get("daily_orders_revenue", [])
            warehouse_summary_dual = cached.get("warehouse_summary_dual", [])
            updated_at = cached.get("updated_at", "")
            # default mode when loading cache
            top_mode = cached.get("top_mode", "orders")
            # Add cache info for display
            cache_info = {"used_cache_days": 0, "fetched_days": 0}  # Will be calculated if needed
            
            # Fallback: если в кэше нет новых данных, рассчитываем их из orders
            if total_active_orders == 0 and total_cancelled_orders == 0 and orders:
                total_active_orders = len([o for o in orders if not o.get("is_cancelled", False)])
                total_cancelled_orders = len([o for o in orders if o.get("is_cancelled", False)])
            
            # Fallback: если в кэше нет данных об отмененных заказах для графиков, рассчитываем их
            if not daily_orders_cancelled_counts and orders:
                _, _, o_cancelled_counts_map = aggregate_daily_counts_and_revenue(orders)
                daily_orders_cancelled_counts = [o_cancelled_counts_map.get(d, 0) for d in daily_labels]

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
                if force_refresh:
                    # Принудительное обновление - загружаем все данные через API, игнорируя кэш
                    raw_orders = fetch_orders_range(token, date_from, date_to)
                    orders = to_rows(raw_orders, date_from, date_to)
                    total_orders = len(orders)
                    total_active_orders = len([o for o in orders if not o.get("is_cancelled", False)])
                    total_cancelled_orders = len([o for o in orders if o.get("is_cancelled", False)])
                    total_revenue = round(sum(float(o.get("Цена со скидкой продавца") or 0) for o in orders if not o.get("is_cancelled", False)), 2)
                    # Обновляем кэш принудительно
                    update_period_cache_with_data(token, date_from, date_to, orders)
                else:
                    # Обычное обновление - используем кэш по дням
                    orders, _meta = get_orders_with_period_cache(
                        token, date_from, date_to
                    )
                    total_orders = len(orders)
                    total_active_orders = len([o for o in orders if not o.get("is_cancelled", False)])
                    total_cancelled_orders = len([o for o in orders if o.get("is_cancelled", False)])
                    total_revenue = round(sum(float(o.get("Цена со скидкой продавца") or 0) for o in orders if not o.get("is_cancelled", False)), 2)
                    cache_info = _meta

                # Aggregates for charts
                o_counts_map, o_rev_map, o_cancelled_counts_map = aggregate_daily_counts_and_revenue(orders)
                daily_labels = sorted(o_counts_map.keys())
                daily_orders_counts = [o_counts_map.get(d, 0) for d in daily_labels]
                daily_orders_cancelled_counts = [o_cancelled_counts_map.get(d, 0) for d in daily_labels]
                daily_orders_revenue = [round(o_rev_map.get(d, 0.0), 2) for d in daily_labels]

                # Warehouses combined summary
                warehouse_summary_dual = aggregate_by_warehouse_orders_only(orders)

                # Top products (by orders)
                top_mode = "orders"
                top_products = aggregate_top_products(orders, limit=15)

                # Сохраняем токен в профиле пользователя при наличии
                if current_user.is_authenticated and token:
                    try:
                        # Проверяем, изменился ли токен
                        token_changed = current_user.wb_token != token
                        current_user.wb_token = token
                        db.session.commit()
                        
                        # Если токен изменился или кэш поставок устарел, строим новый кэш
                        # TODO: Перенести build_supplies_detailed_cache в utils/cache.py
                        # if SUPPLIES_CACHE_AUTO and (token_changed or not is_supplies_cache_fresh()):
                        #     print(f"Токен изменился или кэш устарел, запускаем построение кэша поставок (auto={SUPPLIES_CACHE_AUTO})...")
                        #     # Запускаем в фоне (не блокируем основной запрос)
                        #     def build_cache_background():
                        #         try:
                        #             # Если кэша нет — полная инициализация за 6 мес, иначе инкремент 10 дней
                        #             has_cache = bool(load_fbw_supplies_detailed_cache(current_user.id))
                        #             cache_data = build_supplies_detailed_cache(
                        #                 token,
                        #                 current_user.id,
                        #                 batch_size=10,           # меньше пакет
                        #                 pause_seconds=2.0,       # длиннее пауза
                        #                 force_full=not has_cache,
                        #                 days_back=(180 if not has_cache else 10),
                        #             )
                        #             save_fbw_supplies_detailed_cache(cache_data, current_user.id)
                        #             print(f"Кэш поставок успешно построен для пользователя {current_user.id}")
                        #         except Exception as e:
                        #             print(f"Ошибка построения кэша поставок: {e}")
                        #
                        #     thread = threading.Thread(target=build_cache_background)
                        #     thread.daemon = True
                        #     thread.start()

                        # Если токен изменился или кэш заказов устарел, подогреем кэш заказов
                        if token_changed or not is_orders_cache_fresh():
                            print("Запускаем подогрев кэша заказов (6 месяцев)...")
                            def warm_orders_cache_bg():
                                try:
                                    meta = build_orders_warm_cache(token)
                                    save_orders_cache_meta(meta)
                                    print("Кэш заказов подогрет")
                                except Exception as e:
                                    print(f"Ошибка подогрева кэша заказов: {e}")
                            t2 = threading.Thread(target=warm_orders_cache_bg)
                            t2.daemon = True
                            t2.start()
                            
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
                    "total_active_orders": total_active_orders,
                    "total_cancelled_orders": total_cancelled_orders,
                    "total_revenue": total_revenue,
                    "daily_labels": daily_labels,
                    "daily_orders_counts": daily_orders_counts,
                    "daily_orders_cancelled_counts": daily_orders_cancelled_counts,
                    "daily_orders_revenue": daily_orders_revenue,
                    "warehouse_summary_dual": warehouse_summary_dual,
                    "top_products": top_products,
                    "top_mode": top_mode,
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
        total_active_orders=total_active_orders,
        total_cancelled_orders=total_cancelled_orders,
        total_revenue=total_revenue,
        updated_at=updated_at,
        # Charts
        daily_labels=daily_labels,
        daily_orders_counts=daily_orders_counts,
        daily_orders_cancelled_counts=daily_orders_cancelled_counts,
        daily_orders_revenue=daily_orders_revenue,
        # Warehouses dual
        warehouse_summary_dual=warehouse_summary_dual,
        # TOPs
        top_products=top_products,
        warehouses=warehouses,
        selected_warehouse=selected_warehouse,
        top_products_orders_filtered=top_products_orders_filtered,
        include_orders=include_orders,
        top_mode=top_mode,
        # Cache info
        cache_info=cache_info,
    )


@orders_bp.route("/api/orders-refresh", methods=["POST"]) 
@login_required
def api_orders_refresh():
    """API для обновления данных заказов"""
    token = (current_user.wb_token or "") if current_user.is_authenticated else ""
    if not token:
        return jsonify({"error": "no_token"}), 401
    date_from = request.form.get("date_from", "")
    date_to = request.form.get("date_to", "")
    print(f"API orders-refresh: получены даты date_from='{date_from}', date_to='{date_to}'")
    try:
        df = parse_date(date_from)
        dt = parse_date(date_to)
        print(f"API orders-refresh: распарсенные даты df={df}, dt={dt}")
        if df > dt:
            date_from, date_to = date_to, date_from
            print(f"API orders-refresh: даты поменяны местами: date_from='{date_from}', date_to='{date_to}'")
    except ValueError as e:
        print(f"API orders-refresh: ошибка парсинга дат: {e}")
        return jsonify({"error": "bad_dates"}), 400
    try:
        # Orders
        force_refresh = request.form.get("force_refresh") is not None
        
        if force_refresh:
            # Принудительное обновление - загружаем все данные через API, игнорируя кэш
            raw_orders = fetch_orders_range(token, date_from, date_to)
            orders = to_rows(raw_orders, date_from, date_to)
            total_orders = len(orders)
            total_active_orders = len([o for o in orders if not o.get("is_cancelled", False)])
            total_cancelled_orders = len([o for o in orders if o.get("is_cancelled", False)])
            total_revenue = round(sum(float(o.get("Цена со скидкой продавца") or 0) for o in orders if not o.get("is_cancelled", False)), 2)
            # Обновляем кэш принудительно
            update_period_cache_with_data(token, date_from, date_to, orders)
            meta = {"used_cache_days": 0, "fetched_days": len(_daterange_inclusive(date_from, date_to))}
        else:
            # Обычное обновление - используем кэш по дням
            orders, meta = get_orders_with_period_cache(
                token, date_from, date_to
            )
            total_orders = len(orders)
            total_active_orders = len([o for o in orders if not o.get("is_cancelled", False)])
            total_cancelled_orders = len([o for o in orders if o.get("is_cancelled", False)])
            total_revenue = round(sum(float(o.get("Цена со скидкой продавца") or 0) for o in orders if not o.get("is_cancelled", False)), 2)
        # Aggregates
        o_counts_map, o_rev_map, o_cancelled_counts_map = aggregate_daily_counts_and_revenue(orders)
        daily_labels = sorted(o_counts_map.keys())
        daily_orders_counts = [o_counts_map.get(d, 0) for d in daily_labels]
        daily_orders_cancelled_counts = [o_cancelled_counts_map.get(d, 0) for d in daily_labels]
        daily_orders_revenue = [round(o_rev_map.get(d, 0.0), 2) for d in daily_labels]
        # Warehouses and TOPs
        warehouse_summary_dual = aggregate_by_warehouse_orders_only(orders)
        top_products = aggregate_top_products(orders, limit=15)
        top_mode = "orders"
        warehouses = sorted({(r.get("Склад отгрузки") or "Не указан") for r in orders})
        top_products_orders_filtered = aggregate_top_products_orders(orders, None, limit=50)
        updated_at = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
        # Save last results snapshot
        save_last_results({
            "date_from": date_from,
            "date_to": date_to,
            "orders": orders,
            "total_orders": total_orders,
            "total_active_orders": total_active_orders,
            "total_cancelled_orders": total_cancelled_orders,
            "total_revenue": total_revenue,
            "daily_labels": daily_labels,
            "daily_orders_counts": daily_orders_counts,
            "daily_orders_cancelled_counts": daily_orders_cancelled_counts,
            "daily_orders_revenue": daily_orders_revenue,
            "warehouse_summary_dual": warehouse_summary_dual,
            "top_products": top_products,
            "top_mode": top_mode,
            "updated_at": updated_at,
        })
        resp = {
            "total_orders": total_orders,
            "total_active_orders": total_active_orders,
            "total_cancelled_orders": total_cancelled_orders,
            "total_revenue": total_revenue,
            "daily_labels": daily_labels,
            "daily_orders_counts": daily_orders_counts,
            "daily_orders_cancelled_counts": daily_orders_cancelled_counts,
            "daily_orders_revenue": daily_orders_revenue,
            "warehouse_summary_dual": warehouse_summary_dual,
            "top_products": top_products,
            "warehouses": list(warehouses),
            "top_products_orders_filtered": top_products_orders_filtered,
            "updated_at": updated_at,
            "date_from_fmt": format_dmy(date_from),
            "date_to_fmt": format_dmy(date_to),
            "top_mode": top_mode,
        }
        try:
            resp["cache"] = {"used_cache_days": meta.get("used_cache_days", 0), "fetched_days": meta.get("fetched_days", 0)}
        except Exception:
            pass
        return jsonify(resp)
    except requests.HTTPError as http_err:
        status = 502
        try:
            if http_err.response is not None and http_err.response.status_code:
                status = http_err.response.status_code
        except Exception:
            status = 502
        # Graceful fallback for 429: return cached data if dates match
        if status == 429:
            cached = load_last_results() or {}
            if (
                cached.get("date_from") == date_from
                and cached.get("date_to") == date_to
                and cached.get("_user_id") == (current_user.id if current_user.is_authenticated else None)
            ):
                return jsonify({
                    "total_orders": cached.get("total_orders", 0),
                    "total_revenue": cached.get("total_revenue", 0),
                    "daily_labels": cached.get("daily_labels", []),
                    "daily_orders_counts": cached.get("daily_orders_counts", []),
                    "daily_sales_counts": cached.get("daily_sales_counts", []),
                    "daily_orders_revenue": cached.get("daily_orders_revenue", []),
                    "daily_sales_revenue": cached.get("daily_sales_revenue", []),
                    "warehouse_summary_dual": cached.get("warehouse_summary_dual", []),
                    "top_products": cached.get("top_products", []),
                    "warehouses": cached.get("warehouses", []),
                    "top_products_orders_filtered": cached.get("top_products_orders_filtered", []),
                    "updated_at": cached.get("updated_at", ""),
                    "date_from_fmt": format_dmy(date_from),
                    "date_to_fmt": format_dmy(date_to),
                    "top_mode": cached.get("top_mode", "orders"),
                    "rate_limited": True,
                }), 200
        return jsonify({"error": "http", "status": status}), status
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": str(exc)}), 500


@orders_bp.route("/api/orders-progress", methods=["GET"]) 
@login_required
def api_orders_progress():
    """API для получения прогресса загрузки заказов"""
    try:
        uid = current_user.id
        prog = ORDERS_PROGRESS.get(uid) or {"total": 0, "done": 0}
        return jsonify({"total": int(prog.get("total", 0)), "done": int(prog.get("done", 0))}), 200
    except Exception as exc:
        return jsonify({"total": 0, "done": 0, "error": str(exc)}), 200


@orders_bp.route("/api/orders/refresh-cache", methods=["POST"]) 
@login_required
def api_refresh_orders_cache():
    """API для обновления кэша заказов"""
    user_id = current_user.id
    
    # Проверяем, не идет ли уже обновление для этого пользователя
    if _orders_cache_updating.get(user_id, False):
        return jsonify({
            "error": "Кэш заказов уже обновляется. Пожалуйста, подождите завершения текущего процесса."
        }), 409
    
    token = (current_user.wb_token or "") if current_user.is_authenticated else ""
    if not token:
        return jsonify({"error": "no_token"}), 401
    
    try:
        # Устанавливаем флаг блокировки для этого пользователя
        _orders_cache_updating[user_id] = True
        
        # Запускаем обновление кэша в фоновом потоке
        def build_orders_cache_background():
            global _orders_cache_updating
            try:
                meta = build_orders_warm_cache(token, user_id)
                save_orders_cache_meta(meta, user_id)
                print(f"Кэш заказов успешно обновлен для пользователя {user_id}")
            except Exception as e:
                print(f"Ошибка при обновлении кэша заказов: {e}")
            finally:
                # Сбрасываем флаг блокировки для этого пользователя
                _orders_cache_updating[user_id] = False
        
        thread = threading.Thread(target=build_orders_cache_background)
        thread.daemon = True
        thread.start()
        
        return jsonify({
            "success": True,
            "message": "Обновление кэша заказов запущено в фоновом режиме. Это может занять несколько минут.",
            "total_orders": 0,  # Показываем 0, так как процесс еще идет
            "last_updated": datetime.now(MOSCOW_TZ).strftime("%d.%m.%Y %H:%M")
        })
    except Exception as exc:
        _orders_cache_updating[user_id] = False  # Сбрасываем флаг в случае ошибки
        return jsonify({"error": str(exc)}), 500


@orders_bp.route("/api/top-products-orders", methods=["GET"]) 
@login_required
def api_top_products_orders():
    """API для получения ТОП товаров по заказам"""
    warehouse = request.args.get("warehouse", "") or None
    cached = load_last_results()
    if not cached or not (current_user.is_authenticated and cached.get("_user_id") == current_user.id):
        return jsonify({"items": [], "total_qty": 0, "total_sum": 0})
    orders = cached.get("orders", [])
    items = aggregate_top_products_orders(orders, warehouse, limit=50)
    
    # Рассчитываем общие суммы для выбранного склада
    total_qty = 0
    total_sum = 0.0
    for item in items:
        total_qty += item.get("qty", 0)
        total_sum += item.get("sum", 0.0)
    
    return jsonify({
        "items": items,
        "total_qty": total_qty,
        "total_sum": round(total_sum, 2)
    })


@orders_bp.route("/api/top-products-sales", methods=["GET"]) 
@login_required
def api_top_products_sales():
    """API для получения ТОП товаров по продажам"""
    warehouse = request.args.get("warehouse", "") or None
    cached = load_last_results()
    if not cached or not (current_user.is_authenticated and cached.get("_user_id") == current_user.id):
        return jsonify({"items": []})
    sales_rows = cached.get("sales_rows", [])
    items = aggregate_top_products_sales(sales_rows, warehouse, limit=50)
    return jsonify({"items": items})


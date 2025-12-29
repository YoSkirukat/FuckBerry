# -*- coding: utf-8 -*-
"""Функции для обработки данных заказов"""
from collections import defaultdict
from typing import List, Dict, Any, Tuple
from utils.helpers import parse_date
from utils.cache import load_products_cache, load_stocks_cache


def to_rows(data: List[Dict[str, Any]], start_date: str, end_date: str) -> List[Dict[str, Any]]:
    """Преобразует данные заказов и фильтрует по реальной дате заказа (date), включая отмененные."""
    start = parse_date(start_date).date()
    end = parse_date(end_date).date()

    rows: List[Dict[str, Any]] = []
    for sale in data:
        # Фильтруем по реальной дате заказа (date), а не по lastChangeDate
        date_str = str(sale.get("date", ""))[:10]
        try:
            d = parse_date(date_str).date()
        except ValueError:
            continue
        if not (start <= d <= end):
            continue
        # Теперь включаем ВСЕ заказы, включая отмененные
        is_cancelled = sale.get("isCancel")
        is_cancelled_bool = is_cancelled is True or str(is_cancelled).lower() in ('true', '1', 'истина')
        
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
            "is_cancelled": is_cancelled_bool,  # Добавляем флаг отмены для удобства
        })
    return rows


def aggregate_daily_counts_and_revenue(rows: List[Dict[str, Any]]):
    """Агрегирует данные по дням: количество заказов, выручка, отмененные заказы"""
    count_by_day: Dict[str, int] = defaultdict(int)
    cancelled_count_by_day: Dict[str, int] = defaultdict(int)
    revenue_by_day: Dict[str, float] = defaultdict(float)
    for r in rows:
        day = r.get("Дата")
        is_cancelled = r.get("is_cancelled", False)
        
        # Подсчитываем общее количество заказов
        count_by_day[day] += 1
        
        # Подсчитываем отмененные заказы отдельно
        if is_cancelled:
            cancelled_count_by_day[day] += 1
        
        # Выручку считаем только с активных заказов
        if not is_cancelled:
            try:
                price = float(r.get("Цена со скидкой продавца") or 0)
            except (TypeError, ValueError):
                price = 0.0
            revenue_by_day[day] += price
    return count_by_day, revenue_by_day, cancelled_count_by_day


def aggregate_by_warehouse_orders_only(orders_rows: List[Dict[str, Any]]):
    """Агрегирует заказы по складам (только активные заказы)"""
    orders_map: Dict[str, int] = defaultdict(int)
    for r in orders_rows:
        # Пропускаем отмененные заказы в статистике по складам
        if r.get("is_cancelled", False):
            continue
        warehouse = r.get("Склад отгрузки") or "Не указан"
        orders_map[warehouse] += 1
    summary = []
    for w in sorted(orders_map.keys()):
        summary.append({"warehouse": w, "orders": orders_map.get(w, 0)})
    # сортируем по заказам
    summary.sort(key=lambda x: x["orders"], reverse=True)
    return summary


def aggregate_top_products(rows: List[Dict[str, Any]], limit: int = 15) -> List[Dict[str, Any]]:
    """Агрегирует ТОП товаров по количеству заказов"""
    counts: Dict[str, int] = defaultdict(int)
    revenue_by_product: Dict[str, float] = defaultdict(float)
    nm_by_product: Dict[str, Any] = {}
    barcode_by_product: Dict[str, Any] = {}
    supplier_article_by_product: Dict[str, Any] = {}
    for r in rows:
        # Пропускаем отмененные заказы в ТОП товаров
        if r.get("is_cancelled", False):
            continue
        product = r.get("Артикул продавца") or r.get("Артикул WB") or r.get("Баркод") or "Не указан"
        product = str(product)
        counts[product] += 1
        try:
            price = float(r.get("Цена со скидкой продавца") or 0)
        except (TypeError, ValueError):
            price = 0.0
        revenue_by_product[product] += price
        nm = r.get("Артикул WB") or r.get("nmId") or r.get("nmID")
        if product not in nm_by_product and nm:
            nm_by_product[product] = nm
        barcode = r.get("Баркод")
        if product not in barcode_by_product and barcode:
            barcode_by_product[product] = barcode
        supplier_article = r.get("Артикул продавца")
        if product not in supplier_article_by_product and supplier_article:
            supplier_article_by_product[product] = supplier_article
    # photo map
    nm_to_photo: Dict[Any, Any] = {}
    try:
        prod_cached = load_products_cache() or {}
        for it in (prod_cached.get("items") or []):
            nmv = it.get("nm_id") or it.get("nmId") or it.get("nmID")
            photo = it.get("photo") or it.get("img")
            if nmv is not None and nmv not in nm_to_photo:
                nm_to_photo[nmv] = photo
    except Exception:
        nm_to_photo = {}

    items = [{
        "product": p,
        "qty": c,
        "nm_id": nm_by_product.get(p),
        "barcode": barcode_by_product.get(p),
        "supplier_article": supplier_article_by_product.get(p),
        "sum": round(revenue_by_product.get(p, 0.0), 2),
        "photo": nm_to_photo.get(nm_by_product.get(p))
    } for p, c in counts.items()]
    items.sort(key=lambda x: x["qty"], reverse=True)
    return items[:limit]


def aggregate_top_products_orders(rows: List[Dict[str, Any]], warehouse: str | None = None, limit: int = 50) -> List[Dict[str, Any]]:
    """Агрегирует ТОП товаров по заказам с фильтрацией по складу"""
    counts: Dict[str, int] = defaultdict(int)
    revenue_by_product: Dict[str, float] = defaultdict(float)
    nm_by_product: Dict[str, Any] = {}
    barcode_by_product: Dict[str, Any] = {}
    supplier_article_by_product: Dict[str, Any] = {}
    for r in rows:
        # Пропускаем отмененные заказы в ТОП товаров
        if r.get("is_cancelled", False):
            continue
        if warehouse and (r.get("Склад отгрузки") or "Не указан") != warehouse:
            continue
        product = r.get("Артикул продавца") or r.get("Артикул WB") or r.get("Баркод") or "Не указан"
        product = str(product)
        counts[product] += 1
        try:
            price = float(r.get("Цена со скидкой продавца") or 0)
        except (TypeError, ValueError):
            price = 0.0
        revenue_by_product[product] += price
        nm = r.get("Артикул WB") or r.get("nmId") or r.get("nmID")
        if product not in nm_by_product and nm:
            nm_by_product[product] = nm
        barcode = r.get("Баркод")
        if product not in barcode_by_product and barcode:
            barcode_by_product[product] = barcode
        supplier_article = r.get("Артикул продавца")
        if product not in supplier_article_by_product and supplier_article:
            supplier_article_by_product[product] = supplier_article
    # Enrich with product photos from cache
    nm_to_photo: Dict[Any, Any] = {}
    try:
        prod_cached = load_products_cache() or {}
        for it in (prod_cached.get("items") or []):
            nmv = it.get("nm_id") or it.get("nmId") or it.get("nmID")
            photo = it.get("photo") or it.get("img")
            if nmv is not None and nmv not in nm_to_photo:
                nm_to_photo[nmv] = photo
    except Exception:
        nm_to_photo = {}

    # Load stocks data for current user
    # Build a map: barcode -> total qty (summed across all warehouses or filtered by warehouse)
    stocks_by_barcode: Dict[str, int] = {}
    try:
        from flask_login import current_user
        stocks_cached = load_stocks_cache()
        # Verify that cache belongs to current user
        # load_stocks_cache() already loads cache for current user via _stocks_cache_path_for_user()
        # but we double-check _user_id to be safe
        if stocks_cached and stocks_cached.get("_user_id"):
            # Check if current_user is available and matches
            try:
                user_id_match = current_user.is_authenticated and stocks_cached.get("_user_id") == current_user.id
            except Exception:
                # If current_user is not available, still use cache if it exists
                user_id_match = True
            if user_id_match:
                for stock_item in stocks_cached.get("items", []):
                    barcode = stock_item.get("barcode")
                    stock_warehouse = stock_item.get("warehouse", "")
                    qty = int(stock_item.get("qty", 0) or 0)
                    
                    if barcode:
                        if warehouse:
                            # Filter by warehouse - only add if warehouse matches
                            if stock_warehouse == warehouse or (warehouse and stock_warehouse and warehouse in stock_warehouse):
                                stocks_by_barcode[barcode] = stocks_by_barcode.get(barcode, 0) + qty
                        else:
                            # Sum across all warehouses
                            stocks_by_barcode[barcode] = stocks_by_barcode.get(barcode, 0) + qty
    except Exception as e:
        # Log error but continue without stocks
        import logging
        logging.getLogger(__name__).warning(f"Error loading stocks cache: {e}")
        stocks_by_barcode = {}

    items = []
    for p, c in counts.items():
        nm_id = nm_by_product.get(p)
        barcode = barcode_by_product.get(p)
        supplier_article = supplier_article_by_product.get(p)
        
        # Get stock quantity for this product by barcode
        stock_qty = stocks_by_barcode.get(barcode, 0) if barcode else 0
        
        items.append({
            "product": p,
            "qty": c,
            "nm_id": nm_id,
            "barcode": barcode,
            "supplier_article": supplier_article,
            "sum": round(revenue_by_product.get(p, 0.0), 2),
            "photo": nm_to_photo.get(nm_id),
            "stock_qty": stock_qty  # Changed from "stock" to "stock_qty" to match template
        })
    items.sort(key=lambda x: x["qty"], reverse=True)
    return items[:limit]


def aggregate_top_products_sales(rows: List[Dict[str, Any]], warehouse: str | None = None, limit: int = 50) -> List[Tuple[str, int]]:
    """Агрегирует ТОП товаров по продажам с фильтрацией по складу"""
    counts: Dict[str, int] = defaultdict(int)
    for r in rows:
        if warehouse and (r.get("Склад отгрузки") or "Не указан") != warehouse:
            continue
        product = r.get("Артикул продавца") or r.get("Артикул WB") or r.get("Баркод") or "Не указан"
        counts[str(product)] += 1
    return sorted(counts.items(), key=lambda kv: kv[1], reverse=True)[:limit]


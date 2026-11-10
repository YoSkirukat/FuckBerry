# -*- coding: utf-8 -*-
"""Blueprint для товаров"""
import re

import requests
from flask import Blueprint, render_template, jsonify, request, send_file, url_for
from flask_login import login_required, current_user
from utils.wb_token import effective_wb_api_token
from typing import List, Dict, Any
from utils.api import fetch_all_cards
from utils.cache import load_products_cache_for_user, save_products_cache
from utils.helpers import normalize_cards_response
from blueprints.fbs_supplies import build_product_label_pdf, load_seller_organization_name

products_bp = Blueprint('products', __name__)


def _find_product_item(nm_id: int, token: str, user_id: int) -> Dict[str, Any] | None:
    """Товар по nmId: сначала кэш страницы «Товары», при промахе — live-каталог WB."""
    nm_key = str(nm_id)
    cached = load_products_cache_for_user(user_id) or {}
    for item in cached.get("items") or []:
        if isinstance(item, dict) and str(item.get("nm_id") or "").strip() == nm_key:
            return item

    try:
        raw_cards = fetch_all_cards(token, page_limit=100)
    except Exception:
        return None
    for item in normalize_cards_response({"cards": raw_cards}):
        if str(item.get("nm_id") or "").strip() == nm_key:
            return item
    return None


def _barcode_pdf_error(message: str, status: int = 400):
    """Страница ошибки: скачивание идёт по ссылке, поэтому нужен понятный ответ."""
    return (
        render_template(
            "product_barcode_pdf_error.html",
            message=message,
            products_url=url_for("products.products_page"),
        ),
        status,
    )


@products_bp.route("/products", methods=["GET"]) 
@login_required
def products_page():
    """Страница товаров"""
    token = effective_wb_api_token(current_user)
    error = None
    products: List[Dict[str, Any]] = []
    if not token:
        error = "Укажите токен API в профиле"
    else:
        try:
            # Всегда грузим свежие данные с WB без использования локального кэша
            raw_cards = fetch_all_cards(token, page_limit=100)
            products = normalize_cards_response({"cards": raw_cards})
            # Обновим кэш в фоне для других страниц, но страница /products всегда показывает live-данные
            try:
                save_products_cache({"items": products})
            except Exception:
                pass
        except requests.HTTPError as http_err:
            error = f"Ошибка API: {http_err.response.status_code}"
        except Exception as exc:
            error = f"Ошибка: {exc}"
    return render_template("products.html", error=error, items=products, items_count=len(products))


@products_bp.route("/products/<int:nm_id>/barcode.pdf", methods=["GET"])
@login_required
def product_barcode_pdf(nm_id: int):
    """PDF этикетки 58×40 мм с баркодом товара; имя файла — сам баркод."""
    token = effective_wb_api_token(current_user)
    if not token:
        return _barcode_pdf_error("Укажите API токен Wildberries в профиле")

    item = _find_product_item(nm_id, token, current_user.id) or {}
    barcode = str(item.get("barcode") or request.args.get("barcode") or "").strip()
    if not barcode:
        return _barcode_pdf_error("У этого товара нет баркода Wildberries", status=404)

    label = {
        "barcode": barcode,
        "seller": load_seller_organization_name(current_user.id),
        "title": item.get("name") or "",
        "brand": item.get("brand") or "",
        "color": item.get("color") or "",
        "size": item.get("size") or "",
        "article": item.get("supplier_article") or "",
    }
    try:
        pdf_buffer = build_product_label_pdf(label)
    except Exception as exc:
        return _barcode_pdf_error(f"Ошибка создания PDF: {exc}", status=500)

    # Имя файла — сам баркод: так стикеры не перемешиваются при печати пачкой
    safe_barcode = re.sub(r"[^0-9A-Za-z._-]", "", barcode) or str(nm_id)
    return send_file(
        pdf_buffer,
        mimetype="application/pdf",
        as_attachment=True,
        download_name=f"{safe_barcode}.pdf",
    )


@products_bp.route("/api/products/refresh", methods=["POST"]) 
@login_required
def api_products_refresh():
    """API для обновления списка товаров"""
    token = effective_wb_api_token(current_user)
    if not token:
        return jsonify({"error": "no_token"}), 401
    try:
        raw_cards = fetch_all_cards(token, page_limit=100)
        items = normalize_cards_response({"cards": raw_cards})
        save_products_cache({"items": items, "_user_id": current_user.id})
        return jsonify({"ok": True, "count": len(items)})
    except requests.HTTPError as http_err:
        return jsonify({"error": "http", "status": http_err.response.status_code}), 502
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500



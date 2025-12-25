# -*- coding: utf-8 -*-
"""Blueprint для товаров"""
import requests
from flask import Blueprint, render_template, jsonify
from flask_login import login_required, current_user
from typing import List, Dict, Any
from utils.api import fetch_all_cards
from utils.cache import save_products_cache
from utils.helpers import normalize_cards_response

products_bp = Blueprint('products', __name__)


@products_bp.route("/products", methods=["GET"]) 
@login_required
def products_page():
    """Страница товаров"""
    token = current_user.wb_token or ""
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


@products_bp.route("/api/products/refresh", methods=["POST"]) 
@login_required
def api_products_refresh():
    """API для обновления списка товаров"""
    token = current_user.wb_token or ""
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



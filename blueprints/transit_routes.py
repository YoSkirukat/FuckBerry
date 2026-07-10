# -*- coding: utf-8 -*-
"""Blueprint для транзитных направлений FBW"""
import requests
from datetime import datetime
from typing import Any, Dict, List

from flask import Blueprint, jsonify, render_template
from flask_login import current_user, login_required

from utils.api import fetch_transit_tariffs
from utils.helpers import (
    build_transit_table_rows,
    extract_transit_filter_options,
    normalize_transit_tariff_items,
)
from utils.wb_token import effective_wb_api_token

transit_routes_bp = Blueprint("transit_routes", __name__)


@transit_routes_bp.route("/transit-routes", methods=["GET"])
@login_required
def transit_routes_page():
    """Страница транзитных направлений."""
    error = None
    token = effective_wb_api_token(current_user)
    raw_items: List[Dict[str, Any]] = []
    rows: List[Dict[str, Any]] = []
    transit_warehouses: List[str] = []
    destination_warehouses: List[str] = []
    generated_at = datetime.now().strftime("%d.%m.%Y %H:%M:%S")

    if not token:
        error = "Укажите токен API на странице Настройки"
    else:
        try:
            raw_items = fetch_transit_tariffs(token) or []
            rows = build_transit_table_rows(raw_items)
            transit_warehouses, destination_warehouses = extract_transit_filter_options(raw_items)
        except requests.HTTPError as http_err:
            error = f"Ошибка API: {http_err.response.status_code}"
        except Exception as exc:
            error = f"Ошибка: {exc}"

    return render_template(
        "transit_routes.html",
        error=error,
        rows=rows,
        raw_items=normalize_transit_tariff_items(raw_items),
        transit_warehouses=transit_warehouses,
        destination_warehouses=destination_warehouses,
        generated_at=generated_at,
    )


@transit_routes_bp.route("/api/transit-routes", methods=["GET"])
@login_required
def api_transit_routes():
    """API для обновления транзитных направлений."""
    token = effective_wb_api_token(current_user)
    if not token:
        return jsonify({"error": "no_token"}), 401
    try:
        raw_items = fetch_transit_tariffs(token) or []
        rows = build_transit_table_rows(raw_items)
        transit_warehouses, destination_warehouses = extract_transit_filter_options(raw_items)
        return jsonify({
            "rows": rows,
            "raw_items": normalize_transit_tariff_items(raw_items),
            "transit_warehouses": transit_warehouses,
            "destination_warehouses": destination_warehouses,
            "lastUpdated": datetime.now().strftime("%d.%m.%Y %H:%M:%S"),
        })
    except requests.HTTPError as http_err:
        return jsonify({"error": f"api_{http_err.response.status_code}"}), 500
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500

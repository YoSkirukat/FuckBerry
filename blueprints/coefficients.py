# -*- coding: utf-8 -*-
"""Blueprint для коэффициентов приёмки"""
import requests
from flask import Blueprint, render_template, request, jsonify
from flask_login import login_required, current_user
from datetime import datetime
from typing import List, Dict, Any
from utils.api import fetch_acceptance_coefficients
from utils.helpers import build_acceptance_grid

coefficients_bp = Blueprint('coefficients', __name__)


@coefficients_bp.route("/coefficients", methods=["GET", "POST"]) 
@login_required
def coefficients_page():
    """Страница коэффициентов приёмки"""
    error = None
    token = (current_user.wb_token or "") if current_user.is_authenticated else ""
    items: List[Dict[str, Any]] | None = None
    warehouses: List[str] = []
    date_keys: List[str] = []
    date_labels: List[str] = []
    grid: Dict[str, Dict[str, Dict[str, Any]]] = {}
    generated_at = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
    period_label = None

    if not token:
        error = "Укажите токен API на странице Настройки"
    else:
        try:
            items = fetch_acceptance_coefficients(token)
            if not isinstance(items, list):
                items = []
            warehouses, date_keys, date_labels, grid = build_acceptance_grid(items, days=14)
            if date_keys:
                try:
                    start = datetime.strptime(date_keys[0], "%Y-%m-%d").date()
                    end = datetime.strptime(date_keys[-1], "%Y-%m-%d").date()
                    period_label = f"{start.strftime('%d.%m')} по {end.strftime('%d.%m')}"
                except Exception:
                    period_label = None
        except requests.HTTPError as http_err:
            error = f"Ошибка API: {http_err.response.status_code}"
        except Exception as exc:
            error = f"Ошибка: {exc}"

    return render_template(
        "coefficients.html",
        error=error,
        warehouses=warehouses,
        date_labels=date_labels,
        date_keys=date_keys,
        grid=grid,
        generated_at=generated_at,
        period_label=period_label,
    )


@coefficients_bp.route("/api/acceptance-coefficients", methods=["GET"]) 
@login_required
def api_acceptance_coefficients():
    """API для получения коэффициентов приёмки"""
    token = (current_user.wb_token or "") if current_user.is_authenticated else ""
    if not token:
        return jsonify({"error": "no_token"}), 401
    try:
        items = fetch_acceptance_coefficients(token)
        if not isinstance(items, list):
            items = []
        warehouses, date_keys, date_labels, grid = build_acceptance_grid(items, days=14)
        return jsonify({
            "warehouses": warehouses,
            "date_keys": date_keys,
            "date_labels": date_labels,
            "grid": grid
        })
    except requests.HTTPError as http_err:
        return jsonify({"error": f"api_{http_err.response.status_code}"}), 500
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500



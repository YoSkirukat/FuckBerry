# -*- coding: utf-8 -*-
"""Blueprint для отчётов"""
from flask import Blueprint, render_template, request
from flask_login import login_required, current_user
from datetime import datetime
from utils.cache import load_last_results
from utils.api import fetch_finance_report

reports_bp = Blueprint('reports', __name__)


@reports_bp.route("/report/sales", methods=["GET"]) 
@login_required
def report_sales_page():
    """Страница отчёта по продажам"""
    cached = load_last_results()
    token = (current_user.wb_token or "") if current_user.is_authenticated else ""
    
    # Страница по умолчанию открывается пустой: без данных, пока пользователь не задаст период и не нажмёт Загрузить
    if not request.args.get("date_from") and not request.args.get("date_to"):
        return render_template(
            "report_sales.html",
            error=None,
            items=[],
            date_from_fmt="",
            date_to_fmt="",
            warehouse=None,
            warehouses=[],
            date_from_val="",
            date_to_val="",
        ), 200
    # Если есть параметры, данные загружаются через JavaScript
    return render_template("report_sales.html", error=None, items=[], date_from_fmt="", date_to_fmt="", warehouse=None, warehouses=[], date_from_val="", date_to_val="")


@reports_bp.route("/report/orders", methods=["GET"]) 
@login_required
def report_orders_page():
    """Страница отчёта по заказам"""
    cached = load_last_results()
    
    # Извлекаем параметры дат из URL
    date_from_val = (request.args.get("date_from") or "").strip()
    date_to_val = (request.args.get("date_to") or "").strip()
    warehouse = request.args.get("warehouse") or None
    
    # Форматируем даты для отображения
    date_from_fmt = ""
    date_to_fmt = ""
    if date_from_val:
        try:
            date_from_fmt = datetime.strptime(date_from_val, "%Y-%m-%d").strftime("%d.%m.%Y")
        except Exception:
            date_from_fmt = date_from_val
    if date_to_val:
        try:
            date_to_fmt = datetime.strptime(date_to_val, "%Y-%m-%d").strftime("%d.%m.%Y")
        except Exception:
            date_to_fmt = date_to_val
    
    # Получаем список складов из кэша заказов
    warehouses = []
    if cached and current_user.is_authenticated and cached.get("_user_id") == current_user.id:
        orders = cached.get("orders", [])
        warehouses = sorted({(r.get("Склад отгрузки") or "Не указан") for r in orders})
    
    # Страница открывается пустой, данные загружаются через JavaScript
    # Но передаем параметры дат, чтобы JavaScript мог их использовать для автозагрузки
    return render_template(
        "report_orders.html",
        error=None,
        items=[],
        date_from_fmt=date_from_fmt,
        date_to_fmt=date_to_fmt,
        date_from_val=date_from_val,
        date_to_val=date_to_val,
        warehouse=warehouse,
        warehouses=warehouses,
    )


@reports_bp.route("/report/finance", methods=["GET"]) 
@login_required
def report_finance_page():
    """Страница финансового отчёта"""
    # initial render without data - всегда показываем пустую страницу
    if not request.args.get("date_from") and not request.args.get("date_to"):
        return render_template(
            "finance_report.html",
            error=None,
            rows=[],
            date_from_fmt="",
            date_to_fmt="",
            date_from_val="",
            date_to_val="",
            finance_metrics={},
        ), 200
    token = (current_user.wb_token or "") if current_user.is_authenticated else ""
    if not token:
        return render_template(
            "finance_report.html",
            error="Требуется API токен (Статистика)",
            items=[],
            date_from_fmt="",
            date_to_fmt="",
            date_from_val=(request.args.get("date_from") or ""),
            date_to_val=(request.args.get("date_to") or ""),
        ), 200
    req_from = (request.args.get("date_from") or "").strip()
    req_to = (request.args.get("date_to") or "").strip()
    try:
        raw = fetch_finance_report(token, req_from, req_to)
        date_from_fmt = datetime.strptime(req_from, "%Y-%m-%d").strftime("%d.%m.%Y")
        date_to_fmt = datetime.strptime(req_to, "%Y-%m-%d").strftime("%d.%m.%Y")
    except Exception as exc:
        return render_template(
            "finance_report.html",
            error=str(exc),
            rows=[],
            date_from_fmt="",
            date_to_fmt="",
            date_from_val=req_from,
            date_to_val=req_to,
        ), 200
    return render_template(
        "finance_report.html",
        error=None,
        rows=raw,
        date_from_fmt=date_from_fmt,
        date_to_fmt=date_to_fmt,
        date_from_val=req_from,
        date_to_val=req_to,
    )


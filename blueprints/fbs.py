# -*- coding: utf-8 -*-
"""Blueprint для заказов FBS"""
import io
import requests
from flask import Blueprint, render_template, request, send_file
from flask_login import login_required, current_user
from datetime import datetime
from typing import List, Dict, Any
from utils.api import fetch_fbs_new_orders
from utils.cache import load_fbs_tasks_cache, load_products_cache
from utils.fbs_dbs_processing import to_fbs_rows, _extract_created_at
from utils.wb_token import effective_wb_api_token

fbs_bp = Blueprint('fbs', __name__)


@fbs_bp.route("/fbs", methods=["GET", "POST"]) 
@login_required
def fbs_page():
    """Страница заказов FBS"""
    error = None
    token = effective_wb_api_token(current_user)
    rows: List[Dict[str, Any]] = []
    # Load cached tasks to show immediately
    cached_tasks = load_fbs_tasks_cache() or {}
    cached_rows = cached_tasks.get("rows") or []
    rows = cached_rows

    if request.method == "POST":
        pass  # Раньше была ручная проверка; теперь обновляем через JS-кнопку в блоке

    # If enrichment impossible due to empty products cache
    products_hint = None
    prod_cached_now = load_products_cache()
    if not prod_cached_now or not ((prod_cached_now or {}).get("items")):
        products_hint = "Для отображения фото товара и баркода обновите данные на странице Товары"

    # Не блокируем рендер страницы: текущие задания подтянем AJAX-ом
    return render_template("fbs.html", error=error, rows=rows, products_hint=products_hint, current_orders=[])


@fbs_bp.route("/fbs/export", methods=["POST"]) 
@login_required
def fbs_export():
    """Экспорт заказов FBS в Excel"""
    token = effective_wb_api_token(current_user)
    if not token:
        return ("Требуется API токен", 400)
    try:
        # 1) Используем кэш заданий (как на странице /fbs), чтобы экспорт соответствовал UI.
        cached = load_fbs_tasks_cache() or {}
        rows = (cached.get("rows") or []) if isinstance(cached, dict) else []

        # 2) Fallback: если кэш пуст — забираем свежие /new и сохраняем.
        if not rows:
            raw = fetch_fbs_new_orders(token)
            raw_sorted = sorted(raw, key=_extract_created_at)
            rows = to_fbs_rows(raw_sorted)
            try:
                now_str = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
                from utils.cache import save_fbs_tasks_cache  # локальный импорт, чтобы избежать циклов
                save_fbs_tasks_cache({"rows": rows, "updated_at": now_str})
            except Exception:
                pass

        # 3) Агрегация дублей: nm_id -> barcode -> name
        agg: Dict[tuple[str, str, str], int] = {}
        for r in rows:
            name = (r.get("Наименование товара") or "").strip()
            nm_id = str(r.get("nm_id") or "").strip()
            barcode = str(r.get("barcode") or "").strip()
            key = (name, nm_id, barcode)
            agg[key] = agg.get(key, 0) + 1

        # Build XLS (not XLSX)
        try:
            import xlwt  # type: ignore
        except Exception:
            return ("На сервере отсутствует зависимость xlwt (для .xls)", 500)
        wb = xlwt.Workbook()
        ws = wb.add_sheet("FBS")
        header_style = xlwt.easyxf("font: bold on; align: horiz center")
        num_style = xlwt.easyxf("align: horiz right")
        ws.write(0, 0, "Наименование", header_style)
        ws.write(0, 1, "Артикул WB (nmId)", header_style)
        ws.write(0, 2, "Баркод", header_style)
        ws.write(0, 3, "Количество", header_style)
        row_idx = 1
        for (name, nm_id, barcode), qty in sorted(agg.items(), key=lambda x: (-x[1], x[0][0][0])):
            ws.write(row_idx, 0, name)
            ws.write(row_idx, 1, nm_id)
            ws.write(row_idx, 2, barcode)
            ws.write(row_idx, 3, qty, num_style)
            row_idx += 1
        out = io.BytesIO()
        wb.save(out)
        out.seek(0)
        filename = f"fbs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xls"
        return send_file(out, mimetype="application/vnd.ms-excel", as_attachment=True, download_name=filename)
    except requests.HTTPError as http_err:
        return (f"Ошибка API: {http_err.response.status_code}", 502)
    except Exception as exc:
        return (f"Ошибка: {exc}", 500)



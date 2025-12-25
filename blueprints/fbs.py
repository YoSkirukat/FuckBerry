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

fbs_bp = Blueprint('fbs', __name__)


@fbs_bp.route("/fbs", methods=["GET", "POST"]) 
@login_required
def fbs_page():
    """Страница заказов FBS"""
    error = None
    token = (current_user.wb_token or "") if current_user.is_authenticated else ""
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
    token = (current_user.wb_token or "") if current_user.is_authenticated else ""
    if not token:
        return ("Требуется API токен", 400)
    try:
        raw = fetch_fbs_new_orders(token)
        raw_sorted = sorted(raw, key=_extract_created_at)
        rows = to_fbs_rows(raw_sorted)
        # Enrich from products cache
        prod_cached = load_products_cache()
        items = (prod_cached or {}).get("items") or []
        by_article: Dict[str, Dict[str, Any]] = {}
        by_nm: Dict[int, Dict[str, Any]] = {}
        for it in items:
            art = (it.get("supplier_article") or it.get("vendorCode") or "").strip()
            if art:
                by_article.setdefault(art, it)
            nmv = it.get("nm_id") or it.get("nmID")
            try:
                if nmv:
                    by_nm[int(nmv)] = it
            except Exception:
                pass
        for r in rows:
            art = (r.get("Наименование товара") or "").strip()
            hit = by_article.get(art)
            if not hit and r.get("nm_id"):
                try:
                    hit = by_nm.get(int(r["nm_id"]))
                except Exception:
                    hit = None
            if hit:
                if hit.get("barcode"):
                    r["barcode"] = hit.get("barcode")
                elif isinstance(hit.get("barcodes"), list) and hit.get("barcodes"):
                    r["barcode"] = str(hit.get("barcodes")[0])
                else:
                    sizes = hit.get("sizes") or []
                    if isinstance(sizes, list):
                        for s in sizes:
                            bar_list = s.get("skus") or s.get("barcodes")
                            if isinstance(bar_list, list) and bar_list:
                                r["barcode"] = str(bar_list[0])
                                break

        # Aggregate: Наименование + Баркод -> Количество
        agg: Dict[tuple[str, str], int] = {}
        for r in rows:
            name = (r.get("Наименование товара") or "").strip()
            barcode = (r.get("barcode") or "").strip()
            key = (name, barcode)
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
        ws.write(0, 1, "Баркод", header_style)
        ws.write(0, 2, "Количество", header_style)
        row_idx = 1
        for (name, barcode), qty in sorted(agg.items(), key=lambda x: (-x[1], x[0][0])):
            ws.write(row_idx, 0, name)
            ws.write(row_idx, 1, barcode)
            ws.write(row_idx, 2, qty, num_style)
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



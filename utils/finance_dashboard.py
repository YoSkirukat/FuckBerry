# -*- coding: utf-8 -*-
"""Расчёт сводки финансового отчёта по логике листа DASHBOARD из «Расшифровка WB»."""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Tuple


BUYOUT_OPERS = {
    "продажа",
    "сторно возвратов",
    "корректная продажа",
    "коррекция продаж",
}
RETURN_OPERS = {
    "возврат",
    "сторно продаж",
    "корректный возврат",
}

DEFECT_OPERS = {
    "компенсация брака",
    "оплата брака",
    "частичная компенсация брака",
    "добровольная компенсация при возврате",
}
DAMAGE_OPERS = {
    "оплата потерянного товара",
    "компенсация потерянного товара",
    "авансовая оплата за товар без движения",
    "компенсация подмененного товара",
    "компенсация подмен",
    "компенсация ущерба",
    "компенсация подмена",
}


def _norm(v: Any) -> str:
    return str(v or "").strip().lower()


def _f(v: Any) -> float:
    try:
        return float(v or 0.0)
    except Exception:
        return 0.0


def _i(v: Any) -> int:
    try:
        return int(v or 0)
    except Exception:
        try:
            return int(float(v or 0))
        except Exception:
            return 0


def _is_sale_doc(doc: str) -> bool:
    d = _norm(doc)
    return d == "продажа" or "продаж" in d


def _is_return_doc(doc: str) -> bool:
    d = _norm(doc)
    return d == "возврат" or "возврат" in d


def _retail_with_disc(row: Dict[str, Any]) -> float:
    """Колонка T Excel: цена розничная с учётом согласованной скидки."""
    if row.get("retail_price_withdisc_rub") is not None:
        return _f(row.get("retail_price_withdisc_rub"))
    return _f(row.get("retail_price"))


def _detail_key(row: Dict[str, Any], *, include_oper: bool = True) -> Tuple[str, ...]:
    parts = [
        str(row.get("nm_id") or ""),
        str(row.get("sa_name") or "").strip(),
        str(row.get("barcode") or "").strip(),
        str(row.get("brand_name") or "").strip(),
        str(row.get("subject_name") or "").strip(),
        str(row.get("ts_name") or "").strip(),
    ]
    if include_oper:
        parts.append(str(row.get("supplier_oper_name") or "").strip())
        parts.append(str(row.get("doc_type_name") or "").strip())
        parts.append(str(row.get("bonus_type_name") or "").strip())
    return tuple(parts)


def _add_detail(
    bucket: Dict[Tuple[str, ...], Dict[str, Any]],
    row: Dict[str, Any],
    amount: float,
    *,
    include_oper: bool = True,
) -> None:
    if abs(amount) < 1e-9:
        return
    key = _detail_key(row, include_oper=include_oper)
    item = bucket.get(key)
    if item is None:
        bucket[key] = {
            "nm_id": row.get("nm_id") or "",
            "sa_name": str(row.get("sa_name") or "").strip(),
            "barcode": str(row.get("barcode") or "").strip(),
            "brand_name": str(row.get("brand_name") or "").strip(),
            "subject_name": str(row.get("subject_name") or "").strip(),
            "ts_name": str(row.get("ts_name") or "").strip(),
            "oper": str(row.get("supplier_oper_name") or "").strip() if include_oper else "",
            "doc_type": str(row.get("doc_type_name") or "").strip() if include_oper else "",
            "bonus_type": str(row.get("bonus_type_name") or "").strip() if include_oper else "",
            "qty": _i(row.get("quantity")),
            "amount": round(amount, 2),
        }
    else:
        item["qty"] += _i(row.get("quantity"))
        item["amount"] = round(_f(item["amount"]) + amount, 2)


def _finalize_details(bucket: Dict[Tuple[str, ...], Dict[str, Any]]) -> List[Dict[str, Any]]:
    items = [v for v in bucket.values() if abs(_f(v.get("amount"))) >= 0.01]
    items.sort(key=lambda x: abs(_f(x.get("amount"))), reverse=True)
    return items


def _negate_detail_amounts(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for item in items:
        row = dict(item)
        row["amount"] = round(-_f(item.get("amount")), 2)
        out.append(row)
    return out


def _product_title(row: Dict[str, Any]) -> str:
    brand = str(row.get("brand_name") or "").strip()
    article = str(row.get("sa_name") or "").strip()
    subject = str(row.get("subject_name") or "").strip()
    parts: List[str] = []
    if brand:
        parts.append(brand)
    if article:
        parts.append(f"({article})" if not article.startswith("(") else article)
    if subject:
        parts.append(subject)
    return " ".join(parts).strip() or "—"


def _product_group_key(row: Dict[str, Any]) -> str | None:
    """
    Ключ группировки товара:
    — баркод, если есть;
    — иначе nm_id + артикул продавца (продажи/операции без баркода в отчёте WB).
    """
    barcode = str(row.get("barcode") or "").strip()
    if barcode:
        return f"bc:{barcode}"
    nm = row.get("nm_id")
    sa = str(row.get("sa_name") or "").strip()
    if nm is None and not sa:
        return None
    return f"nm:{nm if nm is not None else ''}|sa:{sa}"


def _row_in_products_breakdown(row: Dict[str, Any], *, is_return_row: bool) -> bool:
    if is_return_row:
        return False
    return _product_group_key(row) is not None


def _build_products_breakdown(raw: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Сводка по товарам без возвратов:
    — с баркодом (как раньше);
    — без баркода, но с nm_id / артикулом продавца (продажи и др. операции).
    """
    bucket: Dict[str, Dict[str, Any]] = {}
    for r in raw:
        oper = _norm(r.get("supplier_oper_name"))
        doc = r.get("doc_type_name")
        is_return_row = oper in RETURN_OPERS or _is_return_doc(doc)
        key = _product_group_key(r)
        if key is None or is_return_row:
            continue

        barcode = str(r.get("barcode") or "").strip()
        pay = _f(r.get("ppvz_for_pay"))
        sales_qty = _i(r.get("quantity")) if oper in BUYOUT_OPERS else 0

        item = bucket.get(key)
        if item is None:
            bucket[key] = {
                "barcode": barcode,
                "name": _product_title(r),
                "nm_id": r.get("nm_id") or "",
                "sa_name": str(r.get("sa_name") or "").strip(),
                "sales_qty": sales_qty,
                "for_pay": round(pay, 2),
            }
        else:
            item["for_pay"] = round(_f(item["for_pay"]) + pay, 2)
            item["sales_qty"] = _i(item.get("sales_qty")) + sales_qty
            if not item.get("barcode") and barcode:
                item["barcode"] = barcode
            if item["name"] in ("", "—"):
                item["name"] = _product_title(r)
            if not item.get("nm_id") and r.get("nm_id"):
                item["nm_id"] = r.get("nm_id")
            if not item.get("sa_name") and r.get("sa_name"):
                item["sa_name"] = str(r.get("sa_name") or "").strip()

    items = list(bucket.values())
    items.sort(key=lambda x: (-abs(_f(x.get("for_pay"))), str(x.get("barcode") or ""), str(x.get("sa_name") or "")))
    return items


def _enrich_products_from_catalog(
    products: List[Dict[str, Any]],
    catalog: List[Dict[str, Any]] | None,
) -> List[Dict[str, Any]]:
    """Подставляет баркод/название из кэша товаров (/products) по баркоду, nm_id или артикулу."""
    if not products or not catalog:
        return products

    by_barcode: Dict[str, Dict[str, Any]] = {}
    by_nm: Dict[int, Dict[str, Any]] = {}
    by_sa: Dict[str, Dict[str, Any]] = {}
    for p in catalog:
        if not isinstance(p, dict):
            continue
        bc = str(p.get("barcode") or "").strip()
        if bc and bc not in by_barcode:
            by_barcode[bc] = p
        nmv = p.get("nm_id") or p.get("nmId") or p.get("nmID")
        if nmv is not None:
            try:
                by_nm[int(nmv)] = p
            except Exception:
                pass
        sa = str(p.get("supplier_article") or p.get("vendor_code") or p.get("sa_name") or "").strip().lower()
        if sa:
            by_sa[sa] = p

    def _needs_name(item: Dict[str, Any]) -> bool:
        name = str(item.get("name") or "").strip()
        if not name or name in ("—", "-"):
            return True
        if name.startswith("nmID "):
            return True
        # Для строк только из хранения — подтягиваем нормальное имя из каталога
        if item.get("storage_only"):
            bc = str(item.get("barcode") or "").strip()
            sa = str(item.get("sa_name") or "").strip()
            if name == bc or name == sa:
                return True
        return False

    for item in products:
        needs_name = _needs_name(item)
        needs_barcode = not str(item.get("barcode") or "").strip()
        needs_sa = not str(item.get("sa_name") or "").strip()
        # storage_only всегда пробуем обогатить имя из каталога
        force_catalog_name = bool(item.get("storage_only"))
        if not needs_name and not needs_barcode and not needs_sa and not force_catalog_name:
            continue

        meta = None
        bc = str(item.get("barcode") or "").strip()
        if bc and bc in by_barcode:
            meta = by_barcode[bc]
        if not meta:
            nm = item.get("nm_id")
            if nm is not None and nm != "":
                try:
                    meta = by_nm.get(int(nm))
                except Exception:
                    meta = by_nm.get(nm)  # type: ignore[arg-type]
        if not meta:
            sa = str(item.get("sa_name") or "").strip().lower()
            if sa:
                meta = by_sa.get(sa)
        if not meta:
            continue

        if needs_barcode:
            cat_bc = str(meta.get("barcode") or "").strip()
            if cat_bc:
                item["barcode"] = cat_bc
        if needs_sa:
            sa_cat = str(meta.get("supplier_article") or meta.get("vendorCode") or "").strip()
            if sa_cat:
                item["sa_name"] = sa_cat

        cat_name = str(meta.get("name") or meta.get("title") or "").strip()
        if cat_name and (needs_name or force_catalog_name):
            item["name"] = cat_name
        elif _needs_name(item):
            sa_cat = str(item.get("sa_name") or meta.get("supplier_article") or "").strip()
            if sa_cat:
                item["name"] = _product_title({
                    "brand_name": "",
                    "sa_name": sa_cat,
                    "subject_name": "",
                })
    return products


def _index_products_for_costs(products: List[Dict[str, Any]]) -> tuple[
    Dict[str, Dict[str, Any]],
    Dict[int, Dict[str, Any]],
    Dict[str, Dict[str, Any]],
]:
    by_barcode: Dict[str, Dict[str, Any]] = {}
    by_nm: Dict[int, Dict[str, Any]] = {}
    by_sa: Dict[str, Dict[str, Any]] = {}
    for p in products:
        bc = str(p.get("barcode") or "").strip()
        if bc and bc not in by_barcode:
            by_barcode[bc] = p
        nm = p.get("nm_id")
        if nm is not None and nm != "":
            try:
                nmi = int(nm)
                if nmi not in by_nm:
                    by_nm[nmi] = p
            except Exception:
                pass
        sa = str(p.get("sa_name") or "").strip().lower()
        if sa and sa not in by_sa:
            by_sa[sa] = p
    return by_barcode, by_nm, by_sa


def _match_product_for_cost(
    row: Dict[str, Any],
    by_barcode: Dict[str, Dict[str, Any]],
    by_nm: Dict[int, Dict[str, Any]],
    by_sa: Dict[str, Dict[str, Any]],
) -> Dict[str, Any] | None:
    bc = str(row.get("barcode") or "").strip()
    if bc and bc in by_barcode:
        return by_barcode[bc]
    nm = row.get("nm_id")
    if nm is not None and nm != "":
        try:
            hit = by_nm.get(int(nm))
            if hit:
                return hit
        except Exception:
            pass
    sa = str(row.get("sa_name") or "").strip().lower()
    if sa and sa in by_sa:
        return by_sa[sa]
    return None


def _paid_storage_row_as_cost_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Нормализует строку paid_storage к полям, понятным матчеру товаров."""
    return {
        "barcode": str(row.get("barcode") or "").strip(),
        "nm_id": row.get("nmId") if row.get("nmId") is not None else row.get("nm_id"),
        "sa_name": str(row.get("vendorCode") or row.get("vendor_code") or row.get("sa_name") or "").strip(),
        "brand_name": str(row.get("brand") or row.get("brand_name") or "").strip(),
        "subject_name": str(row.get("subject") or row.get("subject_name") or "").strip(),
        "ts_name": str(row.get("size") or row.get("ts_name") or "").strip(),
        "quantity": row.get("barcodesCount") if row.get("barcodesCount") is not None else row.get("quantity"),
        "warehousePrice": row.get("warehousePrice") if row.get("warehousePrice") is not None else row.get("warehouse_price"),
    }


def _build_paid_storage_by_product(paid_storage: List[Dict[str, Any]] | None) -> List[Dict[str, Any]]:
    """Агрегация платного хранения по товару (баркод / nmId+артикул)."""
    if not paid_storage:
        return []
    bucket: Dict[str, Dict[str, Any]] = {}
    for raw in paid_storage:
        if not isinstance(raw, dict):
            continue
        r = _paid_storage_row_as_cost_row(raw)
        amount = _f(r.get("warehousePrice"))
        if abs(amount) < 1e-9:
            continue
        barcode = str(r.get("barcode") or "").strip()
        nm = r.get("nm_id")
        sa = str(r.get("sa_name") or "").strip()
        if barcode:
            key = f"bc:{barcode}"
        elif nm is not None or sa:
            key = f"nm:{nm if nm is not None else ''}|sa:{sa}"
        else:
            key = f"misc:{r.get('brand_name')}|{r.get('subject_name')}"
        item = bucket.get(key)
        if item is None:
            bucket[key] = {
                "nm_id": nm if nm is not None else "",
                "sa_name": sa,
                "barcode": barcode,
                "brand_name": str(r.get("brand_name") or "").strip(),
                "subject_name": str(r.get("subject_name") or "").strip(),
                "ts_name": str(r.get("ts_name") or "").strip(),
                "oper": "Платное хранение",
                "doc_type": "",
                "bonus_type": "",
                "qty": _i(r.get("quantity")),
                "amount": round(amount, 2),
            }
        else:
            item["qty"] = _i(item.get("qty")) + _i(r.get("quantity"))
            item["amount"] = round(_f(item["amount"]) + amount, 2)
            if not item.get("barcode") and barcode:
                item["barcode"] = barcode
            if not item.get("nm_id") and nm is not None:
                item["nm_id"] = nm
            if not item.get("sa_name") and sa:
                item["sa_name"] = sa
    return _finalize_details(bucket)


def _apply_product_expense_columns(
    products: List[Dict[str, Any]],
    raw: List[Dict[str, Any]],
    *,
    acceptance_total: float,
    paid_storage: List[Dict[str, Any]] | None = None,
) -> List[Dict[str, Any]]:
    """
    Колонки затрат по товарам:
    — Логистика: сумма delivery_rub по фактическим строкам товара;
    — Платная приёмка: сумма acceptance по фактическим строкам товара;
    — Хранение: сумма warehousePrice из отчёта «Платное хранение» по товару.
      Товары только из хранения (без строк финотчёта) добавляются отдельными строками.
    """
    if not products and not paid_storage:
        return products

    for p in products:
        p["logistics"] = 0.0
        p["acceptance"] = 0.0
        p["storage"] = 0.0
        p["promotion"] = 0.0

    by_barcode, by_nm, by_sa = _index_products_for_costs(products)
    for r in raw:
        if not isinstance(r, dict):
            continue
        prod = _match_product_for_cost(r, by_barcode, by_nm, by_sa)
        if not prod:
            continue
        delivery_rub = _f(r.get("delivery_rub"))
        acceptance_val = _f(r.get("acceptance"))
        if abs(delivery_rub) >= 1e-9:
            prod["logistics"] = round(_f(prod["logistics"]) + delivery_rub, 2)
        if abs(acceptance_val) >= 1e-9:
            prod["acceptance"] = round(_f(prod["acceptance"]) + acceptance_val, 2)

    # В сверке приёмка показывается как abs(total)
    if acceptance_total < 0:
        for p in products:
            if abs(_f(p.get("acceptance"))) >= 1e-9:
                p["acceptance"] = round(-_f(p["acceptance"]), 2)

    unmatched_storage: Dict[str, Dict[str, Any]] = {}
    if paid_storage:
        for raw_row in paid_storage:
            if not isinstance(raw_row, dict):
                continue
            r = _paid_storage_row_as_cost_row(raw_row)
            amount = _f(r.get("warehousePrice"))
            if abs(amount) < 1e-9:
                continue
            prod = _match_product_for_cost(r, by_barcode, by_nm, by_sa)
            if prod:
                prod["storage"] = round(_f(prod["storage"]) + amount, 2)
                continue

            barcode = str(r.get("barcode") or "").strip()
            nm = r.get("nm_id")
            sa = str(r.get("sa_name") or "").strip()
            if barcode:
                key = f"bc:{barcode}"
            elif nm is not None or sa:
                key = f"nm:{nm if nm is not None else ''}|sa:{sa}"
            else:
                key = f"misc:{r.get('brand_name')}|{r.get('subject_name')}"

            item = unmatched_storage.get(key)
            if item is None:
                name = _product_title(r)
                if name in ("", "—"):
                    if sa:
                        name = sa
                    elif nm is not None and str(nm) != "":
                        name = f"nmID {nm}"
                    elif barcode:
                        name = barcode
                unmatched_storage[key] = {
                    "barcode": barcode,
                    "name": name,
                    "nm_id": nm if nm is not None else "",
                    "sa_name": sa,
                    "sales_qty": 0,
                    "for_pay": 0.0,
                    "logistics": 0.0,
                    "acceptance": 0.0,
                    "storage": round(amount, 2),
                    "promotion": 0.0,
                    "storage_only": True,
                    "for_pay_with_services": None,
                    "price_with_services": None,
                }
            else:
                item["storage"] = round(_f(item["storage"]) + amount, 2)
                if not item.get("barcode") and barcode:
                    item["barcode"] = barcode
                if (not item.get("nm_id") or item.get("nm_id") == "") and nm is not None:
                    item["nm_id"] = nm
                if not item.get("sa_name") and sa:
                    item["sa_name"] = sa
                if item.get("name") in ("", "—", None):
                    name = _product_title(r)
                    if name in ("", "—"):
                        name = sa or (f"nmID {nm}" if nm is not None and str(nm) != "" else barcode) or "—"
                    item["name"] = name

    if unmatched_storage:
        extra = list(unmatched_storage.values())
        extra.sort(key=lambda x: (-abs(_f(x.get("storage"))), str(x.get("barcode") or ""), str(x.get("sa_name") or "")))
        products.extend(extra)

    return products


def _is_promotion_deduction_row(row: Dict[str, Any]) -> bool:
    """Строка «прочих удержаний» за WB Продвижение."""
    text = " ".join(
        str(row.get(k) or "")
        for k in ("supplier_oper_name", "bonus_type_name", "doc_type_name", "oper", "bonus_type", "doc_type")
    ).lower()
    return "продвижен" in text


def _promotion_total_from_details(other_details: List[Dict[str, Any]]) -> float:
    total = 0.0
    for item in other_details:
        if _is_promotion_deduction_row(item):
            total += _f(item.get("amount"))
    return round(total, 2)


def _apply_promotion_allocation(
    products: List[Dict[str, Any]],
    *,
    promotion_total: float,
    promotion_spend: List[Dict[str, Any]] | None,
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]], float]:
    """
    Раскидывает сумму удержаний «WB Продвижение» только по рекламировавшимся nmId
    пропорционально затратам из /adv/v3/fullstats.
    Возвращает (products, breakdown_rows, advert_api_sum).
    """
    for p in products:
        p["promotion"] = 0.0

    spend_rows = [r for r in (promotion_spend or []) if isinstance(r, dict) and _f(r.get("sum")) > 0]
    advert_api_sum = round(sum(_f(r.get("sum")) for r in spend_rows), 2)
    target = round(_f(promotion_total), 2)
    if not spend_rows or abs(target) < 1e-9:
        return products, [], advert_api_sum

    by_nm: Dict[int, List[Dict[str, Any]]] = {}
    for p in products:
        nm = p.get("nm_id")
        if nm is None or nm == "":
            continue
        try:
            nmi = int(nm)
        except Exception:
            continue
        by_nm.setdefault(nmi, []).append(p)

    weight_total = round(sum(_f(r.get("sum")) for r in spend_rows), 2)
    if abs(weight_total) < 1e-9:
        return products, [], advert_api_sum

    allocated = 0.0
    last_idx = len(spend_rows) - 1
    breakdown: List[Dict[str, Any]] = []

    for i, row in enumerate(spend_rows):
        try:
            nmi = int(row.get("nm_id"))
        except Exception:
            continue
        weight = _f(row.get("sum"))
        if i == last_idx:
            amt = round(target - allocated, 2)
        else:
            amt = round(target * weight / weight_total, 2)
            allocated = round(allocated + amt, 2)

        matched = by_nm.get(nmi) or []
        if matched:
            if len(matched) == 1:
                matched[0]["promotion"] = round(_f(matched[0].get("promotion")) + amt, 2)
                barcode = str(matched[0].get("barcode") or "").strip()
                sa_name = str(matched[0].get("sa_name") or "").strip()
                name = str(matched[0].get("name") or "").strip()
            else:
                pay_base = sum(_f(p.get("for_pay")) for p in matched)
                sub_alloc = 0.0
                for j, p in enumerate(matched):
                    if j == len(matched) - 1:
                        part = round(amt - sub_alloc, 2)
                    elif abs(pay_base) >= 1e-9:
                        part = round(amt * _f(p.get("for_pay")) / pay_base, 2)
                        sub_alloc = round(sub_alloc + part, 2)
                    else:
                        part = round(amt / len(matched), 2)
                        sub_alloc = round(sub_alloc + part, 2)
                    p["promotion"] = round(_f(p.get("promotion")) + part, 2)
                barcode = str(matched[0].get("barcode") or "").strip()
                sa_name = str(matched[0].get("sa_name") or row.get("vendor_code") or "").strip()
                name = str(matched[0].get("name") or row.get("name") or "").strip()
        else:
            barcode = str(row.get("barcode") or "").strip()
            sa_name = str(row.get("vendor_code") or "").strip()
            name = str(row.get("name") or "").strip()

        breakdown.append({
            "nm_id": nmi,
            "sa_name": sa_name,
            "barcode": barcode,
            "brand_name": "",
            "subject_name": name,
            "ts_name": "",
            "oper": "WB Продвижение",
            "doc_type": "",
            "bonus_type": "",
            "qty": 0,
            "amount": amt,
            "advert_sum": round(weight, 2),
        })

    breakdown.sort(key=lambda x: abs(_f(x.get("amount"))), reverse=True)
    return products, breakdown, advert_api_sum


def _apply_services_allocation(
    products: List[Dict[str, Any]],
    payment_to_account: float,
    products_total: float | None = None,
) -> List[Dict[str, Any]]:
    """
    Сумма с услугами (только товары с кол-вом продаж > 0):

      base = Сумма − Логистика − Хранение − Платная приёмка − Продвижение

    Затем на проданные товары пропорционально их Сумме (выкупу) раскидываем
    только:
      • разницу в разнесённых = (Сумма по товарам − Оплата на РС) − Σ разнесённых
        (логистика+хранение+приёмка+продвижение по всем строкам);
      • затраты тех же 4 колонок у товаров без продаж (qty = 0),
        т.е. −Σ base по непроданным.

    Итого к вычету из base проданных:
      to_distribute = −Σ base_непроданных + разница_в_разнесённых

    Σ «Сумма с услугами» по проданным = Оплата на РС.
    Цена с услугами = Сумма с услугами / Кол-во.
    """
    if not products:
        return products

    target = round(_f(payment_to_account), 2)
    sold: List[Dict[str, Any]] = []
    unsold: List[Dict[str, Any]] = []
    for p in products:
        if _i(p.get("sales_qty")) > 0:
            sold.append(p)
        else:
            unsold.append(p)
            p["for_pay_with_services"] = None
            p["price_with_services"] = None

    if not sold:
        return products

    def _costs(p: Dict[str, Any]) -> float:
        return (
            _f(p.get("logistics"))
            + _f(p.get("storage"))
            + _f(p.get("acceptance"))
            + _f(p.get("promotion"))
        )

    def _base(p: Dict[str, Any]) -> float:
        return round(_f(p.get("for_pay")) - _costs(p), 2)

    bases: List[float] = []
    weights: List[float] = []
    for p in sold:
        pay = _f(p.get("for_pay"))
        bases.append(_base(p))
        weights.append(pay if pay > 0 else 0.0)

    # Затраты / base непроданных (обычно for_pay=0 → base = −затраты)
    unsold_services_sum = round(sum(_base(p) for p in unsold), 2)

    allocated_total = round(sum(_costs(p) for p in products), 2)
    pt = round(_f(products_total), 2) if products_total is not None else round(
        sum(_f(p.get("for_pay")) for p in products), 2
    )
    # Как в подвале сверки: разница = Итого Сумма (gap) − Итого Разнесено
    recon_gap = round(pt - target, 2)
    allocation_residual = round(recon_gap - allocated_total, 2)

    # Сколько вычесть суммарно с проданных (пропорционально Сумме)
    to_distribute = round((-unsold_services_sum) + allocation_residual, 2)
    # Эквивалентно: target - sum(bases), при for_pay непроданных ≈ 0
    gap = round(-to_distribute, 2)

    weight_total = round(sum(weights), 2)
    allocated_adj = 0.0
    last_idx = len(sold) - 1
    for i, p in enumerate(sold):
        if i == last_idx:
            adj = round(gap - allocated_adj, 2)
        elif weight_total > 1e-9:
            adj = round(gap * (weights[i] / weight_total), 2)
            allocated_adj = round(allocated_adj + adj, 2)
        else:
            n = len(sold)
            adj = round(gap / n, 2)
            allocated_adj = round(allocated_adj + adj, 2)
        amt = round(bases[i] + adj, 2)
        p["for_pay_with_services"] = amt
        qty = _i(p.get("sales_qty"))
        p["price_with_services"] = round(amt / qty, 2) if qty else None

    return products


def _is_ksale_oper(oper: str, doc: Any) -> bool:
    if oper in ("продажа", "сторно возвратов", "корректная продажа"):
        return True
    if oper == "коррекция продаж" and _is_sale_doc(doc):
        return True
    return False


def _is_kreturn_oper(oper: str, doc: Any) -> bool:
    if oper in ("возврат", "сторно продаж", "корректный возврат"):
        return True
    if oper == "коррекция продаж" and _is_return_doc(doc):
        return True
    return False


def _build_payment_vs_products_reconciliation(
    *,
    products_total: float,
    payment_to_account: float,
    k_sale: float,
    k_return: float,
    logistics: float,
    storage: float,
    other_deductions: float,
    acceptance: float,
    penalties: float,
    additional_payment: float,
    defect: float,
    damage: float,
    paid_delivery: float,
    e3_acquiring_corr: float,
    promotion_total: float = 0.0,
    allocated: Dict[str, float] | None = None,
) -> Dict[str, Any]:
    """
    Тождество:
    Сумма по товарам − Оплата на РС =
      Логистика + Хранение + Продвижение + Прочие + Приёмка + Штрафы + Доплаты
      + К перечислению по возвратам
      − Компенсация брака − Компенсация ущерба − Платная доставка − Корр. эквайринга
      + (Сумма по товарам − К перечислению по продажам)
    """
    gap = round(products_total - payment_to_account, 2)
    products_minus_ksale = round(products_total - k_sale, 2)
    alloc = allocated or {}
    promo = round(_f(promotion_total), 2)
    other_rest = round(_f(other_deductions) - promo, 2)

    def _line(key: str, label: str, amount: float, alloc_key: str | None = None) -> Dict[str, Any]:
        amt = round(_f(amount), 2)
        row: Dict[str, Any] = {"key": key, "label": label, "amount": amt}
        if alloc_key and alloc_key in alloc:
            allocated_amt = round(_f(alloc.get(alloc_key)), 2)
            row["allocated"] = allocated_amt
            row["diff"] = round(amt - allocated_amt, 2)
        return row

    lines: List[Dict[str, Any]] = [
        _line("logistics", "Логистика", logistics, "logistics"),
        _line("storage", "Хранение", storage, "storage"),
    ]
    if abs(promo) >= 0.01:
        lines.append(_line("promotion", "Продвижение", promo, "promotion"))
    lines.extend([
        _line("other", "Прочие удержания", other_rest),
        _line("acceptance", "Платная приёмка", abs(acceptance), "acceptance"),
        _line("penalties", "Штрафы", penalties),
        _line("additional", "Доплаты", additional_payment),
        {
            "key": "returns_for_pay",
            "label": "К перечислению по возвратам (в оплате на РС вычитается)",
            "amount": round(k_return, 2),
        },
        {"key": "defect", "label": "Компенсация брака (уже учтена в оплате на РС)", "amount": round(-defect, 2)},
        {"key": "damage", "label": "Компенсация ущерба (уже учтена в оплате на РС)", "amount": round(-damage, 2)},
        {
            "key": "paid_delivery",
            "label": "Платная доставка (уже учтена в оплате на РС)",
            "amount": round(-paid_delivery, 2),
        },
        {
            "key": "e3",
            "label": "Корректировка эквайринга (добавляется к оплате на РС)",
            "amount": round(-e3_acquiring_corr, 2),
        },
        {
            "key": "products_vs_ksale",
            "label": "Сумма по товарам − к перечислению по продажам (компенсации и др. в товарах / продажи без идентификации)",
            "amount": products_minus_ksale,
        },
    ])
    explained = round(sum(_f(x["amount"]) for x in lines), 2)
    return {
        "products_total": round(products_total, 2),
        "payment_to_account": round(payment_to_account, 2),
        "gap": gap,
        "k_sale": round(k_sale, 2),
        "k_return": round(k_return, 2),
        "lines": lines,
        "explained": explained,
        "residual": round(gap - explained, 2),
    }


def compute_finance_dashboard(
    raw: List[Dict[str, Any]],
    date_from: str,
    date_to: str,
    products_catalog: List[Dict[str, Any]] | None = None,
    user_id: int | None = None,
    paid_storage: List[Dict[str, Any]] | None = None,
    promotion_spend: List[Dict[str, Any]] | None = None,
) -> Dict[str, Any]:
    """
    Считает метрики как на DASHBOARD:
    выручка / выкупы / возвраты / WB реализовал / удержания / компенсации / оплата на РС.
    """
    if products_catalog is None and user_id is not None:
        try:
            from utils.cache import load_products_cache_for_user
            products_catalog = (load_products_cache_for_user(user_id) or {}).get("items") or []
        except Exception:
            products_catalog = []
    buyouts_rub = 0.0
    returns_rub = 0.0
    buyouts_qty = 0
    returns_qty = 0
    wb_plus = 0.0
    wb_minus = 0.0
    delivery_count = 0.0

    logistics = 0.0
    storage = 0.0
    acceptance = 0.0
    other_deductions = 0.0
    penalties = 0.0
    additional_payment = 0.0
    acquiring = 0.0
    paid_delivery = 0.0
    e3_acquiring_corr = 0.0

    # AH (ppvz_for_pay) для комиссии
    k_sale = 0.0  # продажа + сторно возвратов + корректная продажа + коррекция(продажа)
    k_return = 0.0  # возврат + сторно продаж + корректный возврат + коррекция(возврат)

    # Компенсация брака (G18)
    defect = 0.0
    # Компенсация ущерба (G20)
    damage = 0.0

    defect_details: Dict[Tuple[str, ...], Dict[str, Any]] = {}
    damage_details: Dict[Tuple[str, ...], Dict[str, Any]] = {}
    penalty_details: Dict[Tuple[str, ...], Dict[str, Any]] = {}
    additional_details: Dict[Tuple[str, ...], Dict[str, Any]] = {}
    return_details: Dict[Tuple[str, ...], Dict[str, Any]] = {}
    logistics_details: Dict[Tuple[str, ...], Dict[str, Any]] = {}
    other_details: Dict[Tuple[str, ...], Dict[str, Any]] = {}
    acceptance_details: Dict[Tuple[str, ...], Dict[str, Any]] = {}
    returns_for_pay_details: Dict[Tuple[str, ...], Dict[str, Any]] = {}
    paid_delivery_details: Dict[Tuple[str, ...], Dict[str, Any]] = {}
    e3_details: Dict[Tuple[str, ...], Dict[str, Any]] = {}
    products_vs_ksale_details: Dict[Tuple[str, ...], Dict[str, Any]] = {}

    for r in raw:
        oper = _norm(r.get("supplier_oper_name"))
        doc = r.get("doc_type_name")
        pay = _f(r.get("ppvz_for_pay"))
        qty = _i(r.get("quantity"))
        retail_t = _retail_with_disc(r)
        retail_p = _f(r.get("retail_amount"))
        barcode = str(r.get("barcode") or "").strip()

        delivery_rub = _f(r.get("delivery_rub"))
        storage_fee = _f(r.get("storage_fee"))
        acceptance_val = _f(r.get("acceptance"))
        deduction_val = _f(r.get("deduction"))

        delivery_count += _f(r.get("delivery_amount"))
        logistics += delivery_rub
        storage += storage_fee
        acceptance += acceptance_val
        other_deductions += deduction_val

        if abs(delivery_rub) >= 1e-9:
            _add_detail(logistics_details, r, delivery_rub)
        if abs(acceptance_val) >= 1e-9:
            _add_detail(acceptance_details, r, acceptance_val)
        if abs(deduction_val) >= 1e-9:
            _add_detail(other_details, r, deduction_val)

        penalty_val = _f(r.get("penalty"))
        additional_val = _f(r.get("additional_payment"))
        penalties += penalty_val
        additional_payment += additional_val
        if abs(penalty_val) >= 1e-9:
            _add_detail(penalty_details, r, penalty_val)
        if abs(additional_val) >= 1e-9:
            _add_detail(additional_details, r, additional_val)

        # Выкупы / возвраты (руб по T, шт по N)
        if oper in BUYOUT_OPERS:
            # «коррекция продаж» учитываем только с типом документа Продажа для qty/руб как в Excel SUMIFS без фильтра J
            # (в Excel для T/N на выкупах фильтр только по K, без J)
            buyouts_rub += retail_t
            buyouts_qty += qty
            wb_plus += retail_p
        if oper in RETURN_OPERS:
            returns_rub += retail_t
            returns_qty += qty
            wb_minus += retail_p
            _add_detail(return_details, r, retail_t)

        # Эквайринг: sale +fee, return -fee при percent > 0
        acq_pct = _f(r.get("acquiring_percent"))
        afee = _f(r.get("acquiring_fee"))
        if acq_pct > 0:
            if _is_sale_doc(doc):
                acquiring += afee
            elif _is_return_doc(doc):
                acquiring -= afee

        if "коррект" in oper and "эквайр" in oper:
            e3_acquiring_corr += pay
            _add_detail(e3_details, r, pay)

        if oper == "услуга платная доставка":
            paid_delivery += pay
            _add_detail(paid_delivery_details, r, pay)

        # Комиссия: суммы к перечислению по операциям
        is_ksale = _is_ksale_oper(oper, doc)
        is_kreturn = _is_kreturn_oper(oper, doc)
        if is_ksale:
            k_sale += pay
        if is_kreturn:
            k_return += pay
            _add_detail(returns_for_pay_details, r, pay)

        # Сумма по товарам − k_sale:
        # + операции в товарной сводке, не входящие в k_sale
        # − продажи k_sale, которые не попали в товарную сводку
        is_return_row = oper in RETURN_OPERS or _is_return_doc(doc)
        in_products = _row_in_products_breakdown(r, is_return_row=is_return_row)
        if in_products and not is_ksale and abs(pay) >= 1e-9:
            _add_detail(products_vs_ksale_details, r, pay)
        if is_ksale and not in_products and abs(pay) >= 1e-9:
            _add_detail(products_vs_ksale_details, r, -pay)

        # Компенсация брака (упрощённо по наборам Excel G18)
        if oper in DEFECT_OPERS:
            if _is_sale_doc(doc):
                defect += pay
                _add_detail(defect_details, r, pay)
            elif _is_return_doc(doc):
                defect -= pay
                _add_detail(defect_details, r, -pay)

        # Компенсация ущерба (G20)
        if oper in DAMAGE_OPERS:
            if _is_sale_doc(doc):
                damage += pay
                _add_detail(damage_details, r, pay)
            elif _is_return_doc(doc):
                damage -= pay
                _add_detail(damage_details, r, -pay)

    revenue_rub = buyouts_rub - returns_rub
    revenue_qty = buyouts_qty - returns_qty
    wb_realized = wb_plus - wb_minus

    # Комиссия B18 (без e3 внутри — e3 добавим в оплату на РС, как в рабочем _process_finance_data)
    commission = revenue_rub - k_sale + k_return - acquiring

    # Удержания WB (только удержания, без компенсаций/возвратов)
    deductions_wb_total = (
        commission
        + acquiring
        + logistics
        + storage
        + other_deductions
        + abs(acceptance)
    )

    # Полная формула G14 / оплата на РС (платная доставка уменьшает удержания = возвращается продавцу)
    deductions_total = (
        deductions_wb_total
        - defect
        - damage
        + penalties
        + additional_payment
        - paid_delivery
    )

    # Оплата на РС (M6) ≈ выручка − удержания + корректировка эквайринга
    payment_to_account = revenue_rub - deductions_total + e3_acquiring_corr

    storage_by_product = _build_paid_storage_by_product(paid_storage)
    paid_storage_total = round(sum(_f(x.get("amount")) for x in storage_by_product), 2)

    products = _build_products_breakdown(raw)
    products = _enrich_products_from_catalog(products, products_catalog)
    products = _apply_product_expense_columns(
        products,
        raw,
        acceptance_total=acceptance,
        paid_storage=paid_storage,
    )
    # Дообогащаем имена/баркоды у строк «только хранение»
    products = _enrich_products_from_catalog(products, products_catalog)
    other_details_final = _finalize_details(other_details)
    promotion_total = _promotion_total_from_details(other_details_final)
    products, promotion_by_product, advert_api_sum = _apply_promotion_allocation(
        products,
        promotion_total=promotion_total,
        promotion_spend=promotion_spend,
    )
    products_total = round(sum(_f(p.get("for_pay")) for p in products), 2)
    products = _apply_services_allocation(
        products,
        payment_to_account,
        products_total=products_total,
    )
    allocated_to_products = {
        "logistics": round(sum(_f(p.get("logistics")) for p in products), 2),
        "storage": round(sum(_f(p.get("storage")) for p in products), 2),
        "acceptance": round(sum(_f(p.get("acceptance")) for p in products), 2),
        "promotion": round(sum(_f(p.get("promotion")) for p in products), 2),
    }
    reconciliation = _build_payment_vs_products_reconciliation(
        products_total=products_total,
        payment_to_account=payment_to_account,
        k_sale=k_sale,
        k_return=k_return,
        logistics=logistics,
        storage=storage,
        other_deductions=other_deductions,
        acceptance=acceptance,
        penalties=penalties,
        additional_payment=additional_payment,
        defect=defect,
        damage=damage,
        paid_delivery=paid_delivery,
        e3_acquiring_corr=e3_acquiring_corr,
        promotion_total=promotion_total,
        allocated=allocated_to_products,
    )

    def pct(part: float, whole: float) -> float | None:
        if not whole:
            return None
        return round(part / whole * 100.0, 2)

    avg_check = round(revenue_rub / revenue_qty, 2) if revenue_qty else None
    buyout_rate = round(revenue_qty / delivery_count * 100.0, 2) if delivery_count else None
    spp_pct = pct(revenue_rub - wb_realized, revenue_rub)

    try:
        date_from_fmt = datetime.strptime(date_from, "%Y-%m-%d").strftime("%d.%m.%Y")
        date_to_fmt = datetime.strptime(date_to, "%Y-%m-%d").strftime("%d.%m.%Y")
    except Exception:
        date_from_fmt, date_to_fmt = date_from, date_to

    return {
        "success": True,
        "rows_count": len(raw),
        "date_from": date_from,
        "date_to": date_to,
        "date_from_fmt": date_from_fmt,
        "date_to_fmt": date_to_fmt,
        "sales": {
            "revenue_rub": round(revenue_rub, 2),
            "revenue_qty": int(revenue_qty),
            "buyouts_rub": round(buyouts_rub, 2),
            "buyouts_qty": int(buyouts_qty),
            "returns_rub": round(returns_rub, 2),
            "returns_qty": int(returns_qty),
            "avg_check": avg_check,
            "wb_realized": round(wb_realized, 2),
            "delivery_count": int(round(delivery_count)),
            "buyout_rate_pct": buyout_rate,
            "spp_pct": spp_pct,
            "payment_to_account": round(payment_to_account, 2),
        },
        "deductions": {
            "commission": round(commission, 2),
            "commission_pct": pct(commission, revenue_rub),
            "acquiring": round(acquiring, 2),
            "acquiring_pct": pct(acquiring, revenue_rub),
            "logistics": round(logistics, 2),
            "logistics_pct": pct(logistics, revenue_rub),
            "storage": round(storage, 2),
            "storage_pct": pct(storage, revenue_rub),
            "other": round(other_deductions, 2),
            "other_pct": pct(other_deductions, revenue_rub),
            "acceptance": round(abs(acceptance), 2),
            "acceptance_pct": pct(abs(acceptance), revenue_rub),
            "total": round(deductions_wb_total, 2),
            "total_pct": pct(deductions_wb_total, revenue_rub),
        },
        "compensations": {
            "defect": round(defect, 2),
            "damage": round(damage, 2),
            "penalties": round(penalties, 2),
            "additional_payment": round(additional_payment, 2),
            "paid_delivery": round(paid_delivery, 2),
        },
        "compensation_details": {
            "defect": _finalize_details(defect_details),
            "damage": _finalize_details(damage_details),
            "penalties": _finalize_details(penalty_details),
            "additional_payment": _finalize_details(additional_details),
            "returns": _finalize_details(return_details),
            "paid_delivery": _finalize_details(paid_delivery_details),
        },
        "reconciliation_details": {
            "logistics": _finalize_details(logistics_details),
            "storage": storage_by_product,
            "promotion": promotion_by_product,
            "other": [x for x in other_details_final if not _is_promotion_deduction_row(x)],
            "acceptance": (
                _negate_detail_amounts(_finalize_details(acceptance_details))
                if acceptance < 0
                else _finalize_details(acceptance_details)
            ),
            "penalties": _finalize_details(penalty_details),
            "additional": _finalize_details(additional_details),
            "returns_for_pay": _finalize_details(returns_for_pay_details),
            "defect": _negate_detail_amounts(_finalize_details(defect_details)),
            "damage": _negate_detail_amounts(_finalize_details(damage_details)),
            "paid_delivery": _negate_detail_amounts(_finalize_details(paid_delivery_details)),
            "e3": _negate_detail_amounts(_finalize_details(e3_details)),
            "products_vs_ksale": _finalize_details(products_vs_ksale_details),
        },
        "promotion_by_product": promotion_by_product,
        "promotion_total": round(promotion_total, 2),
        "promotion_advert_sum": advert_api_sum,
        "paid_storage_total": paid_storage_total,
        "paid_storage_rows": len(paid_storage or []),
        "products": products,
        "products_total": products_total,
        "reconciliation": reconciliation,
        "summary": {
            "commission": round(commission, 2),
            "logistics": round(logistics, 2),
            "storage": round(storage, 2),
            "other": round(other_deductions, 2),
            "acceptance": round(abs(acceptance), 2),
            "rest": round(acquiring + penalties + additional_payment - damage - defect, 2),
            "payment_to_account": round(payment_to_account, 2),
        },
    }

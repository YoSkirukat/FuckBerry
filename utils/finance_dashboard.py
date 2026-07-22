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
    """Подставляет баркод/название из кэша товаров (/products) по nm_id или артикулу."""
    if not products or not catalog:
        return products

    by_nm: Dict[int, Dict[str, Any]] = {}
    by_sa: Dict[str, Dict[str, Any]] = {}
    for p in catalog:
        if not isinstance(p, dict):
            continue
        nmv = p.get("nm_id") or p.get("nmId") or p.get("nmID")
        if nmv is not None:
            try:
                by_nm[int(nmv)] = p
            except Exception:
                pass
        sa = str(p.get("supplier_article") or p.get("vendor_code") or p.get("sa_name") or "").strip().lower()
        if sa:
            by_sa[sa] = p

    for item in products:
        if str(item.get("barcode") or "").strip():
            continue
        meta = None
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
        bc = str(meta.get("barcode") or "").strip()
        if bc:
            item["barcode"] = bc
        cat_name = str(meta.get("name") or "").strip()
        if cat_name and (not item.get("name") or item.get("name") in ("", "—")):
            item["name"] = cat_name
        if not item.get("sa_name"):
            sa_cat = str(meta.get("supplier_article") or "").strip()
            if sa_cat:
                item["sa_name"] = sa_cat
                if item.get("name") in ("", "—", None):
                    item["name"] = _product_title({
                        "brand_name": "",
                        "sa_name": sa_cat,
                        "subject_name": cat_name,
                    }) if not cat_name else cat_name
    return products


def _apply_services_allocation(
    products: List[Dict[str, Any]],
    payment_to_account: float,
) -> List[Dict[str, Any]]:
    """
    Раскидывает затраты (разницу до оплаты на РС) пропорционально сумме
    только по товарам с продажами (кол-во > 0).
    Итог «Сумма с услугами» по проданным = Оплата на РС.
    """
    if not products:
        return products

    target = round(_f(payment_to_account), 2)
    sold: List[Dict[str, Any]] = []
    for p in products:
        if _i(p.get("sales_qty")) > 0:
            sold.append(p)
        else:
            p["for_pay_with_services"] = None
            p["price_with_services"] = None

    if not sold:
        return products

    total = round(sum(_f(p.get("for_pay")) for p in sold), 2)
    if abs(total) < 1e-9:
        for p in sold:
            p["for_pay_with_services"] = 0.0
            p["price_with_services"] = None
        return products

    allocated = 0.0
    last_idx = len(sold) - 1
    for i, p in enumerate(sold):
        pay = _f(p.get("for_pay"))
        if i == last_idx:
            amt = round(target - allocated, 2)
        else:
            amt = round(pay / total * target, 2)
            allocated = round(allocated + amt, 2)
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
) -> Dict[str, Any]:
    """
    Тождество:
    Сумма по товарам − Оплата на РС =
      Логистика + Хранение + Прочие + Приёмка + Штрафы + Доплаты
      + К перечислению по возвратам
      − Компенсация брака − Компенсация ущерба − Платная доставка − Корр. эквайринга
      + (Сумма по товарам − К перечислению по продажам)
    """
    gap = round(products_total - payment_to_account, 2)
    products_minus_ksale = round(products_total - k_sale, 2)

    lines = [
        {"key": "logistics", "label": "Логистика", "amount": round(logistics, 2)},
        {"key": "storage", "label": "Хранение", "amount": round(storage, 2)},
        {"key": "other", "label": "Прочие удержания", "amount": round(other_deductions, 2)},
        {"key": "acceptance", "label": "Платная приёмка", "amount": round(abs(acceptance), 2)},
        {"key": "penalties", "label": "Штрафы", "amount": round(penalties, 2)},
        {"key": "additional", "label": "Доплаты", "amount": round(additional_payment, 2)},
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
    ]
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
    storage_details: Dict[Tuple[str, ...], Dict[str, Any]] = {}
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
        if abs(storage_fee) >= 1e-9:
            _add_detail(storage_details, r, storage_fee)
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

    products = _build_products_breakdown(raw)
    products = _enrich_products_from_catalog(products, products_catalog)
    products_total = round(sum(_f(p.get("for_pay")) for p in products), 2)
    products = _apply_services_allocation(products, payment_to_account)
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
            "storage": _finalize_details(storage_details),
            "other": _finalize_details(other_details),
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

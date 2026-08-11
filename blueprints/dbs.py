# -*- coding: utf-8 -*-
"""Blueprint для заказов DBS — вкладки как в ЛК Wildberries."""
import logging
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List

from flask import Blueprint, jsonify, render_template, request
from flask_login import current_user, login_required

from utils.api import (
    dbs_confirm_orders,
    dbs_deliver_orders,
    fetch_dbs_clients,
    fetch_dbs_delivery_dates,
    fetch_dbs_new_orders,
    fetch_dbs_orders,
    fetch_dbs_statuses,
    get_with_retry,
)
from utils.cache import (
    add_dbs_active_ids,
    add_dbs_known_orders,
    load_dbs_active_ids,
    load_dbs_known_orders,
    load_products_cache,
    save_dbs_active_ids,
    save_dbs_tasks_cache,
)
from utils.constants import API_URL, CACHE_DIR, MOSCOW_TZ
from utils.fbs_dbs_processing import _extract_created_at, to_dbs_rows
from utils.wb_token import effective_wb_api_token

logger = logging.getLogger(__name__)

dbs_bp = Blueprint("dbs", __name__)

TAB_NEW = "new"
TAB_CONFIRM = "confirm"
TAB_DELIVER = "deliver"
TAB_ARCHIVE = "archive"
TAB_CANCEL = "cancel"


def _enrich_dbs_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Подставляет фото/название из кэша товаров."""
    prod_cached = load_products_cache() or {}
    items = prod_cached.get("items") or []
    by_nm: Dict[int, Dict[str, Any]] = {}
    by_article: Dict[str, Dict[str, Any]] = {}
    by_barcode: Dict[str, Dict[str, Any]] = {}
    for it in items:
        if not isinstance(it, dict):
            continue
        nmv = it.get("nm_id") or it.get("nmID")
        try:
            if nmv is not None:
                by_nm[int(nmv)] = it
        except Exception:
            pass
        art = (it.get("supplier_article") or it.get("vendorCode") or it.get("article") or "").strip()
        if art:
            by_article.setdefault(art, it)
        bc = str(it.get("barcode") or "").strip()
        if bc:
            by_barcode[bc] = it

    for r in rows:
        hit = None
        nm = r.get("nm_id")
        try:
            nm_i = int(nm) if nm is not None else None
        except Exception:
            nm_i = None
        if nm_i is not None:
            hit = by_nm.get(nm_i)
        if not hit:
            art = (r.get("Наименование товара") or "").strip()
            if art:
                hit = by_article.get(art)
        if not hit:
            bc = str(r.get("barcode") or "").strip()
            if bc:
                hit = by_barcode.get(bc)
        if not hit:
            # фото уже могло быть проставлено при обогащении из stats
            if r.get("photo") and r.get("title"):
                continue
            continue
        name = (
            hit.get("name")
            or hit.get("title")
            or hit.get("vendorCode")
            or hit.get("supplier_article")
            or ""
        )
        if name:
            # Показываем бренд/название, артикул оставляем как подпись если был только article
            if not (r.get("Наименование товара") or "").strip() or r.get("Наименование товара") == hit.get("vendorCode"):
                r["Наименование товара"] = name
            r["title"] = name
        r["photo"] = hit.get("photo") or hit.get("img") or r.get("photo")
        if not r.get("barcode"):
            if hit.get("barcode"):
                r["barcode"] = hit.get("barcode")
            elif isinstance(hit.get("barcodes"), list) and hit.get("barcodes"):
                r["barcode"] = str(hit.get("barcodes")[0])
            else:
                sizes = hit.get("sizes") or []
                if isinstance(sizes, list):
                    for s in sizes:
                        bl = s.get("skus") or s.get("barcodes")
                        if isinstance(bl, list) and bl:
                            r["barcode"] = str(bl[0])
                            break
    return rows


def _extract_orders_from_dbs_list_payload(raw: Any) -> tuple[list[dict[str, Any]], Any]:
    orders: list[dict[str, Any]] = []
    next_cursor = None
    if isinstance(raw, list):
        return [x for x in raw if isinstance(x, dict)], None
    if not isinstance(raw, dict):
        return [], None
    arr = raw.get("orders")
    if isinstance(arr, list):
        orders = [x for x in arr if isinstance(x, dict)]
    elif isinstance(arr, dict):
        inner = arr.get("items") or arr.get("data") or []
        if isinstance(inner, list):
            orders = [x for x in inner if isinstance(x, dict)]
        next_cursor = arr.get("next")
    if not orders:
        data_val = raw.get("data")
        if isinstance(data_val, list):
            orders = [x for x in data_val if isinstance(x, dict)]
        elif isinstance(data_val, dict):
            for key in ("orders", "items"):
                val = data_val.get(key)
                if isinstance(val, list):
                    orders = [x for x in val if isinstance(x, dict)]
                    break
            next_cursor = data_val.get("next") if next_cursor is None else next_cursor
    if next_cursor is None:
        next_cursor = raw.get("next")
    return orders, next_cursor


def _order_id(it: dict[str, Any]) -> int | None:
    oid = it.get("id") or it.get("orderId") or it.get("ID") or it.get("orderID")
    try:
        return int(oid) if oid is not None else None
    except Exception:
        return None


def _apply_status_map(orders: list[dict[str, Any]], status_payload: Dict[str, Any]) -> Dict[int, dict[str, Any]]:
    status_arr = status_payload.get("orders") if isinstance(status_payload, dict) else []
    status_map: dict[int, dict[str, Any]] = {}
    if isinstance(status_arr, list):
        for x in status_arr:
            if not isinstance(x, dict):
                continue
            try:
                oid = int(x.get("id") or x.get("orderId") or x.get("orderID") or 0)
            except Exception:
                continue
            if oid:
                status_map[oid] = x
    for it in orders:
        oid = _order_id(it)
        if oid is None:
            continue
        sx = status_map.get(oid) or {}
        supplier = str(sx.get("supplierStatus") or sx.get("status") or it.get("supplierStatus") or it.get("status") or "").lower()
        wb = str(sx.get("wbStatus") or it.get("wbStatus") or "").lower()
        if supplier:
            it["status"] = supplier
            it["supplierStatus"] = supplier
        if wb:
            it["wbStatus"] = wb
        if sx.get("supplierStatusName") or sx.get("statusName"):
            it["statusName"] = sx.get("supplierStatusName") or sx.get("statusName")
    return status_map


_CANCEL_WB = {
    "canceled",
    "canceled_by_client",
    "declined_by_client",
    "defect",
    "canceled_by_missed_call",
}


def _bucket_for_order(it: dict[str, Any], *, was_active: bool = False) -> str | None:
    """Возвращает вкладку или None, если статус ещё неизвестен."""
    supplier = str(it.get("supplierStatus") or it.get("status") or "").lower().strip()
    wb = str(it.get("wbStatus") or "").lower().strip()
    # Отмены WB важнее supplierStatus=new (клиент отменил до сборки)
    if supplier in ("cancel", "reject", "cancel_missed_call") or wb in _CANCEL_WB:
        return TAB_CANCEL
    if supplier == "new":
        return TAB_NEW
    if supplier == "confirm":
        return TAB_CONFIRM
    if supplier == "deliver":
        return TAB_DELIVER
    if supplier == "receive" or wb in ("sold",):
        return TAB_ARCHIVE
    # Нет supplier/wb статуса: для ранее активных не кладём в архив
    if not supplier and not wb:
        if was_active:
            return TAB_DELIVER
        if it.get("createdAt") or it.get("address") or it.get("article"):
            return TAB_ARCHIVE
        return None
    return TAB_ARCHIVE


def _status_label(it: dict[str, Any]) -> str:
    supplier = str(it.get("supplierStatus") or it.get("status") or "").lower()
    wb = str(it.get("wbStatus") or "").lower()
    mapping = {
        "new": "Новый",
        "confirm": "Ждёт передачи в доставку",
        "deliver": "В доставке",
        "receive": "Получен покупателем",
        "cancel": "Отменён продавцом",
        "reject": "Отказ при получении",
        "cancel_missed_call": "Отмена: покупатель недоступен",
    }
    if wb in ("canceled_by_client", "declined_by_client"):
        return "Отменено покупателем"
    if supplier in mapping:
        return mapping[supplier]
    if wb == "sold":
        return "Выкуплен"
    return it.get("statusName") or supplier or wb or "—"


def _collect_tracked_ids() -> set[int]:
    ids: set[int] = set()
    try:
        active = load_dbs_active_ids() or {}
        for aid in (active.get("ids") or []):
            try:
                ids.add(int(aid))
            except Exception:
                pass
    except Exception:
        pass
    # legacy path dbs_active_user_{id}.json
    try:
        if current_user.is_authenticated:
            legacy = os.path.join(CACHE_DIR, f"dbs_active_user_{current_user.id}.json")
            if os.path.isfile(legacy):
                import json
                with open(legacy, "r", encoding="utf-8") as f:
                    data = json.load(f) or {}
                for aid in (data.get("ids") or []):
                    try:
                        ids.add(int(aid))
                    except Exception:
                        pass
    except Exception:
        pass
    try:
        known = load_dbs_known_orders() or {}
        for k in (known.get("orders") or {}):
            try:
                ids.add(int(k))
            except Exception:
                pass
    except Exception:
        pass
    return ids


def _paginate_dbs_orders(
    token: str,
    days: int = 90,
    max_pages_per_window: int = 8,
    page_limit: int = 200,
) -> list[dict[str, Any]]:
    """
    История DBS. WB принимает dateFrom/dateTo только в окне ≤30 дней;
    для большего периода идём скользящими окнами по 30 дней.
    В ответе бывают и незавершённые (deliver), но не confirm — их ищем отдельно.
    """
    now = datetime.now(MOSCOW_TZ)
    window_days = 30
    collected: list[dict[str, Any]] = []
    seen_ids: set[int] = set()
    total_days = max(1, int(days or 30))
    windows = max(1, (total_days + window_days - 1) // window_days)
    for w in range(windows):
        date_to = now - timedelta(days=w * window_days)
        date_from = now - timedelta(days=min(total_days, (w + 1) * window_days))
        date_to_ts = int(date_to.timestamp())
        date_from_ts = int(date_from.timestamp())
        cursor: Any = 0
        seen_cursors: set[Any] = set()
        for _ in range(max_pages_per_window):
            raw = fetch_dbs_orders(
                token,
                limit=page_limit,
                next_cursor=cursor,
                date_from_ts=date_from_ts,
                date_to_ts=date_to_ts,
            )
            # Явная ошибка параметров — не крутим дальше это окно
            if isinstance(raw, dict) and raw.get("code") == "IncorrectParameter":
                logger.warning("dbs orders IncorrectParameter window %s..%s", date_from_ts, date_to_ts)
                break
            page, next_cursor = _extract_orders_from_dbs_list_payload(raw)
            if not page:
                break
            for it in page:
                oid = _order_id(it)
                if oid is None or oid in seen_ids:
                    continue
                seen_ids.add(oid)
                collected.append(it)
            if next_cursor is None or next_cursor == "" or next_cursor == cursor or next_cursor in seen_cursors:
                break
            seen_cursors.add(next_cursor)
            cursor = next_cursor
    return collected


def _open_scan_ranges(seed_ids: set[int], *, max_gap: int = 2_000_000, forward: int = 100_000) -> list[tuple[int, int]]:
    """Диапазоны ID (start inclusive, end exclusive) для поиска confirm/deliver."""
    if not seed_ids:
        return []
    ordered = sorted(seed_ids)
    ranges: list[tuple[int, int]] = []
    for a, b in zip(ordered, ordered[1:]):
        gap = b - a - 1
        if 0 < gap <= max_gap:
            ranges.append((a + 1, b))
    ranges.append((ordered[-1] + 1, ordered[-1] + 1 + max(0, forward)))
    # Сначала свежие (большие ID)
    ranges.sort(key=lambda r: r[0], reverse=True)
    return ranges


def _stats_guided_scan_ranges(
    token: str,
    seed_ids: set[int],
    orders_map: dict[int, dict[str, Any]] | None = None,
    *,
    recent_days: int = 20,
    window: int = 750_000,
) -> list[tuple[int, int]]:
    """
    Узкие окна вокруг ID, оценённых по statistics-api.
    Нужно, когда между известными ID дыры > max_gap (иначе confirm/deliver теряются).
    """
    orders_map = orders_map or {}
    anchors: list[tuple[int, datetime]] = []
    used_bc: dict[str, int] = {}
    for oid in seed_ids:
        it = orders_map.get(oid) or {}
        created = _parse_dbs_created(it)
        if created is None:
            continue
        # createdAt после _parse_dbs_created уже в МСК (naive), как date в statistics-api
        if _order_has_product_card(it):
            anchors.append((oid, created))
            skus = it.get("skus") if isinstance(it.get("skus"), list) else []
            bcs = {str(x) for x in skus if x}
            if it.get("barcode"):
                bcs.add(str(it.get("barcode")))
            for bc in bcs:
                used_bc[bc] = used_bc.get(bc, 0) + 1
    anchors.sort(key=lambda x: x[0])
    if len(anchors) < 2:
        return []

    try:
        stats = _fetch_stats_seller_orders(token, days=recent_days)
    except Exception as exc:
        logger.warning("stats guided ranges failed: %s", exc)
        return []

    cutoff = datetime.now() - timedelta(days=recent_days)
    pool = dict(used_bc)
    ranges: list[tuple[int, int]] = []
    seen_est: set[int] = set()

    for st in stats:
        if st.get("isCancel"):
            continue
        st_dt = _parse_stats_dt(st.get("date"))
        if st_dt is None or st_dt < cutoff:
            continue
        bc = str(st.get("barcode") or "")
        # уже покрыт полной DBS-карточкой около того же времени
        linked = False
        if bc and pool.get(bc, 0) > 0:
            for oid, adt in anchors:
                it = orders_map.get(oid) or {}
                skus = it.get("skus") if isinstance(it.get("skus"), list) else []
                card_bcs = {str(x) for x in skus if x}
                if it.get("barcode"):
                    card_bcs.add(str(it.get("barcode")))
                if bc in card_bcs and abs((st_dt - adt).total_seconds()) <= 8 * 3600:
                    pool[bc] = pool.get(bc, 0) - 1
                    linked = True
                    break
        if linked:
            continue

        prev = None
        nxt = None
        for oid, adt in anchors:
            if adt <= st_dt:
                prev = (oid, adt)
            elif adt > st_dt and nxt is None:
                nxt = (oid, adt)
                break
        if prev and nxt and nxt[1] != prev[1]:
            ratio = (st_dt - prev[1]).total_seconds() / (nxt[1] - prev[1]).total_seconds()
            ratio = max(0.0, min(1.0, ratio))
            est = int(prev[0] + ratio * (nxt[0] - prev[0]))
        elif prev:
            est = prev[0] + 50_000
        elif nxt:
            est = max(1, nxt[0] - 50_000)
        else:
            continue
        # округляем, чтобы не плодить почти одинаковые окна
        bucket = est // 50_000
        if bucket in seen_est:
            continue
        seen_est.add(bucket)
        lo = max(1, est - window)
        hi = est + window + 1
        # не сканируем уже известные id как единственную цель — окно всё равно нужно
        ranges.append((lo, hi))

    ranges.sort(key=lambda r: r[0], reverse=True)
    return ranges


def _merge_scan_ranges(ranges: list[tuple[int, int]]) -> list[tuple[int, int]]:
    if not ranges:
        return []
    ordered = sorted(ranges, key=lambda r: r[0])
    merged: list[tuple[int, int]] = [ordered[0]]
    for start, end in ordered[1:]:
        ps, pe = merged[-1]
        if start <= pe:
            merged[-1] = (ps, max(pe, end))
        else:
            merged.append((start, end))
    # свежие первыми
    merged.sort(key=lambda r: r[0], reverse=True)
    return merged


def _discover_open_order_ids(
    token: str,
    seed_ids: set[int],
    *,
    max_batches: int = 2000,
    orders_map: dict[int, dict[str, Any]] | None = None,
) -> dict[int, dict[str, Any]]:
    """
    WB не отдаёт список заказов «на сборке»/части «в доставке».
    Ищем status/info'ом:
      1) в небольших промежутках между известными ID;
      2) в узких окнах вокруг ID, оценённых по statistics-api (дыры >2M).
    """
    import time

    found: dict[int, dict[str, Any]] = {}
    seed_ids = set(seed_ids)
    ranges = _open_scan_ranges(seed_ids)
    try:
        guided = _stats_guided_scan_ranges(token, seed_ids, orders_map)
        ranges = _merge_scan_ranges(list(ranges) + list(guided))
    except Exception as exc:
        logger.warning("stats guided discover skipped: %s", exc)
    if not ranges:
        return found
    batches = 0
    for start, end in ranges:
        chunk_start = start
        while chunk_start < end:
            if batches >= max_batches:
                logger.info("dbs discover stopped at max_batches=%s, found=%s", max_batches, len(found))
                return found
            chunk_end = min(chunk_start + 1000, end)
            batch_ids = list(range(chunk_start, chunk_end))
            chunk_start = chunk_end
            batches += 1
            try:
                st = fetch_dbs_statuses(token, batch_ids)
            except Exception as exc:
                logger.warning("dbs discover batch failed: %s", exc)
                time.sleep(0.35)
                continue
            arr = st.get("orders") if isinstance(st, dict) else None
            if not isinstance(arr, list):
                continue
            for x in arr:
                if not isinstance(x, dict):
                    continue
                try:
                    oid = int(x.get("orderId") or x.get("id") or x.get("orderID") or 0)
                except Exception:
                    continue
                if not oid:
                    continue
                supplier = str(x.get("supplierStatus") or x.get("status") or "").lower().strip()
                wb = str(x.get("wbStatus") or "").lower().strip()
                if not supplier and not wb:
                    continue
                if supplier in ("confirm", "deliver") or (supplier == "new" and wb not in _CANCEL_WB and not wb):
                    found[oid] = {
                        "id": oid,
                        "supplierStatus": supplier,
                        "status": supplier,
                        "wbStatus": wb,
                    }
            time.sleep(0.22)
    logger.info("dbs discover done batches=%s found=%s", batches, sorted(found.keys()))
    return found


def _order_has_product_card(it: dict[str, Any]) -> bool:
    if it.get("article") or it.get("nmId") or it.get("nmID"):
        return True
    skus = it.get("skus")
    if isinstance(skus, list) and skus:
        return True
    if it.get("barcode"):
        return True
    return False


def _parse_stats_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        s = str(value).replace("Z", "")
        # statistics-api отдаёт локальное время продавца без таймзоны
        return datetime.fromisoformat(s)
    except Exception:
        return None


def _parse_dbs_created(it: dict[str, Any]) -> datetime | None:
    raw = it.get("createdAt") or it.get("dateCreated") or it.get("date")
    if not raw:
        return None
    try:
        s = str(raw).replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is not None:
            dt = dt.astimezone(MOSCOW_TZ).replace(tzinfo=None)
        return dt
    except Exception:
        return None


def _fetch_stats_seller_orders(token: str, days: int = 30) -> list[dict[str, Any]]:
    """Заказы со склада продавца из statistics-api (есть артикул/nmId/баркод)."""
    if not token:
        return []
    date_from = (datetime.now(MOSCOW_TZ) - timedelta(days=max(1, days))).strftime("%Y-%m-%dT00:00:00")
    headers_list = [
        {"Authorization": f"{token}"},
        {"Authorization": f"Bearer {token}"},
    ]
    collected: list[dict[str, Any]] = []
    seen_srid: set[str] = set()
    cursor = date_from
    for _ in range(40):
        page = None
        last_err = None
        for headers in headers_list:
            try:
                resp = get_with_retry(API_URL, headers, {"dateFrom": cursor, "flag": 0}, max_retries=2)
                data = resp.json()
                if isinstance(data, list):
                    page = data
                    break
            except Exception as exc:
                last_err = exc
                continue
        if page is None:
            if last_err:
                logger.warning("stats seller orders failed: %s", last_err)
            break
        if not page:
            break
        advanced = False
        for it in page:
            if not isinstance(it, dict):
                continue
            if it.get("warehouseType") != "Склад продавца":
                continue
            srid = str(it.get("srid") or "")
            if srid and srid in seen_srid:
                continue
            if srid:
                seen_srid.add(srid)
            collected.append(it)
            lcd = str(it.get("lastChangeDate") or it.get("date") or "")
            if lcd > cursor:
                cursor = lcd
                advanced = True
        if not advanced or len(page) < 100:
            break
    return collected


def _enrich_incomplete_orders_from_stats(token: str, orders_map: dict[int, dict[str, Any]]) -> int:
    """
    Для заказов без карточки (только id/status после discovery) подтягиваем
    артикул/nmId/баркод/цену из statistics-api и фото из кэша товаров.
    Сопоставление: свободные stats-строки ↔ stub по ближайшему времени
    (время stub оцениваем интерполяцией между соседними известными DBS id).
    """
    incomplete = [oid for oid, it in orders_map.items() if not _order_has_product_card(it)]
    if not incomplete:
        return 0

    complete = sorted(
        (
            (oid, _parse_dbs_created(it))
            for oid, it in orders_map.items()
            if _order_has_product_card(it) and _parse_dbs_created(it) is not None
        ),
        key=lambda x: x[0],
    )
    # Сколько раз каждый баркод уже «занят» полными DBS-карточками
    used_barcode_counts: dict[str, int] = {}
    for oid, it in orders_map.items():
        if not _order_has_product_card(it):
            continue
        bcs: list[str] = []
        skus = it.get("skus")
        if isinstance(skus, list):
            bcs.extend(str(x) for x in skus if x)
        if it.get("barcode"):
            bcs.append(str(it.get("barcode")))
        for bc in set(bcs):
            used_barcode_counts[bc] = used_barcode_counts.get(bc, 0) + 1

    try:
        stats = _fetch_stats_seller_orders(token, days=35)
    except Exception as exc:
        logger.warning("enrich from stats failed: %s", exc)
        return 0

    # Привязываем stats к полным карточкам по баркоду+времени, остальное — кандидаты
    candidates: list[dict[str, Any]] = []
    barcode_pool = dict(used_barcode_counts)
    for st in stats:
        if st.get("isCancel"):
            continue
        bc = str(st.get("barcode") or "")
        st_dt = _parse_stats_dt(st.get("date"))
        linked = False
        if bc and barcode_pool.get(bc, 0) > 0 and st_dt is not None:
            # ищем полную карточку с этим баркодом около того же времени
            for oid, created in complete:
                it = orders_map.get(oid) or {}
                skus = it.get("skus") if isinstance(it.get("skus"), list) else []
                card_bcs = {str(x) for x in skus if x}
                if it.get("barcode"):
                    card_bcs.add(str(it.get("barcode")))
                if bc not in card_bcs or created is None:
                    continue
                if abs((st_dt - created).total_seconds()) <= 8 * 3600:
                    barcode_pool[bc] = barcode_pool.get(bc, 0) - 1
                    linked = True
                    break
        if not linked:
            candidates.append(st)

    def estimate_created(oid: int) -> datetime | None:
        prev = None
        nxt = None
        for coid, cdt in complete:
            if coid < oid:
                prev = (coid, cdt)
            elif coid > oid and nxt is None:
                nxt = (coid, cdt)
                break
        if prev and nxt and prev[1] and nxt[1] and nxt[0] != prev[0]:
            ratio = (oid - prev[0]) / (nxt[0] - prev[0])
            ratio = max(0.0, min(1.0, ratio))
            delta = (nxt[1] - prev[1]).total_seconds()
            return prev[1] + timedelta(seconds=delta * ratio)
        if prev and prev[1]:
            return prev[1] + timedelta(hours=1)
        if nxt and nxt[1]:
            return nxt[1] - timedelta(hours=1)
        return None

    prod_cached = load_products_cache() or {}
    prod_items = prod_cached.get("items") or []
    by_nm: dict[int, dict] = {}
    by_bc: dict[str, dict] = {}
    by_art: dict[str, dict] = {}
    for p in prod_items:
        if not isinstance(p, dict):
            continue
        try:
            nm = p.get("nm_id") or p.get("nmID")
            if nm is not None:
                by_nm[int(nm)] = p
        except Exception:
            pass
        bc = str(p.get("barcode") or "").strip()
        if bc:
            by_bc[bc] = p
        art = (p.get("supplier_article") or p.get("vendorCode") or p.get("article") or "").strip()
        if art:
            by_art[art] = p

    enriched = 0
    used_srids: set[str] = set()
    for oid in sorted(incomplete):
        est = estimate_created(oid)
        best = None
        best_score = None
        for st in candidates:
            srid = str(st.get("srid") or "")
            if srid and srid in used_srids:
                continue
            st_dt = _parse_stats_dt(st.get("date"))
            if est is not None and st_dt is not None:
                score = abs((st_dt - est).total_seconds())
            elif st_dt is not None:
                score = abs((datetime.now() - st_dt).total_seconds()) + 10**9
            else:
                continue
            # отсекаем явный мусор > 3 суток от оценки
            if est is not None and score > 3 * 86400:
                continue
            if best_score is None or score < best_score:
                best_score = score
                best = st
        if not best:
            continue
        srid = str(best.get("srid") or "")
        if srid:
            used_srids.add(srid)

        article = best.get("supplierArticle") or ""
        nm_id = best.get("nmId")
        barcode = str(best.get("barcode") or "")
        try:
            price_rub = float(best.get("finishedPrice") or best.get("priceWithDisc") or 0)
            price_kop = int(round(price_rub * 100))
        except Exception:
            price_kop = 0

        it = orders_map[oid]
        it["article"] = article
        if nm_id is not None:
            it["nmId"] = nm_id
        if barcode:
            it["skus"] = [barcode]
            it["barcode"] = barcode
        if price_kop:
            it["convertedFinalPrice"] = price_kop
            it["finalPrice"] = price_kop
            it["price"] = price_kop
        if not it.get("createdAt") and best.get("date"):
            # stats date ≈ Moscow local; сохраняем как ISO Z≈UTC-3 грубо не трогаем — для UI ок
            it["createdAt"] = str(best.get("date"))
        if best.get("warehouseName"):
            it["warehouseName"] = best.get("warehouseName")

        hit = None
        try:
            if nm_id is not None:
                hit = by_nm.get(int(nm_id))
        except Exception:
            hit = None
        if not hit and barcode:
            hit = by_bc.get(barcode)
        if not hit and article:
            hit = by_art.get(str(article).strip())
        if hit:
            if hit.get("photo") or hit.get("img"):
                it["photo"] = hit.get("photo") or hit.get("img")
            if hit.get("name") and not it.get("title"):
                it["title"] = hit.get("name")
        enriched += 1

    if enriched:
        try:
            add_dbs_known_orders([orders_map[oid] for oid in incomplete if _order_has_product_card(orders_map[oid])])
        except Exception:
            pass
        logger.info("enriched %s incomplete DBS orders from stats", enriched)
    return enriched


# Фоновый поиск заказов «на сборке» / «в доставке», которых нет в /new и /orders
_discovery_state: dict[int, dict[str, Any]] = {}
_discovery_lock = None


def _get_discovery_lock():
    global _discovery_lock
    if _discovery_lock is None:
        import threading
        _discovery_lock = threading.Lock()
    return _discovery_lock


def get_dbs_discovery_state(user_id: int) -> dict[str, Any]:
    st = _discovery_state.get(int(user_id)) or {}
    return {
        "status": st.get("status") or "idle",
        "found": st.get("found") or 0,
        "message": st.get("message") or "",
        "updated_at": st.get("updated_at"),
    }


def _run_discovery_job(user_id: int, token: str, seed_ids: set[int]) -> None:
    # flask current_user недоступен в фоне — пишем кэш по суффиксу user_id напрямую
    import json

    def _save_found(found: dict[int, dict[str, Any]]) -> None:
        if not found:
            return
        path_known = os.path.join(CACHE_DIR, f"dbs_known_orders_user_{user_id}.json")
        path_active = os.path.join(CACHE_DIR, f"dbs_active_ids_user_{user_id}.json")
        try:
            known = {"orders": {}, "updated_at": None}
            if os.path.isfile(path_known):
                with open(path_known, "r", encoding="utf-8") as f:
                    known = json.load(f) or known
            orders = known.get("orders") or {}
            now_s = datetime.now(MOSCOW_TZ).strftime("%Y-%m-%d %H:%M:%S")
            for oid, it in found.items():
                key = str(int(oid))
                prev = orders.get(key)
                prev_item = prev.get("item") if isinstance(prev, dict) else None
                base = dict(prev_item) if isinstance(prev_item, dict) else {}
                base.update(it)
                orders[key] = {"item": base, "seen_at": now_s}
            known["orders"] = orders
            known["updated_at"] = datetime.now(MOSCOW_TZ).strftime("%d.%m.%Y %H:%M:%S")
            with open(path_known, "w", encoding="utf-8") as f:
                json.dump(known, f, ensure_ascii=False)
        except Exception as exc:
            logger.warning("discovery save known failed: %s", exc)
        try:
            active = {"ids": [], "updated_at": None}
            if os.path.isfile(path_active):
                with open(path_active, "r", encoding="utf-8") as f:
                    active = json.load(f) or active
            ids = set(int(x) for x in (active.get("ids") or []))
            ids.update(int(x) for x in found.keys())
            active["ids"] = sorted(ids)
            active["updated_at"] = datetime.now(MOSCOW_TZ).strftime("%d.%m.%Y %H:%M:%S")
            with open(path_active, "w", encoding="utf-8") as f:
                json.dump(active, f, ensure_ascii=False)
        except Exception as exc:
            logger.warning("discovery save active failed: %s", exc)

    try:
        _discovery_state[user_id] = {
            "status": "running",
            "found": 0,
            "message": "Ищем заказы на сборке и в доставке…",
            "updated_at": datetime.now().strftime("%d.%m.%Y %H:%M:%S"),
        }
        # known-карточки нужны для stats-guided окон (дыры >2M между ID)
        orders_map: dict[int, dict[str, Any]] = {}
        try:
            path_known = os.path.join(CACHE_DIR, f"dbs_known_orders_user_{user_id}.json")
            if os.path.isfile(path_known):
                with open(path_known, "r", encoding="utf-8") as f:
                    known = json.load(f) or {}
                for k, v in (known.get("orders") or {}).items():
                    try:
                        oid = int(k)
                    except Exception:
                        continue
                    item = v.get("item") if isinstance(v, dict) else None
                    if isinstance(item, dict):
                        orders_map[oid] = item
        except Exception as exc:
            logger.warning("discovery load known failed: %s", exc)
        for oid in seed_ids:
            try:
                orders_map.setdefault(int(oid), {"id": int(oid)})
            except Exception:
                pass
        found = _discover_open_order_ids(
            token,
            set(seed_ids) | set(orders_map.keys()),
            max_batches=3500,
            orders_map=orders_map,
        )
        _save_found(found)
        _discovery_state[user_id] = {
            "status": "done",
            "found": len(found),
            "message": f"Найдено незавершённых: {len(found)}" if found else "Дополнительных незавершённых не найдено",
            "updated_at": datetime.now().strftime("%d.%m.%Y %H:%M:%S"),
            "ids": sorted(found.keys()),
        }
    except Exception as exc:
        logger.exception("dbs discovery failed")
        _discovery_state[user_id] = {
            "status": "error",
            "found": 0,
            "message": str(exc),
            "updated_at": datetime.now().strftime("%d.%m.%Y %H:%M:%S"),
        }


def start_dbs_discovery(user_id: int, token: str, seed_ids: set[int], *, force: bool = False) -> dict[str, Any]:
    import threading
    from datetime import datetime as dt

    with _get_discovery_lock():
        cur = _discovery_state.get(user_id) or {}
        if cur.get("status") == "running":
            return get_dbs_discovery_state(user_id)
        if not force and cur.get("status") == "done":
            # повторный автозапуск не чаще чем раз в 15 минут
            try:
                prev = dt.strptime(str(cur.get("updated_at") or ""), "%d.%m.%Y %H:%M:%S")
                if (dt.now() - prev).total_seconds() < 15 * 60:
                    return get_dbs_discovery_state(user_id)
            except Exception:
                pass
        t = threading.Thread(
            target=_run_discovery_job,
            args=(int(user_id), token, set(seed_ids)),
            daemon=True,
            name=f"dbs-discover-{user_id}",
        )
        _discovery_state[user_id] = {
            "status": "running",
            "found": 0,
            "message": "Ищем заказы на сборке и в доставке…",
            "updated_at": datetime.now().strftime("%d.%m.%Y %H:%M:%S"),
        }
        t.start()
    return get_dbs_discovery_state(user_id)


def _merge_extra_info(token: str, orders: list[dict[str, Any]]) -> None:
    ids = [oid for oid in (_order_id(o) for o in orders) if oid is not None]
    if not ids:
        return
    try:
        dates = fetch_dbs_delivery_dates(token, ids[:1000])
        for it in orders:
            oid = _order_id(it)
            if oid and oid in dates:
                it.update({k: v for k, v in dates[oid].items() if k != "id"})
    except Exception as exc:
        logger.warning("dbs delivery-date failed: %s", exc)
    try:
        clients = fetch_dbs_clients(token, ids[:1000])
        for it in orders:
            oid = _order_id(it)
            if oid and oid in clients:
                c = clients[oid]
                # Для курьера WB чаще даёт мобильный в replacementPhone
                phone = c.get("replacementPhone") or c.get("phone")
                if phone:
                    it["phone"] = phone
                if c.get("phoneCode") is not None:
                    it["phoneCode"] = c.get("phoneCode")
                if c.get("fullName") or c.get("firstName"):
                    it["fullName"] = c.get("fullName") or c.get("firstName")
    except Exception as exc:
        logger.warning("dbs client info failed: %s", exc)


def _build_board(token: str, *, run_discovery: bool = True, force_discovery: bool = False) -> Dict[str, Any]:
    orders_map: dict[int, dict[str, Any]] = {}
    previously_active = _collect_tracked_ids()

    # 1) Новые — источник истины для вкладки «Новые»
    new_raw = fetch_dbs_new_orders(token) or []
    for it in new_raw:
        oid = _order_id(it)
        if oid is None:
            continue
        it = dict(it)
        it["status"] = "new"
        it["supplierStatus"] = "new"
        orders_map[oid] = it

    try:
        if new_raw:
            add_dbs_known_orders([dict(x) for x in new_raw if isinstance(x, dict)])
            add_dbs_active_ids([oid for oid in (_order_id(x) for x in new_raw) if oid is not None])
    except Exception:
        pass

    # 2) История API — окна по 30 дней (лимит WB). Включает deliver и завершённые.
    recent = _paginate_dbs_orders(token, days=90, max_pages_per_window=8, page_limit=200)
    for it in recent:
        oid = _order_id(it)
        if oid is None:
            continue
        if oid not in orders_map:
            orders_map[oid] = dict(it)

    # 3) Известные ранее заказы (карточки) — критично для «На сборке»
    try:
        known = load_dbs_known_orders() or {}
        for k, v in (known.get("orders") or {}).items():
            try:
                oid = int(k)
            except Exception:
                continue
            item = v.get("item") if isinstance(v, dict) and isinstance(v.get("item"), dict) else v
            if not isinstance(item, dict):
                continue
            if oid not in orders_map:
                orders_map[oid] = dict(item)
            else:
                for fk, fv in item.items():
                    if fk in ("status", "supplierStatus", "wbStatus", "statusName"):
                        continue
                    if orders_map[oid].get(fk) in (None, "", [], {}):
                        orders_map[oid][fk] = fv
    except Exception:
        pass

    # 4) Трекаемые ID без карточки
    for oid in previously_active:
        if oid not in orders_map:
            orders_map[oid] = {"id": oid}

    # 4b) Stub без артикула/фото — обогащаем из statistics-api + кэш товаров
    try:
        _enrich_incomplete_orders_from_stats(token, orders_map)
    except Exception as exc:
        logger.warning("enrich incomplete dbs failed: %s", exc)

    seed_ids = set(orders_map.keys()) | set(previously_active)

    # 5) Фоновый поиск confirm/deliver между известными ID
    discovery = {"status": "idle"}
    try:
        if run_discovery and current_user.is_authenticated:
            discovery = start_dbs_discovery(int(current_user.id), token, seed_ids, force=force_discovery)
    except Exception as exc:
        logger.warning("start discovery failed: %s", exc)

    all_ids = list(orders_map.keys())
    new_id_set = {oid for oid in (_order_id(x) for x in new_raw) if oid is not None}
    priority_ids = [oid for oid in all_ids if oid in previously_active or oid in new_id_set]
    rest_ids = [oid for oid in all_ids if oid not in set(priority_ids)]
    ordered_ids = priority_ids + rest_ids
    for i in range(0, len(ordered_ids), 1000):
        batch = ordered_ids[i : i + 1000]
        try:
            st = fetch_dbs_statuses(token, batch)
            _apply_status_map([orders_map[oid] for oid in batch if oid in orders_map], st)
            status_arr = st.get("orders") if isinstance(st, dict) else []
            if isinstance(status_arr, list):
                for x in status_arr:
                    if not isinstance(x, dict):
                        continue
                    try:
                        oid = int(x.get("id") or x.get("orderId") or x.get("orderID") or 0)
                    except Exception:
                        continue
                    if not oid:
                        continue
                    supplier = str(x.get("supplierStatus") or x.get("status") or "").lower()
                    wb = str(x.get("wbStatus") or "").lower()
                    if oid not in orders_map:
                        orders_map[oid] = {"id": oid}
                    if supplier:
                        orders_map[oid]["supplierStatus"] = supplier
                        orders_map[oid]["status"] = supplier
                    if wb:
                        orders_map[oid]["wbStatus"] = wb
                    if x.get("supplierStatusName") or x.get("statusName"):
                        orders_map[oid]["statusName"] = x.get("supplierStatusName") or x.get("statusName")
        except Exception as exc:
            logger.warning("board status batch failed: %s", exc)

    new_ids = set()
    for it in new_raw:
        oid = _order_id(it)
        if oid is not None:
            new_ids.add(oid)
            orders_map[oid]["supplierStatus"] = "new"
            orders_map[oid]["status"] = "new"

    try:
        add_dbs_known_orders(list(orders_map.values()))
    except Exception:
        pass

    buckets: dict[str, list[dict[str, Any]]] = {
        TAB_NEW: [],
        TAB_CONFIRM: [],
        TAB_DELIVER: [],
        TAB_ARCHIVE: [],
        TAB_CANCEL: [],
    }
    still_active: list[int] = []
    for oid, it in orders_map.items():
        if oid in new_ids:
            bucket = TAB_NEW
        else:
            bucket = _bucket_for_order(it, was_active=oid in previously_active)
        if bucket is None:
            continue
        it["statusName"] = _status_label(it)
        buckets[bucket].append(it)
        if bucket in (TAB_CONFIRM, TAB_DELIVER, TAB_NEW):
            still_active.append(oid)

    try:
        save_dbs_active_ids({
            "ids": sorted(set(still_active)),
            "updated_at": datetime.now(MOSCOW_TZ).strftime("%d.%m.%Y %H:%M:%S"),
        })
    except Exception:
        pass

    live = buckets[TAB_CONFIRM] + buckets[TAB_DELIVER] + buckets[TAB_NEW]
    _merge_extra_info(token, live)

    result_rows: dict[str, list] = {}
    for key, items in buckets.items():
        try:
            # «В доставке» — от старых к новым; остальные — сначала свежие
            newest_first = key != TAB_DELIVER

            def _sort_key(it: dict[str, Any]):
                dt = _extract_created_at(it)
                try:
                    oid = int(it.get("id") or it.get("orderId") or 0)
                except Exception:
                    oid = 0
                return (dt, oid)

            items_sorted = sorted(items, key=_sort_key, reverse=newest_first)
        except Exception:
            items_sorted = items
        rows = _enrich_dbs_rows(to_dbs_rows(items_sorted))
        # Повторная сортировка по полям строки (на случай сбоя ключа на сырых данных)
        try:
            if key == TAB_DELIVER:
                rows = sorted(rows, key=lambda r: (_extract_created_at(r), int(r.get("orderId") or 0)))
            else:
                rows = sorted(rows, key=lambda r: (_extract_created_at(r), int(r.get("orderId") or 0)), reverse=True)
        except Exception:
            pass
        result_rows[key] = rows

    counts = {k: len(v) for k, v in result_rows.items()}
    now_str = datetime.now().strftime("%d.%m.%Y %H:%M:%S")
    try:
        save_dbs_tasks_cache({"rows": result_rows.get(TAB_NEW) or [], "updated_at": now_str, "counts": counts})
    except Exception:
        pass

    return {
        "counts": counts,
        "tabs": result_rows,
        "updated_at": now_str,
        "discovery": discovery if isinstance(discovery, dict) else get_dbs_discovery_state(int(current_user.id)) if current_user.is_authenticated else {},
    }


def _results_ok(payload: Dict[str, Any], order_ids: list[int]) -> tuple[bool, list]:
    results = payload.get("results") if isinstance(payload, dict) else None
    if not isinstance(results, list) or not results:
        return True, []
    errors = []
    ok_ids = set()
    for r in results:
        if not isinstance(r, dict):
            continue
        try:
            rid = int(r.get("orderId") or r.get("id") or 0)
        except Exception:
            rid = 0
        if r.get("isError"):
            errors.append(r)
        elif rid:
            ok_ids.add(rid)
    # если все целевые без isError — ок
    if errors:
        return False, errors
    return True, []


@dbs_bp.route("/dbs", methods=["GET"])
@login_required
def dbs_page():
    error = None
    products_hint = None
    prod_cached_now = load_products_cache()
    if not prod_cached_now or not ((prod_cached_now or {}).get("items")):
        products_hint = "Для отображения фото товара и баркода обновите данные на странице Товары"
    return render_template("dbs.html", error=error, products_hint=products_hint)


@dbs_bp.route("/api/dbs/orders/board", methods=["GET"])
@login_required
def api_dbs_orders_board():
    """Единая доска DBS: counts + заказы по вкладкам."""
    token = effective_wb_api_token(current_user)
    if not token:
        empty = {TAB_NEW: [], TAB_CONFIRM: [], TAB_DELIVER: [], TAB_ARCHIVE: [], TAB_CANCEL: []}
        return jsonify({
            "counts": {k: 0 for k in empty},
            "tabs": empty,
            "updated_at": None,
            "error": "no_token",
        }), 200
    try:
        force = str(request.args.get("discover") or "").lower() in ("1", "true", "yes")
        board = _build_board(token, force_discovery=force)
        # актуальный статус поиска (мог обновиться в фоне)
        if current_user.is_authenticated:
            board["discovery"] = get_dbs_discovery_state(int(current_user.id))
        return jsonify(board), 200
    except Exception as exc:
        logger.exception("api_dbs_orders_board failed")
        empty = {TAB_NEW: [], TAB_CONFIRM: [], TAB_DELIVER: [], TAB_ARCHIVE: [], TAB_CANCEL: []}
        return jsonify({
            "counts": {k: 0 for k in empty},
            "tabs": empty,
            "error": str(exc),
        }), 200


@dbs_bp.route("/api/dbs/orders/confirm", methods=["POST"])
@login_required
def api_dbs_orders_confirm_bulk():
    """Новые → На сборке."""
    token = effective_wb_api_token(current_user)
    if not token:
        return jsonify({"error": "No token"}), 401
    body = request.get_json(silent=True) or {}
    order_ids = []
    for x in (body.get("orderIds") or body.get("ordersIds") or body.get("orders") or []):
        try:
            order_ids.append(int(x))
        except Exception:
            continue
    if not order_ids:
        return jsonify({"error": "empty_order_ids"}), 400
    try:
        res = dbs_confirm_orders(token, order_ids)
        ok, errors = _results_ok(res, order_ids)
        add_dbs_active_ids(order_ids)
        if not ok:
            return jsonify({"success": False, "error": errors, "result": res}), 409
        return jsonify({"success": True, "result": res, "count": len(order_ids)}), 200
    except Exception as exc:
        logger.exception("confirm bulk failed")
        return jsonify({"error": str(exc)}), 500


@dbs_bp.route("/api/dbs/orders/deliver", methods=["POST"])
@login_required
def api_dbs_orders_deliver_bulk():
    """На сборке → В доставке (только deliver, без повторного confirm)."""
    token = effective_wb_api_token(current_user)
    if not token:
        return jsonify({"error": "No token"}), 401
    body = request.get_json(silent=True) or {}
    order_ids = []
    for x in (body.get("orderIds") or body.get("ordersIds") or body.get("orders") or []):
        try:
            order_ids.append(int(x))
        except Exception:
            continue
    if not order_ids:
        return jsonify({"error": "empty_order_ids"}), 400
    try:
        # На случай если заказ ещё new — сначала confirm
        do_confirm = bool(body.get("confirm_first"))
        confirm_res = None
        if do_confirm:
            confirm_res = dbs_confirm_orders(token, order_ids)
        deliver_res = dbs_deliver_orders(token, order_ids)
        ok, errors = _results_ok(deliver_res, order_ids)
        add_dbs_active_ids(order_ids)
        if not ok:
            return jsonify({
                "success": False,
                "error": errors,
                "confirm": confirm_res,
                "deliver": deliver_res,
            }), 409
        return jsonify({
            "success": True,
            "confirm": confirm_res,
            "deliver": deliver_res,
            "count": len(order_ids),
        }), 200
    except Exception as exc:
        logger.exception("deliver bulk failed")
        return jsonify({"error": str(exc)}), 500


# Совместимость со старым UI / кэшем
@dbs_bp.route("/api/dbs/orders/new", methods=["GET"])
@login_required
def api_dbs_orders_new():
    token = effective_wb_api_token(current_user)
    if not token:
        return jsonify({"items": [], "updated_at": None}), 200
    try:
        board = _build_board(token)
        return jsonify({
            "items": (board.get("tabs") or {}).get(TAB_NEW) or [],
            "updated_at": board.get("updated_at"),
            "counts": board.get("counts"),
        }), 200
    except Exception as exc:
        return jsonify({"items": [], "error": str(exc)}), 200


@dbs_bp.route("/api/dbs/orders/in-delivery", methods=["GET"])
@login_required
def api_dbs_orders_in_delivery():
    token = effective_wb_api_token(current_user)
    if not token:
        return jsonify({"items": [], "updated_at": None}), 200
    try:
        board = _build_board(token)
        tabs = board.get("tabs") or {}
        items = (tabs.get(TAB_DELIVER) or []) + (tabs.get(TAB_CONFIRM) or [])
        return jsonify({"items": items, "updated_at": board.get("updated_at"), "counts": board.get("counts")}), 200
    except Exception as exc:
        return jsonify({"items": [], "error": str(exc)}), 200


@dbs_bp.route("/api/dbs/orders", methods=["GET"])
@login_required
def api_dbs_orders_list():
    token = effective_wb_api_token(current_user)
    if not token:
        return jsonify({"items": [], "next": None}), 200
    try:
        board = _build_board(token)
        tabs = board.get("tabs") or {}
        items = (tabs.get(TAB_ARCHIVE) or []) + (tabs.get(TAB_CANCEL) or [])
        return jsonify({"items": items, "next": None, "updated_at": board.get("updated_at")}), 200
    except Exception as exc:
        return jsonify({"items": [], "next": None, "error": str(exc)}), 200

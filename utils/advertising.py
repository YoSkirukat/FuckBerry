# -*- coding: utf-8 -*-
"""WB Advertising API — затраты продвижения по nmId (для расшифровки финотчёта)."""
from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from utils.api import get_with_retry_json
from utils.cache import load_products_cache
from utils.constants import (
    ADV_ADVERTS_URL,
    ADV_FULLSTATS_CHUNK,
    ADV_FULLSTATS_MIN_INTERVAL_S,
    ADV_FULLSTATS_URL,
)

logger = logging.getLogger(__name__)

# Статусы, для которых fullstats доступен
FULLSTATS_STATUSES = {7, 9, 11}

_last_fullstats_ts: float = 0.0


def _auth_headers(token: str) -> Dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def _throttle_fullstats() -> None:
    global _last_fullstats_ts
    now = time.time()
    wait = ADV_FULLSTATS_MIN_INTERVAL_S - (now - _last_fullstats_ts)
    if wait > 0:
        time.sleep(wait)
    _last_fullstats_ts = time.time()


def fetch_adverts(
    token: str,
    statuses: Optional[str] = "4,7,9,11",
    payment_type: Optional[str] = None,
    ids: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Список кампаний через GET /api/advert/v2/adverts."""
    params: Dict[str, Any] = {}
    if statuses:
        params["statuses"] = statuses
    if payment_type:
        params["payment_type"] = payment_type
    if ids:
        params["ids"] = ids
    data = get_with_retry_json(ADV_ADVERTS_URL, _auth_headers(token), params, timeout_s=45)
    if isinstance(data, dict):
        adverts = data.get("adverts") or data.get("Adverts") or []
        return adverts if isinstance(adverts, list) else []
    if isinstance(data, list):
        return data
    return []


def fetch_fullstats(
    token: str,
    campaign_ids: List[int],
    begin_date: str,
    end_date: str,
    progress_callback=None,
    soft_fail: bool = False,
) -> List[Dict[str, Any]]:
    """Статистика кампаний GET /adv/v3/fullstats (чанки по 50, с троттлингом)."""
    if not campaign_ids:
        return []
    result: List[Dict[str, Any]] = []
    ids = [int(x) for x in campaign_ids if x is not None]
    total_chunks = (len(ids) + ADV_FULLSTATS_CHUNK - 1) // ADV_FULLSTATS_CHUNK
    for chunk_idx, i in enumerate(range(0, len(ids), ADV_FULLSTATS_CHUNK), 1):
        chunk = ids[i : i + ADV_FULLSTATS_CHUNK]
        if progress_callback:
            progress_callback(
                chunk_idx,
                total_chunks,
                f"продвижение кампании {i + 1}–{i + len(chunk)} из {len(ids)}",
            )
        _throttle_fullstats()
        params = {
            "ids": ",".join(str(x) for x in chunk),
            "beginDate": begin_date,
            "endDate": end_date,
        }
        try:
            data = get_with_retry_json(
                ADV_FULLSTATS_URL, _auth_headers(token), params, max_retries=3, timeout_s=60
            )
            if isinstance(data, list):
                result.extend(data)
            elif isinstance(data, dict):
                items = data.get("data") or data.get("stats") or []
                if isinstance(items, list):
                    result.extend(items)
        except Exception as exc:
            logger.warning("fetch_fullstats chunk %s/%s failed: %s", chunk_idx, total_chunks, exc)
            if not soft_fail:
                raise
    return result


def _safe_div(num: float, den: float) -> float:
    if not den:
        return 0.0
    return num / den


def _product_meta_map(user_id: int | None = None) -> Dict[int, Dict[str, Any]]:
    """nm_id → {title, photo, vendor_code} из кэша товаров."""
    meta: Dict[int, Dict[str, Any]] = {}
    cached: Dict[str, Any] = {}
    try:
        if user_id is not None:
            from utils.cache import load_products_cache_for_user
            cached = load_products_cache_for_user(int(user_id)) or {}
        else:
            cached = load_products_cache() or {}
    except Exception as exc:
        logger.debug("product meta cache unavailable: %s", exc)
        cached = {}
    items = cached.get("items") or []
    for it in items:
        nm = it.get("nm_id") or it.get("nmId") or it.get("nmID")
        try:
            nm_i = int(nm)
        except (TypeError, ValueError):
            continue
        title = (
            it.get("title")
            or it.get("name")
            or it.get("supplier_article")
            or it.get("vendorCode")
            or ""
        )
        meta[nm_i] = {
            "title": title,
            "photo": it.get("photo") or it.get("img"),
            "vendor_code": it.get("supplier_article") or it.get("vendorCode") or "",
        }
    return meta


def _aggregate_nm_from_stats(stats_list: List[Dict[str, Any]]) -> Dict[int, Dict[str, Any]]:
    """Агрегация статистики по nmId из days → apps → nms."""
    by_nm: Dict[int, Dict[str, Any]] = {}
    pos_acc: Dict[int, List[float]] = {}

    for camp in stats_list:
        advert_id = camp.get("advertId")
        for day in camp.get("days") or []:
            for app in day.get("apps") or []:
                for nm in app.get("nms") or []:
                    nm_id = nm.get("nmId")
                    if nm_id is None:
                        continue
                    try:
                        nm_i = int(nm_id)
                    except (TypeError, ValueError):
                        continue
                    row = by_nm.get(nm_i)
                    if not row:
                        row = {
                            "nm_id": nm_i,
                            "name": nm.get("name") or "",
                            "views": 0,
                            "clicks": 0,
                            "orders": 0,
                            "sum": 0.0,
                            "sum_price": 0.0,
                            "atbs": 0,
                            "shks": 0,
                            "campaign_ids": set(),
                        }
                        by_nm[nm_i] = row
                    row["views"] += int(nm.get("views") or 0)
                    row["clicks"] += int(nm.get("clicks") or 0)
                    row["orders"] += int(nm.get("orders") or 0)
                    row["sum"] += float(nm.get("sum") or 0)
                    row["sum_price"] += float(nm.get("sum_price") or 0)
                    row["atbs"] += int(nm.get("atbs") or 0)
                    row["shks"] += int(nm.get("shks") or 0)
                    if advert_id is not None:
                        row["campaign_ids"].add(int(advert_id))
                    if nm.get("name") and not row["name"]:
                        row["name"] = nm.get("name")

        for bs in camp.get("boosterStats") or []:
            nm_raw = bs.get("nm") or bs.get("nmId")
            try:
                nm_i = int(nm_raw)
            except (TypeError, ValueError):
                continue
            pos = bs.get("avg_position")
            if pos is None:
                continue
            try:
                pos_acc.setdefault(nm_i, []).append(float(pos))
            except (TypeError, ValueError):
                pass

    for nm_i, row in by_nm.items():
        views = row["views"]
        clicks = row["clicks"]
        spent = row["sum"]
        orders_sum = row["sum_price"]
        row["ctr"] = round(_safe_div(clicks * 100.0, views), 2) if views else 0.0
        row["cost_share"] = round(_safe_div(spent * 100.0, orders_sum), 2) if orders_sum else 0.0
        row["roas"] = round(_safe_div(orders_sum, spent), 2) if spent else 0.0
        positions = pos_acc.get(nm_i) or []
        row["avg_position"] = round(sum(positions) / len(positions), 1) if positions else None
        row["campaigns_count"] = len(row["campaign_ids"])
        row["campaign_ids"] = sorted(row["campaign_ids"])

    return by_nm


def _split_period_days(date_from: str, date_to: str, max_days: int = 31) -> List[Tuple[str, str]]:
    """Разбивает период на куски не длиннее max_days (включительно)."""
    try:
        start = datetime.strptime(date_from, "%Y-%m-%d").date()
        end = datetime.strptime(date_to, "%Y-%m-%d").date()
    except Exception:
        return [(date_from, date_to)]
    out: List[Tuple[str, str]] = []
    cur = start
    while cur <= end:
        chunk_end = min(cur + timedelta(days=max_days - 1), end)
        out.append((cur.isoformat(), chunk_end.isoformat()))
        cur = chunk_end + timedelta(days=1)
    return out


def fetch_promotion_spend_by_nm(
    token: str,
    date_from: str,
    date_to: str,
    progress_callback=None,
    user_id: int | None = None,
) -> List[Dict[str, Any]]:
    """
    Затраты WB Продвижение по товарам (nmId) за период.
    Источник: GET /adv/v3/fullstats (max 31 день на запрос).
    """
    if not token:
        return []

    adverts = fetch_adverts(token, statuses="7,9,11")
    campaign_ids: List[int] = []
    for adv in adverts:
        try:
            status = int(adv.get("status") or 0)
        except (TypeError, ValueError):
            continue
        if status not in FULLSTATS_STATUSES:
            continue
        cid = adv.get("id")
        if cid is None:
            continue
        try:
            campaign_ids.append(int(cid))
        except (TypeError, ValueError):
            continue

    if not campaign_ids:
        logger.info("promotion spend: нет кампаний со статистикой")
        return []

    intervals = _split_period_days(date_from, date_to, max_days=31)
    all_stats: List[Dict[str, Any]] = []
    total = len(intervals)
    for idx, (df, dt) in enumerate(intervals, 1):
        if progress_callback:
            progress_callback(idx, total, f"продвижение {df} — {dt}")
        logger.info(
            "fullstats promotion %s/%s: %s — %s, campaigns=%s",
            idx, total, df, dt, len(campaign_ids),
        )

        def _chunk_progress(current, chunk_total, period):
            if progress_callback:
                progress_callback(current, chunk_total, period)

        chunk_stats = fetch_fullstats(
            token,
            campaign_ids,
            df,
            dt,
            progress_callback=_chunk_progress,
            soft_fail=True,
        )
        all_stats.extend(chunk_stats)

    by_nm = _aggregate_nm_from_stats(all_stats)
    product_meta = _product_meta_map(user_id=user_id)
    rows: List[Dict[str, Any]] = []
    for nm_i, row in by_nm.items():
        spend = float(row.get("sum") or 0)
        if abs(spend) < 1e-9:
            continue
        meta = product_meta.get(nm_i) or {}
        rows.append({
            "nm_id": nm_i,
            "name": meta.get("title") or row.get("name") or "",
            "vendor_code": meta.get("vendor_code") or "",
            "barcode": "",
            "sum": round(spend, 2),
            "views": int(row.get("views") or 0),
            "clicks": int(row.get("clicks") or 0),
            "orders": int(row.get("orders") or 0),
            "campaign_ids": row.get("campaign_ids") or [],
        })
    rows.sort(key=lambda x: abs(float(x.get("sum") or 0)), reverse=True)
    logger.info("promotion spend: %s товаров, sum=%.2f", len(rows), sum(r["sum"] for r in rows))
    return rows

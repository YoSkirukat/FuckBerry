# -*- coding: utf-8 -*-
"""Работа с WB Advertising / Promotion API."""
from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple, Set

from utils.api import get_with_retry_json, post_with_retry
from utils.cache import load_products_cache
from utils.constants import (
    ADV_ADVERTS_URL,
    ADV_BUDGET_MIN_INTERVAL_S,
    ADV_BUDGET_URL,
    ADV_FULLSTATS_CHUNK,
    ADV_FULLSTATS_MIN_INTERVAL_S,
    ADV_FULLSTATS_URL,
    ADV_NORMQUERY_BIDS_URL,
    ADV_NORMQUERY_MIN_INTERVAL_S,
    ADV_NORMQUERY_MINUS_URL,
    ADV_NORMQUERY_STATS_URL,
    MOSCOW_TZ,
)

logger = logging.getLogger(__name__)

STATUS_LABELS = {
    -1: "Удалена",
    4: "Готова к запуску",
    7: "Завершена",
    8: "Отменена",
    9: "Активна",
    11: "На паузе",
}

# Статусы, для которых fullstats доступен
FULLSTATS_STATUSES = {7, 9, 11}

_last_fullstats_ts: float = 0.0
_last_budget_ts: float = 0.0
_last_normquery_ts: float = 0.0


def _auth_headers(token: str) -> Dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def _throttle(kind: str, min_interval: float) -> None:
    global _last_fullstats_ts, _last_budget_ts, _last_normquery_ts
    now = time.time()
    if kind == "fullstats":
        last = _last_fullstats_ts
    elif kind == "normquery":
        last = _last_normquery_ts
    else:
        last = _last_budget_ts
    if min_interval > 0 and last > 0:
        delta = now - last
        if delta < min_interval:
            time.sleep(min_interval - delta)
    stamp = time.time()
    if kind == "fullstats":
        _last_fullstats_ts = stamp
    elif kind == "normquery":
        _last_normquery_ts = stamp
    else:
        _last_budget_ts = stamp


def default_period() -> Tuple[str, str]:
    """Период по умолчанию: последние 7 дней включительно (МСК)."""
    today = datetime.now(MOSCOW_TZ).date()
    date_to = today
    date_from = today - timedelta(days=6)
    return date_from.isoformat(), date_to.isoformat()


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
        _throttle("fullstats", ADV_FULLSTATS_MIN_INTERVAL_S)
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
                # иногда обёртка
                items = data.get("data") or data.get("stats") or []
                if isinstance(items, list):
                    result.extend(items)
        except Exception as exc:
            logger.warning("fetch_fullstats chunk %s/%s failed: %s", chunk_idx, total_chunks, exc)
            if not soft_fail:
                raise
    return result


def fetch_budget(token: str, campaign_id: int) -> Optional[float]:
    """Остаток бюджета кампании, ₽. GET /adv/v1/budget."""
    _throttle("budget", ADV_BUDGET_MIN_INTERVAL_S)
    try:
        data = get_with_retry_json(
            ADV_BUDGET_URL,
            _auth_headers(token),
            {"id": int(campaign_id)},
            max_retries=2,
            timeout_s=20,
        )
        if isinstance(data, dict):
            total = data.get("total")
            if total is not None:
                return float(total)
    except Exception as exc:
        logger.debug("fetch_budget %s failed: %s", campaign_id, exc)
    return None


def fetch_budgets(token: str, campaign_ids: List[int], limit: int = 40) -> Dict[int, float]:
    """Бюджеты для ограниченного числа кампаний (rate-limit)."""
    out: Dict[int, float] = {}
    for cid in campaign_ids[:limit]:
        val = fetch_budget(token, cid)
        if val is not None:
            out[int(cid)] = val
    return out


def _bid_type_label(bid_type: str | None) -> str:
    bt = (bid_type or "").lower()
    if bt in ("manual",):
        return "Ручная"
    if bt in ("unified", "auto"):
        return "Единая"
    return bid_type or "—"


def _payment_label(payment_type: str | None) -> str:
    pt = (payment_type or "").lower()
    if pt == "cpm":
        return "CPM"
    if pt == "cpc":
        return "CPC"
    return (payment_type or "—").upper()


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
    """Агрегация статистики по nmId из days → apps → nms + средняя позиция."""
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
        logging_msg = f"fullstats promotion {idx}/{total}: {df} — {dt}, campaigns={len(campaign_ids)}"
        logger.info(logging_msg)

        def _chunk_progress(current, chunk_total, period):
            if progress_callback:
                # current/total по чанкам внутри периода
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


def build_marketing_payload(
    token: str,
    date_from: str,
    date_to: str,
    include_budgets: bool = True,
) -> Dict[str, Any]:
    """Собирает кампании, статистику, товары и сводные KPI за период."""
    adverts_raw = fetch_adverts(token, statuses="4,7,9,11")

    campaigns: List[Dict[str, Any]] = []
    stats_eligible_ids: List[int] = []
    budget_ids: List[int] = []

    for adv in adverts_raw:
        cid = adv.get("id")
        if cid is None:
            continue
        try:
            cid_i = int(cid)
        except (TypeError, ValueError):
            continue
        status = int(adv.get("status") or 0)
        settings = adv.get("settings") or {}
        nm_settings = adv.get("nm_settings") or []
        nm_ids = []
        for ns in nm_settings:
            n = ns.get("nm_id") or ns.get("nmId")
            if n is not None:
                try:
                    nm_ids.append(int(n))
                except (TypeError, ValueError):
                    pass

        payment = settings.get("payment_type")
        bid_type = adv.get("bid_type")
        camp = {
            "id": cid_i,
            "name": settings.get("name") or f"Кампания {cid_i}",
            "status": status,
            "status_label": STATUS_LABELS.get(status, str(status)),
            "payment_type": payment,
            "payment_label": _payment_label(payment),
            "bid_type": bid_type,
            "bid_type_label": _bid_type_label(bid_type),
            "nm_ids": nm_ids,
            "nm_count": len(nm_ids),
            "photo_nm_id": nm_ids[0] if nm_ids else None,
            "created": (adv.get("timestamps") or {}).get("created"),
            "budget": None,
            "views": 0,
            "clicks": 0,
            "orders": 0,
            "sum": 0.0,
            "sum_price": 0.0,
            "ctr": 0.0,
            "cost_share": 0.0,
            "roas": 0.0,
        }
        campaigns.append(camp)
        if status in FULLSTATS_STATUSES:
            stats_eligible_ids.append(cid_i)
        if status in (9, 11, 4):
            budget_ids.append(cid_i)

    # Сортировка: активные → пауза → остальные, затем по id desc
    status_order = {9: 0, 11: 1, 4: 2, 7: 3, 8: 4, -1: 5}
    campaigns.sort(key=lambda c: (status_order.get(c["status"], 9), -c["id"]))

    stats_list = fetch_fullstats(token, stats_eligible_ids, date_from, date_to)
    stats_by_id = {}
    for s in stats_list:
        aid = s.get("advertId")
        if aid is not None:
            stats_by_id[int(aid)] = s

    budgets: Dict[int, float] = {}
    if include_budgets and budget_ids:
        try:
            budgets = fetch_budgets(token, budget_ids)
        except Exception as exc:
            logger.warning("budgets fetch failed: %s", exc)

    product_meta = _product_meta_map()

    totals = {
        "sum": 0.0,
        "sum_price": 0.0,
        "views": 0,
        "clicks": 0,
        "orders": 0,
    }

    for camp in campaigns:
        st = stats_by_id.get(camp["id"])
        if st:
            camp["views"] = int(st.get("views") or 0)
            camp["clicks"] = int(st.get("clicks") or 0)
            camp["orders"] = int(st.get("orders") or 0)
            camp["sum"] = float(st.get("sum") or 0)
            camp["sum_price"] = float(st.get("sum_price") or 0)
            camp["ctr"] = float(st.get("ctr") or 0)
            if not camp["ctr"] and camp["views"]:
                camp["ctr"] = round(_safe_div(camp["clicks"] * 100.0, camp["views"]), 2)
            camp["cost_share"] = (
                round(_safe_div(camp["sum"] * 100.0, camp["sum_price"]), 2)
                if camp["sum_price"]
                else 0.0
            )
            camp["roas"] = (
                round(_safe_div(camp["sum_price"], camp["sum"]), 2) if camp["sum"] else 0.0
            )
            totals["sum"] += camp["sum"]
            totals["sum_price"] += camp["sum_price"]
            totals["views"] += camp["views"]
            totals["clicks"] += camp["clicks"]
            totals["orders"] += camp["orders"]

        if camp["id"] in budgets:
            camp["budget"] = budgets[camp["id"]]

        # Превью товара
        pnm = camp.get("photo_nm_id")
        if pnm and pnm in product_meta:
            camp["photo"] = product_meta[pnm].get("photo")
            camp["product_title"] = product_meta[pnm].get("title")
        else:
            camp["photo"] = None
            camp["product_title"] = None

    summary = {
        "sum_price": round(totals["sum_price"], 2),
        "sum": round(totals["sum"], 2),
        "cost_share": round(_safe_div(totals["sum"] * 100.0, totals["sum_price"]), 2)
        if totals["sum_price"]
        else 0.0,
        "roas": round(_safe_div(totals["sum_price"], totals["sum"]), 2) if totals["sum"] else 0.0,
        "ctr": round(_safe_div(totals["clicks"] * 100.0, totals["views"]), 2)
        if totals["views"]
        else 0.0,
        "views": totals["views"],
        "clicks": totals["clicks"],
        "orders": totals["orders"],
        "campaigns_count": len(campaigns),
    }

    nm_agg = _aggregate_nm_from_stats(stats_list)
    products: List[Dict[str, Any]] = []
    for nm_i, row in nm_agg.items():
        meta = product_meta.get(nm_i) or {}
        products.append(
            {
                "nm_id": nm_i,
                "name": meta.get("title") or row.get("name") or f"Товар {nm_i}",
                "vendor_code": meta.get("vendor_code") or "",
                "photo": meta.get("photo"),
                "views": row["views"],
                "clicks": row["clicks"],
                "orders": row["orders"],
                "sum": round(row["sum"], 2),
                "sum_price": round(row["sum_price"], 2),
                "ctr": row["ctr"],
                "cost_share": row["cost_share"],
                "roas": row["roas"],
                "avg_position": row["avg_position"],
                "campaigns_count": row["campaigns_count"],
            }
        )
    products.sort(key=lambda p: (-p["sum"], -p["orders"]))

    return {
        "date_from": date_from,
        "date_to": date_to,
        "summary": summary,
        "campaigns": campaigns,
        "products": products,
    }


def _post_json(token: str, url: str, body: Dict[str, Any], timeout_s: int = 45) -> Any:
    resp = post_with_retry(url, _auth_headers(token), body, max_retries=3)
    try:
        return resp.json()
    except Exception:
        return None


def _nm_items_for_campaign(advert_id: int, nm_ids: List[int]) -> List[Dict[str, int]]:
    ids = nm_ids or [0]
    return [{"advert_id": int(advert_id), "nm_id": int(nm)} for nm in ids]


def fetch_normquery_stats(
    token: str,
    advert_id: int,
    nm_ids: List[int],
    date_from: str,
    date_to: str,
) -> List[Dict[str, Any]]:
    """Статистика поисковых кластеров POST /adv/v0/normquery/stats."""
    items = _nm_items_for_campaign(advert_id, nm_ids)
    if not items:
        return []
    _throttle("normquery", ADV_NORMQUERY_MIN_INTERVAL_S)
    body = {"from": date_from, "to": date_to, "items": items[:100]}
    try:
        data = _post_json(token, ADV_NORMQUERY_STATS_URL, body)
    except Exception as exc:
        logger.warning("normquery stats failed: %s", exc)
        return []
    if isinstance(data, dict):
        return data.get("stats") or []
    return []


def fetch_normquery_bids(token: str, advert_id: int, nm_ids: List[int]) -> List[Dict[str, Any]]:
    """Ставки по кластерам POST /adv/v0/normquery/get-bids."""
    items = _nm_items_for_campaign(advert_id, nm_ids)
    if not items:
        return []
    try:
        data = _post_json(token, ADV_NORMQUERY_BIDS_URL, {"items": items[:100]})
    except Exception as exc:
        logger.warning("normquery bids failed: %s", exc)
        return []
    if isinstance(data, dict):
        return data.get("bids") or []
    return []


def fetch_normquery_minus(token: str, advert_id: int, nm_ids: List[int]) -> Dict[int, List[str]]:
    """Минус-фразы POST /adv/v0/normquery/get-minus → nm_id → list."""
    items = _nm_items_for_campaign(advert_id, [n for n in nm_ids if n])
    if not items:
        return {}
    try:
        data = _post_json(token, ADV_NORMQUERY_MINUS_URL, {"items": items[:100]})
    except Exception as exc:
        logger.warning("normquery minus failed: %s", exc)
        return {}
    out: Dict[int, List[str]] = {}
    rows = (data or {}).get("items") if isinstance(data, dict) else None
    if not isinstance(rows, list):
        return out
    for row in rows:
        try:
            nm_i = int(row.get("nm_id"))
        except (TypeError, ValueError):
            continue
        phrases = row.get("norm_queries") or []
        if isinstance(phrases, list):
            out[nm_i] = [str(p) for p in phrases if p]
    return out


def _fmt_created(iso: str | None) -> str:
    if not iso:
        return ""
    try:
        # 2025-07-29T12:00:00Z or with offset
        raw = iso.replace("Z", "+00:00")
        dt = datetime.fromisoformat(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=MOSCOW_TZ)
        else:
            dt = dt.astimezone(MOSCOW_TZ)
        return dt.strftime("%d.%m.%Y")
    except Exception:
        return iso[:10]


def _kopecks_to_rub(val: Any) -> Optional[float]:
    if val is None:
        return None
    try:
        return round(float(val) / 100.0, 2)
    except (TypeError, ValueError):
        return None


def build_campaign_detail(
    token: str,
    campaign_id: int,
    date_from: str,
    date_to: str,
) -> Dict[str, Any]:
    """Детальная карточка кампании: KPI, товары, поисковые фразы."""
    adverts = fetch_adverts(token, statuses=None, ids=str(int(campaign_id)))
    if not adverts:
        # fallback без фильтра статусов — пробуем типичные
        adverts = fetch_adverts(token, statuses="4,7,8,9,11", ids=str(int(campaign_id)))
    if not adverts:
        raise ValueError("campaign_not_found")

    adv = adverts[0]
    settings = adv.get("settings") or {}
    timestamps = adv.get("timestamps") or {}
    status = int(adv.get("status") or 0)
    payment = settings.get("payment_type")
    bid_type = adv.get("bid_type")
    nm_settings = adv.get("nm_settings") or []

    nm_ids: List[int] = []
    product_bids: Dict[int, Dict[str, Any]] = {}
    for ns in nm_settings:
        n = ns.get("nm_id") or ns.get("nmId")
        if n is None:
            continue
        try:
            nm_i = int(n)
        except (TypeError, ValueError):
            continue
        nm_ids.append(nm_i)
        bids_k = ns.get("bids_kopecks") or {}
        product_bids[nm_i] = {
            "search": _kopecks_to_rub(bids_k.get("search")),
            "recommendations": _kopecks_to_rub(bids_k.get("recommendations")),
        }

    budget = fetch_budget(token, campaign_id)

    stats_list: List[Dict[str, Any]] = []
    if status in FULLSTATS_STATUSES:
        try:
            stats_list = fetch_fullstats(token, [campaign_id], date_from, date_to)
        except Exception as exc:
            logger.warning("campaign fullstats failed: %s", exc)

    st = stats_list[0] if stats_list else {}
    views = int(st.get("views") or 0)
    clicks = int(st.get("clicks") or 0)
    orders = int(st.get("orders") or 0)
    spent = float(st.get("sum") or 0)
    sum_price = float(st.get("sum_price") or 0)
    ctr = float(st.get("ctr") or 0) or (
        round(_safe_div(clicks * 100.0, views), 2) if views else 0.0
    )
    cpc = float(st.get("cpc") or 0) or (
        round(_safe_div(spent, clicks), 2) if clicks else 0.0
    )
    cr = float(st.get("cr") or 0) or (
        round(_safe_div(orders * 100.0, clicks), 2) if clicks else 0.0
    )
    cost_share = round(_safe_div(spent * 100.0, sum_price), 2) if sum_price else 0.0
    roas = round(_safe_div(sum_price, spent), 2) if spent else 0.0

    summary = {
        "views": views,
        "clicks": clicks,
        "orders": orders,
        "sum_price": round(sum_price, 2),
        "sum": round(spent, 2),
        "ctr": round(ctr, 2),
        "cpc": round(cpc, 2),
        "cr": round(cr, 2),
        "cost_share": cost_share,
        "roas": roas,
        "atbs": int(st.get("atbs") or 0),
    }

    product_meta = _product_meta_map()
    nm_agg = _aggregate_nm_from_stats(stats_list)

    products: List[Dict[str, Any]] = []
    seen_nm = set()
    for nm_i in nm_ids:
        seen_nm.add(nm_i)
        meta = product_meta.get(nm_i) or {}
        row = nm_agg.get(nm_i) or {}
        pb = product_bids.get(nm_i) or {}
        products.append(
            {
                "nm_id": nm_i,
                "name": meta.get("title") or row.get("name") or f"Товар {nm_i}",
                "vendor_code": meta.get("vendor_code") or "",
                "photo": meta.get("photo"),
                "in_ads": True,
                "views": row.get("views") or 0,
                "clicks": row.get("clicks") or 0,
                "orders": row.get("orders") or 0,
                "sum": round(float(row.get("sum") or 0), 2),
                "sum_price": round(float(row.get("sum_price") or 0), 2),
                "ctr": row.get("ctr") or 0,
                "avg_position": row.get("avg_position"),
                "bid_search": pb.get("search"),
                "bid_recommendations": pb.get("recommendations"),
            }
        )
    for nm_i, row in nm_agg.items():
        if nm_i in seen_nm:
            continue
        meta = product_meta.get(nm_i) or {}
        products.append(
            {
                "nm_id": nm_i,
                "name": meta.get("title") or row.get("name") or f"Товар {nm_i}",
                "vendor_code": meta.get("vendor_code") or "",
                "photo": meta.get("photo"),
                "in_ads": False,
                "views": row.get("views") or 0,
                "clicks": row.get("clicks") or 0,
                "orders": row.get("orders") or 0,
                "sum": round(float(row.get("sum") or 0), 2),
                "sum_price": round(float(row.get("sum_price") or 0), 2),
                "ctr": row.get("ctr") or 0,
                "avg_position": row.get("avg_position"),
                "bid_search": None,
                "bid_recommendations": None,
            }
        )

    # Поисковые фразы
    phrases: List[Dict[str, Any]] = []
    try:
        stats_nq = fetch_normquery_stats(token, campaign_id, nm_ids or [0], date_from, date_to)
    except Exception:
        stats_nq = []
    try:
        bids_nq = fetch_normquery_bids(token, campaign_id, nm_ids or [0])
    except Exception:
        bids_nq = []
    try:
        minus_map = fetch_normquery_minus(token, campaign_id, nm_ids)
    except Exception:
        minus_map = {}

    bid_map: Dict[Tuple[int, str], float] = {}
    for b in bids_nq:
        try:
            nm_i = int(b.get("nm_id"))
        except (TypeError, ValueError):
            continue
        q = (b.get("norm_query") or "").strip()
        if not q:
            continue
        rub = _kopecks_to_rub(b.get("bid"))
        if rub is not None:
            bid_map[(nm_i, q.lower())] = rub

    minus_set: Set[Tuple[int, str]] = set()
    for nm_i, plist in minus_map.items():
        for p in plist:
            minus_set.add((nm_i, p.strip().lower()))

    for block in stats_nq:
        try:
            nm_i = int(block.get("nm_id"))
        except (TypeError, ValueError):
            nm_i = 0
        for cluster in block.get("stats") or []:
            q = (cluster.get("norm_query") or "").strip()
            if not q:
                continue
            views_q = cluster.get("views")
            clicks_q = int(cluster.get("clicks") or 0)
            orders_q = int(cluster.get("orders") or 0)
            ctr_q = cluster.get("ctr")
            if ctr_q is None and views_q:
                ctr_q = round(_safe_div(clicks_q * 100.0, float(views_q)), 2)
            bid_rub = bid_map.get((nm_i, q.lower()))
            is_minus = (nm_i, q.lower()) in minus_set
            phrases.append(
                {
                    "norm_query": q,
                    "nm_id": nm_i,
                    "bid": bid_rub,
                    "avg_pos": cluster.get("avg_pos"),
                    "views": views_q if views_q is not None else None,
                    "clicks": clicks_q,
                    "ctr": float(ctr_q) if ctr_q is not None else None,
                    "cpc": float(cluster["cpc"]) if cluster.get("cpc") is not None else None,
                    "cpm": float(cluster["cpm"]) if cluster.get("cpm") is not None else None,
                    "orders": orders_q,
                    "atbs": int(cluster.get("atbs") or 0),
                    "is_minus": is_minus,
                    "has_fixed_bid": bid_rub is not None,
                }
            )

    # Фразы только со ставкой, без статистики
    seen_q = {(p["nm_id"], p["norm_query"].lower()) for p in phrases}
    for (nm_i, q_low), rub in bid_map.items():
        # восстановить оригинальный регистр из bids
        orig = next(
            (b.get("norm_query") for b in bids_nq
             if int(b.get("nm_id") or -1) == nm_i
             and (b.get("norm_query") or "").strip().lower() == q_low),
            q_low,
        )
        key = (nm_i, q_low)
        if key in seen_q:
            continue
        phrases.append(
            {
                "norm_query": orig,
                "nm_id": nm_i,
                "bid": rub,
                "avg_pos": None,
                "views": None,
                "clicks": 0,
                "ctr": None,
                "cpc": None,
                "cpm": None,
                "orders": 0,
                "atbs": 0,
                "is_minus": key in minus_set,
                "has_fixed_bid": True,
            }
        )

    # Минус-фразы без статистики
    for nm_i, plist in minus_map.items():
        for p in plist:
            key = (nm_i, p.strip().lower())
            if any(
                x["nm_id"] == nm_i and x["norm_query"].strip().lower() == key[1]
                for x in phrases
            ):
                continue
            phrases.append(
                {
                    "norm_query": p,
                    "nm_id": nm_i,
                    "bid": None,
                    "avg_pos": None,
                    "views": None,
                    "clicks": 0,
                    "ctr": None,
                    "cpc": None,
                    "cpm": None,
                    "orders": 0,
                    "atbs": 0,
                    "is_minus": True,
                    "has_fixed_bid": False,
                }
            )

    phrases.sort(
        key=lambda p: (
            -(p["views"] or 0),
            -(p["clicks"] or 0),
            (p["norm_query"] or "").lower(),
        )
    )

    placements = settings.get("placements") or {}
    campaign = {
        "id": int(campaign_id),
        "name": settings.get("name") or f"Кампания {campaign_id}",
        "status": status,
        "status_label": STATUS_LABELS.get(status, str(status)),
        "payment_type": payment,
        "payment_label": _payment_label(payment),
        "bid_type": bid_type,
        "bid_type_label": _bid_type_label(bid_type),
        "created": timestamps.get("created"),
        "created_fmt": _fmt_created(timestamps.get("created")),
        "started": timestamps.get("started"),
        "budget": budget,
        "placements": {
            "search": bool(placements.get("search")),
            "recommendations": bool(placements.get("recommendations")),
        },
        "nm_count": len(nm_ids),
    }

    return {
        "date_from": date_from,
        "date_to": date_to,
        "campaign": campaign,
        "summary": summary,
        "products": products,
        "phrases": phrases,
        "minus_count": sum(len(v) for v in minus_map.values()),
    }

# -*- coding: utf-8 -*-
"""Функции для работы с API Wildberries"""
import time
import random
import logging
import threading
from typing import Dict, Any, List, Callable, Optional
from datetime import datetime, timedelta
import requests
from utils.constants import (
    MOSCOW_TZ, API_URL, SALES_API_URL, FIN_REPORT_URL,
    PAID_STORAGE_CREATE_URL, PAID_STORAGE_STATUS_URL, PAID_STORAGE_DOWNLOAD_URL,
    PAID_STORAGE_MAX_DAYS, PAID_STORAGE_CREATE_MIN_INTERVAL_S,
    PAID_STORAGE_STATUS_POLL_S, PAID_STORAGE_STATUS_MAX_WAIT_S,
    WB_ORDERS_FETCH_MAX_PAGES, WB_ORDERS_FETCH_MAX_PAGES_INTRADAY,
    WB_ORDERS_PAGE_SLEEP_S, WB_ORDERS_PAGE_SLEEP_INTRADAY_S,
    FBW_SUPPLIES_LIST_URL, FBW_SUPPLY_DETAILS_URL, FBW_SUPPLY_GOODS_URL, FBW_SUPPLY_PACKAGE_URL,
    TRANSIT_TARIFFS_URL,
    FBS_NEW_URL, FBS_ORDERS_URL, FBS_ORDERS_STATUS_URL,
    DBS_NEW_URL, DBS_STATUS_URL, DBS_ORDERS_URL,
    SELLER_INFO_URL, ACCEPT_COEFS_URL,
    FBS_WAREHOUSES_URL, WB_OFFICES_URL, FBS_STOCKS_BY_WAREHOUSE_URL, SUPPLIES_WAREHOUSES_URL,
    STOCKS_API_URL, STOCKS_API_PAGE_LIMIT, STOCKS_API_MIN_INTERVAL_S, WB_CARDS_LIST_URL,
    SUPPLIES_API_MIN_INTERVAL_S, DISCOUNTS_PRICES_API_URL,
    COMMISSION_API_URL, DIMENSIONS_API_URL, WAREHOUSES_API_URL
)
from utils.helpers import parse_date, parse_wb_datetime, _parse_iso_datetime, to_moscow, _fmt_dt_moscow, _fbw_status_from_id

logger = logging.getLogger(__name__)

# Размер страницы финотчёта: 100k одним ответом слишком долго качается без прогресса
FIN_REPORT_PAGE_LIMIT = 10000

# --- Throttling for WB supplies API ---
_last_supplies_api_call_ts: float = 0.0


def supplies_api_throttle() -> None:
    """Ensure at most ~30 req/min (min interval ~2s) across supplies endpoints."""
    global _last_supplies_api_call_ts
    if SUPPLIES_API_MIN_INTERVAL_S <= 0:
        return
    now = time.time()
    delta = now - _last_supplies_api_call_ts
    if delta < SUPPLIES_API_MIN_INTERVAL_S:
        time.sleep(SUPPLIES_API_MIN_INTERVAL_S - delta)
    _last_supplies_api_call_ts = time.time()


def _with_progress_heartbeat(
    progress_callback: Optional[Callable],
    current: int,
    total: int,
    base_period: str,
    fn: Callable,
    interval_s: float = 2.0,
):
    """Пока выполняется fn(), обновляет progress «ожидание ответа WB (N с)»."""
    if not progress_callback:
        return fn()
    stop = threading.Event()
    started = time.time()

    def _beat() -> None:
        while not stop.wait(interval_s):
            elapsed = int(time.time() - started)
            try:
                progress_callback(
                    current,
                    total,
                    f"{base_period} · ожидание ответа WB ({elapsed} с)",
                )
            except Exception:
                pass

    thread = threading.Thread(target=_beat, daemon=True)
    thread.start()
    try:
        return fn()
    finally:
        stop.set()


def get_with_retry(url: str, headers: Dict[str, str], params: Dict[str, Any], max_retries: int = 3, timeout_s: int = 30) -> requests.Response:
    """GET запрос с повторными попытками при ошибках"""
    last_exc: Exception | None = None
    last_resp: requests.Response | None = None
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=timeout_s)
            last_resp = resp
            if resp.status_code in (429, 500, 502, 503, 504):
                sleep_s = None
                if resp.status_code == 429:
                    # Для ошибки 429 проверяем заголовки X-Ratelimit-Retry (WB API) и Retry-After (стандартный)
                    retry_header = resp.headers.get("X-Ratelimit-Retry") or resp.headers.get("Retry-After")
                    if retry_header is not None:
                        try:
                            sleep_s = float(retry_header)
                        except ValueError:
                            sleep_s = None
                
                if sleep_s is None:
                    # Если заголовков нет, используем экспоненциальную задержку
                    if resp.status_code == 429:
                        sleep_s = min(60, 15 * (attempt + 1))
                    else:
                        sleep_s = min(15, 0.8 * (2 ** attempt) + random.uniform(0, 0.7))
                # Не даём Retry-After увести поток в многоминутный сон без прогресса
                sleep_s = min(60.0, max(0.5, float(sleep_s)))
                
                time.sleep(sleep_s)
                continue
            resp.raise_for_status()
            return resp
        except requests.RequestException as exc:  # network or HTTP error
            last_exc = exc
            time.sleep(min(8, 0.5 * (2 ** attempt) + random.uniform(0, 0.5)))
            continue
    if last_exc:
        raise last_exc
    if last_resp is not None:
        raise requests.HTTPError(f"HTTP {last_resp.status_code} after {max_retries} retries", response=last_resp)
    raise RuntimeError("Request failed after retries")


def get_with_retry_json(url: str, headers: Dict[str, str], params: Dict[str, Any], max_retries: int = 3, timeout_s: int = 30) -> Any:
    """GET запрос с повторными попытками, возвращает JSON"""
    resp = get_with_retry(url, headers, params, max_retries=max_retries, timeout_s=timeout_s)
    try:
        return resp.json()
    except Exception:
        raise RuntimeError("Invalid JSON from API")


def post_with_retry(url: str, headers: Dict[str, str], json_body: Dict[str, Any], max_retries: int = 3) -> requests.Response:
    """POST запрос с повторными попытками при ошибках"""
    last_exc: Exception | None = None
    last_resp: requests.Response | None = None
    for attempt in range(max_retries):
        try:
            resp = requests.post(url, headers=headers, json=json_body, timeout=30)
            last_resp = resp
            if resp.status_code in (429, 500, 502, 503, 504):
                retry_after = resp.headers.get("Retry-After")
                if retry_after is not None:
                    try:
                        sleep_s = float(retry_after)
                    except ValueError:
                        sleep_s = 1.0
                else:
                    sleep_s = min(15, 0.8 * (2 ** attempt) + random.uniform(0, 0.7))
                time.sleep(sleep_s)
                continue
            resp.raise_for_status()
            return resp
        except requests.RequestException as exc:
            last_exc = exc
            time.sleep(min(8, 0.5 * (2 ** attempt) + random.uniform(0, 0.5)))
            continue
    if last_exc:
        raise last_exc
    if last_resp is not None:
        raise requests.HTTPError(f"HTTP {last_resp.status_code} after {max_retries} retries", response=last_resp)
    raise RuntimeError("Request failed after retries")


# --- Orders API ---
def fetch_orders_page(token: str, date_from_iso: str, flag: int = 0) -> List[Dict[str, Any]]:
    """Получает одну страницу заказов"""
    headers = {"Authorization": f"Bearer {token}"}
    params = {"dateFrom": date_from_iso, "flag": flag}
    response = get_with_retry(API_URL, headers, params)
    return response.json()


def fetch_orders_range(token: str, start_date: str, end_date: str, days_back: int = 1) -> List[Dict[str, Any]]:
    """Получает заказы за период с пагинацией
    
    Args:
        token: API токен
        start_date: Начальная дата (YYYY-MM-DD)
        end_date: Конечная дата (YYYY-MM-DD)
        days_back: Количество дней назад от start_date для начала загрузки (по умолчанию 1 день)
    """
    start_dt = parse_date(start_date)
    end_dt = parse_date(end_date)

    # Загружаем данные с небольшим запасом (по умолчанию 1 день) для захвата заказов, 
    # которые могли быть обновлены позже. Для отчетов используем минимальный запас.
    extended_start = start_dt - timedelta(days=days_back)
    cursor_dt = datetime.combine(extended_start.date(), datetime.min.time())

    collected: List[Dict[str, Any]] = []
    seen_srid: set[str] = set()

    pages = 0
    # Для прошлых дат оставляем «+1 календарный день» к end: иначе можно обрезать заказы с
    # отложенным lastChangeDate. Если end — сегодня (или позже), требование «> end+1 день»
    # почти никогда не наступает в тот же день → пагинация уходит в сотни/тысячи страниц.
    now_date = datetime.now(MOSCOW_TZ).date()
    if end_dt.date() < now_date:
        lcd_stop_date = end_dt.date() + timedelta(days=1)
    else:
        lcd_stop_date = end_dt.date()

    span_inclusive = (end_dt.date() - start_dt.date()).days + 1
    # Окно 1–2 календарных дня с концом «сегодня»: ограничиваем страницы (полнота vs время).
    intraday_short = end_dt.date() >= now_date and span_inclusive <= 2
    if intraday_short:
        max_pages = min(WB_ORDERS_FETCH_MAX_PAGES, WB_ORDERS_FETCH_MAX_PAGES_INTRADAY)
        page_sleep_s = WB_ORDERS_PAGE_SLEEP_INTRADAY_S
    else:
        max_pages = WB_ORDERS_FETCH_MAX_PAGES
        page_sleep_s = WB_ORDERS_PAGE_SLEEP_S

    while pages < max_pages:
        pages += 1
        before_cursor = cursor_dt
        page = fetch_orders_page(token, cursor_dt.strftime("%Y-%m-%dT%H:%M:%S"), flag=0)
        if not page:
            break
        try:
            page.sort(key=lambda x: parse_wb_datetime(x.get("lastChangeDate")) or datetime.min)
        except Exception:
            pass

        last_page_lcd: datetime | None = parse_wb_datetime(page[-1].get("lastChangeDate"))
        page_exceeds = bool(last_page_lcd and last_page_lcd.date() > lcd_stop_date)

        for item in page:
            srid = str(item.get("srid", ""))
            if srid and srid in seen_srid:
                continue
            # Убираем фильтрацию по lastChangeDate здесь - будем фильтровать по date в to_rows
            if srid:
                seen_srid.add(srid)
            collected.append(item)

        if last_page_lcd is None:
            break
        # Тот же lastChangeDate на новом запросе → зацикливание
        if pages > 1 and last_page_lcd <= before_cursor:
            logger.warning(
                "fetch_orders_range: lastChangeDate did not advance (cursor stall), stopping after %s pages",
                pages,
            )
            break
        cursor_dt = last_page_lcd
        if page_exceeds:
            break
        # Gentle delay between pages to avoid throttling
        if page_sleep_s > 0:
            time.sleep(page_sleep_s)

    if pages >= max_pages:
        logger.warning(
            "fetch_orders_range: stopped at max_pages=%s (start=%s end=%s, intraday_cap=%s)",
            max_pages,
            start_date,
            end_date,
            end_dt.date() >= now_date and span_inclusive <= 2,
        )

    return collected


# --- Sales API ---
def fetch_sales_page(token: str, date_from_iso: str, flag: int = 0) -> List[Dict[str, Any]]:
    """Получает одну страницу продаж"""
    headers = {"Authorization": f"Bearer {token}"}
    params = {"dateFrom": date_from_iso, "flag": flag}
    response = get_with_retry(SALES_API_URL, headers, params)
    return response.json()


def fetch_sales_range(token: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
    """Получает продажи за период с пагинацией"""
    start_dt = parse_date(start_date)
    end_dt = parse_date(end_date)
    cursor_dt = datetime.combine(start_dt.date(), datetime.min.time())

    collected: List[Dict[str, Any]] = []
    seen_id: set[str] = set()

    max_pages = 2000
    pages = 0
    while pages < max_pages:
        pages += 1
        page = fetch_sales_page(token, cursor_dt.strftime("%Y-%m-%dT%H:%M:%S"), flag=0)
        if not page:
            break
        try:
            page.sort(key=lambda x: parse_wb_datetime(x.get("lastChangeDate")) or datetime.min)
        except Exception:
            pass

        last_page_lcd: datetime | None = parse_wb_datetime(page[-1].get("lastChangeDate"))
        page_exceeds = last_page_lcd and last_page_lcd.date() > end_dt.date()

        for item in page:
            key = str(item.get("srid")) or f"{item.get('gNumber','')}_{item.get('barcode','')}_{item.get('date','')}"
            if key and key in seen_id:
                continue
            lcd = parse_wb_datetime(item.get("lastChangeDate"))
            if lcd and lcd.date() > end_dt.date():
                continue
            if key:
                seen_id.add(key)
            collected.append(item)

        if last_page_lcd is None:
            break
        cursor_dt = last_page_lcd
        if page_exceeds:
            break
        # Gentle delay between pages
        time.sleep(0.2)

    return collected


# --- Finance Report API ---
def _split_date_range(date_from: str, date_to: str, days_per_chunk: int = 7) -> List[tuple[str, str]]:
    """Разбивает период на интервалы по указанному количеству дней."""
    try:
        start = datetime.strptime(date_from, "%Y-%m-%d").date()
        end = datetime.strptime(date_to, "%Y-%m-%d").date()
    except Exception:
        return [(date_from, date_to)]
    
    intervals = []
    current_start = start
    
    while current_start <= end:
        current_end = min(current_start + timedelta(days=days_per_chunk - 1), end)
        intervals.append((
            current_start.strftime("%Y-%m-%d"),
            current_end.strftime("%Y-%m-%d")
        ))
        current_start = current_end + timedelta(days=1)
    
    return intervals


def fetch_finance_report(token: str, date_from: str, date_to: str, limit: int = 100000, progress_callback=None) -> List[Dict[str, Any]]:
    """Получает финансовый отчет с разбивкой по интервалам"""
    headers = {"Authorization": f"Bearer {token}"}
    
    # Разбиваем период на интервалы по 7 дней
    intervals = _split_date_range(date_from, date_to, days_per_chunk=7)
    total_intervals = len(intervals)
    all_rows: List[Dict[str, Any]] = []
    
    logging.info(f"Начинаем загрузку финансового отчета за период {date_from} - {date_to}, интервалов: {total_intervals}")
    failed_intervals: List[str] = []
    for idx, (interval_from, interval_to) in enumerate(intervals, 1):
        logging.info(f"Загрузка интервала {idx}/{total_intervals}: {interval_from} - {interval_to}")
        
        # Вызываем callback для обновления прогресса
        if progress_callback:
            progress_callback(idx, total_intervals, f"{interval_from} - {interval_to}")
        
        # Compose RFC3339-like dateFrom in MSK start of day
        try:
            df_iso = datetime.strptime(interval_from, "%Y-%m-%d").strftime("%Y-%m-%dT00:00:00")
        except Exception:
            df_iso = f"{interval_from}T00:00:00"
        
        page_limit = max(1, min(FIN_REPORT_PAGE_LIMIT, int(limit or FIN_REPORT_PAGE_LIMIT)))
        params_base: Dict[str, Any] = {
            "dateFrom": df_iso,
            "dateTo": interval_to,
            "limit": page_limit,
        }
        interval_rows: List[Dict[str, Any]] = []
        rrdid = 0
        interval_error = None
        page_count = 0
        base_period = f"{interval_from} - {interval_to}"
        
        while True:
            page_count += 1
            params = dict(params_base)
            params["rrdid"] = rrdid
            if progress_callback:
                progress_callback(
                    idx,
                    total_intervals,
                    f"{base_period} · стр. {page_count} ({len(interval_rows)} зап.)",
                )
            try:
                resp = _with_progress_heartbeat(
                    progress_callback,
                    idx,
                    total_intervals,
                    f"{base_period} · стр. {page_count}",
                    lambda p=dict(params): get_with_retry(
                        FIN_REPORT_URL, headers, p, max_retries=3, timeout_s=60
                    ),
                )                
                # Проверяем, что ответ не пустой и имеет правильный Content-Type
                if not resp.text or not resp.text.strip():
                    logging.warning(f"Пустой ответ от API для интервала {interval_from} - {interval_to}, страница {page_count} (rrdid={rrdid})")
                    if rrdid == 0:
                        logging.error(f"Пропускаем интервал {interval_from} - {interval_to} из-за пустого ответа при первой загрузке")
                        interval_error = "Empty response from API"
                        break
                    time.sleep(2)
                    continue
                
                # Проверяем Content-Type
                content_type = resp.headers.get('Content-Type', '').lower()
                if 'application/json' not in content_type and 'text/json' not in content_type:
                    logging.warning(f"Неожиданный Content-Type для интервала {interval_from} - {interval_to}: {content_type}, первые 200 символов: {resp.text[:200]}")
                    if rrdid == 0:
                        logging.error(f"Пропускаем интервал {interval_from} - {interval_to} из-за неверного Content-Type")
                        interval_error = f"Invalid Content-Type: {content_type}"
                        break
                    time.sleep(2)
                    continue
                
                # Пытаемся распарсить JSON
                try:
                    data = resp.json()
                except ValueError as json_err:
                    logging.warning(f"Ошибка парсинга JSON для интервала {interval_from} - {interval_to}, страница {page_count} (rrdid={rrdid}): {json_err}")
                    logging.warning(f"Статус ответа: {resp.status_code}, Content-Type: {content_type}, первые 500 символов: {resp.text[:500]}")
                    if rrdid == 0:
                        logging.error(f"Пропускаем интервал {interval_from} - {interval_to} из-за ошибки парсинга JSON при первой загрузке")
                        interval_error = f"JSON parse error: {json_err}"
                        break
                    time.sleep(2)
                    continue
                    
            except requests.HTTPError as e:
                interval_error = str(e)
                error_str = str(e)
                is_429 = "429" in error_str or "Too Many Requests" in error_str or (hasattr(e, 'response') and e.response is not None and e.response.status_code == 429)
                
                if is_429:
                    retry_after = 60
                    if hasattr(e, 'response') and e.response is not None:
                        retry_header = e.response.headers.get('X-Ratelimit-Retry') or e.response.headers.get('Retry-After')
                        if retry_header:
                            try:
                                retry_after = int(float(retry_header))
                            except (ValueError, TypeError):
                                pass
                    
                    max_429_retries = 3
                    retry_success = False
                    for retry_attempt in range(1, max_429_retries + 1):
                        wait_time = retry_after * retry_attempt
                        logging.warning(f"Ошибка 429 для интервала {interval_from} - {interval_to}, попытка {retry_attempt}/{max_429_retries}, пауза {wait_time} секунд...")
                        time.sleep(wait_time)
                        
                        try:
                            resp = get_with_retry(FIN_REPORT_URL, headers, params, max_retries=1, timeout_s=30)
                            
                            # Проверяем, что ответ не пустой
                            if not resp.text or not resp.text.strip():
                                logging.warning(f"Пустой ответ от API при retry 429 для интервала {interval_from} - {interval_to}")
                                if retry_attempt < max_429_retries:
                                    continue
                                else:
                                    interval_error = "Empty response from API after 429 retries"
                                    break
                            
                            # Проверяем Content-Type
                            content_type = resp.headers.get('Content-Type', '').lower()
                            if 'application/json' not in content_type and 'text/json' not in content_type:
                                logging.warning(f"Неожиданный Content-Type при retry 429: {content_type}, первые 200 символов: {resp.text[:200]}")
                                if retry_attempt < max_429_retries:
                                    continue
                                else:
                                    interval_error = f"Invalid Content-Type: {content_type}"
                                    break
                            
                            # Пытаемся распарсить JSON
                            try:
                                data = resp.json()
                            except ValueError as json_err:
                                logging.warning(f"Ошибка парсинга JSON при retry 429: {json_err}, первые 500 символов: {resp.text[:500]}")
                                if retry_attempt < max_429_retries:
                                    continue
                                else:
                                    interval_error = f"JSON parse error: {json_err}"
                                    break
                            
                            interval_error = None
                            retry_success = True
                            break
                        except Exception as retry_exc:
                            logging.warning(f"Ошибка при retry 429, попытка {retry_attempt}/{max_429_retries}: {retry_exc}")
                            if retry_attempt < max_429_retries:
                                continue
                            else:
                                interval_error = str(e)
                                break
                    
                    if not retry_success:
                        if rrdid == 0:
                            logging.error(f"Пропускаем интервал {interval_from} - {interval_to} из-за ошибки 429")
                            break
                        time.sleep(5)
                        continue
                else:
                    logging.warning(f"Ошибка загрузки данных для интервала {interval_from} - {interval_to}: {e}")
                    if rrdid == 0:
                        logging.error(f"Пропускаем интервал {interval_from} - {interval_to}")
                        break
                    time.sleep(2)
                    continue
            except Exception as e:
                interval_error = str(e)
                logging.warning(f"Ошибка загрузки данных для интервала {interval_from} - {interval_to}, страница {page_count} (rrdid={rrdid}): {e}")
                import traceback
                logging.debug(f"Traceback: {traceback.format_exc()}")
                if rrdid == 0:
                    logging.error(f"Пропускаем интервал {interval_from} - {interval_to} из-за ошибки при первой загрузке")
                    break
                time.sleep(2)
                continue
            
            if not isinstance(data, list) or not data:
                logging.info(f"Интервал {interval_from} - {interval_to}: получен пустой ответ")
                break
            interval_rows.extend(data)
            logging.info(
                "Интервал %s - %s: стр. %s, +%s записей (итого %s)",
                interval_from, interval_to, page_count, len(data), len(interval_rows),
            )
            if progress_callback:
                progress_callback(
                    idx,
                    total_intervals,
                    f"{base_period} · стр. {page_count} ({len(interval_rows)} зап.)",
                )
            
            try:
                last = data[-1]
                rrdid = int(last.get("rrd_id") or last.get("rrdid") or last.get("rrdId") or 0)
            except Exception:
                break
            if len(data) < params_base.get("limit", page_limit):
                break
            time.sleep(0.5)
        
        if interval_rows:
            all_rows.extend(interval_rows)
            logging.info(f"Интервал {interval_from} - {interval_to}: загружено {len(interval_rows)} записей")
        elif interval_error:
            failed_intervals.append(f"{interval_from} — {interval_to}: {interval_error}")
            logging.error(f"ВНИМАНИЕ: Интервал {interval_from} - {interval_to} не загружен из-за ошибки: {interval_error}")
        
        if idx < total_intervals:
            pause_time = 2
            if interval_rows and len(interval_rows) > 15000:
                pause_time = 5
            elif interval_rows and len(interval_rows) > 10000:
                pause_time = 3
            time.sleep(pause_time)
    
    logging.info(f"Загрузка финансового отчета завершена. Всего загружено {len(all_rows)} записей")
    # Если ни одной строки и все интервалы упали с ошибкой — не продолжаем (иначе
    # уйдём в хранение/продвижение на десятки тысяч строк и «зависнет» UI).
    if not all_rows and failed_intervals:
        raise RuntimeError(
            "Не удалось загрузить финансовый отчёт Wildberries. "
            f"{failed_intervals[0]}. Попробуйте ещё раз через минуту."
        )
    return all_rows


# --- Paid Storage Report API ---
def _paid_storage_create_task(headers: Dict[str, str], date_from: str, date_to: str) -> str:
    """Создаёт задание на генерацию отчёта платного хранения. Возвращает taskId."""
    params = {"dateFrom": date_from, "dateTo": date_to}
    resp = get_with_retry(PAID_STORAGE_CREATE_URL, headers, params, max_retries=3, timeout_s=60)
    data = resp.json() if resp.text else {}
    task_id = None
    if isinstance(data, dict):
        inner = data.get("data") if isinstance(data.get("data"), dict) else data
        task_id = (inner or {}).get("taskId") or (inner or {}).get("task_id") or data.get("taskId")
    if not task_id:
        raise RuntimeError(f"Не получен taskId для платного хранения: {data!r}")
    return str(task_id)


def _paid_storage_wait_done(
    headers: Dict[str, str],
    task_id: str,
    progress_callback=None,
    progress_meta: tuple[int, int, str] | None = None,
) -> None:
    """Ждёт статус done у задания платного хранения."""
    url = PAID_STORAGE_STATUS_URL.format(task_id=task_id)
    started = time.time()
    last_status = ""
    while True:
        elapsed = time.time() - started
        if elapsed > PAID_STORAGE_STATUS_MAX_WAIT_S:
            raise TimeoutError(f"Таймаут ожидания отчёта платного хранения (task={task_id})")
        resp = get_with_retry(url, headers, {}, max_retries=3, timeout_s=30)
        data = resp.json() if resp.text else {}
        status = ""
        if isinstance(data, dict):
            inner = data.get("data") if isinstance(data.get("data"), dict) else data
            status = str((inner or {}).get("status") or "").strip().lower()
        if status and status != last_status:
            logging.info("Платное хранение task=%s status=%s (%.0f с)", task_id, status, elapsed)
            last_status = status
        if progress_callback and progress_meta:
            cur, total, base = progress_meta
            progress_callback(cur, total, f"{base} · ожидание ({status or '…'}, {int(elapsed)} с)")
        if status == "done":
            return
        if status in ("canceled", "purged", "cancelled"):
            raise RuntimeError(f"Задание платного хранения отклонено: status={status}")
        time.sleep(PAID_STORAGE_STATUS_POLL_S)


def _paid_storage_download(headers: Dict[str, str], task_id: str) -> List[Dict[str, Any]]:
    """Скачивает готовый отчёт платного хранения."""
    url = PAID_STORAGE_DOWNLOAD_URL.format(task_id=task_id)
    resp = get_with_retry(url, headers, {}, max_retries=3, timeout_s=180)
    if resp.status_code == 204 or not (resp.text or "").strip():
        return []
    data = resp.json()
    if isinstance(data, list):
        return [r for r in data if isinstance(r, dict)]
    if isinstance(data, dict):
        rows = data.get("data") or data.get("report") or data.get("rows") or []
        if isinstance(rows, list):
            return [r for r in rows if isinstance(r, dict)]
    return []


def fetch_paid_storage_report(
    token: str,
    date_from: str,
    date_to: str,
    progress_callback=None,
) -> List[Dict[str, Any]]:
    """
    Отчёт «Платное хранение» за период.
    API: max 8 дней на задание, создание — 1 запрос/мин.
    """
    if not token:
        return []
    headers = {"Authorization": f"Bearer {token}"}
    intervals = _split_date_range(date_from, date_to, days_per_chunk=PAID_STORAGE_MAX_DAYS)
    total = len(intervals)
    all_rows: List[Dict[str, Any]] = []
    last_create_ts = 0.0

    logging.info(
        "Начинаем загрузку платного хранения за период %s — %s, интервалов: %s",
        date_from, date_to, total,
    )
    for idx, (interval_from, interval_to) in enumerate(intervals, 1):
        base_period = f"хранение {interval_from} — {interval_to}"
        if progress_callback:
            progress_callback(idx, total, f"{base_period} · создание задания")

        if last_create_ts > 0:
            wait = PAID_STORAGE_CREATE_MIN_INTERVAL_S - (time.time() - last_create_ts)
            if wait > 0:
                logging.info("Пауза %.0f с перед созданием задания хранения %s/%s", wait, idx, total)
                if progress_callback:
                    progress_callback(idx, total, f"{base_period} · пауза API {int(wait)} с")
                time.sleep(wait)

        logging.info("Платное хранение %s/%s: %s — %s", idx, total, interval_from, interval_to)
        task_id = _paid_storage_create_task(headers, interval_from, interval_to)
        last_create_ts = time.time()
        _paid_storage_wait_done(
            headers,
            task_id,
            progress_callback=progress_callback,
            progress_meta=(idx, total, base_period),
        )
        if progress_callback:
            progress_callback(idx, total, f"{base_period} · скачивание")
        logging.info("Платное хранение %s/%s: скачивание task=%s…", idx, total, task_id)
        rows = _paid_storage_download(headers, task_id)
        all_rows.extend(rows)
        logging.info("Платное хранение %s/%s: получено %s строк", idx, total, len(rows))

    logging.info("Платное хранение: всего %s строк", len(all_rows))
    return all_rows


# --- FBW Supplies API ---
def fetch_fbw_supplies_list(token: str, days_back: int = 90) -> list[dict[str, Any]]:
    """Получает список поставок FBW"""
    if not token:
        return []
    date_to = datetime.now(MOSCOW_TZ).date()
    date_from = date_to - timedelta(days=days_back)
    date_till = date_to + timedelta(days=1)
    body = {
        "dates": [
            {
                "from": date_from.strftime("%Y-%m-%d"),
                "till": date_till.strftime("%Y-%m-%d"),
                "type": "createDate",
            }
        ]
    }
    headers1 = {"Authorization": f"Bearer {token}"}
    try:
        supplies_api_throttle()
        resp = post_with_retry(FBW_SUPPLIES_LIST_URL, headers1, body)
        items = resp.json() or []
    except Exception:
        headers2 = {"Authorization": f"{token}"}
        supplies_api_throttle()
        resp = post_with_retry(FBW_SUPPLIES_LIST_URL, headers2, body)
        items = resp.json() or []
    # Sort by createDate desc
    def _key(it: dict[str, Any]):
        return _parse_iso_datetime(str(it.get("createDate") or "")) or datetime.min.replace(tzinfo=MOSCOW_TZ)
    items.sort(key=_key, reverse=True)
    return items


def fetch_fbw_supply_details(token: str, supply_id: int | str) -> dict[str, Any] | None:
    """Получает детали поставки FBW"""
    if not token or not supply_id:
        return None
    url = FBW_SUPPLY_DETAILS_URL.format(id=supply_id)
    headers1 = {"Authorization": f"Bearer {token}"}
    try:
        resp = get_with_retry(url, headers1, params={})
        return resp.json()
    except Exception:
        headers2 = {"Authorization": f"{token}"}
        try:
            resp = get_with_retry(url, headers2, params={})
            return resp.json()
        except Exception:
            return None


def fetch_fbw_supply_goods(token: str, supply_id: int | str, limit: int = 200, offset: int = 0) -> list[dict[str, Any]]:
    """Получает товары поставки FBW"""
    if not token or not supply_id:
        return []
    url = FBW_SUPPLY_GOODS_URL.format(id=supply_id)
    headers1 = {"Authorization": f"Bearer {token}"}
    params = {"limit": limit, "offset": offset}
    try:
        resp = get_with_retry(url, headers1, params=params)
        return resp.json() or []
    except Exception:
        headers2 = {"Authorization": f"{token}"}
        try:
            resp = get_with_retry(url, headers2, params=params)
            return resp.json() or []
        except Exception:
            return []


def fetch_fbw_supply_packages(token: str, supply_id: int | str) -> list[dict[str, Any]]:
    """Получает упаковки поставки FBW"""
    if not token or not supply_id:
        return []
    url = FBW_SUPPLY_PACKAGE_URL.format(id=supply_id)
    headers1 = {"Authorization": f"Bearer {token}"}
    try:
        resp = get_with_retry(url, headers1, params={})
        return resp.json() or []
    except Exception:
        headers2 = {"Authorization": f"{token}"}
        try:
            resp = get_with_retry(url, headers2, params={})
            return resp.json() or []
        except Exception:
            return []


# --- FBS API ---
def fetch_fbs_new_orders(token: str) -> List[Dict[str, Any]]:
    """Получает новые заказы FBS"""
    headers = {"Authorization": f"Bearer {token}"}
    try:
        resp = get_with_retry(FBS_NEW_URL, headers, params={})
        return resp.json() or []
    except Exception:
        return []


def fetch_fbs_orders(token: str, limit: int = 100, next_cursor: str | None = None) -> Dict[str, Any]:
    """Получает заказы FBS с пагинацией"""
    headers = {"Authorization": f"Bearer {token}"}
    params: Dict[str, Any] = {"limit": limit}
    if next_cursor:
        params["next"] = next_cursor
    try:
        resp = get_with_retry(FBS_ORDERS_URL, headers, params=params)
        return resp.json() or {}
    except Exception:
        return {}


def fetch_fbs_statuses(token: str, order_ids: List[int]) -> Dict[str, Any]:
    """Получает статусы заказов FBS"""
    headers = {"Authorization": f"Bearer {token}"}
    body = {"orders": order_ids}
    try:
        resp = post_with_retry(FBS_ORDERS_STATUS_URL, headers, body)
        return resp.json() or {}
    except Exception:
        return {}


# --- DBS API ---
def fetch_dbs_new_orders(token: str) -> List[Dict[str, Any]]:
    """Получает новые заказы DBS"""
    headers = {"Authorization": f"Bearer {token}"}
    try:
        resp = get_with_retry(DBS_NEW_URL, headers, params={})
        return resp.json() or []
    except Exception:
        return []


def fetch_dbs_statuses(token: str, order_ids: List[int]) -> Dict[str, Any]:
    """Получает статусы заказов DBS"""
    headers = {"Authorization": f"Bearer {token}"}
    body = {"orders": order_ids}
    try:
        resp = post_with_retry(DBS_STATUS_URL, headers, body)
        return resp.json() or {}
    except Exception:
        return {}


def fetch_dbs_orders(token: str, limit: int = 100, next_cursor: str | None = None) -> Dict[str, Any]:
    """Получает заказы DBS с пагинацией"""
    headers = {"Authorization": f"Bearer {token}"}
    params: Dict[str, Any] = {"limit": limit}
    if next_cursor:
        params["next"] = next_cursor
    try:
        resp = get_with_retry(DBS_ORDERS_URL, headers, params=params)
        return resp.json() or {}
    except Exception:
        return {}


# --- Other APIs ---
def fetch_seller_info(token: str) -> Dict[str, Any] | None:
    """Получает информацию о продавце"""
    headers = {"Authorization": f"Bearer {token}"}
    try:
        resp = get_with_retry(SELLER_INFO_URL, headers, params={})
        return resp.json()
    except Exception:
        return None


def fetch_acceptance_coefficients(token: str) -> List[Dict[str, Any]] | None:
    """Получает коэффициенты приёмки"""
    headers = {"Authorization": f"Bearer {token}"}
    try:
        resp = get_with_retry(ACCEPT_COEFS_URL, headers, params={})
        data = resp.json()
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            # Be tolerant to wrapper objects if WB changes payload shape.
            for key in ("response", "data", "result", "items"):
                payload = data.get(key)
                if isinstance(payload, list):
                    return payload
                if isinstance(payload, dict):
                    for nested_key in ("data", "result", "items"):
                        nested = payload.get(nested_key)
                        if isinstance(nested, list):
                            return nested
        return []
    except Exception:
        return None


def fetch_fbs_warehouses(token: str) -> list[dict[str, Any]]:
    """Получает список складов FBS"""
    headers = {"Authorization": f"Bearer {token}"}
    try:
        resp = get_with_retry(FBS_WAREHOUSES_URL, headers, params={})
        return resp.json() or []
    except Exception:
        return []


def fetch_supplies_warehouses(token: str) -> list[dict[str, Any]]:
    """Получает список FBW складов с адресами."""
    if not token:
        return []
    headers = {"Authorization": f"Bearer {token}"}
    try:
        resp = get_with_retry(SUPPLIES_WAREHOUSES_URL, headers, params={})
        data = resp.json()
        return data if isinstance(data, list) else []
    except Exception:
        try:
            headers2 = {"Authorization": f"{token}"}
            resp = get_with_retry(SUPPLIES_WAREHOUSES_URL, headers2, params={})
            data = resp.json()
            return data if isinstance(data, list) else []
        except Exception:
            return []


def fetch_wb_offices(token: str) -> list[dict[str, Any]]:
    """Получает список складов WB (offices) с адресом, cargoType и deliveryType."""
    if not token:
        return []
    headers = {"Authorization": f"Bearer {token}"}
    try:
        resp = get_with_retry(WB_OFFICES_URL, headers, params={})
        data = resp.json()
        return data if isinstance(data, list) else []
    except Exception:
        return []


def fetch_acceptance_warehouse_metadata(token: str) -> dict[str, dict[str, Any]]:
    """Возвращает индекс метаданных складов для страницы коэффициентов."""
    from utils.helpers import (
        ACCEPTANCE_CARGO_LABELS,
        ACCEPTANCE_DELIVERY_LABELS,
        _merge_meta_dict,
        _finalize_meta_entry,
        _normalize_wh_key,
        _base_wh_name,
        _cargo_labels_from_name,
    )

    store: dict[str, dict[str, Any]] = {}
    entries_by_id: dict[int, dict[str, Any]] = {}

    def _resolve_entry(name: str | None = None, warehouse_id: Any | None = None) -> dict[str, Any]:
        entry: dict[str, Any] | None = None
        if warehouse_id is not None:
            try:
                entry = entries_by_id.get(int(warehouse_id))
            except Exception:
                entry = None
        if entry is None and name:
            entry = store.get(name) or store.get(_normalize_wh_key(name)) or store.get(_base_wh_name(name))
        if entry is None:
            entry = {"cargo_types": [], "delivery_types": []}
        return entry

    def _register(name: str | None, warehouse_id: Any | None, patch: dict[str, Any]) -> dict[str, Any]:
        entry = _resolve_entry(name, warehouse_id)
        _merge_meta_dict(entry, patch)
        if warehouse_id is not None:
            try:
                wid = int(warehouse_id)
                entries_by_id[wid] = entry
                store[f"id:{wid}"] = entry
                entry["warehouse_id"] = wid
            except Exception:
                pass
        if name:
            clean = str(name).strip()
            if clean:
                store[clean] = entry
                store[_normalize_wh_key(clean)] = entry
                base = _base_wh_name(clean)
                if base:
                    store[base] = entry
        return entry

    def _register_cargo_delivery(
        name: str | None,
        warehouse_id: Any | None,
        cargo_raw: Any,
        delivery_raw: Any,
        extra: dict[str, Any] | None = None,
    ) -> None:
        patch: dict[str, Any] = dict(extra or {})
        cargo_types: list[str] = list(patch.get("cargo_types") or [])
        delivery_types: list[str] = list(patch.get("delivery_types") or [])
        try:
            if cargo_raw is not None:
                label = ACCEPTANCE_CARGO_LABELS.get(int(cargo_raw), f"Тип груза {cargo_raw}")
                if label not in cargo_types:
                    cargo_types.append(label)
        except Exception:
            pass
        try:
            if delivery_raw is not None:
                label = ACCEPTANCE_DELIVERY_LABELS.get(int(delivery_raw), f"Тип доставки {delivery_raw}")
                if label not in delivery_types:
                    delivery_types.append(label)
        except Exception:
            pass
        if name:
            for label in _cargo_labels_from_name(name):
                if label not in cargo_types:
                    cargo_types.append(label)
        if cargo_types:
            patch["cargo_types"] = cargo_types
        if delivery_types:
            patch["delivery_types"] = delivery_types
        _register(name, warehouse_id, patch)

    for office in fetch_wb_offices(token) or []:
        name = str(office.get("name") or office.get("warehouseName") or "").strip()
        if not name:
            continue
        office_id = office.get("id")
        _register_cargo_delivery(
            name,
            office_id,
            office.get("cargoType"),
            office.get("deliveryType"),
            {
                "address": str(office.get("address") or office.get("fullAddress") or "").strip(),
                "city": str(office.get("city") or "").strip(),
                "office_id": office_id,
                "is_fbs": int(office.get("deliveryType") or 0) == 1,
            },
        )

    for warehouse in fetch_fbs_warehouses(token) or []:
        name = str(warehouse.get("name") or warehouse.get("warehouseName") or "").strip()
        if not name:
            continue
        wid = warehouse.get("id") or warehouse.get("warehouseId") or warehouse.get("warehouseID")
        _register_cargo_delivery(
            name,
            wid,
            warehouse.get("cargoType"),
            warehouse.get("deliveryType"),
            {
                "address": str(warehouse.get("address") or warehouse.get("fullAddress") or "").strip(),
                "city": str(warehouse.get("city") or "").strip(),
                "is_fbs": int(warehouse.get("deliveryType") or 0) == 1,
            },
        )

    for warehouse in fetch_supplies_warehouses(token) or []:
        name = str(warehouse.get("name") or warehouse.get("warehouseName") or warehouse.get("officeName") or "").strip()
        if not name:
            continue
        wid = warehouse.get("ID") or warehouse.get("id") or warehouse.get("warehouseID") or warehouse.get("warehouseId")
        patch: dict[str, Any] = {
            "address": str(warehouse.get("address") or warehouse.get("fullAddress") or "").strip(),
            "city": str(warehouse.get("city") or warehouse.get("settlement") or "").strip(),
            "is_fbw": True,
        }
        cargo_types = _cargo_labels_from_name(name)
        if not cargo_types:
            cargo_types = [ACCEPTANCE_CARGO_LABELS[1]]
        patch["cargo_types"] = cargo_types
        patch["delivery_types"] = [ACCEPTANCE_DELIVERY_LABELS[1]]
        _register(name, wid, patch)

    seen: set[int] = set()
    for entry in list(entries_by_id.values()) + list(store.values()):
        oid = id(entry)
        if oid in seen:
            continue
        seen.add(oid)
        _finalize_meta_entry(entry)

    return store


def fetch_transit_tariffs(token: str) -> list[dict[str, Any]]:
    """Получает транзитные направления и тарифы (supplies-api)."""
    if not token:
        return []
    headers = {"Authorization": f"Bearer {token}"}
    try:
        supplies_api_throttle()
        resp = get_with_retry(TRANSIT_TARIFFS_URL, headers, params={})
        data = resp.json()
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            for key in ("data", "result", "items", "response"):
                payload = data.get(key)
                if isinstance(payload, list):
                    return payload
        return []
    except Exception:
        try:
            supplies_api_throttle()
            headers2 = {"Authorization": f"{token}"}
            resp = get_with_retry(TRANSIT_TARIFFS_URL, headers2, params={})
            data = resp.json()
            return data if isinstance(data, list) else []
        except Exception:
            return []


def fetch_fbs_stocks_by_warehouse(token: str, warehouse_id: int, skus: list[str]) -> list[dict[str, Any]]:
    """Получает остатки FBS по складу"""
    if not token or not warehouse_id or not skus:
        return []
    url = FBS_STOCKS_BY_WAREHOUSE_URL.format(warehouseId=warehouse_id)
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    body = {"skus": skus}
    try:
        resp = post_with_retry(url, headers, body)
        print(f"Статус ответа API остатков FBS для склада {warehouse_id}: {resp.status_code}")
        if resp.status_code != 200:
            print(f"Ошибка API: {resp.status_code} - {resp.text[:200]}")
            return []
        data = resp.json()
        print(f"Ответ API остатков FBS для склада {warehouse_id}: тип={type(data)}, ключи={list(data.keys()) if isinstance(data, dict) else 'не словарь'}")
        # API может возвращать данные в разных форматах
        if isinstance(data, list):
            print(f"Получен список из {len(data)} элементов")
            return data
        elif isinstance(data, dict):
            # Проверяем разные возможные ключи
            if "stocks" in data:
                stocks = data["stocks"]
                print(f"Найдено 'stocks' с {len(stocks) if isinstance(stocks, list) else 'не список'} элементами")
                return stocks if isinstance(stocks, list) else []
            elif "data" in data:
                stocks = data["data"]
                if isinstance(stocks, list):
                    print(f"Найдено 'data' со списком из {len(stocks)} элементов")
                    return stocks
                elif isinstance(stocks, dict) and "stocks" in stocks:
                    stocks_list = stocks["stocks"]
                    print(f"Найдено 'data.stocks' с {len(stocks_list) if isinstance(stocks_list, list) else 'не список'} элементами")
                    return stocks_list if isinstance(stocks_list, list) else []
            elif "items" in data:
                items = data["items"]
                print(f"Найдено 'items' с {len(items) if isinstance(items, list) else 'не список'} элементами")
                return items if isinstance(items, list) else []
            print(f"Неизвестный формат ответа API. Ключи: {list(data.keys())}")
        return []
    except Exception as e:
        print(f"Ошибка при получении остатков FBS для склада {warehouse_id}: {e}")
        import traceback
        traceback.print_exc()
        return []


def fetch_stocks(token: str, date_from: str | None = None) -> List[Dict[str, Any]]:
    """Получает остатки на складах WB (совместимая обёртка)."""
    return fetch_wb_warehouse_stocks(token)


def _extract_wb_stocks_items(payload: Any) -> list[dict[str, Any]]:
    """Достаёт список строк остатков из ответа Analytics API."""
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if not isinstance(payload, dict):
        return []
    data = payload.get("data")
    if isinstance(data, dict):
        items = data.get("items")
        if isinstance(items, list):
            return [x for x in items if isinstance(x, dict)]
    for key in ("items", "stocks", "result"):
        value = payload.get(key)
        if isinstance(value, list):
            return [x for x in value if isinstance(x, dict)]
        if isinstance(value, dict) and isinstance(value.get("items"), list):
            return [x for x in value["items"] if isinstance(x, dict)]
    return []


def fetch_wb_warehouse_stocks(token: str) -> List[Dict[str, Any]]:
    """Текущие остатки на складах WB через Analytics API.

    POST /api/analytics/v1/stocks-report/wb-warehouses
    Замена устаревшего GET /api/v1/supplier/stocks.
    Лимит: 1 запрос / 20 сек, до 250000 строк в ответе, offset-пагинация.
    """
    if not token:
        return []

    headers_variants = [
        {"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        {"Authorization": f"{token}", "Content-Type": "application/json"},
    ]
    collected: List[Dict[str, Any]] = []
    offset = 0
    limit = int(STOCKS_API_PAGE_LIMIT or 250000)
    page_num = 0
    auth_idx = 0

    while True:
        page_num += 1
        if page_num > 50:
            logger.warning("WB stocks pagination safety stop after %s pages", page_num - 1)
            break

        body = {"limit": limit, "offset": offset}
        last_err: Exception | None = None
        resp = None

        for idx in range(auth_idx, len(headers_variants)):
            headers = headers_variants[idx]
            try:
                if page_num > 1:
                    time.sleep(float(STOCKS_API_MIN_INTERVAL_S or 20.0))
                logger.info(
                    "Fetching WB warehouse stocks page=%s offset=%s limit=%s auth=%s",
                    page_num,
                    offset,
                    limit,
                    "Bearer" if idx == 0 else "raw",
                )
                resp = post_with_retry(STOCKS_API_URL, headers, body, max_retries=2)
                auth_idx = idx
                break
            except requests.HTTPError as err:
                last_err = err
                status = err.response.status_code if err.response is not None else None
                if status in (401, 403) and idx + 1 < len(headers_variants):
                    continue
                raise
            except Exception as err:
                last_err = err
                raise

        if resp is None:
            if last_err:
                raise last_err
            break

        payload = resp.json()
        items = _extract_wb_stocks_items(payload)
        logger.info("WB warehouse stocks page=%s got %s rows", page_num, len(items))
        if not items:
            break

        collected.extend(items)
        if len(items) < limit:
            break
        offset += limit

    return collected


def fetch_cards_list(
    token: str,
    nm_ids: List[int] | None = None,
    cursor: Dict[str, Any] | None = None,
    limit: int = 100,
    text_search: str | None = None,
    vendor_codes: List[str] | None = None,
) -> Dict[str, Any]:
    """Получает список карточек товаров"""
    # Build request body per WB docs: settings.cursor + settings.filter
    base_cursor = {"limit": limit, "nmID": 0}
    if cursor:
        base_cursor.update(cursor)
    body: Dict[str, Any] = {
        "settings": {
            "cursor": base_cursor,
            "filter": {
                "textSearch": (text_search or ""),
                "withPhoto": -1,  # -1 — не фильтровать по наличию фото
            },
        }
    }
    if nm_ids:
        body["nmID"] = nm_ids
    if vendor_codes:
        body["settings"]["filter"]["vendorCode"] = vendor_codes
    # Try with Bearer first, then raw token
    headers1 = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    try:
        resp = post_with_retry(WB_CARDS_LIST_URL, headers1, body)
        return resp.json()
    except requests.HTTPError as err:
        if err.response is not None and err.response.status_code in (401, 403):
            headers2 = {"Authorization": f"{token}", "Content-Type": "application/json"}
            resp2 = post_with_retry(WB_CARDS_LIST_URL, headers2, body)
            return resp2.json()
        raise


def fetch_all_cards(token: str, page_limit: int = 1000) -> List[Dict[str, Any]]:
    """Получает все карточки товаров с пагинацией"""
    all_cards: List[Dict[str, Any]] = []
    seen_keys: set[tuple] = set()
    cursor: Dict[str, Any] = {"limit": page_limit, "nmID": 0}
    safety = 0
    while True:
        safety += 1
        if safety > 5000:
            break
        data = fetch_cards_list(token, cursor=cursor, limit=page_limit)
        payload = data.get("data") or data
        cards = payload.get("cards") or []
        if not cards:
            break
        all_cards.extend(cards)
        cur = payload.get("cursor") or {}
        key = (cur.get("updatedAt"), cur.get("nmID"), cur.get("nmIDNext"))
        if key in seen_keys:
            break
        seen_keys.add(key)
        # Prepare next cursor
        next_nm = cur.get("nmIDNext") or cur.get("nmID")
        next_cursor: Dict[str, Any] = {"limit": page_limit}
        if cur.get("updatedAt"):
            next_cursor["updatedAt"] = cur.get("updatedAt")
        if next_nm is not None:
            next_cursor["nmID"] = next_nm
        cursor = next_cursor
        # If страница меньше лимита, вероятно, достигнут конец
        if len(cards) < page_limit:
            break
    return all_cards


def fetch_commission_data(token: str) -> Dict[int, Dict[str, Any]]:
    """Получает данные о комиссиях Wildberries по всем категориям"""
    from utils.constants import COMMISSION_API_URL
    try:
        print("Получаем данные о комиссиях...")
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        response = requests.get(COMMISSION_API_URL, headers=headers, timeout=30)
        print(f"Статус ответа комиссий: {response.status_code}")
        if response.status_code == 200:
            result = response.json()
            report_data = result.get("report", [])
            print(f"Получено {len(report_data)} записей о комиссиях")
            commission_data = {}
            for item in report_data:
                subject_id = item.get("subjectID")
                if subject_id:
                    commission_data[subject_id] = {
                        "parent_name": item.get("parentName", ""),
                        "subject_name": item.get("subjectName", ""),
                        "fbs_commission": item.get("kgvpMarketplace", 0),
                        "cc_commission": item.get("kgvpPickup", 0),
                        "dbs_dbw_commission": item.get("kgvpSupplier", 0),
                        "edbs_commission": item.get("kgvpSupplierExpress", 0),
                        "fbw_commission": item.get("paidStorageKgvp", 0),
                    }
            print(f"Обработано {len(commission_data)} комиссий")
            return commission_data
        else:
            print(f"Ошибка получения комиссий: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"Ошибка при получении комиссий: {e}")
    return {}


def fetch_warehouses_data(token: str) -> List[Dict[str, Any]]:
    """Получение данных о складах через API Wildberries"""
    from utils.constants import WAREHOUSES_API_URL
    from datetime import datetime
    try:
        current_date = datetime.now().strftime("%Y-%m-%d")
        print(f"Получаем данные о складах на дату {current_date}")
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        url = f"{WAREHOUSES_API_URL}?date={current_date}"
        response = requests.get(url, headers=headers, timeout=30)
        print(f"Статус ответа API складов: {response.status_code}")
        response.raise_for_status()
        data = response.json()
        warehouses = []
        if isinstance(data, list):
            warehouses = data
        elif isinstance(data, dict):
            if 'response' in data and isinstance(data['response'], dict):
                response_data = data['response']
                if 'data' in response_data and isinstance(response_data['data'], dict):
                    if 'warehouseList' in response_data['data']:
                        warehouses = response_data['data']['warehouseList']
                elif 'data' in response_data and isinstance(response_data['data'], list):
                    warehouses = response_data['data']
            elif 'data' in data:
                if isinstance(data['data'], list):
                    warehouses = data['data']
                elif isinstance(data['data'], dict) and 'warehouseList' in data['data']:
                    warehouses = data['data']['warehouseList']
            elif 'warehouseList' in data:
                warehouses = data['warehouseList']
        print(f"Найдено {len(warehouses)} складов в ответе API")
        warehouses_list = []
        for warehouse in warehouses:
            warehouse_name = (warehouse.get('warehouseName') or warehouse.get('name') or warehouse.get('warehouse_name') or warehouse.get('title') or '')
            box_delivery_coef = (warehouse.get('boxDeliveryCoefExpr') or warehouse.get('coefficient') or warehouse.get('coef') or 0)
            if warehouse_name:
                try:
                    coef_value = int(float(box_delivery_coef)) if box_delivery_coef else 0
                    warehouses_list.append({'name': warehouse_name, 'coefficient': coef_value})
                except (ValueError, TypeError):
                    warehouses_list.append({'name': warehouse_name, 'coefficient': 0})
        print(f"Загружено {len(warehouses_list)} складов")
        return warehouses_list
    except Exception as e:
        print(f"Ошибка загрузки складов: {e}")
        return []


def fetch_prices_data(token: str, nm_ids: List[int]) -> Dict[int, Dict[str, Any]]:
    """Получает данные о ценах товаров через API Wildberries"""
    from utils.constants import DISCOUNTS_PRICES_API_URL
    if not nm_ids:
        return {}
    try:
        print(f"Пробуем получить цены для {len(nm_ids)} товаров")
        headers = {"Authorization": f"Bearer {token}"}
        params = {"limit": 500}
        response = requests.get(DISCOUNTS_PRICES_API_URL, headers=headers, params=params, timeout=30)
        print(f"Статус ответа: {response.status_code}")
        if response.status_code == 200:
            result = response.json()
            print(f"Ответ API получен")
            prices_data = {}
            if isinstance(result, dict) and "data" in result:
                list_goods = result["data"].get("listGoods", [])
                print(f"Найдено {len(list_goods)} товаров в listGoods")
                for goods_item in list_goods:
                    nm_id = goods_item.get("nmID")
                    if nm_id:
                        sizes = goods_item.get("sizes", [])
                        print(f"Товар {nm_id}: найдено {len(sizes)} размеров")
                        if sizes:
                            first_size = sizes[0]
                            price = first_size.get("price", 0)
                            discounted_price = first_size.get("discountedPrice", price)
                            if price > 0:
                                club_discounted_price = first_size.get("clubDiscountedPrice", discounted_price)
                                seller_discount_amount = price - discounted_price if price > discounted_price else 0
                                seller_discount_percent = (seller_discount_amount / price * 100) if price > 0 else 0
                                prices_data[nm_id] = {
                                    "price": price,
                                    "discount_price": discounted_price,
                                    "club_discount_price": club_discounted_price,
                                    "seller_discount_amount": round(seller_discount_amount, 2),
                                    "seller_discount_percent": round(seller_discount_percent, 2)
                                }
                                print(f"Товар {nm_id}: цена до скидки {price}, цена со скидкой {discounted_price}")
            if prices_data:
                print(f"Успешно получено {len(prices_data)} цен из API")
                return prices_data
            else:
                print("Нет данных о ценах в ответе от API")
        else:
            print(f"Ошибка {response.status_code}: {response.text}")
    except Exception as e:
        print(f"Ошибка при получении цен: {e}")
    print("Не удалось получить цены от API")
    return {}

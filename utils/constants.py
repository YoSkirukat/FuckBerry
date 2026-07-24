# -*- coding: utf-8 -*-
"""Константы приложения"""
import os
from datetime import timedelta, timezone

# Версия приложения
APP_VERSION = "1.0.1"
# Порог (дней до exp JWT): жёлтый баннер в шапке, как у подписки. Переопределение: WB_TOKEN_EXPIRY_BANNER_DAYS
WB_TOKEN_EXPIRY_BANNER_DAYS = int(os.getenv("WB_TOKEN_EXPIRY_BANNER_DAYS", "20"))

# FBS warehouses/stocks
FBS_WAREHOUSES_URL = "https://marketplace-api.wildberries.ru/api/v3/warehouses"
WB_OFFICES_URL = "https://marketplace-api.wildberries.ru/api/v3/offices"
FBS_STOCKS_BY_WAREHOUSE_URL = "https://marketplace-api.wildberries.ru/api/v3/stocks/{warehouseId}"

# Supplies API warehouses (for labels tool)
SUPPLIES_WAREHOUSES_URL = "https://supplies-api.wildberries.ru/api/v1/warehouses"

# Prices API
DISCOUNTS_PRICES_API_URL = "https://discounts-prices-api.wildberries.ru/api/v2/list/goods/filter"
PRICES_API_URL = "https://marketplace-api.wildberries.ru/api/v2/list/goods/filter"

# Product history API (including price history)
PRODUCT_HISTORY_API_URL = "https://product-history.wildberries.ru/products/history"
PRODUCT_HISTORY_API_URL_ALT = "https://product-history.wildberries.ru/products/history"

# Commission API
COMMISSION_API_URL = "https://common-api.wildberries.ru/api/v1/tariffs/commission"
DIMENSIONS_API_URL = "https://content-api.wildberries.ru/content/v1/cards/list"
WAREHOUSES_API_URL = "https://common-api.wildberries.ru/api/v1/tariffs/box"

# Orders API
API_URL = "https://statistics-api.wildberries.ru/api/v1/supplier/orders"
SALES_API_URL = "https://statistics-api.wildberries.ru/api/v1/supplier/sales"

# FBS API
FBS_NEW_URL = "https://marketplace-api.wildberries.ru/api/v3/orders/new"
FBS_ORDERS_URL = "https://marketplace-api.wildberries.ru/api/v3/orders"
FBS_ORDERS_STATUS_URL = "https://marketplace-api.wildberries.ru/api/v3/orders/status"
FBS_SUPPLIES_LIST_URL = "https://marketplace-api.wildberries.ru/api/v3/supplies"
FBS_SUPPLY_INFO_URL = "https://marketplace-api.wildberries.ru/api/v3/supplies/{supplyId}"
# Старый endpoint списка товаров в поставке (только чтение, оставляем как fallback)
FBS_SUPPLY_ORDERS_URL = "https://marketplace-api.wildberries.ru/api/v3/supplies/{supplyId}/orders"
FBS_SUPPLY_ORDERS_IDS_URL_V2 = "https://marketplace-api.wildberries.ru/api/v2/supplies/orders/ids"  # Try v2 first
FBS_SUPPLY_ORDERS_IDS_URL_V3 = "https://marketplace-api.wildberries.ru/api/v3/supply/orders/ids"  # Try v3 as fallback
# Новый endpoint для добавления сборочных заданий в поставку
FBS_SUPPLY_ADD_ORDERS_URL = "https://marketplace-api.wildberries.ru/api/marketplace/v3/supplies/{supplyId}/orders"

# DBS (Delivery by Seller) API
DBS_NEW_URL = "https://marketplace-api.wildberries.ru/api/v3/dbs/orders/new"
DBS_STATUS_URL = "https://marketplace-api.wildberries.ru/api/v3/dbs/orders/status"
DBS_ORDERS_URL = "https://marketplace-api.wildberries.ru/api/v3/dbs/orders"

# Seller info
SELLER_INFO_URL = "https://common-api.wildberries.ru/api/v1/seller-info"

# Acceptance coefficients
# WB moved this method from supplies-api to common-api tariffs section.
ACCEPT_COEFS_URL = "https://common-api.wildberries.ru/api/tariffs/v1/acceptance/coefficients"

# FBW supplies API
FBW_SUPPLIES_LIST_URL = "https://supplies-api.wildberries.ru/api/v1/supplies"
TRANSIT_TARIFFS_URL = "https://supplies-api.wildberries.ru/api/v1/transit-tariffs"
FBW_SUPPLY_DETAILS_URL = "https://supplies-api.wildberries.ru/api/v1/supplies/{id}"
FBW_SUPPLY_GOODS_URL = "https://supplies-api.wildberries.ru/api/v1/supplies/{id}/goods"
FBW_SUPPLY_PACKAGE_URL = "https://supplies-api.wildberries.ru/api/v1/supplies/{id}/package"

# Wildberries Content API
WB_CARDS_LIST_URL = "https://content-api.wildberries.ru/content/v2/get/cards/list"
WB_CARDS_UPDATE_URL = "https://content-api.wildberries.ru/content/v2/cards/update"

# Stocks API — остатки на складах WB (Analytics)
# Старый GET statistics-api /api/v1/supplier/stocks отключён (release notes #494).
STOCKS_API_URL = "https://seller-analytics-api.wildberries.ru/api/analytics/v1/stocks-report/wb-warehouses"
STOCKS_API_PAGE_LIMIT = 250000
STOCKS_API_MIN_INTERVAL_S = 20.0  # лимит WB: 1 запрос / 20 сек, 3 / мин
# Фоновый авто-рефреш и «устаревание» кэша остатков (сек)
STOCKS_AUTO_REFRESH_INTERVAL_S = 30 * 60  # раз в 30 минут
STOCKS_CACHE_STALE_S = 25 * 60  # если старше — можно обновлять

# Finance report API
FIN_REPORT_URL = "https://statistics-api.wildberries.ru/api/v5/supplier/reportDetailByPeriod"

# Paid storage report (async, seller-analytics-api; max 8 days per task)
PAID_STORAGE_CREATE_URL = "https://seller-analytics-api.wildberries.ru/api/v1/paid_storage"
PAID_STORAGE_STATUS_URL = "https://seller-analytics-api.wildberries.ru/api/v1/paid_storage/tasks/{task_id}/status"
PAID_STORAGE_DOWNLOAD_URL = "https://seller-analytics-api.wildberries.ru/api/v1/paid_storage/tasks/{task_id}/download"
PAID_STORAGE_MAX_DAYS = 8
PAID_STORAGE_CREATE_MIN_INTERVAL_S = 61.0  # лимит WB: 1 создание / мин
PAID_STORAGE_STATUS_POLL_S = 5.0
PAID_STORAGE_STATUS_MAX_WAIT_S = 300.0

# Advertising / Promotion API (для колонки «Продвижение» в расшифровке финотчёта)
ADVERT_API_BASE = "https://advert-api.wildberries.ru"
ADV_ADVERTS_URL = f"{ADVERT_API_BASE}/api/advert/v2/adverts"
ADV_FULLSTATS_URL = f"{ADVERT_API_BASE}/adv/v3/fullstats"
ADV_FULLSTATS_CHUNK = 50
ADV_FULLSTATS_MIN_INTERVAL_S = float(os.getenv("ADV_FULLSTATS_MIN_INTERVAL_S", "20.0"))

# Cache directory
CACHE_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "cache")
if not os.path.isdir(CACHE_DIR):
    os.makedirs(CACHE_DIR, exist_ok=True)

# Макс. размер last_results (orders_user_*.json) при json.load, МиБ. Длинные периоды = крупный JSON.
# Переменная окружения: FUCKBERRY_LAST_RESULTS_MAX_MB
LAST_RESULTS_CACHE_MAX_BYTES = (
    int(os.getenv("FUCKBERRY_LAST_RESULTS_MAX_MB", "80")) * 1024 * 1024
)

# Пагинация WB supplier/orders (lastChangeDate). Для короткого окна, заканчивающегося «сегодня»,
# условие выхода по дате часто не наступает в течение дня → без лимита возможны сотни страниц и минуты ожидания.
WB_ORDERS_FETCH_MAX_PAGES = int(os.getenv("WB_ORDERS_FETCH_MAX_PAGES", "2000"))
WB_ORDERS_FETCH_MAX_PAGES_INTRADAY = int(os.getenv("WB_ORDERS_FETCH_MAX_PAGES_INTRADAY", "200"))
# Пауза между страницами пагинации WB orders (сек). Intraday — короче, чтобы «сегодня» не тянулось минутами.
WB_ORDERS_PAGE_SLEEP_S = float(os.getenv("WB_ORDERS_PAGE_SLEEP_S", "0.1"))
WB_ORDERS_PAGE_SLEEP_INTRADAY_S = float(os.getenv("WB_ORDERS_PAGE_SLEEP_INTRADAY_S", "0.02"))
# Не дёргать WB для «сегодня», если срез дня в period-cache обновлялся недавно (сек). 0 = всегда обновлять.
ORDERS_TODAY_CACHE_TTL_SECONDS = int(os.getenv("ORDERS_TODAY_CACHE_TTL_SECONDS", "180"))

# Управление автопостроением кэша поставок
SUPPLIES_CACHE_AUTO = os.getenv("SUPPLIES_CACHE_AUTO", "0") == "1"

# Throttling для WB supplies API
SUPPLIES_API_MIN_INTERVAL_S = float(os.getenv("SUPPLIES_API_MIN_INTERVAL_S", "2.0"))

# Timezone helpers (Moscow)
try:
    from zoneinfo import ZoneInfo  # Python 3.9+
    MOSCOW_TZ = ZoneInfo("Europe/Moscow")
except Exception:  # Fallback to fixed UTC+3 if zoneinfo unavailable
    MOSCOW_TZ = timezone(timedelta(hours=3))

# Настройки маржи по умолчанию
DEFAULT_MARGIN_SETTINGS = {
    "tax": 6.0,         # Налог
    "storage": 0.5,     # Хранение
    "receiving": 1.0,   # Приёмка
    "acquiring": 1.7,   # Эквайринг
    "scheme": "FBW",   # Схема работы с WB
    "warehouse": "",    # Склад поставки
    "warehouse_coef": 0.0,  # Коэффициент логистики склада
    "localization_index": 1.0,  # Индекс локализации
}



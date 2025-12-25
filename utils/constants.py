# -*- coding: utf-8 -*-
"""Константы приложения"""
import os
from datetime import timedelta, timezone

# Версия приложения
APP_VERSION = "1.0.1"

# FBS warehouses/stocks
FBS_WAREHOUSES_URL = "https://marketplace-api.wildberries.ru/api/v3/warehouses"
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
ACCEPT_COEFS_URL = "https://supplies-api.wildberries.ru/api/v1/acceptance/coefficients"

# FBW supplies API
FBW_SUPPLIES_LIST_URL = "https://supplies-api.wildberries.ru/api/v1/supplies"
FBW_SUPPLY_DETAILS_URL = "https://supplies-api.wildberries.ru/api/v1/supplies/{id}"
FBW_SUPPLY_GOODS_URL = "https://supplies-api.wildberries.ru/api/v1/supplies/{id}/goods"
FBW_SUPPLY_PACKAGE_URL = "https://supplies-api.wildberries.ru/api/v1/supplies/{id}/package"

# Wildberries Content API
WB_CARDS_LIST_URL = "https://content-api.wildberries.ru/content/v2/get/cards/list"
WB_CARDS_UPDATE_URL = "https://content-api.wildberries.ru/content/v2/cards/update"

# Stocks API
STOCKS_API_URL = "https://statistics-api.wildberries.ru/api/v1/supplier/stocks"

# Finance report API
FIN_REPORT_URL = "https://statistics-api.wildberries.ru/api/v5/supplier/reportDetailByPeriod"

# Cache directory
CACHE_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "cache")
if not os.path.isdir(CACHE_DIR):
    os.makedirs(CACHE_DIR, exist_ok=True)

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



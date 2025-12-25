# Инструкции по завершению рефакторинга

## Текущий статус

✅ Создано:
- `utils/constants.py` - все константы приложения
- `models.py` - модели базы данных
- `REFACTORING_PLAN.md` - план рефакторинга

## Следующие шаги

### 1. Создать утилиты (utils/)

#### utils/helpers.py
Включить функции:
- `to_moscow()` - конвертация времени в московское
- `format_int_thousands()` - форматирование чисел
- `format_money_ru()` - форматирование денег
- `format_dmy()` - форматирование дат
- `parse_date()` - парсинг дат
- `parse_wb_datetime()` - парсинг дат WB
- `_parse_iso_datetime()` - парсинг ISO дат
- `_fmt_dt_moscow()` - форматирование дат в московском времени
- `time_ago_ru()` - относительное время на русском
- `extract_nm()` - извлечение nm_id
- `days_left_from_str()` - дни до даты
- `_fbw_status_from_id()` - статус FBW из ID
- И другие вспомогательные функции

#### utils/cache.py
Включить функции:
- `load_products_cache()`, `save_products_cache()`
- `load_articles_cache()`, `save_articles_cache()`
- `load_stocks_cache()`, `save_stocks_cache()`
- `load_fbs_supplies_cache()`, `save_fbs_supplies_cache()`
- `load_fbw_supplies_cache()`, `save_fbw_supplies_cache()`
- `load_fbw_supplies_detailed_cache()`, `save_fbw_supplies_detailed_cache()`
- `load_orders_cache_meta()`, `save_orders_cache_meta()`
- `load_orders_period_cache()`, `save_orders_period_cache()`
- `load_fbs_tasks_cache()`, `save_fbs_tasks_cache()`
- `load_last_results()`, `save_last_results()`
- `load_user_margin_settings()`, `save_user_margin_settings()`
- `_get_cache_dir()` - получение директории кэша
- `_cache_path_for_user()` - путь к кэшу пользователя
- И другие функции кэширования

#### utils/api.py
Включить функции:
- `get_with_retry()` - запрос с повторами
- `get_with_retry_json()` - запрос с повторами (JSON)
- `fetch_orders_page()` - получение страницы заказов
- `fetch_orders_range()` - получение заказов за период
- `fetch_sales_page()` - получение страницы продаж
- `fetch_sales_range()` - получение продаж за период
- `fetch_finance_report()` - получение финансового отчета
- `fetch_fbw_supplies_list()` - список поставок FBW
- `fetch_fbw_supply_details()` - детали поставки FBW
- `fetch_fbw_supply_goods()` - товары поставки FBW
- `fetch_fbw_supply_packages()` - упаковки поставки FBW
- И другие функции для работы с API

#### utils/progress.py
Включить функции:
- `_set_orders_progress()` - установка прогресса заказов
- `_clear_orders_progress()` - очистка прогресса заказов
- `_set_finance_progress()` - установка прогресса финансов
- `_get_finance_progress()` - получение прогресса финансов
- `_clear_finance_progress()` - очистка прогресса финансов
- Глобальные переменные: `ORDERS_PROGRESS`, `FINANCE_PROGRESS`, `FINANCE_RESULTS`, `FINANCE_LOADING`

#### utils/margin.py
Включить функции:
- `load_user_margin_settings()` - загрузка настроек маржи
- `save_user_margin_settings()` - сохранение настроек маржи
- `DEFAULT_MARGIN_SETTINGS` - настройки по умолчанию

### 2. Создать blueprints (blueprints/)

Каждый blueprint должен:
1. Импортировать необходимые зависимости из utils и models
2. Создать Blueprint объект
3. Определить роуты с декораторами
4. Экспортировать blueprint для регистрации в app.py

#### Пример структуры blueprint:

```python
# blueprints/orders.py
from flask import Blueprint, render_template, request, current_user
from flask_login import login_required
from models import db
from utils.constants import *
from utils.helpers import *
from utils.cache import *
from utils.api import *

orders_bp = Blueprint('orders', __name__)

@orders_bp.route("/orders", methods=["GET", "POST"])
@login_required
def index():
    # Код из app.py функции index()
    pass

# Экспорт
__all__ = ['orders_bp']
```

### 3. Обновить app.py

1. Импортировать все blueprints
2. Зарегистрировать их через `app.register_blueprint()`
3. Удалить старые роуты
4. Оставить только:
   - Инициализацию Flask приложения
   - Настройку БД
   - Настройку логирования
   - Middleware (before_request)
   - Регистрацию blueprints
   - Запуск приложения

## Порядок создания blueprints

Рекомендуемый порядок (от простых к сложным):

1. `auth.py` - авторизация (простой, мало зависимостей)
2. `changelog.py` - changelog (простой)
3. `profile.py` - профиль (средний)
4. `notifications.py` - уведомления (средний)
5. `orders.py` - аналитика заказов (сложный, много зависимостей)
6. `fbs.py` - заказы FBS (сложный)
7. `dbs.py` - заказы DBS (средний)
8. `fbs_stock.py` - остатки FBS (средний)
9. `fbw.py` - поставки FBW (средний)
10. `fbw_planning.py` - планирование FBW (сложный)
11. `coefficients.py` - коэффициенты (средний)
12. `products.py` - товары (сложный)
13. `reports.py` - отчёты (сложный)
14. `stocks.py` - остатки (средний)
15. `tools.py` - инструменты (сложный)
16. `admin.py` - админка (средний)

## Тестирование

После создания каждого модуля:
1. Проверить импорты
2. Проверить синтаксис (python -m py_compile)
3. Протестировать функционал вручную
4. Убедиться, что нет циклических импортов

## Важные замечания

1. Все функции, которые используют `current_user`, должны быть внутри request context
2. Функции кэширования должны работать как с `current_user`, так и с `user_id` параметром
3. Все импорты должны быть относительными или абсолютными (не циклическими)
4. При переносе кода проверять все зависимости



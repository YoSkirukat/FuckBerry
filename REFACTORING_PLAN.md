# План рефакторинга app.py

## Структура модулей

### Созданные модули:
- ✅ `utils/constants.py` - все константы приложения
- ✅ `models.py` - модели базы данных

### Модули для создания:

#### 1. Утилиты (utils/)
- `utils/helpers.py` - вспомогательные функции (format_*, parse_*, to_moscow, etc.)
- `utils/cache.py` - функции кэширования (load_*, save_*, clear_*)
- `utils/api.py` - функции для работы с API (fetch_*, get_with_retry, etc.)
- `utils/progress.py` - управление прогрессом длительных операций
- `utils/margin.py` - функции для работы с настройками маржи

#### 2. Blueprints (blueprints/)
- `blueprints/orders.py` - аналитика заказов (/orders)
- `blueprints/fbs.py` - заказы FBS (/fbs)
- `blueprints/dbs.py` - заказы DBS (/dbs)
- `blueprints/fbs_stock.py` - остатки на складе FBS (/fbs-stock)
- `blueprints/fbw.py` - список поставок (/fbw)
- `blueprints/fbw_planning.py` - планирование поставки (/fbw/planning)
- `blueprints/coefficients.py` - коэффициенты приёмки (/coefficients)
- `blueprints/products.py` - товар (/products)
- `blueprints/reports.py` - отчёты (/report/orders, /report/finance, /report/sales)
- `blueprints/stocks.py` - остатки на складах (/stocks)
- `blueprints/tools.py` - инструменты (/tools/prices, /tools/labels)
- `blueprints/profile.py` - настройки (/profile)
- `blueprints/admin.py` - админка (/admin/users)
- `blueprints/auth.py` - авторизация (/login, /logout)
- `blueprints/notifications.py` - уведомления (/api/notifications/*)
- `blueprints/changelog.py` - changelog (/changelog)

#### 3. Основной файл
- `app.py` - инициализация приложения, регистрация blueprints, middleware

## Порядок выполнения

1. ✅ Создать базовые модули (constants, models)
2. Создать утилиты (helpers, cache, api, progress, margin)
3. Создать blueprints по одному, начиная с самых простых
4. Обновить app.py для использования blueprints
5. Протестировать каждый модуль

## Зависимости между модулями

- Все blueprints зависят от:
  - `models` - модели БД
  - `utils.constants` - константы
  - `utils.helpers` - вспомогательные функции
  - `utils.cache` - кэширование
  - `utils.api` - API функции

- `app.py` зависит от всех blueprints



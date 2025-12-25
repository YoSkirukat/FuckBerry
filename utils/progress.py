# -*- coding: utf-8 -*-
"""Управление прогрессом длительных операций"""
from typing import Any

# Глобальные переменные для хранения прогресса
ORDERS_PROGRESS: dict[int, dict[str, object]] = {}
FINANCE_PROGRESS: dict[int, dict[str, object]] = {}
FINANCE_RESULTS: dict[int, dict[str, Any]] = {}  # Хранит результаты финансового отчета по user_id
FINANCE_LOADING: dict[int, bool] = {}  # Флаг загрузки финансового отчета


def set_orders_progress(user_id: int, total: int, done: int, key: str | None = None) -> None:
    """Устанавливает прогресс загрузки заказов"""
    try:
        current = ORDERS_PROGRESS.get(user_id) or {}
        if key is not None and current.get("key") not in (None, key):
            # New batch -> reset
            current = {"total": 0, "done": 0}
        prev_total = int(current.get("total", 0) or 0)
        prev_done = int(current.get("done", 0) or 0)
        new_total = max(prev_total, max(0, int(total)))
        new_done = max(prev_done, max(0, int(done)))
        ORDERS_PROGRESS[user_id] = {"key": key, "total": new_total, "done": new_done}
    except Exception:
        pass


def clear_orders_progress(user_id: int, key: str | None = None) -> None:
    """Очищает прогресс загрузки заказов"""
    try:
        cur = ORDERS_PROGRESS.get(user_id)
        if cur is None:
            return
        if key is None or cur.get("key") == key:
            del ORDERS_PROGRESS[user_id]
    except Exception:
        pass


def set_finance_progress(user_id: int, current: int, total: int, period: str = "") -> None:
    """Устанавливает прогресс загрузки финансового отчета"""
    try:
        FINANCE_PROGRESS[user_id] = {"current": current, "total": total, "period": period}
    except Exception:
        pass


def get_finance_progress(user_id: int) -> dict[str, object]:
    """Получает прогресс загрузки финансового отчета"""
    return FINANCE_PROGRESS.get(user_id) or {"current": 0, "total": 0, "period": ""}


def clear_finance_progress(user_id: int) -> None:
    """Очищает прогресс загрузки финансового отчета"""
    try:
        if user_id in FINANCE_PROGRESS:
            del FINANCE_PROGRESS[user_id]
    except Exception:
        pass



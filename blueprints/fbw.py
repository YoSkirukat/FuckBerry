# -*- coding: utf-8 -*-
"""Blueprint для поставок FBW"""
from flask import Blueprint, render_template
from flask_login import login_required, current_user
from utils.cache import load_fbw_supplies_cache

fbw_bp = Blueprint('fbw', __name__)


def _filter_fbw_display_items(items: list[dict] | None) -> list[dict]:
    """Фильтрует список поставок FBW для отображения в UI"""
    if not items:
        return []
    result: list[dict] = []
    for it in items:
        try:
            status = str((it or {}).get("status") or "")
            if "Не запланировано" in status:
                continue
            result.append(it)
        except Exception:
            result.append(it)
    return result


@fbw_bp.route("/fbw", methods=["GET"]) 
@login_required
def fbw_supplies_page():
    """Страница поставок FBW"""
    token = (current_user.wb_token or "") if current_user.is_authenticated else ""
    error = None
    supplies: list[dict] = []
    generated_at = ""
    
    # Загружаем только кэш для быстрого начального отображения (если есть)
    cached = load_fbw_supplies_cache() or {}
    if cached and cached.get("_user_id") == (current_user.id if current_user.is_authenticated else None):
        supplies = cached.get("items", [])
        generated_at = cached.get("updated_at", "")
    
    if not token:
        error = "Укажите API токен в профиле"

    # Скрываем черновики ("Не запланировано") из списка для отображения
    supplies = _filter_fbw_display_items(supplies)

    return render_template(
        "fbw_supplies.html",
        error=error,
        supplies=supplies,
        generated_at=generated_at,
    )



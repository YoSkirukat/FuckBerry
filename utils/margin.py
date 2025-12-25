# -*- coding: utf-8 -*-
"""Функции для работы с настройками маржи"""
import os
import json
from utils.constants import DEFAULT_MARGIN_SETTINGS, CACHE_DIR


def _get_cache_dir() -> str:
    """Получает директорию для кэша"""
    cache_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "cache")
    os.makedirs(cache_dir, exist_ok=True)
    return cache_dir


def load_user_margin_settings(user_id: int) -> dict:
    """Загружает настройки маржи для пользователя"""
    try:
        cache_dir = _get_cache_dir()
        path = os.path.join(cache_dir, f"margin_settings_{user_id}.json")
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f) or {}
            result = DEFAULT_MARGIN_SETTINGS.copy()
            for key, default_val in DEFAULT_MARGIN_SETTINGS.items():
                val = data.get(key, default_val)
                if key in ["scheme", "warehouse"]:
                    # строковые значения
                    result[key] = str(val or default_val)
                else:
                    # числовые значения
                    try:
                        result[key] = float(val)
                    except Exception:
                        result[key] = default_val
            return result
    except Exception as e:
        print(f"Ошибка чтения настроек маржи: {e}")
    return DEFAULT_MARGIN_SETTINGS.copy()


def save_user_margin_settings(user_id: int, settings: dict) -> dict:
    """Сохраняет настройки маржи для пользователя"""
    normalized = DEFAULT_MARGIN_SETTINGS.copy()
    for key, default_val in DEFAULT_MARGIN_SETTINGS.items():
        val = settings.get(key, default_val)
        if key in ["scheme", "warehouse"]:
            # Строковые значения
            normalized[key] = str(val or default_val)
        else:
            # Числовые значения
            try:
                normalized[key] = float(val)
            except Exception:
                normalized[key] = default_val
    try:
        cache_dir = _get_cache_dir()
        path = os.path.join(cache_dir, f"margin_settings_{user_id}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(normalized, f, ensure_ascii=False)
    except Exception as e:
        print(f"Ошибка сохранения настроек маржи: {e}")
    return normalized



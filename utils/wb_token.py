# -*- coding: utf-8 -*-
"""Проверки доступа к API Wildberries по учётке и по сроку JWT токена."""
from __future__ import annotations

from datetime import datetime
from typing import Any

import jwt

from utils.constants import MOSCOW_TZ, WB_TOKEN_EXPIRY_BANNER_DAYS


def is_user_valid_for_app(u: Any) -> bool:
    """Активная учётка и даты valid_from / valid_to (как при доступе к интерфейсу)."""
    if u is None or not getattr(u, "is_active", True):
        return False
    today = datetime.now(MOSCOW_TZ).date()
    vf = getattr(u, "valid_from", None)
    vt = getattr(u, "valid_to", None)
    if vf and today < vf:
        return False
    if vt and today > vt:
        return False
    return True


def wb_jwt_token_expired(token: str) -> bool:
    """
    True, если строка успешно декодируется как JWT, в payload есть exp и срок истёк.
    Не-JWT или отсутствие exp — не считаем просрочкой по календарю (решение на стороне WB).
    """
    if not token:
        return False
    raw = token.strip()
    if raw.lower().startswith("bearer "):
        raw = raw[7:].strip()
    try:
        decoded = jwt.decode(raw, options={"verify_signature": False})
    except Exception:
        return False
    exp = decoded.get("exp")
    if exp is None:
        return False
    try:
        exp_dt = datetime.fromtimestamp(float(exp), tz=MOSCOW_TZ)
    except (TypeError, ValueError, OSError):
        return False
    return exp_dt <= datetime.now(MOSCOW_TZ)


def user_may_call_wb_api(u: Any) -> bool:
    """
    Запросы к API Wildberries только для активного пользователя с непустым токеном,
    в пределах valid_from / valid_to и с неистёкшим JWT (если в токене есть exp).
    """
    if u is None:
        return False
    tok = (getattr(u, "wb_token", None) or "").strip()
    if not tok:
        return False
    if not is_user_valid_for_app(u):
        return False
    if wb_jwt_token_expired(tok):
        return False
    return True


def effective_wb_api_token(u: Any) -> str:
    """Токен для вызова WB или пустая строка, если вызывать нельзя."""
    if not user_may_call_wb_api(u):
        return ""
    return (getattr(u, "wb_token", None) or "").strip()


def wb_api_key_expiry_summary(token: str | None) -> dict[str, Any]:
    """
    Сводка по сроку действия JWT WB API ключа (поле exp) — для админки и отчётов.

    Возвращает dict:
      kind: empty | not_jwt | no_exp | expired | valid
      days_left: int | None — полных суток до exp (для valid); 0 если истёк
      expires_at_fmt: str | None — дата/время окончания, МСК
    """
    empty: dict[str, Any] = {
        "kind": "empty",
        "days_left": None,
        "expires_at_fmt": None,
    }
    if token is None or not str(token).strip():
        return empty
    raw = str(token).strip()
    if raw.lower().startswith("bearer "):
        raw = raw[7:].strip()
    try:
        decoded = jwt.decode(raw, options={"verify_signature": False})
    except Exception:
        return {"kind": "not_jwt", "days_left": None, "expires_at_fmt": None}
    exp = decoded.get("exp")
    if exp is None:
        return {"kind": "no_exp", "days_left": None, "expires_at_fmt": None}
    try:
        exp_dt = datetime.fromtimestamp(float(exp), tz=MOSCOW_TZ)
    except (TypeError, ValueError, OSError):
        return {"kind": "no_exp", "days_left": None, "expires_at_fmt": None}
    expires_at_fmt = exp_dt.strftime("%d.%m.%Y %H:%M") + " МСК"
    now = datetime.now(MOSCOW_TZ)
    if exp_dt <= now:
        return {
            "kind": "expired",
            "days_left": 0,
            "expires_at_fmt": expires_at_fmt,
        }
    days_left = (exp_dt - now).days
    return {
        "kind": "valid",
        "days_left": days_left,
        "expires_at_fmt": expires_at_fmt,
    }


def wb_api_key_expiry_banner(token: str | None, within_days: int | None = None) -> dict[str, Any]:
    """
    Параметры верхнего баннера (как у подписки): показать, если до exp JWT осталось
    от 0 до within_days полных суток включительно.
    """
    max_days = WB_TOKEN_EXPIRY_BANNER_DAYS if within_days is None else within_days
    s = wb_api_key_expiry_summary(token)
    if s.get("kind") != "valid":
        return {"show": False}
    dl = s.get("days_left")
    if dl is None or not (0 <= dl <= max_days):
        return {"show": False}
    fmt = s.get("expires_at_fmt") or ""
    end_date = fmt.split()[0].strip() if fmt else ""
    return {"show": True, "days_left": dl, "end_date": end_date}


def token_for_wb_request(user: Any, form_token: str | None) -> str:
    """
    Токен для запроса: непустой из формы (если не просрочен по JWT exp),
    иначе сохранённый в профиле при выполнении user_may_call_wb_api.
    """
    ft = (form_token or "").strip()
    if ft:
        if wb_jwt_token_expired(ft):
            return ""
        return ft
    return effective_wb_api_token(user)

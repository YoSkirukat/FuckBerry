# -*- coding: utf-8 -*-
"""Blueprint для профиля пользователя"""
from typing import Any
from flask import Blueprint, render_template, request, redirect, url_for, flash
from flask_login import login_required, current_user
from models import db
from utils.constants import MOSCOW_TZ
from utils.api import fetch_seller_info
from utils.cache import load_fbw_supplies_detailed_cache, load_orders_cache_meta, is_supplies_cache_fresh, is_orders_cache_fresh
from datetime import datetime
import jwt

profile_bp = Blueprint('profile', __name__)


def decode_token_info(token: str) -> dict[str, Any] | None:
    """Декодирует информацию из JWT токена"""
    if not token:
        return None
    
    try:
        # Decode JWT token without verification (we only need the payload)
        decoded = jwt.decode(token, options={"verify_signature": False})
        
        token_info = {}
        
        # Extract creation date (iat - issued at)
        if 'iat' in decoded:
            iat_timestamp = decoded['iat']
            token_info['created_at'] = datetime.fromtimestamp(iat_timestamp, tz=MOSCOW_TZ)
        
        # Extract expiration date (exp - expiration)
        if 'exp' in decoded:
            exp_timestamp = decoded['exp']
            token_info['expires_at'] = datetime.fromtimestamp(exp_timestamp, tz=MOSCOW_TZ)
            
            # Calculate days until expiration
            now = datetime.now(MOSCOW_TZ)
            if token_info['expires_at'] > now:
                days_left = (token_info['expires_at'] - now).days
                token_info['days_until_expiry'] = days_left
                token_info['is_expired'] = False
            else:
                token_info['days_until_expiry'] = 0
                token_info['is_expired'] = True
        else:
            token_info['is_expired'] = None
            token_info['days_until_expiry'] = None
        
        return token_info
    except Exception:
        return None


@profile_bp.route("/profile", methods=["GET"]) 
@login_required
def profile():
    """Страница профиля"""
    seller_info: dict[str, Any] | None = None
    token_info: dict[str, Any] | None = None
    supplies_cache_info: dict[str, Any] | None = None
    orders_cache_info: dict[str, Any] | None = None
    token = current_user.wb_token or ""
    if token:
        try:
            seller_info = fetch_seller_info(token)
            token_info = decode_token_info(token)
            
            # Информация о кэше поставок (безопасная загрузка)
            supplies_cache_info = None
            try:
                supplies_cache = load_fbw_supplies_detailed_cache()
                if supplies_cache:
                    # Определяем период кэша по ключам дней
                    supplies_by_date = supplies_cache.get("supplies_by_date") or {}
                    period_from = None
                    period_to = None
                    try:
                        if supplies_by_date:
                            keys = sorted(supplies_by_date.keys())
                            period_from = keys[0]
                            period_to = keys[-1]
                    except Exception:
                        pass

                    supplies_cache_info = {
                        "last_updated": supplies_cache.get("last_updated"),
                        "total_supplies": supplies_cache.get("total_supplies_processed", 0),
                        "is_fresh": is_supplies_cache_fresh(),
                        "cache_period_from": period_from,
                        "cache_period_to": period_to,
                    }
            except Exception as e:
                print(f"Ошибка загрузки кэша поставок: {e}")
                supplies_cache_info = None
            
            # Информация о кэше заказов (безопасная загрузка)
            orders_cache_info = None
            try:
                orders_meta = load_orders_cache_meta()
                if orders_meta:
                    orders_cache_info = {
                        "last_updated": orders_meta.get("last_updated"),
                        "date_from": orders_meta.get("date_from"),
                        "date_to": orders_meta.get("date_to"),
                        "total_orders_cached": orders_meta.get("total_orders_cached", 0),
                        "is_fresh": is_orders_cache_fresh(),
                        "cache_version": orders_meta.get("cache_version", "1.0")
                    }
            except Exception as e:
                print(f"Ошибка загрузки кэша заказов: {e}")
                orders_cache_info = None
        except Exception:
            seller_info = None
            token_info = None
            supplies_cache_info = None
            orders_cache_info = None
    validity_status = None
    if current_user.valid_from or current_user.valid_to:
        today = datetime.now(MOSCOW_TZ).date()
        active = True
        if current_user.valid_from and today < current_user.valid_from:
            active = False
        if current_user.valid_to and today > current_user.valid_to:
            active = False
        validity_status = "active" if active and current_user.is_active else "inactive"
    return render_template(
        "profile.html",
        message=None,
        token=token,
        seller_info=seller_info,
        token_info=token_info,
        orders_cache_info=orders_cache_info,
        supplies_cache_info=supplies_cache_info,
        valid_from=current_user.valid_from.strftime("%d.%m.%Y") if current_user.valid_from else None,
        valid_to=current_user.valid_to.strftime("%d.%m.%Y") if current_user.valid_to else None,
        validity_status=validity_status,
    )


@profile_bp.route("/profile/token", methods=["POST"]) 
@login_required
def profile_token():
    """Обновление токена API"""
    new_token = request.form.get("token", "").strip()
    try:
        current_user.wb_token = new_token or None
        db.session.commit()
        if new_token:
            hint = []
            if not (current_user.phone and current_user.email and current_user.shipper_address):
                hint.append(" Заполните телефон, email и адрес склада для этикеток в профиле.")
            flash("Токен успешно добавлен." + (hint[0] if hint else ""))
        else:
            flash("Токен удален")
    except Exception:
        db.session.rollback()
        flash("Ошибка сохранения токена")
    return redirect(url_for("profile.profile"))


@profile_bp.route("/profile/shipping", methods=["POST"]) 
@login_required
def profile_shipping():
    """Обновление реквизитов доставки"""
    current_user.shipper_name = (request.form.get("shipper_name") or "").strip() or None
    current_user.contact_person = (request.form.get("contact_person") or "").strip() or None
    current_user.phone = (request.form.get("phone") or "").strip() or None
    current_user.email = (request.form.get("email") or "").strip() or None
    current_user.shipper_address = (request.form.get("shipper_address") or "").strip() or None
    try:
        db.session.commit()
        flash("Реквизиты сохранены")
    except Exception:
        db.session.rollback()
        flash("Ошибка сохранения реквизитов")
    return redirect(url_for("profile.profile"))


@profile_bp.route("/profile/tax-rate", methods=["POST"]) 
@login_required
def profile_tax_rate():
    """Обновление налоговой ставки"""
    tax_rate_str = request.form.get("tax_rate", "").strip()
    try:
        if tax_rate_str:
            tax_rate = float(tax_rate_str)
            if tax_rate < 0 or tax_rate > 100:
                flash("Налоговая ставка должна быть от 0 до 100%")
                return redirect(url_for("profile.profile"))
            current_user.tax_rate = tax_rate
        else:
            current_user.tax_rate = None
        db.session.commit()
        flash("Налоговая ставка сохранена")
    except ValueError:
        flash("Ошибка: неверное значение налоговой ставки")
    except Exception:
        db.session.rollback()
        flash("Ошибка сохранения налоговой ставки")
    return redirect(url_for("profile.profile"))


@profile_bp.route("/profile/password", methods=["POST"]) 
@login_required
def profile_password():
    """Обновление пароля"""
    old_password = (request.form.get("old_password", "") or "").strip()
    new_password = (request.form.get("new_password", "") or "").strip()
    if not old_password or not new_password:
        flash("Заполните оба поля")
        return redirect(url_for("profile.profile"))
    if current_user.password != old_password:
        flash("Текущий пароль неверен")
        return redirect(url_for("profile.profile"))
    if len(new_password) < 4:
        flash("Новый пароль слишком короткий (мин. 4 символа)")
        return redirect(url_for("profile.profile"))
    if new_password == old_password:
        flash("Новый пароль совпадает с текущим")
        return redirect(url_for("profile.profile"))
    try:
        current_user.password = new_password
        db.session.commit()
        flash("Пароль обновлён")
    except Exception:
        db.session.rollback()
        flash("Ошибка обновления пароля")
    return redirect(url_for("profile.profile"))


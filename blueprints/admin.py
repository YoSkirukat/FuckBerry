# -*- coding: utf-8 -*-
"""Blueprint для админки"""
import os
from functools import wraps
from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from flask_login import login_required, current_user
from models import User, db
from datetime import datetime
from utils.cache import _cache_path_for_user_id

admin_bp = Blueprint('admin', __name__)


def admin_required(f):
    """Декоратор для проверки прав администратора"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not current_user.is_authenticated or not current_user.is_admin:
            flash("У вас нет прав для доступа к этой странице", "error")
            return redirect(url_for("orders.index"))
        return f(*args, **kwargs)
    return decorated_function


@admin_bp.route("/admin/users", methods=["GET"]) 
@login_required
@admin_required
def admin_users():
    """Страница управления пользователями"""
    users = User.query.order_by(User.id.asc()).all()
    return render_template("admin_users.html", users=users, message=None)


@admin_bp.route("/admin/users/create", methods=["POST"]) 
@login_required
@admin_required
def admin_users_create():
    """Создание нового пользователя"""
    username = request.form.get("username", "").strip()
    password = request.form.get("password", "")
    display_name = request.form.get("display_name", "").strip() or None
    is_admin = bool(request.form.get("is_admin"))
    vf = request.form.get("valid_from") or None
    vt = request.form.get("valid_to") or None
    if not username or not password:
        flash("Укажите логин и пароль")
        return redirect(url_for("admin.admin_users"))
    if User.query.filter_by(username=username).first():
        flash("Такой логин уже существует")
        return redirect(url_for("admin.admin_users"))
    try:
        vf_d = None
        vt_d = None
        if vf:
            try:
                vf_d = datetime.strptime(vf, "%Y-%m-%d").date()
            except Exception:
                vf_d = None
        if vt:
            try:
                vt_d = datetime.strptime(vt, "%Y-%m-%d").date()
            except Exception:
                vt_d = None
        u = User(username=username, password=password, display_name=display_name, is_admin=is_admin, is_active=True, valid_from=vf_d, valid_to=vt_d)
        db.session.add(u)
        db.session.commit()
        flash("Пользователь создан")
    except Exception as exc:
        try:
            db.session.rollback()
        except Exception:
            pass
        flash(f"Ошибка создания пользователя: {exc}")
    return redirect(url_for("admin.admin_users"))


@admin_bp.route("/admin/users/<int:user_id>/block", methods=["POST"]) 
@login_required
@admin_required
def admin_users_block(user_id: int):
    """Блокировка пользователя"""
    u = db.session.get(User, user_id)
    if u:
        try:
            u.is_active = False
            db.session.commit()
            flash("Пользователь заблокирован")
        except Exception:
            db.session.rollback()
            flash("Ошибка")
    return redirect(url_for("admin.admin_users"))


@admin_bp.route("/admin/users/<int:user_id>/unblock", methods=["POST"]) 
@login_required
@admin_required
def admin_users_unblock(user_id: int):
    """Разблокировка пользователя"""
    u = db.session.get(User, user_id)
    if u:
        try:
            u.is_active = True
            db.session.commit()
            flash("Пользователь разблокирован")
        except Exception:
            db.session.rollback()
            flash("Ошибка")
    return redirect(url_for("admin.admin_users"))


@admin_bp.route("/admin/users/<int:user_id>/reset", methods=["POST"]) 
@login_required
@admin_required
def admin_users_reset(user_id: int):
    """Сброс пароля пользователя"""
    new_pass = request.form.get("password", "")
    if not new_pass:
        flash("Укажите новый пароль")
        return redirect(url_for("admin.admin_users"))
    u = db.session.get(User, user_id)
    if u:
        try:
            u.password = new_pass
            db.session.commit()
            flash("Пароль обновлён")
        except Exception:
            db.session.rollback()
            flash("Ошибка")
    return redirect(url_for("admin.admin_users"))


@admin_bp.route("/admin/users/<int:user_id>/delete", methods=["POST"]) 
@login_required
@admin_required
def admin_users_delete(user_id: int):
    """Удаление пользователя"""
    u = db.session.get(User, user_id)
    if u:
        try:
            db.session.delete(u)
            db.session.commit()
            # Remove user's cache file if exists
            try:
                cache_path = _cache_path_for_user_id(user_id)
                if os.path.isfile(cache_path):
                    os.remove(cache_path)
            except Exception:
                pass
            flash("Пользователь удалён")
        except Exception:
            db.session.rollback()
            flash("Ошибка")
    return redirect(url_for("admin.admin_users"))


@admin_bp.route("/admin/users/<int:user_id>/validity", methods=["POST"]) 
@login_required
@admin_required
def admin_users_validity(user_id: int):
    """Обновление срока действия пользователя"""
    vf = request.form.get("valid_from") or None
    vt = request.form.get("valid_to") or None
    u = db.session.get(User, user_id)
    if u:
        try:
            vf_d = None
            vt_d = None
            if vf:
                try:
                    vf_d = datetime.strptime(vf, "%Y-%m-%d").date()
                except Exception:
                    vf_d = None
            if vt:
                try:
                    vt_d = datetime.strptime(vt, "%Y-%m-%d").date()
                except Exception:
                    vt_d = None
            u.valid_from = vf_d
            u.valid_to = vt_d
            db.session.commit()
            flash("Срок действия обновлён")
        except Exception as exc:
            try:
                db.session.rollback()
            except Exception:
                pass
            flash(f"Ошибка обновления срока: {exc}")
    return redirect(url_for("admin.admin_users"))


@admin_bp.route("/admin/users/<int:user_id>/display_name", methods=["POST"]) 
@login_required
@admin_required
def admin_users_display_name(user_id: int):
    """Обновление отображаемого имени пользователя"""
    display_name = request.form.get("display_name", "").strip() or None
    u = db.session.get(User, user_id)
    if u:
        try:
            u.display_name = display_name
            db.session.commit()
            flash("Имя пользователя обновлено")
        except Exception as exc:
            try:
                db.session.rollback()
            except Exception:
                pass
            flash(f"Ошибка обновления имени пользователя: {exc}")
    return redirect(url_for("admin.admin_users"))


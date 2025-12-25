# -*- coding: utf-8 -*-
"""Blueprint для авторизации"""
from datetime import datetime
from flask import Blueprint, render_template, request, redirect, url_for, session
from flask_login import login_required, login_user, logout_user, current_user
from models import User, db
from utils.constants import MOSCOW_TZ

auth_bp = Blueprint('auth', __name__)


@auth_bp.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        remember_flag = request.form.get("remember") in ("on", "true", "1")
        user = User.query.filter_by(username=username).first()
        # Authentication and account state checks
        if not user or user.password != password:
            return render_template("login.html", error="Неверный логин или пароль")
        if not user.is_active:
            return render_template("login.html", error="Ваша учетная запись заблокирована, обратитесь в техподдержку")
        # Check validity dates
        today = datetime.now(MOSCOW_TZ).date()
        if user.valid_from and today < user.valid_from:
            return render_template("login.html", error="Учётная запись ещё не активна")
        if user.valid_to and today > user.valid_to:
            return render_template("login.html", error="Срок действия вашей подписки истек")
        # Учитываем параметр "Запомнить меня":
        # - включаем постоянную сессию для продления cookie приложения
        # - активируем remember для Flask-Login, чтобы браузер сохранял токен на 30 дней
        session.permanent = bool(remember_flag)
        login_user(user, remember=bool(remember_flag))
        return redirect(request.args.get("next") or url_for("orders.index"))
    return render_template("login.html")


@auth_bp.route("/logout", methods=["GET"]) 
def logout():
    logout_user()
    return redirect(url_for("auth.login"))


def admin_required():
    """Проверка прав администратора"""
    if not current_user.is_authenticated or not current_user.is_admin:
        return False
    return True



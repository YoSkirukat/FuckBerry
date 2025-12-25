# -*- coding: utf-8 -*-
"""Blueprint для changelog"""
from flask import Blueprint, render_template, request, redirect, url_for
from flask_login import login_required, current_user
from utils.constants import MOSCOW_TZ
from utils.helpers import read_version, write_version, read_changelog_md, write_changelog_md
from datetime import datetime

changelog_bp = Blueprint('changelog', __name__)


@changelog_bp.route("/changelog")
def changelog_page():
    """Страница changelog"""
    md = read_changelog_md()
    return render_template("changelog.html", app_version=read_version(), md=md)


@changelog_bp.route("/changelog/edit", methods=["GET", "POST"]) 
@login_required
def changelog_edit():
    """Редактирование changelog (только для админов)"""
    if not current_user.is_admin:
        return redirect(url_for("changelog.changelog_page"))
    error = None
    message = None
    current_version = read_version()
    md_content = read_changelog_md()
    if request.method == "POST":
        try:
            new_version = (request.form.get("version") or "").strip()
            new_md = request.form.get("md_content")
            if new_md is not None:
                write_changelog_md(new_md)
                md_content = new_md
            if new_version:
                write_version(new_version)
                current_version = new_version
            message = "Сохранено"
        except Exception as exc:
            error = f"Ошибка: {exc}"
    return render_template(
        "changelog_edit.html",
        app_version=current_version,
        md_content=md_content,
        default_date=datetime.now(MOSCOW_TZ).strftime("%d.%m.%Y"),
        error=error,
        message=message,
    )



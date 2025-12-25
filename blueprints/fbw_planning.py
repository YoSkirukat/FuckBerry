# -*- coding: utf-8 -*-
"""Blueprint для планирования поставок FBW"""
from flask import Blueprint, render_template
from flask_login import login_required, current_user

fbw_planning_bp = Blueprint('fbw_planning', __name__)


@fbw_planning_bp.route("/fbw/planning", methods=["GET"])
@login_required
def fbw_planning_page():
    """Страница планирования поставки FBW"""
    token = (current_user.wb_token or "") if current_user.is_authenticated else ""
    error = None
    
    if not token:
        error = "Укажите API токен в профиле"
    
    return render_template(
        "fbw_planning.html",
        error=error,
        token=token
    )



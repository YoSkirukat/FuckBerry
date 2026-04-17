# -*- coding: utf-8 -*-
"""Blueprint для планирования поставок FBW"""
from flask import Blueprint, render_template
from flask_login import login_required, current_user
from utils.wb_token import effective_wb_api_token

fbw_planning_bp = Blueprint('fbw_planning', __name__)


@fbw_planning_bp.route("/fbw/planning", methods=["GET"])
@login_required
def fbw_planning_page():
    """Страница планирования поставки FBW"""
    token = effective_wb_api_token(current_user)
    error = None
    
    if not token:
        error = "Укажите API токен в профиле"
    
    return render_template(
        "fbw_planning.html",
        error=error,
        token=token
    )



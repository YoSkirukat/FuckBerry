# -*- coding: utf-8 -*-
"""Blueprint: Маркетинг и продвижение (рекламные кампании WB)."""
from datetime import datetime

from flask import Blueprint, jsonify, render_template, request
from flask_login import current_user, login_required

from utils.advertising import build_campaign_detail, build_marketing_payload, default_period
from utils.wb_token import effective_wb_api_token

marketing_bp = Blueprint("marketing", __name__)


def _parse_iso_date(value: str) -> str | None:
    value = (value or "").strip()
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date().isoformat()
    except ValueError:
        return None


def _fmt_date(iso: str) -> str:
    try:
        return datetime.strptime(iso, "%Y-%m-%d").strftime("%d.%m.%Y")
    except Exception:
        return iso


def _resolve_period():
    date_from, date_to = default_period()
    q_from = _parse_iso_date(request.args.get("date_from") or "")
    q_to = _parse_iso_date(request.args.get("date_to") or "")
    if q_from:
        date_from = q_from
    if q_to:
        date_to = q_to
    try:
        d0 = datetime.strptime(date_from, "%Y-%m-%d").date()
        d1 = datetime.strptime(date_to, "%Y-%m-%d").date()
    except ValueError:
        return {"error": "Некорректный период", "code": 400}
    if d1 < d0:
        return {"error": "Дата окончания раньше начала", "code": 400}
    if (d1 - d0).days > 30:
        return {"error": "Максимальный период — 31 день (лимит WB API)", "code": 400}
    return {"date_from": date_from, "date_to": date_to}


@marketing_bp.route("/marketing", methods=["GET"])
@login_required
def marketing_page():
    """Страница маркетинга и продвижения."""
    date_from, date_to = default_period()
    q_from = _parse_iso_date(request.args.get("date_from") or "")
    q_to = _parse_iso_date(request.args.get("date_to") or "")
    if q_from:
        date_from = q_from
    if q_to:
        date_to = q_to

    return render_template(
        "marketing.html",
        date_from_val=date_from,
        date_to_val=date_to,
        date_from_fmt=_fmt_date(date_from),
        date_to_fmt=_fmt_date(date_to),
    )


@marketing_bp.route("/marketing/campaign/<int:campaign_id>", methods=["GET"])
@login_required
def marketing_campaign_page(campaign_id: int):
    """Карточка рекламной кампании."""
    date_from, date_to = default_period()
    q_from = _parse_iso_date(request.args.get("date_from") or "")
    q_to = _parse_iso_date(request.args.get("date_to") or "")
    if q_from:
        date_from = q_from
    if q_to:
        date_to = q_to

    return render_template(
        "marketing_campaign.html",
        campaign_id=campaign_id,
        date_from_val=date_from,
        date_to_val=date_to,
        date_from_fmt=_fmt_date(date_from),
        date_to_fmt=_fmt_date(date_to),
    )


@marketing_bp.route("/api/marketing/data", methods=["GET"])
@login_required
def api_marketing_data():
    """Данные кампаний и товаров за период."""
    token = effective_wb_api_token(current_user)
    if not token:
        return jsonify({"error": "no_token", "message": "Укажите токен API в профиле"}), 401

    period = _resolve_period()
    if "error" in period:
        return jsonify({"error": "bad_dates", "message": period["error"]}), period["code"]

    include_budgets = (request.args.get("budgets") or "1").strip() != "0"

    try:
        payload = build_marketing_payload(
            token, period["date_from"], period["date_to"], include_budgets=include_budgets
        )
        return jsonify({"ok": True, **payload})
    except Exception as exc:
        status = getattr(getattr(exc, "response", None), "status_code", None)
        msg = str(exc)
        if status == 401:
            return jsonify({"error": "unauthorized", "message": "Токен отклонён API рекламы WB"}), 401
        if status == 429:
            return jsonify({"error": "rate_limit", "message": "Лимит запросов WB, повторите позже"}), 429
        return jsonify({"error": "api", "message": msg}), 502


@marketing_bp.route("/api/marketing/campaign/<int:campaign_id>", methods=["GET"])
@login_required
def api_marketing_campaign(campaign_id: int):
    """Детальные данные одной кампании."""
    token = effective_wb_api_token(current_user)
    if not token:
        return jsonify({"error": "no_token", "message": "Укажите токен API в профиле"}), 401

    period = _resolve_period()
    if "error" in period:
        return jsonify({"error": "bad_dates", "message": period["error"]}), period["code"]

    try:
        payload = build_campaign_detail(
            token, campaign_id, period["date_from"], period["date_to"]
        )
        return jsonify({"ok": True, **payload})
    except ValueError as exc:
        if str(exc) == "campaign_not_found":
            return jsonify({"error": "not_found", "message": "Кампания не найдена"}), 404
        return jsonify({"error": "bad_request", "message": str(exc)}), 400
    except Exception as exc:
        status = getattr(getattr(exc, "response", None), "status_code", None)
        msg = str(exc)
        if status == 401:
            return jsonify({"error": "unauthorized", "message": "Токен отклонён API рекламы WB"}), 401
        if status == 429:
            return jsonify({"error": "rate_limit", "message": "Лимит запросов WB, повторите позже"}), 429
        return jsonify({"error": "api", "message": msg}), 502

# -*- coding: utf-8 -*-
"""Blueprint для уведомлений"""
import json
from flask import Blueprint, request, jsonify
from flask_login import login_required, current_user
from models import Notification, db
from utils.constants import MOSCOW_TZ
from utils.notifications import (
    get_unread_notifications_count,
    get_user_notifications,
    mark_notification_as_read,
    mark_all_notifications_as_read,
    create_notification
)

notifications_bp = Blueprint('notifications', __name__)


@notifications_bp.route("/api/notifications/count", methods=["GET"])
@login_required
def api_notifications_count():
    """Получает количество непрочитанных уведомлений"""
    count = get_unread_notifications_count(current_user.id)
    return jsonify({"count": count})


@notifications_bp.route("/api/notifications", methods=["GET"])
@login_required
def api_notifications():
    """Получает уведомления пользователя"""
    limit = int(request.args.get('limit', 20))
    notifications = get_user_notifications(current_user.id, limit)
    
    result = []
    for notif in notifications:
        data = None
        if notif.data:
            try:
                data = json.loads(notif.data)
            except:
                pass
                
        # Форматируем время создания уведомления в московском времени
        if notif.created_at.tzinfo is None:
            moscow_time = notif.created_at.replace(tzinfo=MOSCOW_TZ)
        else:
            moscow_time = notif.created_at.astimezone(MOSCOW_TZ)
        
        formatted_time = moscow_time.strftime('%d.%m.%Y %H:%M')
        
        result.append({
            'id': notif.id,
            'title': notif.title,
            'message': notif.message,
            'type': notif.notification_type,
            'is_read': notif.is_read,
            'created_at': formatted_time,
            'data': data
        })
    
    return jsonify({"notifications": result})


@notifications_bp.route("/api/notifications/<int:notification_id>/read", methods=["POST"])
@login_required
def api_mark_notification_read(notification_id: int):
    """Отмечает уведомление как прочитанное"""
    success = mark_notification_as_read(notification_id, current_user.id)
    if success:
        return jsonify({"success": True})
    else:
        return jsonify({"error": "Notification not found"}), 404


@notifications_bp.route("/api/notifications/read-all", methods=["POST"])
@login_required
def api_mark_all_notifications_read():
    """Отмечает все уведомления как прочитанные"""
    count = mark_all_notifications_as_read(current_user.id)
    return jsonify({"success": True, "count": count})


@notifications_bp.route("/api/notifications/<int:notification_id>/delete", methods=["DELETE"])
@login_required
def api_delete_notification(notification_id: int):
    """Удаляет уведомление"""
    notification = Notification.query.filter_by(id=notification_id, user_id=current_user.id).first()
    if notification:
        db.session.delete(notification)
        db.session.commit()
        return jsonify({"success": True})
    else:
        return jsonify({"error": "Notification not found"}), 404


@notifications_bp.route("/api/notifications/delete-all", methods=["DELETE"])
@login_required
def api_delete_all_notifications():
    """Удаляет все уведомления пользователя"""
    count = Notification.query.filter_by(user_id=current_user.id).delete()
    db.session.commit()
    return jsonify({"success": True, "count": count})


@notifications_bp.route("/api/notifications/test", methods=["POST"])
@login_required
def test_notification():
    """Создает тестовое уведомление для отладки"""
    try:
        create_notification(
            user_id=current_user.id,
            title="Тестовое уведомление",
            message="Это тестовое уведомление для проверки системы",
            notification_type="test",
            data={"test": True}
        )
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500



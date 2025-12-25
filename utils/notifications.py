# -*- coding: utf-8 -*-
"""Функции для работы с уведомлениями"""
import json
from datetime import datetime
from typing import List, Dict, Any
from models import Notification, db
from utils.constants import MOSCOW_TZ


def create_notification(user_id: int, title: str, message: str, notification_type: str, data: dict = None, created_at: datetime = None) -> Notification:
    """Создает новое уведомление для пользователя"""
    # Используем переданное время или текущее московское время
    notification_time = created_at if created_at else datetime.now(MOSCOW_TZ)
    
    notification = Notification(
        user_id=user_id,
        title=title,
        message=message,
        notification_type=notification_type,
        data=json.dumps(data) if data else None,
        created_at=notification_time
    )
    db.session.add(notification)
    db.session.commit()
    return notification


def get_unread_notifications_count(user_id: int) -> int:
    """Получает количество непрочитанных уведомлений для пользователя"""
    return Notification.query.filter_by(user_id=user_id, is_read=False).count()


def get_user_notifications(user_id: int, limit: int = 20) -> List[Notification]:
    """Получает последние уведомления для пользователя"""
    return Notification.query.filter_by(user_id=user_id)\
        .order_by(Notification.created_at.desc())\
        .limit(limit).all()


def mark_notification_as_read(notification_id: int, user_id: int) -> bool:
    """Отмечает уведомление как прочитанное"""
    notification = Notification.query.filter_by(id=notification_id, user_id=user_id).first()
    if notification:
        notification.is_read = True
        db.session.commit()
        return True
    return False


def mark_all_notifications_as_read(user_id: int) -> int:
    """Отмечает все уведомления пользователя как прочитанные"""
    count = Notification.query.filter_by(user_id=user_id, is_read=False).update({"is_read": True})
    db.session.commit()
    return count



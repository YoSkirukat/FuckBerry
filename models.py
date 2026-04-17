# -*- coding: utf-8 -*-
"""Модели базы данных"""
from datetime import datetime
from flask_sqlalchemy import SQLAlchemy
from flask_login import UserMixin
from utils.constants import MOSCOW_TZ

# db будет инициализирован в app.py
db = SQLAlchemy()


class User(UserMixin, db.Model):
    __tablename__ = "users"
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(120), unique=True, nullable=False)
    password = db.Column(db.String(255), nullable=False)  # store hashed in production
    is_admin = db.Column(db.Boolean, default=False)
    is_active = db.Column(db.Boolean, default=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    wb_token = db.Column(db.Text, nullable=True)
    valid_from = db.Column(db.Date, nullable=True)
    valid_to = db.Column(db.Date, nullable=True)
    phone = db.Column(db.String(64), nullable=True)
    email = db.Column(db.String(120), nullable=True)
    shipper_name = db.Column(db.String(255), nullable=True)
    shipper_address = db.Column(db.String(255), nullable=True)
    contact_person = db.Column(db.String(255), nullable=True)
    display_name = db.Column(db.String(255), nullable=True)
    tax_rate = db.Column(db.Float, nullable=True)
    # Название организации для шапки сайта (вручную; при лимитах WB API не подставляется автоматически)
    org_display_name = db.Column(db.String(255), nullable=True)

    def get_id(self):
        return str(self.id)


def delete_user_with_related(user_id: int) -> bool:
    """
    Удаляет пользователя и все строки с FK на users.id (каскад в ORM — в SQLite
    по умолчанию RESTRICT на уровне БД).
    """
    u = db.session.get(User, user_id)
    if u is None:
        return False
    try:
        Notification.query.filter_by(user_id=user_id).delete(synchronize_session=False)
        PurchasePrice.query.filter_by(user_id=user_id).delete(synchronize_session=False)
        MarginCalculation.query.filter_by(user_id=user_id).delete(synchronize_session=False)
        db.session.delete(u)
        db.session.commit()
    except Exception:
        db.session.rollback()
        raise
    return True


class Notification(db.Model):
    __tablename__ = "notifications"
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False)
    title = db.Column(db.String(255), nullable=False)
    message = db.Column(db.Text, nullable=False)
    notification_type = db.Column(db.String(50), nullable=False)  # 'fbs_new_order', 'system', etc.
    is_read = db.Column(db.Boolean, default=False)
    data = db.Column(db.Text, nullable=True)  # JSON data for additional notification info
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(MOSCOW_TZ))

    user = db.relationship('User', backref=db.backref('notifications', lazy=True))


class PurchasePrice(db.Model):
    __tablename__ = "purchase_prices"
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False)
    barcode = db.Column(db.String(50), nullable=False)
    price = db.Column(db.Float, nullable=False)
    created_at = db.Column(db.DateTime, default=lambda: datetime.now(MOSCOW_TZ))
    updated_at = db.Column(db.DateTime, default=lambda: datetime.now(MOSCOW_TZ), onupdate=datetime.now(MOSCOW_TZ))
    
    # Уникальный индекс для комбинации user_id + barcode
    __table_args__ = (db.UniqueConstraint('user_id', 'barcode', name='unique_user_barcode'),)
    data = db.Column(db.Text, nullable=True)  # JSON data for additional info
    
    user = db.relationship('User', backref=db.backref('purchase_prices', lazy=True))

    def get_id(self):  # type: ignore[override]
        return str(self.id)


class MarginCalculation(db.Model):
    """
    Сохраненные значения маржинальности (как они отображаются на /tools/prices).

    Ключ: (user_id, nm_id) — т.к. строка маржи на /tools/prices соответствует конкретному nm_id.
    """

    __tablename__ = "margin_calculations"
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=False)
    nm_id = db.Column(db.Integer, nullable=False)
    barcode = db.Column(db.String(50), nullable=True)

    # Прибыль чистая (руб.) и Прибыль % (как в UI /tools/prices)
    profit_net = db.Column(db.Float, nullable=True)
    profit_pct = db.Column(db.Float, nullable=True)

    updated_at = db.Column(db.DateTime, default=lambda: datetime.now(MOSCOW_TZ), onupdate=lambda: datetime.now(MOSCOW_TZ))

    __table_args__ = (db.UniqueConstraint('user_id', 'nm_id', name='unique_user_nm_id_margin'),)

    user = db.relationship('User', backref=db.backref('margin_calculations', lazy=True))

    def get_id(self):  # type: ignore[override]
        return str(self.id)


def delete_user_with_related(user_id: int) -> bool:
    """
    Удаляет пользователя и все строки с FK на users.id (в БД без ON DELETE CASCADE).
    """
    u = db.session.get(User, user_id)
    if u is None:
        return False
    try:
        Notification.query.filter_by(user_id=user_id).delete(synchronize_session=False)
        PurchasePrice.query.filter_by(user_id=user_id).delete(synchronize_session=False)
        MarginCalculation.query.filter_by(user_id=user_id).delete(synchronize_session=False)
        db.session.delete(u)
        db.session.commit()
    except Exception:
        db.session.rollback()
        raise
    return True

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для добавления колонки display_name в таблицу users
Запустите этот скрипт на рабочем сервере для обновления базы данных
"""
import os
import sys
from sqlalchemy import create_engine, text

# Путь к базе данных (по умолчанию app.db в текущей директории)
db_path = os.getenv("DATABASE_URL", f"sqlite:///{os.path.join(os.path.dirname(__file__), 'app.db')}")

def migrate():
    """Добавляет колонку display_name в таблицу users"""
    try:
        engine = create_engine(db_path)
        
        # Определяем тип базы данных
        dialect = engine.dialect.name
        
        with engine.begin() as conn:
            if dialect == "sqlite":
                # Проверяем, существует ли колонка
                rows = conn.execute(text("PRAGMA table_info(users)")).fetchall()
                cols = {r[1] for r in rows}
                
                if "display_name" not in cols:
                    print("Добавление колонки display_name в таблицу users (SQLite)...")
                    conn.execute(text("ALTER TABLE users ADD COLUMN display_name VARCHAR(255)"))
                    print("✓ Колонка display_name успешно добавлена!")
                else:
                    print("✓ Колонка display_name уже существует")
                    
            elif dialect in ("postgresql", "postgres"):
                print("Добавление колонки display_name в таблицу users (PostgreSQL)...")
                try:
                    conn.execute(text("ALTER TABLE users ADD COLUMN IF NOT EXISTS display_name VARCHAR(255)"))
                    print("✓ Колонка display_name успешно добавлена!")
                except Exception as e:
                    if "already exists" in str(e).lower() or "duplicate" in str(e).lower():
                        print("✓ Колонка display_name уже существует")
                    else:
                        raise
                        
            elif dialect in ("mysql", "mariadb"):
                print("Добавление колонки display_name в таблицу users (MySQL/MariaDB)...")
                try:
                    conn.execute(text("ALTER TABLE users ADD COLUMN IF NOT EXISTS display_name VARCHAR(255)"))
                    print("✓ Колонка display_name успешно добавлена!")
                except Exception as e:
                    if "duplicate column" in str(e).lower():
                        print("✓ Колонка display_name уже существует")
                    else:
                        raise
            else:
                print(f"⚠ Неподдерживаемый тип базы данных: {dialect}")
                print("Выполните вручную: ALTER TABLE users ADD COLUMN display_name VARCHAR(255)")
                return False
                
        return True
        
    except Exception as e:
        print(f"✗ Ошибка при миграции: {e}")
        return False

if __name__ == "__main__":
    print("Миграция базы данных: добавление колонки display_name")
    print(f"База данных: {db_path}")
    print("-" * 50)
    
    if migrate():
        print("-" * 50)
        print("Миграция завершена успешно!")
        sys.exit(0)
    else:
        print("-" * 50)
        print("Миграция завершилась с ошибками")
        sys.exit(1)


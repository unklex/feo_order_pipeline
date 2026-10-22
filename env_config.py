"""Загрузка настроек из файла .env рядом с этим модулем (см. .env.example)."""

import os

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))
except ImportError:
    # python-dotenv не установлен - используем только реальные переменные окружения
    # Установка: pip install python-dotenv
    pass


def require_env(name: str) -> str:
    """Возвращает обязательную переменную окружения или падает с понятным сообщением."""
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Не задана переменная окружения {name}. Заполните .env (см. .env.example).")
    return value


def env_int(name: str, default: int) -> int:
    return int(os.environ.get(name) or default)

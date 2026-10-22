#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Упрощенный скрипт для создания заказов в Битрикс24 из таблицы new_webapi_orders_transportation.

Процесс работы:
1. Получает заказы из БД где bitrix_id IS NULL
2. Запрашивает подтверждение на создание
3. Создает пустые заказы в Битрикс24 и обновляет bitrix_id в БД

Особенности:
- Все секреты вынесены в переменные окружения / .env (см. .env.example).
- Устойчивость к обрыву связи: при падении SSH-туннеля или соединения с БД
  скрипт автоматически переподключается и продолжает работу, а не падает.
"""

import os
import sys
import traceback
import logging
from datetime import datetime
import asyncio
import time

from tqdm import tqdm
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, DBAPIError
import pymysql  # noqa: F401  (нужен как драйвер для SQLAlchemy)

# Загрузка переменных окружения из .env (если файл присутствует)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    # python-dotenv не установлен — работаем только с реальными переменными окружения.
    # Установка: pip install python-dotenv
    pass

# Клиент Битрикс24 берём из bitrix_config. Он лежит в корне рабочей папки
# Bitrix24 — добавляем её в путь (можно переопределить переменной BITRIX_LIB_DIR).
# Импорт bx24_import намеренно убран: из него ничего не нужно, а при импорте он
# поднимал лишний SSH-туннель через db_connection.
# bitrix_config лежит рядом со скриптом; внешняя папка нужна только если задана явно.
if os.environ.get("BITRIX_LIB_DIR"):
    sys.path.append(os.environ["BITRIX_LIB_DIR"])
from bitrix_config import b
from audit_log import AuditLog
from db_tunnel import open_db_tunnel  # SSH-туннель или прямое подключение (DB_USE_SSH)
#from tg_bot import send

# JSONL-журнал вызовов Битрикс для анализа после запуска (создаётся в main)
audit = None


def _audit(event, **data):
    if audit is not None:
        audit.write(event, **data)

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"bitrix_create_{datetime.now().strftime('%Y%m%d')}.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Загрузка конфигурации из переменных окружения
# ---------------------------------------------------------------------------

def _get_env(name, default=None, required=False):
    """Читает переменную окружения. Если required и значение отсутствует — ошибка."""
    value = os.environ.get(name, default)
    if required and (value is None or str(value).strip() == ""):
        raise RuntimeError(
            f"Не задана обязательная переменная окружения '{name}'. "
            f"Заполните её в файле .env (см. .env.example)."
        )
    return value


def _get_env_int(name, default=None, required=False):
    """Читает целочисленную переменную окружения."""
    value = _get_env(name, default=default, required=required)
    if value is None or str(value).strip() == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        raise RuntimeError(f"Переменная окружения '{name}' должна быть целым числом, получено: {value!r}")


class Config:
    """Вся конфигурация скрипта, собранная из переменных окружения."""

    # ID смарт-процесса ФЭО в Битрикс24
    ENTITY_TYPE_ID = _get_env_int("BITRIX_ENTITY_TYPE_ID", default=137)

    # Максимум одновременных запросов к API Битрикс24
    MAX_CONCURRENT = _get_env_int("BITRIX_MAX_CONCURRENT", default=20)

    # SSH_* читаются в db_tunnel и нужны только при DB_USE_SSH=1

    # База данных
    DB_HOST = _get_env("DB_HOST", default="localhost")
    DB_PORT = _get_env_int("DB_PORT", default=3306)
    DB_NAME = _get_env("DB_NAME", required=True)
    DB_USER = _get_env("DB_USER", required=True)
    DB_PASSWORD = _get_env("DB_PASSWORD", required=True)

    # Переподключение к БД: 0 = бесконечно (не останавливаться при обрыве связи)
    DB_RECONNECT_MAX_ATTEMPTS = _get_env_int("DB_RECONNECT_MAX_ATTEMPTS", default=0)
    DB_RECONNECT_MAX_DELAY = _get_env_int("DB_RECONNECT_MAX_DELAY", default=60)


# ---------------------------------------------------------------------------
# Менеджер соединения с БД (SSH-туннель + engine) с авто-переподключением
# ---------------------------------------------------------------------------

class DatabaseConnection:
    """Держит SSH-туннель и engine и переподключается при обрыве связи."""

    def __init__(self):
        self.engine = None
        self.tunnel = None

    def connect(self):
        """Создаёт SSH-туннель и engine с нуля (закрыв предыдущие, если были)."""
        self.close()
        logger.info("Установка соединения с базой данных")

        self.tunnel = open_db_tunnel(Config.DB_HOST, Config.DB_PORT, set_keepalive=10)

        db_url = (
            f"mysql+pymysql://{Config.DB_USER}:{Config.DB_PASSWORD}"
            f"@{self.tunnel.local_bind_host}:{self.tunnel.local_bind_port}/{Config.DB_NAME}"
        )
        self.engine = create_engine(
            db_url,
            echo=False,
            pool_recycle=3600,
            pool_pre_ping=True,
            connect_args={'connect_timeout': 30, 'read_timeout': 30, 'write_timeout': 30},
        )

        with self.engine.connect() as conn:
            if conn.execute(text("SELECT 1")).scalar() == 1:
                logger.info("Соединение с базой данных успешно установлено")

    def is_healthy(self) -> bool:
        try:
            if not self.tunnel or not self.tunnel.is_active or not self.engine:
                return False
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception:
            return False

    def reconnect(self):
        """Переподключается с нарастающей задержкой. По умолчанию — бесконечно."""
        max_attempts = Config.DB_RECONNECT_MAX_ATTEMPTS or 0
        max_delay = Config.DB_RECONNECT_MAX_DELAY or 60
        attempt = 0
        while True:
            attempt += 1
            try:
                logger.warning(f"Переподключение к БД (попытка {attempt})...")
                self.connect()
                logger.info("Переподключение к БД выполнено успешно")
                return
            except Exception as e:
                delay = min(max_delay, 5 * attempt)
                logger.error(f"Не удалось переподключиться (попытка {attempt}): {e}. Повтор через {delay}с")
                if max_attempts and attempt >= max_attempts:
                    logger.error("Исчерпан лимит попыток переподключения")
                    raise
                time.sleep(delay)

    def ensure(self):
        if not self.is_healthy():
            logger.warning("Соединение с БД потеряно — выполняется переподключение")
            self.reconnect()

    def run(self, operation, *, retries: int = 5, base_delay: int = 3,
            description: str = "операция с БД", default=None):
        """Выполняет operation(engine) с ретраями и переподключением при обрыве."""
        for attempt in range(1, retries + 1):
            try:
                self.ensure()
                return operation(self.engine)
            except (OperationalError, DBAPIError) as e:
                logger.error(f"{description}: ошибка соединения (попытка {attempt}/{retries}): {e}")
                self._backoff_and_reconnect(attempt, base_delay)
            except Exception as e:
                logger.error(f"{description}: ошибка (попытка {attempt}/{retries}): {e}")
                logger.debug(traceback.format_exc())
                self._backoff_and_reconnect(attempt, base_delay)
        logger.error(f"{description}: исчерпаны все попытки ({retries}), продолжаем работу")
        return default

    def _backoff_and_reconnect(self, attempt: int, base_delay: int):
        delay = min(Config.DB_RECONNECT_MAX_DELAY or 60, base_delay * attempt)
        logger.info(f"Пауза {delay}с перед повтором...")
        time.sleep(delay)
        try:
            self.ensure()
        except Exception as e:
            logger.error(f"Переподключение не удалось: {e}")

    def close(self):
        if self.engine is not None:
            try:
                self.engine.dispose()
            except Exception as e:
                logger.warning(f"Ошибка при закрытии engine: {e}")
            self.engine = None
        if self.tunnel is not None:
            try:
                if self.tunnel.is_active:
                    self.tunnel.stop()
                    logger.info("SSH-туннель закрыт")
            except Exception as e:
                logger.warning(f"Ошибка при закрытии SSH-туннеля: {e}")
            self.tunnel = None


def get_orders_without_bitrix_id(db: DatabaseConnection):
    """Получает все заказы из БД, где bitrix_id не заполнен. Переподключается при обрыве."""
    logger.info("Получение заказов из БД где bitrix_id IS NULL")

    def op(engine):
        query = """
        SELECT bid_id, parsed, parsed_at
        FROM new_webapi_orders_transportation
        WHERE bitrix_id IS NULL
        ORDER BY parsed_at ASC
        """
        with engine.connect() as conn:
            rows = conn.execute(text(query)).fetchall()
        if rows:
            df = pd.DataFrame(rows, columns=['bid_id', 'parsed', 'parsed_at'])
            df['bid_id'] = df['bid_id'].astype(str)
            logger.info(f"Найдено {len(df)} заказов в БД без bitrix_id")
            return df
        logger.info("Все заказы в БД уже имеют bitrix_id")
        return pd.DataFrame(columns=['bid_id', 'parsed', 'parsed_at'])

    return db.run(op, description="получение заказов без bitrix_id",
                  default=pd.DataFrame(columns=['bid_id', 'parsed', 'parsed_at']))


def update_bitrix_id_in_db(db: DatabaseConnection, bid_id, bitrix_id):
    """Обновляет bitrix_id в БД для созданного заказа. Переподключается при обрыве."""
    def op(engine):
        query = """
        UPDATE new_webapi_orders_transportation
        SET bitrix_id = :bitrix_id
        WHERE bid_id = :bid_id
        """
        with engine.connect() as conn:
            conn.execute(text(query), {"bitrix_id": bitrix_id, "bid_id": bid_id})
            conn.commit()
        logger.debug(f"Обновлен bitrix_id={bitrix_id} для bid_id={bid_id}")
        return True

    return db.run(op, description=f"обновление bitrix_id для bid_id {bid_id}", default=False)


async def create_orders_in_bitrix_async(b, orders_to_create, entity_type_id, max_concurrent=20):
    """Асинхронно создает пустые заказы в Битрикс24."""
    success_count = 0
    error_count = 0
    created_orders = []

    pbar = tqdm(total=len(orders_to_create), desc="Создание заказов в Битрикс24")
    start_time = time.time()
    semaphore = asyncio.Semaphore(max_concurrent)

    async def create_single_order(row):
        nonlocal success_count, error_count

        async with semaphore:
            try:
                fields = {}  # Пустые поля - создаем минимальную заглушку

                result = await b.call('crm.item.add', {
                    'entityTypeId': entity_type_id,
                    'fields': fields
                }, raw=True)

                if 'result' in result and 'item' in result['result'] and 'id' in result['result']['item']:
                    created_id = result['result']['item']['id']
                    created_orders.append({'bid_id': row['bid_id'], 'bitrix_id': created_id})
                    success_count += 1
                    _audit("item_add", bid_id=row['bid_id'], ok=True, item_id=created_id)
                    return True, created_id
                else:
                    logger.error(f"Неожиданный ответ API при создании заказа {row['bid_id']}")
                    error_count += 1
                    _audit("item_add", bid_id=row['bid_id'], ok=False, error="unexpected_response", response=result)
                    return False, None

            except Exception as e:
                logger.error(f"Ошибка при создании заказа {row['bid_id']}: {str(e)}")
                error_count += 1
                _audit("item_add", bid_id=row['bid_id'], ok=False, error=str(e))
                return False, None
            finally:
                pbar.update(1)
                processed_count = success_count + error_count
                pbar.set_postfix({
                    'успешно': success_count,
                    'ошибок': error_count,
                    'скорость': f"{processed_count / (time.time() - start_time):.2f} зап/с"
                })

    tasks = [asyncio.create_task(create_single_order(row)) for _, row in orders_to_create.iterrows()]
    await asyncio.gather(*tasks)
    pbar.close()

    total_time = time.time() - start_time
    logger.info(f"Создание заказов завершено: {success_count} успешно, {error_count} ошибок за {total_time:.2f}с")

    return created_orders


def create_orders_in_bitrix(b, orders_to_create, entity_type_id=None, max_concurrent=None):
    """Обертка для асинхронного создания заказов в Битрикс24."""
    if entity_type_id is None:
        entity_type_id = Config.ENTITY_TYPE_ID
    if max_concurrent is None:
        max_concurrent = Config.MAX_CONCURRENT

    if orders_to_create.empty:
        logger.info("Нет заказов для создания в Битрикс24")
        return []

    logger.info(f"Создание {len(orders_to_create)} пустых заказов в Битрикс24")

    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        created_orders = loop.run_until_complete(
            create_orders_in_bitrix_async(b, orders_to_create, entity_type_id, max_concurrent)
        )
        loop.close()
        return created_orders

    except Exception as e:
        logger.error(f"Ошибка при создании заказов в Битрикс24: {e}")
        logger.debug(traceback.format_exc())
        return []


def main():
    """Основная функция скрипта создания заказов в Битрикс24"""
    global audit
    logger.info("Запуск упрощенного скрипта создания заказов в Битрикс24")
    audit = AuditLog("sync_orders")
    logger.info(f"Журнал вызовов Битрикс (JSONL): {audit.path}")

    db = DatabaseConnection()

    try:
        # Устанавливаем соединение с БД (с ретраями)
        db.reconnect()

        # Получаем заказы из БД без bitrix_id
        logger.info("=" * 60)
        logger.info("Получение заказов из БД без bitrix_id")
        db_orders = get_orders_without_bitrix_id(db)

        print("\n" + "=" * 60)
        print("СТАТИСТИКА:")
        print(f"• Заказов в БД без bitrix_id: {len(db_orders)}")
        print("=" * 60)

        if db_orders.empty:
            logger.info("Нет заказов для создания в Битрикс24")
            #send("Скрипт завершен: нет заказов для создания в Битрикс24")
            return

        print(f"\nБУДЕТ СОЗДАНО {len(db_orders)} ПУСТЫХ ЗАКАЗОВ В БИТРИКС24")
        print("Примеры bid_id для создания:", db_orders['bid_id'].head(10).tolist())

        if not db_orders.empty and 'parsed_at' in db_orders.columns:
            print(f"Диапазон дат: {db_orders['parsed_at'].min()} - {db_orders['parsed_at'].max()}")

        confirm = input("\nПродолжить создание заказов? (y/N): ").strip().lower()
        if confirm not in ['y', 'yes', 'да', 'д']:
            logger.info("Создание заказов отменено пользователем")
            return

        # Создаем заказы в Битрикс24
        logger.info("=" * 60)
        logger.info("Создание заказов в Битрикс24")
        created_orders = create_orders_in_bitrix(
            b, db_orders, Config.ENTITY_TYPE_ID, max_concurrent=Config.MAX_CONCURRENT
        )

        if not created_orders:
            logger.error("Не удалось создать ни одного заказа")
            #send("Ошибка: не удалось создать ни одного заказа в Битрикс24")
            return

        # Обновляем bitrix_id в БД для созданных заказов (с авто-reconnect)
        logger.info("=" * 60)
        logger.info("Обновление bitrix_id в БД")
        updated_count = 0
        failed_updates = []

        for created_order in created_orders:
            if update_bitrix_id_in_db(db, created_order['bid_id'], created_order['bitrix_id']):
                updated_count += 1
            else:
                failed_updates.append(created_order['bid_id'])
                # Элемент в Битрикс создан, но id не записан в БД — при следующем запуске будет дубль
                _audit("db_update_failed", bid_id=created_order['bid_id'], item_id=created_order['bitrix_id'])

        message = (
            f"Создание заказов в Битрикс24 завершено:\n"
            f"• Найдено заказов в БД без bitrix_id: {len(db_orders)}\n"
            f"• Успешно создано в Битрикс24: {len(created_orders)}\n"
            f"• Обновлено bitrix_id в БД: {updated_count}"
        )
        if failed_updates:
            message += f"\n• Ошибки обновления БД: {len(failed_updates)}"
            logger.warning(f"Не удалось обновить bitrix_id для: {failed_updates}")

        print("\n" + "=" * 60)
        print("ИТОГОВЫЙ ОТЧЕТ:")
        print(message.replace("•", "-"))
        print("=" * 60)

        logger.info(message.replace("\n", " | "))
        _audit("run_summary", without_bitrix_id=len(db_orders), created=len(created_orders),
               db_updated=updated_count, db_update_failed=len(failed_updates))
        #send(message)

    except Exception as e:
        error_message = f"Критическая ошибка в скрипте создания заказов: {e}"
        logger.error(error_message)
        logger.debug(traceback.format_exc())
        _audit("fatal_error", error=str(e))
        #send(error_message)

    finally:
        db.close()
        audit.close()
        logger.info("Упрощенный скрипт создания заказов в Битрикс24 завершен")


if __name__ == "__main__":
    main()
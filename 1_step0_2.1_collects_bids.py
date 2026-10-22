#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Скрипт для сбора данных о заказах ГИСОПВК из таблицы new_webapi_orders_transportation
ИСПРАВЛЕННАЯ ВЕРСИЯ с улучшенной обработкой SSH-туннеля.

Особенности:
- Все секреты вынесены в переменные окружения / .env (см. .env.example).
- Устойчивость к обрыву связи: при падении SSH-туннеля или соединения с БД
  скрипт автоматически переподключается (пересоздаёт туннель и engine) и
  продолжает работу, а не завершается с ошибкой.
"""

import json
import time
import traceback
import logging
import datetime
import os
import argparse
from typing import List, Dict, Optional

from db_tunnel import open_db_tunnel, use_ssh  # SSH-туннель или прямое подключение (DB_USE_SSH)
from gisopvkwebapiparser import GisopvkWebApiParser
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


# ---------------------------------------------------------------------------
# Логирование
# ---------------------------------------------------------------------------

# Папка с логами (создаётся при необходимости)
LOG_DIR = os.environ.get("LOG_DIR", "logs")
os.makedirs(LOG_DIR, exist_ok=True)

logger = logging.getLogger("data_collector")
logger.setLevel(logging.INFO)
# Не передавать записи в root-логгер (иначе строки могут дублироваться)
logger.propagate = False

# Добавляем обработчики только один раз — защита от дублей при повторном
# импорте модуля (например, по цепочке bx24_import).
if not logger.handlers:
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    file_handler = logging.FileHandler(os.path.join(LOG_DIR, "data_collector.log"), encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)


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

    # SSH_* читаются в db_tunnel и нужны только при DB_USE_SSH=1

    # База данных
    DB_HOST = _get_env("DB_HOST", default="localhost")
    DB_PORT = _get_env_int("DB_PORT", default=3306)
    DB_NAME = _get_env("DB_NAME", required=True)
    DB_USER = _get_env("DB_USER", required=True)
    DB_PASSWORD = _get_env("DB_PASSWORD", required=True)

    # Параметры парсера ГИСОПВК
    GISOPVK_BASE_URL = _get_env("GISOPVK_BASE_URL", default="https://gisopvk.ru/stencilapi/v1/bid/")

    # Переподключение к БД: 0 = бесконечно (не останавливаться при обрыве связи)
    DB_RECONNECT_MAX_ATTEMPTS = _get_env_int("DB_RECONNECT_MAX_ATTEMPTS", default=0)
    DB_RECONNECT_MAX_DELAY = _get_env_int("DB_RECONNECT_MAX_DELAY", default=60)


# ---------------------------------------------------------------------------
# Менеджер соединения с БД (SSH-туннель + SQLAlchemy engine) с авто-переподключением
# ---------------------------------------------------------------------------

class DatabaseConnection:
    """
    Держит SSH-туннель и SQLAlchemy engine и умеет переподключаться при обрыве.

    Использование:
        db = DatabaseConnection()
        db.connect()
        db.run(lambda engine: ..., description="...")  # с авто-ретраем/reconnect
        db.close()
    """

    def __init__(self):
        self.engine = None
        self.tunnel = None

    # --- построение соединения ---------------------------------------------

    def connect(self):
        """Создаёт SSH-туннель и engine с нуля (закрыв предыдущие, если были)."""
        self.close()

        logger.info("Установка соединения с базой данных")

        self.tunnel = open_db_tunnel(
            Config.DB_HOST, Config.DB_PORT,
            set_keepalive=10,  # keep-alive пакеты каждые 10 секунд
            ssh_config_file=None,
            compression=True,
        )

        db_url = (
            f"mysql+pymysql://{Config.DB_USER}:{Config.DB_PASSWORD}"
            f"@{self.tunnel.local_bind_host}:{self.tunnel.local_bind_port}/{Config.DB_NAME}"
        )
        self.engine = create_engine(
            db_url,
            echo=False,
            pool_size=5,
            max_overflow=10,
            pool_recycle=3600,
            pool_pre_ping=True,  # проверка соединения перед использованием
            connect_args={
                'connect_timeout': 30,
                'read_timeout': 30,
                'write_timeout': 30,
            },
        )

        # Проверка соединения
        with self.engine.connect() as conn:
            if conn.execute(text("SELECT 1")).scalar() == 1:
                logger.info("Соединение с базой данных успешно установлено (pool_pre_ping=True)")
            else:
                logger.warning("Проверка соединения с БД вернула неожиданный результат")

    # --- контроль живости и переподключение --------------------------------

    def is_healthy(self) -> bool:
        """Быстрая проверка живости туннеля и соединения с БД."""
        try:
            if not self.tunnel or not self.tunnel.is_active:
                return False
            if not self.engine:
                return False
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception:
            return False

    def reconnect(self):
        """
        Переподключается к БД. По умолчанию (DB_RECONNECT_MAX_ATTEMPTS=0) пробует
        бесконечно с нарастающей задержкой — чтобы скрипт пережил обрыв связи и
        продолжил работу, а не упал.
        """
        max_attempts = Config.DB_RECONNECT_MAX_ATTEMPTS or 0  # 0 => бесконечно
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
        """Гарантирует живое соединение: если оборвалось — переподключается."""
        if not self.is_healthy():
            logger.warning("Соединение с БД потеряно — выполняется переподключение")
            self.reconnect()

    # --- выполнение операций с ретраем/переподключением ---------------------

    def run(self, operation, *, retries: int = 5, base_delay: int = 3,
            description: str = "операция с БД", default=None):
        """
        Выполняет operation(engine) с ретраями. При ошибке связи переподключается
        и повторяет. Если все попытки исчерпаны — логирует и возвращает default
        (скрипт продолжает работу, а не падает).
        """
        for attempt in range(1, retries + 1):
            try:
                self.ensure()
                return operation(self.engine)
            except (OperationalError, DBAPIError) as e:
                # Ошибки уровня БД/соединения — почти наверняка обрыв связи.
                logger.error(f"{description}: ошибка соединения (попытка {attempt}/{retries}): {e}")
                self._backoff_and_reconnect(attempt, base_delay)
            except Exception as e:
                # Прочие ошибки: тоже пробуем переподключиться (на случай мёртвого туннеля).
                logger.error(f"{description}: ошибка (попытка {attempt}/{retries}): {e}")
                logger.debug(traceback.format_exc())
                self._backoff_and_reconnect(attempt, base_delay)

        logger.error(f"{description}: исчерпаны все попытки ({retries}), продолжаем работу")
        return default

    def _backoff_and_reconnect(self, attempt: int, base_delay: int):
        """Пауза и (при необходимости) переподключение перед следующей попыткой."""
        delay = min(Config.DB_RECONNECT_MAX_DELAY or 60, base_delay * attempt)
        logger.info(f"Пауза {delay}с перед повтором...")
        time.sleep(delay)
        try:
            self.ensure()
        except Exception as e:
            logger.error(f"Переподключение не удалось: {e}")

    # --- закрытие -----------------------------------------------------------

    def close(self):
        """Закрывает engine и туннель, игнорируя ошибки."""
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


# ---------------------------------------------------------------------------
# Операции с БД (через менеджер соединения)
# ---------------------------------------------------------------------------

def get_ids_to_parse(db: DatabaseConnection, max_ids: int = 1000) -> List[str]:
    """
    Получает список bid_id заказов для парсинга. Исключает уже обработанные (parsed = 1).
    При обрыве связи переподключается и повторяет.
    """
    logger.info(f"Получение списка bid_id заказов для обработки (не более {max_ids})")

    def op(engine):
        query = """
        SELECT bid_id
        FROM new_webapi_orders_transportation
        WHERE parsed = 0 OR parsed IS NULL
        ORDER BY bid_id
        LIMIT :max_ids
        """
        with engine.connect() as conn:
            rows = conn.execute(text(query), {"max_ids": max_ids}).fetchall()
        ids = [str(row[0]) for row in rows]
        logger.info(f"Получено {len(ids)} bid_id для обработки")
        return ids

    return db.run(op, description="получение списка bid_id", default=[])


def mark_as_parsed(db: DatabaseConnection, bid_id: str) -> bool:
    """Отмечает один заказ как спарсенный. При обрыве связи переподключается."""
    def op(engine):
        query = """
        UPDATE new_webapi_orders_transportation
        SET parsed = 1, parsed_at = NOW()
        WHERE bid_id = :bid_id
        """
        with engine.connect() as conn:
            conn.execute(text(query), {"bid_id": bid_id})
            conn.commit()
        logger.debug(f"Заказ с bid_id {bid_id} отмечен как спарсенный")
        return True

    return db.run(op, description=f"обновление статуса bid_id {bid_id}", default=False)


def mark_batch_as_parsed(db: DatabaseConnection, bid_ids: List[str]) -> int:
    """Отмечает список заказов как спарсенные одной транзакцией. Переподключается при обрыве."""
    if not bid_ids:
        return 0

    def op(engine):
        with engine.begin() as conn:
            placeholders = ','.join([':id_' + str(i) for i in range(len(bid_ids))])
            params = {f'id_{i}': bid_id for i, bid_id in enumerate(bid_ids)}
            query = f"""
            UPDATE new_webapi_orders_transportation
            SET parsed = 1, parsed_at = NOW()
            WHERE bid_id IN ({placeholders})
            """
            updated_count = conn.execute(text(query), params).rowcount
        logger.info(f"Транзакция завершена: обновлено {updated_count} записей")
        return updated_count

    return db.run(op, description="батчевое обновление БД", default=0)


# Обязательные поля ответа по заявке и их ожидаемый тип (есть во всех ранее сохранённых записях)
REQUIRED_BID_FIELDS = {
    'fkko': dict,
    'wasteplace': dict,
    'org_info': dict,
    'waste_containers': list,
}


def validate_bid_response(bid_id: str, data) -> Optional[str]:
    """
    Проверяет ответ API по заявке. Возвращает описание проблемы или None, если ответ корректен.
    """
    if not isinstance(data, dict) or 'id' not in data:
        return "неожиданный формат ответа (нет поля id)"
    if str(data['id']) != str(bid_id).strip():
        return f"вернулась чужая заявка: запрошен {bid_id}, получен {data['id']}"
    problems = []
    for field, expected_type in REQUIRED_BID_FIELDS.items():
        if field not in data:
            problems.append(f"{field}: отсутствует")
        elif not isinstance(data[field], expected_type):
            problems.append(f"{field}: {type(data[field]).__name__} вместо {expected_type.__name__}")
    if problems:
        return "нарушена схема ответа (" + "; ".join(problems) + ")"
    return None


def parse_bid_data(bid_id: str, parser: GisopvkWebApiParser, max_retries: int = 3) -> Optional[Dict]:
    """
    Получает данные о заказе по его ID из API с повторными попытками.
    """
    logger.info(f"Получение данных для заказа с ID: {bid_id}")

    bid_api_url = f"{parser.base_url}{bid_id}"

    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Попытка {attempt}/{max_retries} для ID {bid_id}")

            response = parser.session.get(bid_api_url, timeout=30)

            if response.status_code != 200:
                logger.warning(f"Ошибка при запросе данных для ID {bid_id}: Статус {response.status_code}")

                if response.status_code == 406:
                    logger.info("Исправляем заголовки запроса")
                    parser._fix_request_headers()

                if attempt < max_retries:
                    delay = 2 * attempt
                    logger.info(f"Ожидание {delay} секунд перед следующей попыткой...")
                    time.sleep(delay)
                    continue
                else:
                    logger.error(f"Исчерпаны все попытки для ID {bid_id}")
                    return None

            data = response.json()

            problem = validate_bid_response(bid_id, data)
            if problem is None:
                logger.info(f"Успешно получены данные для заказа {bid_id}")
                return data
            else:
                logger.warning(f"Ответ для ID {bid_id} отклонён: {problem}")
                if attempt < max_retries:
                    time.sleep(2 * attempt)
                    continue
                else:
                    logger.error(f"Исчерпаны все попытки для ID {bid_id}")
                    return None

        except Exception as e:
            logger.error(f"Ошибка при получении данных для заказа {bid_id} (попытка {attempt}): {e}")
            logger.debug(traceback.format_exc())
            if attempt < max_retries:
                time.sleep(2 * attempt)
                continue
            else:
                logger.error(f"Исчерпаны все попытки для ID {bid_id}")
                return None

    return None


def save_data_to_json(data, filename=None):
    """Сохраняет данные в JSON-файл с временной меткой."""
    if filename is None:
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"parsed_data_{timestamp}.json"

    os.makedirs(os.path.dirname(os.path.abspath(filename)) if os.path.dirname(filename) else '.', exist_ok=True)

    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2, default=str)
        logger.info(f"Данные успешно сохранены в файл: {filename}")
        return filename
    except Exception as e:
        logger.error(f"Ошибка при сохранении данных в JSON: {e}")
        logger.debug(traceback.format_exc())
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, default=str)
            logger.info(f"Данные сохранены в файл без форматирования: {filename}")
            return filename
        except Exception as e2:
            logger.error(f"Критическая ошибка при сохранении JSON: {e2}")
            return None


def check_dependencies():
    """Проверяет наличие всех необходимых зависимостей."""
    logger.info("Проверка зависимостей...")

    try:
        import pandas as pd
        import pymysql
        from sqlalchemy import create_engine
        if use_ssh():
            from sshtunnel import SSHTunnelForwarder
        logger.info("Основные модули доступны")

        try:
            from gisopvkwebapiparser import GisopvkWebApiParser
            logger.info("Модуль gisopvkwebapiparser импортирован")
            try:
                test_parser = GisopvkWebApiParser(base_url="https://test.com")
                logger.info("Класс GisopvkWebApiParser создается без ошибок")
                return True
            except ImportError as e:
                logger.error(f"Ошибка при создании экземпляра GisopvkWebApiParser: {e}")
                logger.error("Возможно, отсутствует модуль 'auth' или другие зависимости")
                return False
            except Exception as e:
                logger.warning(f"Предупреждение при создании тестового экземпляра: {e}")
                logger.info("Продолжаем, так как это может быть нормальным поведением")
                return True

        except ImportError as e:
            logger.error(f"Не удалось импортировать gisopvkwebapiparser: {e}")
            return False

    except ImportError as e:
        logger.error(f"Отсутствует критический модуль: {e}")
        return False


def _clean_ids(raw_ids) -> List[str]:
    """Приводит список к строковым bid_id: убирает пустые/дубликаты, сохраняет порядок."""
    seen = set()
    result = []
    for value in raw_ids:
        bid_id = str(value).strip()
        if not bid_id or bid_id in seen:
            continue
        seen.add(bid_id)
        result.append(bid_id)
    return result


def load_ids_from_file(path: str) -> List[str]:
    """
    Загружает список bid_id из файла. Поддерживает разделение по строкам,
    запятым и пробелам. Пустые строки и строки-комментарии (#) игнорируются.
    """
    with open(path, "r", encoding="utf-8") as f:
        content = f.read()

    tokens = []
    for line in content.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        # разделяем по запятым и пробелам
        for part in line.replace(",", " ").split():
            tokens.append(part)

    ids = _clean_ids(tokens)
    logger.info(f"Из файла {path} загружено {len(ids)} номеров")
    return ids


def build_arg_parser() -> argparse.ArgumentParser:
    """Аргументы командной строки: выбор источника номеров заказов."""
    parser = argparse.ArgumentParser(
        description="Сбор данных о заказах ГИСОПВК. По умолчанию берёт неспарсенные "
                    "заказы из БД; можно вместо этого указать конкретные номера."
    )
    parser.add_argument(
        "--ids", nargs="+", metavar="BID_ID",
        help="Список номеров заказов для парсинга (например: --ids 123 456 789). "
             "Отключает запрос в БД."
    )
    parser.add_argument(
        "--ids-file", metavar="PATH",
        help="Путь к файлу со списком номеров (по одному на строку или через запятую/пробел). "
             "Отключает запрос в БД."
    )
    parser.add_argument(
        "--max-ids", type=int, default=3000, metavar="N",
        help="Максимум заказов из БД за запуск (только для режима БД). По умолчанию 3000."
    )
    parser.add_argument(
        "--no-mark", action="store_true",
        help="Не отмечать заказы как обработанные (parsed=1) в БД. "
             "Удобно для разового парсинга произвольных номеров."
    )
    return parser


def main(argv=None):
    """Основная функция скрипта для сбора данных о заказах."""
    args = build_arg_parser().parse_args(argv)

    logger.info("=" * 70)
    logger.info("ЗАПУСК СКРИПТА СБОРА ДАННЫХ (ИСПРАВЛЕННАЯ ВЕРСИЯ)")
    logger.info("=" * 70)

    if not check_dependencies():
        logger.error("Критические зависимости отсутствуют. Завершение работы.")
        return

    # Определяем источник номеров: явный список / файл (режим "номера")
    # либо запрос в БД (режим по умолчанию).
    explicit_ids = None
    try:
        if args.ids and args.ids_file:
            logger.error("Укажите либо --ids, либо --ids-file, но не оба сразу")
            return
        if args.ids:
            explicit_ids = _clean_ids(args.ids)
            logger.info(f"Режим: парсинг номеров из --ids ({len(explicit_ids)} шт.)")
        elif args.ids_file:
            explicit_ids = load_ids_from_file(args.ids_file)
            logger.info(f"Режим: парсинг номеров из файла {args.ids_file} ({len(explicit_ids)} шт.)")
        else:
            logger.info("Режим: выборка неспарсенных заказов из БД")
    except Exception as e:
        logger.error(f"Ошибка при чтении списка номеров: {e}")
        logger.debug(traceback.format_exc())
        return

    if explicit_ids is not None and not explicit_ids:
        logger.error("Список номеров пуст — нечего парсить")
        return

    mark_in_db = not args.no_mark

    # Подключение к БД нужно всегда, КРОМE случая: заданы явные номера И --no-mark.
    # Тогда БД не требуется вовсе (данные берём из API, в БД ничего не пишем).
    need_db = not (explicit_ids is not None and args.no_mark)

    db = DatabaseConnection() if need_db else None
    parser = None

    try:
        # Установка соединения с базой данных (с ретраями)
        if need_db:
            db.reconnect()  # первый коннект тоже через устойчивую логику
        else:
            logger.info("Режим без БД (--no-mark + явные номера): подключение к БД не требуется")

        # Создаем экземпляр парсера
        logger.info("Инициализация парсера и авторизация")
        try:
            parser = GisopvkWebApiParser(base_url=Config.GISOPVK_BASE_URL)
            logger.info("Парсер успешно инициализирован")
        except ImportError as e:
            logger.error(f"Ошибка импорта при создании парсера: {e}")
            raise RuntimeError(f"Не удалось инициализировать парсер: {e}")
        except Exception as e:
            logger.error(f"Неожиданная ошибка при создании парсера: {e}")
            logger.debug(traceback.format_exc())
            raise RuntimeError(f"Критическая ошибка при инициализации парсера: {e}")

        # Авторизация
        try:
            logger.info("Выполнение авторизации...")
            if not parser.authorize():
                raise RuntimeError("Метод authorize() вернул False")
            logger.info("Авторизация успешно выполнена")
        except Exception as e:
            logger.error(f"Произошла ошибка при авторизации: {e}")
            logger.debug(traceback.format_exc())
            raise RuntimeError(f"Не удалось выполнить авторизацию: {e}")

        # Тест соединения с API
        try:
            logger.info("Тестирование соединения с API...")
            if not parser.test_auth():
                logger.warning("Тест авторизации не пройден, но продолжаем работу")
            else:
                logger.info("Тест авторизации пройден успешно")
        except Exception as e:
            logger.warning(f"Ошибка при тестировании авторизации: {e}")
            logger.warning("Продолжаем работу без тестирования")

        # Восстановление из существующих файлов (только в режиме БД с пометкой)
        if need_db and mark_in_db and explicit_ids is None:
            try:
                recovery_func = create_recovery_mechanism(db)
                recovery_func()
            except Exception as e:
                logger.warning(f"Ошибка при восстановлении: {e}")

        # Основная обработка с батчами
        process_orders_in_batches(
            db, parser,
            ids_to_parse=explicit_ids,
            mark_in_db=mark_in_db,
            max_ids=args.max_ids,
        )

    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        logger.debug(traceback.format_exc())

    finally:
        if parser:
            try:
                parser.close()
                logger.info("Ресурсы парсера освобождены")
            except Exception as e:
                logger.warning(f"Ошибка при закрытии парсера: {e}")

        if db is not None:
            db.close()

        logger.info("=" * 70)
        logger.info("СКРИПТ СБОРА ДАННЫХ ЗАВЕРШЕН")
        logger.info("=" * 70)


def process_orders_in_batches(db, parser, ids_to_parse=None, mark_in_db=True, max_ids=3000):
    """
    Обрабатывает заказы батчами с безопасным сохранением.

    Args:
        db: менеджер соединения с БД (может быть None, если mark_in_db=False и заданы номера)
        parser: экземпляр парсера
        ids_to_parse: явный список номеров. Если None — берётся из БД (parsed=0/NULL).
        mark_in_db: отмечать ли обработанные заказы как parsed=1 в БД.
        max_ids: лимит выборки из БД (только когда ids_to_parse=None).
    """
    BATCH_SIZE = 500

    # Источник номеров: явный список или выборка из БД
    if ids_to_parse is None:
        ids_to_parse = get_ids_to_parse(db, max_ids=max_ids)
    if not ids_to_parse:
        logger.info("Нет новых заказов для обработки")
        return

    logger.info(f"Начинаем обработку {len(ids_to_parse)} заказов батчами по {BATCH_SIZE}")

    total_processed = 0
    total_saved = 0
    total_marked = 0

    for batch_start in range(0, len(ids_to_parse), BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE, len(ids_to_parse))
        current_batch_ids = ids_to_parse[batch_start:batch_end]
        batch_number = (batch_start // BATCH_SIZE) + 1

        logger.info(f"=== Обработка батча {batch_number}: заказы {batch_start + 1}-{batch_end} ===")

        batch_data = []
        successfully_parsed_ids = []

        for i, bid_id in enumerate(current_batch_ids, 1):
            logger.info(f"Батч {batch_number}: обработка {i}/{len(current_batch_ids)} - ID: {bid_id}")

            data = parse_bid_data(bid_id, parser)
            if data:
                batch_data.append(data)
                successfully_parsed_ids.append(bid_id)
                logger.debug(f"Данные для {bid_id} успешно получены")
            else:
                logger.warning(f"Не удалось получить данные для {bid_id}")

            total_processed += 1

            if i < len(current_batch_ids):
                time.sleep(1)

        if batch_data:
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"batch_{batch_number:03d}_{timestamp}.json"

            saved_filename = save_data_to_json(batch_data, filename)
            if saved_filename:
                logger.info(f"Батч {batch_number} сохранен в {saved_filename} ({len(batch_data)} заказов)")
                total_saved += len(batch_data)

                # Отмечаем в БД только если это включено и есть соединение
                if mark_in_db and db is not None:
                    # Отмечаем в БД только после успешного сохранения (с авто-reconnect)
                    marked_count = mark_batch_as_parsed(db, successfully_parsed_ids)

                    if marked_count == len(successfully_parsed_ids):
                        logger.info(f"В БД отмечено как обработанные: {marked_count} заказов")
                        total_marked += marked_count
                    else:
                        logger.error(f"Отмечено только {marked_count}/{len(successfully_parsed_ids)} заказов в БД!")
                        if marked_count < len(successfully_parsed_ids):
                            logger.info("Пытаемся отметить неудачные заказы по одному...")
                            for bid_id in successfully_parsed_ids:
                                if mark_as_parsed(db, bid_id):
                                    total_marked += 1
                else:
                    logger.info("Пометка в БД отключена (--no-mark или режим без БД)")
            else:
                logger.error(f"Не удалось сохранить батч {batch_number}! Заказы НЕ помечены в БД.")
        else:
            logger.warning(f"Батч {batch_number} пуст - нечего сохранять")

        logger.info(f"Батч {batch_number} завершен")
        logger.info(f"Прогресс: обработано {total_processed}/{len(ids_to_parse)} заказов\n")

    logger.info("=" * 60)
    logger.info("ИТОГИ БАТЧЕВОЙ ОБРАБОТКИ:")
    logger.info(f"Всего заказов к обработке: {len(ids_to_parse)}")
    logger.info(f"Обработано заказов: {total_processed}")
    logger.info(f"Сохранено в файлы: {total_saved}")
    logger.info(f"Отмечено в БД: {total_marked}")
    logger.info(f"Ошибок при обработке: {total_processed - total_saved}")
    logger.info("=" * 60)


def create_recovery_mechanism(db: DatabaseConnection):
    """Создает механизм восстановления для обработки частично сохраненных батчей."""

    def recover_from_files():
        import glob

        batch_files = glob.glob("batch_*.json")
        if not batch_files:
            logger.info("Файлы батчей для восстановления не найдены")
            return

        logger.info(f"Найдено {len(batch_files)} файлов батчей для возможного восстановления")

        recovered_total = 0

        for batch_file in batch_files:
            try:
                with open(batch_file, 'r', encoding='utf-8') as f:
                    batch_data = json.load(f)

                bid_ids = []
                for order in batch_data:
                    if isinstance(order, dict) and 'id' in order:
                        bid_ids.append(str(order['id']))

                if bid_ids:
                    logger.info(f"Восстановление из {batch_file}: найдено {len(bid_ids)} заказов")

                    def op(engine, _bid_ids=bid_ids):
                        placeholders = ','.join([f":id_{i}" for i in range(len(_bid_ids))])
                        params = {f"id_{i}": bid_id for i, bid_id in enumerate(_bid_ids)}
                        check_query = f"""
                        SELECT bid_id FROM new_webapi_orders_transportation
                        WHERE bid_id IN ({placeholders}) AND (parsed = 0 OR parsed IS NULL)
                        """
                        with engine.connect() as conn:
                            result = conn.execute(text(check_query), params)
                            return [str(row[0]) for row in result.fetchall()]

                    unprocessed_ids = db.run(op, description=f"проверка {batch_file}", default=[])

                    if unprocessed_ids:
                        marked = mark_batch_as_parsed(db, unprocessed_ids)
                        logger.info(f"Восстановлено из {batch_file}: {marked} заказов отмечены в БД")
                        recovered_total += marked
                    else:
                        logger.info(f"Все заказы из {batch_file} уже отмечены в БД")

            except Exception as e:
                logger.error(f"Ошибка при восстановлении из {batch_file}: {e}")

        if recovered_total > 0:
            logger.info(f"Всего восстановлено заказов: {recovered_total}")

    return recover_from_files


if __name__ == "__main__":
    main()
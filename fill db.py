#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Заполнение смарт-процесса ФЭО в Битрикс24 данными из распарсенного JSON + БД.

Пайплайн:
1. Загрузка распарсенного JSON (batch-файл) -> плоские таблицы (wasteplace / containers / main).
2. Выборка заказов из БД (is_active=1, parsed=1, bitrix_filled=0).
3. Обогащение: регионы, ФККО, компании (утилизатор/отходообразователь) из Битрикс.
4. Импорт недостающих компаний в Битрикс, обновление bx_co_id в БД.
5. Массовое параллельное обновление элементов смарт-процесса в Битрикс24.
6. Пометка обработанных заказов bitrix_filled=1 в БД.

Секреты (SSH/БД) вынесены в переменные окружения / .env (см. .env.example).
Устойчивость к обрыву связи с БД: чтение/запись идут через менеджер соединения
с авто-переподключением.
"""

import os
import sys
import json
import time
import logging
import argparse
import traceback
import asyncio
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, DBAPIError
from sshtunnel import SSHTunnelForwarder
import pymysql  # noqa: F401  (драйвер для SQLAlchemy)
from tqdm import tqdm

# Совместимость sshtunnel со свежим paramiko (>=3.0), где удалён класс DSSKey.
# Без этого падает с "module 'paramiko' has no attribute 'DSSKey'".
import paramiko
if not hasattr(paramiko, "DSSKey"):
    paramiko.DSSKey = paramiko.RSAKey

# Папка со скриптом — используется и для поиска .env, и для записи логов.
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Загрузка переменных окружения из .env.
# Ищем .env РЯДОМ СО СКРИПТОМ (надёжно, не зависит от рабочей директории),
# затем — стандартный поиск в текущей папке и выше.
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(SCRIPT_DIR, ".env"))
    load_dotenv()
except ImportError:
    print(
        "[ВНИМАНИЕ] Не установлен пакет python-dotenv — файл .env НЕ загружен. "
        "Установите его: pip install python-dotenv",
        file=sys.stderr,
    )

# Клиент Битрикс24 берём из bitrix_config — он лежит в папке проекта (рядом со
# скриптом). Добавляем папку скрипта в путь, чтобы импорт работал при запуске из
# любой директории. При необходимости путь можно переопределить через BITRIX_LIB_DIR.
# Импорты bx24 / bx24_import намеренно НЕ используются: из них ничего не нужно,
# а bx24_import при импорте поднимал лишний SSH-туннель через db_connection.
sys.path.insert(0, os.environ.get("BITRIX_LIB_DIR", os.path.dirname(os.path.abspath(__file__))))
from bitrix_config import b  # асинхронный клиент Битрикс24 (fast_bitrix24)
from audit_log import AuditLog

# JSONL-журнал вызовов Битрикс для анализа после запуска (создаётся в main_async)
audit = None


def _audit(event, **data):
    if audit is not None:
        audit.write(event, **data)


# ===========================================================================
#  ПУТЬ К JSON-ФАЙЛУ  —  УКАЖИТЕ ЗДЕСЬ
#  (можно переопределить при запуске:  python 1_step1_fill_db.py --json <путь>)
# ===========================================================================
JSON_FILE = os.path.join(SCRIPT_DIR, 'parsed_bids', 'batch_001_20261006_131159.json')


# ---------------------------------------------------------------------------
# Константы Битрикс24 / бизнес-логика (при необходимости меняйте здесь)
# ---------------------------------------------------------------------------
ORDER_ENTITY_TYPE_ID = 137          # смарт-процесс ФЭО (заказы на транспортирование)
REGIONS_LIST_ID = 47                # ID универсального списка регионов
FKKO_ENTITY_TYPE_ID = 191           # смарт-процесс ФККО
FKKO_FIELD_CODE = 'ufCrm22_1753808616'  # поле-ссылка на внешний fkko_id

# Параметры добавления компании
COMPANY_ASSIGNED_BY_ID = 41
COMPANY_UF_EXTERNAL_ID = 'UF_CRM_1695024809'  # UF-поле с внешним id организации

# --- КОСТЫЛИ (оставлены по требованию, вынесены сюда для наглядности) ---
# Принудительное значение поля "экопром" (ufCrm16_1696418503) для ВСЕХ строк:
ECOPROM_FORCED_COMPANY_ID = 2156
ECOPROM_FIELD_CODE = 'ufCrm16_1696418503'
# Ручной маппинг названий организаций -> id:
ORG_NAME_MAP = {
    'ООО "ЭКОПРОМ"': 2156,
    'ООО "ПУЛЬС"': 173,
}

# Маппинг метода определения массы
MASS_METHOD_MAPPING = {
    'calculated': 'рассчетный',
    'weighing': 'взвешивание',
}


# ---------------------------------------------------------------------------
# Логирование
# ---------------------------------------------------------------------------
# По умолчанию папка logs создаётся РЯДОМ СО СКРИПТОМ (а не в текущей рабочей
# директории), чтобы лог-файл всегда лежал в предсказуемом месте.
# Можно переопределить переменной окружения LOG_DIR (относительный путь — от cwd).
LOG_DIR = os.environ.get("LOG_DIR") or os.path.join(SCRIPT_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

LOG_FILE = os.path.join(LOG_DIR, f"fill_db_{datetime.now().strftime('%Y%m%d')}.log")

logger = logging.getLogger("fill_db")
logger.setLevel(logging.INFO)
logger.propagate = False
if not logger.handlers:
    _formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    _console = logging.StreamHandler()
    _console.setFormatter(_formatter)
    _file = logging.FileHandler(LOG_FILE, encoding="utf-8")
    _file.setFormatter(_formatter)
    logger.addHandler(_console)
    logger.addHandler(_file)


# ---------------------------------------------------------------------------
# Конфигурация из переменных окружения
# ---------------------------------------------------------------------------
def _get_env(name, default=None, required=False):
    value = os.environ.get(name, default)
    if required and (value is None or str(value).strip() == ""):
        raise RuntimeError(
            f"Не задана обязательная переменная окружения '{name}'. "
            f"Заполните её в файле .env (см. .env.example)."
        )
    return value


def _get_env_int(name, default=None, required=False):
    value = _get_env(name, default=default, required=required)
    if value is None or str(value).strip() == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        raise RuntimeError(f"Переменная окружения '{name}' должна быть целым числом, получено: {value!r}")


class Config:
    SSH_HOST = _get_env("SSH_HOST", required=True)
    SSH_PORT = _get_env_int("SSH_PORT", default=22)
    SSH_USERNAME = _get_env("SSH_USERNAME", required=True)
    SSH_PASSWORD = _get_env("SSH_PASSWORD", required=True)

    DB_HOST = _get_env("DB_HOST", default="localhost")
    DB_PORT = _get_env_int("DB_PORT", default=3306)
    DB_NAME = _get_env("DB_NAME", required=True)
    DB_USER = _get_env("DB_USER", required=True)
    DB_PASSWORD = _get_env("DB_PASSWORD", required=True)

    BITRIX_MAX_CONCURRENT = _get_env_int("BITRIX_MAX_CONCURRENT", default=20)

    DB_RECONNECT_MAX_ATTEMPTS = _get_env_int("DB_RECONNECT_MAX_ATTEMPTS", default=0)
    DB_RECONNECT_MAX_DELAY = _get_env_int("DB_RECONNECT_MAX_DELAY", default=60)


# ---------------------------------------------------------------------------
# Менеджер соединения с БД с авто-переподключением
# ---------------------------------------------------------------------------
class DatabaseConnection:
    """SSH-туннель + engine с переподключением при обрыве связи."""

    def __init__(self):
        self.engine = None
        self.tunnel = None

    def connect(self):
        self.close()
        logger.info("Установка соединения с базой данных")
        self.tunnel = SSHTunnelForwarder(
            (Config.SSH_HOST, Config.SSH_PORT),
            ssh_username=Config.SSH_USERNAME,
            ssh_password=Config.SSH_PASSWORD,
            remote_bind_address=(Config.DB_HOST, Config.DB_PORT),
            set_keepalive=10,
            allow_agent=False,
            host_pkey_directories=[],
        )
        self.tunnel.start()
        logger.info(f"SSH туннель установлен. Локальный порт: {self.tunnel.local_bind_port}")

        db_url = (
            f"mysql+pymysql://{Config.DB_USER}:{Config.DB_PASSWORD}"
            f"@localhost:{self.tunnel.local_bind_port}/{Config.DB_NAME}"
        )
        self.engine = create_engine(
            db_url,
            echo=False,
            pool_recycle=3600,
            pool_pre_ping=True,
            connect_args={'connect_timeout': 30, 'read_timeout': 60, 'write_timeout': 60},
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
                    raise
                time.sleep(delay)

    def ensure(self):
        if not self.is_healthy():
            logger.warning("Соединение с БД потеряно — выполняется переподключение")
            self.reconnect()

    def run(self, operation, *, retries: int = 5, base_delay: int = 3,
            description: str = "операция с БД", default=None):
        for attempt in range(1, retries + 1):
            try:
                self.ensure()
                return operation(self.engine)
            except (OperationalError, DBAPIError) as e:
                logger.error(f"{description}: ошибка соединения (попытка {attempt}/{retries}): {e}")
                self._backoff(attempt, base_delay)
            except Exception as e:
                logger.error(f"{description}: ошибка (попытка {attempt}/{retries}): {e}")
                logger.debug(traceback.format_exc())
                self._backoff(attempt, base_delay)
        logger.error(f"{description}: исчерпаны все попытки ({retries})")
        return default

    def _backoff(self, attempt, base_delay):
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


# ---------------------------------------------------------------------------
# Извлечение данных из JSON
# ---------------------------------------------------------------------------
def load_json_records(json_file: str) -> list:
    """Загружает JSON-файл и возвращает список записей (без None)."""
    logger.info(f"Загрузка JSON: {json_file}")
    with open(json_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    if isinstance(data, dict):
        data = [data]
    data = [rec for rec in data if rec is not None]
    logger.info(f"Загружено записей: {len(data)}")

    # Защита от дублей: одна заявка — одна запись (оставляем последнюю в файле)
    by_id, counts = {}, {}
    for rec in data:
        by_id[rec.get('id')] = rec
        counts[rec.get('id')] = counts.get(rec.get('id'), 0) + 1
    if len(by_id) != len(data):
        dup_ids = sorted(str(k) for k, n in counts.items() if n > 1)
        logger.warning(f"В JSON повторяются заявки ({len(dup_ids)}): {dup_ids}. Оставлена последняя запись каждой.")
        data = list(by_id.values())
    return data


def extract_wasteplace(record):
    if not record:
        return {'bid_id': None}
    wp = record.get('wasteplace') or {}
    point = wp.get('point') or {}
    coordinates = point.get('coordinates') or [None, None]
    municipality = wp.get('municipality') or {}
    return {
        'bid_id': record.get('id'),
        'wasteplace_name': wp.get('name'),
        'wasteplace_address': wp.get('address'),
        'wasteplace_point_lon': coordinates[0] if len(coordinates) > 0 else None,
        'wasteplace_point_lat': coordinates[1] if len(coordinates) > 1 else None,
        'wasteplace_municipality_name': municipality.get('name'),
        'wasteplace_region_name': municipality.get('region_name'),
        'wasteplace_working_hours': wp.get('working_hours'),
        'wasteplace_vehicle_limit_additional': wp.get('vehicle_limit_additional'),
    }


def extract_containers(record):
    if not record:
        return {'bid_id': None, 'containers_count': 0, 'containers_list': '', 'containers_count_tara': None}
    bid_id = record.get('id')
    containers = record.get('waste_containers') or []
    descriptions = []
    count_values = []
    for c in containers:
        if not c:
            continue
        kind = c.get('type_kind_name') or ''
        material = c.get('type_material_name') or ''
        mass = c.get('mass', '—')
        cub = c.get('cub', '—')
        mass_gross = c.get('mass_gross', '—')
        packing = c.get('packing_method', '—')
        count = c.get('count', '—')
        count_values.append(str(count))
        parts = [
            f"{kind} / {material}" if kind or material else "",
            f"{mass} кг",
            f"{cub} м³",
            f"{mass_gross} кг брутто",
            packing,
            f"шт: {count}",
        ]
        descriptions.append(', '.join(part for part in parts if part))
    return {
        'bid_id': bid_id,
        'containers_count': len(containers),
        'containers_list': ', '.join(descriptions),
        'containers_count_tara': ', '.join(count_values) if count_values else None,
    }


def extract_main(record):
    if not record:
        return {'bid_id': None}
    facility = record.get("facility") or {}
    municipality = facility.get("municipality") or {}
    wasteplace = record.get("wasteplace") or {}

    winner_transportation = record.get("winner_respond_transportation") or {}
    contract_block = winner_transportation.get("contract_transportation_fkko") or {}
    contract_info = contract_block.get("contract_transportation") or {}

    executor_recycling = record.get("executor_recycling") or {}
    fkko = record.get("fkko") or {}
    org_info = record.get("org_info") or {}
    executor_transporter = record.get("executor_transporter") or {}

    contact_email = record.get("_contact_email") or wasteplace.get("contact_email")
    contact_phone = record.get("_contact_phone") or wasteplace.get("contact_phone")
    contact_phone_mobile = record.get("_contact_phone_mobile") or wasteplace.get("contact_phone_mobile")
    contact_name = record.get("_contact_name") or wasteplace.get("contact_name")

    return {
        "bid_id": record.get("id"),
        "number_oon": record.get("number_oon"),
        "shipping_name": record.get("shipping_name"),
        "executor_recycling_id": executor_recycling.get("id"),
        "cub_sum": record.get("cub_sum"),
        "date_unloading_from": record.get("date_facility_unloading_from"),
        "date_unloading_to": record.get("date_facility_unloading_to"),
        "mass_gross": record.get("mass_gross"),
        "mass": record.get("mass"),
        "fkko_id": fkko.get("id"),
        "organization_id": org_info.get("id"),
        "name_short": org_info.get("name_short"),
        "transportation_org_id": executor_transporter.get("id"),
        "contact_email": contact_email,
        "contact_phone": contact_phone,
        "contact_phone_mobile": contact_phone_mobile,
        "contact_name": contact_name,
        "mass_determination_method": record.get("mass_determination_method"),
        "facility_address": facility.get("address"),
        "facility_lat": facility.get("lat"),
        "facility_lon": facility.get("lon"),
        "facility_municipality_name": municipality.get("name"),
        "facility_region_name": municipality.get("region_name"),
        "transport_contract_number": contract_info.get("contract_number"),
        "transport_contract_init_date": contract_info.get("init_date"),
        "executor_recycling_phone": executor_recycling.get("contact_phone"),
    }


def _id_str(series: pd.Series) -> pd.Series:
    """Числовой ID -> строка без '.0' ('156321'); пустой/нечисловой -> пропуск (NaN).

    Ключи-пропуски нужно отбрасывать из правой таблицы перед merge:
    pandas сопоставляет NaN с NaN, и строки без ID склеивались бы между собой.
    """
    ids = pd.to_numeric(series, errors='coerce').astype('Int64')
    # .where: в pandas < 3 astype(str) превращает NA в строку '<NA>'
    return ids.astype(str).where(ids.notna())


def _contract_label(number, init_date):
    """'<номер> от <дата>'; без номера — None (поле в Битрикс не трогаем), без даты — только номер."""
    if pd.isna(number) or not str(number).strip():
        return None
    if pd.isna(init_date) or not str(init_date).strip():
        return str(number)
    return f"{number} от {init_date}"


def build_merged_dataframe(data: list) -> pd.DataFrame:
    """Строит объединённый DataFrame из JSON-записей."""
    df_wasteplace = pd.DataFrame([extract_wasteplace(rec) for rec in data])
    df_containers = pd.DataFrame([extract_containers(rec) for rec in data])
    df_main = pd.DataFrame([extract_main(rec) for rec in data])
    df_merged = (
        df_main
        .merge(df_containers, on='bid_id', how='left')
        .merge(df_wasteplace, on='bid_id', how='left')
    )
    return df_merged


# ---------------------------------------------------------------------------
# Обращения к Битрикс24 (обогащение справочниками)
# ---------------------------------------------------------------------------
async def get_region_map() -> dict:
    """Получает справочник регионов {NAME: ID} из универсального списка Битрикс."""
    try:
        data = await b.get_all('lists.element.get', {
            'IBLOCK_TYPE_ID': 'lists',
            'IBLOCK_ID': REGIONS_LIST_ID,
        })
        if not data:
            logger.warning("Не получены данные о регионах из Битрикс24")
            return {}
        df = pd.DataFrame(data)
        if not {'ID', 'NAME'}.issubset(df.columns):
            logger.warning("В списке регионов нет колонок ID/NAME")
            return {}
        logger.info(f"Получено регионов: {len(df)}")
        return df.set_index('NAME')['ID'].to_dict()
    except Exception as e:
        logger.error(f"Ошибка при получении списка регионов: {e}")
        return {}


async def get_fkko_map() -> dict:
    """Получает справочник ФККО {внешний_fkko_id: bitrix_item_id}."""
    try:
        data = await b.get_all('crm.item.list', params={
            'entityTypeId': FKKO_ENTITY_TYPE_ID,
            'select': ['id', FKKO_FIELD_CODE],
        })
        if not data:
            logger.warning("Не получены данные ФККО из Битрикс24")
            return {}
        fkko = pd.DataFrame(data)
        fkko = fkko[fkko[FKKO_FIELD_CODE].notna()]
        fkko[FKKO_FIELD_CODE] = fkko[FKKO_FIELD_CODE].astype(float).astype(int).astype(str)
        logger.info(f"Получено записей ФККО: {len(fkko)}")
        return fkko.set_index(FKKO_FIELD_CODE)['id'].to_dict()
    except Exception as e:
        logger.error(f"Ошибка при получении ФККО: {e}")
        return {}


async def import_companies_to_bitrix(db: DatabaseConnection, missing_ids) -> int:
    """
    Импортирует компании из organization_info в Битрикс24 по списку ID.
    Обновляет bx_co_id в БД и сохраняет отчёт в Excel.
    """
    missing_ids = list(missing_ids)
    if not missing_ids:
        logger.info("Список missing_ids пуст — нет компаний для импорта")
        return 0

    def fetch(engine):
        query = """
        SELECT id, name_short
        FROM organization_info
        WHERE id IN :ids
          AND (bx_co_id IS NULL OR bx_co_id = '')
        """
        return pd.read_sql(text(query), engine, params={"ids": tuple(missing_ids)})

    df = db.run(fetch, description="выборка компаний для импорта", default=pd.DataFrame())
    if df.empty:
        logger.info("Нет новых компаний для импорта в Битрикс24")
        return 0

    logger.info(f"Найдено {len(df)} компаний для импорта в Битрикс24")

    added_companies_count = 0
    imported_companies = []

    for _, row in df.iterrows():
        company_id = row['id']
        name_short = row['name_short']

        if pd.isna(name_short) or not str(name_short).strip():
            logger.warning(f"Пропуск ID {company_id}: пустое name_short")
            continue

        fields = {
            "TITLE": name_short,
            "COMPANY_TYPE": 2,
            "OPENED": "Y",
            "ASSIGNED_BY_ID": COMPANY_ASSIGNED_BY_ID,
            COMPANY_UF_EXTERNAL_ID: company_id,
            "COMMENTS": "ФЭО",
        }

        try:
            result = b.call('crm.company.add', {'fields': fields})
            if asyncio.iscoroutine(result):
                result = await result

            _audit("company_add", org_id=company_id, name_short=name_short, ok=bool(result), bitrix_company_id=result)
            if result:
                def do_update(engine, _co_id=result, _id=company_id):
                    with engine.begin() as conn:
                        conn.execute(
                            text("UPDATE organization_info SET bx_co_id = :co_id WHERE id = :id"),
                            {'co_id': _co_id, 'id': _id},
                        )
                    return True

                db.run(do_update, description=f"обновление bx_co_id для {company_id}", default=False)

                imported_companies.append({'name_short': name_short, 'bitrix_id': result})
                added_companies_count += 1
                logger.info(f"[+] Добавлена: {name_short} -> Битрикс ID {result}")

        except Exception as e:
            logger.error(f"[X] Ошибка при добавлении ID {company_id}: {e}")
            _audit("company_add", org_id=company_id, name_short=name_short, ok=False, error=str(e))

    if imported_companies:
        try:
            pd.DataFrame(imported_companies).to_excel('bitrix_companies_imported.xlsx', index=False)
            logger.info("Отчёт сохранён: bitrix_companies_imported.xlsx")
        except Exception as e:
            logger.warning(f"Не удалось сохранить отчёт Excel: {e}")

    logger.info(f"Всего добавлено компаний: {added_companies_count}")
    return added_companies_count


# ---------------------------------------------------------------------------
# Массовое обновление элементов смарт-процесса
# ---------------------------------------------------------------------------
async def update_single_item(item_id, fields) -> bool:
    """Обновляет одну запись в Bitrix24."""
    try:
        await b.call('crm.item.update', {
            'entityTypeId': ORDER_ENTITY_TYPE_ID,
            'id': item_id,
            'fields': fields,
        })
        _audit("item_update", item_id=item_id, ok=True, fields=fields)
        return True
    except Exception as e:
        logger.error(f"Ошибка при обновлении записи {item_id}: {e}")
        _audit("item_update", item_id=item_id, ok=False, error=str(e), fields=fields)
        return False


def _build_update_fields(row, field_mappings) -> dict:
    """Формирует словарь полей для crm.item.update из строки DataFrame."""
    update_fields = {}
    for b24_field, df_column in field_mappings.items():
        # КОСТЫЛЬ: поле экопрома всегда константой
        if b24_field == ECOPROM_FIELD_CODE:
            update_fields[b24_field] = ECOPROM_FORCED_COMPANY_ID
            continue
        if df_column in row and pd.notna(row[df_column]):
            value = row[df_column]
            if df_column in ['date_unloading_from', 'date_unloading_to'] and isinstance(value, str):
                update_fields[b24_field] = value
            elif df_column.endswith('_id') or df_column.endswith('_id_bx') or b24_field == 'companyId':
                try:
                    update_fields[b24_field] = int(float(value))
                except (ValueError, TypeError):
                    continue
            elif isinstance(value, (int, float)):
                if isinstance(value, float) and value.is_integer():
                    update_fields[b24_field] = int(value)
                else:
                    update_fields[b24_field] = value
            else:
                update_fields[b24_field] = value
    return update_fields


async def update_all_in_parallel(dataframe, id_column, field_mappings, max_concurrent=20) -> dict:
    """Параллельно обновляет записи в Bitrix24 с ограничением одновременных запросов."""
    semaphore = asyncio.Semaphore(max_concurrent)
    total_rows = len(dataframe)
    success_count = 0
    error_count = 0
    processed_count = 0
    skipped_count = 0
    successful_item_ids = []
    start_time = time.time()

    pbar = tqdm(total=total_rows, desc="Обновление записей")

    async def update_with_semaphore(item_id, fields):
        nonlocal success_count, error_count, processed_count
        async with semaphore:
            ok = await update_single_item(item_id, fields)
            if ok:
                success_count += 1
                successful_item_ids.append(item_id)
            else:
                error_count += 1
            processed_count += 1
            pbar.update(1)
            if processed_count % 10 == 0:
                elapsed = time.time() - start_time
                rps = processed_count / elapsed if elapsed > 0 else 0
                pbar.set_postfix({
                    'успешно': success_count,
                    'ошибок': error_count,
                    'скорость': f"{rps:.2f} зап/с",
                })
            return ok

    update_tasks = []
    skipped_no_id = 0
    skipped_no_fields = 0
    for _, row in dataframe.iterrows():
        if id_column not in row or pd.isna(row[id_column]):
            skipped_count += 1
            skipped_no_id += 1
            _audit("skip", reason="no_bitrix_id", bid_id=row.get('bid_id'))
            continue
        try:
            record_id = int(float(row[id_column]))
        except (ValueError, TypeError):
            skipped_count += 1
            skipped_no_id += 1
            _audit("skip", reason="bad_bitrix_id", bid_id=row.get('bid_id'), bitrix_id=row[id_column])
            continue

        update_fields = _build_update_fields(row, field_mappings)
        if update_fields:
            update_tasks.append(asyncio.create_task(update_with_semaphore(record_id, update_fields)))
        else:
            skipped_count += 1
            skipped_no_fields += 1
            _audit("skip", reason="no_fields", bid_id=row.get('bid_id'), item_id=record_id)

    if skipped_count:
        logger.info(
            f"Пропущено {skipped_count}: без bitrix_id — {skipped_no_id}, "
            f"без полей для обновления — {skipped_no_fields}"
        )

    if skipped_count > 0:
        pbar.total = max(0, total_rows - skipped_count)
        pbar.refresh()

    await asyncio.gather(*update_tasks)
    pbar.close()

    total_time = time.time() - start_time
    return {
        'total_processed': processed_count,
        'success_count': success_count,
        'error_count': error_count,
        'skipped_count': skipped_count,
        'total_time_seconds': total_time,
        'average_speed': processed_count / total_time if total_time > 0 else 0,
        'successful_item_ids': successful_item_ids,
    }


async def run_parallel_update(dataframe, id_column, field_mappings, max_concurrent=20) -> dict:
    """Запускает параллельное обновление и печатает итоговую статистику."""
    result = await update_all_in_parallel(dataframe, id_column, field_mappings, max_concurrent)

    logger.info("===== Итоговая статистика обновления =====")
    logger.info(f"Всего записей: {len(dataframe)}")
    logger.info(f"Обработано: {result['total_processed']}")
    logger.info(f"Успешно обновлено: {result['success_count']}")
    logger.info(f"Ошибок: {result['error_count']}")
    logger.info(f"Пропущено: {result['skipped_count']}")
    logger.info(f"Общее время: {result['total_time_seconds']:.2f} сек")
    logger.info(f"Средняя скорость: {result['average_speed']:.2f} зап/сек")
    return result


def mark_orders_filled(db: DatabaseConnection, bitrix_ids) -> int:
    """Ставит bitrix_filled=1 для заказов с указанными bitrix_id (id элемента СП)."""
    ids = [int(x) for x in bitrix_ids if x is not None]
    if not ids:
        return 0

    def op(engine):
        placeholders = ','.join([f":id_{i}" for i in range(len(ids))])
        params = {f"id_{i}": v for i, v in enumerate(ids)}
        query = f"""
        UPDATE new_webapi_orders_transportation
        SET bitrix_filled = 1
        WHERE bitrix_id IN ({placeholders})
        """
        with engine.begin() as conn:
            return conn.execute(text(query), params).rowcount

    updated = db.run(op, description="пометка bitrix_filled=1", default=0)
    logger.info(f"Отмечено bitrix_filled=1: {updated} заказов")
    return updated


# ---------------------------------------------------------------------------
# Маппинг полей смарт-процесса (bitrix_field -> df_column)
# ---------------------------------------------------------------------------
FIELD_MAPPINGS = {
    'ufCrm16_1694076789': 'bid_id',
    'ufCrm16_1694076828': 'id',
    'ufCrm16_1694679020': 'cost',
    'ufCrm16_1694700845': 'cub_sum',
    'ufCrm16_1694700876': 'mass',
    'ufCrm16_1753872764': 'fkko_bitrix_id',
    'ufCrm16_1740660567200': 'number_oon',
    'ufCrm16_1753946623': 'shipping_name',
    'ufCrm16_1696418694': 'bitrix_executor_recycling_id',
    'ufCrm16_1700473566': 'date_unloading_from',
    'ufCrm16_1700473604': 'date_unloading_to',
    'ufCrm16_1695986110366': 'mass_gross',
    'ufCrm16_1753949491': 'contact_email',
    'ufCrm16_1753949514': 'contact_phone',
    'ufCrm16_1753949527': 'contact_phone_mobile',
    'ufCrm16_1753949539': 'contact_name',
    'ufCrm16_1751570883': 'facility_lon',
    'ufCrm16_1751570873': 'facility_lat',
    'ufCrm16_1751570862': 'wasteplace_point_lon',
    'ufCrm16_1751570847': 'wasteplace_point_lat',
    'ufCrm16_1694701008': 'wasteplace_address',
    'ufCrm16_1753946120': 'containers_count',
    'ufCrm16_1753946134': 'containers_list',
    'ufCrm16_1753943867': 'wasteplace_name',
    'ufCrm16_1753944125': 'wasteplace_working_hours',
    'ufCrm16_1753944809': 'wasteplace_vehicle_limit_additional',
    'ufCrm16_1694701134': 'split_contract',
    'ufCrm16_1694701224': 'facility_address',
    'ufCrm16_1757061907': 'executor_recycling_phone',
    'ufCrm16_1696418503': 'org_id_mapped',   # КОСТЫЛЬ: всегда ECOPROM_FORCED_COMPANY_ID
    'ufCrm16_1703598463': 'wasteplace_region_id',
    'ufCrm16_1703611723': 'facility_region_id',
    'companyId': 'bitrix_organization_id',
    'title': 'title',
    'ufCrm16_1695987668025': 'mass_determination_method',
    'ufCrm16_1754480238': 'containers_count_tara',
}

# Колонки, удаляемые перед обновлением (technical/служебные)
COLS_TO_DROP = [
    'fkko_id', 'wasteplace_contact_phone_additional', 'is_active', 'accrual_status',
    'accrual_date', 'accrual_paid', 'bitrix_sync_status', 'bitrix_last_sync', 'parsed_at',
    'parsed updated_at', 'created_at', 'dt_update', 'dt_create', 'bitrix_error_message',
    'name_short', 'transportation_org_id', 'parsed', 'updated_at', 'org_owner_inn', 'org_id',
    'wasteplace_region_name', 'oi_id', 'bitrix_filled', 'facility_region_name', 'status_label',
    'status_dt', 'org_name', 'executor_recycling_id', 'organization_id', 'oi_id_x', 'oi_id_y',
]


# ---------------------------------------------------------------------------
# Вспомогательные операции с БД
# ---------------------------------------------------------------------------
def read_orders(db: DatabaseConnection) -> pd.DataFrame:
    """Заказы к заполнению: is_active=1, parsed=1, bitrix_filled=0."""
    def op(engine):
        query = "SELECT * FROM new_webapi_orders_transportation WHERE is_active = 1 AND parsed = 1 AND bitrix_filled = 0"
        return pd.read_sql(query, engine)
    return db.run(op, description="выборка заказов к заполнению", default=pd.DataFrame())


def read_companies(db: DatabaseConnection) -> pd.DataFrame:
    """Компании из organization_info, у которых уже есть bx_co_id."""
    def op(engine):
        query = """
        SELECT DISTINCT id AS oi_id, bx_co_id AS oi_bx_id
        FROM organization_info
        WHERE bx_co_id IS NOT NULL
        """
        return pd.read_sql(query, engine)
    return db.run(op, description="выборка компаний", default=pd.DataFrame())


# ---------------------------------------------------------------------------
# Основной пайплайн
# ---------------------------------------------------------------------------
async def run_pipeline(db: DatabaseConnection, json_file: str, max_concurrent: int):
    # 1. JSON -> df_merged
    data = load_json_records(json_file)
    if not data:
        logger.info("JSON пуст — нечего обрабатывать")
        return
    df_merged = build_merged_dataframe(data)

    # 2. Заказы из БД
    orders = read_orders(db)
    logger.info(f"Заказов к заполнению (bitrix_filled=0): {len(orders)}")
    if orders.empty:
        logger.info("Нет заказов к заполнению — выход")
        return

    # 3. Объединение JSON + заказы
    orders['bid_id'] = _id_str(orders['bid_id'])
    df_merged['bid_id'] = _id_str(df_merged['bid_id'])
    no_bid = orders['bid_id'].isna()
    if no_bid.any():
        logger.warning(f"Заказов в БД без bid_id (не сопоставляются с JSON): {int(no_bid.sum())}")
        orders = orders[~no_bid]
    df_joined = df_merged.merge(orders, on='bid_id', how='left')

    # Диагностика: сколько JSON-записей реально сопоставилось с заказами и имеет bitrix_id
    if 'bitrix_id' in df_joined.columns:
        with_bx = int(df_joined['bitrix_id'].notna().sum())
        logger.info(f"JSON-записей: {len(df_joined)}; из них с bitrix_id (элемент создан в Битрикс): {with_bx}")
        missing_bx = df_joined.loc[df_joined['bitrix_id'].isna(), 'bid_id'].tolist()
        if missing_bx:
            logger.warning(
                f"Без bitrix_id и потому будут пропущены ({len(missing_bx)}): {missing_bx}. "
                f"Для них сначала нужно выполнить sync_orders (создать элементы в Битрикс), "
                f"либо они уже заполнены ранее (bitrix_filled=1)."
            )
    else:
        logger.warning("В выборке заказов нет колонки bitrix_id — обновлять нечего")

    # 4. Регионы
    region_map = await get_region_map()
    df_joined['facility_region_id'] = df_joined['facility_region_name'].map(region_map)
    df_joined['wasteplace_region_id'] = df_joined['wasteplace_region_name'].map(region_map)

    # 5. Метод определения массы
    df_joined['mass_determination_method'] = df_joined['mass_determination_method'].map(MASS_METHOD_MAPPING)

    # 6. ФККО
    fkko_map = await get_fkko_map()
    if fkko_map:
        df_joined['fkko_id'] = pd.to_numeric(df_joined['fkko_id'], errors='coerce')
        df_joined['fkko_id'] = df_joined['fkko_id'].where(df_joined['fkko_id'].notna())
        df_joined['fkko_id'] = df_joined['fkko_id'].map(
            lambda x: str(int(x)) if pd.notna(x) else None
        )
        df_joined['fkko_bitrix_id'] = df_joined['fkko_id'].map(fkko_map)

    # 7. Компании: определяем недостающие и импортируем
    bot = read_companies(db)
    ids_to_check = pd.concat([
        df_joined['executor_recycling_id'],
        df_joined['organization_id'],
    ]).dropna()
    ids_to_check = pd.to_numeric(ids_to_check, errors='coerce').dropna().astype('int64').astype(str).unique()
    oi_ids = pd.to_numeric(bot['oi_id'], errors='coerce').dropna().astype('int64').astype(str).unique() \
        if not bot.empty else []
    missing_ids = set(ids_to_check) - set(oi_ids)
    logger.info(f"Недостающих компаний для импорта: {len(missing_ids)}")

    if missing_ids:
        await import_companies_to_bitrix(db, [int(x) for x in missing_ids])
        bot = read_companies(db)  # перечитываем после импорта

    # 8. Приклеиваем утилизатора (executor_recycling) и отходообразователя (organization)
    # Компании без числового oi_id отбрасываем: иначе их NaN-ключ склеится
    # с заказами без утилизатора/отходообразователя и подставит чужую компанию.
    if not bot.empty:
        bot['oi_id'] = _id_str(bot['oi_id'])
        bot = bot.dropna(subset=['oi_id'])

    df_joined['executor_recycling_id'] = _id_str(df_joined['executor_recycling_id'])
    df_joined = df_joined.merge(
        bot[['oi_id', 'oi_bx_id']], left_on='executor_recycling_id', right_on='oi_id', how='left'
    ).rename(columns={'oi_bx_id': 'bitrix_executor_recycling_id'})
    if 'oi_id' in df_joined.columns:
        df_joined = df_joined.drop(columns=['oi_id'])

    df_joined['organization_id'] = _id_str(df_joined['organization_id'])
    df_joined = df_joined.merge(
        bot[['oi_id', 'oi_bx_id']], left_on='organization_id', right_on='oi_id', how='left'
    ).rename(columns={'oi_bx_id': 'bitrix_organization_id'})
    if 'oi_id' in df_joined.columns:
        df_joined = df_joined.drop(columns=['oi_id'])

    # 9. КОСТЫЛЬ: ручной маппинг организаций
    df_joined['org_id_mapped'] = df_joined['org_name'].map(ORG_NAME_MAP) if 'org_name' in df_joined.columns else None

    # 10. Заголовок
    # id после left merge — float (есть пропуски), поэтому без _id_str было бы '…/111044.0';
    # без обоих ID заголовок не формируем (None — поле не отправляется).
    transport_id = _id_str(df_joined['id'])
    df_joined['title'] = (
        'Заказ на транспортирование ' + df_joined['bid_id'] + '/' + transport_id
    ).where(df_joined['bid_id'].notna() & transport_id.notna())

    # 11. Чистим служебные колонки и дубли
    df_joined.drop(columns=[c for c in COLS_TO_DROP if c in df_joined.columns], inplace=True)
    df_joined = df_joined.loc[:, ~df_joined.columns.duplicated()]

    # 12. split_contract
    df_joined["split_contract"] = [
        _contract_label(number, init_date)
        for number, init_date in zip(df_joined["transport_contract_number"], df_joined["transport_contract_init_date"])
    ]

    # 12a. Защита от дублей: на один элемент Битрикс — ровно одна строка.
    # Полные дубли схлопываем; если строки для одного bitrix_id расходятся (например, из-за
    # повторов в таблице компаний), такой элемент не обновляем, чтобы не записать чужие данные.
    if 'bitrix_id' in df_joined.columns:
        df_joined = df_joined.drop_duplicates()
        has_bx = df_joined['bitrix_id'].notna()
        conflicts = df_joined[has_bx & df_joined.duplicated(subset='bitrix_id', keep=False)]
        if not conflicts.empty:
            conflict_ids = sorted(conflicts['bitrix_id'].astype(str).unique())
            logger.warning(
                f"Расходящиеся строки для одного элемента Битрикс ({len(conflict_ids)}), пропущены: "
                f"bitrix_id={conflict_ids}, bid_id={sorted(conflicts['bid_id'].astype(str).unique())}"
            )
            _audit("skip", reason="conflicting_rows", bitrix_ids=conflict_ids)
            df_joined = df_joined.drop(conflicts.index)

    # 13. Сортировка (для наглядности прогресса)
    if 'bitrix_id' in df_joined.columns:
        df_joined = df_joined.sort_values(by='bitrix_id', ascending=False)

    # 14. Массовое обновление в Битрикс
    logger.info("Запуск массового обновления смарт-процесса в Битрикс24")
    result = await run_parallel_update(df_joined, 'bitrix_id', FIELD_MAPPINGS, max_concurrent)

    # 15. Пометка успешно обновлённых заказов bitrix_filled=1
    marked = mark_orders_filled(db, result.get('successful_item_ids', []))

    _audit("run_summary", json_file=json_file, json_records=len(data), orders_to_fill=len(orders),
           rows_to_update=len(df_joined), marked_filled=marked,
           **{k: v for k, v in result.items() if k != 'successful_item_ids'})


async def main_async(args):
    global audit
    audit = AuditLog("fill_db")
    logger.info(f"Журнал вызовов Битрикс (JSONL): {audit.path}")
    db = DatabaseConnection()
    try:
        db.reconnect()
        await run_pipeline(db, args.json, args.max_concurrent)
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        logger.debug(traceback.format_exc())
        _audit("fatal_error", error=str(e))
    finally:
        db.close()
        audit.close()
        logger.info("Скрипт заполнения БД завершён")


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Заполнение смарт-процесса ФЭО в Битрикс24 из JSON + БД.")
    p.add_argument("--json", default=JSON_FILE,
                   help="Путь к распарсенному JSON-файлу (по умолчанию — JSON_FILE в коде).")
    p.add_argument("--max-concurrent", type=int, default=Config.BITRIX_MAX_CONCURRENT,
                   help="Макс. одновременных запросов к API Битрикс24.")
    return p


def main():
    args = build_arg_parser().parse_args()
    logger.info("=" * 70)
    logger.info("ЗАПУСК СКРИПТА ЗАПОЛНЕНИЯ БД -> БИТРИКС24")
    logger.info(f"JSON: {args.json}")
    logger.info(f"Лог-файл: {LOG_FILE}")
    logger.info("=" * 70)
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
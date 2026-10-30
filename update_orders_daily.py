#!/usr/bin/env python3
"""
Скрипт ежедневного обновления заказов из ФГИС в Bitrix24.

ЛОГИКА МАТЧИНГА (выяснено 2026-06-10, см. check_join_keys.py):
  Три ID: bitrix_id (карточка СП 137), ID транспортирования (data.id файла,
  колонка `id` БД), номер заявки (из __str__ файла, колонка `bid_id` БД).
  Заявка стабильна, транспортирования у неё могут пересоздаваться
  (напр. заявка 156321: 111044 -> old, новое 137566 -> dates-wait).
  Поэтому:
  - основной матч по НОМЕРУ ЗАЯВКИ (карточка следует за актуальным
    транспортированием), fallback — по ID транспортирования;
  - при нескольких транспортированиях одной заявки в файле берётся
    актуальное (is_active, затем самое свежее dt_update);
  - в карточке дополнительно обновляется поле "ID транспортирования"
    (ufCrm16_1694076828);
  - лог процента сматченных, предупреждение о статусах без маппинга,
    контрольная сверка одной заявки после обновления.

Запуск:
    python update_orders_daily.py [путь_к_файлу.jsonl.gz]

Для запуска по расписанию (cron):
    0 8 * * * /usr/bin/python3 /path/to/update_orders_daily.py >> /var/log/update_orders.log 2>&1
"""

import os
import sys
import json
import gzip
import re
import time
import asyncio
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

# Локальные модули проекта
from bitrix_config import b
from env_config import require_env, env_int  # значения из .env
from audit_log import AuditLog
from db_tunnel import open_db_tunnel  # SSH-туннель или прямое подключение (DB_USE_SSH)
from pipeline_common import EXIT_PARTIAL

# JSONL-журнал вызовов Битрикс для анализа после запуска (создаётся в main)
audit = None


def _audit(event, **data):
    if audit is not None:
        audit.write(event, **data)

# =============================================================================
# КОНФИГУРАЦИЯ
# =============================================================================

INPUT_FILE = None  # None = автоматически взять последнюю полную выгрузку из папки скрипта


def find_latest_full_export(directory=None):
    """Последняя ПОЛНАЯ выгрузка gisopvk_webapi_<дата>_<время>_1-<N>.jsonl.gz.

    Полная = начинается со страницы 1 (частичные вроде *_177-177 пропускаются).
    Порядок определяется по дате/времени в имени файла.
    """
    directory = Path(directory) if directory else Path(__file__).resolve().parent
    pattern = re.compile(r'^gisopvk_webapi_(\d{8})_(\d{6})_1-\d+\.jsonl\.gz$')
    candidates = []
    for f in directory.glob('gisopvk_webapi_*.jsonl.gz'):
        m = pattern.match(f.name)
        if m:
            candidates.append((m.group(1) + m.group(2), f))
    return max(candidates)[1] if candidates else None

# SSH-туннель: второй шлюз SSH_ALT_* (читается в db_tunnel, нужен только при DB_USE_SSH=1)

# База данных
DB_HOST = os.environ.get('DB_HOST', 'localhost')
DB_PORT = env_int('DB_PORT', 3306)
DB_NAME = require_env('DB_NAME')
DB_USER = require_env('DB_USER')
DB_PASSWORD = require_env('DB_PASSWORD')

# Bitrix24
ENTITY_TYPE_ID = 137  # смарт-процесс "Заказ на транспортирование"

# Контрольная сверка после обновления: номер ЗАЯВКИ -> ожидаемый статус
# проверяется перечитыванием карточки. None = пропустить.
VERIFY_SAMPLE_BID_ID = '156321'

# Маппинг статусов
STATUS_MAP = {
    'old': '549',
    'updating-operator': '550',
    'dates-wait': '551',
    'shipping': '552',
    'awaiting-tr-doc': '553',
    'executed': '554',
    'date-filling-accept': '555',
    'fo-checking': '556',
    'date-filling-fail': '557',
    'declined': '558',
    'awaiting-original-documents': '559',
    'head-signing': '560',
    'payment': '561'
}

# Маппинг полей Bitrix24 -> колонки DataFrame
FIELD_MAPPINGS = {
    'ufCrm16_1754900128': 'Текущий статус_id',
    'ufCrm16_1694076828': 'ID заказа',  # актуальный ID транспортирования (может меняться у заявки)
    'ufCrm16_1754759915': 'История с первого updating-operator',
    'ufCrm16_1754759938': 'Дата_первого_updating_operator',
    'ufCrm16_1754760117': 'Повторных_исправлений',
    'ufCrm16_1755586348': 'Количество транспортных задач',
    'ufCrm16_1694679020': 'Сумма заказа'
}

# Настройки параллелизма
MAX_CONCURRENT_REQUESTS = 20

# Логирование
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        # logging.FileHandler('/var/log/update_orders.log')
    ]
)
logger = logging.getLogger(__name__)

# Библиотеки засоряют консоль (на каждый запрос к Битрикс - по несколько строк) и топят progress bar
for _noisy in ('fast_bitrix24', 'paramiko', 'sshtunnel', 'asyncio', 'urllib3'):
    logging.getLogger(_noisy).setLevel(logging.WARNING)


# =============================================================================
# ФУНКЦИИ РАБОТЫ С БАЗОЙ ДАННЫХ
# =============================================================================

def create_db_engine_through_ssh():
    """Создаёт подключение к MariaDB через SSH-туннель (или напрямую при DB_USE_SSH=0)."""
    tunnel = open_db_tunnel(DB_HOST, DB_PORT, prefix="SSH_ALT")

    engine = create_engine(
        f'mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{tunnel.local_bind_host}:{tunnel.local_bind_port}/{DB_NAME}',
        echo=False
    )

    return engine, tunnel


# =============================================================================
# ФУНКЦИИ ПАРСИНГА ДАННЫХ
# =============================================================================

def load_orders_data(filename: str) -> list:
    """Загружает данные заказов из JSONL.GZ файла."""
    orders_data = []

    with gzip.open(filename, 'rt', encoding='utf-8') as file:
        for line in file:
            if line.strip():
                order_data = json.loads(line)
                orders_data.append(order_data)

    logger.info(f"Загружено {len(orders_data)} заказов из файла")
    return orders_data


def extract_transport_tasks(order_data: dict) -> tuple:
    """Извлекает информацию о транспортных задачах."""
    driver_task_id = None
    task_count = 0
    all_task_ids = None
    driver_tasks = None

    # Структура с relationships
    if 'data' in order_data and 'relationships' in order_data['data']:
        relationships = order_data['data']['relationships']
        if 'driver_tasks' in relationships and relationships['driver_tasks']['data']:
            driver_tasks = relationships['driver_tasks']['data']

    elif 'relationships' in order_data:
        relationships = order_data['relationships']
        if 'driver_tasks' in relationships and relationships['driver_tasks']['data']:
            driver_tasks = relationships['driver_tasks']['data']

    # Плоская структура
    elif 'driver_tasks' in order_data:
        driver_tasks = order_data['driver_tasks']

    # Обрабатываем найденные задачи
    if driver_tasks and len(driver_tasks) > 0:
        task_count = len(driver_tasks)
        task_ids = []

        for task in driver_tasks:
            if isinstance(task, dict) and 'id' in task:
                task_ids.append(task['id'])
            elif isinstance(task, (str, int)):
                task_ids.append(str(task))

        if task_ids:
            driver_task_id = task_ids[0]
            all_task_ids = ', '.join(task_ids)

    return driver_task_id, task_count, all_task_ids


def extract_basic_info(order_data: dict) -> dict:
    """Извлекает основную информацию о заказе."""
    if 'data' in order_data and 'attributes' in order_data['data']:
        attributes = order_data['data']['attributes']
        return {
            'order_id': order_data['data']['id'],
            'status': attributes['status_label'],
            'history': attributes['status_history'],
            'cost': attributes.get('cost'),
            'dt_create': attributes.get('dt_create')
        }

    elif 'attributes' in order_data:
        attributes = order_data['attributes']
        return {
            'order_id': order_data.get('id', 'unknown'),
            'status': attributes['status_label'],
            'history': attributes['status_history'],
            'cost': attributes.get('cost'),
            'dt_create': attributes.get('dt_create')
        }

    elif 'status_label' in order_data:
        return {
            'order_id': order_data.get('id', 'unknown'),
            'status': order_data['status_label'],
            'history': order_data['status_history'],
            'cost': order_data.get('cost'),
            'dt_create': order_data.get('dt_create')
        }

    else:
        raise ValueError(f"Неизвестная структура данных: {list(order_data.keys())}")


def extract_bid_id(order_data: dict) -> str | None:
    """Извлекает номер заявки на обращение из __str__ ('...Заявка на обращение ID: 156321')."""
    if 'data' in order_data and 'attributes' in order_data['data']:
        attributes = order_data['data']['attributes']
    elif 'attributes' in order_data:
        attributes = order_data['attributes']
    else:
        attributes = order_data

    m = re.search(r'Заявка на обращение ID:\s*(\d+)', str(attributes.get('__str__', '')))
    return m.group(1) if m else None


def extract_facility_phone(order_data: dict) -> str | None:
    """Извлекает телефон отходопереработчика из поля _facility."""
    if 'data' in order_data and 'attributes' in order_data['data']:
        attributes = order_data['data']['attributes']
    elif 'attributes' in order_data:
        attributes = order_data['attributes']
    else:
        attributes = order_data

    if '_facility' in attributes:
        facility = attributes['_facility']
        if facility and isinstance(facility, dict):
            return (facility.get('contact_phone_mobile') or
                    facility.get('contact_phone') or
                    facility.get('contact_phone_additional'))

    return None


MSK = timezone(timedelta(hours=3))  # Europe/Moscow: постоянно UTC+3 с 2014 г.


def to_msk(value: str) -> datetime:
    """ISO-дата ФГИС -> московское время.

    История статусов приходит в UTC (+00:00), остальные поля — в +03:00. Раньше
    смещение просто отрезалось, и события 00:00–03:00 МСК попадали на предыдущий день.
    """
    dt = datetime.fromisoformat(value)  # Python 3.11+: понимает и 'Z', и смещения
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=MSK)
    return dt.astimezone(MSK)


def format_history(status_history: list) -> str:
    """Форматирует историю статусов начиная с первого updating-operator."""
    if not status_history:
        return ""

    # Сортируем историю по дате
    sorted_history = sorted(status_history, key=lambda x: to_msk(x['dt']))

    # Находим первый updating-operator
    start_index = -1
    for i, record in enumerate(sorted_history):
        if record['label'] == 'updating-operator':
            start_index = i
            break

    filtered_history = sorted_history[start_index:] if start_index != -1 else sorted_history

    # Форматируем текст
    history_lines = []
    for record in filtered_history:
        date_str = to_msk(record['dt']).strftime('%d.%m.%Y')
        # author/comment приходят как null — get(..., default) их не подменял ("автор: None")
        author = record.get('author') or 'Система'
        comment = record.get('comment') or ''
        label = record['label']

        if comment:
            history_lines.append(f'{date_str} - {label} - "{comment}" (автор: {author})')
        else:
            history_lines.append(f'{date_str} - {label} (автор: {author})')

    return '\n'.join(history_lines)


def process_order(order_data: dict) -> dict | None:
    """Обрабатывает один заказ и возвращает словарь с данными."""
    try:
        basic_info = extract_basic_info(order_data)
        first_task, task_count, all_tasks = extract_transport_tasks(order_data)
        facility_phone = extract_facility_phone(order_data)
        bid_id = extract_bid_id(order_data)

        attrs = order_data.get('data', order_data).get('attributes', order_data)

        history_text = format_history(basic_info['history'])

        # Форматируем дату создания
        dt_create_formatted = None
        if basic_info.get('dt_create'):
            try:
                dt_create_formatted = to_msk(basic_info['dt_create']).strftime('%d-%m-%Y')
            except:
                dt_create_formatted = basic_info.get('dt_create')

        return {
            'ID заказа': basic_info['order_id'],
            'ID заявки': bid_id,
            'Активен': bool(attrs.get('is_active')),
            'Дата обновления ФГИС': str(attrs.get('dt_update') or ''),
            'ID первой транспортной задачи': first_task,
            'Все ID транспортных задач': all_tasks,
            'Количество транспортных задач': task_count,
            'Текущий статус': basic_info['status'],
            'Сумма заказа': basic_info.get('cost'),
            'Дата создания': dt_create_formatted,
            'Телефон ОП': facility_phone,
            'История с первого updating-operator': history_text
        }

    except Exception as e:
        logger.error(f"Ошибка при обработке заказа {order_data.get('id', 'unknown')}: {e}")
        return None


def calculate_corrections(df_updating: pd.DataFrame) -> pd.DataFrame:
    """Вычисляет количество исправлений для заказов со статусом updating-operator."""
    today = datetime.today()
    results = []

    for _, row in df_updating.iterrows():
        order_id = row['ID заказа']
        history = row['История с первого updating-operator']

        if not isinstance(history, str):
            continue

        events = re.findall(r'(\d{2}\.\d{2}\.\d{4})\s*-\s*(updating-operator)', history)

        if events:
            first_date = min([datetime.strptime(d, '%d.%m.%Y') for d, s in events])
            count_updating = max(len(events) - 1, 0)
            days_since_first = (today - first_date).days

            results.append({
                "ID заказа": order_id,
                "Дата_первого_updating_operator": first_date.date(),
                "Повторных_исправлений": count_updating,
                "Дней_с_первого_updating_operator": days_since_first
            })

    df_result = pd.DataFrame(results)

    return df_result.sort_values("Повторных_исправлений", ascending=False) if not df_result.empty else df_result


# =============================================================================
# ФУНКЦИИ ОБНОВЛЕНИЯ BITRIX24
# =============================================================================

async def update_single_item(b, item_id: int, fields: dict) -> bool:
    """Обновляет одну запись в Bitrix24."""
    try:
        await b.call('crm.item.update', {
            'entityTypeId': ENTITY_TYPE_ID,
            'id': item_id,
            'fields': fields
        })
        _audit("item_update", item_id=item_id, ok=True, fields=fields)
        return True
    except Exception as e:
        logger.error(f"Ошибка при обновлении записи {item_id}: {e}")
        _audit("item_update", item_id=item_id, ok=False, error=str(e), fields=fields)
        return False


async def update_all_in_parallel(
        b,
        dataframe: pd.DataFrame,
        id_column: str,
        field_mappings: dict,
        max_concurrent: int = 20
) -> dict:
    """Обновляет все записи параллельно с ограничением на количество одновременных запросов."""
    semaphore = asyncio.Semaphore(max_concurrent)
    total_rows = len(dataframe)
    success_count = 0
    error_count = 0
    processed_count = 0
    start_time = time.time()

    pbar = tqdm(total=total_rows, desc="Обновление записей", dynamic_ncols=True, mininterval=0.3)

    async def update_with_semaphore(item_id, fields):
        nonlocal success_count, error_count, processed_count
        async with semaphore:
            result = await update_single_item(b, item_id, fields)
            if result:
                success_count += 1
            else:
                error_count += 1
            processed_count += 1
            pbar.update(1)

            if processed_count % 10 == 0:
                elapsed_time = time.time() - start_time
                records_per_second = processed_count / elapsed_time if elapsed_time > 0 else 0
                remaining_records = total_rows - processed_count
                estimated_time = remaining_records / records_per_second if records_per_second > 0 else 0

                pbar.set_postfix({
                    'успешно': success_count,
                    'ошибок': error_count,
                    'скорость': f"{records_per_second:.2f} зап/с",
                    'осталось': f"{estimated_time:.1f} сек"
                })

            return result

    # Подготовка запросов
    update_tasks = []
    skipped_count = 0

    for _, row in dataframe.iterrows():
        if id_column not in row or pd.isna(row[id_column]):
            skipped_count += 1
            _audit("skip", reason="no_bitrix_id", order_id=row.get('ID заказа'), bid_id=row.get('ID заявки'))
            continue

        try:
            record_id = int(float(row[id_column]))
        except (ValueError, TypeError):
            skipped_count += 1
            _audit("skip", reason="bad_bitrix_id", order_id=row.get('ID заказа'), bitrix_id=row[id_column])
            continue

        update_fields = {}

        for b24_field, df_column in field_mappings.items():
            # Особая обработка для поля экопром
            if b24_field == 'ufCrm16_1696418503':
                update_fields[b24_field] = 2156
                continue

            if df_column in row and pd.notna(row[df_column]):
                value = row[df_column]

                if df_column in ['date_unloading_from', 'date_unloading_to'] and isinstance(value, str):
                    update_fields[b24_field] = value
                elif df_column.endswith('_id') or df_column.endswith('_id_bx') or b24_field == 'companyId':
                    if pd.notna(value):
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

        if update_fields:
            task = asyncio.create_task(update_with_semaphore(record_id, update_fields))
            update_tasks.append(task)

    if skipped_count > 0:
        pbar.total = total_rows - skipped_count
        pbar.refresh()

    # Сообщения logger печатаются НАД progress bar, а не ломают его
    with logging_redirect_tqdm():
        await asyncio.gather(*update_tasks)
    pbar.close()

    total_time = time.time() - start_time
    average_speed = processed_count / total_time if total_time > 0 else 0

    return {
        'total_processed': processed_count,
        'success_count': success_count,
        'error_count': error_count,
        'skipped_count': skipped_count,
        'total_time_seconds': total_time,
        'average_speed': average_speed
    }


async def run_parallel_update(
        dataframe: pd.DataFrame,
        id_column: str,
        field_mappings: dict,
        max_concurrent: int = 20
) -> dict:
    """Запускает параллельное обновление записей в Bitrix24."""
    result = await update_all_in_parallel(b, dataframe, id_column, field_mappings, max_concurrent)

    logger.info("===== Итоговая статистика =====")
    logger.info(f"Всего записей: {len(dataframe)}")
    logger.info(f"Обработано: {result['total_processed']}")
    logger.info(f"Успешно обновлено: {result['success_count']}")
    logger.info(f"Ошибок: {result['error_count']}")
    logger.info(f"Пропущено: {result['skipped_count']}")
    logger.info(f"Общее время выполнения: {result['total_time_seconds']:.2f} секунд")
    logger.info(f"Средняя скорость: {result['average_speed']:.2f} записей/сек")

    return result


async def verify_sample(df_joined: pd.DataFrame, bid_id: str) -> bool:
    """Контрольная сверка: перечитывает карточку тестовой заявки и сравнивает статус.

    False — статус в карточке не совпал. Заявки нет в данных — не ошибка (True).
    """
    sample = df_joined[df_joined['ID заявки'] == str(bid_id)]
    if sample.empty or pd.isna(sample.iloc[0].get('bitrix_id')):
        logger.warning(f"Контрольная сверка: заявка {bid_id} не найдена в обновлённых данных")
        return True

    row = sample.iloc[0]
    bitrix_id = int(row['bitrix_id'])
    expected = str(row['Текущий статус_id'])

    result = await b.call('crm.item.get', {'entityTypeId': ENTITY_TYPE_ID, 'id': bitrix_id})
    # fast_bitrix24 может вернуть item напрямую ЛИБО с обёртками
    # {'result': {'order0000000000': {'item': ...}}} — снимаем все слои.
    r = result
    for key in ('result', 'result', 'order0000000000', 'item'):
        if isinstance(r, dict) and key in r:
            r = r[key]
    actual = str(r.get('ufCrm16_1754900128')) if isinstance(r, dict) else None

    _audit("verify_sample", bid_id=bid_id, item_id=bitrix_id, expected=expected, actual=actual,
           ok=actual == expected)
    if actual == expected:
        logger.info(f"✅ Контрольная сверка OK: заявка {bid_id}, bitrix_id={bitrix_id}, статус={actual}")
        return True
    logger.error(f"❌ Контрольная сверка FAIL: заявка {bid_id}, bitrix_id={bitrix_id}, "
                 f"ожидалось {expected}, в карточке {actual}")
    return False


# =============================================================================
# ОСНОВНОЙ ПРОЦЕСС
# =============================================================================

def main() -> int:
    """Основная функция скрипта.

    Код выхода: 0 — успех, EXIT_PARTIAL — ошибки обновления карточек или сверки, 1 — сбой.
    """

    # Определяем путь к файлу
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
    elif INPUT_FILE:
        input_file = INPUT_FILE
    else:
        latest = find_latest_full_export()
        if latest is None:
            logger.error("Не указан входной файл и полная выгрузка не найдена!")
            logger.error("Использование: python update_orders_daily.py <путь_к_файлу.jsonl.gz>")
            sys.exit(1)
        input_file = str(latest)
        logger.info(f"Автовыбор последней полной выгрузки: {input_file}")

    if not Path(input_file).exists():
        logger.error(f"Файл не найден: {input_file}")
        sys.exit(1)

    global audit
    audit = AuditLog("update_orders_daily")
    logger.info(f"Журнал вызовов Битрикс (JSONL): {audit.path}")
    logger.info(f"Начало обработки файла: {input_file}")
    logger.info(f"Время запуска: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    try:
        # 1. Загружаем данные
        orders_data = load_orders_data(input_file)

        # 2. Обрабатываем все заказы
        processed_orders = []
        failed_orders = []

        for i, order in enumerate(orders_data):
            result = process_order(order)
            if result:
                processed_orders.append(result)
            else:
                failed_orders.append({
                    'index': i + 1,
                    'order_id': order.get('data', {}).get('id', order.get('id', 'unknown'))
                })

        logger.info(f"Успешно обработано: {len(processed_orders)} из {len(orders_data)}")
        if failed_orders:
            logger.warning(f"Не удалось обработать: {len(failed_orders)} заказов")
            _audit("parse_failed", orders=failed_orders)

        # 3. Создаём DataFrame
        df = pd.DataFrame(processed_orders)

        # 4. Статистика
        df_updating = df[df['Текущий статус'] == 'updating-operator'].copy()
        logger.info(f"Заказов со статусом 'updating-operator': {len(df_updating)}")

        # 5. Вычисляем исправления
        result_corr_df = calculate_corrections(df_updating)

        # 6. Объединяем данные
        df_merged = pd.merge(df, result_corr_df, on="ID заказа", how="left") if not result_corr_df.empty else df

        # 7. Матчинг с БД и получение bitrix_id
        # Стабильный ключ — НОМЕР ЗАЯВКИ ('ID заявки' из __str__ файла <-> БД.bid_id):
        # заявка может получить новое транспортирование (напр. 156321: 111044->137566),
        # и карточка должна следовать за актуальным. Fallback — ID транспортирования
        # ('ID заказа' = data.id файла <-> БД.id) для записей без распарсенной заявки.

        # 7а. Одна заявка может иметь несколько транспортирований в файле —
        # оставляем актуальное (is_active, затем самое свежее по dt_update)
        # Нечисловой ID ('unknown' из process_order) раньше ронял весь запуск на astype(int)
        order_ids = pd.to_numeric(df_merged['ID заказа'], errors='coerce').astype('Int64')
        if order_ids.isna().any():
            bad = df_merged.loc[order_ids.isna(), 'ID заказа'].tolist()
            logger.warning(f"Пропущены записи с некорректным ID заказа ({len(bad)}): {bad[:20]}")
            _audit("skip", reason="bad_order_id", order_ids=bad)
            df_merged, order_ids = df_merged[order_ids.notna()].copy(), order_ids[order_ids.notna()]
        df_merged['ID заказа'] = order_ids.astype(str)
        before_dedup = len(df_merged)
        has_bid = df_merged['ID заявки'].notna()
        df_with_bid = (df_merged[has_bid]
                       .sort_values(['ID заявки', 'Активен', 'Дата обновления ФГИС'])
                       .drop_duplicates('ID заявки', keep='last'))
        df_merged = pd.concat([df_with_bid, df_merged[~has_bid]], ignore_index=True)
        if len(df_merged) < before_dedup:
            logger.info(f"Свёрнуто дублей транспортирований по одной заявке: "
                        f"{before_dedup - len(df_merged)}")

        logger.info("Подключение к базе данных...")
        engine, tunnel = create_db_engine_through_ssh()

        try:
            orders_query = "SELECT id, bid_id, bitrix_id FROM new_webapi_orders_transportation"
            orders = pd.read_sql(orders_query, engine)
        finally:
            tunnel.stop()

        orders = orders.dropna(subset=['bitrix_id'])
        # При дублях ключа берём последнюю строку
        by_bid = (orders.dropna(subset=['bid_id'])
                  .assign(bid_id=lambda d: d['bid_id'].astype(int).astype(str))
                  .drop_duplicates('bid_id', keep='last')
                  .set_index('bid_id')['bitrix_id'])
        by_transport = (orders.dropna(subset=['id'])
                        .assign(id=lambda d: d['id'].astype(int).astype(str))
                        .drop_duplicates('id', keep='last')
                        .set_index('id')['bitrix_id'])

        # 7б. Основной матч по заявке, fallback по транспортированию
        df_joined = df_merged.copy()
        df_joined['bitrix_id'] = df_joined['ID заявки'].map(by_bid)
        df_joined['_match'] = df_joined['bitrix_id'].notna().map({True: 1, False: 0})  # 1=по заявке
        matched_by_bid = df_joined['bitrix_id'].notna().sum()
        fb = df_joined['bitrix_id'].isna()
        if fb.any():  # pandas 3 не даёт записать пустой (строковый) результат map в int64-колонку
            df_joined.loc[fb, 'bitrix_id'] = df_joined.loc[fb, 'ID заказа'].map(by_transport)
        df_joined['bitrix_id'] = df_joined['bitrix_id'].astype('Int64')

        # 7в. Разные записи файла могут вести в ОДНУ карточку (заявка-матч + fallback
        # по старому транспортированию, как 137566 и 111044 -> карточка 5559).
        # Оставляем одну: приоритет матчу по заявке, затем актуальность.
        before_collide = len(df_joined)
        has_bx = df_joined['bitrix_id'].notna()
        df_bx = (df_joined[has_bx]
                 .sort_values(['bitrix_id', '_match', 'Активен', 'Дата обновления ФГИС'])
                 .drop_duplicates('bitrix_id', keep='last'))
        df_joined = pd.concat([df_bx, df_joined[~has_bx]], ignore_index=True)
        if len(df_joined) < before_collide:
            logger.info(f"Свёрнуто коллизий (несколько записей файла -> одна карточка): "
                        f"{before_collide - len(df_joined)}")

        matched = df_joined['bitrix_id'].notna().sum()
        logger.info(f"Матчинг: всего {len(df_joined)}, по заявке: {matched_by_bid}, "
                    f"по транспортированию (fallback): {matched - matched_by_bid}, "
                    f"итого с bitrix_id: {matched} ({matched / len(df_joined) * 100:.1f}%)")
        if matched < len(df_joined):
            logger.warning(f"Без bitrix_id (новые, нужно импортировать): {len(df_joined) - matched}")

        # 8. Маппинг статусов
        df_joined['Текущий статус_id'] = df_joined['Текущий статус'].map(STATUS_MAP)
        unmapped = df_joined[df_joined['Текущий статус_id'].isna()]['Текущий статус'].unique()
        if len(unmapped):
            logger.warning(f"Статусы без маппинга (не будут обновлены): {list(unmapped)}")
            _audit("unmapped_status", statuses=list(unmapped))

        # 9. Обновляем Bitrix24
        logger.info("Начинаю обновление Bitrix24...")
        result = asyncio.run(
            run_parallel_update(df_joined, 'bitrix_id', FIELD_MAPPINGS, MAX_CONCURRENT_REQUESTS)
        )

        # 10. Контрольная сверка
        verify_ok = True
        if VERIFY_SAMPLE_BID_ID:
            verify_ok = asyncio.run(verify_sample(df_joined, VERIFY_SAMPLE_BID_ID))

        _audit("run_summary", input_file=input_file, orders_in_file=len(orders_data),
               failed_to_parse=len(failed_orders), rows=len(df_joined),
               matched_by_bid=int(matched_by_bid), matched_total=int(matched), **result)
        logger.info(f"Обработка завершена: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        if result['error_count'] or not verify_ok:
            logger.error(f"Запуск завершён с ошибками: ошибок обновления {result['error_count']}, "
                         f"сверка {'OK' if verify_ok else 'FAIL'}")
            return EXIT_PARTIAL
        return 0

    except Exception as e:
        logger.exception(f"Критическая ошибка: {e}")
        _audit("fatal_error", error=str(e))
        sys.exit(1)

    finally:
        audit.close()


if __name__ == "__main__":
    sys.exit(main())
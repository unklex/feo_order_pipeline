"""
Улучшенный скрипт для импорта заказов на транспортирование из JSONL.GZ архивов
в минимальную таблицу new_webapi_orders_transportation.

Основные улучшения:
- Минимальная структура данных для интеграции с Bitrix24
- Использование json_normalize для надежного извлечения данных
- Защита от дубликатов и контроль состояния синхронизации
- Обновление только несинхронизированных записей
"""

import os
import sys
import gzip
import json
import glob
import logging
import pandas as pd
import re
from typing import Dict, Any, List, Tuple, Optional
from datetime import datetime
from pandas import json_normalize

from sqlalchemy import create_engine, text
import pymysql

from audit_log import AuditLog
from db_tunnel import open_db_tunnel, use_ssh  # SSH-туннель или прямое подключение (DB_USE_SSH)

# JSONL-журнал итогов по архивам для анализа после запуска (создаётся в main)
audit = None

# Определяем путь к папке с логами
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/minimal_order_import.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("MinimalOrderImport")

# Конфигурация SSH-туннеля и подключения к базе данных
# Значения берутся из файла .env рядом со скриптом (см. .env.example)
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))
except ImportError:
    # python-dotenv не установлен - используем только реальные переменные окружения
    # Установка: pip install python-dotenv
    pass


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        sys.exit(f"Не задана переменная окружения {name}. Заполните .env (см. .env.example).")
    return value


# SSH_* (в т.ч. вход по ключу SSH_KEY_PATH / SSH_KEY_PASSPHRASE) читаются в db_tunnel
# и нужны только при DB_USE_SSH=1.
DB_HOST = os.environ.get('DB_HOST', 'localhost')
DB_PORT = int(os.environ.get('DB_PORT', '3306'))
DB_NAME = _require_env('DB_NAME')
DB_USER = _require_env('DB_USER')
DB_PASSWORD = _require_env('DB_PASSWORD')


def import_minimal_orders(file_path: str) -> pd.DataFrame:
    """Импорт минимального набора данных заказов с использованием json_normalize"""

    logger.info(f"Импорт минимальных данных из {file_path}")
    records = []

    with gzip.open(file_path, 'rt', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            try:
                if not line.strip():
                    continue

                data = json.loads(line)

                # Проверяем что это заказ на транспортирование
                if data.get('type') != 'order_transportation' or not data.get('id'):
                    continue

                # Извлекаем только ключевые поля с помощью json_normalize
                flat = json_normalize(
                    data,
                    sep="__",
                    record_path=None,
                    meta=[
                        # Основные поля
                        'id',
                        'type',

                        # Статус
                        ['attributes', 'status_label'],
                        ['attributes', 'status_dt'],
                        ['attributes', 'is_active'],

                        # Финансы
                        ['attributes', 'cost'],
                        ['attributes', 'accrual_status'],
                        ['attributes', 'accrual_date'],
                        ['attributes', 'accrual_paid'],

                        # Организация
                        ['attributes', '_org_info', 'id'],
                        ['attributes', '_org_info', 'name_short'],
                        ['attributes', 'org_owner_inn'],

                        # Связанная заявка
                        ['relationships', 'bid', 'data', 'id'],

                        # Технические поля
                        ['attributes', 'dt_create'],
                        ['attributes', 'dt_update']
                    ],
                    errors='ignore'
                )

                if len(flat) > 0:
                    records.append(flat)

                if line_num % 1000 == 0:
                    logger.info(f"Обработано {line_num} строк, получено {len(records)} заказов")

            except json.JSONDecodeError as e:
                logger.error(f"Ошибка JSON в строке {line_num}: {e}")
                continue
            except Exception as e:
                logger.error(f"Ошибка обработки строки {line_num}: {e}")
                continue

    if not records:
        logger.warning("Нет записей для обработки")
        return pd.DataFrame()

    # Объединяем все записи
    df = pd.concat(records, ignore_index=True)
    logger.info(f"Создан DataFrame с {len(df)} строками и {len(df.columns)} колонками")

    return df


def prepare_minimal_data(df: pd.DataFrame) -> pd.DataFrame:
    """Подготавливает данные для минимальной таблицы"""

    logger.info("Подготовка данных для минимальной таблицы")

    if df.empty:
        return pd.DataFrame()

    # Создаем финальный DataFrame с правильными названиями колонок
    result_df = pd.DataFrame()

    # Маппинг колонок с проверкой существования
    column_mapping = {
        'id': 'id',
        'attributes__status_label': 'status_label',
        'attributes__status_dt': 'status_dt',
        'attributes__is_active': 'is_active',
        'attributes__cost': 'cost',
        'attributes__accrual_status': 'accrual_status',
        'attributes__accrual_date': 'accrual_date',
        'attributes__accrual_paid': 'accrual_paid',
        'attributes___org_info__id': 'org_id',
        'attributes___org_info__name_short': 'org_name',
        'attributes__org_owner_inn': 'org_owner_inn',
        'relationships__bid__data__id': 'bid_id',
        'attributes__dt_create': 'dt_create',
        'attributes__dt_update': 'dt_update'
    }

    # Копируем данные с переименованием
    for source_col, target_col in column_mapping.items():
        if source_col in df.columns:
            result_df[target_col] = df[source_col]
            non_null = df[source_col].notna().sum()
            logger.info(f"✅ {source_col} -> {target_col} ({non_null} значений)")
        else:
            result_df[target_col] = None
            logger.warning(f"❌ Отсутствует: {source_col}")

    # Добавляем поля для Bitrix интеграции (только для новых записей)
    result_df['bitrix_sync_status'] = 'pending'
    #result_df[''] = None
    result_df['bitrix_last_sync'] = None
    result_df['bitrix_error_message'] = None

    # Обрабатываем даты
    date_columns = ['status_dt', 'accrual_date', 'dt_create', 'dt_update']
    for col in date_columns:
        if col in result_df.columns:
            result_df[col] = process_datetime_column(result_df[col])
            processed_count = result_df[col].notna().sum()
            logger.info(f"Обработано дат в {col}: {processed_count}")

    # Показываем финальную статистику
    logger.info("=== ФИНАЛЬНАЯ СТАТИСТИКА ===")
    for col in result_df.columns:
        non_null = result_df[col].notna().sum()
        total = len(result_df)
        percentage = (non_null / total) * 100 if total > 0 else 0
        status = "✅" if percentage > 90 else "⚠️" if percentage > 10 else "❌"
        logger.info(f"{status} {col:<25} {non_null:>4}/{total} ({percentage:5.1f}%)")

    # Проверяем критические поля
    critical_fields = ['id', 'status_label', 'cost', 'org_id', 'org_name', 'org_owner_inn']
    missing_critical = []

    for field in critical_fields:
        if field in result_df.columns:
            non_null = result_df[field].notna().sum()
            if non_null == 0:
                missing_critical.append(field)
        else:
            missing_critical.append(field)

    if missing_critical:
        logger.error(f"❌ КРИТИЧЕСКИЕ ПОЛЯ ПУСТЫЕ: {missing_critical}")
        return pd.DataFrame()

    logger.info(f"✅ Подготовлено {len(result_df)} записей для импорта")
    return result_df


def process_datetime_column(series: pd.Series) -> pd.Series:
    """Обрабатывает колонку с датами для совместимости с MySQL"""

    def clean_datetime(dt_str):
        if pd.isna(dt_str) or dt_str is None:
            return None
        try:
            dt_str = str(dt_str)
            # Удаляем timezone информацию
            dt_str = re.sub(r'[+-]\d{2}:?\d{2}$', '', dt_str)  # +03:00 или +0300
            dt_str = re.sub(r'Z$', '', dt_str)  # UTC marker Z

            # Обрезаем микросекунды до 6 символов
            if '.' in dt_str:
                parts = dt_str.split('.')
                if len(parts[1]) > 6:
                    dt_str = parts[0] + '.' + parts[1][:6]

            # Парсим дату
            dt = pd.to_datetime(dt_str, errors='coerce')
            if pd.notna(dt):
                return dt.strftime('%Y-%m-%d %H:%M:%S')
            return None
        except Exception as e:
            logger.debug(f"Не удалось обработать дату '{dt_str}': {e}")
            return None

    return series.apply(clean_datetime)


def save_minimal_to_database(df: pd.DataFrame, engine) -> Tuple[int, int]:
    """
    Сохраняет минимальные данные в БД с защитой от дубликатов и 
    сохранением статуса синхронизации Bitrix
    """
    if df.empty:
        logger.warning("Пустой DataFrame")
        return 0, 0

    logger.info("Сохранение минимальных данных в БД")

    # Названия таблиц
    table_name = 'new_webapi_orders_transportation'
    temp_table_name = 'temp_new_webapi_orders_transportation'

    # Записываем во временную таблицу
    logger.info(f"Запись во временную таблицу {temp_table_name}")
    try:
        df.to_sql(temp_table_name, con=engine, if_exists='replace', index=False, method='multi')
        logger.info("✅ Данные успешно записаны во временную таблицу")
    except Exception as e:
        logger.error(f"❌ Ошибка при записи во временную таблицу: {e}")
        raise

    rows_added = 0
    rows_updated = 0

    with engine.begin() as conn:
        try:
            # 1. ДОБАВЛЯЕМ ТОЛЬКО НОВЫЕ ЗАПИСИ
            logger.info("Добавление новых записей")
            insert_query = text(f"""
                INSERT INTO {table_name} (
                    id, status_label, status_dt, is_active, cost, accrual_status,
                    accrual_date, accrual_paid, bid_id, org_id, org_name, org_owner_inn,
                    dt_create, dt_update, bitrix_sync_status, 
                    bitrix_last_sync, bitrix_error_message
                )
                SELECT 
                    id, status_label, status_dt, is_active, cost, accrual_status,
                    accrual_date, accrual_paid, bid_id, org_id, org_name, org_owner_inn,
                    dt_create, dt_update, bitrix_sync_status,
                    bitrix_last_sync, bitrix_error_message
                FROM {temp_table_name}
                WHERE NOT EXISTS (
                    SELECT 1 FROM {table_name} 
                    WHERE {table_name}.id = {temp_table_name}.id
                )
            """)

            result = conn.execute(insert_query)
            rows_added = result.rowcount
            logger.info(f"✅ Добавлено {rows_added} новых записей")

            # 2. СБРОС СТАТУСА СИНХРОНИЗАЦИИ ПРИ КРИТИЧЕСКИХ ИЗМЕНЕНИЯХ
            # Идёт ДО обновления полей: сброшенная запись становится 'pending' и получает
            # новые значения в шаге 3 этого же запуска (раньше — только при следующем).
            # NOT (a <=> b) вместо a != b: != с NULL даёт NULL, и переход NULL <-> значение
            # не считался изменением.
            logger.info("Проверка критических изменений в синхронизированных записях")
            reset_sync_query = text(f"""
                UPDATE {table_name} AS target
                JOIN {temp_table_name} AS source ON target.id = source.id
                SET
                    target.bitrix_sync_status = 'pending',
                    target.bitrix_last_sync = NULL,
                    target.bitrix_error_message = 'Требует пересинхронизации из-за критических изменений',
                    target.updated_at = NOW()
                WHERE
                    target.bitrix_sync_status = 'synced'
                    AND (
                        NOT (target.cost <=> source.cost) OR
                        NOT (target.accrual_status <=> source.accrual_status) OR
                        NOT (target.status_label <=> source.status_label)
                    )
            """)

            result = conn.execute(reset_sync_query)
            reset_count = result.rowcount
            if reset_count > 0:
                logger.warning(f"⚠️ Сброшен статус синхронизации для {reset_count} записей из-за критических изменений")

            # 3. ОБНОВЛЯЕМ ТОЛЬКО НЕСИНХРОНИЗИРОВАННЫЕ ЗАПИСИ
            # Защищаем уже синхронизированные записи от изменений
            logger.info("Обновление несинхронизированных записей")
            update_query = text(f"""
                UPDATE {table_name} AS target
                JOIN {temp_table_name} AS source ON target.id = source.id
                SET
                    target.status_label = source.status_label,
                    target.status_dt = source.status_dt,
                    target.is_active = source.is_active,
                    target.cost = source.cost,
                    target.accrual_status = source.accrual_status,
                    target.accrual_date = source.accrual_date,
                    target.accrual_paid = source.accrual_paid,
                    target.bid_id = source.bid_id,
                    target.org_id = source.org_id,
                    target.org_name = source.org_name,
                    target.org_owner_inn = source.org_owner_inn,
                    target.dt_create = source.dt_create,
                    target.dt_update = source.dt_update,
                    target.updated_at = NOW()
                WHERE 
                    target.bitrix_sync_status IN ('pending', 'error')
                    AND (
                        NOT (target.status_label <=> source.status_label) OR
                        NOT (target.cost <=> source.cost) OR
                        NOT (target.accrual_status <=> source.accrual_status) OR
                        NOT (target.is_active <=> source.is_active)
                    )
            """)

            result = conn.execute(update_query)
            rows_updated = result.rowcount
            logger.info(f"✅ Обновлено {rows_updated} несинхронизированных записей")

            # 4. ЛОГИРУЕМ ПРОПУЩЕННЫЕ СИНХРОНИЗИРОВАННЫЕ ЗАПИСИ
            count_synced_query = text(f"""
                SELECT COUNT(*) as synced_count
                FROM {table_name} AS target
                JOIN {temp_table_name} AS source ON target.id = source.id
                WHERE target.bitrix_sync_status = 'synced'
            """)

            result = conn.execute(count_synced_query)
            synced_count = result.fetchone()[0]
            if synced_count > 0:
                logger.info(f"ℹ️ Пропущено {synced_count} уже синхронизированных записей")

            if audit is not None:
                audit.write("db_merge", rows_in_file=len(df), rows_added=rows_added,
                            rows_updated=rows_updated, sync_reset=reset_count, synced_untouched=synced_count)

            # Удаляем временную таблицу
            drop_query = text(f"DROP TABLE IF EXISTS {temp_table_name}")
            conn.execute(drop_query)

        except Exception as e:
            logger.error(f"❌ Ошибка при работе с БД: {e}")
            raise

    return rows_added, rows_updated


def process_archive(archive_path: str, engine) -> Tuple[int, int, int]:
    """Обрабатывает один архив с минимальным импортом"""
    try:
        logger.info(f"Обработка архива {os.path.basename(archive_path)}")

        # Импортируем данные
        df_raw = import_minimal_orders(archive_path)
        if df_raw.empty:
            logger.warning(f"Нет данных в архиве {archive_path}")
            return 0, 0, 0

        # Подготавливаем данные
        df_prepared = prepare_minimal_data(df_raw)
        if df_prepared.empty:
            logger.warning(f"Не удалось подготовить данные из архива {archive_path}")
            return 0, 0, 0

        # Сохраняем в БД
        if audit is not None:
            audit.write("archive_start", archive=os.path.basename(archive_path), rows_raw=len(df_raw))
        rows_added, rows_updated = save_minimal_to_database(df_prepared, engine)

        total_processed = len(df_prepared)
        logger.info(
            f"Архив {os.path.basename(archive_path)}: обработано {total_processed}, добавлено {rows_added}, обновлено {rows_updated}")

        return total_processed, rows_added, rows_updated

    except Exception as e:
        logger.error(f"Ошибка при обработке архива {archive_path}: {e}")
        if audit is not None:
            audit.write("archive_error", archive=os.path.basename(archive_path), error=str(e))
        return 0, 0, 0


def find_archives(directory: str = '.', pattern: str = 'gisopvk_webapi_*.jsonl.gz') -> List[str]:
    """Находит все архивы JSONL.GZ в указанной директории"""
    search_pattern = os.path.join(directory, pattern)
    archives = glob.glob(search_pattern)

    if not archives:
        logger.warning(f"Архивы не найдены по шаблону {search_pattern}")
    else:
        logger.info(f"Найдено {len(archives)} архивов")

    return archives


def check_database_structure(engine):
    """Проверяет структуру таблицы в БД"""
    try:
        with engine.connect() as conn:
            # Проверяем существование таблицы
            check_table_query = text("""
                SELECT COUNT(*) as table_exists 
                FROM information_schema.tables 
                WHERE table_schema = :db_name AND table_name = :table_name
            """)
            result = conn.execute(check_table_query,
                                  {'db_name': DB_NAME, 'table_name': 'new_webapi_orders_transportation'})
            table_exists = result.fetchone()[0]

            if table_exists == 0:
                logger.error("❌ Таблица 'new_webapi_orders_transportation' не существует!")
                logger.info("Создайте таблицу с помощью SQL скрипта из артефакта")
                return False

            # Проверяем количество записей
            count_query = text("SELECT COUNT(*) as total_records FROM new_webapi_orders_transportation")
            result = conn.execute(count_query)
            total_records = result.fetchone()[0]

            # Проверяем статистику синхронизации
            sync_stats_query = text("""
                SELECT 
                    bitrix_sync_status, 
                    COUNT(*) as count 
                FROM new_webapi_orders_transportation 
                GROUP BY bitrix_sync_status
            """)
            sync_stats = conn.execute(sync_stats_query).fetchall()

            logger.info(f"✅ Таблица существует. Записей: {total_records}")
            logger.info("Статистика синхронизации:")
            for status, count in sync_stats:
                logger.info(f"  {status}: {count} записей")

            return True

    except Exception as e:
        logger.error(f"Ошибка при проверке БД: {e}")
        return False


def main():
    """Основная функция скрипта"""
    global audit
    logger.info("Запуск улучшенного импорта минимальных данных заказов")
    audit = AuditLog("import_to_db")
    logger.info(f"Журнал итогов (JSONL): {audit.path}")

    tunnel = None
    try:
        # SSH-туннель (или прямое подключение при DB_USE_SSH=0)
        try:
            # keepalive-пакет каждые 30 с, чтобы туннель не рвался при долгом импорте
            tunnel = open_db_tunnel(DB_HOST, DB_PORT, set_keepalive=30.0)
        except Exception as e:
            msg = (
                f"Не удалось подключиться к SSH-шлюзу {os.environ.get('SSH_HOST')}:{os.environ.get('SSH_PORT', 22)}: {e}. "
                "Проверьте: доступность порта (Test-NetConnection), VPN, "
                "разрешён ли ваш IP на сервере, запущен ли sshd, логин/пароль."
            )
            logger.error(msg)
            print(f"\n{msg}")
            return 1

        # Создание подключения к базе данных
        engine = create_engine(
            f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{tunnel.local_bind_host}:{tunnel.local_bind_port}/{DB_NAME}",
            echo=False
        )

        # Проверяем структуру БД
        if not check_database_structure(engine):
            return 1

        # Находим все архивы
        archives = find_archives()
        if not archives:
            logger.error("Не найдено архивов для обработки")
            return 1

        # Предлагаем пользователю выбрать архивы для обработки
        print("\nНайденные архивы:")
        for i, archive in enumerate(archives, 1):
            file_size = os.path.getsize(archive) / (1024 * 1024)  # в МБ
            print(f"{i}. {os.path.basename(archive)} ({file_size:.2f} МБ)")

        choice = input("\nВыберите архивы для обработки (номера через запятую или 'all'): ")

        if choice.lower() == 'all':
            archives_to_process = archives
        else:
            try:
                selected_indices = [int(idx.strip()) - 1 for idx in choice.split(',')]
                archives_to_process = [archives[idx] for idx in selected_indices if 0 <= idx < len(archives)]
            except (ValueError, IndexError):
                logger.error("Некорректный ввод, обрабатываем все архивы")
                archives_to_process = archives

        # Обработка выбранных архивов
        total_records = 0
        total_added = 0
        total_updated = 0

        for archive in archives_to_process:
            print(f"\nОбработка архива {os.path.basename(archive)}...")
            records, added, updated = process_archive(archive, engine)

            total_records += records
            total_added += added
            total_updated += updated

            print(f"Обработано {records} записей, добавлено {added}, обновлено {updated}")

        # Итоговая информация
        logger.info(f"Итоги: обработано {total_records}, добавлено {total_added}, обновлено {total_updated}")
        print(f"\n=== ИТОГИ ИМПОРТА ===")
        print(f"Обработано архивов: {len(archives_to_process)}")
        print(f"Обработано записей: {total_records}")
        print(f"Добавлено в базу: {total_added}")
        print(f"Обновлено в базе: {total_updated}")

        return 0

    except Exception as e:
        logger.error(f"Ошибка при выполнении импорта: {e}")
        print(f"\nПроизошла ошибка: {str(e)}")
        return 1

    finally:
        # Закрытие SSH-туннеля
        if tunnel is not None and tunnel.is_active:
            if use_ssh():
                logger.info("Закрытие SSH-туннеля")
            tunnel.stop()
        audit.close()


if __name__ == "__main__":
    sys.exit(main())
"""
История статусов заказов из полной выгрузки ФГИС -> таблица order_status_history (MySQL/MariaDB).

В выгрузке у каждого транспортирования есть attributes.status_history — все смены статуса
(dt в UTC, label, author, у части — comment/reason/date). В new_webapi_orders_transportation
хранится только текущий статус, поэтому без этой таблицы нельзя посчитать время на этапах,
возвраты на доработку и путь заказа по статусам.

Загрузка идемпотентна (INSERT IGNORE по ключу заказ + время + статус): одну и ту же выгрузку
можно загружать сколько угодно раз, новые события просто добавляются.

    python status_history.py [путь_к_выгрузке.jsonl.gz]   # по умолчанию — последняя полная выгрузка

Время событий хранится по Москве (как status_dt в new_webapi_orders_transportation).
Код выхода: 0 — успех, 1 — ошибка.
"""

import argparse
import gzip
import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine, text

from env_config import require_env  # заодно подгружает .env
from db_tunnel import open_db_tunnel
from pipeline_common import latest_full_export

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("status_history")

TABLE = "order_status_history"
MSK = timezone(timedelta(hours=3))
CHUNK = 1000

DDL = f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
    order_id     BIGINT       NOT NULL COMMENT 'ID транспортирования (= new_webapi_orders_transportation.id)',
    bid_id       BIGINT       NULL     COMMENT 'Номер заявки',
    status_label VARCHAR(64)  NOT NULL,
    status_dt    DATETIME(6)  NOT NULL COMMENT 'Время смены статуса, МСК',
    author       VARCHAR(255) NULL,
    comment      TEXT         NULL,
    reason       TEXT         NULL,
    date_value   VARCHAR(64)  NULL     COMMENT 'Поле date события, как в ФГИС',
    files_count  INT          NULL,
    loaded_at    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (order_id, status_dt, status_label),
    KEY idx_bid (bid_id),
    KEY idx_label_dt (status_label, status_dt)
) DEFAULT CHARSET = utf8mb4 COMMENT = 'История статусов заказов ФГИС (status_history из выгрузки)'
"""

INSERT = text(f"""
INSERT IGNORE INTO {TABLE}
    (order_id, bid_id, status_label, status_dt, author, comment, reason, date_value, files_count)
VALUES
    (:order_id, :bid_id, :status_label, :status_dt, :author, :comment, :reason, :date_value, :files_count)
""")


def _int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _text(value, max_len=None):
    if value is None:
        return None
    value = str(value).strip()
    return value[:max_len] if value else None


def to_msk_naive(value: str) -> datetime:
    """ISO-время ФГИС (UTC или со смещением) -> московское без часового пояса (для DATETIME)."""
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=MSK)
    return dt.astimezone(MSK).replace(tzinfo=None)


def history_rows(record: dict) -> list:
    """Строки для order_status_history из одной записи выгрузки. Битые события пропускаются."""
    order_id = _int(record.get("id"))
    if order_id is None:
        return []
    bid_id = _int(((record.get("relationships") or {}).get("bid") or {}).get("data", {}).get("id"))
    rows = []
    for event in (record.get("attributes") or {}).get("status_history") or []:
        if not isinstance(event, dict) or not event.get("dt") or not event.get("label"):
            continue
        try:
            status_dt = to_msk_naive(event["dt"])
        except (TypeError, ValueError):
            continue
        files = event.get("files")
        rows.append({
            "order_id": order_id,
            "bid_id": bid_id,
            "status_label": str(event["label"])[:64],
            "status_dt": status_dt,
            "author": _text(event.get("author"), 255),
            "comment": _text(event.get("comment")),
            "reason": _text(event.get("reason")),
            "date_value": _text(event.get("date"), 64),
            "files_count": len(files) if isinstance(files, list) else None,
        })
    return rows


def read_export(path) -> list:
    """Все события истории из файла выгрузки (.jsonl.gz)."""
    rows = []
    with gzip.open(path, "rt", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                rows.extend(history_rows(json.loads(line)))
    return rows


def load(engine, rows: list) -> int:
    """Создаёт таблицу при необходимости и добавляет новые события. Возвращает число добавленных."""
    added = 0
    with engine.begin() as conn:
        conn.execute(text(DDL))
        for i in range(0, len(rows), CHUNK):
            added += conn.execute(INSERT, rows[i:i + CHUNK]).rowcount
    return added


def main() -> int:
    ap = argparse.ArgumentParser(description="Загрузка истории статусов из выгрузки ФГИС в order_status_history.")
    ap.add_argument("export", nargs="?", help="Файл выгрузки; по умолчанию — последняя полная.")
    args = ap.parse_args()

    path = args.export or latest_full_export()
    if not path or not os.path.exists(path):
        logger.error("Полная выгрузка не найдена")
        return 1

    rows = read_export(path)
    orders = len({r["order_id"] for r in rows})
    logger.info(f"{os.path.basename(str(path))}: событий истории {len(rows)}, заказов {orders}")

    tunnel = open_db_tunnel(os.environ.get("DB_HOST", "localhost"), int(os.environ.get("DB_PORT") or 3306))
    try:
        engine = create_engine(
            f"mysql+pymysql://{require_env('DB_USER')}:{require_env('DB_PASSWORD')}"
            f"@{tunnel.local_bind_host}:{tunnel.local_bind_port}/{require_env('DB_NAME')}",
            connect_args={"connect_timeout": 30},
        )
        try:
            added = load(engine, rows)
        finally:
            engine.dispose()
    except Exception as e:
        logger.error(f"Не удалось загрузить историю статусов: {e}")
        return 1
    finally:
        tunnel.stop()

    logger.info(f"Добавлено новых событий в {TABLE}: {added} (уже были: {len(rows) - added})")
    return 0


if __name__ == "__main__":
    sys.exit(main())

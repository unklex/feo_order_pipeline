#!/usr/bin/env python3
"""
Отладка обновления заказа bid_id=156321 (смарт-процесс 137).

РЕЗУЛЬТАТЫ ЖИВОГО ТЕСТА 2026-06-10 (через webhook):
  - crm.item.update со СТРОКОЙ '551' для enumeration-поля ufCrm16_1754900128
    записывается корректно и держится (перечитано через 8 сек) — тип значения НЕ проблема.
  - Роботы/БП значение не откатывают.
  - Причина бага: join в update_orders_daily.py шёл по внутреннему PK `id`
    вместо bid_id -> обновлялись чужие карточки.

Этот скрипт проверяет маппинг в БД: какой bitrix_id даёт join по id (старый, баг)
vs join по bid_id (правильный), затем сверяет статус карточки в Битриксе.
"""
import os, asyncio, json, logging

from sqlalchemy import create_engine, text
from sshtunnel import SSHTunnelForwarder

from bitrix_config import b
from env_config import require_env, env_int  # значения из .env

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

ENTITY_TYPE_ID = 137
BID_ID = 156321                       # ID заявки ФГИС
STATUS_FIELD = 'ufCrm16_1754900128'   # поле "Текущий статус ФГИС"
TARGET_VALUE = '551'                  # dates-wait по STATUS_MAP

SSH_HOST = require_env('SSH_ALT_HOST')
SSH_PORT = env_int('SSH_ALT_PORT', 22)
SSH_USERNAME = require_env('SSH_ALT_USERNAME')
SSH_PASSWORD = require_env('SSH_ALT_PASSWORD')
DB_HOST = os.environ.get('DB_HOST', 'localhost')
DB_PORT = env_int('DB_PORT', 3306)
DB_NAME = require_env('DB_NAME')
DB_USER = require_env('DB_USER')
DB_PASSWORD = require_env('DB_PASSWORD')


def unwrap(result):
    """Снимает batch-обёртку fast_bitrix24 (order0000000000)."""
    if isinstance(result, dict) and 'order0000000000' in result:
        return result['order0000000000']
    return result


async def get_status(item_id):
    r = unwrap(await b.call('crm.item.get', {'entityTypeId': ENTITY_TYPE_ID, 'id': item_id}))
    return r.get('item', {}).get(STATUS_FIELD)


async def main():
    # ===== ШАГ 1: Маппинг в БД — старый join (по id) vs правильный (по bid_id) =====
    logger.info("=" * 60)
    logger.info(f"ШАГ 1: Маппинг bid_id={BID_ID} в БД")
    logger.info("=" * 60)

    tunnel = SSHTunnelForwarder(
        (SSH_HOST, SSH_PORT),
        ssh_username=SSH_USERNAME, ssh_password=SSH_PASSWORD,
        # Только пароль: без перебора ключей из ~/.ssh и ssh-agent (иначе fail2ban банит IP на 7 дней)
        allow_agent=False, host_pkey_directories=[],
        remote_bind_address=('localhost', 3306)
    )
    tunnel.start()
    engine = create_engine(
        f'mysql+pymysql://{DB_USER}:{DB_PASSWORD}@localhost:{tunnel.local_bind_port}/{DB_NAME}'
    )

    try:
        with engine.connect() as conn:
            # Правильный join: по bid_id
            correct = conn.execute(text(
                "SELECT id, bid_id, bitrix_id FROM new_webapi_orders_transportation "
                "WHERE bid_id = :v"), {'v': BID_ID}).fetchall()
            # Старый (баговый) join: по внутреннему PK id
            buggy = conn.execute(text(
                "SELECT id, bid_id, bitrix_id FROM new_webapi_orders_transportation "
                "WHERE id = :v"), {'v': BID_ID}).fetchall()

        logger.info(f"✅ join по bid_id={BID_ID}: {[tuple(r) for r in correct]}")
        logger.info(f"⚠️  join по id={BID_ID} (старый баг): {[tuple(r) for r in buggy]}")

        if buggy and correct and buggy[0].bitrix_id != correct[0].bitrix_id:
            logger.error(f"❌ ПОДТВЕРЖДЕНО: старый join обновлял карточку {buggy[0].bitrix_id}, "
                         f"а нужно было {correct[0].bitrix_id}")
        elif not buggy:
            logger.error(f"❌ ПОДТВЕРЖДЕНО: при старом join bid_id={BID_ID} вообще не матчился "
                         f"(нет строки с PK id={BID_ID}) либо матчился с чужой строкой")

        if not correct:
            logger.error("bid_id не найден в БД — дальше проверять нечего")
            return
        bitrix_id = int(correct[0].bitrix_id)

    finally:
        tunnel.stop()

    # ===== ШАГ 2: Текущий статус карточки =====
    logger.info("=" * 60)
    logger.info(f"ШАГ 2: Карточка bitrix_id={bitrix_id}")
    logger.info("=" * 60)
    before = await get_status(bitrix_id)
    logger.info(f"{STATUS_FIELD} ДО: {before!r}")

    # ===== ШАГ 3: Обновление строкой (живой тест 2026-06-10 показал — работает) =====
    if str(before) != TARGET_VALUE:
        logger.info(f"ШАГ 3: Обновляем {STATUS_FIELD} = '{TARGET_VALUE}' (строка)")
        upd = unwrap(await b.call('crm.item.update', {
            'entityTypeId': ENTITY_TYPE_ID,
            'id': bitrix_id,
            'fields': {STATUS_FIELD: TARGET_VALUE}
        }))
        logger.info("Ответ Битрикса (фрагмент): "
                    + json.dumps({k: upd.get('item', {}).get(k) for k in ('id', STATUS_FIELD)},
                                 ensure_ascii=False))
        await asyncio.sleep(5)  # пауза на случай роботов
        after = await get_status(bitrix_id)
        logger.info(f"{STATUS_FIELD} ПОСЛЕ: {after!r}")
        logger.info("✅ Записалось и держится" if str(after) == TARGET_VALUE
                    else "❌ Не записалось / откатилось — копать роботов на SP 137")
    else:
        logger.info(f"Статус уже {TARGET_VALUE} — обновление не требуется")


if __name__ == "__main__":
    asyncio.run(main())
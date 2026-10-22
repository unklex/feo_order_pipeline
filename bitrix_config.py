# bitrix_config_utils.py
from fast_bitrix24 import Bitrix

# Your Bitrix webhook
from env_config import require_env  # заодно подгружает .env
WEBHOOK_URL = require_env('BITRIX_WEBHOOK_URL')

# def get_bitrix_connection():
#     return Bitrix(WEBHOOK_URL)
#
# b = get_bitrix_connection()
# bx = get_bitrix_connection()

from fast_bitrix24 import BitrixAsync

def get_bitrix_connection():
    return BitrixAsync(
        WEBHOOK_URL,
        request_pool_size=100,    # Увеличиваем до 500
        requests_per_second=10.0,    # Увеличиваем до 50 запросов в секунду
        verbose=False
    )

# def get_bitrix_connection():
#     return Bitrix(WEBHOOK_URL, verbose=False)

b = get_bitrix_connection()
bx = get_bitrix_connection()
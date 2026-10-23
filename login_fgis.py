"""
Вход в ФГИС ОПВК для запуска без вопросов в консоли (pipeline.py, app.py).

Открывает Chrome с профилем FGIS_Profile (см. auth.py), ждёт, пока вы войдёте на gisopvk.ru,
сам проверяет доступ к API и сохраняет cookies в gisopvk_webapi_cookies.json. Нажимать Enter
не нужно — окно браузера закроется после успешного входа.

    python login_fgis.py            # открыть браузер и ждать вход (до 10 минут)
    python login_fgis.py --check    # только проверить сохранённый вход, без браузера

Код выхода: 0 — вход работает, 1 — нет.
"""

import argparse
import sys
import time

from gisopvkwebapiparser import GisopvkWebApiParser

# Проверяем тем же API, с которого начинается выгрузка заказов
ORDERS_URL = "https://gisopvk.ru/webapi/v1/order_transportation/"
POLL_SECONDS = 5


def check_saved_login() -> bool:
    parser = GisopvkWebApiParser(base_url=ORDERS_URL)
    try:
        return parser._load_cookies_from_file() and parser.test_auth()
    finally:
        parser.close()


def login(timeout_minutes: int) -> bool:
    import auth

    parser = GisopvkWebApiParser(base_url=ORDERS_URL)
    driver = auth.launch_browser()
    try:
        driver.get(auth.FGIS_URL)
        print("Войдите на gisopvk.ru в открывшемся окне Chrome — после входа окно закроется само.", flush=True)
        deadline = time.time() + timeout_minutes * 60
        while time.time() < deadline:
            time.sleep(POLL_SECONDS)
            try:
                cookies = driver.get_cookies()
            except Exception:
                print("Окно браузера закрыто до входа.", flush=True)
                return False
            if not cookies:
                continue
            parser.session.cookies.clear()
            for c in cookies:
                parser.session.cookies.set(c['name'], c['value'], domain=c.get('domain', ''), path=c.get('path', '/'))
            if parser.test_auth():
                parser._save_cookies_to_file()
                print("Вход выполнен, cookies сохранены.", flush=True)
                return True
        print(f"Вход не выполнен за {timeout_minutes} мин.", flush=True)
        return False
    finally:
        try:
            driver.quit()
        except Exception:
            pass
        parser.close()


def main() -> int:
    ap = argparse.ArgumentParser(description="Вход в ФГИС ОПВК и сохранение cookies.")
    ap.add_argument("--check", action="store_true", help="Только проверить сохранённый вход.")
    ap.add_argument("--timeout", type=int, default=10, help="Сколько минут ждать вход (по умолчанию 10).")
    args = ap.parse_args()

    if args.check:
        ok = check_saved_login()
        print("Сохранённый вход работает." if ok else "Сохранённый вход недействителен.", flush=True)
        return 0 if ok else 1
    if check_saved_login():
        print("Сохранённый вход уже работает — браузер не нужен.", flush=True)
        return 0
    return 0 if login(args.timeout) else 1


if __name__ == "__main__":
    sys.exit(main())

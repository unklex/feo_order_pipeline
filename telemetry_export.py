"""
Выгрузка телеметрии из ФГИС ОПВК с автоматической авторизацией.

Обёртка над Go-программой opvk-export: берёт cookies так же, как сбор заказов
(сохранённый gisopvk_webapi_cookies.json, а если он не подходит — вход через браузер),
передаёт их в opvk-export через OPVK_COOKIE и, если сессия истекла посреди выгрузки,
предлагает войти заново и продолжает с места остановки (opvk-export докачивает по done.txt).

Запуск — те же аргументы, что у opvk-export:
    python telemetry_export.py --from 2026-08-01 --to 2026-10-16 --out export --dry-run
    python telemetry_export.py --from 2026-08-01 --to 2026-10-16 --out export
Свои параметры обёртки: --exe <путь к opvk-export.exe>, --relogins <число повторных входов>.
"""

import argparse
import logging
import os
import subprocess
import sys

from gisopvkwebapiparser import GisopvkWebApiParser
import auth  # после парсера: logging.basicConfig из gisopvkwebapiparser должен сработать первым

logger = logging.getLogger("telemetry_export")

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
EXE_CANDIDATES = [
    os.path.join(SCRIPT_DIR, "opvk-export", "opvk-export.exe"),
    os.path.join(SCRIPT_DIR, "opvk-export (1)", "opvk-export", "opvk-export.exe"),
]
# Проверяем cookies на том же списке ТС, который первым запрашивает opvk-export
VEHICLE_API_URL = "https://gisopvk.ru/webapi/v1/vehicle/"
# Текст ошибки авторизации из opvk-export (errAuth в main.go)
AUTH_ERROR_MARKER = "сессия не принята"


def find_exe(explicit: str = None) -> str:
    candidates = [explicit] if explicit else EXE_CANDIDATES
    for path in candidates:
        if path and os.path.isfile(path):
            return os.path.abspath(path)
    raise FileNotFoundError(
        "Не найден opvk-export.exe (искали: " + "; ".join(c for c in candidates if c) + "). "
        "Соберите его: go build -o opvk-export.exe .  или укажите путь через --exe"
    )


def _browser_alive(parser: GisopvkWebApiParser) -> bool:
    if not parser.driver:
        return False
    try:
        parser.driver.current_url
        return True
    except Exception:
        return False


def _relogin_in_open_browser(parser: GisopvkWebApiParser) -> bool:
    """Повторный вход в уже открытом окне браузера: без закрытия и нового запуска Chrome."""
    driver = parser.driver
    try:
        driver.get(auth.FGIS_URL)
    except Exception as e:
        logger.warning(f"Не удалось открыть портал в браузере: {e}")
    print("\n" + "=" * 50)
    print("Сессия ФГИС ОПВК истекла. Войдите заново в открытом окне браузера.")
    print("=" * 50)
    input("Нажмите Enter после входа...")
    parser.session.cookies.clear()
    for c in driver.get_cookies():
        parser.session.cookies.set(c["name"], c["value"], domain=c.get("domain", ""), path=c.get("path", "/"))
    parser._save_cookies_to_file()
    return parser.test_auth()


def login(parser: GisopvkWebApiParser, force_browser: bool = False) -> bool:
    """Сохранённые cookies, если они ещё действуют; иначе вход через браузер (как в сборе заказов)."""
    if not force_browser and parser._load_cookies_from_file() and parser.test_auth():
        logger.info("Сохранённые cookies действуют, браузер не нужен")
        return True

    if _browser_alive(parser):
        return _relogin_in_open_browser(parser)

    parser.driver = None
    parser.session.cookies.clear()
    logger.info("Нужен вход в ФГИС ОПВК через браузер")
    if not parser.authorize():
        return False
    return parser.test_auth()


def cookie_header(parser: GisopvkWebApiParser) -> str:
    return "; ".join(f"{c.name}={c.value}" for c in parser.session.cookies)


def user_agent(parser: GisopvkWebApiParser) -> str:
    """User-Agent того браузера, в котором выполнен вход (портал блокирует небраузерные запросы)."""
    if _browser_alive(parser):
        try:
            return parser.driver.execute_script("return navigator.userAgent")
        except Exception:
            pass
    return parser.session.headers.get("User-Agent", "")


def run_exporter(exe: str, args: list, cookie: str, ua: str) -> tuple:
    """Запускает opvk-export, транслирует его вывод. Возвращает (код выхода, была ли ошибка авторизации)."""
    env = dict(os.environ, OPVK_COOKIE=cookie)
    if ua:
        env["OPVK_USER_AGENT"] = ua
    env.pop("OPVK_AUTH", None)
    auth_failed = False
    proc = subprocess.Popen(
        [exe] + args, env=env, cwd=os.getcwd(),
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
    )
    for raw in proc.stdout:
        line = raw.decode("utf-8", errors="replace")
        sys.stdout.write(line)
        sys.stdout.flush()
        if AUTH_ERROR_MARKER in line:
            auth_failed = True
    return proc.wait(), auth_failed


def main(argv=None) -> int:
    sys.stdout.reconfigure(errors="replace")
    ap = argparse.ArgumentParser(
        description="opvk-export с автоматической авторизацией (остальные аргументы передаются в opvk-export)",
        allow_abbrev=False,
    )
    ap.add_argument("--exe", help="путь к opvk-export.exe")
    ap.add_argument("--relogins", type=int, default=3,
                    help="сколько раз входить заново, если сессия истекла во время выгрузки (по умолчанию 3)")
    own, exporter_args = ap.parse_known_args(argv)

    exe = find_exe(own.exe)
    logger.info(f"opvk-export: {exe}")

    parser = GisopvkWebApiParser(base_url=VEHICLE_API_URL)
    try:
        if not login(parser):
            logger.error("Авторизация не выполнена — выгрузка не запущена")
            return 1

        for attempt in range(own.relogins + 1):
            code, auth_failed = run_exporter(exe, exporter_args, cookie_header(parser), user_agent(parser))
            if not auth_failed:
                return code
            if attempt == own.relogins:
                logger.error(f"Сессия снова истекла, повторные входы исчерпаны ({own.relogins}). "
                             f"Запустите ту же команду ещё раз — выгрузка продолжится.")
                return code
            logger.warning(f"Сессия истекла во время выгрузки — повторный вход "
                           f"({attempt + 1}/{own.relogins}), затем продолжение с места остановки")
            if not login(parser, force_browser=True):
                logger.error("Повторная авторизация не удалась. Уже выгруженное сохранено.")
                return code
        return 1
    finally:
        parser.close()


if __name__ == "__main__":
    sys.exit(main())

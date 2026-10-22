"""
Упрощенный модуль для авторизации в ФГИС ОПВК.
Либо использует сохраненные cookies, либо запрашивает ручную авторизацию через браузер.
"""

import os
import sys
import getpass
import platform
import logging
import json
from time import sleep
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt='%H:%M:%S',
    handlers=[
        logging.FileHandler("auth.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# URL системы ФГИС ОПВК
FGIS_URL = "https://gisopvk.ru/"

def get_user_profile_dir():
    """
    Определяет путь к директории профиля пользователя Chrome в зависимости от ОС.
    """
    username = getpass.getuser()
    system = platform.system()

    if system == "Windows":
        base_dir = f"C:\\Users\\{username}\\AppData\\Local\\Google\\Chrome\\User Data"
    elif system == "Darwin":  # macOS
        base_dir = f"/Users/{username}/Library/Application Support/Google/Chrome"
    elif system == "Linux":
        base_dir = f"/home/{username}/.config/google-chrome"
    else:
        logger.warning(f"Неизвестная ОС: {system}. Используется профиль по умолчанию.")
        base_dir = f"/tmp/chrome_profile_{username}"

    # Создаем отдельный профиль для работы с ФГИС ОПВК
    profile_dir = os.path.join(base_dir, "FGIS_Profile")

    # Проверка и создание директории профиля
    if not os.path.exists(profile_dir):
        logger.info(f"Создание директории профиля: {profile_dir}")
        os.makedirs(profile_dir, exist_ok=True)

    return profile_dir

def setup_chrome_options():
    """
    Настраивает опции Chrome для работы с ФГИС ОПВК.
    """
    options = Options()

    # Основные настройки
    options.add_argument("start-maximized")
    options.add_argument("--disable-popup-blocking")

    # Настройка для отдельного профиля
    profile_dir = get_user_profile_dir()
    options.add_argument(f"--user-data-dir={profile_dir}")

    # Настройки для стабильности
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-gpu")
    options.add_argument("--ignore-certificate-errors")

    return options

def launch_browser():
    """
    Запускает браузер Chrome с настроенными опциями.
    """
    max_attempts = 3

    for attempt in range(1, max_attempts + 1):
        try:
            logger.info(f"Попытка запуска браузера {attempt}/{max_attempts}")
            options = setup_chrome_options()
            driver = webdriver.Chrome(options=options)
            logger.info("Браузер успешно запущен")
            return driver
        except Exception as e:
            logger.error(f"Ошибка при запуске браузера (попытка {attempt}): {str(e)}")

            # Закрываем драйвер при ошибке
            try:
                if 'driver' in locals() and driver is not None:
                    driver.quit()
            except:
                pass

            if attempt < max_attempts:
                logger.info(f"Повторная попытка через 3 секунды...")
                sleep(3)
            else:
                raise RuntimeError(f"Не удалось запустить браузер после {max_attempts} попыток")

def manual_auth():
    """
    Запускает браузер и ожидает ручной авторизации пользователя.
    """
    try:
        driver = launch_browser()

        # Навигация на сайт ФГИС ОПВК (без проверок загрузки)
        logger.info(f"Открытие сайта ФГИС ОПВК: {FGIS_URL}")
        try:
            driver.get(FGIS_URL)
        except Exception as e:
            logger.warning(f"Ошибка при загрузке страницы: {str(e)}")
            print(f"\nВозникла ошибка при загрузке страницы: {str(e)}")
            print("Пожалуйста, вручную откройте сайт в браузере.")

        # Инструкция для пользователя
        print("\n========== ИНСТРУКЦИЯ ПО АВТОРИЗАЦИИ ==========")
        print("1. Выполните вход в систему ФГИС ОПВК через открывшийся браузер")
        print("2. Используйте ваши учетные данные или ЭЦП для входа")
        print("3. Дождитесь полной загрузки личного кабинета после входа")
        print("4. После успешного входа вернитесь в консоль")
        print("=================================================\n")

        # Ожидание подтверждения пользователя
        input("Нажмите Enter после успешной авторизации: ")

        logger.info("Получено подтверждение авторизации от пользователя")
        return driver
    except Exception as e:
        logger.error(f"Ошибка при ручной авторизации: {str(e)}")

        # Закрываем драйвер при ошибке
        try:
            if 'driver' in locals() and driver is not None:
                driver.quit()
        except:
            pass

        raise RuntimeError(f"Ошибка при ручной авторизации: {str(e)}")

def auth():
    """
    Упрощенная функция авторизации.
    Сначала проверяет наличие сохраненных cookies, если их нет - запускает ручную авторизацию.
    """
    cookies_file = "gisopvk_webapi_cookies.json"

    # Проверяем наличие сохраненных cookies
    if os.path.exists(cookies_file):
        logger.info(f"Найден файл с cookies: {cookies_file}")
        print(f"\nНайден файл с сохраненными cookies. Попробуем использовать его.")

        # Пользователь может выбрать, использовать сохраненные cookies или выполнить новую авторизацию
        use_saved = input("Использовать сохраненные данные авторизации? (y/n): ")

        if use_saved.lower() == 'y':
            print("\nИспользуем сохраненные cookies. Браузер не будет запущен.")
            return None  # Возвращаем None, указывая что используются сохраненные cookies

    # Если cookies нет или пользователь выбрал не использовать их, запускаем ручную авторизацию
    print("\nЗапуск браузера для ручной авторизации...")
    return manual_auth()

if __name__ == "__main__":
    try:
        driver = auth()

        if driver:
            print("\nАвторизация успешно выполнена. Браузер готов к работе.")
            print("Нажмите Ctrl+C для завершения работы и закрытия браузера.")

            try:
                while True:
                    sleep(1)
            except KeyboardInterrupt:
                print("\nЗавершение работы...")
            finally:
                if driver:
                    driver.quit()
                    print("Браузер закрыт.")
        else:
            print("\nИспользуются сохраненные данные авторизации.")

    except Exception as e:
        logger.error(f"Критическая ошибка: {str(e)}")
        sys.exit(1)
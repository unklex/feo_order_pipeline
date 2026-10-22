import json
import requests
import urllib3
from typing import Dict, Any, Optional, List
import logging
from datetime import datetime
import os
import re
import time
from collections import defaultdict
import sys
from requests.exceptions import RequestException

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Переменная для хранения драйвера Selenium (будет инициализирована при авторизации)
driver = None

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("EndpointTester")


class GisopvkEndpointTester:
    """Класс для тестирования доступности эндпоинтов API gisopvk.ru"""

    def __init__(self, cookies_file: str = "gisopvk_webapi_cookies.json"):
        """
        Инициализация тестера
        """
        self.session = requests.Session()
        self.session.verify = False

        # Установка базовых заголовков JSON:API
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/vnd.api+json, application/json',
            'Content-Type': 'application/vnd.api+json',
            'Accept-Encoding': 'identity',
            'Referer': 'https://gisopvk.ru/',
            'Origin': 'https://gisopvk.ru',
        })

        # Загрузка cookies
        self._load_cookies(cookies_file)

        # Статус авторизации
        self.is_authorized = False
        self.driver = None

        # Список приоритетных эндпоинтов для проверки
        self.priority_endpoints: List[tuple[str, str]] = [
            ("bid", "https://gisopvk.ru/stencilapi/v1/order_waste_source/"),
            ("order_transportation", "https://gisopvk.ru/stencilapi/v1/order_transportation/"),
            ("order_recycling", "https://gisopvk.ru/stencilapi/v1/order_recycling/"),
            ("contract_waste_generator", "https://gisopvk.ru/stencilapi/v1/contract_waste_generator/"),
            ("contract_transportation", "https://gisopvk.ru/stencilapi/v1/contract_transportation/"),
            ("contract_recycling", "https://gisopvk.ru/stencilapi/v1/contract_recycling/"),
            ("wasteplace", "https://gisopvk.ru/stencilapi/v1/wasteplace/"),
            ("facility", "https://gisopvk.ru/stencilapi/v1/facility/"),
            ("vehicle", "https://gisopvk.ru/stencilapi/v1/vehicle/"),
            ("organization_info", "https://gisopvk.ru/stencilapi/v1/organization_info/"),
        ]

    def _fix_request_headers(self):
        """
        Исправляет заголовки запросов для решения проблемы 406 Not Acceptable (ошибка JSON:API)
        """
        self.session.headers.update({
            'Accept': 'application/vnd.api+json, application/json',
            'Content-Type': 'application/vnd.api+json',
            'Accept-Encoding': 'identity',
        })
        logger.info("Обновлены заголовки запросов для соответствия JSON:API")

    def authorize(self):
        """
        Выполняет авторизацию на сайте через Selenium
        """
        try:
            # Пробуем загрузить cookies из файла и проверяем их работоспособность
            if self.test_auth():
                logger.info("Используем сохраненные cookies авторизации")
                print("\n✅ Используем сохраненные cookies авторизации")
                self.is_authorized = True
                return True

            # Если сохраненные cookies не работают, запускаем процесс авторизации
            logger.info("Запуск процесса авторизации через Selenium...")
            print("\n" + "=" * 70)
            print("АВТОРИЗАЦИЯ ЧЕРЕЗ БРАУЗЕР")
            print("=" * 70)
            print("\nСохраненные cookies не найдены или не работают.")
            print("Запускаем браузер для авторизации...")

            # Импортируем модуль авторизации
            try:
                import auth

                # Запускаем авторизацию
                self.driver = auth.auth()

                if self.driver is None:
                    logger.info("Авторизация отменена пользователем")
                    print("\n❌ Авторизация отменена")
                    return False

                # Получаем cookies из браузера
                logger.info("Получение cookies из браузера...")
                selenium_cookies = self.driver.get_cookies()

                if not selenium_cookies:
                    logger.warning("Не удалось получить cookies из браузера")
                    print("\n❌ Не удалось получить cookies из браузера")
                    return False

                # Применяем cookies к сессии requests
                for cookie in selenium_cookies:
                    self.session.cookies.set(
                        name=cookie['name'],
                        value=cookie['value'],
                        domain=cookie.get('domain', ''),
                        path=cookie.get('path', '/')
                    )

                # Сохраняем cookies в файл
                self._save_cookies_to_file()
                logger.info(f"Успешно применены cookies авторизации ({len(selenium_cookies)} cookies)")
                print(f"\n✅ Получено {len(selenium_cookies)} cookies")

                # Проверяем работу авторизации ПЕРЕД закрытием браузера
                if self.test_auth():
                    logger.info("Проверка авторизации успешна")
                    print("✅ Авторизация успешно выполнена и проверена")
                    self.is_authorized = True

                    # Теперь можно закрыть браузер
                    try:
                        self.driver.quit()
                        self.driver = None
                    except:
                        pass

                    return True
                else:
                    logger.warning("Проверка авторизации не прошла")
                    print("\n⚠️  Проверка авторизации не прошла")
                    print("\nВозможные причины:")
                    print("1. Вы не завершили вход на сайт")
                    print("2. Страница не полностью загрузилась")
                    print("3. Cookies не были правильно получены (или нужен другой заголовок/токен)")
                    print("\nЧто делать:")
                    print("1. Проверьте что вы ВОШЛИ на сайт в браузере")
                    print("2. Убедитесь что видите главную страницу после входа")
                    print("3. Попробуйте запустить скрипт снова")
                    print("\n💡 Браузер оставлен открытым для проверки")

                    # Показываем текущий URL для диагностики
                    try:
                        current_url = self.driver.current_url
                        print(f"Текущий URL в браузере: {current_url}")
                    except:
                        pass

                    return False

            except ImportError:
                logger.error("Модуль auth.py не найден")
                print("\n" + "=" * 70)
                print("❌ ОШИБКА: Модуль auth.py не найден!")
                print("=" * 70)
                print("\nДля авторизации нужен файл auth.py из основного парсера.")
                print("\nВарианты решения:")
                print("1. Скопируйте auth.py в текущую директорию")
                print("2. Или создайте cookies вручную (см. COOKIES_GUIDE.md)")
                print("=" * 70)
                return False

            except Exception as e:
                logger.error(f"Ошибка при работе с модулем auth: {str(e)}")
                print(f"\n❌ Ошибка при авторизации: {str(e)}")
                return False

        except Exception as e:
            logger.error(f"Ошибка при авторизации: {str(e)}")
            print(f"\n❌ Произошла ошибка при авторизации: {str(e)}")
            return False

    def close_driver(self):
        """Закрывает браузер Selenium если он открыт"""
        if self.driver:
            try:
                self.driver.quit()
                self.driver = None
                logger.info("Браузер Selenium закрыт")
            except Exception as e:
                logger.error(f"Ошибка при закрытии браузера: {str(e)}")

    def _save_cookies_to_file(self, filename="gisopvk_webapi_cookies.json"):
        """
        Сохраняет cookies сессии в файл
        """
        try:
            cookies_dict = {c.name: c.value for c in self.session.cookies}
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(cookies_dict, f, ensure_ascii=False, indent=2)
            logger.info(f"Cookies сохранены в файл {filename}")
            print(f"💾 Cookies сохранены в файл: {filename}")
        except Exception as e:
            logger.error(f"Ошибка при сохранении cookies: {str(e)}")

    def test_auth(self) -> bool:
        """
        Проверяет, работает ли авторизация, делая тестовый запрос.
        Улучшено: Добавлена обработка 406 и строгая проверка JSON:API.
        """
        max_attempts = 2  # Попытка 1 + повтор после исправления заголовков
        test_url = f"{self.priority_endpoints[0][1]}?page[limit]=1&page[offset]=0"

        for attempt in range(max_attempts):
            try:
                response = self.session.get(test_url, timeout=10)

                # --- 1. Проверка 406 Not Acceptable (ошибка заголовков) ---
                if response.status_code == 406:
                    if attempt < max_attempts - 1:
                        logger.warning(f"Получен статус 406 Not Acceptable. Корректируем заголовки и повторяем...")
                        self._fix_request_headers()
                        continue  # Повторяем попытку
                    else:
                        logger.error("406 Not Acceptable после исправления заголовков.")
                        return False

                # --- 2. Проверка 200 OK (успешный доступ) ---
                if response.status_code == 200:
                    try:
                        data = response.json()
                        # Строгая проверка JSON:API: наличие 'data'
                        if 'data' in data and isinstance(data['data'], (list, dict)):
                            logger.info("Авторизация работает успешно")
                            return True
                        else:
                            logger.warning(f"Ответ 200 OK, но не соответствует JSON:API.")
                            return False
                    except json.JSONDecodeError:
                        logger.warning("Ответ 200 OK, но неверный формат JSON (вероятно, HTML страница входа).")
                        return False

                # --- 3. Проверка ошибок авторизации ---
                elif response.status_code in (401, 403):
                    logger.warning(f"Неуспешный статус ответа: {response.status_code} (Unauthorized/Forbidden).")
                    return False

                # --- 4. Другие ошибки ---
                else:
                    logger.warning(f"Неуспешный статус ответа: {response.status_code}")
                    return False

            except Exception as e:
                logger.debug(f"Ошибка при проверке авторизации: {str(e)}")
                # В случае ошибки соединения/таймаута, повторяем, если есть попытки
                if attempt < max_attempts - 1:
                    continue
                return False

        return False  # Все попытки исчерпаны

    def ensure_authorized(self):
        """
        Проверяет авторизацию и выполняет при необходимости
        """
        if self.is_authorized or self.test_auth():
            self.is_authorized = True
            return True

        print("\n" + "=" * 70)
        print("⚠️  Требуется авторизация")
        print("=" * 70)

        return self.authorize()

    def _load_cookies(self, cookies_file: str):
        """Загрузка cookies из файла"""
        try:
            if not os.path.exists(cookies_file):
                logger.info(f"Файл с cookies не найден: {cookies_file}")
                return

            with open(cookies_file, 'r', encoding='utf-8') as f:
                cookies_dict = json.load(f)

            if not cookies_dict:
                logger.warning(f"Файл с cookies пуст: {cookies_file}")
                return

            for name, value in cookies_dict.items():
                self.session.cookies.set(name, value)

            logger.info(f"Загружено {len(cookies_dict)} cookies из файла {cookies_file}")

        except Exception as e:
            logger.error(f"Ошибка при загрузке cookies: {str(e)}")

    def test_endpoint(self, url: str, limit: int = 1) -> Dict[str, Any]:
        """
        Тестирование одного эндпоинта.
        Улучшено: Добавлена логика обработки 406.
        """
        result = {
            'url': url,
            'status': 'unknown',
            'status_code': None,
            'accessible': False,
            'record_count': 0,
            'total_count': None,
            'sample_record': None,
            'error': None,
            'response_time': None
        }

        test_url = f"{url}?page[limit]={limit}&page[offset]=0"
        max_attempts = 2  # Попытка 1 + повтор после исправления заголовков

        for attempt in range(max_attempts):
            try:
                start_time = datetime.now()
                response = self.session.get(test_url, timeout=10)
                end_time = datetime.now()
                result['response_time'] = (end_time - start_time).total_seconds()
                result['status_code'] = response.status_code

                # --- Проверка 406 Not Acceptable (ошибка заголовков) ---
                if response.status_code == 406:
                    if attempt < max_attempts - 1:
                        logger.warning(f"Endpoint {url} получил 406. Корректируем заголовки и повторяем...")
                        self._fix_request_headers()
                        continue  # Повторяем попытку
                    else:
                        result['status'] = 'not_acceptable'
                        result['error'] = 'Wrong Accept header (406) after retry'
                        return result

                # --- Успешный ответ (200) ---
                if response.status_code == 200:
                    try:
                        data = response.json()

                        if 'data' in data:
                            result['status'] = 'success'
                            result['accessible'] = True

                            records = data['data']
                            if isinstance(records, list):
                                result['record_count'] = len(records)
                                if records:
                                    result['sample_record'] = records[0]
                            else:
                                result['record_count'] = 1
                                result['sample_record'] = records

                            # Пытаемся определить общее количество записей

                            # Проверка links.last (поддерживаем оба формата кодировки)
                            if 'links' in data and 'last' in data['links']:
                                last_link = data['links']['last']
                                # Используем регулярное выражение для поиска offset в обоих форматах кодировки
                                offset_match = re.search(r'(?:page%5Boffset%5D|page\[offset\])=(\d+)', last_link)
                                if offset_match:
                                    max_offset = int(offset_match.group(1))
                                    result['total_count'] = max_offset + limit

                            # Проверка meta.total
                            if 'meta' in data and 'total' in data['meta']:
                                result['total_count'] = data['meta']['total']

                        else:
                            result['status'] = 'unexpected_format'
                            result['error'] = 'Response does not contain "data" field'

                    except json.JSONDecodeError as e:
                        result['status'] = 'invalid_json'
                        result['error'] = str(e)

                # --- Ошибки, не связанные с заголовками ---
                elif response.status_code == 401:
                    result['status'] = 'unauthorized'
                    result['error'] = 'Authentication required'

                elif response.status_code == 403:
                    result['status'] = 'forbidden'
                    result['error'] = 'Access denied'

                elif response.status_code == 404:
                    result['status'] = 'not_found'
                    result['error'] = 'Endpoint not found'

                else:
                    result['status'] = 'error'
                    result['error'] = f'HTTP {response.status_code}'

                # Если мы здесь, значит, мы получили финальный ответ (не 406 с возможностью повтора)
                return result

            except requests.exceptions.Timeout:
                result['status'] = 'timeout'
                result['error'] = 'Request timeout'
                return result

            except requests.exceptions.ConnectionError:
                result['status'] = 'connection_error'
                result['error'] = 'Connection failed'
                return result

            except Exception as e:
                result['status'] = 'exception'
                result['error'] = str(e)
                return result

        return result

    def test_all_priority_endpoints(self):
        """Тестирование всех приоритетных эндпоинтов"""
        print("\n" + "=" * 80)
        print("ТЕСТИРОВАНИЕ ДОСТУПНОСТИ ЭНДПОИНТОВ API GISOPVK.RU")
        print("=" * 80)

        results = []

        for name, url in self.priority_endpoints:
            print(f"\n[{len(results) + 1}/{len(self.priority_endpoints)}] Тестирование: {name}")
            print(f"URL: {url}")

            result = self.test_endpoint(url)
            result['name'] = name
            results.append(result)

            # Вывод результата
            if result['accessible']:
                print(f"✅ ДОСТУПЕН | Записей: {result['record_count']} | "
                      f"Всего: {result['total_count'] or 'неизвестно'} | "
                      f"Время: {result['response_time']:.2f}с")
            else:
                print(f"❌ НЕДОСТУПЕН | Статус: {result['status']} | "
                      f"Ошибка: {result['error']}")

        # Итоговая статистика
        print("\n" + "=" * 80)
        print("ИТОГОВАЯ СТАТИСТИКА")
        print("=" * 80)

        accessible_count = sum(1 for r in results if r['accessible'])
        print(f"\nДоступно эндпоинтов: {accessible_count}/{len(results)}")

        # Группировка по статусам

        status_groups = defaultdict(list)
        for r in results:
            status_groups[r['status']].append(r['name'])

        print("\nРаспределение по статусам:")
        for status, names in sorted(status_groups.items()):
            print(f"  {status}: {len(names)} - {', '.join(names)}")

        # Сохранение результатов
        self._save_results(results)

        return results

    def get_bid_sample(self, output_file: str = "bid_sample.json"):
        """
        Получение примера заказа (bid) для анализа структуры
        """
        print("\n" + "=" * 80)
        print("ПОЛУЧЕНИЕ ПРИМЕРА ЗАКАЗА (BID)")
        print("=" * 80)

        bid_url = "https://gisopvk.ru/stencilapi/v1/order_waste_source/"

        result = self.test_endpoint(bid_url, limit=1)

        if result['accessible'] and result['sample_record']:
            # Сохраняем полный ответ
            full_data = {
                'endpoint': bid_url,
                'timestamp': datetime.now().isoformat(),
                'total_records': result['total_count'],
                'sample_record': result['sample_record']
            }

            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(full_data, f, ensure_ascii=False, indent=2)

            print(f"\n✅ Пример заказа успешно сохранен в файл: {output_file}")
            print(f"Всего доступно заказов: {result['total_count'] or 'неизвестно'}")

            # Выводим краткую информацию о структуре
            self._print_record_structure(result['sample_record'])

            return result['sample_record']
        else:
            print(f"\n❌ Не удалось получить пример заказа")
            print(f"Статус: {result['status']}")
            print(f"Ошибка: {result['error']}")
            return None

    def _print_record_structure(self, record: Dict, indent: int = 0):
        """Вывод структуры записи"""
        print("\nСтруктура записи:")
        print("-" * 60)

        if not isinstance(record, dict):
            print("Запись не является словарем")
            return

        def print_keys(obj, level=0):
            prefix = "  " * level
            if isinstance(obj, dict):
                for key, value in obj.items():
                    if isinstance(value, dict):
                        print(f"{prefix}{key}: {{...}} (dict with {len(value)} keys)")
                        if level < 2:  # Ограничиваем глубину вложенности
                            print_keys(value, level + 1)
                    elif isinstance(value, list):
                        print(f"{prefix}{key}: [...] (list with {len(value)} items)")
                        if value and level < 2:
                            print(f"{prefix}  └─ Пример элемента:")
                            print_keys(value[0], level + 2)
                    else:
                        value_str = str(value)[:50]
                        print(f"{prefix}{key}: {type(value).__name__} = {value_str}")

        print_keys(record)

    def _save_results(self, results: list):
        """Сохранение результатов тестирования"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"endpoint_test_results_{timestamp}.json"

        # Готовим данные для сохранения (убираем sample_record для экономии места)
        results_to_save = []
        for r in results:
            r_copy = r.copy()
            if 'sample_record' in r_copy:
                # Сохраняем только структуру, не сами данные
                if r_copy['sample_record']:
                    r_copy['record_structure'] = list(r_copy['sample_record'].keys()) if isinstance(
                        r_copy['sample_record'], dict) else 'not_dict'
                del r_copy['sample_record']
            results_to_save.append(r_copy)

        output_data = {
            'timestamp': datetime.now().isoformat(),
            'total_endpoints': len(results),
            'accessible_endpoints': sum(1 for r in results if r['accessible']),
            'results': results_to_save
        }

        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, ensure_ascii=False, indent=2)

        print(f"\n📄 Результаты тестирования сохранены в файл: {filename}")

    def load_and_test_all_endpoints(self, endpoints_file: str = "gisopvk_endpoints.json"):
        """
        Загрузка и тестирование всех эндпоинтов из файла
        """
        try:
            with open(endpoints_file, 'r', encoding='utf-8') as f:
                data = json.load(f)

            endpoints = data.get('data', {})

            print(f"\n📋 Загружено {len(endpoints)} эндпоинтов из файла")

            # Спрашиваем, тестировать ли все
            response = input("\nТестировать ВСЕ эндпоинты? Это может занять время (y/n): ")

            if response.lower() != 'y':
                print("Тестирование отменено")
                return

            print("\n" + "=" * 80)
            print(f"ТЕСТИРОВАНИЕ ВСЕХ {len(endpoints)} ЭНДПОИНТОВ")
            print("=" * 80)

            results = []
            accessible = []

            for i, (name, url) in enumerate(endpoints.items(), 1):
                print(f"\n[{i}/{len(endpoints)}] {name}")

                result = self.test_endpoint(url)
                result['name'] = name
                results.append(result)

                if result['accessible']:
                    accessible.append(name)
                    print(f"  ✅ Доступен ({result['record_count']} записей)")
                else:
                    print(f"  ❌ {result['status']}")

                # Небольшая задержка между запросами
                time.sleep(0.3)

            # Итоги
            print("\n" + "=" * 80)
            print("РЕЗУЛЬТАТЫ ПОЛНОГО ТЕСТИРОВАНИЯ")
            print("=" * 80)
            print(f"\nДоступно: {len(accessible)}/{len(endpoints)}")
            print(f"\nДоступные эндпоинты:\n")
            for name in accessible:
                print(f"  • {name}")

            # Сохранение
            self._save_results(results)

            return results

        except FileNotFoundError:
            print(f"❌ Файл {endpoints_file} не найден")
        except Exception as e:
            print(f"❌ Ошибка: {str(e)}")


def main():
    """Основная функция"""
    tester = None  # Инициализируем для блока finally
    try:
        print("=" * 80)
        print("GISOPVK API ENDPOINT TESTER")
        print("=" * 80)

        # Создаем тестер
        tester = GisopvkEndpointTester()

        # Проверяем и выполняем авторизацию при необходимости
        print("\n" + "=" * 80)
        print("ПРОВЕРКА АВТОРИЗАЦИИ")
        print("=" * 80)

        if not tester.ensure_authorized():
            print("\n" + "=" * 80)
            print("❌ НЕ УДАЛОСЬ АВТОРИЗОВАТЬСЯ")
            print("=" * 80)
            print("\nПопробуйте следующее:")
            print("1. Закройте браузер если он открыт")
            print("2. Запустите скрипт снова")
            print("3. Убедитесь что вы ПОЛНОСТЬЮ вошли на сайт")
            print("4. Дождитесь загрузки главной страницы")
            print("5. Только после этого нажимайте Enter")
            print("\nИли закройте браузер вручную и попробуйте другой метод получения cookies")
            print("(см. COOKIES_GUIDE.md)")

            # Даем возможность закрыть браузер
            input("\nНажмите Enter для выхода...")
            return

        print("\n✅ Авторизация успешна! Можно работать с API")

        while True:
            print("\n" + "=" * 80)
            print("МЕНЮ")
            print("=" * 80)
            print("1. Получить пример заказа (bid)")
            print("2. Протестировать приоритетные эндпоинты")
            print("3. Протестировать ВСЕ эндпоинты из файла")
            print("4. Выход")

            choice = input("\nВыберите действие (1-4): ").strip()

            if choice == '1':
                if tester.ensure_authorized():
                    tester.get_bid_sample()
                else:
                    print("\n❌ Требуется авторизация для выполнения действия")

            elif choice == '2':
                if tester.ensure_authorized():
                    tester.test_all_priority_endpoints()
                else:
                    print("\n❌ Требуется авторизация для выполнения действия")

            elif choice == '3':
                if tester.ensure_authorized():
                    # Проверяем наличие файла с эндпоинтами
                    if os.path.exists("gisopvk_endpoints.json"):
                        tester.load_and_test_all_endpoints()
                    else:
                        print("\n❌ Файл gisopvk_endpoints.json не найден")
                        print("Поместите JSON-файл с эндпоинтами в текущую директорию")
                else:
                    print("\n❌ Требуется авторизация для выполнения действия")

            elif choice == '4':
                print("\nВыход из программы")
                break

            else:
                print("\n❌ Неверный выбор, попробуйте снова")

    except KeyboardInterrupt:
        print("\n\n⚠️  Прервано пользователем")
    except Exception as e:
        logger.error(f"Критическая ошибка в main: {str(e)}")
        print(f"\n❌ Критическая ошибка: {str(e)}")
    finally:
        # Гарантированное закрытие драйвера
        if tester:
            tester.close_driver()


if __name__ == "__main__":
    main()
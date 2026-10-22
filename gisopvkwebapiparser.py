import time
import json
import logging
import os
from typing import List, Dict, Any, Optional, Generator, Tuple
from urllib.parse import urlparse, parse_qs
from requests.exceptions import RequestException
import urllib3
import gzip
import datetime
import re
import statistics
from collections import defaultdict
import requests

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("parser_webapi.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("GisopvkWebApiParser")

class GisopvkWebApiParser:
    """Парсер для API https://gisopvk.ru/webapi/v1/bid/ с пагинацией и авторизацией"""

    def __init__(self,
                 base_url: str = "https://gisopvk.ru/webapi/v1/bid/",
                 default_limit: int = 20,
                 fallback_limit: int = 10,
                 request_delay: float = 0.5,
                 max_retries: int = 3,
                 retry_delay: float = 2.0):
        """
        Инициализация парсера для нового webapi

        Args:
            base_url: Базовый URL API
            default_limit: Стандартное количество записей на странице
            fallback_limit: Количество записей при возникновении ошибки
            request_delay: Задержка между запросами в секундах
            max_retries: Максимальное количество повторных попыток при ошибке
            retry_delay: Задержка перед повторной попыткой в секундах
        """
        self.base_url = base_url
        self.default_limit = default_limit
        self.fallback_limit = fallback_limit
        self.request_delay = request_delay
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.session = requests.Session()
        self.session.verify = False  # Отключаем проверку SSL-сертификатов
        self.driver = None

        # Статистика запросов
        self.stats = {
            'total_requests': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'retry_count': 0,
            'total_records': 0,
            'start_time': None,
            'request_times': [],
            'data_sizes': [],
            'status_codes': defaultdict(int),
            'errors': defaultdict(int)
        }

        # Установка заголовков для имитации браузера - корректные заголовки учитывающие JSON:API
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/vnd.api+json, application/json',  # Добавляем правильный MIME-тип для JSON:API
            'Content-Type': 'application/vnd.api+json',  # Указываем правильный Content-Type для JSON:API
            'Accept-Encoding': 'identity',  # Отключаем сжатие, чтобы исключить проблемы с ним
            'Referer': 'https://gisopvk.ru/',
            'Origin': 'https://gisopvk.ru',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache',
        })

    def authorize(self):
        """
        Выполняет авторизацию на сайте через Selenium и ждет подтверждения пользователя

        Returns:
            bool: True если авторизация успешна, иначе False
        """
        try:
            # Импортируем модуль авторизации
            import auth

            logger.info("Запуск процесса авторизации через Selenium...")

            try:
                self.driver = auth.manual_auth()
            except Exception as e:
                # Если произошла ошибка при запуске браузера, но пользователь говорит, что авторизация работает,
                # предложим пользователю вручную подтвердить авторизацию
                logger.warning(f"Ошибка при запуске браузера: {str(e)}")
                print(f"\nПроизошла ошибка при запуске браузера: {str(e)}")

                manual_confirm = input(
                    "\nВы уже авторизованы в системе или можете пропустить этап авторизации? (y/n): ")
                if manual_confirm.lower() == 'y':
                    print("\nПродолжаем без автоматической авторизации...")
                    # Копируем cookies из браузера
                    self._extract_browser_cookies()
                    return True
                else:
                    raise Exception(f"Не удалось выполнить авторизацию: {str(e)}")

            if not self.driver:
                logger.warning("Не удалось получить драйвер после авторизации")

                # Позволяем пользователю продолжить без драйвера, если он считает, что авторизация уже работает
                manual_confirm = input(
                    "\nВы уже авторизованы или можете продолжить без автоматической авторизации? (y/n): ")
                if manual_confirm.lower() == 'y':
                    print("\nПродолжаем без драйвера...")
                    # Копируем cookies из браузера
                    self._extract_browser_cookies()
                    return True
                else:
                    return False

            # Просим пользователя подтвердить завершение авторизации
            print("\n" + "=" * 50)
            print("Выполните авторизацию в открывшемся окне браузера.")
            print("После успешного входа в систему нажмите Enter в консоли для продолжения.")
            print("=" * 50)

            input("Нажмите Enter после завершения авторизации...")

            logger.info("Получено подтверждение завершения авторизации от пользователя")

            # Получаем cookies из драйвера и применяем к сессии requests
            logger.info("Получение cookies из браузера...")
            selenium_cookies = self.driver.get_cookies()

            if not selenium_cookies:
                logger.warning("Не удалось получить cookies из браузера. Возможно, авторизация не выполнена.")
                print(
                    "\nПредупреждение: Не удалось получить cookies. Проверьте, что вы действительно авторизованы в браузере.")
                retry = input("Попробовать ещё раз? (y/n): ")
                if retry.lower() == 'y':
                    # Даём пользователю ещё одну попытку
                    input("Нажмите Enter после завершения авторизации...")
                    selenium_cookies = self.driver.get_cookies()
                    if not selenium_cookies:
                        logger.warning("Повторная попытка не удалась. Cookies не получены.")
                        # Даем пользователю возможность копировать заголовки из браузера
                        print("\nВы можете получить cookies из инструментов разработчика браузера.")
                        self._extract_browser_cookies()
                        return True

            for cookie in selenium_cookies:
                self.session.cookies.set(
                    name=cookie['name'],
                    value=cookie['value'],
                    domain=cookie.get('domain', ''),
                    path=cookie.get('path', '/')
                )

            # Получаем текущий URL, чтобы получить актуальный CSRF-токен или другие параметры
            if self.driver:
                current_url = self.driver.current_url
                logger.info(f"Текущий URL после авторизации: {current_url}")

            # Сохраняем cookies в файл для последующего использования
            self._save_cookies_to_file()

            logger.info(f"Успешно применены cookies авторизации ({len(selenium_cookies)} cookies)")

            # Сразу проверяем работу авторизации
            if self.test_auth():
                logger.info("Проверка авторизации успешна")
                print("\nАвторизация успешно выполнена и проверена.")
                return True
            else:
                logger.warning("Проверка авторизации не прошла")
                print("\nПредупреждение: Авторизация выполнена, но проверка доступа не прошла.")

                # Корректируем заголовки запросов
                print("Попробуем скорректировать заголовки запроса...")
                self._fix_request_headers()

                # Проверяем снова
                if self.test_auth():
                    logger.info("Проверка авторизации успешна после корректировки заголовков")
                    print("\nАвторизация успешно проверена после корректировки заголовков!")
                    return True
                else:
                    print("Возможно, получены неполные права доступа или cookies недействительны.")
                    # Запрашиваем заголовки из браузера
                    self._extract_browser_cookies()
                    return True

        except Exception as e:
            logger.error(f"Ошибка при авторизации: {str(e)}")
            print(f"\nПроизошла ошибка при авторизации: {str(e)}")

            # Даем пользователю возможность продолжить, если он считает, что авторизация работает
            manual_confirm = input("\nПродолжить без автоматической авторизации? (y/n): ")
            if manual_confirm.lower() == 'y':
                # Копируем cookies из браузера
                self._extract_browser_cookies()
                return True

            return False

    def _extract_browser_cookies(self):
        """
        Извлекает cookies и заголовки из браузера через инструкции пользователю

        Returns:
            bool: True если данные получены, иначе False
        """
        print("\n" + "=" * 50)
        print("ПОЛУЧЕНИЕ ДАННЫХ ИЗ БРАУЗЕРА")
        print("=" * 50)
        print("\nСледуйте инструкции для получения данных из браузера:")
        print("1. Откройте сайт https://gisopvk.ru/ в своем браузере и войдите в систему")
        print("2. Откройте инструменты разработчика (F12 или Ctrl+Shift+I)")
        print("3. Перейдите на вкладку 'Network' (Сеть)")
        print("4. Перезагрузите страницу (F5)")
        print("5. Найдите в списке запрос к сайту (например, главную страницу)")
        print("6. Нажмите правой кнопкой мыши на запрос и выберите 'Copy' -> 'Copy as cURL' (bash)")

        curl_command = input("\nВставьте команду curl (или нажмите Enter для пропуска): ")

        if not curl_command.strip():
            print("\nПропускаем получение данных из браузера...")
            return False

        # Извлекаем cookies и заголовки из команды curl
        cookies = {}

        # Извлекаем cookie
        cookie_matches = re.findall(r'-H [\'"]Cookie:\s+([^\'"]+)[\'"]', curl_command)
        if cookie_matches:
            cookie_str = cookie_matches[0]
            cookie_parts = cookie_str.split(';')
            for part in cookie_parts:
                if '=' in part:
                    name, value = part.strip().split('=', 1)
                    cookies[name] = value
                    self.session.cookies.set(name, value)

        # Извлекаем важные заголовки
        headers_to_extract = ['accept', 'accept-encoding', 'accept-language', 'content-type', 'user-agent']
        for header in headers_to_extract:
            matches = re.findall(fr'-H [\'"]{header}:\s+([^\'"]+)[\'"]', curl_command, re.IGNORECASE)
            if matches:
                self.session.headers[header] = matches[0]

        # Проверяем наличие заголовка Accept
        if 'accept' not in self.session.headers or 'application/vnd.api+json' not in self.session.headers['accept']:
            self.session.headers['accept'] = 'application/vnd.api+json, application/json'

        print(f"\nИзвлечено {len(cookies)} cookies и обновлены заголовки запросов")

        # Сохраняем cookies
        if cookies:
            self._save_cookies_to_file()

        # Проверяем работу запросов
        if self.test_auth():
            print("\nПроверка запросов успешна!")
            return True
        else:
            print("\nПроверка запросов не прошла, но мы продолжим попытки...")
            self._fix_request_headers()
            return True

    def _fix_request_headers(self):
        """
        Исправляет заголовки запросов для решения проблемы 406 Not Acceptable
        """
        # Настраиваем правильные заголовки для JSON:API
        self.session.headers.update({
            'Accept': 'application/vnd.api+json, application/json',
            'Content-Type': 'application/vnd.api+json',
            'Accept-Encoding': 'identity',  # Отключаем сжатие
        })

        logger.info("Обновлены заголовки запросов для исправления ошибки 406")
        print("\nОбновлены заголовки запросов для исправления ошибки 406 Not Acceptable")

    def _manual_cookie_input(self):
        """
        Позволяет пользователю вручную ввести cookie для авторизации
        """
        print("\n=== Ручной ввод cookies ===")
        print("Вы можете ввести cookie вручную (формат: 'name=value' по одной на строку)")
        print("После ввода всех cookie, введите пустую строку для завершения.")

        cookies = {}
        while True:
            cookie_input = input("Введите cookie (name=value) или пустую строку для завершения: ")
            if not cookie_input:
                break

            try:
                name, value = cookie_input.split('=', 1)
                cookies[name.strip()] = value.strip()
                print(f"Cookie '{name}' добавлена")
            except ValueError:
                print("Неверный формат. Используйте формат 'name=value'")

        # Применяем введенные cookie к сессии
        for name, value in cookies.items():
            self.session.cookies.set(name, value)

        print(f"\nДобавлено {len(cookies)} cookies")

        # Сохраняем cookies в файл для последующего использования
        if cookies:
            self._save_cookies_to_file()

    def _save_cookies_to_file(self, filename="gisopvk_webapi_cookies.json"):
        """
        Сохраняет cookies сессии в файл

        Args:
            filename: Имя файла для сохранения cookies
        """
        try:
            cookies_dict = {c.name: c.value for c in self.session.cookies}
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(cookies_dict, f, ensure_ascii=False, indent=2)
            logger.info(f"Cookies сохранены в файл {filename}")
        except Exception as e:
            logger.error(f"Ошибка при сохранении cookies: {str(e)}")

    def _load_cookies_from_file(self, filename="gisopvk_webapi_cookies.json"):
        """
        Загружает cookies из файла в сессию

        Args:
            filename: Имя файла с сохраненными cookies

        Returns:
            bool: True если cookies успешно загружены, иначе False
        """
        try:
            if not os.path.exists(filename):
                logger.warning(f"Файл с cookies не найден: {filename}")
                return False

            with open(filename, 'r', encoding='utf-8') as f:
                cookies_dict = json.load(f)

            for name, value in cookies_dict.items():
                self.session.cookies.set(name, value)

            logger.info(f"Cookies загружены из файла {filename} ({len(cookies_dict)} cookies)")
            return True
        except Exception as e:
            logger.error(f"Ошибка при загрузке cookies: {str(e)}")
            return False

    def test_auth(self):
        """
        Проверяет, работает ли авторизация, делая тестовый запрос

        Returns:
            bool: True если авторизация работает, иначе False
        """
        try:
            # Делаем тестовый запрос к новому API
            response = self.session.get(
                f"{self.base_url}?page[limit]=1&page[offset]=0",
                timeout=10
            )

            # Если получили ошибку 406, пробуем скорректировать заголовки
            if response.status_code == 406:
                logger.warning(f"Получен статус 406 Not Acceptable, корректируем заголовки")
                self._fix_request_headers()

                # Повторяем запрос с новыми заголовками
                response = self.session.get(
                    f"{self.base_url}?page[limit]=1&page[offset]=0",
                    timeout=10
                )

            # Проверяем статус ответа
            if response.status_code == 200:
                try:
                    data = response.json()
                    # Проверяем, что ответ содержит ожидаемые поля для JSON:API
                    if 'data' in data and 'links' in data:
                        logger.info("Авторизация работает успешно")
                        return True
                    else:
                        logger.warning("Ответ не содержит ожидаемых полей JSON:API")
                        logger.debug(f"Получен ответ: {data}")
                        return False
                except json.JSONDecodeError:
                    logger.warning("Получен неверный формат JSON в ответе")
                    return False
            else:
                logger.warning(f"Неуспешный статус ответа: {response.status_code}")
                return False

        except Exception as e:
            logger.error(f"Ошибка при проверке авторизации: {str(e)}")
            return False

    def _save_stats(self, stats_file: str):
        """
        Сохраняет текущую статистику в JSON файл

        Args:
            stats_file: Путь к файлу для сохранения статистики
        """
        try:
            # Создаем копию статистики для сохранения
            stats_to_save = self.stats.copy()

            # Обрабатываем defaultdict для сериализации в JSON
            stats_to_save['status_codes'] = dict(stats_to_save['status_codes'])
            stats_to_save['errors'] = dict(stats_to_save['errors'])

            # Добавляем расчетные метрики
            if stats_to_save['start_time']:
                stats_to_save['elapsed_time'] = time.time() - stats_to_save['start_time']

                if stats_to_save['total_records'] > 0 and stats_to_save['elapsed_time'] > 0:
                    stats_to_save['records_per_second'] = stats_to_save['total_records'] / stats_to_save['elapsed_time']

            if stats_to_save['request_times']:
                stats_to_save['avg_request_time'] = statistics.mean(stats_to_save['request_times'])
                stats_to_save['min_request_time'] = min(stats_to_save['request_times'])
                stats_to_save['max_request_time'] = max(stats_to_save['request_times'])

                # Убираем полные массивы времен для экономии места
                stats_to_save['request_times'] = {
                    'count': len(self.stats['request_times']),
                    'sample': self.stats['request_times'][-10:] if self.stats['request_times'] else []
                }

            if stats_to_save['data_sizes']:
                stats_to_save['avg_data_size'] = statistics.mean(stats_to_save['data_sizes'])
                stats_to_save['total_data_size'] = sum(stats_to_save['data_sizes'])

                # Убираем полные массивы размеров для экономии места
                stats_to_save['data_sizes'] = {
                    'count': len(self.stats['data_sizes']),
                    'sample': self.stats['data_sizes'][-10:] if self.stats['data_sizes'] else []
                }

            # Добавляем временную метку
            stats_to_save['timestamp'] = datetime.datetime.now().isoformat()

            # Сохраняем в файл
            with open(stats_file, 'w', encoding='utf-8') as f:
                json.dump(stats_to_save, f, ensure_ascii=False, indent=2)

        except Exception as e:
            logger.error(f"Ошибка при сохранении статистики: {str(e)}")

    def get_stats_summary(self):
        """
        Возвращает строку с кратким отчетом о статистике

        Returns:
            str: Текстовый отчет
        """
        if not self.stats['start_time']:
            return "Статистика недоступна (парсинг не запускался)"

        elapsed_time = time.time() - self.stats['start_time']
        records_per_second = self.stats['total_records'] / elapsed_time if elapsed_time > 0 else 0

        avg_request_time = statistics.mean(self.stats['request_times']) if self.stats['request_times'] else 0
        avg_data_size = statistics.mean(self.stats['data_sizes']) if self.stats['data_sizes'] else 0
        total_data_size = sum(self.stats['data_sizes']) if self.stats['data_sizes'] else 0

        summary = [
            "=== СВОДКА СТАТИСТИКИ ПАРСИНГА ===",
            f"Всего запросов: {self.stats['total_requests']}",
            f"Успешных запросов: {self.stats['successful_requests']} ({(self.stats['successful_requests'] / self.stats['total_requests'] * 100) if self.stats['total_requests'] else 0:.1f}%)",
            f"Неудачных запросов: {self.stats['failed_requests']}",
            f"Повторных попыток: {self.stats['retry_count']}",
            f"Всего записей: {self.stats['total_records']}",
            f"Скорость: {records_per_second:.2f} записей/сек",
            f"Среднее время запроса: {avg_request_time:.3f} сек",
            f"Общий объем данных: {total_data_size:.2f} KB",
            f"Средний размер ответа: {avg_data_size:.2f} KB",
            f"Общее время работы: {elapsed_time:.1f} сек ({elapsed_time / 60:.1f} мин)"
        ]

        # Добавляем статус-коды, если они есть
        if self.stats['status_codes']:
            summary.append("\nРаспределение статус-кодов:")
            for code, count in sorted(self.stats['status_codes'].items()):
                summary.append(f"  - {code}: {count} раз")

        # Добавляем ошибки, если они есть
        if self.stats['errors']:
            summary.append("\nРаспределение ошибок:")
            for error, count in sorted(self.stats['errors'].items(), key=lambda x: x[1], reverse=True):
                summary.append(f"  - {error}: {count} раз")

        return "\n".join(summary)

    def get_page(self, offset: int = 0, limit: Optional[int] = None) -> Dict[str, Any]:
        """
        Получение одной страницы данных с повторными попытками при ошибке

        Args:
            offset: Смещение от начала списка
            limit: Количество записей на странице (если None, используется default_limit)

        Returns:
            Dict с ответом API в формате JSON:API
        """
        if limit is None:
            limit = self.default_limit

        url = f"{self.base_url}?page[limit]={limit}&page[offset]={offset}"

        self.stats['total_requests'] += 1
        request_start_time = time.time()

        for attempt in range(self.max_retries + 1):
            try:
                response = self.session.get(url, timeout=30)
                request_duration = time.time() - request_start_time
                self.stats['request_times'].append(request_duration)

                # Записываем статус код
                self.stats['status_codes'][response.status_code] += 1

                # Если получен статус 406, корректируем заголовки и пробуем снова
                if response.status_code == 406:
                    logger.warning(f"Получен статус 406 Not Acceptable, корректируем заголовки")
                    self._fix_request_headers()
                    continue

                # Размер данных (в килобайтах)
                content_length = len(response.content) / 1024
                self.stats['data_sizes'].append(content_length)

                response.raise_for_status()  # Вызывает исключение при ошибке HTTP

                self.stats['successful_requests'] += 1
                return response.json()

            except RequestException as e:
                self.stats['failed_requests'] += 1
                error_type = type(e).__name__
                self.stats['errors'][error_type] += 1

                logger.warning(f"Ошибка при запросе {url} (попытка {attempt + 1}/{self.max_retries + 1}): {str(e)}")

                if attempt < self.max_retries:
                    # Если ошибка 406, пробуем исправить заголовки
                    if '406' in str(e):
                        self._fix_request_headers()

                    # Если это не последняя попытка, уменьшаем limit и пробуем снова
                    if limit > self.fallback_limit:
                        limit = self.fallback_limit
                        url = f"{self.base_url}?page[limit]={limit}&page[offset]={offset}"
                        logger.info(f"Уменьшаем limit до {limit} и пробуем снова...")

                    self.stats['retry_count'] += 1
                    time.sleep(self.retry_delay)  # Задержка перед повторной попыткой
                else:
                    # Если все попытки исчерпаны, выбрасываем исключение
                    raise Exception(f"Не удалось выполнить запрос после {self.max_retries + 1} попыток")

    def extract_next_page_info(self, response_data: Dict[str, Any]) -> Tuple[Optional[int], Optional[int]]:
        """
        Извлечение информации для следующей страницы из ответа JSON:API

        Args:
            response_data: Ответ API в формате JSON:API

        Returns:
            Tuple[Optional[int], Optional[int]]: (offset для следующей страницы, limit из ссылки next)
            или (None, None) если следующей страницы нет
        """
        next_link = response_data.get('links', {}).get('next')
        if not next_link:
            return None, None

        parsed_url = urlparse(next_link)
        query_params = parse_qs(parsed_url.query)

        next_offset = None
        next_limit = None

        if 'page[offset]' in query_params:
            next_offset = int(query_params['page[offset]'][0])

        if 'page[limit]' in query_params:
            next_limit = int(query_params['page[limit]'][0])

        return next_offset, next_limit

    def process_and_save_streaming(self, output_file: Optional[str] = None,
                                   max_pages: Optional[int] = None,
                                   batch_size: int = 100,
                                   start_page: int = 0):
        """
        Последовательная обработка данных с потоковой записью в JSONL.GZ файл

        Args:
            output_file: Имя файла для сохранения (None для автогенерации)
            max_pages: Максимальное количество страниц (None для всех)
            batch_size: Размер пакета записей для сохранения за раз
            start_page: Начальная страница (для продолжения прерванной загрузки)
        """
        # Инициализация статистики
        self.stats['start_time'] = time.time()
        self.stats['total_records'] = 0

        # Получаем информацию о пагинации
        logger.info("Получение информации о пагинации...")

        try:
            first_page = self.get_page(0, self.default_limit)
        except Exception as e:
            logger.error(f"Ошибка при получении первой страницы: {str(e)}")
            print(f"\nОшибка при получении первой страницы: {str(e)}")

            # Возможно, проблема с заголовками - попробуем исправить и повторить
            self._fix_request_headers()
            print("\nИсправили заголовки, повторяем запрос...")
            try:
                first_page = self.get_page(0, self.default_limit)
            except Exception as e2:
                logger.error(f"Повторная ошибка при получении первой страницы: {str(e2)}")
                print(f"\nПовторная ошибка при получении первой страницы: {str(e2)}")

                # Предлагаем пользователю скопировать заголовки из браузера
                print("\nПопробуем получить заголовки напрямую из браузера...")
                self._extract_browser_cookies()

                # Последняя попытка
                try:
                    first_page = self.get_page(0, self.default_limit)
                except Exception as e3:
                    logger.error(f"Не удалось получить первую страницу после всех попыток: {str(e3)}")
                    raise Exception("Не удается получить данные API после всех попыток. Проверьте доступ к API.")

        # Определяем общее количество записей из links.last
        total_count = 0
        total_pages = 0

        if 'links' in first_page and 'last' in first_page['links']:
            last_link = first_page['links']['last']
            # Извлекаем max_offset из ссылки на последнюю страницу
            offset_match = re.search(r'page%5Boffset%5D=(\d+)', last_link)
            if offset_match:
                max_offset = int(offset_match.group(1))
                # Приблизительное количество записей
                total_count = max_offset + self.default_limit
                total_pages = (total_count + self.default_limit - 1) // self.default_limit
                logger.info(f"Оценка общего количества записей: ~{total_count}, страниц: ~{total_pages}")

        pages_to_load = total_pages - start_page if max_pages is None else min(max_pages, total_pages - start_page)
        end_page = start_page + pages_to_load - 1

        if output_file is None:
            current_time = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"gisopvk_webapi_{current_time}_{start_page + 1}-{end_page + 1}.jsonl.gz"

        logger.info(f"Потоковая запись в {output_file} (формат JSONL + GZIP)")

        # Создаем файл статистики с тем же именем
        stats_file = output_file.replace('.jsonl.gz', '_stats.json')

        progress_file = "gisopvk_webapi_progress.txt"
        with open(progress_file, 'a', encoding='utf-8') as pf:
            pf.write(
                f"{datetime.datetime.now():%Y%m%d_%H%M%S} - Запуск с {start_page + 1} по {end_page + 1} из {total_pages}\n")
            pf.write(f"Всего записей: ~{total_count}, страниц к загрузке: {pages_to_load}\n")

        # Начинаем с указанной страницы
        offset = start_page * self.default_limit
        limit = self.default_limit
        page_count = 0
        record_count = 0
        batch = []
        empty_pages_count = 0
        max_empty_pages = 3  # Максимальное количество пустых страниц подряд перед выходом

        def write_batch_jsonl_gz(file_path, records):
            """Записать пакет записей в JSONL.GZ файл"""
            mode = 'at' if os.path.exists(file_path) else 'wt'
            with gzip.open(file_path, mode, encoding='utf-8') as f:
                for rec in records:
                    f.write(json.dumps(rec, ensure_ascii=False) + '\n')

        while True:
            # Проверка ограничения на количество страниц
            if max_pages is not None and page_count >= max_pages:
                logger.info(f"Достигнуто ограничение на количество страниц: {max_pages}")
                break

            logger.info(f"Загрузка страницы с offset={offset}, limit={limit}...")

            try:
                response_data = self.get_page(offset, limit)

                # Обрабатываем результаты текущей страницы
                data_items = response_data.get('data', [])
                if not isinstance(data_items, list):
                    data_items = [data_items]

                if not data_items:
                    empty_pages_count += 1
                    logger.warning(f"Получена пустая страница ({empty_pages_count}/{max_empty_pages})")

                    if empty_pages_count >= max_empty_pages:
                        logger.info("Достигнуто максимальное количество пустых страниц подряд. Завершаем загрузку.")
                        break
                else:
                    empty_pages_count = 0  # Сбрасываем счетчик пустых страниц

                for item in data_items:
                    batch.append(item)
                    record_count += 1
                    self.stats['total_records'] += 1

                    if len(batch) >= batch_size:
                        write_batch_jsonl_gz(output_file, batch)
                        batch.clear()

                # Получаем информацию для следующей страницы
                next_offset, next_limit = self.extract_next_page_info(response_data)

                if next_offset is None:
                    logger.info("Достигнут конец данных (нет ссылки на следующую страницу).")
                    break

                # Обновляем параметры для следующего запроса
                offset = next_offset
                if next_limit is not None:
                    limit = next_limit

                page_count += 1

                # Периодически выводим прогресс и статистику
                if page_count % 5 == 0 or record_count % 100 == 0:
                    elapsed_time = time.time() - self.stats['start_time']
                    records_per_second = record_count / elapsed_time if elapsed_time > 0 else 0
                    avg_request_time = statistics.mean(self.stats['request_times']) if self.stats[
                        'request_times'] else 0
                    avg_data_size = statistics.mean(self.stats['data_sizes']) if self.stats['data_sizes'] else 0

                    progress_msg = (
                        f"Прогресс: {page_count}/{pages_to_load} страниц, {record_count} записей | "
                        f"Скорость: {records_per_second:.2f} записей/сек | "
                        f"Среднее время запроса: {avg_request_time:.2f} сек | "
                        f"Средний размер данных: {avg_data_size:.2f} KB"
                    )
                    logger.info(progress_msg)

                    with open(progress_file, 'a', encoding='utf-8') as pf:
                        pf.write(f"{datetime.datetime.now():%H:%M:%S} - {progress_msg}\n")

                    # Обновляем статистику
                    self._save_stats(stats_file)

                # Задержка между запросами
                time.sleep(self.request_delay)

            except Exception as e:
                logger.error(f"Ошибка при обработке страницы: {str(e)}")
                print(f"\nОшибка при обработке страницы: {str(e)}")

                # Проверяем, не ошибка ли это 406 Not Acceptable
                if '406' in str(e):
                    print("\nОбнаружена ошибка 406 Not Acceptable. Корректируем заголовки...")
                    self._fix_request_headers()
                    # Продолжаем без сохранения пакета (повторим попытку)
                    time.sleep(self.retry_delay)
                    continue

                # Сохраняем текущую партию перед выходом
                if batch:
                    write_batch_jsonl_gz(output_file, batch)
                    batch.clear()

                # Записываем информацию об ошибке
                with open(progress_file, 'a', encoding='utf-8') as pf:
                    pf.write(f"{datetime.datetime.now():%H:%M:%S} - Ошибка: {str(e)}\n")

                # Спрашиваем пользователя, хочет ли он продолжить
                retry_now = input("\nПроизошла ошибка. Повторить запрос с текущей страницы? (y/n): ")
                if retry_now.lower() == 'y':
                    # Повторяем текущую страницу
                    time.sleep(self.retry_delay)
                    continue
                else:
                    # Пробуем следующую страницу
                    try:
                        next_offset, next_limit = offset + limit, limit
                        offset = next_offset
                        page_count += 1
                        continue
                    except Exception:
                        # Если не удалось перейти к следующей странице, выходим
                        break

        # Записываем оставшиеся записи
        if batch:
            write_batch_jsonl_gz(output_file, batch)

        # Финальная статистика
        elapsed_time = time.time() - self.stats['start_time']
        records_per_second = record_count / elapsed_time if elapsed_time > 0 else 0

        completion_msg = (
            f"Загрузка завершена. Всего загружено {record_count} записей из {page_count} страниц. "
            f"Время: {elapsed_time:.2f} сек. Скорость: {records_per_second:.2f} записей/сек"
        )
        logger.info(f"✅ {completion_msg}")
        logger.info(f"Данные сохранены в файл {output_file}")

        # Сохраняем итоговую статистику
        self._save_stats(stats_file)
        logger.info(f"Статистика сохранена в файл {stats_file}")

        with open(progress_file, 'a', encoding='utf-8') as pf:
            pf.write(f"{datetime.datetime.now():%Y%m%d_%H%M%S} - {completion_msg}\n")
            pf.write(f"Файл: {output_file}\n")
            pf.write(f"Оставшиеся страницы: с {start_page + page_count + 1} до {total_pages}\n")
            pf.write("-" * 50 + "\n")

    def close(self):
        """
        Закрывает драйвер и сессию
        """
        try:
            if self.driver:
                self.driver.quit()
                logger.info("Драйвер Selenium закрыт")
        except Exception as e:
            logger.error(f"Ошибка при закрытии драйвера: {str(e)}")

        try:
            self.session.close()
            logger.info("Сессия запросов закрыта")
        except Exception as e:
            logger.error(f"Ошибка при закрытии сессии: {str(e)}")
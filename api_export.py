"""
Выгрузка списка из API ФГИС ОПВК целиком: водители, ТС, транспортные задачи и т.п. — только чтение.

webapi отдаёт данные своей организации (ЭКОПРОМ): тот же вход, что у сбора заказов
(gisopvk_webapi_cookies.json, см. login_fgis.py или кнопку «Войти в ФГИС» в UI).

    python api_export.py drivers                   # водители (адрес эндпоинта подбирается)
    python api_export.py vehicles
    python api_export.py driver_tasks
    python api_export.py --url https://gisopvk.ru/webapi/v1/driver/ --name drivers   # свой адрес

Результат: exports/<имя>_<время>/
    records.jsonl.gz  — записи как есть (по одной в строке)
    records.csv       — плоская таблица: id + атрибуты (вложенные — через точку; списки — JSON),
                        UTF-8 с BOM, разделитель «;», все поля в кавычках — для русского Excel
    info.json         — адрес, число записей, время
Файлы содержат личные данные (ФИО, телефоны, документы водителей): папка exports/ в git не попадает.

Код выхода: 0 — готово, 3 — нужен вход в ФГИС, 1 — ошибка (в т.ч. ни один адрес не ответил).
"""

import argparse
import csv
import gzip
import json
import re
import sys
import time
from datetime import datetime
from pathlib import Path

from api_probe import HOST, is_portal_url, records, total
from gisopvkwebapiparser import GisopvkWebApiParser
from pipeline_common import EXIT_AUTH_REQUIRED, ROOT

# Что выгружать: имя -> адреса-кандидаты (первый ответивший списком и берётся)
DATASETS = {
    "drivers": ("Водители", [f"{HOST}/webapi/v1/driver/", f"{HOST}/webapi/v1/drivers/",
                             f"{HOST}/webapi/v1/vehicle_driver/", f"{HOST}/webapi/v1/employee/",
                             f"{HOST}/webapi/v1/transport_driver/"]),
    "vehicles": ("Транспортные средства", [f"{HOST}/webapi/v1/vehicle/"]),
    "driver_tasks": ("Транспортные задачи", [f"{HOST}/webapi/v1/driver_task/"]),
    "contracts_transportation": ("Договоры на транспортирование", [f"{HOST}/webapi/v1/contract_transportation/"]),
    "licenses": ("Лицензии", [f"{HOST}/webapi/v1/license/", f"{HOST}/webapi/v1/licence/"]),
}
EXPORT_DIR = ROOT / "exports"
PAGE_LIMIT = 50


def flatten(record: dict) -> dict:
    """id, type + атрибуты плоско; связи — как id (или список id)."""
    row = {"id": record.get("id"), "type": record.get("type")}
    attrs = record.get("attributes") if isinstance(record.get("attributes"), dict) else {
        k: v for k, v in record.items() if k not in ("id", "type")}

    def put(prefix, value):
        if isinstance(value, dict) and value and len(prefix.split(".")) < 3:
            for k, v in value.items():
                put(f"{prefix}.{k}", v)
        elif isinstance(value, (dict, list)):
            row[prefix] = json.dumps(value, ensure_ascii=False) if value else ""
        else:
            row[prefix] = value

    for key, value in attrs.items():
        put(key, value)
    for rel, value in (record.get("relationships") or {}).items():
        data = (value or {}).get("data")
        if isinstance(data, list):
            row[f"rel.{rel}"] = ",".join(str(d.get("id")) for d in data)
        elif isinstance(data, dict):
            row[f"rel.{rel}"] = data.get("id")
    return row


CSV_DELIMITER = ";"
FORMULA_START = ("=", "+", "-", "@", "\t", "\r")
PLAIN_NUMBER = re.compile(r"^[+-]?[\d\s().,-]+$")  # телефоны, суммы: формулой не станут


def excel_safe(value):
    """Ячейка, которую Excel не выполнит как формулу: значения из ФГИС заполняют и сторонние люди.

    Строку, начинающуюся с = + - @ (или табуляции), предваряет апостроф — Excel покажет её как текст.
    Телефоны и числа вида +79001234567 / -12.5 не трогаем.
    """
    if isinstance(value, str) and value.startswith(FORMULA_START) and not PLAIN_NUMBER.match(value):
        return "'" + value
    return value


class Fetcher:
    def __init__(self, session, delay: float = 0.3, timeout: float = 90):
        self.session, self.delay, self.timeout = session, delay, timeout

    def get(self, url: str, params: dict, retries: int = 4):
        if not is_portal_url(url):
            raise PermissionError("адрес не на gisopvk.ru")
        time.sleep(self.delay)
        for attempt in range(retries):
            r = self.session.get(url, params=params, timeout=self.timeout, allow_redirects=False)
            if r.status_code in (401, 403) and r.content.lstrip().startswith(b"{"):
                raise PermissionError(f"HTTP {r.status_code}")
            if (r.status_code == 429 or r.status_code >= 500) and attempt < retries - 1:
                time.sleep(2 ** attempt)
                continue
            return r
        return r


def find_endpoint(fetcher: Fetcher, candidates: list):
    """Первый адрес, который отдаёт список записей.

    Сначала с sort=id (стабильный порядок страниц), при отказе — без него.
    Возвращает (url, параметры, ответ первой страницы, проверено).
    """
    tried = []
    for url in candidates:
        for extra in ({"sort": "id"}, {}):
            params = {"page[limit]": PAGE_LIMIT, "page[offset]": 0, **extra}
            r = fetcher.get(url, params, retries=1)  # подбор адреса: отказ (в т.ч. 500 на sort) — сразу дальше
            tried.append(f"{url}{' (sort=id)' if extra else ''} → {r.status_code}")
            if r.status_code == 404:
                break  # адреса нет — без sort тоже не будет
            if r.status_code != 200:
                continue
            try:
                body = r.json()
            except ValueError:
                continue
            if isinstance(body, dict) and ("data" in body or "results" in body):
                return url, extra, body, tried
    return None, None, None, tried


def export(fetcher: Fetcher, name: str, candidates: list, out_root: Path) -> Path:
    url, extra, first, tried = find_endpoint(fetcher, candidates)
    for line in tried:
        print(f"  {line}", flush=True)
    if url is None:
        raise LookupError("ни один адрес не отдал список — пришлите отчёт «Исследовать API», "
                          "в нём будет настоящий адрес")
    count = total({"json": first})
    print(f"Эндпоинт: {url}, записей: {count}", flush=True)

    out = out_root / f"{name}_{datetime.now():%Y%m%d_%H%M%S}"
    out.mkdir(parents=True)
    rows, seen, offset, body = [], set(), 0, first
    with gzip.open(out / "records.jsonl.gz", "wt", encoding="utf-8") as raw:
        while True:
            page = records({"json": body})
            for rec in page:
                key = str(rec.get("id"))
                if key in seen:
                    continue  # на стыке страниц, если список сдвинулся
                seen.add(key)
                raw.write(json.dumps(rec, ensure_ascii=False) + "\n")
                rows.append(flatten(rec))
            offset += len(page)
            print(f"получено {len(seen)}" + (f"/{count}" if isinstance(count, int) else ""), flush=True)
            if not page or (isinstance(count, int) and offset >= count):
                break
            r = fetcher.get(url, {"page[limit]": PAGE_LIMIT, "page[offset]": offset, **extra})
            if r.status_code != 200:
                raise RuntimeError(f"страница offset={offset}: HTTP {r.status_code}")
            body = r.json()

    columns = list(dict.fromkeys(k for row in rows for k in row))
    with open(out / "records.csv", "w", encoding="utf-8-sig", newline="") as f:
        # «;» — разделитель русского Excel; каждое поле в кавычках, чтобы Excel не делил значение
        # по «;» или «,» внутри него (иначе кусок после разделителя стал бы отдельной ячейкой-формулой)
        w = csv.writer(f, delimiter=CSV_DELIMITER, quoting=csv.QUOTE_ALL)
        w.writerow([excel_safe(c) for c in columns])
        for row in rows:
            w.writerow([excel_safe(row.get(c)) for c in columns])
    (out / "info.json").write_text(json.dumps({
        "name": name, "url": url, "sorted_by_id": bool(extra), "records": len(rows), "total_reported": count,
        "exported_at": datetime.now().isoformat(timespec="seconds")}, ensure_ascii=False, indent=1),
        encoding="utf-8")
    print(f"Готово: {len(rows)} записей → {out}", flush=True)
    return out


def main() -> int:
    sys.stdout.reconfigure(errors="replace")  # консоль Windows в cp1251/cp866 не знает «→»
    ap = argparse.ArgumentParser(description="Выгрузка списка из API ФГИС ОПВК (только чтение).")
    ap.add_argument("dataset", nargs="?", choices=sorted(DATASETS), help="Что выгрузить.")
    ap.add_argument("--url", help="Свой адрес эндпоинта (https://gisopvk.ru/webapi/v1/…/).")
    ap.add_argument("--name", help="Имя для папки результата при --url.")
    ap.add_argument("--out", default=str(EXPORT_DIR), help="Куда сохранять (по умолчанию exports/).")
    args = ap.parse_args()
    if not args.dataset and not args.url:
        ap.error("укажите набор данных или --url")
    if args.url and not is_portal_url(args.url):
        ap.error("--url должен быть на https://gisopvk.ru/")

    name = args.name or args.dataset or "export"
    candidates = [args.url if args.url.endswith("/") else args.url + "/"] if args.url else DATASETS[args.dataset][1]
    parser = GisopvkWebApiParser(base_url=f"{HOST}/webapi/v1/vehicle/")
    try:
        if not (parser._load_cookies_from_file() and parser.test_auth()):
            print("Сохранённый вход в ФГИС недействителен — выполните: python login_fgis.py", flush=True)
            return EXIT_AUTH_REQUIRED
        export(Fetcher(parser.session), name, candidates, Path(args.out))
        return 0
    except PermissionError as e:
        print(f"Нет доступа ({e}) — войдите в ФГИС заново (login_fgis.py) и повторите.", flush=True)
        return EXIT_AUTH_REQUIRED
    except Exception as e:
        print(f"Ошибка: {e}", flush=True)
        return 1
    finally:
        parser.close()


if __name__ == "__main__":
    sys.exit(main())

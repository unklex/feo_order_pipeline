"""
Выгрузка списков из API ФГИС ОПВК: любые эндпоинты из корней API (водители, ТС, транспортные задачи,
договоры, документы, справочники…) — только чтение.

Тот же вход, что у сбора заказов (gisopvk_webapi_cookies.json, см. login_fgis.py или кнопку
«Войти в ФГИС» в UI). Что именно отдаёт эндпоинт — решает портал: webapi обычно данные своей организации.

    python api_export.py drivers                             # короткие имена: drivers, driver_tasks, …
    python api_export.py --target driver=https://gisopvk.ru/filterapi/v1/driver/ --target trip_report=…
    python api_export.py --target bid=https://gisopvk.ru/stencilapi/v1/order_waste_source/ --max-records 1000
    python api_export.py --refresh-endpoints                 # обновить список эндпоинтов из корней API

Список эндпоинтов для UI: api_endpoints.json (корни /webapi/v1/ и /stencilapi/v1/), после
--refresh-endpoints — reports/api_endpoints_live.json.

Результат: exports/<имя>_<время>/
    records.json  — записи как есть, JSON-массив (одна запись на строку)
    records.csv   — плоская таблица: id + атрибуты (вложенные — через точку; списки — JSON),
                    UTF-8 с BOM, разделитель «;», все поля в кавычках — для русского Excel
    info.json     — адрес, число записей, время
Файлы содержат личные данные (ФИО, телефоны, документы): папка exports/ в git не попадает.

Код выхода: 0 — готово, 3 — нужен вход в ФГИС, 4 — часть эндпоинтов не выгрузилась, 1 — ошибка.
"""

import argparse
import csv
import json
import re
import sys
import time
from datetime import datetime
from pathlib import Path

from api_probe import HOST, is_portal_url, records, total
from gisopvkwebapiparser import GisopvkWebApiParser
from pipeline_common import EXIT_AUTH_REQUIRED, EXIT_PARTIAL, ROOT

ENDPOINTS_FILE = ROOT / "api_endpoints.json"
LIVE_ENDPOINTS_FILE = ROOT / "reports" / "api_endpoints_live.json"
API_ROOTS = [f"{HOST}/webapi/v1/", f"{HOST}/stencilapi/v1/", f"{HOST}/filterapi/v1/", f"{HOST}/externalapi/v1/"]

# Короткие имена для консоли: имя -> (название, адреса-кандидаты; первый ответивший списком и берётся)
DATASETS = {
    "drivers": ("Водители", [f"{HOST}/filterapi/v1/driver/"]),
    "vehicles": ("Транспортные средства", [f"{HOST}/webapi/v1/vehicle/"]),
    "driver_tasks": ("Транспортные задачи", [f"{HOST}/stencilapi/v1/driver_task/", f"{HOST}/webapi/v1/driver_task/"]),
    "trip_reports": ("Отчёты о рейсах", [f"{HOST}/webapi/v1/trip_report/"]),
    "contracts_transportation": ("Договоры на транспортирование",
                                 [f"{HOST}/stencilapi/v1/contract_transportation/"]),
    "licenses": ("Лицензии", [f"{HOST}/webapi/v1/license_new/"]),
}
EXPORT_DIR = ROOT / "exports"
PAGE_LIMIT = 50

# Группы для списка в UI: (название, префиксы/подстроки имени)
GROUPS = [
    ("Справочники", ("dict/",)),
    ("Водители и рейсы", ("driver", "trip_", "route", "telemetry", "waste_container_report")),
    ("Документы", ("document_", "_invoice", "sign_", "mrpoa", "file", "photo")),
    ("Договоры", ("contract_", "subcontract_")),
    ("Заявки и заказы", ("bid", "order_", "respond_", "charge_", "claim_", "waste_handover")),
    ("Контакты", ("contact", "email", "phone", "user")),
    ("Объекты и лицензии", ("facility", "wasteplace", "pvn", "wastetank", "license", "vehicle", "organization")),
    ("Отчёты", ("report", "analytic", "production_plan", "stock_correction", "recycle_")),
    ("Интеграции и служебное", ("integration_", "erp_", "eosdo", "task", "popup", "notice", "support_",
                                "permission", "role", "aggregation", "off_system", "offsystem", "object_")),
]


def group_of(name: str) -> str:
    for title, keys in GROUPS:
        if any(k in name for k in keys):
            return title
    return "Прочее"


def load_endpoints() -> list:
    """[{name, aliases, url, group}] — из обновлённого списка, если он есть, иначе из api_endpoints.json."""
    path = LIVE_ENDPOINTS_FILE if LIVE_ENDPOINTS_FILE.exists() else ENDPOINTS_FILE
    data = json.loads(path.read_text(encoding="utf-8"))
    out = []
    for e in data["endpoints"]:
        if is_portal_url(e["url"]):
            out.append({**e, "group": group_of(e["name"])})
    return out


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
            if isinstance(body, list):
                return url, extra, {"results": body}, tried  # список без пагинации
    return None, None, None, tried


def export(fetcher: Fetcher, name: str, candidates: list, out_root: Path, max_records: int = 0) -> Path:
    """Выгружает один эндпоинт. max_records > 0 — не больше стольких записей."""
    url, extra, first, tried = find_endpoint(fetcher, candidates)
    for line in tried:
        print(f"  {line}", flush=True)
    if url is None:
        raise LookupError("адрес не отдал список записей")
    count = total({"json": first})
    limit_note = f" (выгружаем не больше {max_records})" if max_records else ""
    print(f"Эндпоинт: {url}, записей: {count}{limit_note}", flush=True)

    out = out_root / f"{re.sub(r'[^0-9A-Za-z_-]+', '_', name)}_{datetime.now():%Y%m%d_%H%M%S}"
    out.mkdir(parents=True)
    rows, seen, offset, body = [], set(), 0, first
    with open(out / "records.json", "w", encoding="utf-8") as raw:
        raw.write("[\n")
        while True:
            page = records({"json": body})
            for rec in page:
                key = str(rec.get("id")) if isinstance(rec, dict) and rec.get("id") is not None else None
                if key is not None and key in seen:
                    continue  # на стыке страниц, если список сдвинулся
                if key is not None:
                    seen.add(key)
                raw.write((",\n" if rows else "") + json.dumps(rec, ensure_ascii=False))
                rows.append(flatten(rec) if isinstance(rec, dict) else {"value": rec})
                if max_records and len(rows) >= max_records:
                    break
            offset += len(page)
            print(f"получено {len(rows)}" + (f"/{count}" if isinstance(count, int) else ""), flush=True)
            paged = isinstance(body.get("meta"), dict) or bool((body.get("links") or {}).get("next"))
            if (not page or not paged or (isinstance(count, int) and offset >= count)
                    or (max_records and len(rows) >= max_records)):
                break  # «paged» — у ответа есть пагинация; голый список приходит одной страницей
            r = fetcher.get(url, {"page[limit]": PAGE_LIMIT, "page[offset]": offset, **extra})
            if r.status_code != 200:
                raise RuntimeError(f"страница offset={offset}: HTTP {r.status_code}")
            body = r.json()
        raw.write("\n]\n")

    columns = list(dict.fromkeys(k for row in rows for k in row)) or ["id"]  # пустой ответ — файл с заголовком
    with open(out / "records.csv", "w", encoding="utf-8-sig", newline="") as f:
        # «;» — разделитель русского Excel; каждое поле в кавычках, чтобы Excel не делил значение
        # по «;» или «,» внутри него (иначе кусок после разделителя стал бы отдельной ячейкой-формулой)
        w = csv.writer(f, delimiter=CSV_DELIMITER, quoting=csv.QUOTE_ALL)
        w.writerow([excel_safe(c) for c in columns])
        for row in rows:
            w.writerow([excel_safe(row.get(c)) for c in columns])
    (out / "info.json").write_text(json.dumps({
        "name": name, "url": url, "sorted_by_id": bool(extra), "records": len(rows), "total_reported": count,
        "max_records": max_records, "exported_at": datetime.now().isoformat(timespec="seconds")},
        ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"Готово: {name} — {len(rows)} записей → {out}", flush=True)
    return out


def refresh_endpoints(fetcher: Fetcher) -> int:
    """Список эндпоинтов из корней API -> reports/api_endpoints_live.json. Возвращает число адресов."""
    by_url = {}
    for root in API_ROOTS:
        r = fetcher.get(root, {}, retries=2)
        print(f"  {root} → {r.status_code}", flush=True)
        if r.status_code != 200:
            continue
        try:
            data = r.json().get("data")
        except (ValueError, AttributeError):
            continue
        for name, link in (data or {}).items() if isinstance(data, dict) else []:
            if isinstance(link, str) and is_portal_url(link):
                link = link.split("?")[0]
                by_url.setdefault(link if link.endswith("/") else link + "/", []).append(name)
    if not by_url:
        raise RuntimeError("корни API не отдали ни одного адреса")
    endpoints = sorted(({"name": names[0], "aliases": names[1:], "url": url} for url, names in by_url.items()),
                       key=lambda e: e["name"])
    LIVE_ENDPOINTS_FILE.parent.mkdir(exist_ok=True)
    LIVE_ENDPOINTS_FILE.write_text(json.dumps({
        "source": "корни API, обновлено " + datetime.now().isoformat(timespec="seconds"),
        "endpoints": endpoints}, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"Готово: эндпоинтов {len(endpoints)} → {LIVE_ENDPOINTS_FILE}", flush=True)
    return len(endpoints)


def main() -> int:
    sys.stdout.reconfigure(errors="replace")  # консоль Windows в cp1251/cp866 не знает «→»
    ap = argparse.ArgumentParser(description="Выгрузка списков из API ФГИС ОПВК (только чтение).")
    ap.add_argument("dataset", nargs="*", choices=sorted(DATASETS), help="Короткие имена наборов.")
    ap.add_argument("--target", action="append", default=[], metavar="ИМЯ=URL",
                    help="Эндпоинт для выгрузки; можно несколько раз.")
    ap.add_argument("--max-records", type=int, default=0, help="Не больше стольких записей с эндпоинта (0 — все).")
    ap.add_argument("--refresh-endpoints", action="store_true", help="Обновить список эндпоинтов из корней API.")
    ap.add_argument("--out", default=str(EXPORT_DIR), help="Куда сохранять (по умолчанию exports/).")
    args = ap.parse_args()

    targets = [(d, DATASETS[d][1]) for d in args.dataset]
    for item in args.target:
        name, _, url = item.partition("=")
        url = url.strip()
        if not name.strip() or not is_portal_url(url):
            ap.error(f"--target ждёт ИМЯ=https://gisopvk.ru/…, получено {item!r}")
        targets.append((name.strip(), [url if url.endswith("/") else url + "/"]))
    if not targets and not args.refresh_endpoints:
        ap.error("укажите, что выгрузить (набор, --target) или --refresh-endpoints")

    parser = GisopvkWebApiParser(base_url=f"{HOST}/webapi/v1/vehicle/")
    try:
        if not (parser._load_cookies_from_file() and parser.test_auth()):
            print("Сохранённый вход в ФГИС недействителен — выполните: python login_fgis.py", flush=True)
            return EXIT_AUTH_REQUIRED
        fetcher = Fetcher(parser.session)
        if args.refresh_endpoints:
            refresh_endpoints(fetcher)
        failed = []
        for i, (name, candidates) in enumerate(targets, 1):
            print(f"[{i}/{len(targets)}] {name}", flush=True)
            try:
                export(fetcher, name, candidates, Path(args.out), args.max_records)
            except PermissionError:
                raise
            except Exception as e:
                print(f"Не выгружено: {name} — {e}", flush=True)
                failed.append(name)
        if failed:
            print(f"Итого: не выгружены {len(failed)} из {len(targets)}: {', '.join(failed)}", flush=True)
            return EXIT_PARTIAL
        print(f"Итого: выгружено {len(targets)}", flush=True)
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

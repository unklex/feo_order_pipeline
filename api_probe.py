"""
Исследование структуры API ФГИС ОПВК (gisopvk.ru): что отдают эндпоинты и какие параметры работают.

Только чтение: GET и OPTIONS с сохранённым входом (gisopvk_webapi_cookies.json, см. login_fgis.py),
с паузой между запросами. Ничего не создаёт и не меняет ни в ФГИС, ни в БД, ни в Битрикс.

Что проверяется:
  * корни API (/webapi/v1/, /stencilapi/v1/) и типичные адреса схемы (OpenAPI/Swagger) — список эндпоинтов;
  * для каждого эндпоинта: статус, время ответа, meta/links, общее число записей, структура записи
    (attributes, relationships, included) по выборке: типы, доля пустых, длина, повторяющиеся значения;
    детальный ответ /<id>/ в сравнении со списком; куда ведут ссылки relationships;
  * для основных эндпоинтов (--deep): какие sort, фильтры (5 вариантов записи), include, fields[...]
    и варианты пагинации сервер реально применяет, а какие молча игнорирует; время ответа от page[limit];
  * доступ к файлам: по одному примеру каждого вида ссылок на файлы из записей всех эндпоинтов (вложения
    статусов, фото, подписанные документы, …) и документы из relationships (счета, УПД, акты — заказов
    и переработки): отдаёт ли сервер файл, тип и размер. Читаются только первые 8 КБ, на диск не пишется.

    python api_probe.py                         # основные эндпоинты + найденные в корнях API
    python api_probe.py --deep order_transportation --deep stencil_bid
    python api_probe.py --endpoint my=https://gisopvk.ru/webapi/v1/driver_task/ --deep my
    python api_probe.py --raw                   # без маскирования примеров значений

Результат: reports/api_probe_<время>/report.md (сводка — её и присылать), report.json (всё подробно),
samples/<эндпоинт>.json (по одной записи). Личные данные в примерах по умолчанию маскируются:
поля с телефонами, почтой, ФИО, ИНН, адресами и т.п. показываются как <str len=N>.

Код выхода: 0 — готово, 3 — нужен вход в ФГИС (python login_fgis.py), 1 — ошибка.
"""

import argparse
import json
import re
import sys
import time
from collections import Counter
from datetime import date, datetime, timedelta
from pathlib import Path
from urllib.parse import parse_qs, parse_qsl, quote, urlparse

from gisopvkwebapiparser import GisopvkWebApiParser
from pipeline_common import EXIT_AUTH_REQUIRED, ROOT

HOST = "https://gisopvk.ru"
ORDERS_URL = f"{HOST}/webapi/v1/order_transportation/"

# Известные эндпоинты (из скриптов проекта и test_api.py)
ENDPOINTS = {
    "order_transportation": ORDERS_URL,
    "stencil_bid": f"{HOST}/stencilapi/v1/bid/",
    "webapi_bid": f"{HOST}/webapi/v1/bid/",
    "vehicle": f"{HOST}/webapi/v1/vehicle/",
    "stencil_order_transportation": f"{HOST}/stencilapi/v1/order_transportation/",
    "stencil_order_waste_source": f"{HOST}/stencilapi/v1/order_waste_source/",
    "stencil_order_recycling": f"{HOST}/stencilapi/v1/order_recycling/",
    "stencil_contract_transportation": f"{HOST}/stencilapi/v1/contract_transportation/",
    "stencil_contract_waste_generator": f"{HOST}/stencilapi/v1/contract_waste_generator/",
    "stencil_contract_recycling": f"{HOST}/stencilapi/v1/contract_recycling/",
    "stencil_wasteplace": f"{HOST}/stencilapi/v1/wasteplace/",
    "stencil_facility": f"{HOST}/stencilapi/v1/facility/",
    "stencil_vehicle": f"{HOST}/stencilapi/v1/vehicle/",
    "stencil_organization_info": f"{HOST}/stencilapi/v1/organization_info/",
    # webapi-версии переработки и договоров (в stencilapi они есть; здесь — проверить, отдаёт ли webapi)
    "order_recycling": f"{HOST}/webapi/v1/order_recycling/",
    "contract_recycling": f"{HOST}/webapi/v1/contract_recycling/",
    "contract_transportation": f"{HOST}/webapi/v1/contract_transportation/",
    "driver_task": f"{HOST}/webapi/v1/driver_task/",
}
DEFAULT_DEEP = ["order_transportation", "stencil_bid"]

API_ROOTS = [f"{HOST}/webapi/v1/", f"{HOST}/stencilapi/v1/", f"{HOST}/webapi/", f"{HOST}/stencilapi/"]
SCHEMA_URLS = [
    f"{HOST}/webapi/v1/schema/", f"{HOST}/webapi/schema/", f"{HOST}/stencilapi/v1/schema/",
    f"{HOST}/stencilapi/schema/", f"{HOST}/api/schema/", f"{HOST}/schema/", f"{HOST}/openapi.json",
    f"{HOST}/swagger.json", f"{HOST}/webapi/v1/?format=openapi", f"{HOST}/webapi/v1/swagger/",
    f"{HOST}/webapi/docs/", f"{HOST}/redoc/",
]

SENSITIVE = re.compile(r"phone|tel|mail|fio|name|inn|snils|passport|contact|address|author|driver|"
                       r"login|user|person|comment|note|kpp|ogrn|bank|account|plate|imei", re.I)
DATE_FIELD = re.compile(r"(^dt_|_dt$|_date$|^date_|_at$|^date$)", re.I)
MAX_DISCOVERED = 200
API_PATH = re.compile(r"^(?:https?://[^/]+)?/(?:webapi|stencilapi)/v\d+/[\w./-]+$")


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------
class Client:
    """Сессия ФГИС (cookies и заголовки как у выгрузки) с паузой и учётом запросов."""

    def __init__(self, delay: float, timeout: float, max_requests: int):
        self.parser = GisopvkWebApiParser(base_url=ORDERS_URL)
        self.session = self.parser.session
        self.delay, self.timeout, self.max_requests = delay, timeout, max_requests
        self.count = 0

    def login_ok(self) -> bool:
        return self.parser._load_cookies_from_file() and self.parser.test_auth()

    def request(self, url: str, params=None, method: str = "GET") -> dict:
        if self.count >= self.max_requests:
            return {"url": url, "status": None, "error": "лимит запросов (--max-requests) исчерпан"}
        query = "&".join(f"{quote(str(k), safe='[]_.-')}={quote(str(v), safe='[]_.,:-')}"
                         for k, v in (params or []))
        full = f"{url}{'&' if '?' in url else '?'}{query}" if query else url
        self.count += 1
        time.sleep(self.delay)
        started = time.time()
        try:
            r = self.session.request(method, full, timeout=self.timeout)
        except Exception as e:
            return {"url": full, "status": None, "ms": int((time.time() - started) * 1000),
                    "error": f"{type(e).__name__}: {e}"[:300]}
        result = {"url": full, "status": r.status_code, "ms": int((time.time() - started) * 1000),
                  "content_type": r.headers.get("Content-Type", ""), "bytes": len(r.content),
                  "allow": r.headers.get("Allow")}
        try:
            result["json"] = r.json()
        except ValueError:
            result["text"] = r.text[:300]
        if r.status_code >= 400 and "json" in result:
            result["error"] = json.dumps(result["json"], ensure_ascii=False)[:400]
        return result

    def peek(self, url: str, max_bytes: int = 8192) -> dict:
        """GET файла: только заголовки и первые max_bytes, тело дальше не читается и не сохраняется."""
        if self.count >= self.max_requests:
            return {"url": url, "status": None, "error": "лимит запросов (--max-requests) исчерпан"}
        self.count += 1
        time.sleep(self.delay)
        started = time.time()
        try:
            with self.session.get(url, timeout=self.timeout, stream=True) as r:
                head = r.raw.read(max_bytes, decode_content=True) or b""
                result = {"url": url, "status": r.status_code, "ms": int((time.time() - started) * 1000),
                          "content_type": r.headers.get("Content-Type", ""),
                          "size": r.headers.get("Content-Length"), "kind": sniff(head),
                          "filename_ext": disposition_ext(r.headers.get("Content-Disposition", ""))}
                if r.status_code >= 400 or result["kind"] in ("json", "html"):
                    result["text"] = head[:300].decode("utf-8", errors="replace")
                return result
        except Exception as e:
            return {"url": url, "status": None, "ms": int((time.time() - started) * 1000),
                    "error": f"{type(e).__name__}: {e}"[:300]}

    def close(self):
        self.parser.close()


MAGIC = [(b"%PDF", "pdf"), (b"\xff\xd8\xff", "jpeg"), (b"\x89PNG", "png"), (b"GIF8", "gif"),
         (b"PK\x03\x04", "zip/docx/xlsx"), (b"\x30\x82", "der (подпись/сертификат)"),
         (b"-----BEGIN", "pem"), (b"<?xml", "xml"), (b"RIFF", "riff/webp"), (b"II*\x00", "tiff"),
         (b"MM\x00*", "tiff"), (b"\xd0\xcf\x11\xe0", "doc/xls (старый Office)")]


def sniff(head: bytes) -> str:
    """Тип файла по первым байтам."""
    stripped = head.lstrip()
    for magic, kind in MAGIC:
        if head.startswith(magic) or stripped.startswith(magic):
            return kind
    if stripped[:1] in (b"{", b"["):
        return "json"
    if stripped[:15].lower().startswith((b"<!doctype", b"<html")):
        return "html"
    if re.match(rb"^[A-Za-z0-9+/=\r\n]{40,}$", stripped[:200]):
        return "base64 (подпись?)"
    return "другое" if head else "пусто"


def disposition_ext(header: str) -> str:
    """Только расширение имени файла: само имя может содержать ФИО или название организации."""
    match = re.search(r"filename\*?=(?:UTF-8'')?\"?([^\";]+)", header or "", re.I)
    if not match:
        return ""
    name = match.group(1)
    return name.rsplit(".", 1)[-1].lower() if "." in name else "(без расширения)"


def page(client, url, limit, offset=0, extra=None):
    return client.request(url, [("page[limit]", limit), ("page[offset]", offset), *(extra or [])])


# ---------------------------------------------------------------------------
# Разбор ответа
# ---------------------------------------------------------------------------
def records(resp) -> list:
    """Записи ответа: JSON:API ("data"), stencilapi ("results", плоские объекты) или голый список."""
    body = resp.get("json") if isinstance(resp.get("json"), dict) else {}
    if isinstance(body.get("results"), list):
        return body["results"]
    data = body.get("data")
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return [data]
    if isinstance(resp.get("json"), list):
        return resp["json"]
    return []


def total(resp):
    """Общее число записей: meta.pagination.count / meta.count / meta.total, иначе offset из links.last."""
    body = resp.get("json") if isinstance(resp.get("json"), dict) else {}
    meta = body.get("meta") or {}
    for value in ((meta.get("pagination") or {}).get("count"), meta.get("count"), meta.get("total"),
                  body.get("count")):
        if isinstance(value, int):
            return value
    last = (body.get("links") or {}).get("last")
    if isinstance(last, str):
        offset = parse_qs(urlparse(last).query).get("page[offset]")
        if offset and offset[0].isdigit():
            return f"last_offset={offset[0]}"
    return None


def ids(resp) -> list:
    return [str(r.get("id")) for r in records(resp) if isinstance(r, dict)]


def attr(record: dict, field: str):
    if field == "id":
        return record.get("id")
    attrs = record.get("attributes")
    return (attrs if isinstance(attrs, dict) else record).get(field)


def mask(key: str, value, raw: bool):
    if raw or value is None or isinstance(value, bool):
        return value
    if SENSITIVE.search(key or "") and isinstance(value, (str, int, float)):
        return f"<{type(value).__name__} len={len(str(value))}>"
    if isinstance(value, str) and len(value) > 80:
        return value[:80] + f"…(+{len(value) - 80})"
    return value


def _type(value) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, (int, float)):
        return "number"
    if isinstance(value, str):
        if re.match(r"^\d{4}-\d{2}-\d{2}T", value):
            return "datetime"
        if re.match(r"^\d{4}-\d{2}-\d{2}$", value):
            return "date"
        if re.match(r"^-?\d+(\.\d+)?$", value):
            return "numeric-string"
        return "string"
    return "array" if isinstance(value, list) else "object"


def field_schema(items: list, raw: bool) -> dict:
    """{путь: {types, filled, of, max_len, values|example}} по всем записям выборки."""
    stats = {}

    def walk(value, path, key):
        s = stats.setdefault(path, {"types": Counter(), "filled": 0, "seen": 0, "values": Counter(),
                                    "max_len": 0, "example": None})
        s["seen"] += 1
        s["types"][_type(value)] += 1
        if value in (None, "", [], {}):
            return
        s["filled"] += 1
        if isinstance(value, dict):
            for k, v in value.items():
                walk(v, f"{path}.{k}" if path else k, k)
        elif isinstance(value, list):
            for item in value[:20]:
                walk(item, f"{path}[]", key)
        else:
            s["max_len"] = max(s["max_len"], len(str(value)))
            s["values"][str(value)[:60]] += 1
            if s["example"] is None:
                s["example"] = mask(key, value, raw)

    for item in items:
        walk(item, "", "")
    out = {}
    for path, s in sorted(stats.items()):
        if not path:
            continue
        entry = {"types": dict(s["types"]), "filled": s["filled"], "of": s["seen"]}
        if s["max_len"]:
            entry["max_len"] = s["max_len"]
            leaf_key = re.split(r"[.\[]", path.replace("[]", ""))[-1]
            # Повторяющиеся значения похожи на справочник (статусы, типы) — показываем их
            if 1 < len(s["values"]) <= 12 and s["filled"] >= 5 and not SENSITIVE.search(leaf_key or ""):
                entry["values"] = dict(s["values"].most_common())
            else:
                entry["example"] = s["example"]
                entry["distinct"] = len(s["values"])
        out[path] = entry
    return out


def masked_record(value, raw: bool, key=""):
    if isinstance(value, dict):
        return {k: masked_record(v, raw, k) for k, v in value.items()}
    if isinstance(value, list):
        return [masked_record(v, raw, key) for v in value[:5]] + ([f"…(+{len(value) - 5})"] if len(value) > 5 else [])
    return mask(key, value, raw)


# ---------------------------------------------------------------------------
# Проверки
# ---------------------------------------------------------------------------
def summarize(resp) -> dict:
    out = {k: resp.get(k) for k in ("status", "ms", "bytes", "content_type", "error", "allow") if resp.get(k)}
    if resp.get("text") and resp.get("status") != 200:
        out["text"] = resp["text"][:200]
    out["rows"] = len(records(resp))
    out["total"] = total(resp)
    return out


def root_links(body, root_url: str) -> dict:
    """{имя: url} из ответа корня API любой формы: {"имя": url}, {"data": {...}}, списки, вложенные links.

    Имя — ключ, под которым лежит ссылка; для url/href/self/related внутри объекта — его name/type/id
    или ключ родителя. Относительные пути (/webapi/v1/x/) дополняются адресом портала.
    """
    found = {}

    def add(name, link):
        if not isinstance(link, str) or not API_PATH.match(link.split("?")[0]):
            return
        link = link if link.startswith("http") else HOST + link
        link = link.split("?")[0]
        link = link if link.endswith("/") else link + "/"
        if link.rstrip("/") == root_url.rstrip("/"):
            return
        name = str(name or link.rstrip("/").rsplit("/", 1)[-1])
        found.setdefault(("stencil_" if "/stencilapi/" in link else "") + name, link)

    def walk(value, parent_key):
        if isinstance(value, dict):
            label = value.get("name") or value.get("type") or value.get("id") or parent_key
            for key, item in value.items():
                if isinstance(item, str):
                    add(label if key in ("url", "href", "self", "related", "link") else key, item)
                else:
                    # обёртки JSON:API не именуют эндпоинт — имя берём у объекта выше
                    walk(item, label if key in ("links", "attributes", "data", "meta") else key)
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, str):
                    add(None, item)
                else:
                    walk(item, parent_key)

    walk(body, None)
    return found


def discover_roots(client, out_dir: Path) -> dict:
    found, results = {}, {}
    for url in API_ROOTS:
        resp = client.request(url)
        results[url] = summarize(resp)
        body = resp.get("json")
        if resp.get("status") == 200 and isinstance(body, (dict, list)):
            if isinstance(body, dict):
                results[url]["keys"] = list(body)[:80]
            # Ответ корня целиком — список эндпоинтов, личных данных в нём нет
            name = re.sub(r"[^a-z0-9]+", "_", url.replace(HOST, "").lower()).strip("_")
            path = out_dir / "roots" / f"{name}.json"
            path.parent.mkdir(exist_ok=True)
            path.write_text(json.dumps(body, ensure_ascii=False, indent=1), encoding="utf-8")
            results[url]["saved"] = str(path.relative_to(out_dir))
            links = root_links(body, url)
            results[url]["links_found"] = len(links)
            for key, link in links.items():
                found.setdefault(key, link)
    return {"roots": results, "found": found}


def probe_schemas(client, out_dir: Path) -> dict:
    results = {}
    for url in SCHEMA_URLS:
        resp = client.request(url)
        results[url] = summarize(resp)
        if resp.get("status") == 200 and ("json" in resp or "yaml" in resp.get("content_type", "")):
            name = re.sub(r"[^a-z0-9]+", "_", url.replace(HOST, "").lower()).strip("_") or "root"
            path = out_dir / "schemas" / f"{name}.json"
            path.parent.mkdir(exist_ok=True)
            path.write_text(json.dumps(resp.get("json", resp.get("text")), ensure_ascii=False, indent=1),
                            encoding="utf-8")
            results[url]["saved"] = str(path.relative_to(out_dir))
    return results


def probe_structure(client, name, url, sample, raw, out_dir: Path) -> dict:
    report = {"url": url}
    opts = client.request(url, method="OPTIONS")
    report["options"] = summarize(opts)
    if isinstance(opts.get("json"), dict):
        report["options"]["json"] = masked_record(opts["json"], raw)

    first = page(client, url, sample)
    report["list"] = summarize(first)
    body = first.get("json") if isinstance(first.get("json"), dict) else {}
    report["top_level_keys"] = list(body)
    report["meta"] = masked_record(body.get("meta"), raw)
    report["links"] = body.get("links")
    rows = records(first)
    if first.get("status") != 200 or not rows:
        return report

    report["record_keys"] = sorted({k for r in rows if isinstance(r, dict) for k in r})
    report["types"] = dict(Counter(r.get("type") for r in rows if isinstance(r, dict)))
    report["schema"] = field_schema(rows, raw)
    if body.get("included"):
        report["included_types"] = dict(Counter(i.get("type") for i in body["included"]))
        report["included_schema"] = field_schema(body["included"], raw)

    rels = {}
    for r in rows:
        for rel, value in (r.get("relationships") or {}).items():
            info = rels.setdefault(rel, {"data_kinds": Counter(), "data_types": Counter(), "related": None})
            data = (value or {}).get("data")
            info["data_kinds"]["list" if isinstance(data, list) else "null" if data is None else "object"] += 1
            for d in (data if isinstance(data, list) else [data] if data else []):
                info["data_types"][d.get("type")] += 1
            info["related"] = info["related"] or ((value or {}).get("links") or {}).get("related")
    report["relationships"] = {k: {**v, "data_kinds": dict(v["data_kinds"]), "data_types": dict(v["data_types"])}
                               for k, v in rels.items()}

    sample_path = out_dir / "samples" / f"{name}.json"
    sample_path.parent.mkdir(exist_ok=True)
    sample_path.write_text(json.dumps(masked_record(rows[0], raw), ensure_ascii=False, indent=1), encoding="utf-8")

    # Детальный ответ по одной записи: те же поля или больше?
    rec_id = rows[0].get("id")
    drows = []
    if rec_id is not None:
        detail = client.request(f"{url}{rec_id}/")
        report["detail"] = summarize(detail)
        drows = records(detail)
        if drows:
            list_paths, detail_paths = set(field_schema(rows[:1], True)), set(field_schema(drows, True))
            report["detail"]["only_in_detail"] = sorted(detail_paths - list_paths)[:100]
            report["detail"]["only_in_list"] = sorted(list_paths - detail_paths)[:100]

    # Куда ведут ссылки relationships (по одной записи на связь)
    related = {}
    for rel, info in rels.items():
        if info["related"]:
            resp = client.request(info["related"])
            rel_rows = records(resp)
            related[rel] = {**summarize(resp), "url": info["related"],
                            "types": dict(Counter(r.get("type") for r in rel_rows if isinstance(r, dict))),
                            "schema": field_schema(rel_rows[:10], raw) if rel_rows else None}
    report["related"] = related
    report["_rows"] = rows + (drows if rec_id is not None else [])
    return report


FILE_WORDS = re.compile(r"(file|photo|_dl\b|_dl/|download|print|thumbnail|/view/|\.(pdf|jpe?g|png|gif|xml|zip|"
                        r"docx?|xlsx?|sig|p7s|tiff?)$)", re.I)


def file_link_kind(link: str):
    """(вид, полный url) для ссылки на файл; None — не файл. Вид — шаблон адреса без конкретных id."""
    if not isinstance(link, str) or len(link) > 600 or " " in link.strip():
        return None
    url = link.strip()
    if url.startswith("http"):
        if not url.startswith(HOST):
            return None
    elif url.startswith("/"):
        url = HOST + url
    elif re.match(r"^(webapi|stencilapi|files|media)/", url):
        url = f"{HOST}/{url}"
    else:
        return None
    path, _, query = url[len(HOST):].partition("?")
    if not FILE_WORDS.search(path) and not FILE_WORDS.search(query):
        return None
    if re.match(r"^/(webapi|stencilapi)/v\d+/[\w-]+/$", path) and not query:
        return None  # список эндпоинта, не файл (у адресов скачивания вида .../sign_files_dl/ есть параметры)
    template = re.sub(r"/\d+(?=/|$)", "/{n}", path)  # только id-сегменты, не «v1»
    if path.startswith(("/files/", "/media/")):
        template = path.split("/")[1] + "/…" + (("." + path.rsplit(".", 1)[-1].lower()) if "." in path else "")
    params = dict(parse_qsl(query))
    # resource_name и file_field_name различают виды файлов (подпись, печатная форма, доверенность, …)
    named = [f"{k}={params[k]}" for k in ("resource_name", "file_field_name") if k in params]
    rest = sorted(k for k in params if k not in ("resource_name", "file_field_name"))
    return template + ("?" + "&".join(named + rest) if params else ""), url


def collect_file_links(rows: list, source: str, found: dict):
    """Добавляет в found по первому примеру каждого вида файловых ссылок из записей."""
    def walk(value):
        if isinstance(value, dict):
            for item in value.values():
                walk(item)
        elif isinstance(value, list):
            for item in value:
                walk(item)
        else:
            hit = file_link_kind(value)
            if hit and hit[0] not in found:
                found[hit[0]] = {"url": hit[1], "found_in": source}

    walk(rows)


def document_refs(rows: list) -> dict:
    """{тип: id} документов и фото из relationships (по первому id каждого типа)."""
    refs = {}
    for r in rows:
        for value in (r.get("relationships") or {}).values() if isinstance(r, dict) else []:
            data = (value or {}).get("data")
            for d in data if isinstance(data, list) else [data] if isinstance(data, dict) else []:
                t = str(d.get("type") or "")
                if d.get("id") and (t.startswith("document") or t in ("photo", "file", "claim")):
                    refs.setdefault(t, str(d["id"]))
    return refs


def probe_files(client, rows_by_endpoint: dict) -> dict:
    """Доступ к файлам: каждый вид ссылки из записей + документы из relationships."""
    links, documents = {}, {}
    for name, rows in rows_by_endpoint.items():
        collect_file_links(rows, name, links)
    refs = {}
    for name, rows in rows_by_endpoint.items():
        for t, i in document_refs(rows).items():
            refs.setdefault(t, (i, name))

    for t, (doc_id, source) in sorted(refs.items()):
        if t in ("photo", "file"):
            # у фото и файлов есть прямые адреса просмотра — проверяем их как ссылки
            for suffix in ("view/", "thumbnail/") if t == "photo" else ("view/",):
                hit = file_link_kind(f"{HOST}/webapi/v1/{t}/{doc_id}/{suffix}")
                if hit and hit[0] not in links:
                    links[hit[0]] = {"url": hit[1], "found_in": f"{source} (relationships)"}
            continue
        print(f"    документ {t}…", flush=True)
        resp = client.request(f"{HOST}/webapi/v1/{t}/{doc_id}/")
        entry = {**summarize(resp), "found_in": source, "id": doc_id}
        body = resp.get("json") if isinstance(resp.get("json"), dict) else {}
        data = body.get("data") if isinstance(body.get("data"), dict) else {}
        if data:
            entry["attributes"] = sorted((data.get("attributes") or {}).keys())[:60]
            inner = {}
            collect_file_links([body], f"{t}/{doc_id}", inner)
            entry["file_links"] = sorted(inner)
            for kind, info in inner.items():
                links.setdefault(kind, info)
        documents[t] = entry

    checked = {}
    for kind, info in sorted(links.items()):
        print(f"    файл {kind}…", flush=True)
        checked[kind] = {**client.peek(info["url"]), "found_in": info["found_in"]}
    return {"links": checked, "documents": documents}


def verdict_sort(resp, base_ids, field):
    if resp.get("status") != 200:
        return "error"
    rows = records(resp)
    values = [attr(r, field) for r in rows]
    if not rows or any(v is None for v in values):
        return "no rows / empty values"
    keys = [float(v) if re.match(r"^-?\d+(\.\d+)?$", str(v)) else str(v) for v in values]
    order = "asc" if keys == sorted(keys) else "desc" if keys == sorted(keys, reverse=True) else "unordered"
    same = ids(resp) == base_ids
    return f"{order}{' (= без sort)' if same else ''}"


def verdict_filter(resp, base_total, base_ids, check):
    if resp.get("status") != 200:
        return "error"
    rows = records(resp)
    count = total(resp)
    if count == base_total and ids(resp) == base_ids:
        return "ignored"
    satisfied = all(check(r) for r in rows) if rows else None
    if count != base_total and satisfied in (True, None):
        return "works"
    return f"unclear (total {base_total} -> {count}, все строки подходят: {satisfied})"


def probe_params(client, url, rows, limit) -> dict:
    """Какие параметры сервер применяет: sort, фильтры, include, fields, пагинация, время от limit."""
    base = page(client, url, limit)
    base_ids, base_total = ids(base), total(base)
    out = {"baseline": {**summarize(base), "ids": base_ids}}
    attrs = {k for r in rows for k in (r.get("attributes") if isinstance(r.get("attributes"), dict) else r)}
    date_fields = sorted(a for a in attrs if DATE_FIELD.search(a))
    date_fields = sorted(date_fields, key=lambda f: (f != "dt_update", f != "dt_create", f))[:5]
    status_field = next((f for f in ("status_label", "status", "state") if f in attrs), None)

    sorts = {}
    for field in ["id", *date_fields, *([status_field] if status_field else [])]:
        for value in (field, f"-{field}"):
            resp = page(client, url, limit, extra=[("sort", value)])
            sorts[value] = {**summarize(resp), "verdict": verdict_sort(resp, base_ids, field)}
    out["sort"] = sorts

    threshold = (date.today() - timedelta(days=30)).isoformat()
    filters = {}
    for field in date_fields[:3]:
        def check(r, f=field):
            return str(attr(r, f) or "")[:10] >= threshold
        for key in (f"filter[{field}__gte]", f"{field}__gte", f"filter[{field}][gte]",
                    f"filter[{field}.gte]", f"{field}_from"):
            resp = page(client, url, limit, extra=[(key, threshold)])
            filters[f"{key}={threshold}"] = {**summarize(resp),
                                             "verdict": verdict_filter(resp, base_total, base_ids, check)}
    eq_fields = [f for f in (status_field, "is_active") if f and f in attrs]
    for field in eq_fields:
        values = Counter(str(attr(r, field)).lower() for r in rows if attr(r, field) is not None)
        if not values:
            continue
        value = values.most_common()[-1][0]  # редкое значение: фильтр заметнее сократит выборку
        def check(r, f=field, v=value):
            return str(attr(r, f)).lower() == v
        for key in (f"filter[{field}]", field, f"filter[{field}][eq]", f"filter[{field}__in]"):
            resp = page(client, url, limit, extra=[(key, value)])
            filters[f"{key}={value}"] = {**summarize(resp),
                                         "verdict": verdict_filter(resp, base_total, base_ids, check)}
    if rows and rows[0].get("id") is not None:
        rec_id = str(rows[0]["id"])
        for key in ("filter[id]", "id", "filter[id__in]", "id__gt"):
            resp = page(client, url, limit, extra=[(key, rec_id)])
            filters[f"{key}={rec_id}"] = {**summarize(resp), "returned_ids": ids(resp)[:5]}
    for rel, value in (rows[0].get("relationships") or {}).items() if rows else []:
        data = (value or {}).get("data")
        if isinstance(data, dict) and data.get("id"):
            for key in (f"filter[{rel}]", rel, f"{rel}_id", f"filter[{rel}.id]"):
                resp = page(client, url, limit, extra=[(key, data["id"])])
                filters[f"{key}={data['id']}"] = {**summarize(resp), "returned_ids": ids(resp)[:5]}
            break
    out["filter"] = filters

    rel_names = list((rows[0].get("relationships") or {}).keys())[:3] if rows else []
    includes = {}
    for rel in rel_names:
        resp = page(client, url, limit, extra=[("include", rel)])
        body = resp.get("json") if isinstance(resp.get("json"), dict) else {}
        includes[rel] = {**summarize(resp), "included": dict(Counter(i.get("type") for i in body.get("included") or []))}
    out["include"] = includes

    if rows and rows[0].get("type") and isinstance(rows[0].get("attributes"), dict):
        first_attr = sorted(rows[0]["attributes"])[0]
        resp = page(client, url, limit, extra=[(f"fields[{rows[0]['type']}]", first_attr)])
        kept = {k for r in records(resp) for k in (r.get("attributes") or {})}
        out["sparse_fields"] = {**summarize(resp), "param": f"fields[{rows[0]['type']}]={first_attr}",
                                "attributes_returned": sorted(kept)[:20],
                                "faster_than_baseline": (resp.get("ms") or 0) < (base.get("ms") or 0)}

    paging = {}
    for label, params in (("page[size]+page[number]", [("page[size]", limit), ("page[number]", 2)]),
                          ("limit+offset", [("limit", limit), ("offset", limit)]),
                          ("page=2", [("page", 2)])):
        resp = client.request(url, params)
        paging[label] = {**summarize(resp), "ids": ids(resp)[:5]}
    second = page(client, url, limit, offset=limit)
    paging["page[offset]=limit (эталон 2-й страницы)"] = {**summarize(second), "ids": ids(second)[:5]}
    out["paging"] = paging

    timing = {}
    for size in (1, 20, 50):
        resp = page(client, url, size)
        timing[size] = {k: resp.get(k) for k in ("status", "ms", "bytes")} | {"rows": len(records(resp))}
    out["timing_by_limit"] = timing
    return out


# ---------------------------------------------------------------------------
# Отчёт
# ---------------------------------------------------------------------------
def write_markdown(report: dict, path: Path):
    lines = [f"# API ФГИС ОПВК: структура ({report['started']})", "",
             f"Запросов: {report['requests']}, маскирование: {'нет' if report['raw'] else 'да'}", ""]
    lines += ["## Корни API и схема", "", "| URL | статус | ключи |", "|---|---|---|"]
    for url, r in report["discovery"]["roots"].items():
        extra = f" → ссылок {r['links_found']}, ответ: {r['saved']}" if r.get("saved") else ""
        lines.append(f"| {url} | {r.get('status')} | {', '.join(r.get('keys', [])[:15])}{extra} |")
    for url, r in report["schemas"].items():
        if r.get("status") == 200:
            lines.append(f"| {url} | 200 | схема: {r.get('saved', r.get('content_type'))} |")
    if report["discovery"]["found"]:
        lines += ["", f"Найдено в корнях ({len(report['discovery']['found'])}): "
                  + ", ".join(sorted(report["discovery"]["found"]))]

    files = report.get("files") or {}
    if files.get("links") or files.get("documents"):
        lines += ["", "## Доступ к файлам", "", "Читались только первые 8 КБ каждого файла.", "",
                  "| вид ссылки | где найдена | статус | тип по содержимому | Content-Type | размер | имя файла |",
                  "|---|---|---|---|---|---|---|"]
        for kind, f in files.get("links", {}).items():
            size = f.get("size")
            lines.append(f"| `{kind}` | {f.get('found_in')} | {f.get('status')} {f.get('error', '')[:60]} | "
                         f"{f.get('kind', '')} | {f.get('content_type', '')} | "
                         f"{(str(round(int(size) / 1024)) + ' КБ') if size and str(size).isdigit() else ''} | "
                         f"{'.' + f['filename_ext'] if f.get('filename_ext') else ''} |")
        if files.get("documents"):
            lines += ["", "Документы из relationships (детальная запись /webapi/v1/<тип>/<id>/):", "",
                      "| тип | где найден | статус | ссылки на файлы внутри | поля |", "|---|---|---|---|---|"]
            for t, d in files["documents"].items():
                lines.append(f"| `{t}` | {d.get('found_in')} | {d.get('status')} | "
                             f"{', '.join(f'`{k}`' for k in d.get('file_links', [])) or '—'} | "
                             f"{', '.join(d.get('attributes', [])[:12])} |")

    lines += ["", "## Эндпоинты", "", "| имя | статус | мс | записей всего | полей | связи |", "|---|---|---|---|---|---|"]
    for name, e in report["endpoints"].items():
        lst = e.get("list", {})
        lines.append(f"| {name} | {lst.get('status')} | {lst.get('ms')} | {lst.get('total')} | "
                     f"{len(e.get('schema') or {})} | {', '.join(e.get('relationships') or {})} |")

    for name, e in report["endpoints"].items():
        if not e.get("schema"):
            continue
        lines += ["", f"### {name}", "", f"`{e['url']}` — типы: {e.get('types')}, ключи записи: {e.get('record_keys')}"]
        if e.get("detail"):
            d = e["detail"]
            lines.append(f"Детально /id/: {d.get('status')}, только в детальном: {d.get('only_in_detail', [])[:30]}")
        lines += ["", "| поле | типы | заполнено | значения / пример |", "|---|---|---|---|"]
        for field, s in e["schema"].items():
            shown = s.get("values") or s.get("example")
            shown = json.dumps(shown, ensure_ascii=False)[:120].replace("|", "\\|") if shown is not None else ""
            lines.append(f"| `{field}` | {', '.join(s['types'])} | {s['filled']}/{s['of']} | {shown} |")
        if e.get("related"):
            lines += ["", "Связи (links.related):"]
            for rel, r in e["related"].items():
                lines.append(f"- `{rel}` → {r.get('status')}, {r.get('rows')} зап., типы {r.get('types')}, "
                             f"полей {len(r.get('schema') or {})}")
        params = e.get("params")
        if params:
            lines += ["", "Параметры (вердикт сервера):", ""]
            for value, r in params["sort"].items():
                lines.append(f"- sort=`{value}` → {r.get('status')} {r.get('verdict')}")
            for value, r in params["filter"].items():
                lines.append(f"- `{value}` → {r.get('status')} total {r.get('total')} "
                             f"{r.get('verdict', '')} {r.get('returned_ids', '')} {r.get('error', '')[:80] if r.get('error') else ''}")
            for value, r in params["include"].items():
                lines.append(f"- include=`{value}` → {r.get('status')} included {r.get('included')}")
            if params.get("sparse_fields"):
                sf = params["sparse_fields"]
                lines.append(f"- `{sf['param']}` → {sf.get('status')}, атрибуты {sf['attributes_returned'][:5]}, "
                             f"{sf.get('ms')} мс (без: {params['baseline'].get('ms')})")
            for label, r in params["paging"].items():
                lines.append(f"- пагинация {label} → {r.get('status')} ids {r.get('ids')}")
            lines.append(f"- время от page[limit]: " + ", ".join(
                f"{k}: {v.get('ms')} мс" for k, v in params["timing_by_limit"].items()))
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="Исследование структуры API ФГИС ОПВК (только чтение).")
    ap.add_argument("--endpoint", action="append", default=[], metavar="ИМЯ=URL",
                    help="Дополнительный эндпоинт; можно несколько раз.")
    ap.add_argument("--only", action="append", default=[], metavar="ИМЯ",
                    help="Проверять только эти эндпоинты (и не искать новые в корнях API).")
    ap.add_argument("--deep", action="append", default=[], metavar="ИМЯ",
                    help=f"Проверить параметры (sort, фильтры, ...). По умолчанию: {', '.join(DEFAULT_DEEP)}.")
    ap.add_argument("--sample", type=int, default=20, help="Записей в выборке для структуры (по умолчанию 20).")
    ap.add_argument("--limit", type=int, default=5, help="page[limit] при проверке параметров (по умолчанию 5).")
    ap.add_argument("--delay", type=float, default=0.5, help="Пауза между запросами, с.")
    ap.add_argument("--timeout", type=float, default=90, help="Таймаут запроса, с.")
    ap.add_argument("--max-requests", type=int, default=1500, help="Предел запросов за запуск.")
    ap.add_argument("--raw", action="store_true", help="Не маскировать примеры значений.")
    ap.add_argument("--no-files", action="store_true", help="Не проверять доступ к файлам.")
    args = ap.parse_args()

    endpoints = dict(ENDPOINTS)
    for item in args.endpoint:
        name, _, url = item.partition("=")
        if not url.startswith("http"):
            ap.error(f"--endpoint ждёт ИМЯ=URL, получено {item!r}")
        endpoints[name] = url if url.endswith("/") else url + "/"
    deep = set(args.deep or DEFAULT_DEEP)

    client = Client(args.delay, args.timeout, args.max_requests)
    if not client.login_ok():
        client.close()
        print("Сохранённый вход в ФГИС недействителен — выполните: python login_fgis.py", flush=True)
        return EXIT_AUTH_REQUIRED

    started = datetime.now()
    out_dir = ROOT / "reports" / f"api_probe_{started:%Y%m%d_%H%M%S}"
    out_dir.mkdir(parents=True)
    report = {"started": started.isoformat(timespec="seconds"), "raw": args.raw, "endpoints": {}}
    try:
        if args.only:
            endpoints = {k: v for k, v in endpoints.items() if k in args.only}
            report["discovery"], report["schemas"] = {"roots": {}, "found": {}}, {}
        else:
            print("Корни API и адреса схемы...", flush=True)
            report["discovery"] = discover_roots(client, out_dir)
            report["schemas"] = probe_schemas(client, out_dir)
            known = {u.rstrip("/") for u in endpoints.values()}
            for name, url in list(report["discovery"]["found"].items())[:MAX_DISCOVERED]:
                if url.rstrip("/") not in known:
                    endpoints.setdefault(name, url)

        discovered = set(report["discovery"]["found"]) - set(ENDPOINTS)
        rows_by_endpoint = {}
        for name, url in endpoints.items():
            print(f"[{client.count} запр.] {name}: {url}", flush=True)
            sample = min(args.sample, 5) if name in discovered and name not in deep else args.sample
            entry = probe_structure(client, name, url, sample, args.raw, out_dir)
            rows_by_endpoint[name] = entry.pop("_rows", [])
            if name in deep and entry.get("schema"):
                print(f"    параметры {name}...", flush=True)
                rows = records(page(client, url, args.sample))
                entry["params"] = probe_params(client, url, rows, args.limit)
            report["endpoints"][name] = entry

        if not args.no_files:
            print(f"[{client.count} запр.] Доступ к файлам…", flush=True)
            report["files"] = probe_files(client, rows_by_endpoint)
    except KeyboardInterrupt:
        print("Прервано — сохраняю то, что успели собрать.", flush=True)
    finally:
        client.close()
        report["requests"] = client.count
        report.setdefault("discovery", {"roots": {}, "found": {}})
        report.setdefault("schemas", {})
        (out_dir / "report.json").write_text(json.dumps(report, ensure_ascii=False, indent=1, default=str),
                                            encoding="utf-8")
        write_markdown(report, out_dir / "report.md")
        print(f"Готово: {out_dir / 'report.md'} (подробно: report.json, примеры: samples/)", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())

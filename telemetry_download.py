"""
Выгрузка телеметрии (история точек ТС) из ФГИС ОПВК — на Python, без opvk-export.exe.

Только чтение: те же GET-запросы, что делает портал, с сохранённым входом
(gisopvk_webapi_cookies.json, см. login_fgis.py или кнопку «Войти в ФГИС» в UI):

    /webapi/v1/vehicle/              — список ТС
    /webapi/v1/tracking/telemetry/   — точки трекера за интервал (Unix-время)

    python telemetry_download.py --from 2025-01-01 --out D:\\telemetry_full --dry-run   # только подсчёт
    python telemetry_download.py --from 2025-01-01 --out D:\\telemetry_full             # выгрузка
    python telemetry_download.py --from 2026-08-24 --to 2026-09-07 --vehicles 10469    # одна машина

Как в opvk-export: окна по суткам (UTC), насыщенное окно (>= --split-at точек) делится пополам до 10 минут
(на больших интервалах сервер прореживает трек), повторы точек убираются, лимит --rps на все потоки,
повторы при 429/5xx. Повторный запуск докачивает только недостающее (done.txt — тот же формат, что у
opvk-export, поэтому его папки тоже можно докачивать).

Результат в папке --out:
    telemetry_<время запуска>.csv.gz  — точки этого запуска: vehicle_id, plate, tracker_id, date_time,
                                         lat, lon, speed, course, sat, telemetry_state
    summary.csv   — по каждой машине: точек, первая и последняя (дополняется каждым запуском;
                    --rebuild-summary пересчитывает по всем частям, включая telemetry.csv от opvk-export)
    vehicles.csv  — все ТС: трекер, гос. номер, последняя телеметрия, статус
    done.txt      — что уже скачано
Часть, оборванная остановкой, при следующем запуске перепаковывается (всё записанное до обрыва сохраняется).

Код выхода: 0 — готово, 3 — нужен вход в ФГИС (вход истёк — запустите снова после входа, продолжит),
4 — часть окон не скачалась (повторятся при следующем запуске), 1 — ошибка.
"""

import argparse
import csv
import gzip
import json
import logging
import random
import sys
import threading
import time
import zlib
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import requests

from gisopvkwebapiparser import GisopvkWebApiParser
from pipeline_common import EXIT_AUTH_REQUIRED, EXIT_PARTIAL

logger = logging.getLogger("telemetry")

BASE = "https://gisopvk.ru"
VEHICLE_URL = f"{BASE}/webapi/v1/vehicle/"
TELEMETRY_URL = f"{BASE}/webapi/v1/tracking/telemetry/"
POINT_FIELDS = ["vehicle_id", "plate", "tracker_id", "date_time", "lat", "lon", "speed", "course", "sat",
                "telemetry_state"]
VEHICLE_FIELDS = ["vehicle_id", "tracker_id", "imei", "plate", "brand", "model", "telemetry_last_date",
                  "telemetry_status"]
MIN_SPLIT_SECONDS = 600
PROGRESS_EVERY = 50


class AuthError(Exception):
    """401/403 от API: сессия истекла."""


class BlockedError(Exception):
    """403 не от API (HTML-страница защиты): дело в заголовках запроса, не в сессии."""


# ---------------------------------------------------------------------------
# HTTP: общий лимит частоты, повторы, своя сессия на поток
# ---------------------------------------------------------------------------
class Api:
    def __init__(self, base_session: requests.Session, rps: float, timeout: float = 120):
        self.base = base_session
        self.interval = 1.0 / rps
        self.timeout = timeout
        self.lock = threading.Lock()
        self.next_slot = 0.0
        self.local = threading.local()
        self.stop = threading.Event()  # выставляется при ошибке входа — остальные потоки не стучатся зря

    def session(self) -> requests.Session:
        s = getattr(self.local, "session", None)
        if s is None:
            s = requests.Session()
            s.verify = self.base.verify
            s.headers.update(self.base.headers)
            s.cookies.update(self.base.cookies)
            self.local.session = s
        return s

    def _wait_slot(self):
        with self.lock:
            now = time.monotonic()
            slot = max(now, self.next_slot)
            self.next_slot = slot + self.interval
        time.sleep(max(0.0, slot - time.monotonic()))

    def get_json(self, url: str, params: dict):
        last = None
        for attempt in range(6):
            if self.stop.is_set():
                raise AuthError("остановлено")
            if attempt:
                time.sleep(2 ** (attempt - 1) + random.random() / 2)  # 1, 2, 4, 8, 16 с + джиттер
            self._wait_slot()
            try:
                r = self.session().get(url, params=params, timeout=self.timeout)
            except requests.RequestException as e:
                last = e
                continue
            body = r.content
            if r.status_code == 403 and not body.lstrip().startswith(b"{"):
                raise BlockedError("запрос заблокирован защитой портала (403, не ответ API)")
            if r.status_code in (401, 403):
                raise AuthError(f"HTTP {r.status_code}")
            if r.status_code == 429 or r.status_code >= 500:
                last = f"HTTP {r.status_code}"
                continue
            if r.status_code != 200:
                raise RuntimeError(f"HTTP {r.status_code}: {body[:200]!r}")
            return json.loads(body)
        raise RuntimeError(f"после повторов: {last}")


# ---------------------------------------------------------------------------
# Данные
# ---------------------------------------------------------------------------
def tracker_id(attrs: dict) -> str:
    if attrs.get("tracker_id"):
        return str(attrs["tracker_id"])
    port = attrs.get("telemetry_port")
    if attrs.get("imei") and isinstance(port, (int, float)):
        return f"{attrs['imei']}.{int(port)}"
    return ""


def list_vehicles(api: Api) -> list:
    vehicles, offset = [], 0
    while True:
        body = api.get_json(VEHICLE_URL, {"page[offset]": offset, "page[limit]": 200})
        rows = body.get("data") or []
        for d in rows:
            a = d.get("attributes") or {}
            vehicles.append({"vehicle_id": str(d["id"]), "tracker_id": tracker_id(a), "imei": a.get("imei") or "",
                             "plate": a.get("auto_gnum") or "", "brand": a.get("auto_brand") or "",
                             "model": a.get("auto_model") or "",
                             "telemetry_last_date": a.get("telemetry_last_date") or "",
                             "telemetry_status": a.get("telemetry_status") or ""})
        offset += len(rows)
        count = ((body.get("meta") or {}).get("pagination") or {}).get("count") or 0
        if not rows or offset >= count:
            return vehicles


def point_list(body) -> list:
    """Точки: голый массив или обёртка {"data": [...]} / {"results": [...]}."""
    if isinstance(body, list):
        return body
    if isinstance(body, dict):
        return body.get("data") if body.get("data") is not None else (body.get("results") or [])
    return []


def num(value) -> str:
    """Число как в opvk-export (Go 'f', -1): 12 -> '12', 55.1234 -> '55.1234', None -> ''."""
    if value is None or value == "":
        return ""
    try:
        f = float(value)
    except (TypeError, ValueError):
        return str(value)
    text = repr(f)
    return text[:-2] if text.endswith(".0") else text


def dedup(points: list) -> list:
    """Убирает повторы одной точки (время + координаты)."""
    seen, out = set(), []
    for p in points:
        key = (p.get("date_time"), num(p.get("lat")), num(p.get("lon")))
        if key not in seen:
            seen.add(key)
            out.append(p)
    return out


def fetch_range(api: Api, vehicle: dict, start: int, end: int, split_at: int) -> list:
    """Точки за [start, end] (Unix, включительно); насыщенное окно делится пополам до 10 минут."""
    pts = point_list(api.get_json(TELEMETRY_URL, {
        "tracker_id_set": vehicle["tracker_id"], "vehicle_id": vehicle["vehicle_id"],
        "timestamp_from": start, "timestamp_to": end, "query": ""}))
    if len(pts) >= split_at and end - start > MIN_SPLIT_SECONDS:
        mid = start + (end - start) // 2
        return dedup(fetch_range(api, vehicle, start, mid, split_at)
                     + fetch_range(api, vehicle, mid + 1, end, split_at))
    return dedup(pts)


def job_key(vehicle_id: str, start: int, end: int) -> str:
    return f"{vehicle_id}|{start}|{end}"


def make_jobs(vehicles: list, day_from: date, day_to: date, window: timedelta, done: set) -> list:
    """Окна [s, e-1] по суткам UTC от day_from до конца day_to, кроме уже скачанных."""
    start = datetime(day_from.year, day_from.month, day_from.day, tzinfo=timezone.utc)
    stop = datetime(day_to.year, day_to.month, day_to.day, tzinfo=timezone.utc) + timedelta(days=1)
    jobs = []
    for v in vehicles:
        s = start
        while s < stop:
            e = min(s + window, stop)
            key = job_key(v["vehicle_id"], int(s.timestamp()), int(e.timestamp()) - 1)
            if key not in done:
                jobs.append((v, int(s.timestamp()), int(e.timestamp()) - 1))
            s = e
    return jobs


# ---------------------------------------------------------------------------
# Файлы
# ---------------------------------------------------------------------------
def gz_is_complete(path: Path) -> bool:
    try:
        with gzip.open(path, "rb") as f:
            while f.read(1 << 20):
                pass
        return True
    except (EOFError, OSError, zlib.error):
        return False


def salvage_gz(src: Path, dst: Path) -> int:
    """Копирует в dst (новый gzip) всё, что читается из оборванного src, без недописанной строки.

    Читает потоком: часть может быть большой. Возвращает число строк.
    """
    lines, tail = 0, b""
    with open(src, "rb") as f, gzip.open(dst, "wb") as out:
        d = zlib.decompressobj(16 + zlib.MAX_WBITS)
        while True:
            chunk = f.read(1 << 20)
            if not chunk:
                break
            while chunk:
                try:
                    data = d.decompress(chunk)
                except zlib.error:
                    chunk = b""
                    break
                if d.eof:  # конец члена gzip — дальше может идти следующий
                    chunk = d.unused_data
                    d = zlib.decompressobj(16 + zlib.MAX_WBITS)
                else:
                    chunk = b""
                data = tail + data
                cut = data.rfind(b"\n") + 1
                out.write(data[:cut])
                lines += data[:cut].count(b"\n")
                tail = data[cut:]
    return lines  # tail — оборванная строка: её окна нет в done.txt, оно скачается заново


def repair_parts(out: Path) -> list:
    """Перепаковывает части, оборванные остановкой процесса. Возвращает их имена."""
    repaired = []
    for part in sorted(out.glob("telemetry_*.csv.gz")):
        if gz_is_complete(part):
            continue
        tmp = part.with_suffix(".tmp")
        salvage_gz(part, tmp)
        tmp.replace(part)
        repaired.append(part.name)
    return repaired


def write_vehicles(path: Path, vehicles: list):
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=VEHICLE_FIELDS)
        w.writeheader()
        w.writerows(vehicles)


def point_files(out: Path) -> list:
    files = sorted(out.glob("telemetry_*.csv.gz"))
    plain = out / "telemetry.csv"  # от opvk-export, если докачиваем его папку
    return files + ([plain] if plain.exists() else [])


SUMMARY_FIELDS = ["vehicle_id", "plate", "tracker_id", "points", "first", "last"]


def add_point(stats: dict, row: list):
    s = stats.get(row[0])
    if s is None:
        stats[row[0]] = {"plate": row[1], "tracker_id": row[2], "points": 1, "first": row[3], "last": row[3]}
        return
    s["points"] += 1
    if row[3] < s["first"]:
        s["first"] = row[3]
    if row[3] > s["last"]:
        s["last"] = row[3]


def merge_stats(total: dict, part: dict):
    for vid, s in part.items():
        t = total.get(vid)
        if t is None:
            total[vid] = dict(s)
        else:
            t["points"] += s["points"]
            t["first"], t["last"] = min(t["first"], s["first"]), max(t["last"], s["last"])


def read_summary(out: Path) -> dict:
    path = out / "summary.csv"
    if not path.exists():
        return {}
    with open(path, encoding="utf-8", newline="") as f:
        return {r["vehicle_id"]: {"plate": r["plate"], "tracker_id": r["tracker_id"], "points": int(r["points"]),
                                  "first": r["first"], "last": r["last"]} for r in csv.DictReader(f)}


def write_summary(out: Path, stats: dict):
    tmp = out / "summary.tmp"
    with open(tmp, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(SUMMARY_FIELDS)
        for vid in sorted(stats, key=lambda x: (len(x), x)):
            s = stats[vid]
            w.writerow([vid, s["plate"], s["tracker_id"], s["points"], s["first"], s["last"]])
    tmp.replace(out / "summary.csv")


def rebuild_summary(out: Path) -> dict:
    """Сводка заново по всем частям (потоком)."""
    stats = {}
    for path in point_files(out):
        opener = gzip.open if path.suffix == ".gz" else open
        with opener(path, "rt", encoding="utf-8", newline="") as f:
            try:
                for row in csv.reader(f):
                    if len(row) >= 4 and row[0] != "vehicle_id":
                        add_point(stats, row)
            except (EOFError, zlib.error):
                logger.warning(f"{path.name} оборван — учтено то, что читается")
    write_summary(out, stats)
    return stats


# ---------------------------------------------------------------------------
# Запуск
# ---------------------------------------------------------------------------
def run(api: Api, args) -> int:
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    repaired = repair_parts(out)
    if repaired:
        logger.info(f"Перепакованы части, оборванные прошлой остановкой: {repaired}")
        args.rebuild_summary = True  # строки из оборванной части в сводку не попали
    if args.rebuild_summary:
        logger.info(f"Сводка пересчитана по всем частям: машин {len(rebuild_summary(out))}")

    vehicles = list_vehicles(api)
    write_vehicles(out / "vehicles.csv", vehicles)
    only = {v.strip() for v in args.vehicles.split(",")} if args.vehicles else None
    selected = [v for v in vehicles if v["tracker_id"]
                and (v["vehicle_id"] in only if only else (args.all_vehicles or v["telemetry_last_date"]))]

    done_path = out / "done.txt"
    done = set(done_path.read_text(encoding="utf-8").split()) if done_path.exists() else set()
    jobs = make_jobs(selected, args.day_from, args.day_to, timedelta(hours=args.window_hours), done)
    eta_h = len(jobs) / args.rps / 3600
    logger.info(f"ТС в ФГИС: {len(vehicles)}, к выгрузке: {len(selected)}, заданий: {len(jobs)} "
                f"(уже готово: {len(done)}); не меньше {eta_h:.1f} ч при {args.rps} запр/с "
                f"(насыщенные окна дробятся — запросов будет больше)")
    if args.dry_run or not jobs:
        logger.info(f"Список ТС: {out / 'vehicles.csv'}")
        return 0

    part = out / f"telemetry_{datetime.now():%Y%m%d_%H%M%S}.csv.gz"
    failed = finished = points = 0
    auth_error = None
    run_stats = {}
    pending_jobs = iter(jobs)
    with gzip.open(part, "wt", encoding="utf-8", newline="") as gz, open(done_path, "a", encoding="utf-8") as df:
        writer = csv.writer(gz)
        writer.writerow(POINT_FIELDS)
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            in_flight = {}

            def submit_more():
                # в очереди не больше нескольких заданий на поток: окон бывают сотни тысяч
                while len(in_flight) < args.workers * 3 and not api.stop.is_set():
                    job = next(pending_jobs, None)
                    if job is None:
                        return
                    in_flight[pool.submit(fetch_range, api, *job, args.split_at)] = job

            try:
                submit_more()
                while in_flight:
                    ready, _ = wait(in_flight, return_when=FIRST_COMPLETED)
                    for fut in ready:
                        v, s, e = in_flight.pop(fut)
                        try:
                            pts = fut.result()
                        except (AuthError, BlockedError) as err:
                            auth_error = auth_error or err
                            api.stop.set()
                            failed += 1
                            continue
                        except Exception as err:
                            if not api.stop.is_set():
                                logger.warning(f"ТС {v['vehicle_id']} ({v['plate']}) "
                                               f"{datetime.fromtimestamp(s, timezone.utc):%Y-%m-%d}: {err} — "
                                               f"повторится при следующем запуске")
                            failed += 1
                            continue
                        for p in pts:
                            row = [v["vehicle_id"], v["plate"], v["tracker_id"], p.get("date_time") or "",
                                   num(p.get("lat")), num(p.get("lon")), num(p.get("speed")),
                                   num(p.get("course")), num(p.get("sat")), p.get("telemetry_state") or ""]
                            writer.writerow(row)
                            add_point(run_stats, row)
                        gz.flush()  # точки на диске до отметки в done.txt
                        df.write(job_key(v["vehicle_id"], s, e) + "\n")
                        df.flush()
                        finished += 1
                        points += len(pts)
                        if finished % PROGRESS_EVERY == 0 or finished == len(jobs):
                            logger.info(f"готово {finished}/{len(jobs)} заданий, точек в этом запуске: {points}")
                    submit_more()
            except KeyboardInterrupt:
                api.stop.set()
                for f in in_flight:
                    f.cancel()
                logger.warning("Прервано — уже скачанное сохранено, повторный запуск продолжит")
                raise
    if points == 0 and part.exists():
        part.unlink()  # пустая часть (только заголовок) не нужна

    stats = read_summary(out)
    merge_stats(stats, run_stats)
    write_summary(out, stats)
    logger.info(f"Итого: точек {points}, заданий с ошибкой {failed}; машин с точками всего {len(stats)}; "
                f"папка {out}")
    if auth_error:
        logger.error(f"Остановлено: {auth_error}. Нужен вход в ФГИС (python login_fgis.py) — "
                     f"затем тот же запуск продолжит с места остановки.")
        return EXIT_AUTH_REQUIRED
    return EXIT_PARTIAL if failed else 0


def parse_day(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def main() -> int:
    ap = argparse.ArgumentParser(description="Выгрузка телеметрии ТС из ФГИС ОПВК (только чтение).")
    ap.add_argument("--from", dest="day_from", type=parse_day, required=True, help="Начало, YYYY-MM-DD.")
    ap.add_argument("--to", dest="day_to", type=parse_day, default=date.today(),
                    help="Конец включительно, YYYY-MM-DD (по умолчанию сегодня).")
    ap.add_argument("--out", default="telemetry", help="Папка результата.")
    ap.add_argument("--dry-run", action="store_true", help="Только список ТС и число заданий.")
    ap.add_argument("--all-vehicles", action="store_true", help="И ТС, у которых никогда не было телеметрии.")
    ap.add_argument("--vehicles", default="", help="Только эти vehicle_id через запятую.")
    ap.add_argument("--workers", type=int, default=4, help="Параллельных запросов (по умолчанию 4).")
    ap.add_argument("--rps", type=float, default=5, help="Запросов в секунду, всего (по умолчанию 5).")
    ap.add_argument("--window-hours", type=int, default=24, help="Окно запроса, часов (по умолчанию 24).")
    ap.add_argument("--rebuild-summary", action="store_true", help="Пересчитать summary.csv по всем частям.")
    ap.add_argument("--split-at", type=int, default=3000,
                    help="Если в ответе столько точек — делить окно пополам, до 10 минут (по умолчанию 3000).")
    args = ap.parse_args()
    if args.day_from > args.day_to:
        ap.error("--from позже --to")

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", force=True)
    parser = GisopvkWebApiParser(base_url=VEHICLE_URL)
    try:
        if not (parser._load_cookies_from_file() and parser.test_auth()):
            logger.error("Сохранённый вход в ФГИС недействителен — выполните: python login_fgis.py")
            return EXIT_AUTH_REQUIRED
        return run(Api(parser.session, args.rps), args)
    except (AuthError, BlockedError) as e:
        logger.error(f"Остановлено: {e}. Войдите в ФГИС и запустите снова — продолжит с места остановки.")
        return EXIT_AUTH_REQUIRED
    except KeyboardInterrupt:
        return 1
    except Exception as e:
        logger.exception(f"Ошибка: {e}")
        return 1
    finally:
        parser.close()


if __name__ == "__main__":
    sys.exit(main())

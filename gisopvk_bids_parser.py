"""
Сборщик заявок (bid) ГИС ОПВК через GET https://gisopvk.ru/stencilapi/v1/bid/

Что известно про эндпоинт (проверено):
  * список отдаёт полные объекты заявок (99 полей); /bid/{id}/ возвращает ровно то же самое,
    поэтому по одной заявке не ходим — читаем прямо страницы списка;
  * пагинация page[offset] / page[limit], общее число — meta.pagination.count;
  * без sort порядок нестабилен (дубли и пропуски на глубоких offset), с sort=id — стабильный, по возрастанию id;
  * keyset-фильтры (id__gt, id__gte, id_from, ...) сервером молча игнорируются — листать можно только offset-ом;
  * сервер тратит ~0.3 c на строку независимо от limit: limit=100 ≈ 32 c, limit=500 > 60 c (таймаут прокси,
    вместо JSON приходит HTML). limit=50 ≈ 15 c — безопасно.

ВАЖНО: поля «последнее изменение» нет, а sort=-dt_update сервер отклоняет, поэтому смену статуса у старых
заявок инкрементально поймать нельзя — для этого нужен периодический ПОЛНЫЙ перекрав (этот скрипт, с --fresh).

Алгоритм: sort=id, limit=50, шаг offset=45 (нахлёст 5 строк, чтобы удалённая посреди обхода заявка не сдвинула
запись мимо страницы), дедупликация по id, чекпойнт (offset + последний id) для продолжения после обрыва.
Запись — JSONL.GZ как в GisopvkWebApiParser, в БД не пишем.
"""
import argparse
import datetime
import gzip
import json
import logging
import os
import threading
import time
from typing import Any, Dict, List, Optional

from gisopvkwebapiparser import GisopvkWebApiParser

logger = logging.getLogger("GisopvkBidsParser")

BASE_URL = "https://gisopvk.ru/stencilapi/v1/bid/"
PAGE_LIMIT = 50          # ~15 c на страницу, запас ×2 до таймаута прокси в 60 c
OFFSET_STEP = 45         # нахлёст 5 строк между соседними страницами
REQUEST_TIMEOUT = 90     # заведомо больше таймаута прокси: ответ-HTML придёт сам, а не оборвётся у нас
CHECKPOINT_FILE = "gisopvk_bids_checkpoint.json"
MAX_WORKERS = 3


class RateLimiter:
    """Общий для всех воркеров лимит частоты + глобальная пауза по Retry-After."""

    def __init__(self, min_interval: float):
        self.min_interval = min_interval
        self._lock = threading.Lock()
        self._next_allowed = 0.0

    def wait(self):
        with self._lock:
            now = time.monotonic()
            start = max(now, self._next_allowed)
            self._next_allowed = start + self.min_interval
        delay = start - now
        if delay > 0:
            time.sleep(delay)

    def pause(self, seconds: float):
        """Отложить ВСЕ следующие запросы (429)."""
        with self._lock:
            self._next_allowed = max(self._next_allowed, time.monotonic() + seconds)


class TransientError(Exception):
    pass


class GisopvkBidsParser(GisopvkWebApiParser):
    """Обход /stencilapi/v1/bid/ с sort=id, нахлёстом страниц, дедупом и чекпойнтом."""

    def __init__(self, workers: int = 1, min_request_interval: float = 1.0,
                 max_attempts: int = 8, checkpoint_file: str = CHECKPOINT_FILE):
        super().__init__(base_url=BASE_URL, default_limit=PAGE_LIMIT)
        self.workers = max(1, min(workers, MAX_WORKERS))
        self.max_attempts = max_attempts
        self.checkpoint_file = checkpoint_file
        self.limiter = RateLimiter(min_request_interval)

        self._auth_lock = threading.Lock()   # authorize() интерактивен — только один поток за раз
        self._auth_generation = 0
        self._write_lock = threading.Lock()  # запись в файл + dedup + чекпойнт
        self._seen_ids: set = set()
        self._checkpoint: Dict[str, Any] = {}
        self._server_count: Optional[int] = None
        self._stop = threading.Event()

    # ---------------------------------------------------------------- HTTP
    def _reauthorize(self, seen_generation: int):
        """Повторная авторизация; если другой воркер уже обновил сессию — просто повторяем запрос."""
        with self._auth_lock:
            if self._auth_generation != seen_generation:
                return
            logger.warning("401/403 — повторная авторизация")
            if not self.authorize():
                self._stop.set()
                raise RuntimeError("Повторная авторизация не удалась")
            self._auth_generation += 1

    def _fetch_page(self, offset: int, limit: int = PAGE_LIMIT) -> Dict[str, Any]:
        """Одна страница. 401/403 -> authorize() и тот же запрос; не-JSON/429/сеть -> backoff и тот же запрос."""
        url = f"{self.base_url}?sort=id&page[limit]={limit}&page[offset]={offset}"
        backoff = 5.0
        for attempt in range(1, self.max_attempts + 1):
            if self._stop.is_set():
                raise RuntimeError("Остановлено")
            generation = self._auth_generation
            self.limiter.wait()
            try:
                resp = self.session.get(url, timeout=REQUEST_TIMEOUT)
            except Exception as e:  # сеть/таймаут — временная ошибка
                logger.warning(f"offset={offset}: сетевая ошибка ({attempt}/{self.max_attempts}): {e}")
                time.sleep(backoff)
                backoff = min(backoff * 2, 120)
                continue

            if resp.status_code in (401, 403):
                self._reauthorize(generation)
                continue  # тот же offset, не считается «неудачей» бэкоффа

            if resp.status_code == 429:
                try:
                    wait = float(resp.headers.get("Retry-After", ""))
                except ValueError:
                    wait = backoff
                wait = max(wait, 1.0)
                logger.warning(f"offset={offset}: 429, пауза {wait:.0f} c для всех воркеров")
                self.limiter.pause(wait)
                continue

            if resp.status_code != 200:
                logger.warning(f"offset={offset}: HTTP {resp.status_code} ({attempt}/{self.max_attempts})")
                time.sleep(backoff)
                backoff = min(backoff * 2, 120)
                continue

            try:
                payload = resp.json()
            except ValueError:
                # HTML-страница таймаута прокси и т.п.
                logger.warning(f"offset={offset}: тело не JSON ({attempt}/{self.max_attempts}), "
                               f"пауза {backoff:.0f} c, повтор той же страницы")
                time.sleep(backoff)
                backoff = min(backoff * 2, 120)
                continue

            if not isinstance(payload, dict) or "data" not in payload:
                logger.warning(f"offset={offset}: неожиданная структура ответа ({attempt}/{self.max_attempts})")
                time.sleep(backoff)
                backoff = min(backoff * 2, 120)
                continue
            return payload

        raise TransientError(f"offset={offset}: страница не получена за {self.max_attempts} попыток")

    @staticmethod
    def _total_count(payload: Dict[str, Any]) -> Optional[int]:
        try:
            return int(payload["meta"]["pagination"]["count"])
        except (KeyError, TypeError, ValueError):
            return None

    @staticmethod
    def _item_id(item: Dict[str, Any]) -> str:
        item_id = item.get("id")
        if item_id is None:
            item_id = (item.get("attributes") or {}).get("id")
        return str(item_id)

    # ---------------------------------------------------------- checkpoint
    def _save_checkpoint(self):
        """Атомарная запись чекпойнта; вызывать под _write_lock."""
        tmp = self.checkpoint_file + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self._checkpoint, f, ensure_ascii=False, indent=2)
        os.replace(tmp, self.checkpoint_file)

    def _load_seen_from_output(self, path: str):
        """При продолжении восстанавливаем множество уже сохранённых id из выходного файла."""
        if not os.path.exists(path):
            return
        try:
            with gzip.open(path, "rt", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        self._seen_ids.add(self._item_id(json.loads(line)))
        except EOFError:
            logger.warning(f"{path}: обрезанный gzip-хвост, использую прочитанное")
        logger.info(f"Из {path} восстановлено {len(self._seen_ids)} уникальных id")

    def _init_checkpoint(self, fresh: bool, output_file: Optional[str]):
        if not fresh and os.path.exists(self.checkpoint_file):
            with open(self.checkpoint_file, "r", encoding="utf-8") as f:
                self._checkpoint = json.load(f)
            if not self._checkpoint.get("finished"):
                logger.info(f"Продолжаем по чекпойнту {self.checkpoint_file}: "
                            f"{[(w['id'], w['offset']) for w in self._checkpoint['workers']]}")
                self._load_seen_from_output(self._checkpoint["output_file"])
                return
            logger.info("Предыдущий обход завершён — начинаем новый")

        first = self._fetch_page(0, 1)
        total = self._total_count(first)
        if not total:
            raise RuntimeError("meta.pagination.count не получен или равен 0")
        # Поверхность обхода: с запасом на рост списка за время работы
        end = total + PAGE_LIMIT
        n = self.workers
        per_worker = -(-end // n)
        per_worker = -(-per_worker // OFFSET_STEP) * OFFSET_STEP  # выравнивание по шагу
        workers = []
        for i in range(n):
            start, stop = i * per_worker, min((i + 1) * per_worker, end)
            if start >= end:
                break
            workers.append({"id": i, "start": start, "end": stop, "offset": start,
                            "last_id": None, "done": False})
        stamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        last_page = -(-total // OFFSET_STEP)
        output_file = output_file or f"gisopvk_webapi_{stamp}_1-{last_page}.jsonl.gz"
        self._checkpoint = {"output_file": output_file, "total_at_start": total, "step": OFFSET_STEP,
                            "limit": PAGE_LIMIT, "workers": workers, "finished": False,
                            "started": stamp}
        with self._write_lock:
            self._save_checkpoint()
        logger.info(f"Всего по серверу: {total}; воркеров: {len(workers)}; файл: {output_file}")

    # --------------------------------------------------------------- crawl
    def _write_records(self, records: List[Dict[str, Any]]):
        path = self._checkpoint["output_file"]
        mode = "at" if os.path.exists(path) else "wt"
        with gzip.open(path, mode, encoding="utf-8") as f:
            for rec in records:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    def _worker(self, w: Dict[str, Any]):
        wid = w["id"]
        while not w["done"] and not self._stop.is_set():
            offset = w["offset"]
            payload = self._fetch_page(offset)
            items = payload.get("data") or []
            if not isinstance(items, list):
                items = [items]
            count = self._total_count(payload)
            if count is not None:
                self._server_count = count

            with self._write_lock:
                fresh = []
                for item in items:
                    iid = self._item_id(item)
                    if iid not in self._seen_ids:
                        self._seen_ids.add(iid)
                        fresh.append(item)
                # Сначала запись страницы, и только потом сдвиг чекпойнта
                if fresh:
                    try:
                        self._write_records(fresh)
                    except Exception:
                        for item in fresh:  # запись не удалась — id не считаем сохранёнными
                            self._seen_ids.discard(self._item_id(item))
                        raise
                if items:
                    w["last_id"] = self._item_id(items[-1])
                next_offset = offset + OFFSET_STEP
                if len(items) < PAGE_LIMIT or next_offset >= w["end"]:
                    w["done"] = True
                w["offset"] = next_offset
                self._save_checkpoint()
            logger.info(f"[w{wid}] offset={offset}: получено {len(items)}, новых {len(fresh)}, "
                        f"уникальных всего {len(self._seen_ids)}, last_id={w['last_id']}")

    def run(self, fresh: bool = False, output_file: Optional[str] = None):
        if not self._load_cookies_from_file() or not self.test_auth():
            if not self.authorize():
                raise RuntimeError("Авторизация не удалась")
        self._init_checkpoint(fresh, output_file)

        threads, errors = [], []

        def target(w):
            try:
                self._worker(w)
            except Exception as e:
                logger.error(f"[w{w['id']}] остановлен: {e}")
                errors.append(e)
                self._stop.set()

        for w in self._checkpoint["workers"]:
            if not w["done"]:
                t = threading.Thread(target=target, args=(w,), name=f"w{w['id']}")
                t.start()
                threads.append(t)
        for t in threads:
            t.join()

        if errors:
            logger.error("Обход прерван; запустите снова — продолжим с чекпойнта")
            return False

        with self._write_lock:
            self._checkpoint["finished"] = True
            self._save_checkpoint()

        unique = len(self._seen_ids)
        expected = self._server_count if self._server_count is not None else self._checkpoint["total_at_start"]
        if unique < expected:
            logger.error(f"НЕДОБОР: собрано {unique} уникальных id из {expected} "
                         f"(meta.pagination.count), не хватает {expected - unique}")
        else:
            logger.info(f"Готово: {unique} уникальных id, meta.pagination.count={expected}")
        logger.info(f"Данные сохранены в {self._checkpoint['output_file']}")
        return unique >= expected


def main():
    ap = argparse.ArgumentParser(description="Сборщик заявок ГИС ОПВК (stencilapi, sort=id, JSONL.GZ)")
    ap.add_argument("--workers", type=int, default=1, help="1-3 воркера по непересекающимся диапазонам offset")
    ap.add_argument("--interval", type=float, default=1.0, help="мин. интервал между запросами, c (общий)")
    ap.add_argument("--fresh", action="store_true", help="игнорировать чекпойнт, начать полный обход заново")
    ap.add_argument("--output", default=None, help="имя выходного .jsonl.gz")
    args = ap.parse_args()

    parser = GisopvkBidsParser(workers=args.workers, min_request_interval=args.interval)
    try:
        parser.run(fresh=args.fresh, output_file=args.output)
    finally:
        parser.close()


if __name__ == "__main__":
    main()

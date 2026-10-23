"""
Запуск сценария целиком, без вопросов в консоли (его же запускает UI — app.py).

    python pipeline.py add          # новые заказы: выгрузка -> импорт -> карточки -> заявки -> заполнение -> статусы
    python pipeline.py statuses     # статусы: выгрузка -> обновление статусов в Битрикс
    python pipeline.py statuses --skip-download   # взять последнюю полную выгрузку, не скачивать

Ход выполнения: logs/pipeline_<время>.log (полный вывод шагов) и logs/pipeline_state.json (читает UI).
После успешного запуска старые выгрузки переносятся в archive/<дата>/, папки архива старше
ARCHIVE_KEEP_DAYS (по умолчанию 30) дней удаляются.

Код выхода: 0 — успех, 3 — нужен вход в ФГИС (python login_fgis.py), 1 — ошибка шага.
Шаг с кодом 4 (выполнен, но часть элементов с ошибками) не останавливает сценарий:
следующие шаги выполняются, а весь запуск завершается с кодом 1.
"""

import argparse
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import env_config  # noqa: F401  (подгружает .env: ARCHIVE_KEEP_DAYS и др.)
from pipeline_common import (ROOT, EXIT_AUTH_REQUIRED, EXIT_PARTIAL, archive_export, archive_file, export_time,
                             full_exports, latest_full_export, pid_alive, purge_archive)

LOG_DIR = Path(os.environ.get("LOG_DIR") or ROOT / "logs")
STATE_FILE = LOG_DIR / "pipeline_state.json"

# Шаги: ключ -> (название, скрипт, функция аргументов от пути к выгрузке)
STEPS = {
    "download": ("Выгрузка заказов из ФГИС", "0_step0_get_orders.py", lambda export: []),
    "import": ("Импорт выгрузки в БД", "1_step0_1_import_to_db.py", lambda export: ["--file", str(export)]),
    "sync": ("Создание карточек в Битрикс24", "new_2.1_sync_orders_transportation.py", lambda export: ["--yes"]),
    "collect": ("Загрузка заявок из ФГИС", "1_step0_2.1_collects_bids.py", lambda export: ["--include-unfilled"]),
    "fill": ("Заполнение карточек в Битрикс24", "fill db.py", lambda export: []),
    "statuses": ("Обновление статусов в Битрикс24", "update_orders_daily.py", lambda export: [str(export)]),
    "history": ("История статусов в БД", "status_history.py", lambda export: [str(export)]),
}

SCENARIOS = {
    "add": ("Добавить и заполнить новые заказы",
            ["download", "import", "sync", "collect", "fill", "statuses", "history"]),
    "statuses": ("Обновить статусы", ["download", "statuses", "history"]),
}


# Прогресс шага из его вывода (шаблоны без кириллицы — не зависят от формулировок логов)
RE_TOTAL_PAGES = re.compile(r"~\d+,\s*\S+\s*~(\d+)")          # "записей: ~3540, страниц: ~177"
RE_PAGE = re.compile(r"offset=(\d+), limit=(\d+)")            # "Загрузка страницы с offset=140, limit=20"
RE_BID = re.compile(r"(\d+)/(\d+) - ID: ")                     # "Батч 1: обработка 3/59 - ID: 156321"
RE_TQDM = re.compile(r"(\d+)%\|.*?(\d+)/(\d+) \[")             # "Обновление записей:  10%|█  | 350/3500 [..."


def parse_progress(line: str, progress: dict):
    """Обновляет progress {done, total, unit} по строке вывода; True — строка прогресс-бара tqdm."""
    if m := RE_TQDM.search(line):
        progress.update(done=int(m.group(2)), total=int(m.group(3)), unit="карточек")
        return True
    if m := RE_TOTAL_PAGES.search(line):
        progress.update(done=0, total=int(m.group(1)), unit="стр.")
    elif (m := RE_PAGE.search(line)) and progress.get("unit") == "стр.":
        progress["done"] = min(int(m.group(1)) // max(int(m.group(2)), 1), progress["total"])
    elif m := RE_BID.search(line):
        progress.update(done=int(m.group(1)), total=int(m.group(2)), unit="заявок")
    return False


def read_state() -> dict:
    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def is_running(state: dict) -> bool:
    return state.get("status") == "running" and pid_alive(state.get("pid"))


class Run:
    def __init__(self, scenario: str, skip_download: bool):
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        title, keys = SCENARIOS[scenario]
        if skip_download:
            keys = [k for k in keys if k != "download"]
        started = datetime.now()
        self.log_path = LOG_DIR / f"pipeline_{started:%Y%m%d_%H%M%S}.log"
        self.log = open(self.log_path, "a", encoding="utf-8")
        self.state = {
            "pid": os.getpid(), "scenario": scenario, "title": title, "status": "running",
            "started": started.isoformat(timespec="seconds"), "finished": None,
            "log_file": str(self.log_path), "export": None, "message": "",
            "steps": [{"key": k, "title": STEPS[k][0], "status": "pending", "rc": None,
                       "started": None, "finished": None} for k in keys],
        }
        self.save()

    def save(self):
        tmp = STATE_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.state, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(tmp, STATE_FILE)

    def echo(self, line: str):
        self.log.write(line + "\n")
        self.log.flush()
        try:
            print(line, flush=True)
        except Exception:
            pass  # консоли может не быть (запуск из UI)

    def run_step(self, step: dict, export) -> int:
        title, script, make_args = STEPS[step["key"]]
        step.update(status="running", started=datetime.now().isoformat(timespec="seconds"))
        self.save()
        number = self.state["steps"].index(step) + 1
        self.echo(f"\n===== Шаг {number}/{len(self.state['steps'])}: {title} =====")
        env = dict(os.environ, FEO_NONINTERACTIVE="1", PYTHONIOENCODING="utf-8", PYTHONUTF8="1",
                   PYTHONUNBUFFERED="1")
        proc = subprocess.Popen(
            [sys.executable, "-u", script, *make_args(export)], cwd=ROOT, env=env,
            stdin=subprocess.DEVNULL,  # пропущенный input() упадёт сразу, а не зависнет
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, encoding="utf-8", errors="replace",
        )
        progress = step.setdefault("progress", {})
        last_save, last_bar_decile = 0.0, -1
        for line in proc.stdout:  # '\r' прогресс-бара tqdm тоже разбивает строки
            line = line.rstrip("\n")
            if not line.strip():
                continue
            if parse_progress(line, progress):
                # tqdm обновляется несколько раз в секунду — в журнал только каждые 10%
                decile = progress["done"] * 10 // max(progress["total"], 1)
                if decile != last_bar_decile:
                    last_bar_decile = decile
                    self.echo(line.strip())
            else:
                self.echo(line)
            if progress and time.time() - last_save >= 1:
                last_save = time.time()
                self.save()
        rc = proc.wait()
        status = "ok" if rc == 0 else "errors" if rc == EXIT_PARTIAL else "failed"
        step.update(status=status, rc=rc, finished=datetime.now().isoformat(timespec="seconds"))
        self.save()
        result = {"ok": "готово", "errors": "готово с ошибками"}.get(status, f"ошибка (код {rc})")
        self.echo(f"----- {title}: {result} -----")
        return rc

    def finish(self, status: str, message: str):
        self.state.update(status=status, message=message, finished=datetime.now().isoformat(timespec="seconds"))
        for step in self.state["steps"]:
            if step["status"] == "pending":
                step["status"] = "skipped"
        self.save()
        self.echo(f"\n===== {message} =====")
        self.log.close()


def cleanup(keep_export):
    """Убирает в архив всё, кроме последней полной выгрузки, и чистит старый архив."""
    moved = []
    for export in full_exports():
        if export != keep_export:
            moved += archive_export(export)
    for partial in ROOT.glob("gisopvk_webapi_*.jsonl.gz.partial"):
        moved.append(archive_file(partial))
    removed = purge_archive()
    return moved, removed


def main() -> int:
    ap = argparse.ArgumentParser(description="Запуск сценария пайплайна ФЭО без вопросов в консоли.")
    ap.add_argument("scenario", choices=sorted(SCENARIOS))
    ap.add_argument("--skip-download", action="store_true",
                    help="Не скачивать заказы заново, взять последнюю полную выгрузку.")
    args = ap.parse_args()

    current = read_state()
    if is_running(current):
        print(f"Уже выполняется: {current.get('title')} (PID {current.get('pid')}). Дождитесь окончания.")
        return 1

    run = Run(args.scenario, args.skip_download)
    run.echo(f"Сценарий: {run.state['title']}  ({run.state['started']})")
    export = None
    if args.skip_download:
        export = latest_full_export()
        if export is None:
            run.finish("failed", "Нет полной выгрузки — запустите без «не скачивать»")
            return 1
        run.echo(f"Используется последняя выгрузка: {export.name} ({export_time(export):%d.%m.%Y %H:%M})")
        run.state["export"] = export.name

    partial = []
    for step in run.state["steps"]:
        started = time.time()
        rc = run.run_step(step, export)
        if rc == EXIT_AUTH_REQUIRED:
            run.finish("auth_required", "Нужен вход в ФГИС: нажмите «Войти в ФГИС» (или python login_fgis.py)")
            return EXIT_AUTH_REQUIRED
        if rc == EXIT_PARTIAL:
            partial.append(step["title"])
        elif rc != 0:
            run.finish("failed", f"Ошибка на шаге «{step['title']}» — подробности в журнале выше")
            return 1
        if step["key"] == "download":
            export = latest_full_export()
            # выгрузка должна быть создана этим запуском, а не остаться от прошлого
            if export is None or export_time(export).timestamp() < started - 60:
                run.finish("failed", "Выгрузка не найдена после скачивания")
                return 1
            run.state["export"] = export.name
            run.save()

    moved, removed = cleanup(export)
    if moved:
        run.echo(f"В архив перенесено файлов: {len(moved)}")
    if removed:
        run.echo(f"Удалены старые папки архива: {[p.name for p in removed]}")
    if partial:
        run.finish("failed", "Готово, но с ошибками на шагах: " + ", ".join(f"«{t}»" for t in partial)
                   + " — подробности в журнале выше и во вкладке «Журнал Битрикс»")
        return 1
    run.finish("ok", "Готово")
    return 0


if __name__ == "__main__":
    sys.exit(main())

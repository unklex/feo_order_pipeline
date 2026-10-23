"""
Общие функции для неинтерактивного запуска (pipeline.py, app.py) и работы с файлами данных.

FEO_NONINTERACTIVE=1 — скрипты не задают вопросов в консоли: берут значения по умолчанию,
при ошибке завершаются с ненулевым кодом, а если сохранённый вход в ФГИС недействителен —
с кодом EXIT_AUTH_REQUIRED (войти: python login_fgis.py или кнопка в UI).

Файлы данных лежат в папке проекта:
    gisopvk_webapi_<дата>_<время>_1-<N>.jsonl.gz  — полная выгрузка заказов (со страницы 1)
    batch_<NNN>_<дата>_<время>.json               — распарсенные заявки, ещё не заполненные в Битрикс
Отработанные файлы переносятся в archive/<ГГГГ-ММ-ДД>/ и удаляются через ARCHIVE_KEEP_DAYS дней.
"""

import os
import re
import shutil
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent
ARCHIVE_DIR = ROOT / "archive"

# Код выхода скриптов, когда нужен повторный вход в ФГИС
EXIT_AUTH_REQUIRED = 3

FULL_EXPORT_RE = re.compile(r'^gisopvk_webapi_(\d{8})_(\d{6})_1-\d+\.jsonl\.gz$')


def noninteractive() -> bool:
    return os.environ.get("FEO_NONINTERACTIVE") == "1"


class AuthRequired(RuntimeError):
    """Сохранённый вход в ФГИС недействителен, а спросить пользователя нельзя."""


def pid_alive(pid) -> bool:
    """Жив ли процесс (без сигналов: в Windows os.kill(pid, 0) завершил бы процесс)."""
    if not pid:
        return False
    if os.name == 'nt':
        import ctypes
        kernel32 = ctypes.windll.kernel32
        handle = kernel32.OpenProcess(0x1000, False, int(pid))  # PROCESS_QUERY_LIMITED_INFORMATION
        if not handle:
            return False
        code = ctypes.c_ulong()
        kernel32.GetExitCodeProcess(handle, ctypes.byref(code))
        kernel32.CloseHandle(handle)
        return code.value == 259  # STILL_ACTIVE
    try:
        os.kill(int(pid), 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def full_exports(directory=ROOT) -> list:
    """Полные выгрузки (начинаются со страницы 1), от старых к новым по дате в имени."""
    found = []
    for path in Path(directory).glob('gisopvk_webapi_*.jsonl.gz'):
        m = FULL_EXPORT_RE.match(path.name)
        if m:
            found.append((m.group(1) + m.group(2), path))
    return [path for _, path in sorted(found)]


def latest_full_export(directory=ROOT):
    exports = full_exports(directory)
    return exports[-1] if exports else None


def export_time(path) -> datetime:
    m = FULL_EXPORT_RE.match(Path(path).name)
    return datetime.strptime(m.group(1) + m.group(2), '%Y%m%d%H%M%S')


def pending_batch_files(directory=ROOT) -> list:
    """Файлы распарсенных заявок, ещё не перенесённые в архив."""
    return sorted(Path(directory).glob('batch_*.json'))


def archive_file(path) -> Path:
    """Переносит файл в archive/<ГГГГ-ММ-ДД>/ (не удаляет) и возвращает новый путь."""
    path = Path(path)
    target_dir = ARCHIVE_DIR / datetime.now().strftime('%Y-%m-%d')
    target_dir.mkdir(parents=True, exist_ok=True)
    target = target_dir / path.name
    if target.exists():
        target = target_dir / f"{path.stem}_{datetime.now():%H%M%S}{path.suffix}"
    shutil.move(str(path), str(target))
    return target


def archive_export(path) -> list:
    """Переносит выгрузку и её файл статистики (*_stats.json) в архив."""
    path = Path(path)
    moved = [archive_file(path)]
    stats = path.with_name(path.name.replace('.jsonl.gz', '_stats.json'))
    if stats.exists():
        moved.append(archive_file(stats))
    return moved


def purge_archive(keep_days=None) -> list:
    """Удаляет папки archive/<ГГГГ-ММ-ДД> старше keep_days (по умолчанию ARCHIVE_KEEP_DAYS или 30)."""
    if keep_days is None:
        keep_days = int(os.environ.get('ARCHIVE_KEEP_DAYS') or 30)
    cutoff = datetime.now().date() - timedelta(days=keep_days)
    removed = []
    if ARCHIVE_DIR.is_dir():
        for folder in ARCHIVE_DIR.iterdir():
            try:
                day = datetime.strptime(folder.name, '%Y-%m-%d').date()
            except ValueError:
                continue  # чужие папки не трогаем
            if folder.is_dir() and day < cutoff:
                shutil.rmtree(folder)
                removed.append(folder)
    return removed

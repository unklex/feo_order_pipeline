"""
JSONL-журнал для последующего анализа запусков.

Один файл на запуск: logs/audit_<скрипт>_<ГГГГММДД_ЧЧММСС>.jsonl, одна строка — одно событие:
    {"ts": "...", "script": "fill_db", "event": "item_update", "item_id": 5559, "ok": true, "fields": {...}}

Пример анализа:
    import pandas as pd
    df = pd.read_json("logs/audit_fill_db_20261022_120000.jsonl", lines=True)
    df[df.event == "item_update"].ok.value_counts()
"""

import json
import os
from datetime import date, datetime
from decimal import Decimal

LOG_DIR = os.environ.get("LOG_DIR") or os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")


def _default(obj):
    """Сериализация того, что json не умеет: даты, Decimal, numpy/pandas-скаляры."""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return str(obj)
    if hasattr(obj, "item"):  # numpy.int64, numpy.float64 и т.п.
        return obj.item()
    return str(obj)


class AuditLog:
    """Пишет события в JSONL; каждая строка сбрасывается на диск сразу (не теряется при падении)."""

    def __init__(self, script: str, log_dir: str = LOG_DIR):
        os.makedirs(log_dir, exist_ok=True)
        self.script = script
        self.path = os.path.join(log_dir, f"audit_{script}_{datetime.now():%Y%m%d_%H%M%S}.jsonl")
        self._file = open(self.path, "a", encoding="utf-8")

    def write(self, event: str, **data):
        record = {"ts": datetime.now().isoformat(timespec="seconds"), "script": self.script, "event": event, **data}
        self._file.write(json.dumps(record, ensure_ascii=False, default=_default) + "\n")
        self._file.flush()

    def close(self):
        if not self._file.closed:
            self._file.close()

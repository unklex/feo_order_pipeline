"""pipeline.py: шаг с кодом EXIT_PARTIAL не останавливает сценарий, но запуск — ошибочный."""
import json
import sys

import pytest

import pipeline
from pipeline_common import EXIT_PARTIAL


@pytest.fixture
def fake_run(tmp_path, monkeypatch):
    def run(codes: dict):
        steps = {}
        for key, code in codes.items():
            script = tmp_path / f"{key}.py"
            script.write_text(f"import sys; print('{key}'); sys.exit({code})", encoding="utf-8")
            steps[key] = (key, str(script), lambda export: [])
        monkeypatch.setattr(pipeline, "STEPS", steps)
        monkeypatch.setattr(pipeline, "SCENARIOS", {"test": ("Тест", list(codes))})
        monkeypatch.setattr(pipeline, "LOG_DIR", tmp_path)
        monkeypatch.setattr(pipeline, "STATE_FILE", tmp_path / "state.json")
        monkeypatch.setattr(pipeline, "cleanup", lambda export: ([], []))
        monkeypatch.setattr(sys, "argv", ["pipeline.py", "test", "--skip-download"])
        monkeypatch.setattr(pipeline, "latest_full_export", lambda: tmp_path / "export.jsonl.gz")
        monkeypatch.setattr(pipeline, "export_time", lambda p: pipeline.datetime.now())
        rc = pipeline.main()
        return rc, json.loads((tmp_path / "state.json").read_text(encoding="utf-8"))
    return run


def test_all_ok(fake_run):
    rc, state = fake_run({"a": 0, "b": 0})
    assert rc == 0 and state["status"] == "ok"


def test_partial_step_continues_and_fails_run(fake_run):
    rc, state = fake_run({"a": EXIT_PARTIAL, "b": 0})
    assert rc == 1 and state["status"] == "failed"
    assert [s["status"] for s in state["steps"]] == ["errors", "ok"]
    assert "«a»" in state["message"]


def test_failed_step_stops(fake_run):
    rc, state = fake_run({"a": 1, "b": 0})
    assert rc == 1
    assert [s["status"] for s in state["steps"]] == ["failed", "skipped"]

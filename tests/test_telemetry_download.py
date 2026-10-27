"""telemetry_download на поддельном API; совместимость с выгрузкой opvk-export (папка chk_24h, если есть)."""
import csv
import gzip
from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

import telemetry_download as td

ROOT = Path(__file__).resolve().parent.parent
DAY = 86400


class FakeApi(td.Api):
    """Две машины с трекером, одна без; точка каждые 60 с, кроме ТС 2 — каждые 10 с (насыщенные окна)."""

    def __init__(self, fail_after=None, active_days=None, last_date="2026-09-01T00:00:00Z", max_window=None):
        import threading
        self.stop = threading.Event()
        self.calls = 0
        self.fail_after = fail_after
        self.active_days = active_days      # {vehicle_id: {date, …}} — в остальные дни точек нет
        self.last_date = last_date
        self.max_window = max_window        # окно длиннее — сервер «отвечает» пусто

    def get_json(self, url, params):
        self.calls += 1
        if self.fail_after is not None and self.calls > self.fail_after:
            raise td.AuthError("HTTP 403")
        if url == td.VEHICLE_URL:
            data = [{"id": "1", "attributes": {"tracker_id": "111.10302", "auto_gnum": "А111АА 14",
                                               "telemetry_last_date": self.last_date}},
                    {"id": "2", "attributes": {"imei": "222", "telemetry_port": 10302, "auto_gnum": "В222ВВ 14",
                                               "telemetry_last_date": self.last_date}},
                    {"id": "3", "attributes": {"auto_gnum": "без трекера"}}]
            return {"data": data[params["page[offset]"]:], "meta": {"pagination": {"count": 3}}}
        step = 10 if params["vehicle_id"] == "2" else 60
        start, end = params["timestamp_from"], params["timestamp_to"]
        if self.max_window and end - start > self.max_window:
            return []
        first = start + (-start) % step
        active = (self.active_days or {}).get(params["vehicle_id"])
        pts = [{"date_time": f"t{t:012d}", "lat": 55.5, "lon": 37.0 + t % 7, "speed": 12.0, "course": 90,
                "sat": None, "telemetry_state": "ok"} for t in range(first, end + 1, step)
               if active is None or date(1970, 1, 1) + timedelta(seconds=t) in active]
        if len(pts) > 5000:  # сервер прореживает большие окна (но не ниже порога дробления)
            pts = pts[::4]
        return {"results": pts} if params["vehicle_id"] == "1" else pts


def args(out, **kw):
    base = dict(out=str(out), day_from=date(2026, 9, 1), day_to=date(2026, 9, 2), dry_run=False,
                all_vehicles=False, vehicles="", workers=3, rps=1000, window_hours=24, split_at=1000,
                rebuild_summary=False, probe_days=30, no_last_date_cut=False)
    return SimpleNamespace(**(base | kw))


def all_rows(out):
    rows = []
    for part in sorted(Path(out).glob("telemetry_*.csv.gz")):
        with gzip.open(part, "rt", encoding="utf-8", newline="") as f:
            rows += [r for r in csv.reader(f) if r[0] != "vehicle_id"]
    return rows


def test_full_run_split_and_summary(tmp_path):
    assert td.run(FakeApi(), args(tmp_path)) == 0
    rows = all_rows(tmp_path)
    by_vehicle = {vid: [r for r in rows if r[0] == vid] for vid in ("1", "2")}
    assert len(by_vehicle["1"]) == 2 * 1440            # 2 суток по точке в минуту
    assert len(by_vehicle["2"]) == 2 * 8640            # окна дробились, прореживания нет
    assert len({r[3] for r in by_vehicle["2"]}) == len(by_vehicle["2"])  # без повторов на стыках
    assert by_vehicle["2"][0][2] == "222.10302"        # трекер из imei + порта
    assert by_vehicle["1"][0][4:9] == ["55.5", by_vehicle["1"][0][5], "12", "90", ""]
    summary = {r["vehicle_id"]: r for r in csv.DictReader(open(tmp_path / "summary.csv", encoding="utf-8"))}
    assert summary["1"]["points"] == "2880" and summary["2"]["points"] == "17280"
    assert len((tmp_path / "done.txt").read_text().split()) == 4
    assert sum(1 for _ in csv.reader(open(tmp_path / "vehicles.csv", encoding="utf-8"))) == 4


def test_resume_after_auth_error_and_repair(tmp_path):
    # Первый запуск: сессия «истекает» посреди выгрузки
    # 1 поток: запрос 1 — список ТС, 2–4 — первые сутки ТС 1 (1440 точек >= 1000: окно + 2 половины),
    # на 5-м сессия «истекает»
    assert td.run(FakeApi(fail_after=4), args(tmp_path, workers=1, probe_days=0)) == td.EXIT_AUTH_REQUIRED
    done_first = (tmp_path / "done.txt").read_text().split()
    assert len(done_first) == 1
    # Имитируем обрыв процесса: обрезаем часть посередине
    part = next(tmp_path.glob("telemetry_*.csv.gz"))
    raw = part.read_bytes()
    part.write_bytes(raw[: len(raw) * 2 // 3])
    (tmp_path / "done.txt").write_text("")  # обрыв — окна не считаем скачанными
    assert td.run(FakeApi(), args(tmp_path)) == 0
    assert td.gz_is_complete(part)
    rows = all_rows(tmp_path)
    # Каждая точка — минимум один раз; повторы из перепакованной части убирает сводка не обязана,
    # но done.txt полный и сводка пересчитана по всем частям
    assert {r[0] for r in rows} == {"1", "2"}
    assert len((tmp_path / "done.txt").read_text().split()) == 4


def test_dry_run_writes_nothing_but_vehicles(tmp_path):
    assert td.run(FakeApi(), args(tmp_path, dry_run=True)) == 0
    assert (tmp_path / "vehicles.csv").exists()
    assert not list(tmp_path.glob("telemetry_*.csv.gz"))


def test_salvage_keeps_whole_lines(tmp_path):
    src, dst = tmp_path / "a.csv.gz", tmp_path / "b.csv.gz"
    with gzip.open(src, "wt", encoding="utf-8") as f:
        for i in range(20000):
            f.write(f"{i},строка\n")
            if i == 10000:
                f.flush()
    raw = src.read_bytes()
    src.write_bytes(raw[: len(raw) - 50])
    lines = td.salvage_gz(src, dst)
    with gzip.open(dst, "rt", encoding="utf-8") as f:
        text = f.read()
    assert text.count("\n") == lines and lines >= 10001 and text.endswith("\n")
    assert text.splitlines()[-1].endswith(",строка")


@pytest.mark.skipif(not (ROOT / "chk_24h" / "done.txt").exists(), reason="нет выгрузки opvk-export")
def test_same_windows_as_opvk_export():
    go_keys = set((ROOT / "chk_24h" / "done.txt").read_text().split())
    vid = next(iter(go_keys)).split("|")[0]
    starts = sorted(int(k.split("|")[1]) for k in go_keys)
    day_from = date(1970, 1, 1) + timedelta(days=starts[0] // DAY)
    day_to = date(1970, 1, 1) + timedelta(days=starts[-1] // DAY)
    jobs = td.make_jobs([{"vehicle_id": vid}], day_from, day_to, timedelta(hours=24), set())
    assert {td.job_key(v["vehicle_id"], s, e) for v, s, e in jobs} == go_keys


@pytest.mark.skipif(not (ROOT / "chk_24h" / "telemetry.csv").exists(), reason="нет выгрузки opvk-export")
def test_number_format_matches_opvk_export():
    with open(ROOT / "chk_24h" / "telemetry.csv", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    for r in rows[:20000]:
        for col in ("lat", "lon", "speed", "course", "sat"):
            if r[col]:
                assert td.num(float(r[col])) == r[col], (col, r[col])


def sparse_args(out, **kw):
    # 60 суток; ТС 1 ездила только 15.09, ТС 2 — только 20.10 (последняя телеметрия 20.10)
    return args(out, day_from=date(2026, 9, 1), day_to=date(2026, 10, 30), **kw)


SPARSE = {"1": {date(2026, 9, 15)}, "2": {date(2026, 10, 20)}}


def test_blocks_skip_empty_days_and_keep_all_points(tmp_path):
    api = FakeApi(active_days=SPARSE, last_date="2026-10-20T15:00:00Z")
    assert td.run(api, sparse_args(tmp_path)) == 0
    rows = all_rows(tmp_path)
    assert len([r for r in rows if r[0] == "1"]) == 1440 and len([r for r in rows if r[0] == "2"]) == 8640
    # обрезка по последней телеметрии (+1 день): 51 сутки на машину вместо 60
    assert len((tmp_path / "done.txt").read_text().split()) == 2 * 51
    # те же данные по суткам — для сравнения числа запросов
    daily = FakeApi(active_days=SPARSE, last_date="2026-10-20T15:00:00Z")
    assert td.run(daily, sparse_args(tmp_path / "daily", probe_days=0)) == 0
    assert len(all_rows(tmp_path / "daily")) == len(rows)
    assert api.calls < daily.calls * 0.6, (api.calls, daily.calls)


def test_blocks_disabled_when_long_windows_come_back_empty(tmp_path):
    # сервер «молчит» на окнах длиннее суток: калибровка это замечает, качаем по суткам без потерь
    api = FakeApi(active_days=SPARSE, last_date="2026-10-20T15:00:00Z", max_window=86400)
    assert td.run(api, sparse_args(tmp_path)) == 0
    rows = all_rows(tmp_path)
    assert len([r for r in rows if r[0] == "1"]) == 1440 and len([r for r in rows if r[0] == "2"]) == 8640


def test_make_work_groups_contiguous_days():
    v1, v2 = {"vehicle_id": "1"}, {"vehicle_id": "2"}
    jobs = [(v1, d * 86400, d * 86400 + 86399) for d in range(65)] + [(v2, 0, 86399)]
    work = td.make_work(jobs, 30)
    assert [w[0] for w in work] == ["probe", "probe", "probe", "job"]
    assert [len(w[2]) for w in work[:3]] == [30, 30, 5]
    assert td.make_work(jobs, 0) == [("job", j) for j in jobs]

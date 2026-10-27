import csv
import gzip
import json
import re

import api_export as ax

H = "https://gisopvk.ru"
DRIVERS = [{"type": "driver", "id": str(i),
            "attributes": {"fio": f"Водитель {i}", "phone": f"+7900{i:07d}", "license": {"number": f"77{i}",
                                                                                         "date": "2030-01-01"},
                           "categories": ["C", "E"]},
            "relationships": {"vehicles": {"data": [{"type": "vehicle", "id": "10"}, {"type": "vehicle", "id": "11"}]},
                              "org_owner": {"data": {"type": "organization", "id": "54906"}}}}
           for i in range(1, 121)]


class Resp:
    def __init__(self, status, body=None):
        self.status_code = status
        self.content = json.dumps(body or {}).encode()

    def json(self):
        return json.loads(self.content)


class Session:
    def __init__(self):
        self.calls = []

    def get(self, url, params=None, timeout=None, allow_redirects=True):
        assert allow_redirects is False
        self.calls.append((url, dict(params)))
        if url == f"{H}/webapi/v1/driver/":
            return Resp(404)
        if "sort" in params:
            return Resp(500)  # сортировку этот эндпоинт не умеет
        offset, limit = params["page[offset]"], params["page[limit]"]
        if offset == 50:
            offset -= 2  # список «сдвинулся»: два повтора на стыке страниц
        return Resp(200, {"data": DRIVERS[offset:offset + limit], "meta": {"pagination": {"count": 120}}})


def test_export_finds_endpoint_pages_and_writes(tmp_path):
    session = Session()
    out = ax.export(ax.Fetcher(session, delay=0), "drivers", ax.DATASETS["drivers"][1], tmp_path)
    assert session.calls[0][0] == f"{H}/webapi/v1/driver/"
    assert any(c[0] == f"{H}/webapi/v1/drivers/" and "sort" not in c[1] for c in session.calls)
    with gzip.open(out / "records.jsonl.gz", "rt", encoding="utf-8") as f:
        ids = [json.loads(line)["id"] for line in f]
    assert len(ids) == len(set(ids)) == 118  # повторы на стыке отброшены (а 2 записи «сдвинулись» за конец)
    rows = list(csv.DictReader(open(out / "records.csv", encoding="utf-8-sig"), delimiter=";"))
    first = rows[0]
    assert first["fio"] == "Водитель 1" and first["license.number"] == "771"
    assert first["categories"] == '["C", "E"]'
    assert first["rel.vehicles"] == "10,11" and first["rel.org_owner"] == "54906"
    info = json.loads((out / "info.json").read_text(encoding="utf-8"))
    assert info["url"] == f"{H}/webapi/v1/drivers/" and info["sorted_by_id"] is False


def test_no_endpoint_found(tmp_path):
    class Missing(Session):
        def get(self, url, params=None, timeout=None, allow_redirects=True):
            return Resp(404)

    try:
        ax.export(ax.Fetcher(Missing(), delay=0), "drivers", ax.DATASETS["drivers"][1], tmp_path)
    except LookupError as e:
        assert "Исследовать API" in str(e)
    else:
        raise AssertionError("ожидалась ошибка")
    assert not list(tmp_path.iterdir())


def test_fetcher_refuses_other_hosts():
    try:
        ax.Fetcher(Session(), delay=0).get("https://gisopvk.ru.evil.com/webapi/v1/driver/", {})
    except PermissionError:
        pass
    else:
        raise AssertionError("ожидался отказ")


def test_excel_formula_cells_are_neutralised(tmp_path):
    assert ax.excel_safe('=HYPERLINK("http://evil","x")') == '\'=HYPERLINK("http://evil","x")'
    assert ax.excel_safe("+cmd|' /C calc'!A0") == "'+cmd|' /C calc'!A0"
    assert ax.excel_safe("@SUM(1)") == "'@SUM(1)"
    assert ax.excel_safe("\t=1") == "'\t=1"
    for keep in ("+79001234567", "+7 (900) 123-45-67", "-12.5", "Иванов", "", None, 5):
        assert ax.excel_safe(keep) == keep

    class Evil(Session):
        def get(self, url, params=None, timeout=None, allow_redirects=True):
            if url.endswith("/driver/"):
                return Resp(200, {"data": [{"id": "1", "attributes": {"fio": "=1+1", "=bad": "x",
                                                                       "phone": "+79001234567"}}],
                                  "meta": {"pagination": {"count": 1}}})
            return Resp(404)

    out = ax.export(ax.Fetcher(Evil(), delay=0), "drivers", ax.DATASETS["drivers"][1], tmp_path)
    header, row = list(csv.reader(open(out / "records.csv", encoding="utf-8-sig"), delimiter=";"))
    assert "'=bad" in header
    values = dict(zip(header, row))
    assert values["fio"] == "'=1+1" and values["phone"] == "+79001234567"


def test_csv_is_not_split_inside_values(tmp_path):
    """Русский Excel делит по «;» — значение с «;=…» внутри должно остаться одной ячейкой."""
    class Evil(Session):
        def get(self, url, params=None, timeout=None, allow_redirects=True):
            if url.endswith("/driver/"):
                return Resp(200, {"data": [{"id": "1", "attributes": {"fio": 'Иванов;=HYPERLINK("http://evil")',
                                                                       "note": "a,=1+1"}}],
                                  "meta": {"pagination": {"count": 1}}})
            return Resp(404)

    out = ax.export(ax.Fetcher(Evil(), delay=0), "drivers", ax.DATASETS["drivers"][1], tmp_path)
    line = (out / "records.csv").read_text(encoding="utf-8-sig").splitlines()[1]
    # все поля в кавычках, между ними «;»: разделитель вне кавычек — только между полями
    assert line.startswith('"') and line.endswith('"')
    outside_quotes = re.sub(r'"(?:[^"]|"")*"', "", line)
    assert set(outside_quotes) <= {";"}
    header, row = list(csv.reader(open(out / "records.csv", encoding="utf-8-sig"), delimiter=";"))
    values = dict(zip(header, row))
    assert values["fio"] == 'Иванов;=HYPERLINK("http://evil")' and values["note"] == "a,=1+1"

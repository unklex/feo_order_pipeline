"""api_probe на поддельном JSON:API: сервер понимает sort=id/-id и filter[dt_update__gte], остальное игнорирует."""
from datetime import date, timedelta
from urllib.parse import parse_qsl, urlparse

import api_probe as ap

RECENT = (date.today() - timedelta(days=5)).isoformat()
OLD = (date.today() - timedelta(days=200)).isoformat()
URL = "https://gisopvk.ru/webapi/v1/thing/"
ROWS = [{"type": "thing", "id": str(i),
         "attributes": {"status_label": "shipping" if i % 3 else "old", "is_active": bool(i % 3),
                        "dt_update": f"{RECENT if i % 2 else OLD}T10:00:00+03:00",
                        "contact_phone": "+79991234567", "cost": i * 10},
         "relationships": {"bid": {"data": {"type": "bid", "id": str(1000 + i)},
                                   "links": {"related": f"{URL}{i}/bid/"}}}}
        for i in range(1, 31)]


class FakeClient:
    count = 0

    def request(self, url, params=None, method="GET"):
        self.count += 1
        p = dict(params or [])
        if method == "OPTIONS" or not url.startswith(URL):
            return {"url": url, "status": 404, "json": {"errors": [{"detail": "Not found"}]}}
        rest = url[len(URL):]
        if rest.endswith("/bid/"):
            return {"url": url, "status": 200, "json": {"data": {"type": "bid", "id": "1", "attributes": {"number": 5}}}}
        if rest:
            row = next(r for r in ROWS if r["id"] == rest.strip("/"))
            return {"url": url, "status": 200, "json": {"data": {**row, "attributes": {**row["attributes"], "extra": 1}}}}
        rows = ROWS
        if "filter[dt_update__gte]" in p:
            rows = [r for r in rows if r["attributes"]["dt_update"][:10] >= p["filter[dt_update__gte]"]]
        if p.get("sort") == "-id":
            rows = sorted(rows, key=lambda r: -int(r["id"]))
        limit, offset = int(p.get("page[limit]", 20)), int(p.get("page[offset]", 0))
        return {"url": url, "status": 200, "ms": 10,
                "json": {"data": rows[offset:offset + limit], "meta": {"pagination": {"count": len(rows)}},
                         "links": {}}}


def test_params_verdicts():
    client = FakeClient()
    rows = ap.records(client.request(URL, [("page[limit]", 20)]))
    out = ap.probe_params(client, URL, rows, limit=5)
    assert out["sort"]["-id"]["verdict"] == "desc"
    assert out["sort"]["id"]["verdict"].startswith("asc")
    assert out["sort"]["-dt_update"]["verdict"] != "desc"
    flt = out["filter"]
    assert flt[f"filter[dt_update__gte]={(date.today() - timedelta(days=30)).isoformat()}"]["verdict"] == "works"
    assert flt[f"dt_update__gte={(date.today() - timedelta(days=30)).isoformat()}"]["verdict"] == "ignored"
    assert any(k.startswith("filter[status_label]=") and v["verdict"] == "ignored" for k, v in flt.items())


def test_structure_masks_and_reports(tmp_path):
    entry = ap.probe_structure(FakeClient(), "thing", URL, 20, raw=False, out_dir=tmp_path)
    schema = entry["schema"]
    assert schema["attributes.contact_phone"]["example"] == "<str len=12>"
    assert schema["attributes.status_label"]["values"] == {"shipping": 14, "old": 6}
    assert entry["list"]["total"] == 30
    assert entry["detail"]["only_in_detail"] == ["attributes.extra"]
    assert entry["related"]["bid"]["types"] == {"bid": 1}
    assert (tmp_path / "samples" / "thing.json").exists()

    report = {"started": "t", "raw": False, "requests": 1, "discovery": {"roots": {}, "found": {}},
              "schemas": {}, "endpoints": {"thing": entry}}
    ap.write_markdown(report, tmp_path / "report.md")
    text = (tmp_path / "report.md").read_text(encoding="utf-8")
    assert "attributes.contact_phone" in text and "+7999" not in text


def test_total_from_last_link():
    resp = {"json": {"data": [], "links": {"last": f"{URL}?page%5Blimit%5D=20&page%5Boffset%5D=3520"}}}
    assert ap.total(resp) == "last_offset=3520"

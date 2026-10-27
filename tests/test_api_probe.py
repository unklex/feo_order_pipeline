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


def test_stencil_results_format():
    resp = {"json": {"results": [{"id": 7, "dt_update": "2026-10-01T10:00:00+03:00", "status_label": "x"}],
                     "meta": {"pagination": {"count": 326843}}, "links": {}}}
    assert ap.records(resp)[0]["id"] == 7
    assert ap.total(resp) == 326843
    assert ap.attr(ap.records(resp)[0], "dt_update").startswith("2026-10-01")


def test_root_links_any_shape():
    root = "https://gisopvk.ru/webapi/v1/"
    body = {"data": {
        "bid": "https://gisopvk.ru/webapi/v1/bid/",
        "vehicle": "/webapi/v1/vehicle/?page[limit]=20",
        "nested": {"driver_task": {"url": "https://gisopvk.ru/webapi/v1/driver_task/"}},
        "items": [{"type": "fkko", "links": {"self": "/stencilapi/v1/fkko/"}}],
        "self": root,
        "not_api": "https://gisopvk.ru/files/x.png",
        "text": "Просто строка",
    }}
    assert ap.root_links(body, root) == {
        "bid": "https://gisopvk.ru/webapi/v1/bid/",
        "vehicle": "https://gisopvk.ru/webapi/v1/vehicle/",
        "driver_task": "https://gisopvk.ru/webapi/v1/driver_task/",
        "stencil_fkko": "https://gisopvk.ru/stencilapi/v1/fkko/",
    }


def test_stencil_params_use_flat_fields():
    rows = [{"id": i, "dt_update": f"{RECENT}T10:00:00+03:00", "status_label": "a"} for i in range(1, 6)]
    client = FakeClient()
    out = ap.probe_params(client, URL, rows, limit=5)
    assert "dt_update" in out["sort"] and "-dt_update" in out["sort"]
    assert any(k.startswith("filter[dt_update__gte]") for k in out["filter"])


def test_file_link_kinds_from_real_shapes():
    h = "https://gisopvk.ru"
    cases = {
        f"{h}/webapi/v1/file/3586215/view/": "/webapi/v1/file/{n}/view/",
        f"{h}/webapi/v1/photo/371217/thumbnail/": "/webapi/v1/photo/{n}/thumbnail/",
        "webapi/v1/sign_files_dl/?resource_name=bid&obj_id=327398&file_field_name=sign_print_form":
            "/webapi/v1/sign_files_dl/?resource_name=bid&file_field_name=sign_print_form&obj_id",
        "webapi/v1/status_history_file_dl/?resource_name=bid&obj_id=327389&file_id=3586215":
            "/webapi/v1/status_history_file_dl/?resource_name=bid&file_id&obj_id",
        "files/photo/4/3/d/43dd0b40950a948d.снимок.png": "files/….png",
    }
    for link, kind in cases.items():
        assert ap.file_link_kind(link)[0] == kind, link
    for not_file in (f"{h}/webapi/v1/vehicle/", "https://other.ru/file/1/view/", "Заказ ID: 5", "photo"):
        assert ap.file_link_kind(not_file) is None, not_file


def test_sniff_and_disposition():
    assert ap.sniff(b"%PDF-1.7\n...") == "pdf"
    assert ap.sniff(b"\xff\xd8\xff\xe0JFIF") == "jpeg"
    assert ap.sniff(b'{"errors": []}') == "json"
    assert ap.sniff(b"<!DOCTYPE html><html>") == "html"
    assert ap.disposition_ext('attachment; filename="Счёт Иванов.PDF"') == "pdf"
    assert ap.disposition_ext("attachment; filename*=UTF-8''%D0%B0.xml") == "xml"


def test_probe_files_documents_and_links():
    rows = {"order_transportation": [{
        "relationships": {"documents_invoice": {"data": [{"type": "document_invoice_order_transportation",
                                                          "id": "55337"}]},
                          "photos": {"data": [{"type": "photo", "id": "9"}]}},
        "attributes": {"status_history": [{"files": [{"url": "https://gisopvk.ru/webapi/v1/file/1/view/"}]}]}}],
        "order_recycling": [{"meta": {"signatures": [{"sign_file":
            "webapi/v1/sign_files_dl/?resource_name=order_recycling&obj_id=3&file_field_name=sign_file"}]}}]}

    class C:
        count = 0

        def request(self, url, params=None, method="GET"):
            assert url.endswith("/webapi/v1/document_invoice_order_transportation/55337/")
            return {"status": 200, "json": {"data": {"attributes": {"number": "1", "file_pdf":
                                                                     "/webapi/v1/document_file_dl/?id=55337"}}}}

        def peek(self, url, max_bytes=8192):
            return {"url": url, "status": 200, "kind": "pdf", "content_type": "application/pdf", "size": "2048"}

    out = ap.probe_files(C(), rows)
    kinds = set(out["links"])
    assert "/webapi/v1/file/{n}/view/" in kinds
    assert "/webapi/v1/photo/{n}/view/" in kinds and "/webapi/v1/photo/{n}/thumbnail/" in kinds
    assert "/webapi/v1/sign_files_dl/?resource_name=order_recycling&file_field_name=sign_file&obj_id" in kinds
    assert "/webapi/v1/document_file_dl/?id" in kinds
    doc = out["documents"]["document_invoice_order_transportation"]
    assert doc["status"] == 200 and doc["file_links"] == ["/webapi/v1/document_file_dl/?id"]

from datetime import datetime

import status_history as sh


def record(history, order_id="137566", bid_id="156321"):
    return {"id": order_id, "relationships": {"bid": {"data": {"id": bid_id}}},
            "attributes": {"status_history": history}}


def test_utc_converted_to_msk():
    # 21:30 UTC — уже следующий день по Москве
    assert sh.to_msk_naive("2026-01-19T21:30:00+00:00") == datetime(2026, 1, 20, 0, 30)
    assert sh.to_msk_naive("2026-10-05T13:32:06.894327+03:00") == datetime(2026, 10, 5, 13, 32, 6, 894327)


def test_history_rows_fields():
    rows = sh.history_rows(record([
        {"dt": "2026-10-05T10:32:06.894327+00:00", "label": "shipping", "author": None},
        {"dt": "2026-01-19T13:54:21+00:00", "label": "updating-operator", "author": "Иванов И.",
         "comment": "  Ожидаем данные  ", "reason": None, "date": None, "files": [{"id": "1"}]},
    ]))
    assert [r["status_label"] for r in rows] == ["shipping", "updating-operator"]
    first, second = rows
    assert first["order_id"] == 137566 and first["bid_id"] == 156321
    assert first["status_dt"] == datetime(2026, 10, 5, 13, 32, 6, 894327)
    assert first["author"] is None and first["files_count"] is None
    assert second["comment"] == "Ожидаем данные"
    assert second["reason"] is None and second["files_count"] == 1


def test_broken_events_skipped():
    rows = sh.history_rows(record([
        {"dt": None, "label": "shipping"},
        {"dt": "not a date", "label": "shipping"},
        {"dt": "2026-10-05T10:32:06+00:00"},
        "garbage",
        {"dt": "2026-10-05T10:32:06+00:00", "label": "executed"},
    ]))
    assert [r["status_label"] for r in rows] == ["executed"]


def test_record_without_id_or_history():
    assert sh.history_rows({"attributes": {"status_history": [{"dt": "2026-10-05T10:32:06+00:00",
                                                                "label": "x"}]}}) == []
    assert sh.history_rows(record(None)) == []
    assert sh.history_rows({"id": "1", "attributes": {}})[:1] == []

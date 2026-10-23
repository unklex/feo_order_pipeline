import importlib.util

import pandas as pd

spec = importlib.util.spec_from_file_location("import_to_db", "1_step0_1_import_to_db.py")
imp = importlib.util.module_from_spec(spec)
spec.loader.exec_module(imp)


def test_active_then_latest_transport_wins():
    df = pd.DataFrame([
        {"id": "111044", "bid_id": "156321", "is_active": False, "dt_update": "2026-10-01 10:00:00"},
        {"id": "137566", "bid_id": "156321", "is_active": True, "dt_update": "2026-09-01 10:00:00"},
        {"id": "2", "bid_id": "200", "is_active": True, "dt_update": "2026-09-01 10:00:00"},
        {"id": "3", "bid_id": "200", "is_active": True, "dt_update": "2026-10-01 10:00:00"},
        {"id": "4", "bid_id": None, "is_active": True, "dt_update": "2026-10-01 10:00:00"},
    ])
    result = imp.one_transport_per_bid(df).set_index("bid_id")["id"].to_dict()
    assert result == {"156321": "137566", "200": "3"}

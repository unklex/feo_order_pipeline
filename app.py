"""
UI пайплайна ФЭО: заказы ФГИС ОПВК -> MySQL -> Битрикс24.

    streamlit run app.py

Откроется http://localhost:8501. Сценарии запускаются через pipeline.py отдельным процессом:
обновление страницы или закрытие вкладки работу не прерывает, а повторный запуск во время
работы невозможен.
"""

import gzip
import io
import json
import os
import re
import signal
import subprocess
import sys
import time
import zipfile
from collections import Counter
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text

import api_export
import api_probe
import env_config  # noqa: F401  (подгружает .env)
import pipeline
from db_tunnel import open_db_tunnel
from pipeline_common import ARCHIVE_DIR, ROOT, export_time, latest_full_export, pending_batch_files, pid_alive

ENTITY_TYPE_ID = int(os.environ.get("BITRIX_ENTITY_TYPE_ID") or 137)
BITRIX_HOST = urlparse(os.environ.get("BITRIX_WEBHOOK_URL", "")).netloc
STEP_ICONS = {"pending": "⏳", "running": "🔄", "ok": "✅", "errors": "⚠️", "failed": "❌", "skipped": "⏭️"}
STATUS_TEXT = {"running": "Выполняется", "ok": "Готово", "failed": "Ошибка",
               "auth_required": "Нужен вход в ФГИС", "stopped": "Остановлено"}

st.set_page_config(page_title="ФЭО: ФГИС → Битрикс24", page_icon="🚚", layout="wide")


# ---------------------------------------------------------------------------
# Процессы
# ---------------------------------------------------------------------------
def launch(args: list, log_path: Path = None) -> int:
    """Запускает python-скрипт отдельным процессом, не привязанным к сессии Streamlit. Возвращает PID.

    log_path — куда писать вывод скрипта (иначе он отбрасывается).
    """
    kwargs = dict(cwd=ROOT, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if os.name == "nt":
        # своя группа (Ctrl+C в окне Streamlit её не задевает) и без консольных окон у шагов
        kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.CREATE_NO_WINDOW
    else:
        kwargs["start_new_session"] = True
    if log_path is None:
        return subprocess.Popen([sys.executable, *args], **kwargs).pid
    kwargs["env"] = dict(os.environ, PYTHONIOENCODING="utf-8", PYTHONUTF8="1", PYTHONUNBUFFERED="1")
    with open(log_path, "wb") as log:
        kwargs.update(stdout=log, stderr=subprocess.STDOUT)
        return subprocess.Popen([sys.executable, *args], **kwargs).pid


def launch_run(args: list, timeout=10.0):
    """Запускает pipeline.py и ждёт, пока он запишет своё состояние «running».

    Иначе страница перерисуется раньше, чем runner создаст pipeline_state.json, и не увидит запуск.
    """
    before = pipeline.read_state().get("started")
    launch(["pipeline.py", *args])
    deadline = time.time() + timeout
    while time.time() < deadline:
        state = pipeline.read_state()
        if state.get("started") != before and state.get("status") == "running":
            return True
        time.sleep(0.3)
    return False


def kill_tree(pid: int) -> str:
    """Завершает процесс с дочерними и ждёт его исчезновения. Возвращает текст ошибки или ''."""
    if os.name == "nt":
        r = subprocess.run(["taskkill", "/PID", str(pid), "/T", "/F"], capture_output=True,
                           text=True, encoding="oem", errors="replace")
        details = (r.stdout + r.stderr).strip()
    else:
        os.killpg(pid, signal.SIGTERM)
        details = ""
    # TerminateProcess асинхронный — ждём, пока процесс действительно исчезнет
    deadline = time.time() + 10
    while pid_alive(pid) and time.time() < deadline:
        time.sleep(0.3)
    if pid_alive(pid):
        return f"Не удалось остановить процесс {pid}: {details or 'процесс всё ещё работает'}"
    return ""


def stop(state: dict) -> str:
    """Останавливает runner вместе с шагами. Возвращает текст ошибки или '' при успехе."""
    error = kill_tree(int(state.get("pid")))
    if error:
        return error
    # runner больше не пишет состояние — отмечаем остановку сами
    state = pipeline.read_state() or state
    state.update(status="stopped", message="Остановлено вручную",
                 finished=datetime.now().isoformat(timespec="seconds"))
    for step in state.get("steps", []):
        if step["status"] in ("running", "pending"):
            step["status"] = "failed" if step["status"] == "running" else "skipped"
    pipeline.STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    return ""


def tail(path, lines=200) -> str:
    """Последние строки файла — читаем только его конец (журнал выгрузки бывает большим)."""
    try:
        with open(path, "rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            f.seek(max(0, size - max(lines * 400, 64_000)))
            data = f.read().decode("utf-8", errors="replace")
    except (FileNotFoundError, OSError, TypeError):
        return ""
    return "\n".join(data.splitlines()[-lines:])


def duration(started: str, finished: str = None) -> str:
    """'4 мин 05 с' между двумя ISO-метками (без finished — до текущего момента)."""
    try:
        end = datetime.fromisoformat(finished) if finished else datetime.now()
        seconds = int((end - datetime.fromisoformat(started)).total_seconds())
    except (TypeError, ValueError):
        return ""
    return f"{seconds // 60} мин {seconds % 60:02d} с" if seconds >= 60 else f"{seconds} с"


def current_state() -> dict:
    state = pipeline.read_state()
    if state.get("status") == "running" and not pipeline.is_running(state):
        state["status"] = "stopped"  # процесс завершился, не обновив состояние (закрыт/убит)
        state["message"] = state.get("message") or "Процесс прервался"
    return state


# ---------------------------------------------------------------------------
# Данные
# ---------------------------------------------------------------------------
@st.cache_data(ttl=60, show_spinner="Читаю БД…")
def db_stats() -> dict:
    tunnel = open_db_tunnel(os.environ.get("DB_HOST", "localhost"), int(os.environ.get("DB_PORT") or 3306))
    try:
        engine = create_engine(
            f"mysql+pymysql://{os.environ['DB_USER']}:{os.environ['DB_PASSWORD']}"
            f"@{tunnel.local_bind_host}:{tunnel.local_bind_port}/{os.environ['DB_NAME']}",
            connect_args={"connect_timeout": 10},
        )
        with engine.connect() as conn:
            row = conn.execute(text("""
                SELECT COUNT(*) AS total,
                       SUM(is_active = 1) AS active,
                       SUM(bitrix_id IS NULL) AS no_card,
                       SUM(is_active = 1 AND (parsed = 0 OR parsed IS NULL)) AS not_parsed,
                       SUM(is_active = 1 AND parsed = 1 AND bitrix_filled = 0 AND bitrix_id IS NOT NULL) AS waiting_fill,
                       SUM(bitrix_filled = 1) AS filled,
                       MAX(updated_at) AS last_update
                FROM new_webapi_orders_transportation
            """)).mappings().one()
        engine.dispose()
        return {k: (int(v) if isinstance(v, (int, float)) or hasattr(v, "as_integer_ratio") else v)
                for k, v in dict(row).items()}
    finally:
        tunnel.stop()


@st.cache_data(show_spinner=False)
def export_statuses(path: str, mtime: float) -> pd.DataFrame:
    """Статусы заказов в выгрузке ФГИС (кэш по пути и времени изменения файла)."""
    counts, active = Counter(), Counter()
    with gzip.open(path, "rt", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                attrs = json.loads(line).get("attributes", {})
                counts[attrs.get("status_label")] += 1
                active[attrs.get("status_label")] += bool(attrs.get("is_active"))
    return (pd.DataFrame({"Статус": list(counts), "Всего": list(counts.values()),
                          "Активных": [active[k] for k in counts]})
            .sort_values("Всего", ascending=False, ignore_index=True))


def load_audit(path: Path) -> pd.DataFrame:
    return pd.read_json(path, lines=True) if path.stat().st_size else pd.DataFrame()


# ---------------------------------------------------------------------------
# Вкладка «Запуск»
# ---------------------------------------------------------------------------
def scenario_buttons(state: dict):
    busy = state.get("status") == "running"
    latest = latest_full_export()

    c1, c2 = st.columns(2)
    with c1, st.container(border=True):
        st.subheader("Новые заказы")
        st.caption("Выгрузка из ФГИС → импорт в БД → карточки в Битрикс → заявки из ФГИС → "
                   "заполнение карточек → статусы. Зависшие заявки (распарсены, но не заполнены) "
                   "запрашиваются заново.")
        if st.button("▶ Добавить и заполнить", type="primary", disabled=busy, use_container_width=True):
            with st.spinner("Запускаю…"):
                started = launch_run(["add"])
            st.toast("Запущено: добавление новых заказов" if started else "Запуск не подтвердился — см. журнал")
            st.rerun()
    with c2, st.container(border=True):
        st.subheader("Статусы")
        st.caption("Выгрузка из ФГИС → обновление статуса, истории, суммы и транспортных задач "
                   "в карточках Битрикс.")
        skip = st.checkbox(
            f"Не скачивать заново — взять выгрузку от {export_time(latest):%d.%m.%Y %H:%M}" if latest
            else "Не скачивать заново (выгрузки пока нет)",
            disabled=busy or latest is None)
        if st.button("▶ Обновить статусы", type="primary", disabled=busy, use_container_width=True):
            with st.spinner("Запускаю…"):
                started = launch_run(["statuses", *(["--skip-download"] if skip else [])])
            st.toast("Запущено: обновление статусов" if started else "Запуск не подтвердился — см. журнал")
            st.rerun()

    with st.container(border=True):
        a, b, c = st.columns([1, 1, 3])
        if a.button("Проверить вход в ФГИС", disabled=busy):
            with st.spinner("Проверяю…"):
                r = subprocess.run([sys.executable, "login_fgis.py", "--check"], cwd=ROOT,
                                   capture_output=True, text=True, encoding="utf-8", errors="replace",
                                   env=dict(os.environ, PYTHONIOENCODING="utf-8"))
            (c.success if r.returncode == 0 else c.warning)(
                "Сохранённый вход работает" if r.returncode == 0 else "Вход недействителен — нажмите «Войти в ФГИС»")
        if b.button("Войти в ФГИС", disabled=busy):
            launch(["login_fgis.py"])
            c.info("На этом компьютере откроется Chrome — войдите на gisopvk.ru, окно закроется само "
                   "(ждём до 10 минут). Затем можно запускать сценарий.")


@st.fragment(run_every="2s")
def run_panel():
    state = current_state()
    if not state:
        st.info("Запусков ещё не было.")
        return
    status = state.get("status")
    # Запуск начался или закончился — перерисовать всю страницу: кнопки сценариев и «Остановить»
    # живут вне этого фрагмента и должны включиться/выключиться.
    seen = (state.get("started"), status == "running")
    previous = st.session_state.get("run_seen")
    st.session_state["run_seen"] = seen
    if previous is not None and previous != seen:
        st.rerun(scope="app")
    st.markdown(f"**{state.get('title')}** — {STATUS_TEXT.get(status, status)}  \n"
                f"Начало: {state.get('started', '').replace('T', ' ')}"
                + (f" · конец: {state['finished'].replace('T', ' ')}" if state.get("finished") else "")
                + (f" · выгрузка: `{state['export']}`" if state.get("export") else ""))
    if status == "ok":
        st.success(state.get("message") or "Готово")
    elif status == "auth_required":
        st.warning(state.get("message"))
    elif status in ("failed", "stopped"):
        st.error(state.get("message") or STATUS_TEXT[status])

    steps = state.get("steps", [])
    st.markdown("  \n".join(f"{STEP_ICONS.get(s['status'], '')} {s['title']}"
                            + (f" — {duration(s['started'], s.get('finished'))}" if s.get("started") else "")
                            + (f" (код {s['rc']})" if s.get("rc") not in (None, 0) else "")
                            for s in steps))

    current = next((s for s in steps if s["status"] == "running"), None) if status == "running" else None
    if current:
        number = steps.index(current) + 1
        p = current.get("progress") or {}
        label = f"Шаг {number}/{len(steps)}: {current['title']} · {duration(current['started'])}"
        if p.get("total"):
            frac = min(p["done"] / p["total"], 1.0)
            st.progress(frac, text=f"{label} · {p['done']} из {p['total']} {p['unit']} ({frac:.0%})")
        else:
            st.progress(0, text=f"{label} · подготовка…")

    log_file = state.get("log_file", "")
    st.code(tail(log_file, 20) or "(журнал пуст)", language=None)
    # весь журнал рисуем только по запросу: перерисовка тысяч строк раз в 2 с тормозит страницу
    if st.toggle("Показать весь журнал запуска"):
        st.code(tail(log_file, 3000) or "(журнал пуст)", language=None, height=500)


def run_tab():
    state = current_state()
    scenario_buttons(state)
    st.divider()
    st.subheader("Текущий / последний запуск")
    # «Остановить» — вне обновляемого раз в 2 с фрагмента: кнопка внутри него пересоздаётся
    # при каждой перерисовке, и нажатие могло теряться
    if state.get("status") == "running":
        if st.button("■ Остановить запуск", type="secondary"):
            with st.spinner("Останавливаю…"):
                error = stop(state)
            if error:
                st.error(error)
            else:
                st.toast("Запуск остановлен")
                st.rerun()
    run_panel()
    with st.expander("Прошлые запуски"):
        logs = sorted(pipeline.LOG_DIR.glob("pipeline_*.log"), reverse=True)
        if logs:
            chosen = st.selectbox("Журнал", logs, format_func=lambda p: p.name)
            st.code(tail(chosen, 400) or "(пусто)", language=None)
        else:
            st.caption("Журналов пока нет.")


# ---------------------------------------------------------------------------
# Вкладка «Данные»
# ---------------------------------------------------------------------------
def data_tab():
    if st.button("Обновить"):
        db_stats.clear()
    try:
        s = db_stats()
        cols = st.columns(6)
        cols[0].metric("Заказов в БД", s["total"])
        cols[1].metric("Активных", s["active"])
        cols[2].metric("Без карточки в Битрикс", s["no_card"])
        cols[3].metric("Ждут загрузки заявки", s["not_parsed"])
        cols[4].metric("Ждут заполнения", s["waiting_fill"],
                       help="Распарсены, карточка есть, но не заполнена. Сценарий «Новые заказы» "
                            "заполнит их или запросит заявки заново.")
        cols[5].metric("Заполнено", s["filled"])
        st.caption(f"Последнее изменение в БД: {s['last_update']}")
    except Exception as e:
        st.error(f"Не удалось прочитать БД: {e}")

    st.divider()
    left, right = st.columns(2)
    with left:
        st.subheader("Файлы")
        latest = latest_full_export()
        if latest:
            age = datetime.now() - export_time(latest)
            st.markdown(f"Последняя выгрузка: `{latest.name}`  \n"
                        f"{export_time(latest):%d.%m.%Y %H:%M} ({age.days} дн. {age.seconds // 3600} ч назад)")
        else:
            st.markdown("Полных выгрузок нет.")
        batches = pending_batch_files()
        st.markdown(f"Файлов заявок, ждущих заполнения: **{len(batches)}**")
        if batches:
            st.caption(", ".join(p.name for p in batches))
        folders = sorted(p.name for p in ARCHIVE_DIR.iterdir() if p.is_dir()) if ARCHIVE_DIR.is_dir() else []
        st.markdown(f"Архив: {len(folders)} папок" + (f" ({folders[0]} … {folders[-1]})" if folders else "")
                    + f"; удаляется через {os.environ.get('ARCHIVE_KEEP_DAYS') or 30} дн.")
    with right:
        st.subheader("Статусы в последней выгрузке ФГИС")
        if latest:
            st.dataframe(export_statuses(str(latest), latest.stat().st_mtime), hide_index=True,
                         use_container_width=True)


# ---------------------------------------------------------------------------
# Вкладка «Журнал Битрикс»
# ---------------------------------------------------------------------------
def audit_tab():
    # пустые журналы (запуск, которому нечего было делать) не показываем
    files = sorted((p for p in pipeline.LOG_DIR.glob("audit_*.jsonl") if p.stat().st_size),
                   key=lambda p: p.stat().st_mtime, reverse=True)
    if not files:
        st.info("Журналов пока нет — они появятся после первого запуска.")
        return
    chosen = st.selectbox("Запуск", files, format_func=lambda p: p.stem.replace("audit_", ""))
    df = load_audit(chosen)
    if df.empty:
        st.caption("Журнал пуст.")
        return

    summary = df[df["event"] == "run_summary"]
    if not summary.empty:
        row = summary.iloc[-1].dropna().drop(labels=["ts", "script", "event"], errors="ignore")
        st.json(row.to_dict(), expanded=False)
    st.caption("События: " + ", ".join(f"{k} — {v}" for k, v in df["event"].value_counts().items()))

    items = df[df["event"].isin(["item_update", "item_add"])].copy()
    if items.empty:
        st.caption("Обращений к карточкам в этом запуске не было.")
    else:
        if st.toggle("Только ошибки"):
            items = items[~items["ok"].astype(bool)]
        if BITRIX_HOST and "item_id" in items:
            items["Карточка"] = items["item_id"].map(
                lambda i: f"https://{BITRIX_HOST}/crm/type/{ENTITY_TYPE_ID}/details/{int(i)}/" if pd.notna(i) else None)
        if "fields" in items:
            items["fields"] = items["fields"].map(
                lambda f: json.dumps(f, ensure_ascii=False) if isinstance(f, dict) else f)
        cols = [c for c in ["ts", "event", "item_id", "bid_id", "ok", "error", "Карточка", "fields"] if c in items]
        st.dataframe(items[cols], hide_index=True, use_container_width=True,
                     column_config={"Карточка": st.column_config.LinkColumn(display_text="открыть"),
                                    "ok": st.column_config.CheckboxColumn("ok")})

    other = df[~df["event"].isin(["item_update", "item_add", "run_summary"])]
    if not other.empty:
        with st.expander("Прочие события (пропуски, ошибки, архив)"):
            st.dataframe(other.dropna(axis=1, how="all"), hide_index=True, use_container_width=True)


# ---------------------------------------------------------------------------
# Фоновые запуски вне пайплайна (исследование API, телеметрия)
# ---------------------------------------------------------------------------
REPORTS_DIR = ROOT / "reports"


def bg_state(kind: str) -> dict:
    """Состояние фонового запуска kind ('api_probe', 'telemetry') + running по живости процесса."""
    try:
        state = json.loads((REPORTS_DIR / f"{kind}_state.json").read_text(encoding="utf-8"))
    except (FileNotFoundError, ValueError):
        return {}
    state["running"] = pid_alive(state.get("pid"))
    return state


def bg_start(kind: str, args: list, **extra):
    REPORTS_DIR.mkdir(exist_ok=True)
    log = REPORTS_DIR / f"{kind}_run_{datetime.now():%Y%m%d_%H%M%S}.log"
    pid = launch(args, log_path=log)
    state = {"pid": pid, "started": datetime.now().isoformat(timespec="seconds"), "log": str(log),
             "args": args[1:], **extra}
    (REPORTS_DIR / f"{kind}_state.json").write_text(json.dumps(state, ensure_ascii=False), encoding="utf-8")


def bg_finished_rerun(kind: str, running: bool):
    """Запуск начался/закончился — перерисовать страницу целиком (кнопки вне фрагмента)."""
    key = f"{kind}_seen"
    previous = st.session_state.get(key)
    st.session_state[key] = running
    if previous is not None and previous != running:
        st.rerun(scope="app")


def busy_reason() -> str:
    """Что уже нагружает ФГИС: тогда новый запуск не даём."""
    if current_state().get("status") == "running":
        return "Идёт сценарий пайплайна"
    if bg_state("api_probe").get("running"):
        return "Идёт исследование API"
    if bg_state("telemetry").get("running"):
        return "Идёт выгрузка телеметрии"
    if bg_state("export").get("running"):
        return "Идёт выгрузка списка"
    return ""


# ---------------------------------------------------------------------------
# Вкладка «API ФГИС» (api_probe.py)
# ---------------------------------------------------------------------------
def probe_state() -> dict:
    return bg_state("api_probe")


def probe_reports() -> list:
    return sorted((p for p in REPORTS_DIR.glob("api_probe_*") if p.is_dir()), reverse=True)


def zip_folder(folder: Path) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for f in folder.rglob("*"):
            if f.is_file():
                z.write(f, f.relative_to(folder.parent))
    return buf.getvalue()


def start_probe(deep: list, extra: str, raw: bool, sample: int) -> str:
    """Запускает api_probe.py. Возвращает текст ошибки или ''."""
    args = ["api_probe.py", "--sample", str(sample)]
    for name in deep:
        args += ["--deep", name]
    for line in extra.splitlines():
        line = line.strip()
        if not line:
            continue
        name, _, url = line.partition("=")
        if not name.strip() or not url.strip().startswith("http"):
            return f"Строка «{line}»: нужно ИМЯ=URL"
        args += ["--endpoint", f"{name.strip()}={url.strip()}"]
        if st.session_state.get("probe_deep_extra"):
            args += ["--deep", name.strip()]
    if raw:
        args.append("--raw")
    bg_start("api_probe", args)
    return ""


@st.fragment(run_every="2s")
def probe_panel():
    state = probe_state()
    if not state:
        st.caption("Исследование ещё не запускалось.")
        return
    bg_finished_rerun("api_probe", state["running"])
    log = tail(state.get("log"), 400)
    requests = re.findall(r"\[(\d+) запр\.\]", log)
    head = f"Запуск {state.get('started', '').replace('T', ' ')} · {duration(state.get('started'))}"
    if state["running"]:
        st.info(f"{head} · идёт" + (f" · запросов: {requests[-1]}" if requests else ""))
    elif "login_fgis" in log:
        st.warning("Сохранённый вход в ФГИС недействителен — нажмите «Войти в ФГИС» на вкладке «Запуск».")
    elif "Готово:" in log:
        st.success(f"{head} · готово")
    else:
        st.error(f"{head} · завершился без отчёта — см. журнал")
    st.code("\n".join(log.splitlines()[-15:]) or "(журнал пуст)", language=None)


def api_tab():
    st.caption("Только чтение: GET/OPTIONS к gisopvk.ru с сохранённым входом, пауза между запросами. "
               "Проверяет поля эндпоинтов, связи, какие sort/фильтры/include сервер применяет "
               "и можно ли скачать файлы (вложения, фото, подписи, счета, УПД, акты — первые 8 КБ, без сохранения). "
               "Обычно 5–15 минут.")
    state = probe_state()
    running = state.get("running", False)
    busy = busy_reason() if not running else ""

    with st.container(border=True):
        names = list(api_probe.ENDPOINTS)
        deep = st.multiselect("Проверка параметров (sort, фильтры, include, пагинация) для",
                              names, default=api_probe.DEFAULT_DEEP)
        extra = st.text_area("Дополнительные эндпоинты (по одному в строке: ИМЯ=URL)",
                             placeholder="driver_task=https://gisopvk.ru/webapi/v1/driver_task/", height=80)
        c1, c2, c3 = st.columns(3)
        c1.checkbox("Проверять параметры и у дополнительных", key="probe_deep_extra")
        raw = c2.checkbox("Без маскирования (телефоны, ФИО, ИНН — как есть)")
        sample = c3.number_input("Записей в выборке", min_value=5, max_value=100, value=20, step=5)
        b1, b2, b3 = st.columns([1, 1, 2])
        if b1.button("🔎 Исследовать API", type="primary", disabled=running or bool(busy),
                     use_container_width=True):
            error = start_probe(deep, extra, raw, int(sample))
            if error:
                b3.error(error)
            else:
                st.toast("Исследование API запущено")
                st.rerun()
        if running and b2.button("■ Остановить", use_container_width=True):
            error = kill_tree(int(state["pid"]))
            (b3.error(error) if error else st.toast("Остановлено"))
            st.rerun()
        if busy:
            b3.caption(f"{busy} — исследование можно запустить после него.")

    probe_panel()

    reports = probe_reports()
    if not reports:
        return
    st.divider()
    chosen = st.selectbox("Отчёт", reports, format_func=lambda p: p.name.replace("api_probe_", ""))
    md, js = chosen / "report.md", chosen / "report.json"
    d1, d2, d3 = st.columns(3)
    if md.exists():
        d1.download_button("⬇ report.md", md.read_bytes(), file_name=f"{chosen.name}.md",
                           use_container_width=True)
    if js.exists():
        d2.download_button("⬇ report.json", js.read_bytes(), file_name=f"{chosen.name}.json",
                           use_container_width=True)
    d3.download_button("⬇ Всё (zip)", zip_folder(chosen), file_name=f"{chosen.name}.zip",
                       use_container_width=True)
    if md.exists():
        with st.container(border=True, height=700):
            st.markdown(md.read_text(encoding="utf-8"))
    else:
        st.caption("Отчёт ещё не записан.")


# ---------------------------------------------------------------------------
# Вкладка «Телеметрия» (telemetry_download.py)
# ---------------------------------------------------------------------------
RE_TEL_TOTAL = re.compile(r"заданий: (\d+) \(уже готово: (\d+)\)")
RE_TEL_DONE = re.compile(r"готово (\d+)/(\d+) заданий, точек в этом запуске: (\d+)")


def default_telemetry_dir() -> str:
    last = bg_state("telemetry").get("out")
    if last:
        return last
    if os.environ.get("TELEMETRY_DIR"):
        return os.environ["TELEMETRY_DIR"]
    return str(ROOT / "telemetry")


def size_text(n: int) -> str:
    for unit in ("Б", "КБ", "МБ", "ГБ"):
        if n < 1024 or unit == "ГБ":
            return f"{n:.0f} {unit}" if unit == "Б" else f"{n:.1f} {unit}"
        n /= 1024


@st.fragment(run_every="2s")
def telemetry_panel():
    state = bg_state("telemetry")
    if not state:
        st.caption("Выгрузка телеметрии ещё не запускалась.")
        return
    bg_finished_rerun("telemetry", state["running"])
    log = tail(state.get("log"), 400)
    head = (f"{'Проверка' if state.get('dry_run') else 'Выгрузка'} "
            f"{state.get('started', '').replace('T', ' ')} · {duration(state.get('started'))} · "
            f"папка `{state.get('out')}`")
    done = RE_TEL_DONE.findall(log)
    if state["running"]:
        st.info(f"{head} · идёт")
        if done:
            n, total, pts = map(int, done[-1])
            st.progress(min(n / max(total, 1), 1.0), text=f"{n} из {total} окон (сутки × машина) · "
                                                           f"точек: {pts:,}".replace(",", " "))
        else:
            st.progress(0, text="список машин и подготовка…")
    elif "login_fgis" in log or "Нужен вход" in log:
        st.warning("Вход в ФГИС недействителен или истёк — нажмите «Войти в ФГИС» на вкладке «Запуск» "
                   "и запустите снова: выгрузка продолжится с места остановки.")
    elif "Итого:" in log or (state.get("dry_run") and RE_TEL_TOTAL.search(log)):
        failed = re.findall(r"заданий с ошибкой (\d+)", log)
        if failed and int(failed[-1]):
            st.warning(f"{head} · готово, но {failed[-1]} окон не скачались — запустите снова, они повторятся")
        else:
            st.success(f"{head} · готово")
    else:
        st.error(f"{head} · остановлено или прервано — запустите снова, продолжит с места остановки")
    st.code("\n".join(log.splitlines()[-12:]) or "(журнал пуст)", language=None)


def telemetry_results(folder: Path):
    if not folder.is_dir():
        return
    parts = sorted(folder.glob("telemetry_*.csv.gz"))
    summary, vehicles = folder / "summary.csv", folder / "vehicles.csv"
    if not (parts or summary.exists() or vehicles.exists()):
        return
    st.divider()
    st.subheader("Скачано")
    total = sum(p.stat().st_size for p in parts)
    m1, m2, m3 = st.columns(3)
    m1.metric("Файлов с точками", len(parts))
    m2.metric("Объём (gzip)", size_text(total))
    if summary.exists():
        df = pd.read_csv(summary, dtype={"vehicle_id": str, "tracker_id": str})  # трекер «IMEI.порт» — не число
        m3.metric("Машин с точками / точек", f"{len(df)} / {int(df['points'].sum()):,}".replace(",", " "))
        st.dataframe(df.sort_values("points", ascending=False), hide_index=True, use_container_width=True,
                     height=300)
    d1, d2, _ = st.columns(3)
    if summary.exists():
        d1.download_button("⬇ summary.csv", summary.read_bytes(), file_name="summary.csv",
                           use_container_width=True)
    if vehicles.exists():
        d2.download_button("⬇ vehicles.csv", vehicles.read_bytes(), file_name="vehicles.csv",
                           use_container_width=True)
    if parts:
        st.caption(f"Точки — в папке `{folder}`: " + ", ".join(p.name for p in parts[-5:])
                   + (f" и ещё {len(parts) - 5}" if len(parts) > 5 else "")
                   + ". Колонки: vehicle_id, plate, tracker_id, date_time, lat, lon, speed, course, sat, "
                     "telemetry_state. Открываются pandas (`pd.read_csv(файл)`), 7-Zip, DBeaver.")


def telemetry_tab():
    st.caption("Только чтение: история точек ТС из ФГИС (/webapi/v1/tracking/telemetry/) с сохранённым входом. "
               "Пустые дни пропускаются быстро: период машины обрезается датой её последней телеметрии, а месяц "
               "без точек проверяется одним запросом. Остановить можно в любой момент — повторный запуск "
               "докачает только недостающее (и продолжит прошлую выгрузку в той же папке).")
    state = bg_state("telemetry")
    running = state.get("running", False)
    busy = busy_reason() if not running else ""

    with st.container(border=True):
        c1, c2, c3 = st.columns([1, 1, 2])
        day_from = c1.date_input("С", value=datetime(2025, 1, 1).date(), format="DD.MM.YYYY")
        day_to = c2.date_input("По (включительно)", value=datetime.now().date(), format="DD.MM.YYYY")
        out = c3.text_input("Папка результата", value=default_telemetry_dir(),
                            help="На диске с запасом места: за весь период это десятки ГБ.")
        c4, c5, c6 = st.columns([1, 2, 1])
        all_vehicles = c4.checkbox("И машины без телеметрии", help="По умолчанию — только те, у кого она была.")
        only = c5.text_input("Только эти vehicle_id (через запятую)", placeholder="10469, 10566",
                             help="Для проверки на одной-двух машинах.")
        rps = c6.number_input("Запросов в секунду", min_value=1, max_value=10, value=5,
                              help="Нагрузка на портал ФГИС. Больше 5 — только если не приходят ошибки 429.")
        b1, b2, b3, b4 = st.columns([1, 1, 1, 2])
        base = ["telemetry_download.py", "--from", f"{day_from:%Y-%m-%d}", "--to", f"{day_to:%Y-%m-%d}",
                "--out", out.strip()]
        base += ["--rps", str(int(rps))]
        base += ["--all-vehicles"] if all_vehicles else []
        base += ["--vehicles", only.replace(" ", "")] if only.strip() else []
        error = ""
        if day_from > day_to:
            error = "«С» позже «По»"
        elif not out.strip():
            error = "Укажите папку"
        if b1.button("Проверить", disabled=running or bool(busy) or bool(error), use_container_width=True,
                     help="Только список машин и число запросов, без скачивания точек."):
            bg_start("telemetry", base + ["--dry-run"], out=out.strip(), dry_run=True)
            st.rerun()
        if b2.button("▶ Скачать", type="primary", disabled=running or bool(busy) or bool(error),
                     use_container_width=True):
            bg_start("telemetry", base, out=out.strip(), dry_run=False)
            st.toast("Выгрузка телеметрии запущена")
            st.rerun()
        if running and b3.button("■ Остановить", use_container_width=True):
            err = kill_tree(int(state["pid"]))
            (b4.error(err) if err else st.toast("Остановлено — продолжить можно кнопкой «Скачать»"))
            st.rerun()
        if error:
            b4.error(error)
        elif busy:
            b4.caption(f"{busy} — выгрузку можно запустить после него.")

    telemetry_panel()
    telemetry_results(Path(state.get("out") or out.strip()))


# ---------------------------------------------------------------------------
# Вкладка «Выгрузки» (api_export.py): водители, ТС, транспортные задачи…
# ---------------------------------------------------------------------------
@st.fragment(run_every="2s")
def export_panel():
    state = bg_state("export")
    if not state:
        return
    bg_finished_rerun("export", state["running"])
    log = tail(state.get("log"), 200)
    head = f"{state.get('title')} · {state.get('started', '').replace('T', ' ')} · {duration(state.get('started'))}"
    if state["running"]:
        st.info(f"{head} · идёт")
    elif "login_fgis" in log or "Нет доступа" in log:
        st.warning("Вход в ФГИС недействителен — нажмите «Войти в ФГИС» на вкладке «Запуск» и повторите.")
    elif "Готово:" in log:
        st.success(f"{head} · готово")
    else:
        st.error(f"{head} · не получилось — см. журнал")
    st.code("\n".join(log.splitlines()[-12:]) or "(журнал пуст)", language=None)


def export_tab():
    st.caption("Только чтение: список целиком из API ФГИС (данные своей организации — ЭКОПРОМ) в CSV для Excel "
               "и JSONL. Файлы с личными данными (ФИО, телефоны, документы) хранятся в папке exports/ "
               "на этом компьютере и в git не попадают.")
    state = bg_state("export")
    running = state.get("running", False)
    busy = busy_reason() if not running else ""
    names = list(api_export.DATASETS)
    with st.container(border=True):
        c1, c2 = st.columns([1, 2])
        dataset = c1.selectbox("Что выгрузить", names, format_func=lambda n: api_export.DATASETS[n][0])
        url = c2.text_input("Или свой адрес эндпоинта", placeholder="https://gisopvk.ru/webapi/v1/driver/",
                            help="Если адрес из списка не подошёл — возьмите настоящий из отчёта «Исследовать API».")
        b1, b2 = st.columns([1, 3])
        error = ""
        if url.strip() and not api_probe.is_portal_url(url.strip()):
            error = "Адрес должен быть на https://gisopvk.ru/"
        if b1.button("▶ Выгрузить", type="primary", disabled=running or bool(busy) or bool(error),
                     use_container_width=True):
            args = ["api_export.py"] + (["--url", url.strip(), "--name", dataset] if url.strip() else [dataset])
            bg_start("export", args, title=api_export.DATASETS[dataset][0])
            st.rerun()
        if error:
            b2.error(error)
        elif busy:
            b2.caption(f"{busy} — выгрузку можно запустить после него.")
        else:
            b2.caption("Адреса, которые пробуются: "
                       + ", ".join(u.replace(api_probe.HOST, "") for u in api_export.DATASETS[dataset][1]))

    export_panel()

    folders = sorted((p for p in api_export.EXPORT_DIR.glob("*_*") if (p / "records.csv").exists()),
                     key=lambda p: p.stat().st_mtime, reverse=True)
    if not folders:
        return
    st.divider()

    def label(folder: Path) -> str:
        try:
            info = json.loads((folder / "info.json").read_text(encoding="utf-8"))
            title = api_export.DATASETS.get(info["name"], (info["name"],))[0]
            return f"{title} · {info['exported_at'].replace('T', ' ')} · {info['records']} зап."
        except (FileNotFoundError, ValueError, KeyError):
            return folder.name

    chosen = st.selectbox("Выгрузка", folders, format_func=label)
    csv_path, raw_path = chosen / "records.csv", chosen / "records.jsonl.gz"
    d1, d2, _ = st.columns(3)
    d1.download_button("⬇ CSV (Excel)", csv_path.read_bytes(), file_name=f"{chosen.name}.csv",
                       use_container_width=True)
    if raw_path.exists():
        d2.download_button("⬇ JSONL (всё как в API)", raw_path.read_bytes(), file_name=f"{chosen.name}.jsonl.gz",
                           use_container_width=True)
    df = pd.read_csv(csv_path, dtype=str, encoding="utf-8-sig", sep=api_export.CSV_DELIMITER)
    st.caption(f"{len(df)} записей, {len(df.columns)} колонок · папка `{chosen}`")
    st.dataframe(df.head(500), hide_index=True, use_container_width=True, height=420)


# ---------------------------------------------------------------------------
st.title("ФЭО: заказы ФГИС → Битрикс24")
tab_run, tab_data, tab_audit, tab_api, tab_tel, tab_exp = st.tabs(
    ["Запуск", "Данные", "Журнал Битрикс", "API ФГИС", "Телеметрия", "Выгрузки"])
with tab_run:
    run_tab()
with tab_data:
    data_tab()
with tab_audit:
    audit_tab()
with tab_api:
    api_tab()
with tab_tel:
    telemetry_tab()
with tab_exp:
    export_tab()

"""
UI пайплайна ФЭО: заказы ФГИС ОПВК -> MySQL -> Битрикс24.

    streamlit run app.py

Откроется http://localhost:8501. Сценарии запускаются через pipeline.py отдельным процессом:
обновление страницы или закрытие вкладки работу не прерывает, а повторный запуск во время
работы невозможен.
"""

import gzip
import json
import os
import signal
import subprocess
import sys
import time
from collections import Counter
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text

import env_config  # noqa: F401  (подгружает .env)
import pipeline
from db_tunnel import open_db_tunnel
from pipeline_common import ARCHIVE_DIR, ROOT, export_time, latest_full_export, pending_batch_files, pid_alive

ENTITY_TYPE_ID = int(os.environ.get("BITRIX_ENTITY_TYPE_ID") or 137)
BITRIX_HOST = urlparse(os.environ.get("BITRIX_WEBHOOK_URL", "")).netloc
STEP_ICONS = {"pending": "⏳", "running": "🔄", "ok": "✅", "failed": "❌", "skipped": "⏭️"}
STATUS_TEXT = {"running": "Выполняется", "ok": "Готово", "failed": "Ошибка",
               "auth_required": "Нужен вход в ФГИС", "stopped": "Остановлено"}

st.set_page_config(page_title="ФЭО: ФГИС → Битрикс24", page_icon="🚚", layout="wide")


# ---------------------------------------------------------------------------
# Процессы
# ---------------------------------------------------------------------------
def launch(args: list):
    """Запускает python-скрипт отдельным процессом, не привязанным к сессии Streamlit."""
    kwargs = dict(cwd=ROOT, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if os.name == "nt":
        # своя группа (Ctrl+C в окне Streamlit её не задевает) и без консольных окон у шагов
        kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.CREATE_NO_WINDOW
    else:
        kwargs["start_new_session"] = True
    subprocess.Popen([sys.executable, *args], **kwargs)


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


def stop(state: dict) -> str:
    """Останавливает runner вместе с шагами. Возвращает текст ошибки или '' при успехе."""
    pid = int(state.get("pid"))
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
st.title("ФЭО: заказы ФГИС → Битрикс24")
tab_run, tab_data, tab_audit = st.tabs(["Запуск", "Данные", "Журнал Битрикс"])
with tab_run:
    run_tab()
with tab_data:
    data_tab()
with tab_audit:
    audit_tab()

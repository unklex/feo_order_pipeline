"""
Доступ к MySQL: через SSH-туннель или напрямую (запуск внутри сети).

    DB_USE_SSH=1 (по умолчанию) — как раньше: туннель через SSH-шлюз, DB_HOST — адрес БД
                 относительно SSH-хоста (обычно localhost).
    DB_USE_SSH=0 — без туннеля: подключение к DB_HOST:DB_PORT напрямую. Переменные SSH_*
                 не нужны, пакеты sshtunnel/paramiko можно не устанавливать.

open_db_tunnel() в обоих режимах возвращает объект с интерфейсом SSHTunnelForwarder
(local_bind_host, local_bind_port, is_active, stop()), поэтому код скриптов не различает режимы:
    tunnel = open_db_tunnel(DB_HOST, DB_PORT)
    url = f"mysql+pymysql://...@{tunnel.local_bind_host}:{tunnel.local_bind_port}/{DB_NAME}"
"""

import logging
import os

import env_config  # noqa: F401  (подгружает .env рядом с модулем, независимо от рабочей папки)

logger = logging.getLogger(__name__)


def use_ssh() -> bool:
    return os.environ.get("DB_USE_SSH", "1").strip().lower() not in ("0", "false", "no", "off")


class DirectConnection:
    """Прямое подключение к БД под интерфейсом SSHTunnelForwarder (туннеля нет)."""

    is_active = True

    def __init__(self, host: str, port: int):
        self.local_bind_host = host
        self.local_bind_port = port

    def stop(self):
        pass


def _require(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(
            f"Не задана переменная окружения {name} (нужна для SSH-туннеля). "
            f"Заполните .env или отключите туннель: DB_USE_SSH=0."
        )
    return value


def open_db_tunnel(db_host: str, db_port: int, *, prefix: str = "SSH", **tunnel_kwargs):
    """Открывает SSH-туннель к db_host:db_port (или прямое подключение при DB_USE_SSH=0).

    prefix — набор переменных шлюза: "SSH" (SSH_HOST, SSH_PORT, ...) или "SSH_ALT".
    tunnel_kwargs — доп. параметры SSHTunnelForwarder (set_keepalive, compression, ...).
    """
    if not use_ssh():
        logger.info(f"DB_USE_SSH=0: прямое подключение к БД {db_host}:{db_port}")
        return DirectConnection(db_host, int(db_port))

    import paramiko
    # Совместимость sshtunnel с paramiko >= 3.0, где удалён класс DSSKey
    if not hasattr(paramiko, "DSSKey"):
        paramiko.DSSKey = paramiko.RSAKey
    from sshtunnel import SSHTunnelForwarder

    # Вход по ключу (SSH_KEY_PATH) приоритетнее пароля. allow_agent=False и
    # host_pkey_directories=[] — не перебирать ключи из ssh-agent и ~/.ssh: каждый
    # отклонённый ключ — неудачный вход для fail2ban (бан IP на 7 дней).
    key_path = os.environ.get(f"{prefix}_KEY_PATH")
    if key_path:
        auth = {"ssh_pkey": key_path, "ssh_private_key_password": os.environ.get(f"{prefix}_KEY_PASSPHRASE") or None}
    else:
        auth = {"ssh_password": _require(f"{prefix}_PASSWORD")}

    ssh_host = _require(f"{prefix}_HOST")
    ssh_port = int(os.environ.get(f"{prefix}_PORT") or 22)
    tunnel = SSHTunnelForwarder(
        (ssh_host, ssh_port),
        ssh_username=_require(f"{prefix}_USERNAME"),
        **auth,
        allow_agent=False,
        host_pkey_directories=[],
        remote_bind_address=(db_host, int(db_port)),
        **tunnel_kwargs,
    )
    tunnel.start()
    logger.info(f"SSH-туннель через {ssh_host}:{ssh_port} установлен. Локальный порт: {tunnel.local_bind_port}")
    return tunnel

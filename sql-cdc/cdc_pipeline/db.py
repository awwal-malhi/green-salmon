"""SQL connection helpers."""

from __future__ import annotations

import logging
import re
import time
from pathlib import Path

import pyodbc

from .config import SqlSettings

log = logging.getLogger(__name__)

# Errors worth retrying on connect: serverless resume (40613), throttling/transient
# (40197, 40501, 49918-49920, 4060 while resuming) and plain network timeouts (HYT00, 08001).
_TRANSIENT_MARKERS = ("40613", "40197", "40501", "49918", "49919", "49920", "4060", "HYT00", "08001", "08S01")

SQL_DIR = Path(__file__).resolve().parent.parent / "sql"


def connect(settings: SqlSettings | None = None, *, attempts: int = 8, autocommit: bool = False) -> pyodbc.Connection:
    settings = settings or SqlSettings.from_env()
    delay = 5.0
    for attempt in range(1, attempts + 1):
        try:
            return pyodbc.connect(settings.connection_string(), autocommit=autocommit)
        except pyodbc.Error as exc:
            text = str(exc)
            if attempt == attempts or not any(m in text for m in _TRANSIENT_MARKERS):
                raise
            log.warning("connect attempt %d failed (%s); retrying in %.0fs", attempt, text[:120], delay)
            time.sleep(delay)
            delay = min(delay * 1.5, 30)
    raise AssertionError("unreachable")


def split_batches(script: str) -> list[str]:
    """Split a T-SQL script on GO lines, as sqlcmd does."""
    batches = re.split(r"^\s*GO\s*;?\s*$", script, flags=re.IGNORECASE | re.MULTILINE)
    return [b.strip() for b in batches if b.strip()]


def run_script(conn: pyodbc.Connection, path: Path) -> None:
    log.info("running %s", path.name)
    cur = conn.cursor()
    for batch in split_batches(path.read_text(encoding="utf-8")):
        cur.execute(batch)
        while cur.nextset():
            pass
    conn.commit()

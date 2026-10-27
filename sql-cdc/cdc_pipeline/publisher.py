"""Read new changes from CDC and send one Service Bus message per change.

The checkpoint (highest LSN sent) is stored in dbo.cdc_publisher_checkpoint and only
advanced after the messages are accepted by Service Bus. A crash between the two
re-sends the same changes with the same message ids; the consumer skips them.

    python -m cdc_pipeline.publisher            # publish what is there, then exit
    python -m cdc_pipeline.publisher --follow   # keep polling
"""

from __future__ import annotations

import argparse
import logging
import time
from dataclasses import dataclass

import pyodbc
from azure.servicebus import ServiceBusClient, ServiceBusMessage, ServiceBusSender

from . import db
from .config import CAPTURE_INSTANCE, COLUMNS, ServiceBusSettings
from .messages import OPERATIONS, ChangeMessage

log = logging.getLogger("publisher")


class CdcGapError(RuntimeError):
    """Changes after the checkpoint were removed by CDC cleanup before being published."""


@dataclass
class PublishResult:
    sent: int
    from_lsn: bytes | None
    to_lsn: bytes | None


def _scalar(cur: pyodbc.Cursor, sql: str, *params):
    row = cur.execute(sql, *params).fetchone()
    return row[0] if row else None


def read_checkpoint(conn: pyodbc.Connection) -> bytes | None:
    return _scalar(
        conn.cursor(),
        "SELECT last_lsn FROM dbo.cdc_publisher_checkpoint WHERE capture_instance = ?",
        CAPTURE_INSTANCE,
    )


def write_checkpoint(conn: pyodbc.Connection, lsn: bytes) -> None:
    cur = conn.cursor()
    cur.execute(
        "UPDATE dbo.cdc_publisher_checkpoint SET last_lsn = ?, updated_at = SYSUTCDATETIME() "
        "WHERE capture_instance = ?",
        lsn,
        CAPTURE_INSTANCE,
    )
    if cur.rowcount == 0:
        cur.execute(
            "INSERT INTO dbo.cdc_publisher_checkpoint (capture_instance, last_lsn) VALUES (?, ?)",
            CAPTURE_INSTANCE,
            lsn,
        )
    conn.commit()


def lsn_range(conn: pyodbc.Connection) -> tuple[bytes, bytes] | None:
    """The (from, to) LSN window still to publish, or None if there is nothing new."""
    cur = conn.cursor()
    min_lsn = _scalar(cur, "SELECT sys.fn_cdc_get_min_lsn(?)", CAPTURE_INSTANCE)
    max_lsn = _scalar(cur, "SELECT sys.fn_cdc_get_max_lsn()")
    if not min_lsn or min_lsn == bytes(10):
        raise RuntimeError(f"CDC capture instance {CAPTURE_INSTANCE} not found; run setup_db first")
    if not max_lsn or max_lsn == bytes(10):
        return None
    checkpoint = read_checkpoint(conn)
    if checkpoint is None:
        from_lsn = min_lsn
    else:
        from_lsn = _scalar(cur, "SELECT sys.fn_cdc_increment_lsn(?)", checkpoint)
        if from_lsn < min_lsn:
            raise CdcGapError(
                f"checkpoint {checkpoint.hex()} is older than CDC retention ({min_lsn.hex()}); "
                "changes were lost. Re-seed the target and reset the checkpoint."
            )
    conn.commit()
    if from_lsn > max_lsn:
        return None
    return from_lsn, max_lsn


def fetch_changes(conn: pyodbc.Connection, from_lsn: bytes, to_lsn: bytes) -> list[ChangeMessage]:
    cols = ", ".join(COLUMNS)
    sql = (
        "SELECT __$start_lsn, __$seqval, __$operation, "
        "CONVERT(VARCHAR(33), sys.fn_cdc_map_lsn_to_time(__$start_lsn), 126), "
        f"{cols} "
        f"FROM cdc.fn_cdc_get_all_changes_{CAPTURE_INSTANCE}(?, ?, N'all') "
        "ORDER BY __$start_lsn, __$seqval, __$operation"
    )
    rows = conn.cursor().execute(sql, from_lsn, to_lsn).fetchall()
    conn.commit()
    changes = []
    for r in rows:
        changes.append(
            ChangeMessage(
                start_lsn=bytes(r[0]),
                seqval=bytes(r[1]),
                operation=OPERATIONS[r[2]],
                commit_time=r[3],
                row=dict(zip(COLUMNS, r[4:])),
            )
        )
    return changes


def to_servicebus_message(change: ChangeMessage) -> ServiceBusMessage:
    return ServiceBusMessage(
        change.to_json(),
        message_id=change.message_id,
        content_type="application/json",
        subject=f"{change.table}:{change.operation}",
        application_properties={
            "table": change.table,
            "operation": change.operation,
            "start_lsn": change.start_lsn.hex(),
        },
    )


def send_all(sender: ServiceBusSender, changes: list[ChangeMessage]) -> int:
    batch = sender.create_message_batch()
    sent = 0
    for change in changes:
        message = to_servicebus_message(change)
        try:
            batch.add_message(message)
        except ValueError:  # batch full
            sender.send_messages(batch)
            sent += len(batch)
            batch = sender.create_message_batch()
            batch.add_message(message)
    if len(batch):
        sender.send_messages(batch)
        sent += len(batch)
    return sent


def publish_once(conn: pyodbc.Connection, sender: ServiceBusSender) -> PublishResult:
    window = lsn_range(conn)
    if window is None:
        return PublishResult(0, None, None)
    from_lsn, to_lsn = window
    changes = fetch_changes(conn, from_lsn, to_lsn)
    sent = send_all(sender, changes)
    write_checkpoint(conn, to_lsn)
    log.info("sent %d change(s) for LSN %s..%s", sent, from_lsn.hex(), to_lsn.hex())
    return PublishResult(sent, from_lsn, to_lsn)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--follow", action="store_true", help="keep polling for new changes")
    parser.add_argument("--interval", type=float, default=10.0, help="poll interval in seconds with --follow")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

    sb = ServiceBusSettings.from_env()
    conn = db.connect()
    with ServiceBusClient.from_connection_string(sb.require_send()) as client, client.get_queue_sender(sb.queue) as sender:
        total = publish_once(conn, sender).sent
        while args.follow:
            time.sleep(args.interval)
            total += publish_once(conn, sender).sent
    print(f"published {total} change(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

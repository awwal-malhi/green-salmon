"""Receive change messages and apply each to dbo.customers_replica.

Service Bus delivers at least once, so every change is applied in one SQL transaction
together with a row in dbo.cdc_applied_changes keyed by the change's CDC position. A
redelivered message finds its ledger row and is skipped, so each change takes effect
exactly once. dbo.cdc_row_version keeps the CDC position each target row reflects, so a
change that arrives after a newer one for the same row is recorded but not applied.

    python -m cdc_pipeline.consumer             # drain the queue, exit when idle
    python -m cdc_pipeline.consumer --follow    # keep listening
"""

from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass

import pyodbc
from azure.servicebus import ServiceBusClient, ServiceBusReceiver

from . import db
from .config import COLUMNS, KEY_COLUMN, TARGET_TABLE, ServiceBusSettings
from .messages import ChangeMessage

log = logging.getLogger("consumer")

APPLIED, STALE, DUPLICATE = "applied", "stale", "duplicate"


@dataclass
class ConsumeResult:
    applied: int = 0
    stale: int = 0
    duplicates: int = 0
    dead_lettered: int = 0

    @property
    def received(self) -> int:
        return self.applied + self.stale + self.duplicates + self.dead_lettered

    def count(self, outcome: str) -> None:
        if outcome == APPLIED:
            self.applied += 1
        elif outcome == STALE:
            self.stale += 1
        else:
            self.duplicates += 1


def _is_duplicate_key(exc: pyodbc.Error) -> bool:
    return "2627" in str(exc) or "2601" in str(exc)


def apply_change(conn: pyodbc.Connection, change: ChangeMessage) -> str:
    """Apply one change in its own transaction. Returns applied, stale or duplicate."""
    cur = conn.cursor()
    ledger_key = (change.start_lsn, change.seqval, change.operation_code, change.key)
    try:
        seen = cur.execute(
            "SELECT 1 FROM dbo.cdc_applied_changes WITH (UPDLOCK, HOLDLOCK) "
            "WHERE start_lsn = ? AND seqval = ? AND operation = ? AND customer_id = ?",
            *ledger_key,
        ).fetchone()
        if seen:
            conn.rollback()
            return DUPLICATE

        current = cur.execute(
            "SELECT start_lsn, seqval FROM dbo.cdc_row_version WITH (UPDLOCK, HOLDLOCK) WHERE customer_id = ?",
            change.key,
        ).fetchone()
        is_newer = current is None or change.position > (bytes(current[0]), bytes(current[1]))

        if is_newer:
            _apply_to_target(cur, change)
            _set_row_version(cur, change, exists=current is not None)
            outcome = APPLIED
        else:
            outcome = STALE

        cur.execute(
            "INSERT INTO dbo.cdc_applied_changes (start_lsn, seqval, operation, customer_id, outcome) "
            "VALUES (?, ?, ?, ?, ?)",
            *ledger_key,
            outcome,
        )
        conn.commit()
        return outcome
    except pyodbc.IntegrityError as exc:
        conn.rollback()
        if _is_duplicate_key(exc):
            return DUPLICATE
        raise
    except Exception:
        conn.rollback()
        raise


def _apply_to_target(cur: pyodbc.Cursor, change: ChangeMessage) -> None:
    key = change.key
    if change.operation == "delete":
        cur.execute(f"DELETE FROM {TARGET_TABLE} WHERE {KEY_COLUMN} = ?", key)
        return
    values = [change.row[c] for c in COLUMNS]
    non_key = [c for c in COLUMNS if c != KEY_COLUMN]
    cur.execute(
        f"UPDATE {TARGET_TABLE} SET {', '.join(f'{c} = ?' for c in non_key)} WHERE {KEY_COLUMN} = ?",
        *[change.row[c] for c in non_key],
        key,
    )
    if cur.rowcount == 0:
        cur.execute(
            f"INSERT INTO {TARGET_TABLE} ({', '.join(COLUMNS)}) VALUES ({', '.join('?' for _ in COLUMNS)})",
            *values,
        )


def _set_row_version(cur: pyodbc.Cursor, change: ChangeMessage, *, exists: bool) -> None:
    deleted = 1 if change.operation == "delete" else 0
    if exists:
        cur.execute(
            "UPDATE dbo.cdc_row_version SET start_lsn = ?, seqval = ?, is_deleted = ? WHERE customer_id = ?",
            change.start_lsn,
            change.seqval,
            deleted,
            change.key,
        )
    else:
        cur.execute(
            "INSERT INTO dbo.cdc_row_version (customer_id, start_lsn, seqval, is_deleted) VALUES (?, ?, ?, ?)",
            change.key,
            change.start_lsn,
            change.seqval,
            deleted,
        )


def consume(
    conn: pyodbc.Connection,
    receiver: ServiceBusReceiver,
    *,
    idle_seconds: float = 10.0,
    follow: bool = False,
    batch_size: int = 50,
) -> ConsumeResult:
    """Receive and apply messages until the queue has been idle for idle_seconds (or forever)."""
    result = ConsumeResult()
    while True:
        messages = receiver.receive_messages(max_message_count=batch_size, max_wait_time=idle_seconds)
        if not messages:
            if follow:
                continue
            return result
        for message in messages:
            try:
                change = ChangeMessage.from_json(b"".join(message.body))
            except (ValueError, KeyError, TypeError) as exc:
                log.error("dead-lettering malformed message %s: %s", message.message_id, exc)
                receiver.dead_letter_message(message, reason="malformed", error_description=str(exc)[:1000])
                result.dead_lettered += 1
                continue
            try:
                outcome = apply_change(conn, change)
            except Exception:
                receiver.abandon_message(message)
                raise
            receiver.complete_message(message)
            result.count(outcome)
            log.debug("%s %s", outcome, change.message_id)
        log.info(
            "applied=%d stale=%d duplicates=%d dead_lettered=%d",
            result.applied, result.stale, result.duplicates, result.dead_lettered,
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--follow", action="store_true", help="keep listening instead of exiting when idle")
    parser.add_argument("--idle-seconds", type=float, default=10.0, help="exit after this long with no messages")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

    sb = ServiceBusSettings.from_env()
    conn = db.connect()
    with ServiceBusClient.from_connection_string(sb.require_listen()) as client, client.get_queue_receiver(
        sb.queue
    ) as receiver:
        result = consume(conn, receiver, idle_seconds=args.idle_seconds, follow=args.follow)
    print(
        f"received {result.received}: applied {result.applied}, stale {result.stale}, "
        f"duplicates skipped {result.duplicates}, dead-lettered {result.dead_lettered}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

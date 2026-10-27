"""End-to-end: source changes -> CDC -> publisher -> Service Bus -> consumer -> target.

Run with scripts/e2e.sh local|azure. Each test resets the POC tables first.
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta

import pytest
from azure.servicebus import ServiceBusClient, ServiceBusMessage, ServiceBusReceiveMode, ServiceBusSubQueue

from cdc_pipeline import consumer, db, publisher, setup_db
from cdc_pipeline.config import CAPTURE_INSTANCE, COLUMNS, ServiceBusSettings, SqlSettings
from cdc_pipeline.messages import ChangeMessage

pytestmark = pytest.mark.e2e

CAPTURE_TIMEOUT_S = 300
T0 = datetime(2026, 1, 1, 9, 0, 0)


@pytest.fixture(scope="module")
def sb():
    return ServiceBusSettings.from_env()


@pytest.fixture(scope="module")
def sb_send(sb):
    with ServiceBusClient.from_connection_string(sb.require_send()) as client, client.get_queue_sender(sb.queue) as s:
        yield s


@pytest.fixture(scope="module")
def sb_listen(sb):
    with ServiceBusClient.from_connection_string(sb.require_listen()) as client:
        yield client


@pytest.fixture
def conn(sb_listen, sb):
    setup_db.setup(SqlSettings.from_env(), reset=True)
    purge(sb_listen, sb.queue)
    c = db.connect()
    yield c
    c.close()


def purge(client: ServiceBusClient, queue: str) -> int:
    removed = 0
    for sub_queue in (None, ServiceBusSubQueue.DEAD_LETTER):
        with client.get_queue_receiver(
            queue, sub_queue=sub_queue, receive_mode=ServiceBusReceiveMode.RECEIVE_AND_DELETE
        ) as rx:
            while batch := rx.receive_messages(max_message_count=100, max_wait_time=3):
                removed += len(batch)
    return removed


def run_consumer(client: ServiceBusClient, queue: str, conn) -> consumer.ConsumeResult:
    with client.get_queue_receiver(queue) as rx:
        return consumer.consume(conn, rx, idle_seconds=5)


def queue_depths(client: ServiceBusClient, queue: str) -> tuple[int, int]:
    counts = []
    for sub_queue in (None, ServiceBusSubQueue.DEAD_LETTER):
        with client.get_queue_receiver(queue, sub_queue=sub_queue) as rx:
            counts.append(len(rx.peek_messages(max_message_count=100)))
    return counts[0], counts[1]


class Source:
    """Makes changes to dbo.customers and remembers every row change it caused."""

    def __init__(self, conn):
        self.conn = conn
        self.changes: list[tuple[str, int]] = []
        self.clock = T0

    def _ts(self):
        self.clock += timedelta(seconds=1, milliseconds=7)
        return self.clock

    def insert(self, ids, commit=True):
        cur = self.conn.cursor()
        for i in ids:
            cur.execute(
                "INSERT INTO dbo.customers (customer_id, full_name, email, city, loyalty_points, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                i, f"Customer {i:04d}", f"customer{i}@example.test",
                None if i % 5 == 0 else ("Toronto", "Montreal", "Calgary", "Halifax")[i % 4], i * 10, self._ts(),
            )
            self.changes.append(("insert", i))
        if commit:
            self.conn.commit()

    def update(self, ids, commit=True):
        """One multi-row UPDATE statement; every row's values really change."""
        ids = list(ids)
        cur = self.conn.cursor()
        marks = ", ".join("?" for _ in ids)
        cur.execute(
            "UPDATE dbo.customers SET loyalty_points = loyalty_points + 1, "
            f"full_name = full_name + N' *', city = N'Vancouver', updated_at = ? WHERE customer_id IN ({marks})",
            self._ts(), *ids,
        )
        assert cur.rowcount == len(ids)
        self.changes += [("update", i) for i in ids]
        if commit:
            self.conn.commit()

    def delete(self, ids, commit=True):
        ids = list(ids)
        cur = self.conn.cursor()
        cur.execute(f"DELETE FROM dbo.customers WHERE customer_id IN ({', '.join('?' for _ in ids)})", *ids)
        assert cur.rowcount == len(ids)
        self.changes += [("delete", i) for i in ids]
        if commit:
            self.conn.commit()


def cdc_changes(conn) -> list[tuple]:
    cur = conn.cursor()
    min_lsn = cur.execute("SELECT sys.fn_cdc_get_min_lsn(?)", CAPTURE_INSTANCE).fetchone()[0]
    max_lsn = cur.execute("SELECT sys.fn_cdc_get_max_lsn()").fetchone()[0]
    if not max_lsn or max_lsn < min_lsn:
        conn.commit()
        return []
    rows = cur.execute(
        f"SELECT __$start_lsn, __$seqval, __$operation, customer_id "
        f"FROM cdc.fn_cdc_get_all_changes_{CAPTURE_INSTANCE}(?, ?, N'all')",
        min_lsn, max_lsn,
    ).fetchall()
    conn.commit()
    return [(bytes(r[0]), bytes(r[1]), r[2], r[3]) for r in rows]


def wait_for_capture(conn, expected: int) -> list[tuple]:
    """CDC capture is asynchronous (log scan every few seconds); wait until it has caught up."""
    deadline = time.monotonic() + CAPTURE_TIMEOUT_S
    while True:
        rows = cdc_changes(conn)
        if len(rows) >= expected or time.monotonic() > deadline:
            assert len(rows) == expected, f"CDC captured {len(rows)} changes, expected {expected}"
            return rows
        time.sleep(2)


def table_rows(conn, table):
    rows = conn.cursor().execute(f"SELECT {', '.join(COLUMNS)} FROM {table} ORDER BY customer_id").fetchall()
    conn.commit()
    return [tuple(r) for r in rows]


def ledger(conn):
    rows = conn.cursor().execute(
        "SELECT start_lsn, seqval, operation, customer_id, outcome FROM dbo.cdc_applied_changes"
    ).fetchall()
    conn.commit()
    return [(bytes(r[0]), bytes(r[1]), r[2], r[3], r[4]) for r in rows]


def assert_replica_matches(conn, captured):
    source, target = table_rows(conn, "dbo.customers"), table_rows(conn, "dbo.customers_replica")
    assert target == source, "target table differs from source"
    entries = ledger(conn)
    keys = [e[:4] for e in entries]
    assert len(keys) == len(set(keys)), "a change was applied more than once"
    assert sorted(keys) == sorted(captured), "ledger does not match the CDC change set exactly"
    return entries


def test_changes_replicate_exactly_once(conn, sb, sb_send, sb_listen):
    src = Source(conn)

    # Batch 1: inserts, multi-row updates, deletes, a row inserted and deleted in one
    # transaction, and a row updated twice then deleted.
    src.insert(range(1, 21))
    src.update(range(3, 9))
    src.update([4, 5])
    src.delete([2, 10, 15])
    src.insert([900], commit=False)
    src.delete([900], commit=False)
    conn.commit()
    src.update([7])
    src.delete([7])
    src.insert(range(21, 26))
    src.update([21, 25])

    expected = len(src.changes)
    captured = wait_for_capture(conn, expected)
    assert sorted((op, cid) for _, _, op, cid in captured) == sorted(
        ({"insert": 2, "update": 4, "delete": 1}[op], cid) for op, cid in src.changes
    )

    published = publisher.publish_once(conn, sb_send)
    assert published.sent == expected

    result = run_consumer(sb_listen, sb.queue, conn)
    assert result.applied + result.stale == expected
    assert result.duplicates == 0 and result.dead_lettered == 0
    assert_replica_matches(conn, captured)

    # Nothing new: the publisher sends nothing.
    assert publisher.publish_once(conn, sb_send).sent == 0

    # Simulate a publisher crash after sending but before saving its checkpoint:
    # every change is delivered again and the consumer must skip all of them.
    conn.cursor().execute("DELETE FROM dbo.cdc_publisher_checkpoint")
    conn.commit()
    assert publisher.publish_once(conn, sb_send).sent == expected
    replay = run_consumer(sb_listen, sb.queue, conn)
    assert (replay.duplicates, replay.applied, replay.stale) == (expected, 0, 0)
    assert len(assert_replica_matches(conn, captured)) == expected

    # Batch 2: only the new changes are published and applied.
    before = len(src.changes)
    src.insert(range(26, 31))
    src.update([1, 26, 30])
    src.delete([3, 27])
    new = len(src.changes) - before
    captured = wait_for_capture(conn, expected + new)

    assert publisher.publish_once(conn, sb_send).sent == new
    incremental = run_consumer(sb_listen, sb.queue, conn)
    assert incremental.applied + incremental.stale == new and incremental.duplicates == 0
    assert len(assert_replica_matches(conn, captured)) == expected + new

    assert queue_depths(sb_listen, sb.queue) == (0, 0)
    assert len(table_rows(conn, "dbo.customers")) == 30 - 6  # ids 1-30, minus 2, 3, 7, 10, 15, 27


def test_out_of_order_and_redelivered_changes(conn):
    """Changes for one row arriving in the wrong order or twice leave the target correct."""
    row = {
        "customer_id": 5000, "full_name": "Ordering Test", "email": "o@example.test",
        "city": "Ottawa", "loyalty_points": 1, "updated_at": T0,
    }

    def msg(op, lsn, **changes):
        return ChangeMessage(lsn.to_bytes(10, "big"), bytes(10), op, None, {**row, **changes})

    insert, update, delete = msg("insert", 1), msg("update", 2, loyalty_points=2), msg("delete", 3)
    update2 = msg("update", 5, loyalty_points=9)
    insert_again = msg("insert", 4, loyalty_points=4)

    assert consumer.apply_change(conn, update) == consumer.APPLIED
    assert consumer.apply_change(conn, insert) == consumer.STALE
    assert table_rows(conn, "dbo.customers_replica")[0][4] == 2
    assert consumer.apply_change(conn, update) == consumer.DUPLICATE
    assert consumer.apply_change(conn, update2) == consumer.APPLIED
    assert consumer.apply_change(conn, delete) == consumer.STALE
    assert consumer.apply_change(conn, insert_again) == consumer.STALE
    assert table_rows(conn, "dbo.customers_replica")[0][4] == 9

    assert consumer.apply_change(conn, msg("delete", 6)) == consumer.APPLIED
    assert consumer.apply_change(conn, msg("update", 5, loyalty_points=9)) == consumer.DUPLICATE
    assert table_rows(conn, "dbo.customers_replica") == []
    assert len(ledger(conn)) == 6


def test_malformed_message_is_dead_lettered(conn, sb, sb_send, sb_listen):
    sb_send.send_messages(ServiceBusMessage("not a change message", message_id="bad-1"))
    result = run_consumer(sb_listen, sb.queue, conn)
    assert (result.dead_lettered, result.applied) == (1, 0)
    assert queue_depths(sb_listen, sb.queue) == (0, 1)
    assert ledger(conn) == []

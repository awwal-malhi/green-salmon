"""Tests that need no database or queue."""

from datetime import datetime

import pytest

from cdc_pipeline import consumer
from cdc_pipeline.config import ConfigError, ServiceBusSettings, SqlSettings
from cdc_pipeline.db import split_batches
from cdc_pipeline.messages import ChangeMessage

ROW = {
    "customer_id": 42,
    "full_name": "Test Person",
    "email": "t@example.test",
    "city": None,
    "loyalty_points": 7,
    "updated_at": datetime(2026, 1, 2, 3, 4, 5, 678000),
}


def change(op="update", lsn=1, seq=1, **row):
    return ChangeMessage(
        start_lsn=lsn.to_bytes(10, "big"),
        seqval=seq.to_bytes(10, "big"),
        operation=op,
        commit_time="2026-01-02T03:04:05.680",
        row={**ROW, **row},
    )


def test_message_round_trip():
    original = change()
    parsed = ChangeMessage.from_json(original.to_json())
    assert parsed == original
    assert parsed.row["updated_at"] == ROW["updated_at"]


def test_message_id_is_stable_and_distinct():
    assert change().message_id == change().message_id
    ids = {change(lsn=1).message_id, change(lsn=2).message_id, change(seq=2).message_id, change(op="delete").message_id}
    assert len(ids) == 4


def test_position_orders_by_lsn_then_seqval():
    assert change(lsn=1, seq=9).position < change(lsn=2, seq=1).position
    assert change(lsn=2, seq=1).position < change(lsn=2, seq=2).position


@pytest.mark.parametrize(
    "mutate",
    [
        lambda s: s.replace("green-salmon.cdc.v1", "other"),
        lambda s: s.replace('"update"', '"truncate"'),
        lambda s: s.replace('"email":"t@example.test",', ""),
        lambda s: "not json",
    ],
)
def test_malformed_messages_are_rejected(mutate):
    with pytest.raises(ValueError):
        ChangeMessage.from_json(mutate(change().to_json()))


def test_split_batches_on_go():
    script = "SELECT 1;\nGO\n  go  \nSELECT 2\nGO;\nSELECT 'GO here'\n"
    assert split_batches(script) == ["SELECT 1;", "SELECT 2", "SELECT 'GO here'"]


def test_sql_connection_string(monkeypatch):
    for k, v in {
        "SQL_SERVER": "x.database.windows.net",
        "SQL_DATABASE": "db",
        "SQL_USER": "u",
        "SQL_PASSWORD": "p;w{d}",
    }.items():
        monkeypatch.setenv(k, v)
    cs = SqlSettings.from_env().connection_string()
    assert "Server=tcp:x.database.windows.net,1433;" in cs
    assert "PWD={p;w{d}}};" in cs
    assert "Encrypt=yes;" in cs and "TrustServerCertificate=no;" in cs


def test_sql_settings_require_env(monkeypatch):
    for k in ("SQL_SERVER", "SQL_DATABASE", "SQL_USER", "SQL_PASSWORD", "SQL_ADMIN_PASSWORD"):
        monkeypatch.delenv(k, raising=False)
    with pytest.raises(ConfigError):
        SqlSettings.from_env()


def test_servicebus_settings_fallback(monkeypatch):
    monkeypatch.setenv("SERVICEBUS_CONNECTION_STRING", "shared")
    monkeypatch.setenv("SERVICEBUS_SEND_CONNECTION_STRING", "send")
    monkeypatch.delenv("SERVICEBUS_LISTEN_CONNECTION_STRING", raising=False)
    s = ServiceBusSettings.from_env()
    assert (s.require_send(), s.require_listen()) == ("send", "shared")


class FakeMessage:
    def __init__(self, body, message_id="m"):
        self.body = iter([body.encode()])
        self.message_id = message_id


class FakeReceiver:
    def __init__(self, batches):
        self.batches = list(batches)
        self.completed, self.abandoned, self.dead = [], [], []

    def receive_messages(self, max_message_count, max_wait_time):
        return self.batches.pop(0) if self.batches else []

    def complete_message(self, m):
        self.completed.append(m)

    def abandon_message(self, m):
        self.abandoned.append(m)

    def dead_letter_message(self, m, reason, error_description):
        self.dead.append(m)


def test_consume_counts_outcomes_and_settles_every_message(monkeypatch):
    outcomes = iter([consumer.APPLIED, consumer.DUPLICATE, consumer.STALE])
    monkeypatch.setattr(consumer, "apply_change", lambda conn, c: next(outcomes))
    good = [FakeMessage(change(lsn=i).to_json()) for i in range(3)]
    bad = FakeMessage("{}")
    rx = FakeReceiver([good[:2], [bad, good[2]]])
    result = consumer.consume(None, rx, idle_seconds=0)
    assert (result.applied, result.duplicates, result.stale, result.dead_lettered) == (1, 1, 1, 1)
    assert rx.completed == good and rx.dead == [bad] and not rx.abandoned


def test_consume_abandons_and_raises_when_apply_fails(monkeypatch):
    def boom(conn, c):
        raise RuntimeError("db down")

    monkeypatch.setattr(consumer, "apply_change", boom)
    msg = FakeMessage(change().to_json())
    rx = FakeReceiver([[msg]])
    with pytest.raises(RuntimeError):
        consumer.consume(None, rx, idle_seconds=0)
    assert rx.abandoned == [msg] and not rx.completed

"""The change message format sent through the queue (JSON, one message per row change)."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from .config import COLUMNS, KEY_COLUMN, SOURCE_TABLE

SCHEMA = "green-salmon.cdc.v1"

# sys.fn_cdc_get_all_changes_* __$operation values (row filter 'all' never returns 3).
OPERATIONS = {1: "delete", 2: "insert", 4: "update"}
OPERATION_CODES = {v: k for k, v in OPERATIONS.items()}


@dataclass(frozen=True)
class ChangeMessage:
    start_lsn: bytes
    seqval: bytes
    operation: str
    commit_time: str | None
    row: dict[str, Any]
    table: str = SOURCE_TABLE

    @property
    def key(self) -> int:
        return int(self.row[KEY_COLUMN])

    @property
    def operation_code(self) -> int:
        return OPERATION_CODES[self.operation]

    @property
    def position(self) -> tuple[bytes, bytes]:
        """Total order of changes: commit LSN, then sequence within the transaction."""
        return (self.start_lsn, self.seqval)

    @property
    def message_id(self) -> str:
        """Stable id per change; the same change always gets the same id."""
        return f"{self.start_lsn.hex()}-{self.seqval.hex()}-{self.operation_code}-{self.key}"

    def to_json(self) -> str:
        return json.dumps(
            {
                "schema": SCHEMA,
                "table": self.table,
                "operation": self.operation,
                "start_lsn": self.start_lsn.hex(),
                "seqval": self.seqval.hex(),
                "commit_time": self.commit_time,
                "key": {KEY_COLUMN: self.key},
                "row": {c: _encode(self.row.get(c)) for c in COLUMNS},
            },
            separators=(",", ":"),
            sort_keys=True,
        )

    @classmethod
    def from_json(cls, body: str | bytes) -> "ChangeMessage":
        data = json.loads(body)
        if data.get("schema") != SCHEMA:
            raise ValueError(f"unexpected schema {data.get('schema')!r}")
        if data.get("operation") not in OPERATION_CODES:
            raise ValueError(f"unexpected operation {data.get('operation')!r}")
        row = dict(data["row"])
        missing = [c for c in COLUMNS if c not in row]
        if missing:
            raise ValueError(f"row is missing columns {missing}")
        if row["updated_at"] is not None:
            row["updated_at"] = datetime.fromisoformat(row["updated_at"])
        start_lsn, seqval = bytes.fromhex(data["start_lsn"]), bytes.fromhex(data["seqval"])
        if len(start_lsn) != 10 or len(seqval) != 10:
            raise ValueError("start_lsn and seqval must be 10 bytes")
        return cls(
            start_lsn=start_lsn,
            seqval=seqval,
            operation=data["operation"],
            commit_time=data.get("commit_time"),
            row=row,
            table=data["table"],
        )


def _encode(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat(timespec="milliseconds")
    return value

"""Settings from environment variables. Nothing secret lives in the repo."""

from __future__ import annotations

import os
from dataclasses import dataclass, field

CAPTURE_INSTANCE = "dbo_customers"
SOURCE_TABLE = "dbo.customers"
TARGET_TABLE = "dbo.customers_replica"
KEY_COLUMN = "customer_id"
COLUMNS = ("customer_id", "full_name", "email", "city", "loyalty_points", "updated_at")


class ConfigError(RuntimeError):
    pass


def _env(name: str, default: str | None = None, *, required: bool = False) -> str | None:
    value = os.environ.get(name, default)
    if required and not value:
        raise ConfigError(f"environment variable {name} is required")
    return value


@dataclass(frozen=True)
class SqlSettings:
    server: str
    database: str
    user: str
    password: str = field(repr=False)
    driver: str = "ODBC Driver 18 for SQL Server"
    encrypt: str = "yes"
    trust_server_certificate: str = "no"
    authentication: str | None = None

    @classmethod
    def from_env(cls) -> "SqlSettings":
        return cls(
            server=_env("SQL_SERVER", required=True),
            database=_env("SQL_DATABASE", required=True),
            user=_env("SQL_USER", required=True),
            password=_env("SQL_PASSWORD") or _env("SQL_ADMIN_PASSWORD", required=True),
            driver=_env("SQL_ODBC_DRIVER", "ODBC Driver 18 for SQL Server"),
            encrypt=_env("SQL_ENCRYPT", "yes"),
            trust_server_certificate=_env("SQL_TRUST_SERVER_CERTIFICATE", "no"),
            authentication=_env("SQL_AUTHENTICATION") or None,
        )

    def connection_string(self) -> str:
        server = self.server
        if "," not in server and not server.lower().startswith("tcp:"):
            server = f"tcp:{server},1433"
        parts = {
            "Driver": "{" + self.driver + "}",
            "Server": server,
            "Database": self.database,
            "UID": self.user,
            "PWD": self.password,
            "Encrypt": self.encrypt,
            "TrustServerCertificate": self.trust_server_certificate,
            "Connection Timeout": "60",
        }
        if self.authentication:
            parts["Authentication"] = self.authentication
        return ";".join(f"{k}={_odbc_quote(v)}" for k, v in parts.items()) + ";"


def _odbc_quote(value: str) -> str:
    if value.startswith("{") or not any(c in value for c in ";{}= "):
        return value
    return "{" + value.replace("}", "}}") + "}"


@dataclass(frozen=True)
class ServiceBusSettings:
    queue: str
    send_connection_string: str | None = field(repr=False)
    listen_connection_string: str | None = field(repr=False)

    @classmethod
    def from_env(cls) -> "ServiceBusSettings":
        shared = _env("SERVICEBUS_CONNECTION_STRING")
        return cls(
            queue=_env("SERVICEBUS_QUEUE", "cdc-changes"),
            send_connection_string=_env("SERVICEBUS_SEND_CONNECTION_STRING") or shared,
            listen_connection_string=_env("SERVICEBUS_LISTEN_CONNECTION_STRING") or shared,
        )

    def require_send(self) -> str:
        if not self.send_connection_string:
            raise ConfigError("set SERVICEBUS_SEND_CONNECTION_STRING or SERVICEBUS_CONNECTION_STRING")
        return self.send_connection_string

    def require_listen(self) -> str:
        if not self.listen_connection_string:
            raise ConfigError("set SERVICEBUS_LISTEN_CONNECTION_STRING or SERVICEBUS_CONNECTION_STRING")
        return self.listen_connection_string

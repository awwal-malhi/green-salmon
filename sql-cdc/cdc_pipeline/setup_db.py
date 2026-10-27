"""Create the tables and enable CDC (idempotent).

    python -m cdc_pipeline.setup_db                     # Azure: database already exists
    python -m cdc_pipeline.setup_db --create-database   # local SQL Server container
    python -m cdc_pipeline.setup_db --reset             # drop POC tables first (tests)
"""

from __future__ import annotations

import argparse
import dataclasses
import logging

from . import db
from .config import SqlSettings

log = logging.getLogger("setup_db")

SCRIPTS = ("01_tables.sql", "02_enable_cdc.sql")
RESET_SCRIPT = "99_reset.sql"


def create_database(settings: SqlSettings) -> None:
    master = dataclasses.replace(settings, database="master")
    conn = db.connect(master, autocommit=True)
    conn.cursor().execute(f"IF DB_ID(N'{settings.database}') IS NULL CREATE DATABASE [{settings.database}]")
    conn.close()


def setup(settings: SqlSettings | None = None, *, reset: bool = False, create: bool = False) -> None:
    settings = settings or SqlSettings.from_env()
    if create:
        create_database(settings)
    conn = db.connect(settings)
    try:
        names = ((RESET_SCRIPT,) if reset else ()) + SCRIPTS
        for name in names:
            db.run_script(conn, db.SQL_DIR / name)
    finally:
        conn.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--reset", action="store_true", help="drop and recreate the POC tables")
    parser.add_argument("--create-database", action="store_true", help="CREATE DATABASE if missing (local only)")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    setup(reset=args.reset, create=args.create_database)
    print("database ready")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Settings, all from the environment so nothing secret lives in the repo."""

from __future__ import annotations

import os
from pathlib import Path

PGHOST = os.getenv("PGHOST", "localhost")
PGPORT = int(os.getenv("PGPORT", "5433"))
PGDATABASE = os.getenv("PGDATABASE", "dataingest")
PGUSER = os.getenv("PGUSER", "dataingest")
PGPASSWORD = os.getenv("PGPASSWORD", "")

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

#: Files shipped with a source (ticker lists and the like).
DATA_DIR = Path(__file__).parent / "sources" / "data"


def dsn() -> str:
    """libpq connection string built from the PG* environment variables."""
    password = f" password={PGPASSWORD}" if PGPASSWORD else ""
    return f"host={PGHOST} port={PGPORT} dbname={PGDATABASE} user={PGUSER}{password}"


def require(name: str) -> str:
    """Reads a credential a source cannot work without."""
    value = os.getenv(name)
    if not value:
        raise RuntimeError(
            f"{name} is not set. Copy .env.example to .env and fill it in, "
            f"or export {name} before running."
        )
    return value

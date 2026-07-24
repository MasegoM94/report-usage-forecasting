"""In-process write guard for append-only CSV history files.

Single-writer assumption
------------------------
CSV history persistence assumes a single active pipeline writer. Concurrent
processes may create duplicate or lost updates because the read-check-append-
write sequence is not transactional.

This module provides a lightweight threading.Lock per canonical file path that
prevents two threads within the same Python process from writing the same
history file simultaneously.  It does NOT coordinate across separate OS
processes — that requires either a transactional database, Delta/Iceberg
tables, orchestrator-enforced mutual exclusion, or an explicit cross-process
file-locking library.

For production deployments with concurrent writers, replace CSV persistence
with one of:
  - A transactional relational database (PostgreSQL, SQLite with WAL mode)
  - Delta Lake or Apache Iceberg tables (ACID append semantics)
  - Database upserts keyed on the deduplication key
  - Orchestrator-enforced mutual exclusion (Airflow, Prefect, etc.)
  - An explicit file-locking mechanism (fcntl.flock, filelock library)

Usage
-----
    from src.persistence._csv_lock import csv_write_lock

    with csv_write_lock(path):
        # read-check-append-write is now serialised within this process
        ...

The lock is released automatically when the with-block exits, even if an
exception is raised.
"""

from __future__ import annotations

import logging
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

logger = logging.getLogger(__name__)

# Registry: one lock object per resolved absolute path.
# Keys are str(Path.resolve()) so symlinks that point to the same file
# share a lock.
_lock_registry: dict[str, threading.Lock] = {}
_registry_lock = threading.Lock()  # protects mutations to _lock_registry


def _get_lock(path: Path) -> threading.Lock:
    key = str(path.resolve())
    with _registry_lock:
        if key not in _lock_registry:
            _lock_registry[key] = threading.Lock()
        return _lock_registry[key]


@contextmanager
def csv_write_lock(path: Path) -> Iterator[None]:
    """Acquire the in-process write lock for *path*, yield, then release.

    Parameters
    ----------
    path:
        Absolute or relative path to the CSV file being written.  Paths are
        resolved to their canonical form before lookup so that ``./a.csv`` and
        ``/abs/a.csv`` share a lock when they refer to the same file.

    Notes
    -----
    - Lock is per-path within one Python interpreter; separate processes are
      NOT coordinated.
    - The lock is always released, even when the body raises an exception.
    - Do not hold this lock during model fitting or any long-running
      computation — acquire it only for the read-check-append-write block.
    """
    lock = _get_lock(path)
    lock.acquire()
    try:
        yield
    finally:
        lock.release()


def log_write_plan(
    *,
    target: Path,
    candidate_rows: int,
    existing_rows: int,
    new_rows: int,
    skipped_rows: int,
) -> None:
    """Emit a structured INFO log line before a history CSV write.

    Parameters
    ----------
    target:
        Destination file path (logged as a relative-looking string).
    candidate_rows:
        Total rows considered for writing this call.
    existing_rows:
        Rows already present in the file before this write.
    new_rows:
        Rows that will be appended.
    skipped_rows:
        Duplicate rows excluded from the append.
    """
    logger.info(
        "csv_history_write | file=%s | candidate=%d | existing=%d"
        " | new=%d | skipped=%d",
        target,
        candidate_rows,
        existing_rows,
        new_rows,
        skipped_rows,
    )

from __future__ import annotations

import datetime as _dt
import sqlite3
from dataclasses import dataclass
from typing import Iterable, Sequence

from .database import _normalize_entry_type, _normalize_fuel_type


def date_filtering_function(
    *,
    column: str = "created_at",
    on_date: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> tuple[str, list[object]]:
    if on_date and (start_date or end_date):
        raise ValueError("Use either on_date or start_date/end_date (not both).")

    def _validate_iso_date(value: str) -> str:
        _dt.date.fromisoformat(value)
        return value

    params: list[object] = []
    if on_date:
        params.append(_validate_iso_date(on_date))
        return f"date({column}) = date(?)", params

    if start_date and end_date:
        params.extend([_validate_iso_date(start_date), _validate_iso_date(end_date)])
        return f"date({column}) BETWEEN date(?) AND date(?)", params

    if start_date:
        params.append(_validate_iso_date(start_date))
        return f"date({column}) >= date(?)", params

    if end_date:
        params.append(_validate_iso_date(end_date))
        return f"date({column}) <= date(?)", params

    return "", params


@dataclass(frozen=True)
class WarehouseLedgerEntry:
    id: int
    entry_type: str
    fuel_type: str
    liters: float
    created_at: str


def record_warehouse_transaction(
    conn: sqlite3.Connection,
    *,
    entry_type: str,
    fuel_type: str,
    liters: float,
    created_at: str | None = None,
) -> int:
    entry_type = _normalize_entry_type(entry_type)
    fuel_type = _normalize_fuel_type(fuel_type)
    liters = float(liters)
    if liters < 0:
        raise ValueError("liters must be >= 0")

    if created_at is None:
        conn.execute(
            """
            INSERT INTO warehouse_transaction_ledger (entry_type, fuel_type, liters)
            VALUES (?, ?, ?)
            """,
            (entry_type, fuel_type, liters),
        )
    else:
        conn.execute(
            """
            INSERT INTO warehouse_transaction_ledger (entry_type, fuel_type, liters, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (entry_type, fuel_type, liters, created_at),
        )

    conn.commit()
    return int(conn.execute("SELECT last_insert_rowid();").fetchone()[0])


def list_warehouse_ledger(
    conn: sqlite3.Connection,
    *,
    fuel_type: str | None = None,
    on_date: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> Sequence[sqlite3.Row]:
    where_parts: list[str] = []
    params: list[object] = []

    if fuel_type:
        where_parts.append("fuel_type = ?")
        params.append(_normalize_fuel_type(fuel_type))

    date_clause, date_params = date_filtering_function(
        column="created_at",
        on_date=on_date,
        start_date=start_date,
        end_date=end_date,
    )
    if date_clause:
        where_parts.append(date_clause)
        params.extend(date_params)

    where_sql = f"WHERE {' AND '.join(where_parts)}" if where_parts else ""
    return conn.execute(
        f"SELECT * FROM warehouse_transaction_ledger {where_sql} ORDER BY id",
        params,
    ).fetchall()


@dataclass(frozen=True)
class CustomerLedgerEntry:
    id: int
    customer_id: int
    entry_type: str
    fuel_type: str
    liters: float
    created_at: str


def record_customer_transaction(
    conn: sqlite3.Connection,
    *,
    customer_id: int,
    entry_type: str,
    fuel_type: str,
    liters: float,
    created_at: str | None = None,
) -> int:
    entry_type = _normalize_entry_type(entry_type)
    fuel_type = _normalize_fuel_type(fuel_type)
    liters = float(liters)
    if liters < 0:
        raise ValueError("liters must be >= 0")

    if created_at is None:
        conn.execute(
            """
            INSERT INTO customer_transaction_ledger (customer_id, entry_type, fuel_type, liters)
            VALUES (?, ?, ?, ?)
            """,
            (int(customer_id), entry_type, fuel_type, liters),
        )
    else:
        conn.execute(
            """
            INSERT INTO customer_transaction_ledger (customer_id, entry_type, fuel_type, liters, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (int(customer_id), entry_type, fuel_type, liters, created_at),
        )

    conn.commit()
    return int(conn.execute("SELECT last_insert_rowid();").fetchone()[0])


def list_customer_ledger(
    conn: sqlite3.Connection,
    *,
    customer_id: int,
    fuel_type: str | None = None,
    on_date: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> Sequence[sqlite3.Row]:
    where_parts: list[str] = ["customer_id = ?"]
    params: list[object] = [int(customer_id)]

    if fuel_type:
        where_parts.append("fuel_type = ?")
        params.append(_normalize_fuel_type(fuel_type))

    date_clause, date_params = date_filtering_function(
        column="created_at",
        on_date=on_date,
        start_date=start_date,
        end_date=end_date,
    )
    if date_clause:
        where_parts.append(date_clause)
        params.extend(date_params)

    where_sql = f"WHERE {' AND '.join(where_parts)}"
    return conn.execute(
        f"SELECT * FROM customer_transaction_ledger {where_sql} ORDER BY id",
        params,
    ).fetchall()


def compute_running_balance(rows: Iterable[sqlite3.Row]) -> float:
    balance = 0.0
    for row in rows:
        entry_type = (row["entry_type"] or "").upper()
        liters = float(row["liters"])
        if entry_type == "CREDIT":
            balance += liters
        elif entry_type == "DEBIT":
            balance -= liters
        else:
            raise ValueError(f"Unknown entry_type in row: {entry_type!r}")
    return balance

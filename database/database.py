from __future__ import annotations

import sqlite3
from pathlib import Path


DB_NAME = "petrol_auto_invoice.db"


def connect(db_path: str | Path = DB_NAME) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT,
            phone_number TEXT,
            address TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        """
    )
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_users_email ON users(email);")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS warehouse_transaction_ledger (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            entry_type TEXT NOT NULL CHECK (entry_type IN ('DEBIT','CREDIT')),
            fuel_type TEXT NOT NULL,
            liters REAL NOT NULL CHECK (liters >= 0),
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_warehouse_ledger_fuel_type "
        "ON warehouse_transaction_ledger(fuel_type);"
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS customer_transaction_ledger (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_id INTEGER NOT NULL,
            entry_type TEXT NOT NULL CHECK (entry_type IN ('DEBIT','CREDIT')),
            fuel_type TEXT NOT NULL,
            liters REAL NOT NULL CHECK (liters >= 0),
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (customer_id) REFERENCES users(id) ON DELETE CASCADE
        );
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_customer_ledger_customer_id "
        "ON customer_transaction_ledger(customer_id);"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_customer_ledger_fuel_type "
        "ON customer_transaction_ledger(fuel_type);"
    )

    conn.commit()


def init(db_path: str | Path = DB_NAME) -> sqlite3.Connection:
    conn = connect(db_path)
    init_db(conn)
    return conn


def _normalize_entry_type(entry_type: str) -> str:
    normalized = (entry_type or "").strip().upper()
    if normalized not in {"DEBIT", "CREDIT"}:
        raise ValueError("entry_type must be 'DEBIT' or 'CREDIT'")
    return normalized


def _normalize_fuel_type(fuel_type: str) -> str:
    normalized = (fuel_type or "").strip().upper()
    if normalized not in {"PETROL", "DIESEL"}:
        raise ValueError("fuel_type must be 'PETROL' or 'DIESEL'")
    return normalized


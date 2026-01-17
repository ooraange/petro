from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class User:
    id: int
    name: str
    email: Optional[str]
    phone_number: Optional[str]
    address: Optional[str]
    created_at: str


def create_user(
    conn: sqlite3.Connection,
    *,
    name: str,
    email: str | None = None,
    phone_number: str | None = None,
    address: str | None = None,
) -> int:
    if not (name or "").strip():
        raise ValueError("name is required")

    conn.execute(
        """
        INSERT INTO users (name, email, phone_number, address)
        VALUES (?, ?, ?, ?)
        """,
        (name.strip(), (email or None), (phone_number or None), (address or None)),
    )
    conn.commit()
    return int(conn.execute("SELECT last_insert_rowid();").fetchone()[0])


def delete_user_by_id(conn: sqlite3.Connection, user_id: int) -> None:
    conn.execute("DELETE FROM users WHERE id = ?", (user_id,))
    conn.commit()


def edit_user_by_id(
    conn: sqlite3.Connection,
    user_id: int,
    *,
    name: str | None = None,
    email: str | None = None,
    phone_number: str | None = None,
    address: str | None = None,
) -> None:
    updates: list[str] = []
    params: list[object] = []

    if name is not None:
        updates.append("name = ?")
        params.append(name.strip())
    if email is not None:
        updates.append("email = ?")
        params.append(email.strip() if email.strip() else None)
    if phone_number is not None:
        updates.append("phone_number = ?")
        params.append(phone_number.strip() if phone_number.strip() else None)
    if address is not None:
        updates.append("address = ?")
        params.append(address.strip() if address.strip() else None)

    if not updates:
        return

    params.append(user_id)
    conn.execute(
        f"""
        UPDATE users
        SET {', '.join(updates)}
        WHERE id = ?
        """,
        params,
    )
    conn.commit()


def get_user_by_id(conn: sqlite3.Connection, user_id: int) -> Optional[User]:
    row = conn.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
    if not row:
        return None
    return User(**dict(row))


def print_hello() -> None:
    print("Hello, Petro App!")

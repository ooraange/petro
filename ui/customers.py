from __future__ import annotations

import sqlite3

from .tk_support import messagebox, tk, ttk
from ..database import create_user, delete_user_by_id, edit_user_by_id


class CustomersFrame(ttk.Frame):
    def __init__(self, parent: tk.Misc, *, conn: sqlite3.Connection) -> None:
        super().__init__(parent, padding=12)
        self.conn = conn

        actions = ttk.Frame(self)
        actions.grid(row=0, column=0, sticky="ew")
        ttk.Button(actions, text="Add Customer", command=self._open_add_modal).pack(
            side="left"
        )
        ttk.Button(actions, text="Edit Selected", command=self._open_edit_modal).pack(
            side="left", padx=(8, 0)
        )

        table = ttk.LabelFrame(self, text="Customers", padding=12)
        table.grid(row=1, column=0, sticky="nsew", pady=(12, 0))

        self.tree = ttk.Treeview(
            table,
            columns=("id", "name", "email", "phone", "address", "created_at"),
            show="headings",
            height=10,
        )
        for col, title, width in [
            ("id", "ID", 60),
            ("name", "Name", 180),
            ("email", "Email", 200),
            ("phone", "Phone", 120),
            ("address", "Address", 220),
            ("created_at", "Created", 150),
        ]:
            self.tree.heading(col, text=title, anchor="center")
            self.tree.column(col, width=width, anchor="center")

        yscroll = ttk.Scrollbar(table, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=yscroll.set)

        self.tree.grid(row=0, column=0, sticky="nsew")
        yscroll.grid(row=0, column=1, sticky="ns")

        table_actions = ttk.Frame(table)
        table_actions.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(8, 0))
        ttk.Button(table_actions, text="Refresh", command=self.refresh).pack(
            side="left"
        )
        ttk.Button(
            table_actions, text="Delete Selected", command=self._delete_selected
        ).pack(side="left", padx=(8, 0))

        table.columnconfigure(0, weight=1)
        table.rowconfigure(0, weight=1)

        self.columnconfigure(0, weight=1)
        self.rowconfigure(1, weight=1)

        self.refresh()

    def refresh(self) -> None:
        for item in self.tree.get_children():
            self.tree.delete(item)

        rows = self.conn.execute("SELECT * FROM users ORDER BY id").fetchall()
        for row in rows:
            self.tree.insert(
                "",
                "end",
                values=(
                    row["id"],
                    row["name"],
                    row["email"] or "",
                    row["phone_number"] or "",
                    row["address"] or "",
                    row["created_at"],
                ),
            )

    def get_selected_customer_id(self) -> int | None:
        selection = self.tree.selection()
        if not selection:
            return None
        values = self.tree.item(selection[0], "values")
        if not values:
            return None
        return int(values[0])

    def _open_add_modal(self) -> None:
        self._open_customer_modal(title="Add Customer")

    def _open_edit_modal(self) -> None:
        customer_id = self.get_selected_customer_id()
        if customer_id is None:
            messagebox.showinfo("Edit customer", "Select a customer first.", parent=self)
            return
        self._open_customer_modal(title="Edit Customer", customer_id=customer_id)

    def _open_customer_modal(self, *, title: str, customer_id: int | None = None) -> None:
        window = tk.Toplevel(self)
        window.title(title)
        window.transient(self)
        window.grab_set()

        name_var = tk.StringVar()
        email_var = tk.StringVar()
        phone_var = tk.StringVar()
        address_var = tk.StringVar()

        if customer_id is not None:
            row = self.conn.execute(
                "SELECT name, email, phone_number, address FROM users WHERE id = ?",
                (customer_id,),
            ).fetchone()
            if row:
                name_var.set(row["name"] or "")
                email_var.set(row["email"] or "")
                phone_var.set(row["phone_number"] or "")
                address_var.set(row["address"] or "")

        body = ttk.Frame(window, padding=16)
        body.pack(fill="both", expand=True)

        ttk.Label(body, text="Name").grid(row=0, column=0, sticky="w")
        ttk.Entry(body, textvariable=name_var, width=32).grid(
            row=0, column=1, sticky="ew", padx=(8, 0)
        )

        ttk.Label(body, text="Email").grid(row=1, column=0, sticky="w", pady=(8, 0))
        ttk.Entry(body, textvariable=email_var, width=32).grid(
            row=1, column=1, sticky="ew", padx=(8, 0), pady=(8, 0)
        )

        ttk.Label(body, text="Phone").grid(row=2, column=0, sticky="w", pady=(8, 0))
        ttk.Entry(body, textvariable=phone_var, width=32).grid(
            row=2, column=1, sticky="ew", padx=(8, 0), pady=(8, 0)
        )

        ttk.Label(body, text="Address").grid(row=3, column=0, sticky="w", pady=(8, 0))
        ttk.Entry(body, textvariable=address_var, width=32).grid(
            row=3, column=1, sticky="ew", padx=(8, 0), pady=(8, 0)
        )

        buttons = ttk.Frame(body)
        buttons.grid(row=4, column=0, columnspan=2, sticky="ew", pady=(12, 0))

        def submit() -> None:
            try:
                if customer_id is None:
                    create_user(
                        self.conn,
                        name=name_var.get(),
                        email=email_var.get() or None,
                        phone_number=phone_var.get() or None,
                        address=address_var.get() or None,
                    )
                else:
                    edit_user_by_id(
                        self.conn,
                        customer_id,
                        name=name_var.get(),
                        email=email_var.get(),
                        phone_number=phone_var.get(),
                        address=address_var.get(),
                    )
            except Exception as exc:  # student-friendly: show any error as a dialog
                messagebox.showerror(f"{title} failed", str(exc), parent=window)
                return

            window.destroy()
            self.refresh()

        ttk.Button(buttons, text="Save", command=submit).pack(side="left")
        ttk.Button(buttons, text="Cancel", command=window.destroy).pack(
            side="left", padx=(8, 0)
        )

        body.columnconfigure(1, weight=1)

    def _delete_selected(self) -> None:
        customer_id = self.get_selected_customer_id()
        if customer_id is None:
            messagebox.showinfo(
                "Delete customer", "Select a customer first.", parent=self
            )
            return

        has_ledger = self.conn.execute(
            "SELECT 1 FROM customer_transaction_ledger WHERE customer_id = ? LIMIT 1",
            (customer_id,),
        ).fetchone()
        if has_ledger:
            messagebox.showerror(
                "Delete customer",
                "Cannot delete a customer who has ledger entries.",
                parent=self,
            )
            return

        if not messagebox.askyesno(
            "Delete customer",
            f"Delete customer ID {customer_id}?",
            parent=self,
        ):
            return

        delete_user_by_id(self.conn, customer_id)
        self.refresh()

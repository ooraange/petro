from __future__ import annotations

import sqlite3

from .tk_support import messagebox, tk, ttk
from ..database import (
    compute_running_balance,
    list_customer_ledger,
    record_customer_transaction,
)


class CustomerLedgerPage(ttk.Frame):
    def __init__(
        self, parent: tk.Misc, *, conn: sqlite3.Connection, controller: object | None = None
    ) -> None:
        super().__init__(parent, padding=16)
        self.conn = conn
        self.controller = controller

        header = ttk.Frame(self)
        header.grid(row=0, column=0, sticky="ew")
        if self.controller is not None:
            ttk.Button(
                header, text="Back", command=lambda: self.controller.show("home")
            ).pack(side="left")
        ttk.Label(header, text="Customer Ledger", style="Header.TLabel").pack(
            side="left", padx=(12, 0)
        )

        self._customer_display_to_id: dict[str, int] = {}
        self._customer_id_to_display: dict[int, str] = {}

        view = ttk.LabelFrame(self, text="Customer Transactions History", padding=12)
        view.grid(row=1, column=0, sticky="nsew", pady=(12, 0))

        filters = ttk.Frame(view)
        filters.grid(row=0, column=0, sticky="ew")

        self.filter_customer_name_var = tk.StringVar()
        self.on_date_var = tk.StringVar()
        self.start_date_var = tk.StringVar()
        self.end_date_var = tk.StringVar()

        ttk.Label(filters, text="Customer").grid(row=0, column=0, sticky="w")
        self.filter_combo = ttk.Combobox(
            filters,
            textvariable=self.filter_customer_name_var,
            state="readonly",
            width=28,
        )
        self.filter_combo.grid(row=0, column=1, sticky="w", padx=(8, 0))
        self.filter_combo.bind("<<ComboboxSelected>>", self._on_filter_selected)

        ttk.Button(filters, text="Refresh", command=self.refresh_customer_ledger).grid(
            row=0, column=2, sticky="e", padx=(12, 0)
        )

        table_row = ttk.Frame(view)
        table_row.grid(row=1, column=0, sticky="nsew", pady=(10, 0))
        table_row.columnconfigure(0, weight=1)
        table_row.columnconfigure(1, weight=0)
        table_row.columnconfigure(2, weight=0)

        self.ledger_tree = ttk.Treeview(
            table_row,
            columns=("id", "entry_type", "fuel_type", "liters", "created_at"),
            show="headings",
            height=10,
        )
        for col, title, width in [
            ("id", "ID", 60),
            ("entry_type", "Type", 90),
            ("fuel_type", "Fuel", 90),
            ("liters", "Liters", 90),
            ("created_at", "Created", 160),
        ]:
            self.ledger_tree.heading(col, text=title, anchor="center")
            self.ledger_tree.column(col, width=width, anchor="center")

        ledger_scroll = ttk.Scrollbar(
            table_row, orient="vertical", command=self.ledger_tree.yview
        )
        self.ledger_tree.configure(yscrollcommand=ledger_scroll.set)

        self.ledger_tree.grid(row=0, column=0, sticky="nsew")
        ledger_scroll.grid(row=0, column=1, sticky="ns")

        action_panel = ttk.Frame(table_row)
        action_panel.grid(row=0, column=2, sticky="ns", padx=(12, 0))
        action_panel.rowconfigure(0, weight=1)
        action_panel.rowconfigure(1, weight=0)
        action_panel.rowconfigure(2, weight=0)
        action_panel.rowconfigure(3, weight=1)

        ttk.Frame(action_panel, height=10).grid(row=0, column=0, sticky="n")
        ttk.Button(
            action_panel, text="Buy Stock", command=self._start_credit, width=18
        ).grid(row=1, column=0, sticky="ew", pady=(0, 12))
        ttk.Button(
            action_panel, text="Retrieve Stock", command=self._start_debit, width=18
        ).grid(row=2, column=0, sticky="ew")
        ttk.Frame(action_panel, height=10).grid(row=3, column=0, sticky="s")

        self.balance_var = tk.StringVar(value="Total Stock: 0 L")
        ttk.Label(view, textvariable=self.balance_var, style="Header.TLabel").grid(
            row=2, column=0, sticky="w", pady=(12, 0)
        )

        view.columnconfigure(0, weight=1)
        view.rowconfigure(1, weight=1)

        self.columnconfigure(0, weight=1)
        self.rowconfigure(1, weight=1)

        self.refresh_customer_options()

    def on_show(self) -> None:
        self.refresh_customer_options()

    def _on_filter_selected(self, _event: tk.Event | None = None) -> None:
        self.refresh_customer_ledger()

    def refresh_customer_options(self) -> None:
        rows = self.conn.execute("SELECT id, name FROM users ORDER BY name").fetchall()
        self._customer_display_to_id.clear()
        self._customer_id_to_display.clear()

        values: list[str] = []
        for row in rows:
            display = f"{row['name']} (ID {row['id']})"
            self._customer_display_to_id[display] = int(row["id"])
            self._customer_id_to_display[int(row["id"])] = display
            values.append(display)

        self.filter_combo["values"] = values

    def _selected_customer_id(self, selection: str) -> int:
        customer_id = self._customer_display_to_id.get(selection)
        if customer_id is None:
            raise ValueError("Select a customer.")
        return customer_id

    def _parse_liters(self, raw: str) -> float:
        liters = float(raw)
        if liters < 0:
            raise ValueError("liters must be >= 0")
        return liters
 
    def _start_credit(self) -> None:
        self._open_transaction_modal(entry_type="CREDIT", title="Buy Stock")

    def _start_debit(self) -> None:
        self._open_transaction_modal(entry_type="DEBIT", title="Retrieve Stock")

    def _open_transaction_modal(self, *, entry_type: str, title: str) -> None:
        selection = self.filter_customer_name_var.get()
        if not selection:
            messagebox.showinfo("Customer", "Choose a customer first.", parent=self)
            return

        window = tk.Toplevel(self)
        window.title(title)
        window.transient(self)
        window.grab_set()

        liters_var = tk.StringVar(value="0")
        fuel_var = tk.StringVar(value="PETROL")

        body = ttk.Frame(window, padding=16)
        body.pack(fill="both", expand=True)

        ttk.Label(body, text="Customer").grid(row=0, column=0, sticky="w")
        ttk.Label(body, text=selection).grid(
            row=0, column=1, sticky="w", padx=(8, 0)
        )

        ttk.Label(body, text="Fuel Type").grid(row=1, column=0, sticky="w", pady=(8, 0))
        ttk.Combobox(
            body,
            textvariable=fuel_var,
            values=["PETROL", "DIESEL"],
            state="readonly",
            width=14,
        ).grid(row=1, column=1, sticky="w", padx=(8, 0), pady=(8, 0))

        ttk.Label(body, text="Liters").grid(row=2, column=0, sticky="w", pady=(8, 0))
        ttk.Entry(body, textvariable=liters_var, width=14).grid(
            row=2, column=1, sticky="w", padx=(8, 0), pady=(8, 0)
        )

        buttons = ttk.Frame(body)
        buttons.grid(row=3, column=0, columnspan=2, sticky="ew", pady=(12, 0))

        def submit() -> None:
            try:
                customer_id = self._selected_customer_id(selection)
                liters = self._parse_liters(liters_var.get())
                record_customer_transaction(
                    self.conn,
                    customer_id=customer_id,
                    entry_type=entry_type,
                    fuel_type=fuel_var.get(),
                    liters=liters,
                )
            except Exception as exc:
                messagebox.showerror(f"{title} failed", str(exc), parent=window)
                return

            window.destroy()
            self.refresh_customer_ledger()

        ttk.Button(buttons, text=title, command=submit).pack(side="left")
        ttk.Button(buttons, text="Cancel", command=window.destroy).pack(
            side="left", padx=(8, 0)
        )

        body.columnconfigure(1, weight=1)

    def refresh_customer_ledger(self) -> None:
        for item in self.ledger_tree.get_children():
            self.ledger_tree.delete(item)

        raw_customer = (self.filter_customer_name_var.get() or "").strip()
        if not raw_customer:
            self.balance_var.set("Total Stock: 0 L")
            return

        try:
            customer_id = self._selected_customer_id(raw_customer)
            rows = list_customer_ledger(
                self.conn,
                customer_id=customer_id,
                on_date=self.on_date_var.get().strip() or None,
                start_date=self.start_date_var.get().strip() or None,
                end_date=self.end_date_var.get().strip() or None,
            )
        except Exception as exc:
            messagebox.showerror("Load ledger failed", str(exc), parent=self)
            self.balance_var.set("Total Stock: 0 L")
            return

        for row in rows:
            self.ledger_tree.insert(
                "",
                "end",
                values=(
                    row["id"],
                    row["entry_type"],
                    row["fuel_type"],
                    row["liters"],
                    row["created_at"],
                ),
            )

        balance = compute_running_balance(rows)
        self.balance_var.set(f"Total Stock: {balance:g} L")

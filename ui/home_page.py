from __future__ import annotations

from .controller import ScreenController
from .tk_support import tk, ttk


class HomePage(ttk.Frame):
    def __init__(self, parent: tk.Misc, *, controller: ScreenController) -> None:
        super().__init__(parent, padding=24)
        self.controller = controller

        header = ttk.Frame(self)
        header.pack(fill="x")
        ttk.Label(header, text="Home", style="Header.TLabel").pack(side="left")
        ttk.Button(header, text="Log Out", command=self._logout).pack(side="right")

        self.total_stock_var = tk.StringVar(value="Total stock: 0")
        ttk.Label(self, textvariable=self.total_stock_var, style="Subtle.TLabel").pack(
            anchor="w", pady=(6, 0)
        )

        body = ttk.Frame(self)
        body.pack(fill="both", expand=True, pady=(16, 0))
        body.columnconfigure(0, weight=2)
        body.columnconfigure(1, weight=1)
        body.rowconfigure(0, weight=1)

        left = ttk.LabelFrame(body, text="Top 5 customers", padding=12)
        left.grid(row=0, column=0, sticky="nsew", padx=(0, 16))

        self.top_customers = tk.Listbox(left, height=8)
        self.top_customers.pack(fill="both", expand=True)
        self._refresh_top_customers()

        right = ttk.Frame(body)
        right.grid(row=0, column=1, sticky="n")

        ttk.Button(
            right,
            text="Customer Ledger",
            command=lambda: self.controller.show("ledger"),
            width=20,
        ).pack(pady=(0, 12), fill="x")
        ttk.Button(
            right,
            text="Customer Management",
            command=lambda: self.controller.show("customers"),
            width=20,
        ).pack(fill="x")

    def _logout(self) -> None:
        self.controller.show("login")

    def on_show(self) -> None:
        self._refresh_top_customers()

    def _refresh_top_customers(self) -> None:
        conn = self.controller.conn

        total_row = conn.execute(
            """
            SELECT COALESCE(SUM(
                CASE
                    WHEN entry_type = 'CREDIT' THEN liters
                    WHEN entry_type = 'DEBIT' THEN -liters
                    ELSE 0
                END
            ), 0) AS total_stock
            FROM customer_transaction_ledger
            """
        ).fetchone()
        total_stock = float(total_row["total_stock"] or 0)
        self.total_stock_var.set(f"Total stock: {total_stock:g} L")

        rows = conn.execute(
            """
            SELECT users.name AS name,
                   SUM(
                        CASE
                            WHEN customer_transaction_ledger.entry_type = 'CREDIT'
                                THEN customer_transaction_ledger.liters
                            WHEN customer_transaction_ledger.entry_type = 'DEBIT'
                                THEN -customer_transaction_ledger.liters
                            ELSE 0
                        END
                   ) AS balance
            FROM customer_transaction_ledger
            JOIN users ON users.id = customer_transaction_ledger.customer_id
            GROUP BY users.id
            ORDER BY balance DESC
            LIMIT 5
            """
        ).fetchall()

        self.top_customers.delete(0, "end")
        if not rows:
            self.top_customers.insert("end", "No ledger entries yet.")
            return

        for row in rows:
            self.top_customers.insert("end", f"{row['name']}: {row['balance']:.2f} L")

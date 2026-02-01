from __future__ import annotations

from .controller import ScreenController
from .tk_support import tk, ttk


class InvoicePage(ttk.Frame):
    def __init__(self, parent: tk.Misc, *, controller: ScreenController) -> None:
        super().__init__(parent, padding=24)
        self.controller = controller

        header = ttk.Frame(self)
        header.pack(fill="x")
        ttk.Button(header, text="Back", command=lambda: controller.show("ledger")).pack(
            side="left"
        )
        ttk.Label(header, text="Invoice", style="Header.TLabel").pack(
            side="left", padx=(12, 0)
        )

        body = ttk.Frame(self)
        body.pack(fill="both", expand=True, pady=(16, 0))

        pdf = ttk.LabelFrame(body, text="Invoice PDF", padding=12)
        pdf.pack(side="left", fill="both", expand=True, padx=(0, 16))
        ttk.Label(pdf, text="(PDF preview placeholder)").pack(expand=True)

        side = ttk.Frame(body)
        side.pack(side="right", fill="y")

        ttk.Label(side, text="Stock: 100").pack(anchor="w")
        ttk.Label(side, text="Retrieve: 20").pack(anchor="w", pady=(0, 12))
        ttk.Button(side, text="Confirm Transaction").pack(fill="x", pady=(0, 8))
        ttk.Button(side, text="Download Invoice").pack(fill="x")

from __future__ import annotations

from .controller import ScreenController
from .customers import CustomersFrame
from .ledger import LedgerFrame
from .tk_support import tk, ttk


class CustomerLedgerPage(ttk.Frame):
    def __init__(
        self,
        parent: tk.Misc,
        *,
        controller: ScreenController,
        customers_frame: CustomersFrame,
    ) -> None:
        super().__init__(parent, padding=16)
        self.controller = controller

        top = ttk.Frame(self)
        top.pack(fill="x", pady=(0, 8))
        ttk.Button(top, text="Back", command=lambda: controller.show("home")).pack(
            side="left"
        )
        ttk.Label(top, text="Customer Ledger", style="Header.TLabel").pack(
            side="left", padx=(12, 0)
        )
 

        self.ledger_frame = LedgerFrame(
            self, conn=controller.conn, customers=customers_frame
        )
        self.ledger_frame.pack(fill="both", expand=True)

    def on_show(self) -> None:
        self.ledger_frame.refresh_customer_options()

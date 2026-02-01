from __future__ import annotations

from .controller import ScreenController
from .customers import CustomersFrame
from .tk_support import tk, ttk


class CustomerManagementPage(ttk.Frame):
    def __init__(self, parent: tk.Misc, *, controller: ScreenController) -> None:
        super().__init__(parent, padding=16)
        self.controller = controller

        header = ttk.Frame(self)
        header.pack(fill="x")
        ttk.Button(header, text="Back", command=lambda: controller.show("home")).pack(
            side="left"
        )
        ttk.Label(header, text="Customer Management", style="Header.TLabel").pack(
            side="left", padx=(12, 0)
        )

        self.customers_frame = CustomersFrame(self, conn=controller.conn)
        self.customers_frame.pack(fill="both", expand=True)

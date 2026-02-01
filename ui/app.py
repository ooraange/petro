from __future__ import annotations

from .controller import ScreenController
from .customer_ledger_page import CustomerLedgerPage
from .customer_management_page import CustomerManagementPage
from .home_page import HomePage
from .invoice_page import InvoicePage
from .login_page import LoginPage
from .tk_support import tk, ttk
from ..database import DB_NAME, init


class PetroApp(tk.Tk):
    def __init__(self, *, db_path: str = DB_NAME) -> None:
        super().__init__()
        self.title("Petro UI (Student Example)")
        self.minsize(980, 620)

        self.conn = init(db_path)

        style = ttk.Style(self)
        try:
            style.theme_use("clam")
        except tk.TclError:
            pass
        style.configure("Header.TLabel", font=("Segoe UI", 16, "bold"))
        style.configure("Subtle.TLabel", foreground="#555")

        self.status_var = tk.StringVar(value=f"Database: {db_path}")
        ttk.Label(self, textvariable=self.status_var, padding=(12, 6)).pack(
            side="bottom", fill="x"
        )

        container = ttk.Frame(self)
        container.pack(fill="both", expand=True)
        container.rowconfigure(0, weight=1)
        container.columnconfigure(0, weight=1)

        controller = ScreenController(root=self, conn=self.conn)

        self.login_page = LoginPage(container, controller=controller)
        self.home_page = HomePage(container, controller=controller)
        self.customers_page = CustomerManagementPage(container, controller=controller)
        self.ledger_page = CustomerLedgerPage(
            container,
            controller=controller,
            customers_frame=self.customers_page.customers_frame,
        )
        self.invoice_page = InvoicePage(container, controller=controller)

        for name, frame in [
            ("login", self.login_page),
            ("home", self.home_page),
            ("customers", self.customers_page),
            ("ledger", self.ledger_page),
            ("invoice", self.invoice_page),
        ]:
            frame.grid(row=0, column=0, sticky="nsew")
            controller.register(name, frame)

        controller.show("login")

        self.protocol("WM_DELETE_WINDOW", self._on_close)

    def _on_close(self) -> None:
        try:
            self.conn.close()
        finally:
            self.destroy()


def main() -> None:
    app = PetroApp()
    app.mainloop()

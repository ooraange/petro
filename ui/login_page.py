from __future__ import annotations

from .controller import ScreenController
from .tk_support import messagebox, tk, ttk

_LOGIN_PASSWORD = "admin"


class LoginPage(ttk.Frame):
    def __init__(self, parent: tk.Misc, *, controller: ScreenController) -> None:
        super().__init__(parent, padding=24)
        self.controller = controller

        card = ttk.Frame(self, padding=28)
        card.place(relx=0.5, rely=0.5, anchor="center")

        ttk.Label(card, text="Petro Login", style="Header.TLabel").grid(
            row=0, column=0, columnspan=2, pady=(0, 12)
        )

        avatar = tk.Canvas(card, width=72, height=72, highlightthickness=0)
        avatar.create_oval(10, 10, 70, 70, outline="#333", width=2)
        avatar.grid(row=1, column=0, columnspan=2, pady=(0, 16))

        ttk.Label(card, text="Accounting Password").grid(
            row=2, column=0, columnspan=2, pady=(0, 4)
        )
        self.password_var = tk.StringVar()
        ttk.Entry(card, textvariable=self.password_var, show="*").grid(
            row=3, column=0, columnspan=2, sticky="ew", pady=(0, 16)
        )

        ttk.Button(card, text="Log In", command=self._login).grid(
            row=4, column=0, columnspan=2, sticky="ew"
        )

        card.columnconfigure(0, weight=1)
        card.columnconfigure(1, weight=1)

    def _login(self) -> None:
        if self.password_var.get() != _LOGIN_PASSWORD:
            messagebox.showerror(
                "Login failed", "Incorrect password.", parent=self
            )
            self.password_var.set("")
            return

        self.controller.show("home")

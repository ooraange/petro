from __future__ import annotations

try:
    import tkinter as tk
    from tkinter import messagebox, ttk
except ModuleNotFoundError as exc:
    if exc.name == "tkinter":
        raise ModuleNotFoundError(
            "Tkinter is not available in this Python install. "
            "On Ubuntu/Debian: sudo apt-get install python3-tk",
            name="tkinter",
        ) from exc
    raise

__all__ = ["tk", "ttk", "messagebox"]

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

def center_window(window: tk.Misc) -> None:
    window.update_idletasks()
    width = window.winfo_width()
    height = window.winfo_height()
    if width <= 1 or height <= 1:
        width = window.winfo_reqwidth()
        height = window.winfo_reqheight()
    screen_width = window.winfo_screenwidth()
    screen_height = window.winfo_screenheight()
    x = max((screen_width - width) // 2, 0)
    y = max((screen_height - height) // 2, 0)
    window.geometry(f"{width}x{height}+{x}+{y}")


__all__ = ["tk", "ttk", "messagebox", "center_window"]

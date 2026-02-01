from __future__ import annotations

import sqlite3

from .tk_support import tk, ttk


class ScreenController:
    def __init__(self, *, root: tk.Tk, conn: sqlite3.Connection) -> None:
        self.root = root
        self.conn = conn
        self._frames: dict[str, ttk.Frame] = {}

    def register(self, name: str, frame: ttk.Frame) -> None:
        self._frames[name] = frame

    def show(self, name: str) -> None:
        frame = self._frames[name]
        frame.tkraise()
        on_show = getattr(frame, "on_show", None)
        if callable(on_show):
            on_show()

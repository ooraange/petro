from __future__ import annotations

import datetime as _dt
from pathlib import Path

from .controller import ScreenController
from .tk_support import messagebox, tk, ttk


class InvoicePage(ttk.Frame):
    def __init__(self, parent: tk.Misc, *, controller: ScreenController) -> None:
        super().__init__(parent, padding=24)
        self.controller = controller
        self._invoice_payload: dict[str, object] | None = None

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

        preview = ttk.LabelFrame(body, text="Invoice Preview", padding=12)
        preview.pack(side="left", fill="both", expand=True, padx=(0, 16))
        logo_path = Path(__file__).resolve().parents[1] / "logo.png"
        self._logo_image: tk.PhotoImage | None = None
        if logo_path.exists():
            try:
                self._logo_image = tk.PhotoImage(file=str(logo_path))
            except tk.TclError:
                self._logo_image = None
        if self._logo_image is not None:
            ttk.Label(preview, image=self._logo_image).pack(anchor="w", pady=(0, 8))
        self.preview = tk.Text(preview, height=12, width=42, state="disabled", wrap="word")
        self.preview.pack(fill="both", expand=True)

        side = ttk.Frame(body)
        side.pack(side="right", fill="y")

        self.summary_var = tk.StringVar(value="No retrieval selected.")
        ttk.Label(side, textvariable=self.summary_var).pack(anchor="w", pady=(0, 12))
        ttk.Button(side, text="Download JPG", command=self._download_jpg).pack(
            fill="x", pady=(0, 8)
        )

    def on_show(self) -> None:
        self._invoice_payload = self.controller.last_invoice
        if not self._invoice_payload:
            self._set_preview("(No retrieval data. Go back and retrieve stock first.)")
            self.summary_var.set("No retrieval selected.")
            return

        customer = self._invoice_payload.get("customer_display", "")
        fuel_type = self._invoice_payload.get("fuel_type", "")
        liters = self._invoice_payload.get("liters", 0)
        timestamp = _dt.datetime.now().strftime("%Y-%m-%d %H:%M")

        text = (
            "Petro Invoice\n"
            f"Date: {timestamp}\n"
            f"Customer: {customer}\n"
            f"Fuel Type: {fuel_type}\n"
            f"Liters Retrieved: {liters}\n"
        )
        self._set_preview(text)
        self.summary_var.set(f"{customer}\n{fuel_type} - {liters} L")

    def _set_preview(self, text: str) -> None:
        self.preview.configure(state="normal")
        self.preview.delete("1.0", "end")
        self.preview.insert("1.0", text)
        self.preview.configure(state="disabled")

    def _download_jpg(self) -> None:
        if not self._invoice_payload:
            messagebox.showinfo("Invoice", "No retrieval data to export.", parent=self)
            return
        try:
            from PIL import Image, ImageDraw  # type: ignore
        except Exception:
            messagebox.showerror(
                "Invoice",
                "Pillow is not installed. Install it to export JPG.",
                parent=self,
            )
            return

        customer = str(self._invoice_payload.get("customer_display", ""))
        fuel_type = str(self._invoice_payload.get("fuel_type", ""))
        liters = self._invoice_payload.get("liters", 0)
        timestamp = _dt.datetime.now().strftime("%Y-%m-%d %H:%M")

        image = Image.new("RGB", (600, 420), color="white")
        draw = ImageDraw.Draw(image)
        text = [
            "Petro Invoice",
            f"Date: {timestamp}",
            f"Customer: {customer}",
            f"Fuel Type: {fuel_type}",
            f"Liters Retrieved: {liters}",
        ]
        y = 40
        for line in text:
            draw.text((40, y), line, fill="black")
            y += 32

        filename = f"invoice_{_dt.datetime.now().strftime('%Y%m%d_%H%M%S')}.jpg"
        path = self._download_path() / filename
        image.save(path, "JPEG")
        messagebox.showinfo("Invoice", f"Saved {path}", parent=self)

    def _download_path(self) -> Path:
        downloads = Path.home() / "Downloads"
        downloads.mkdir(parents=True, exist_ok=True)
        return downloads

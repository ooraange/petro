"""App entrypoint (runs the Tkinter UI)."""

from __future__ import annotations

import sys
from pathlib import Path


if __name__ == "__main__":
    try:
        if not __package__:
            repo_root = Path(__file__).resolve().parents[1]
            sys.path.insert(0, str(repo_root))
            from petro.ui import main
        else:
            from .ui import main
    except ModuleNotFoundError as exc:
        if exc.name == "tkinter":
            print(
                "Tkinter is not available in this Python install.\n"
                "On Ubuntu/Debian: sudo apt-get install python3-tk\n"
                "On Windows/macOS: install the standard Python distribution that includes Tk.",
                file=sys.stderr,
            )
            raise SystemExit(1) from exc
        raise

    main()


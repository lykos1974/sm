"""Bootstrap Manager MVP step 1: static Tkinter UI shell only."""

from __future__ import annotations

import calendar
from datetime import datetime, timezone
from pathlib import Path
import sqlite3
import tkinter as tk
from tkinter import ttk

DEFAULT_DB_PATH = "data/pnf_mvp_research_clean.sqlite3"
PLACEHOLDER_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
MODES = [
    "Inspect Only",
    "Dry-Run Preview",
    "Import From Local Cache",
    "Download + Import",
]


class BootstrapManagerApp:
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title("Bootstrap Manager")
        self.root.geometry("980x720")
        self.root.minsize(900, 640)

        self.db_path_var = tk.StringVar(value=DEFAULT_DB_PATH)
        self.start_month_var = tk.StringVar(value="2026-01")
        self.end_month_var = tk.StringVar(value="2026-03")
        self.local_cache_var = tk.StringVar(value="")
        self.mode_var = tk.StringVar(value=MODES[0])

        self._build_ui()

    def _build_ui(self) -> None:
        container = ttk.Frame(self.root, padding=12)
        container.pack(fill="both", expand=True)
        container.columnconfigure(0, weight=1)
        container.rowconfigure(3, weight=1)
        container.rowconfigure(4, weight=1)

        self._build_target_db_section(container)
        self._build_inputs_section(container)
        self._build_actions_section(container)
        self._build_log_section(container)
        self._build_summary_section(container)

    def _build_target_db_section(self, parent: ttk.Frame) -> None:
        frame = ttk.LabelFrame(parent, text="Target DB", padding=10)
        frame.grid(row=0, column=0, sticky="ew", pady=(0, 10))
        frame.columnconfigure(1, weight=1)

        ttk.Label(frame, text="DB Path:").grid(row=0, column=0, sticky="w", padx=(0, 8))
        entry = ttk.Entry(frame, textvariable=self.db_path_var)
        entry.grid(row=0, column=1, sticky="ew")

    def _build_inputs_section(self, parent: ttk.Frame) -> None:
        frame = ttk.LabelFrame(parent, text="Bootstrap Inputs", padding=10)
        frame.grid(row=1, column=0, sticky="nsew", pady=(0, 10))
        frame.columnconfigure(0, weight=2)
        frame.columnconfigure(1, weight=3)
        frame.rowconfigure(1, weight=1)

        symbols_frame = ttk.Frame(frame)
        symbols_frame.grid(row=0, column=0, rowspan=4, sticky="nsew", padx=(0, 12))
        symbols_frame.columnconfigure(0, weight=1)
        symbols_frame.rowconfigure(1, weight=1)

        ttk.Label(symbols_frame, text="Symbols (multi-select):").grid(row=0, column=0, sticky="w", pady=(0, 6))
        self.symbol_listbox = tk.Listbox(symbols_frame, selectmode=tk.MULTIPLE, height=8, exportselection=False)
        self.symbol_listbox.grid(row=1, column=0, sticky="nsew")
        for symbol in PLACEHOLDER_SYMBOLS:
            self.symbol_listbox.insert(tk.END, symbol)

        ttk.Label(frame, text="Start Month (YYYY-MM):").grid(row=0, column=1, sticky="w")
        ttk.Entry(frame, textvariable=self.start_month_var).grid(row=1, column=1, sticky="ew", pady=(2, 8))

        ttk.Label(frame, text="End Month (YYYY-MM):").grid(row=2, column=1, sticky="w")
        ttk.Entry(frame, textvariable=self.end_month_var).grid(row=3, column=1, sticky="ew", pady=(2, 8))

        ttk.Label(frame, text="Local Cache Root:").grid(row=4, column=0, sticky="w", pady=(8, 0))
        ttk.Entry(frame, textvariable=self.local_cache_var).grid(row=5, column=0, columnspan=2, sticky="ew", pady=(2, 8))

        ttk.Label(frame, text="Mode:").grid(row=6, column=0, sticky="w")
        mode_box = ttk.Combobox(frame, textvariable=self.mode_var, values=MODES, state="readonly")
        mode_box.grid(row=7, column=0, columnspan=2, sticky="ew", pady=(2, 0))

    def _build_actions_section(self, parent: ttk.Frame) -> None:
        frame = ttk.Frame(parent)
        frame.grid(row=2, column=0, sticky="ew", pady=(0, 10))

        ttk.Button(frame, text="Inspect / Preview", command=self.inspect_preview).pack(side="left")
        ttk.Button(frame, text="Execute Bootstrap").pack(side="left", padx=(8, 0))

    def _build_log_section(self, parent: ttk.Frame) -> None:
        frame = ttk.LabelFrame(parent, text="Log / Output", padding=10)
        frame.grid(row=3, column=0, sticky="nsew", pady=(0, 10))
        frame.columnconfigure(0, weight=1)
        frame.rowconfigure(0, weight=1)

        self.log_text = tk.Text(frame, height=10, wrap="word")
        self.log_text.grid(row=0, column=0, sticky="nsew")
        self._append_log("Bootstrap Manager UI shell loaded.")
        self._append_log("Coverage inspection (read-only) is available via Inspect / Preview.")

    def _build_summary_section(self, parent: ttk.Frame) -> None:
        frame = ttk.LabelFrame(parent, text="Summary (placeholder)", padding=10)
        frame.grid(row=4, column=0, sticky="nsew")
        ttk.Label(frame, text="Summary output will appear here in a later step.").pack(anchor="w")

    def _append_log(self, message: str) -> None:
        self.log_text.configure(state="normal")
        self.log_text.insert("end", message + "\n")
        self.log_text.see("end")
        self.log_text.configure(state="disabled")

    def _selected_symbols(self) -> list[str]:
        out: list[str] = []
        for idx in self.symbol_listbox.curselection():
            out.append(str(self.symbol_listbox.get(idx)).strip().upper())
        return [s for s in out if s]

    def _parse_month(self, value: str) -> tuple[int, int]:
        text = value.strip()
        try:
            dt = datetime.strptime(text, "%Y-%m")
            return dt.year, dt.month
        except ValueError as exc:
            raise ValueError(f"Invalid month '{value}'. Expected YYYY-MM.") from exc

    def _iter_month_tokens(self, start_text: str, end_text: str) -> list[tuple[int, int, str]]:
        start_y, start_m = self._parse_month(start_text)
        end_y, end_m = self._parse_month(end_text)
        if (start_y, start_m) > (end_y, end_m):
            raise ValueError("Start month must be <= end month.")

        out: list[tuple[int, int, str]] = []
        year, month = start_y, start_m
        while (year, month) <= (end_y, end_m):
            out.append((year, month, f"{year:04d}-{month:02d}"))
            if month == 12:
                year += 1
                month = 1
            else:
                month += 1
        return out

    @staticmethod
    def _month_bounds_ms(year: int, month: int) -> tuple[int, int]:
        start = datetime(year, month, 1, tzinfo=timezone.utc)
        _, month_days = calendar.monthrange(year, month)
        end = datetime(year, month, month_days, 23, 59, 59, 999000, tzinfo=timezone.utc)
        return int(start.timestamp() * 1000), int(end.timestamp() * 1000)

    @staticmethod
    def _ms_to_utc_text(value: int | None) -> str:
        if value is None:
            return "-"
        dt = datetime.fromtimestamp(value / 1000, tz=timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def _expected_rows_for_month(year: int, month: int) -> int:
        _, month_days = calendar.monthrange(year, month)
        return month_days * 24 * 60

    @staticmethod
    def _classify(count: int, expected_rows: int) -> str:
        if count == 0:
            return "MISSING"
        if count >= int(expected_rows * 0.95):
            return "PRESENT"
        return "PARTIAL"

    def inspect_preview(self) -> None:
        db_path = Path(self.db_path_var.get().strip())
        symbols = self._selected_symbols()

        if not symbols:
            self._append_log("WARNING: No symbols selected.")
            return

        if not db_path.exists():
            self._append_log(f"ERROR: DB file not found: {db_path}")
            return

        try:
            months = self._iter_month_tokens(self.start_month_var.get(), self.end_month_var.get())
        except ValueError as exc:
            self._append_log(f"ERROR: {exc}")
            return

        self._append_log(
            f"Inspecting coverage | db={db_path} | symbols={','.join(symbols)} | months={months[0][2]}..{months[-1][2]}"
        )

        query = (
            "SELECT COUNT(*), MIN(open_time), MAX(open_time) "
            "FROM candles WHERE symbol=? AND interval='1m' AND open_time>=? AND open_time<=?"
        )

        try:
            with sqlite3.connect(str(db_path)) as conn:
                for symbol in symbols:
                    ns_symbol = f"BINANCE_FUT:{symbol}"
                    for year, month, month_token in months:
                        start_ms, end_ms = self._month_bounds_ms(year, month)
                        row = conn.execute(query, (ns_symbol, start_ms, end_ms)).fetchone()
                        count = int(row[0] or 0)
                        first_ts = int(row[1]) if row[1] is not None else None
                        last_ts = int(row[2]) if row[2] is not None else None
                        expected_rows = self._expected_rows_for_month(year, month)
                        status = self._classify(count, expected_rows)
                        self._append_log(
                            f"{symbol} | {month_token} | {status} | rows={count} | first={self._ms_to_utc_text(first_ts)} | last={self._ms_to_utc_text(last_ts)}"
                        )
        except sqlite3.Error as exc:
            self._append_log(f"ERROR: SQLite failure during inspection: {exc}")
            return

        self._append_log("Inspect / Preview complete (read-only).")


def main() -> int:
    root = tk.Tk()
    BootstrapManagerApp(root)
    root.mainloop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

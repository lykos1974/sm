from __future__ import annotations

import calendar
import queue
import sqlite3
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import tkinter as tk
from tkinter import ttk
from urllib.error import HTTPError, URLError

from collector_import_binance_vision_fut_research import (
    YearMonth,
    build_local_zip_candidates,
    download_month_zip_to_path,
    import_symbol_month,
    namespace_symbol,
    parse_month_rows_from_payload,
    resolve_local_month_zip_path,
)
from storage import Storage

DEFAULT_DB_PATH = "data/pnf_mvp_research_clean.sqlite3"
PLACEHOLDER_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
MODES = [
    "Inspect Only",
    "Download Only",
    "Import From Local Cache",
    "Download + Import",
]


@dataclass
class PlanRow:
    symbol: str
    year: int
    month: int
    month_token: str
    status: str
    action: str
    source: str
    rows: int
    first: str
    last: str


class BootstrapManagerApp:
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title("Bootstrap Manager")
        self.root.geometry("1180x820")
        self.root.minsize(980, 700)

        self.db_path_var = tk.StringVar(value=DEFAULT_DB_PATH)
        self.start_month_var = tk.StringVar(value="2026-01")
        self.end_month_var = tk.StringVar(value="2026-03")
        self.local_cache_var = tk.StringVar(value="")
        self.mode_var = tk.StringVar(value=MODES[0])
        self.progress_var = tk.StringVar(value="Processed 0 / 0 | 0% | mode=- | success=0 | failed=0")
        self.summary_var = tk.StringVar(value="Summary output will appear here.")

        self._last_db_counts: dict[str, int] = {}
        self._latest_db_delta_text = "db_delta=-"
        self._worker: threading.Thread | None = None
        self._event_queue: queue.Queue[tuple[str, object]] = queue.Queue()
        self._current_run_active = False
        self._progress_state = {"processed": 0, "total": 0, "mode": "-", "success": 0, "failed": 0}

        self._build_ui()
        self.root.after(100, self._drain_event_queue)

    def _build_ui(self) -> None:
        container = ttk.Frame(self.root, padding=12)
        container.pack(fill="both", expand=True)
        container.columnconfigure(0, weight=1)
        container.rowconfigure(3, weight=1)

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
        ttk.Entry(frame, textvariable=self.db_path_var).grid(row=0, column=1, sticky="ew")

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
        ttk.Combobox(frame, textvariable=self.mode_var, values=MODES, state="readonly").grid(
            row=7, column=0, columnspan=2, sticky="ew", pady=(2, 0)
        )

    def _build_actions_section(self, parent: ttk.Frame) -> None:
        frame = ttk.Frame(parent)
        frame.grid(row=2, column=0, sticky="ew", pady=(0, 10))
        frame.columnconfigure(4, weight=1)

        self.inspect_button = ttk.Button(frame, text="Inspect / Preview", command=self.inspect_preview)
        self.inspect_button.grid(row=0, column=0, padx=(0, 8), sticky="w")

        self.execute_button = ttk.Button(frame, text="Execute Bootstrap", command=self.execute_bootstrap)
        self.execute_button.grid(row=0, column=1, padx=(0, 8), sticky="w")

        self.copy_button = ttk.Button(frame, text="Copy Log", command=self._copy_log)
        self.copy_button.grid(row=0, column=2, padx=(0, 8), sticky="w")

        self.db_check_button = ttk.Button(frame, text="Check DB Growth", command=self._check_db_growth)
        self.db_check_button.grid(row=0, column=3, padx=(0, 12), sticky="w")

        self.progress_bar = ttk.Progressbar(frame, orient="horizontal", mode="determinate", length=280)
        self.progress_bar.grid(row=0, column=4, sticky="ew", padx=(0, 8))

        ttk.Label(frame, textvariable=self.progress_var).grid(row=0, column=5, sticky="w")

    def _build_log_section(self, parent: ttk.Frame) -> None:
        frame = ttk.LabelFrame(parent, text="Log / Output", padding=10)
        frame.grid(row=3, column=0, sticky="nsew", pady=(0, 10))
        frame.columnconfigure(0, weight=1)
        frame.rowconfigure(0, weight=1)

        self.log_text = tk.Text(frame, height=18, wrap="none")
        self.log_text.grid(row=0, column=0, sticky="nsew")

        ybar = ttk.Scrollbar(frame, orient="vertical", command=self.log_text.yview)
        ybar.grid(row=0, column=1, sticky="ns")
        xbar = ttk.Scrollbar(frame, orient="horizontal", command=self.log_text.xview)
        xbar.grid(row=1, column=0, sticky="ew")
        self.log_text.configure(yscrollcommand=ybar.set, xscrollcommand=xbar.set)

        self._append_log("Bootstrap Manager UI loaded.")
        self._append_log("Use Inspect / Preview for read-only planning.")

    def _build_summary_section(self, parent: ttk.Frame) -> None:
        frame = ttk.LabelFrame(parent, text="Summary", padding=10)
        frame.grid(row=4, column=0, sticky="ew")
        ttk.Label(frame, textvariable=self.summary_var).pack(anchor="w")

    def _append_log(self, message: str) -> None:
        self.log_text.configure(state="normal")
        self.log_text.insert("end", message + "\n")
        self.log_text.see("end")
        self.log_text.configure(state="disabled")

    def _copy_log(self) -> None:
        text = self.log_text.get("1.0", "end-1c")
        self.root.clipboard_clear()
        self.root.clipboard_append(text)
        self._append_log("[PHASE] Log copied to clipboard")

    def _selected_symbols(self) -> list[str]:
        return [str(self.symbol_listbox.get(i)).strip().upper() for i in self.symbol_listbox.curselection() if str(self.symbol_listbox.get(i)).strip()]

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

    def _build_action(self, status: str, symbol: str, month_token: str, local_root: str) -> tuple[str, str]:
        if status == "PRESENT":
            return "SKIP", "NONE"
        has_local = False
        if local_root.strip():
            ym = YearMonth(*self._parse_month(month_token))
            for candidate in build_local_zip_candidates(local_root, symbol, ym):
                try:
                    if candidate.exists():
                        has_local = True
                        break
                except OSError:
                    has_local = False
        return ("USE_LOCAL", "LOCAL") if has_local else ("DOWNLOAD", "REMOTE")


    def _load_local_zip_payload(self, local_root: str, symbol: str, ym: YearMonth) -> tuple[bytes | None, str | None]:
        for candidate in build_local_zip_candidates(local_root, symbol, ym):
            try:
                if candidate.exists():
                    return candidate.read_bytes(), str(candidate)
            except OSError:
                continue
        return None, None

    def _collect_plan_rows(self, log_phase: bool = True) -> list[PlanRow] | None:
        db_path = Path(self.db_path_var.get().strip())
        symbols = self._selected_symbols()
        local_root = self.local_cache_var.get().strip()
        plan_rows: list[PlanRow] = []

        if not symbols:
            self._append_log("[PHASE] WARNING: No symbols selected")
            return None
        if not db_path.exists():
            self._append_log(f"[PHASE] ERROR: DB file not found: {db_path}")
            return None
        try:
            months = self._iter_month_tokens(self.start_month_var.get(), self.end_month_var.get())
        except ValueError as exc:
            self._append_log(f"[PHASE] ERROR: {exc}")
            return None

        if log_phase:
            self._append_log(
                f"[PHASE] Inspecting coverage | db={db_path} | symbols={','.join(symbols)} | months={months[0][2]}..{months[-1][2]}"
            )

        query = (
            "SELECT COUNT(*), MIN(open_time), MAX(open_time) "
            "FROM candles WHERE symbol=? AND interval='1m' AND open_time>=? AND open_time<=?"
        )
        try:
            with sqlite3.connect(str(db_path)) as conn:
                for symbol in symbols:
                    ns_symbol = namespace_symbol(symbol)
                    for year, month, month_token in months:
                        start_ms, end_ms = self._month_bounds_ms(year, month)
                        row = conn.execute(query, (ns_symbol, start_ms, end_ms)).fetchone()
                        count = int(row[0] or 0)
                        first_ts = int(row[1]) if row[1] is not None else None
                        last_ts = int(row[2]) if row[2] is not None else None
                        status = self._classify(count, self._expected_rows_for_month(year, month))
                        action, source = self._build_action(status, symbol, month_token, local_root)
                        plan_rows.append(
                            PlanRow(
                                symbol=symbol,
                                year=year,
                                month=month,
                                month_token=month_token,
                                status=status,
                                action=action,
                                source=source,
                                rows=count,
                                first=self._ms_to_utc_text(first_ts),
                                last=self._ms_to_utc_text(last_ts),
                            )
                        )
        except sqlite3.Error as exc:
            self._append_log(f"[PHASE] ERROR: SQLite failure during inspection: {exc}")
            return None
        return plan_rows

    def inspect_preview(self) -> None:
        plan_rows = self._collect_plan_rows(log_phase=True)
        if plan_rows is None:
            return
        for row in plan_rows:
            self._append_log(
                f"[PLAN] {row.symbol} | {row.month_token} | status={row.status} | action={row.action} | "
                f"source={row.source} | rows={row.rows} | first={row.first} | last={row.last}"
            )
        self._append_log("[PHASE] Inspect / Preview complete")

    def _refresh_progress_text(self) -> None:
        processed = self._progress_state["processed"]
        total = max(1, self._progress_state["total"])
        mode = self._progress_state["mode"]
        success = self._progress_state["success"]
        failed = self._progress_state["failed"]
        pct = int((processed / total) * 100) if total else 0
        self.progress_bar.configure(maximum=100, value=pct)
        self.progress_var.set(
            f"Processed {processed} / {self._progress_state['total']} | {pct}% | mode={mode} | success={success} | failed={failed} | {self._latest_db_delta_text}"
        )

    def _set_progress(self, *, processed: int | None = None, total: int | None = None,
                      mode: str | None = None, success: int | None = None, failed: int | None = None) -> None:
        if processed is not None:
            self._progress_state["processed"] = processed
        if total is not None:
            self._progress_state["total"] = total
        if mode is not None:
            self._progress_state["mode"] = mode
        if success is not None:
            self._progress_state["success"] = success
        if failed is not None:
            self._progress_state["failed"] = failed
        self._refresh_progress_text()

    def _drain_event_queue(self) -> None:
        try:
            while True:
                event, payload = self._event_queue.get_nowait()
                if event == "log":
                    self._append_log(str(payload))
                elif event == "progress":
                    self._set_progress(**payload)  # type: ignore[arg-type]
                elif event == "summary":
                    self.summary_var.set(str(payload))
                elif event == "db_delta":
                    self._latest_db_delta_text = str(payload)
                    self._refresh_progress_text()
                elif event == "done":
                    self._current_run_active = False
                    self.inspect_button.configure(state="normal")
                    self.execute_button.configure(state="normal")
                    self.db_check_button.configure(state="normal")
                    self.copy_button.configure(state="normal")
                elif event == "error":
                    self._append_log(f"[PHASE] ERROR: {payload}")
        except queue.Empty:
            pass
        self.root.after(100, self._drain_event_queue)

    def execute_bootstrap(self) -> None:
        if self._current_run_active:
            self._append_log("[PHASE] Run already active")
            return
        mode = self.mode_var.get().strip()
        if mode == "Inspect Only":
            self._append_log("[PHASE] Inspect Only is read-only; use Inspect / Preview")
            return

        plan_rows = self._collect_plan_rows(log_phase=False)
        if plan_rows is None:
            return

        self._current_run_active = True
        self.inspect_button.configure(state="disabled")
        self.execute_button.configure(state="disabled")
        self.db_check_button.configure(state="disabled")
        self.copy_button.configure(state="disabled")
        self.summary_var.set("Execution started...")
        self._latest_db_delta_text = "db_delta=-"
        self._set_progress(processed=0, total=len(plan_rows), mode=mode, success=0, failed=0)
        self._append_log(f"[PHASE] Execute {mode}")

        self._worker = threading.Thread(target=self._run_execution, args=(mode, plan_rows), daemon=True)
        self._worker.start()

    def _queue_log(self, message: str) -> None:
        self._event_queue.put(("log", message))

    def _queue_progress(self, processed: int, total: int, mode: str, success: int, failed: int) -> None:
        self._event_queue.put(("progress", {
            "processed": processed,
            "total": total,
            "mode": mode,
            "success": success,
            "failed": failed,
        }))

    def _count_symbol_rows(self, db_path: str, symbol: str) -> int:
        with sqlite3.connect(db_path) as conn:
            row = conn.execute("SELECT COUNT(*) FROM candles WHERE symbol=?", (namespace_symbol(symbol),)).fetchone()
            return int(row[0] or 0)

    def _emit_db_delta(self, db_path: str, symbol: str) -> tuple[int, int, int]:
        current = self._count_symbol_rows(db_path, symbol)
        ns_symbol = namespace_symbol(symbol)
        previous = self._last_db_counts.get(ns_symbol, current)
        delta = current - previous
        self._last_db_counts[ns_symbol] = current
        self._event_queue.put(("db_delta", f"db_delta={delta:+d}"))
        self._queue_log(f"[DB] {symbol} | rows={current} | delta={delta:+d}")
        return previous, current, delta

    def _check_db_growth(self) -> None:
        db_path = self.db_path_var.get().strip()
        symbols = self._selected_symbols()
        if not db_path:
            self._append_log("[DB] ERROR: No DB path set")
            return
        if not symbols:
            self._append_log("[DB] No symbols selected")
            return
        try:
            for symbol in symbols:
                current = self._count_symbol_rows(db_path, symbol)
                ns_symbol = namespace_symbol(symbol)
                previous = self._last_db_counts.get(ns_symbol)
                self._last_db_counts[ns_symbol] = current
                if previous is None:
                    delta_text = "n/a"
                    self._append_log(f"[DB] {symbol} | rows={current}")
                else:
                    delta = current - previous
                    delta_text = f"{delta:+d}"
                    self._append_log(f"[DB] {symbol} | rows={current} | delta={delta:+d}")
                self._latest_db_delta_text = f"db_delta={delta_text}"
            self._refresh_progress_text()
        except Exception as exc:  # noqa: BLE001
            self._append_log(f"[DB] ERROR: {exc}")

    def _run_execution(self, mode: str, plan_rows: list[PlanRow]) -> None:
        try:
            if mode == "Download Only":
                self._run_download_only(plan_rows)
            elif mode == "Import From Local Cache":
                self._run_import_from_local_cache(plan_rows)
            elif mode == "Download + Import":
                self._run_download_and_import(plan_rows)
            else:
                self._queue_log(f"[PHASE] {mode} is not implemented")
        except Exception as exc:  # noqa: BLE001
            self._event_queue.put(("error", str(exc)))
        finally:
            self._event_queue.put(("done", None))

    def _run_download_only(self, plan_rows: list[PlanRow]) -> None:
        local_root = self.local_cache_var.get().strip()
        if not local_root:
            self._queue_log("[PHASE] ERROR: Local cache root is required for Download Only mode")
            return
        total = len(plan_rows)
        processed = success = failed = skipped_months = downloaded_zips = reused_local_zips = failed_months = 0

        for row in plan_rows:
            self._queue_log(
                f"[PLAN] {row.symbol} | {row.month_token} | status={row.status} | action={row.action} | source={row.source} | rows={row.rows} | first={row.first} | last={row.last}"
            )
            result = "SKIPPED"
            if row.status == "PRESENT":
                skipped_months += 1
                success += 1
            elif row.source == "LOCAL":
                reused_local_zips += 1
                skipped_months += 1
                success += 1
            else:
                ym = YearMonth(row.year, row.month)
                target_path = resolve_local_month_zip_path(local_root, row.symbol, ym)
                try:
                    self._queue_log(f"[ACTION] {row.symbol} | {row.month_token} | downloading zip")
                    download_month_zip_to_path(row.symbol, ym, target_path)
                    downloaded_zips += 1
                    success += 1
                    result = "DOWNLOADED"
                except (HTTPError, URLError, OSError) as exc:
                    failed += 1
                    failed_months += 1
                    result = "FAILED"
                    self._queue_log(f"[RESULT] {row.symbol} | {row.month_token} | FAILED | error={exc}")
            if result != "FAILED":
                self._queue_log(f"[RESULT] {row.symbol} | {row.month_token} | {result}")
            processed += 1
            self._queue_progress(processed, total, "Download Only", success, failed)

        self._event_queue.put(("summary", " | ".join([
            f"processed_months={processed}",
            f"skipped_months={skipped_months}",
            f"downloaded_zips={downloaded_zips}",
            f"reused_local_zips={reused_local_zips}",
            f"failed_months={failed_months}",
        ])))

    def _run_import_from_local_cache(self, plan_rows: list[PlanRow]) -> None:
        db_path = self.db_path_var.get().strip()
        if db_path != DEFAULT_DB_PATH:
            self._queue_log(f"[PHASE] WARNING: Import From Local Cache is restricted to {DEFAULT_DB_PATH}")
            return
        local_root = self.local_cache_var.get().strip()
        if not local_root:
            self._queue_log("[PHASE] ERROR: Local cache root is required for Import From Local Cache mode")
            return

        total = len(plan_rows)
        processed = success = failed = skipped_months = imported_months = overwritten_partial_months = failed_months = 0
        storage = Storage(db_path)

        for row in plan_rows:
            self._queue_log(
                f"[PLAN] {row.symbol} | {row.month_token} | status={row.status} | action={row.action} | source={row.source} | rows={row.rows} | first={row.first} | last={row.last}"
            )
            result = "FAILED"
            try:
                if row.status == "PRESENT":
                    skipped_months += 1
                    success += 1
                    result = "SKIPPED"
                elif row.source == "LOCAL":
                    ym = YearMonth(row.year, row.month)
                    self._queue_log(f"[ACTION] {row.symbol} | {row.month_token} | loading local zip")
                    payload, _ = self._load_local_zip_payload(local_root, row.symbol, ym)
                    if payload is None:
                        raise FileNotFoundError("Local ZIP not found for requested month")
                    before, _, _ = self._emit_db_delta(db_path, row.symbol)
                    self._queue_log(f"[ACTION] {row.symbol} | {row.month_token} | importing candles")
                    month_rows = parse_month_rows_from_payload(payload)
                    import_symbol_month(storage, namespace_symbol(row.symbol), month_rows)
                    _, after, delta = self._emit_db_delta(db_path, row.symbol)
                    if row.status == "PARTIAL":
                        overwritten_partial_months += 1
                        result = "OVERWRITTEN"
                    else:
                        imported_months += 1
                        result = "IMPORTED"
                    if delta == 0:
                        self._queue_log(f"[PHASE] WARNING: {row.symbol} | {row.month_token} | DB delta is 0")
                    success += 1
                else:
                    failed += 1
                    failed_months += 1
                    result = "FAILED"
            except Exception as exc:  # noqa: BLE001
                failed += 1
                failed_months += 1
                result = "FAILED"
                self._queue_log(f"[RESULT] {row.symbol} | {row.month_token} | FAILED | error={exc}")
            if result != "FAILED":
                self._queue_log(f"[RESULT] {row.symbol} | {row.month_token} | {result}")
            processed += 1
            self._queue_progress(processed, total, "Import From Local Cache", success, failed)

        self._event_queue.put(("summary", " | ".join([
            f"processed_months={processed}",
            f"skipped_months={skipped_months}",
            f"imported_months={imported_months}",
            f"overwritten_partial_months={overwritten_partial_months}",
            f"failed_months={failed_months}",
        ])))

    def _run_download_and_import(self, plan_rows: list[PlanRow]) -> None:
        db_path = self.db_path_var.get().strip()
        if db_path != DEFAULT_DB_PATH:
            self._queue_log(f"[PHASE] WARNING: Download + Import is restricted to {DEFAULT_DB_PATH}")
            return
        local_root = self.local_cache_var.get().strip()
        if not local_root:
            self._queue_log("[PHASE] ERROR: Local cache root is required for Download + Import mode")
            return

        total = len(plan_rows)
        processed = success = failed = skipped_months = downloaded_zips = imported_months = overwritten_partial_months = failed_months = 0
        storage = Storage(db_path)

        for row in plan_rows:
            self._queue_log(
                f"[PLAN] {row.symbol} | {row.month_token} | status={row.status} | action={row.action} | source={row.source} | rows={row.rows} | first={row.first} | last={row.last}"
            )
            result = "FAILED"
            try:
                if row.status == "PRESENT":
                    skipped_months += 1
                    success += 1
                    result = "SKIPPED"
                else:
                    ym = YearMonth(row.year, row.month)
                    payload = None
                    if row.source == "LOCAL":
                        self._queue_log(f"[ACTION] {row.symbol} | {row.month_token} | loading local zip")
                        payload, _ = self._load_local_zip_payload(local_root, row.symbol, ym)
                    else:
                        target_path = resolve_local_month_zip_path(local_root, row.symbol, ym)
                        self._queue_log(f"[ACTION] {row.symbol} | {row.month_token} | downloading zip")
                        download_month_zip_to_path(row.symbol, ym, target_path)
                        downloaded_zips += 1
                        payload, _ = self._load_local_zip_payload(local_root, row.symbol, ym)
                    if payload is None:
                        raise FileNotFoundError("ZIP payload unavailable after download/load")
                    self._emit_db_delta(db_path, row.symbol)
                    self._queue_log(f"[ACTION] {row.symbol} | {row.month_token} | importing candles")
                    month_rows = parse_month_rows_from_payload(payload)
                    import_symbol_month(storage, namespace_symbol(row.symbol), month_rows)
                    _, _, delta = self._emit_db_delta(db_path, row.symbol)
                    if row.status == "PARTIAL":
                        overwritten_partial_months += 1
                        result = "OVERWRITTEN"
                    elif row.source == "REMOTE":
                        imported_months += 1
                        result = "DOWNLOADED_AND_IMPORTED"
                    else:
                        imported_months += 1
                        result = "IMPORTED"
                    if delta == 0:
                        self._queue_log(f"[PHASE] WARNING: {row.symbol} | {row.month_token} | DB delta is 0")
                    success += 1
            except Exception as exc:  # noqa: BLE001
                failed += 1
                failed_months += 1
                result = "FAILED"
                self._queue_log(f"[RESULT] {row.symbol} | {row.month_token} | FAILED | error={exc}")
            if result != "FAILED":
                self._queue_log(f"[RESULT] {row.symbol} | {row.month_token} | {result}")
            processed += 1
            self._queue_progress(processed, total, "Download + Import", success, failed)

        self._event_queue.put(("summary", " | ".join([
            f"processed_months={processed}",
            f"skipped_months={skipped_months}",
            f"downloaded_zips={downloaded_zips}",
            f"imported_months={imported_months}",
            f"overwritten_partial_months={overwritten_partial_months}",
            f"failed_months={failed_months}",
        ])))


def main() -> int:
    root = tk.Tk()
    BootstrapManagerApp(root)
    root.mainloop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

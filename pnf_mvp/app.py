import json
import threading
import tkinter as tk
from urllib import parse, request
from tkinter import ttk, messagebox
from datetime import datetime

try:
    import winsound
except ImportError:
    winsound = None

from pnf_engine import PnFProfile, PnFEngine, PnFColumn
from storage import Storage
from structure_engine import build_structure_state
from strategy_engine import evaluate_pullback_retest_long, evaluate_pullback_retest_short
from strategy_validation import StrategyValidationStore

APP_TITLE = "PnF MVP - Scanner"

TELEGRAM_ENABLED = True
TELEGRAM_TOKEN = "8408323454:AAH4bpkEF5YFJQGSs_wlceH_zxG3DdlquUs"
TELEGRAM_CHAT_ID = "-1003200939539"


def send_telegram_alert(message: str):
    if not TELEGRAM_ENABLED or not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return

    data = parse.urlencode({
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
    }).encode("utf-8")

    req = request.Request(
        f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
        data=data,
        method="POST",
    )
    with request.urlopen(req, timeout=8) as resp:
        resp.read()

REFRESH_MS = 3000

VALIDATION_ELIGIBLE_STATUSES = {"CANDIDATE", "WATCH", "REJECT"}

BOX_W = 18
BOX_H = 18
LEFT_AXIS_W = 70
RIGHT_AXIS_W = 70

EXTRA_ROWS_ABOVE = 8
EXTRA_ROWS_BELOW = 8
EXTRA_COLS_LEFT = 30
EXTRA_COLS_RIGHT = 120
EXTRA_ROWS_SCROLL_ABOVE = 40
EXTRA_ROWS_SCROLL_BELOW = 40


class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title(APP_TITLE)
        self.geometry("1600x960")
        self.minsize(1280, 800)

        with open("settings.json", "r", encoding="utf-8") as f:
            self.settings = json.load(f)

        self.storage = Storage(self.settings["database_path"])
        self.validation_store = StrategyValidationStore(self.settings.get("strategy_validation_db_path", "strategy_validation.db"))

        self.profiles = {}
        for symbol in self.settings["symbols"]:
            p = self.settings["profiles"][symbol]
            self.profiles[symbol] = PnFProfile(
                name=symbol,
                box_size=float(p["box_size"]),
                reversal_boxes=int(p["reversal_boxes"]),
            )

        self.alert_filters = self.settings.get(
            "alert_filters",
            {
                "allowed_types": ["DOUBLE_TOP_BREAKOUT", "DOUBLE_BOTTOM_BREAKDOWN"],
                "minimum_priority": "HIGH",
                "minimum_score": 85,
                "allowed_symbols": ["ALL"],
            },
        )

        self.engines = {}
        self.latest_scanner = {}
        self.last_processed_close_ts_by_symbol = {}

        self.selected_symbol = tk.StringVar(value=self.settings["symbols"][0])
        self.active_symbol = self.settings["symbols"][0]
        self.auto_follow = tk.BooleanVar(value=True)
        self.exchange_filter = tk.StringVar(value="ALL")
        self.structure_debug_enabled = tk.BooleanVar(value=False)
        self.structure_debug_symbol = tk.StringVar(value="BTCUSDT")
        self.status_var = tk.StringVar(value="Starting...")
        self.profile_info_var = tk.StringVar(value="Profile: loading...")
        self._refresh_running = False
        self.bootstrap_completed = False

        self.seen_alerts = set()
        self.current_red_alert_tag = None
        self.red_alert_counter = 0

        self.chart_surface = None
        self.current_symbol_drawn = None
        self.user_panned = False
        self.first_focus_done_for_symbol = {}
        self.saved_view_by_symbol = {}
        self.suppress_tree_select_handler = False

        self._build_ui()
        self._setup_signal_tags()

        self._log("Scanner started (persisted-state incremental mode).")
        self._log(f"Reading DB: {self.settings['database_path']}")
        self._log(f"Alert filters loaded: {self.alert_filters}")
        self._log(f"Strategy validation DB: {self.validation_store.db_path}")

        threading.Thread(target=self._bootstrap_from_db_once, daemon=True).start()
        self.after(REFRESH_MS, self._schedule_refresh)

    def _get_profile(self, symbol: str) -> PnFProfile:
        return self.profiles[symbol]

    def _build_ui(self):
        self.columnconfigure(1, weight=1)
        self.rowconfigure(0, weight=1)

        left = ttk.Frame(self, padding=10)
        left.grid(row=0, column=0, sticky="nsew")
        left.rowconfigure(2, weight=1)
        left.columnconfigure(0, weight=1)

        main = ttk.Frame(self, padding=10)
        main.grid(row=0, column=1, sticky="nsew")
        main.rowconfigure(1, weight=1)
        main.columnconfigure(0, weight=1)

        ttk.Label(left, text="Scanner", font=("Segoe UI", 14, "bold")).grid(row=0, column=0, sticky="w")
        ttk.Label(left, textvariable=self.profile_info_var).grid(row=1, column=0, sticky="w", pady=(0, 8))

        columns = ("exchange", "native_symbol", "state", "signal", "priority", "last", "score", "updated")
        self.tree = ttk.Treeview(left, columns=columns, show="headings", height=16)
        for col, width in [
            ("exchange", 90),
            ("native_symbol", 130),
            ("state", 150),
            ("signal", 80),
            ("priority", 90),
            ("last", 100),
            ("score", 60),
            ("updated", 90),
        ]:
            self.tree.heading(col, text=col.upper())
            self.tree.column(col, width=width, anchor="center")
        self.tree.grid(row=2, column=0, sticky="nsew")
        self.tree.bind("<<TreeviewSelect>>", self._on_tree_select)

        controls = ttk.Frame(left)
        controls.grid(row=3, column=0, sticky="ew", pady=(8, 0))
        controls.columnconfigure(0, weight=1)

        ttk.Checkbutton(controls, text="Auto-follow first active symbol", variable=self.auto_follow).grid(row=0, column=0, sticky="w")
        ttk.Label(controls, text="Exchange filter").grid(row=1, column=0, sticky="w", pady=(8, 0))
        self.exchange_filter_combo = ttk.Combobox(
            controls,
            textvariable=self.exchange_filter,
            values=["ALL", "BINANCE", "MEXC_FUT"],
            state="readonly",
        )
        self.exchange_filter_combo.grid(row=2, column=0, sticky="ew", pady=(4, 0))
        self.exchange_filter_combo.bind("<<ComboboxSelected>>", lambda _e: self._on_exchange_filter_changed())

        ttk.Checkbutton(
            controls,
            text="Structure debug",
            variable=self.structure_debug_enabled,
        ).grid(row=3, column=0, sticky="w", pady=(8, 0))

        self.structure_debug_combo = ttk.Combobox(
            controls,
            textvariable=self.structure_debug_symbol,
            values=[],
            state="readonly",
        )
        self.structure_debug_combo.grid(row=4, column=0, sticky="ew", pady=(4, 0))

        ttk.Button(controls, text="Show structure now", command=self._show_structure_debug_now).grid(row=5, column=0, sticky="ew", pady=(8, 0))
        ttk.Button(controls, text="Copy log", command=self._copy_log).grid(row=6, column=0, sticky="ew", pady=(8, 0))
        ttk.Button(controls, text="Refresh now", command=self._manual_refresh).grid(row=7, column=0, sticky="ew", pady=(8, 0))
        ttk.Button(controls, text="Rebuild selected", command=self._rebuild_selected).grid(row=8, column=0, sticky="ew", pady=(8, 0))
        ttk.Button(controls, text="Center latest", command=self._center_latest).grid(row=9, column=0, sticky="ew", pady=(8, 0))
        ttk.Button(controls, text="Export selected columns CSV", command=self._export_selected).grid(row=10, column=0, sticky="ew", pady=(8, 0))

        header = ttk.Frame(main)
        header.grid(row=0, column=0, sticky="ew")
        header.columnconfigure(0, weight=1)

        ttk.Label(header, textvariable=self.selected_symbol, font=("Segoe UI", 16, "bold")).grid(row=0, column=0, sticky="w")
        self.meta_label = ttk.Label(header, text="")
        self.meta_label.grid(row=1, column=0, sticky="w")
        ttk.Label(header, textvariable=self.status_var).grid(row=0, column=1, sticky="e")

        chart_frame = ttk.Frame(main)
        chart_frame.grid(row=1, column=0, sticky="nsew", pady=(8, 8))
        chart_frame.rowconfigure(0, weight=1)
        chart_frame.columnconfigure(1, weight=1)

        self.left_axis = tk.Canvas(chart_frame, width=LEFT_AXIS_W, background="#101418", highlightthickness=0)
        self.left_axis.grid(row=0, column=0, sticky="ns")
        self.chart_canvas = tk.Canvas(chart_frame, background="#101418", highlightthickness=0)
        self.chart_canvas.grid(row=0, column=1, sticky="nsew")
        self.right_axis = tk.Canvas(chart_frame, width=RIGHT_AXIS_W, background="#101418", highlightthickness=0)
        self.right_axis.grid(row=0, column=2, sticky="ns")

        self.v_scroll = ttk.Scrollbar(chart_frame, orient="vertical", command=self._y_scroll_all)
        self.v_scroll.grid(row=0, column=3, sticky="ns")
        self.h_scroll = ttk.Scrollbar(chart_frame, orient="horizontal", command=self._x_scroll_all)
        self.h_scroll.grid(row=1, column=1, sticky="ew")

        self.chart_canvas.configure(xscrollcommand=self.h_scroll.set, yscrollcommand=self._on_chart_yview)
        self.chart_canvas.bind("<ButtonPress-1>", self._on_chart_drag_start)
        self.chart_canvas.bind("<B1-Motion>", self._on_chart_drag_move)
        self.chart_canvas.bind("<MouseWheel>", self._on_mousewheel_vertical)
        self.chart_canvas.bind("<Shift-MouseWheel>", self._on_mousewheel_horizontal)

        bottom = ttk.Notebook(main)
        bottom.grid(row=2, column=0, sticky="nsew")
        signals_tab = ttk.Frame(bottom, padding=8)
        log_tab = ttk.Frame(bottom, padding=8)
        bottom.add(signals_tab, text="Signals")
        bottom.add(log_tab, text="Log")

        self.signal_text = tk.Text(signals_tab, height=10, wrap="word")
        self.signal_text.pack(fill="both", expand=True)
        self.log_text = tk.Text(log_tab, height=10, wrap="word")
        self.log_text.pack(fill="both", expand=True)
        self.log_text.insert("end", "Application started.\n")
        self.log_text.configure(state="disabled")
        self.log_text.bind("<Control-c>", self._copy_log)

    def _setup_signal_tags(self):
        self.signal_text.tag_configure("normal_alert", foreground="#000000")
        self.signal_text.tag_configure("latest_alert", foreground="#cc0000")

    def _schedule_refresh(self):
        if not self._refresh_running and self.bootstrap_completed:
            threading.Thread(target=self._refresh_incremental_once, daemon=True).start()
        self.after(REFRESH_MS, self._schedule_refresh)

    def _manual_refresh(self):
        if self._refresh_running:
            self._log("Refresh already running.")
            return
        if not self.bootstrap_completed:
            self._log("Bootstrap still running.")
            return
        threading.Thread(target=self._refresh_incremental_once, daemon=True).start()

    def _priority_from_snapshot(self, state: str, signal: str, score: int) -> str:
        if signal in {"BUY", "SELL"} and score >= 85:
            return "HIGH"
        if state in {"BULLISH_BREAKOUT", "BEARISH_BREAKDOWN"}:
            return "HIGH"
        if state in {"BULLISH_TREND", "BEARISH_TREND"} and score >= 65:
            return "MEDIUM"
        if state == "RANGE":
            return "LOW"
        if state == "EARLY":
            return "LOW"
        if score >= 60:
            return "MEDIUM"
        return "LOW"

    def _priority_rank(self, priority: str) -> int:
        return {"LOW": 1, "MEDIUM": 2, "HIGH": 3}.get(priority, 0)

    def _signal_alert_key(self, symbol: str, sig: dict) -> tuple:
        return (symbol, sig["type"], int(sig["column_idx"]), round(float(sig["trigger"]), 10))

    def _passes_alert_filters(self, symbol: str, sig: dict, snapshot: dict) -> bool:
        allowed_types = set(self.alert_filters.get("allowed_types", []))
        minimum_priority = str(self.alert_filters.get("minimum_priority", "HIGH")).upper()
        minimum_score = int(self.alert_filters.get("minimum_score", 85))
        allowed_symbols = self.alert_filters.get("allowed_symbols", ["ALL"])

        if allowed_types and sig["type"] not in allowed_types:
            return False
        if "ALL" not in allowed_symbols and symbol not in allowed_symbols:
            return False
        if self._priority_rank(str(snapshot.get("priority", "LOW")).upper()) < self._priority_rank(minimum_priority):
            return False
        if int(snapshot.get("score", 0)) < minimum_score:
            return False
        return True

    def _format_signal_timestamp(self, ts_ms) -> str:
        try:
            ts_ms = int(ts_ms)
            if ts_ms <= 0:
                return "UNKNOWN_TIME"
            dt = datetime.utcfromtimestamp(ts_ms / 1000.0)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return "UNKNOWN_TIME"

    def _box_decimals(self, box_size: float) -> int:
        box_str = f"{box_size:.10f}".rstrip("0").rstrip(".")
        if "." in box_str:
            return len(box_str.split(".")[1])
        return 0

    def _format_price(self, price: float, box_size: float) -> str:
        decimals = max(2, self._box_decimals(box_size))
        return f"{price:.{decimals}f}"

    def _play_alarm_sound(self):
        if winsound is None:
            return
        try:
            winsound.MessageBeep(winsound.MB_ICONHAND)
            winsound.Beep(1200, 250)
            winsound.Beep(900, 250)
            winsound.Beep(1200, 250)
            winsound.Beep(900, 250)
        except Exception:
            pass

    def _show_alert_popup(self, symbol: str, sig: dict, snapshot: dict):
        popup = tk.Toplevel(self)
        popup.title("NEW SIGNAL ALERT")
        popup.geometry("520x220")
        popup.transient(self)
        popup.lift()
        popup.attributes("-topmost", True)

        frame = ttk.Frame(popup, padding=14)
        frame.pack(fill="both", expand=True)

        profile = self._get_profile(symbol)
        trigger_fmt = self._format_price(sig["trigger"], profile.box_size)
        signal_time = self._format_signal_timestamp(sig.get("timestamp"))
        exchange, native_symbol = self._split_storage_symbol(symbol)

        ttk.Label(frame, text="NEW SIGNAL", font=("Segoe UI", 14, "bold")).pack(anchor="w", pady=(0, 10))
        body = (
            f"Time: {signal_time}\n"
            f"Exchange: {exchange}\n"
            f"Symbol: {native_symbol}\n"
            f"Type: {sig['type']}\n"
            f"Trigger: {trigger_fmt}\n"
            f"State: {snapshot.get('state', 'N/A')}\n"
            f"Priority: {snapshot.get('priority', 'N/A')}\n"
            f"Score: {snapshot.get('score', 'N/A')}\n"
            f"Note: {sig['note']}"
        )
        ttk.Label(frame, text=body, justify="left").pack(anchor="w")
        ttk.Button(frame, text="OK", command=popup.destroy).pack(anchor="e", pady=(14, 0))
        self._play_alarm_sound()

    def _format_telegram_alert_message(self, symbol: str, sig: dict, snapshot: dict) -> str:
        profile = self._get_profile(symbol)
        exchange, native_symbol = self._split_storage_symbol(symbol)
        trigger_fmt = self._format_price(sig["trigger"], profile.box_size)
        signal_time = self._format_signal_timestamp(sig.get("timestamp"))
        comparison_level = sig.get("comparison_level")
        comparison_fmt = "N/A" if comparison_level is None else self._format_price(float(comparison_level), profile.box_size)

        return (
            f"🚨 PNF ALERT\n"
            f"Exchange: {exchange}\n"
            f"Symbol: {native_symbol}\n"
            f"Type: {sig['type']}\n"
            f"Trigger: {trigger_fmt}\n"
            f"Comparison: {comparison_fmt}\n"
            f"Column: {sig.get('column_idx', 'N/A')}\n"
            f"State: {snapshot.get('state', 'N/A')}\n"
            f"Priority: {snapshot.get('priority', 'N/A')}\n"
            f"Score: {snapshot.get('score', 'N/A')}\n"
            f"Time: {signal_time}"
        )

    def _append_filtered_alert_if_needed(self, symbol: str, sig: dict, snapshot: dict):
        if not self._passes_alert_filters(symbol, sig, snapshot):
            return
        key = self._signal_alert_key(symbol, sig)
        if key in self.seen_alerts:
            return
        self.seen_alerts.add(key)

        profile = self._get_profile(symbol)
        trigger_fmt = self._format_price(sig["trigger"], profile.box_size)
        signal_time = self._format_signal_timestamp(sig.get("timestamp"))
        line = (
            f"[{signal_time}] {symbol} | {sig['type']} | "
            f"trigger={trigger_fmt} | priority={snapshot.get('priority', 'N/A')} | "
            f"score={snapshot.get('score', 'N/A')} | state={snapshot.get('state', 'N/A')} | "
            f"note={sig['note']}\n"
        )
        self.signal_text.insert("1.0", line, "normal_alert")

        if self.current_red_alert_tag:
            self.signal_text.tag_remove(self.current_red_alert_tag, "1.0", "end")
        self.red_alert_counter += 1
        tag_name = f"latest_dynamic_{self.red_alert_counter}"
        self.signal_text.tag_configure(tag_name, foreground="#cc0000")
        self.signal_text.tag_add(tag_name, "1.0", f"1.0 + {len(line)}c")
        self.current_red_alert_tag = tag_name

        try:
            telegram_message = self._format_telegram_alert_message(symbol, sig, snapshot)
            send_telegram_alert(telegram_message)
        except Exception as e:
            self._log(f"Telegram send failed: {e}")

        self._show_alert_popup(symbol, sig, snapshot)

    def _build_snapshot(self, symbol: str, engine: PnFEngine) -> dict:
        profile = self._get_profile(symbol)
        last_price = engine.last_price or 0.0
        signal = engine.latest_signal_name() or "NONE"
        state = engine.market_state()
        score = engine.score()
        priority = self._priority_from_snapshot(state, signal, score)
        return {
            "symbol": symbol,
            "state": state,
            "signal": signal,
            "priority": priority,
            "last": self._format_price(last_price, profile.box_size),
            "score": score,
            "updated": datetime.utcnow().strftime("%H:%M:%S"),
        }

    def _save_engine_snapshot(self, symbol: str, engine: PnFEngine, last_processed_close_ts: int | None, snapshot: dict):
        profile = self._get_profile(symbol)
        state = engine.state_dict()
        state["last_processed_close_ts"] = last_processed_close_ts
        self.storage.save_state(symbol, profile, state)
        self.storage.replace_columns(symbol, profile, engine.columns)
        self.storage.upsert_scanner_snapshot(
            symbol,
            profile.name,
            snapshot["state"],
            snapshot["signal"],
            float(engine.last_price or 0.0),
            int(snapshot["score"]),
            snapshot["updated"],
        )

    def _load_stateful_engine(self, symbol: str):
        profile = self._get_profile(symbol)
        engine = PnFEngine(profile)
        state = self.storage.load_state(symbol, profile.name)
        columns = self.storage.load_columns(symbol, profile.name)
        if not columns:
            return engine, None, False

        engine.columns = [
            PnFColumn(
                idx=int(row["idx"]),
                kind=row["kind"],
                top=float(row["top"]),
                bottom=float(row["bottom"]),
                start_ts=int(row["start_ts"]),
                end_ts=int(row["end_ts"]),
            )
            for row in columns
        ]

        if state:
            engine.last_price = state.get("last_price")
            signals = state.get("signals") or []
            if isinstance(signals, list):
                engine.signals = signals[-100:]
                for sig in signals:
                    try:
                        engine._emitted_signal_keys.add((sig["type"], int(sig["column_idx"])))
                    except Exception:
                        pass
            last_processed = state.get("last_processed_close_ts")
        else:
            last_processed = engine.columns[-1].end_ts if engine.columns else None

        return engine, (int(last_processed) if last_processed else None), True

    def _load_all_closed_candles(self, symbol: str):
        candles = self.storage.load_recent_candles(symbol, None)
        return candles[:-1] if len(candles) > 1 else []

    def _load_new_closed_candles(self, symbol: str, after_close_ts: int | None):
        candles = self.storage.load_candles_after(symbol, after_close_ts)
        return candles[:-1] if len(candles) > 1 else []

    def _bootstrap_from_db_once(self):
        self._refresh_running = True
        self.status_var.set("Bootstrapping from DB...")
        try:
            new_engines = {}
            new_snapshots = {}
            new_last_processed = {}

            recent_persisted = self.storage.load_recent_signals(limit=2000)
            for row in recent_persisted:
                sig = {
                    "type": row["signal_type"],
                    "trigger": row["trigger"],
                    "column_idx": row["column_idx"],
                }
                self.seen_alerts.add(self._signal_alert_key(row["symbol"], sig))

            for symbol in self.settings["symbols"]:
                engine, last_processed, loaded_from_cache = self._load_stateful_engine(symbol)
                delta_candles = []

                if loaded_from_cache:
                    delta_candles = self._load_new_closed_candles(symbol, last_processed)
                    source_label = f"cache+delta({len(delta_candles)})"
                else:
                    full_candles = self._load_all_closed_candles(symbol)
                    source_label = f"full-rebuild({len(full_candles)})"
                    latest_closed = None
                    for candle in full_candles:
                        latest_closed = int(candle["close_time"])
                        engine.update_from_price(latest_closed, candle["close"])
                    last_processed = latest_closed

                for candle in delta_candles:
                    last_processed = int(candle["close_time"])
                    engine.update_from_price(last_processed, candle["close"])

                snapshot = self._build_snapshot(symbol, engine)
                self._save_engine_snapshot(symbol, engine, last_processed, snapshot)
                new_engines[symbol] = engine
                new_snapshots[symbol] = snapshot
                if last_processed is not None:
                    new_last_processed[symbol] = last_processed
                self._log(f"{symbol} bootstrap source={source_label} columns={len(engine.columns)}")

            def apply_ui_updates():
                current_symbol = self.active_symbol
                self.engines = new_engines
                self.latest_scanner = new_snapshots
                self.last_processed_close_ts_by_symbol = new_last_processed
                self._refresh_tree()
                self._draw_selected(current_symbol)
                self._focus_active_area(current_symbol)
                self._save_current_view_for_symbol(current_symbol)
                self.bootstrap_completed = True
                self._update_structure_debug_choices()
                self.status_var.set("DB synced")
                self._log("Bootstrap completed. Incremental refresh enabled.")

            self.after(0, apply_ui_updates)
        except Exception as e:
            self.after(0, lambda: self._log(f"Bootstrap failed: {e}"))
            self.after(0, lambda: self.status_var.set("Bootstrap error"))
        finally:
            self._refresh_running = False

    def _refresh_incremental_once(self):
        self._refresh_running = True
        self.status_var.set("Refreshing from DB...")
        try:
            new_snapshots = {}
            new_signal_objects = []

            for symbol in self.settings["symbols"]:
                engine = self.engines.get(symbol)
                if engine is None:
                    engine, last_processed, _ = self._load_stateful_engine(symbol)
                    self.engines[symbol] = engine
                    self.last_processed_close_ts_by_symbol[symbol] = last_processed

                last_processed = self.last_processed_close_ts_by_symbol.get(symbol)
                new_candles = self._load_new_closed_candles(symbol, last_processed)

                for candle in new_candles:
                    candle_close_ts = int(candle["close_time"])
                    result = engine.update_from_price(candle_close_ts, candle["close"])
                    last_processed = candle_close_ts
                    if result["new_signal"]:
                        event_snapshot = self._build_snapshot(symbol, engine)
                        for sig in result["new_signals"]:
                            new_signal_objects.append((symbol, sig, event_snapshot))
                            self.storage.insert_signal(symbol, self._get_profile(symbol), sig)

                self.last_processed_close_ts_by_symbol[symbol] = last_processed
                snapshot = self._build_snapshot(symbol, engine)
                new_snapshots[symbol] = snapshot
                self._save_engine_snapshot(symbol, engine, last_processed, snapshot)
                self._run_validation_for_symbol(symbol, engine, new_candles)

            def apply_ui_updates():
                previous_symbol = self.current_symbol_drawn
                current_symbol = self.active_symbol
                self.selected_symbol.set(current_symbol)
                symbol_changed = previous_symbol != current_symbol

                self.latest_scanner = new_snapshots
                self._refresh_tree()
                self._draw_selected(current_symbol)

                if symbol_changed:
                    self.user_panned = False
                    self._focus_active_area(current_symbol)
                    self._save_current_view_for_symbol(current_symbol)
                else:
                    if self.user_panned:
                        self.after(10, lambda: self._restore_saved_view_for_symbol(current_symbol))
                    elif not self.first_focus_done_for_symbol.get(current_symbol, False):
                        self._focus_active_area(current_symbol)
                        self._save_current_view_for_symbol(current_symbol)

                for symbol, sig, snapshot in new_signal_objects:
                    self._append_filtered_alert_if_needed(symbol, sig, snapshot)
                self.status_var.set("DB synced")

            self.after(0, apply_ui_updates)
        except Exception as e:
            self.after(0, lambda: self._log(f"Refresh failed: {e}"))
            self.after(0, lambda: self.status_var.set("Refresh error"))
        finally:
            self._refresh_running = False

    def _refresh_tree(self):
        existing = set(self.tree.get_children())
        priority_rank_map = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}

        selected_filter = self.exchange_filter.get()
        filtered_items = []
        for item in self.latest_scanner.values():
            exchange, native_symbol = self._split_storage_symbol(item["symbol"])
            if selected_filter == "ALL" or exchange == selected_filter:
                filtered_items.append(item)

        ordered = sorted(
            filtered_items,
            key=lambda x: (priority_rank_map.get(x["priority"], 99), -x["score"], x["symbol"]),
        )

        chosen = self.active_symbol
        best = ordered[0]["symbol"] if ordered else None
        for item in ordered:
            iid = item["symbol"]
            exchange, native_symbol = self._split_storage_symbol(item["symbol"])
            values = (exchange, native_symbol, item["state"], item["signal"], item["priority"], item["last"], item["score"], item["updated"])
            if iid in existing:
                self.tree.item(iid, values=values)
                existing.remove(iid)
            else:
                self.tree.insert("", "end", iid=iid, values=values)
        for iid in existing:
            self.tree.delete(iid)

        self.suppress_tree_select_handler = True
        try:
            if chosen in self.tree.get_children():
                if self.tree.selection() != (chosen,):
                    self.tree.selection_set(chosen)
            elif self.auto_follow.get() and best:
                self.active_symbol = best
                self.selected_symbol.set(best)
                if self.tree.selection() != (best,):
                    self.tree.selection_set(best)
        finally:
            self.suppress_tree_select_handler = False
        self._update_profile_info()


    def _update_structure_debug_choices(self):
        try:
            symbols = sorted(self.engines.keys())
            self.structure_debug_combo["values"] = symbols
            current = (self.structure_debug_symbol.get() or "").strip()
            if symbols and (not current or current not in symbols):
                self.structure_debug_symbol.set(symbols[0])
        except Exception as e:
            self._log(f"Structure debug choices update failed: {e}")

    def _show_structure_debug_now(self):
        try:
            if not self.structure_debug_enabled.get():
                self._log("Structure debug is disabled.")
                return

            symbol = (self.structure_debug_symbol.get() or "").strip()
            if not symbol:
                self._log("No structure debug symbol selected.")
                return

            engine = self.engines.get(symbol)
            if engine is None:
                self._log(f"Structure debug symbol not loaded: {symbol}")
                return

            structure = build_structure_state(
                symbol=symbol,
                profile=self._get_profile(symbol),
                columns=engine.columns,
                latest_signal_name=engine.latest_signal_name(),
                market_state=engine.market_state(),
                last_price=getattr(engine, "last_price", None),
            )

            self._log("---- STRUCTURE DEBUG (manual) ----")
            for key, value in structure.items():
                self._log(f"{key}: {value}")
            self._log("----------------------------------")
            self._log_strategy_if_available(symbol, engine, structure)
        except Exception as e:
            self._log(f"Structure debug manual error: {e}")


    def _log_strategy_if_available(self, symbol: str, engine, structure: dict):
        try:
            setup_long = evaluate_pullback_retest_long(
                symbol=symbol,
                profile=self._get_profile(symbol),
                columns=engine.columns,
                structure_state=structure,
            )

            setup_short = evaluate_pullback_retest_short(
                symbol=symbol,
                profile=self._get_profile(symbol),
                columns=engine.columns,
                structure_state=structure,
            )

            setups = [s for s in (setup_long, setup_short) if s]
            if not setups:
                return

            profile = self._get_profile(symbol)

            def fmt(v):
                if v is None:
                    return "N/A"
                try:
                    return self._format_price(float(v), profile.box_size)
                except Exception:
                    return str(v)

            for setup in setups:
                parts = [
                    "[STRATEGY]",
                    str(symbol),
                    str(setup.get("strategy")),
                    str(setup.get("side")),
                    str(setup.get("status")),
                ]

                if setup.get("zone_low") is not None or setup.get("zone_high") is not None:
                    parts.append(f"zone={fmt(setup.get('zone_low'))}-{fmt(setup.get('zone_high'))}")
                if setup.get("ideal_entry") is not None:
                    parts.append(f"entry={fmt(setup.get('ideal_entry'))}")
                if setup.get("invalidation") is not None:
                    parts.append(f"sl={fmt(setup.get('invalidation'))}")
                if setup.get("tp1") is not None:
                    parts.append(f"tp1={fmt(setup.get('tp1'))}")
                if setup.get("tp2") is not None:
                    parts.append(f"tp2={fmt(setup.get('tp2'))}")
                if setup.get("rr1") is not None:
                    parts.append(f"rr1={float(setup.get('rr1')):.2f}")
                if setup.get("rr2") is not None:
                    parts.append(f"rr2={float(setup.get('rr2')):.2f}")
                if setup.get("pullback_quality") is not None:
                    parts.append(f"pullback={setup.get('pullback_quality')}")
                if setup.get("risk_quality") is not None:
                    parts.append(f"risk={setup.get('risk_quality')}")
                if setup.get("reward_quality") is not None:
                    parts.append(f"reward={setup.get('reward_quality')}")
                if setup.get("quality_grade") is not None:
                    parts.append(f"grade={setup.get('quality_grade')}")
                if setup.get("quality_score") is not None:
                    parts.append(f"score={setup.get('quality_score')}")
                if setup.get("reject_reason") is not None:
                    parts.append(f"reject={setup.get('reject_reason')}")
                if setup.get("reason") is not None:
                    parts.append(f"reason={setup.get('reason')}")

                self._log(" | ".join(parts))
        except Exception as e:
            self._log(f"Strategy engine error for {symbol}: {e}")


    def _debug_structure_if_needed(self, symbol: str, engine):
        try:
            if not self.structure_debug_enabled.get():
                return

            watch = (self.structure_debug_symbol.get() or "").strip()
            if not watch:
                return

            if watch not in symbol:
                return

            structure = build_structure_state(
                symbol=symbol,
                profile=self._get_profile(symbol),
                columns=engine.columns,
                latest_signal_name=engine.latest_signal_name(),
                market_state=engine.market_state(),
                last_price=getattr(engine, "last_price", None),
            )

            self._log("---- STRUCTURE DEBUG ----")
            for key, value in structure.items():
                self._log(f"{key}: {value}")
            self._log("-------------------------")
            self._log_strategy_if_available(symbol, engine, structure)
        except Exception as e:
            self._log(f"Structure engine error for {symbol}: {e}")


    def _evaluate_strategy_setups(self, symbol: str, engine: PnFEngine):
        structure = build_structure_state(
            symbol=symbol,
            profile=self._get_profile(symbol),
            columns=engine.columns,
            latest_signal_name=engine.latest_signal_name(),
            market_state=engine.market_state(),
            last_price=getattr(engine, "last_price", None),
        )

        setup_long = evaluate_pullback_retest_long(
            symbol=symbol,
            profile=self._get_profile(symbol),
            columns=engine.columns,
            structure_state=structure,
        )

        setup_short = evaluate_pullback_retest_short(
            symbol=symbol,
            profile=self._get_profile(symbol),
            columns=engine.columns,
            structure_state=structure,
        )

        return structure, [s for s in (setup_long, setup_short) if s]

    def _run_validation_for_symbol(self, symbol: str, engine: PnFEngine, new_candles: list):
        if engine is None or not engine.columns:
            return

        for candle in new_candles:
            close_ts = int(candle.get("close_time") or 0)
            close_price = float(candle.get("close") or 0.0)
            high_price = float(candle.get("high", close_price) or close_price)
            low_price = float(candle.get("low", close_price) or close_price)
            self.validation_store.update_pending_with_candle(
                symbol=symbol,
                close_ts=close_ts,
                high_price=high_price,
                low_price=low_price,
                close_price=close_price,
            )

        structure, setups = self._evaluate_strategy_setups(symbol, engine)
        reference_ts = self.last_processed_close_ts_by_symbol.get(symbol)
        if reference_ts is None:
            return

        for setup in setups:
            status = str(setup.get("status") or "").upper()
            if status not in VALIDATION_ELIGIBLE_STATUSES:
                continue
            self.validation_store.register_setup(
                symbol=symbol,
                setup=setup,
                structure_state=structure,
                reference_ts=int(reference_ts),
            )

    def _split_storage_symbol(self, storage_symbol: str):
        if ":" in storage_symbol:
            exchange, native_symbol = storage_symbol.split(":", 1)
            return exchange, native_symbol
        return "BINANCE", storage_symbol

    def _safe_filename_part(self, value: str) -> str:
        return value.replace(":", "_").replace("/", "_").replace("\\", "_")

    def _on_exchange_filter_changed(self):
        self._refresh_tree()

        selected_filter = self.exchange_filter.get()
        available = []
        for item in self.latest_scanner.values():
            exchange, _native_symbol = self._split_storage_symbol(item["symbol"])
            if selected_filter == "ALL" or exchange == selected_filter:
                available.append(item["symbol"])

        if self.active_symbol not in available and available:
            self.active_symbol = available[0]
            self.selected_symbol.set(available[0])

        self._draw_selected(self.active_symbol)
        self._update_profile_info()

    def _update_profile_info(self):
        symbol = self.selected_symbol.get()
        if symbol not in self.profiles:
            self.profile_info_var.set("Profile: N/A")
            return
        profile = self._get_profile(symbol)
        exchange, native_symbol = self._split_storage_symbol(symbol)
        self.profile_info_var.set(f"Profile: {native_symbol} | Exchange {exchange} | Box {profile.box_size} | Rev {profile.reversal_boxes}")

    def _on_tree_select(self, _event=None):
        if self.suppress_tree_select_handler:
            return
        selection = self.tree.selection()
        if selection:
            symbol = selection[0]
            self.active_symbol = symbol
            self.selected_symbol.set(symbol)
            self._update_profile_info()
            self.user_panned = False
            self._draw_selected(symbol)
            self._focus_active_area(symbol)
            self._save_current_view_for_symbol(symbol)

    def _on_chart_drag_start(self, event):
        self.chart_canvas.scan_mark(event.x, event.y)

    def _on_chart_drag_move(self, event):
        self.chart_canvas.scan_dragto(event.x, event.y, gain=1)
        self.user_panned = True
        self._sync_axes_to_chart_yview()
        self._save_current_view_for_symbol()

    def _on_mousewheel_vertical(self, event):
        delta = -1 * int(event.delta / 120)
        self.chart_canvas.yview_scroll(delta, "units")
        self.user_panned = True
        self._sync_axes_to_chart_yview()
        self._save_current_view_for_symbol()

    def _on_mousewheel_horizontal(self, event):
        delta = -1 * int(event.delta / 120)
        self.chart_canvas.xview_scroll(delta, "units")
        self.user_panned = True
        self._save_current_view_for_symbol()

    def _on_chart_yview(self, first, last):
        self.v_scroll.set(first, last)
        self._sync_axes_to_chart_yview()

    def _x_scroll_all(self, *args):
        self.chart_canvas.xview(*args)
        self.user_panned = True
        self._save_current_view_for_symbol()

    def _y_scroll_all(self, *args):
        self.chart_canvas.yview(*args)
        self.user_panned = True
        self._sync_axes_to_chart_yview()
        self._save_current_view_for_symbol()

    def _sync_axes_to_chart_yview(self):
        try:
            first, _ = self.chart_canvas.yview()
            self.left_axis.yview_moveto(first)
            self.right_axis.yview_moveto(first)
        except Exception:
            pass

    def _build_chart_surface(self, symbol: str):
        engine = self.engines.get(symbol)
        if not engine or not engine.columns:
            return None

        profile = self._get_profile(symbol)
        cols = engine.columns
        actual_top = max(c.top for c in cols)
        actual_bottom = min(c.bottom for c in cols)
        display_top = actual_top + EXTRA_ROWS_ABOVE * profile.box_size
        display_bottom = actual_bottom - EXTRA_ROWS_BELOW * profile.box_size

        price_levels = []
        current = display_bottom
        while current <= display_top + 1e-9:
            price_levels.append(round(current, 10))
            current += profile.box_size

        rows = len(price_levels)
        cols_count = len(cols)
        scroll_rows = rows + EXTRA_ROWS_SCROLL_ABOVE + EXTRA_ROWS_SCROLL_BELOW
        scroll_cols = cols_count + EXTRA_COLS_LEFT + EXTRA_COLS_RIGHT
        return {
            "profile": profile,
            "cols": cols,
            "display_bottom": display_bottom,
            "plot_w": max(scroll_cols * BOX_W, 1),
            "plot_h": max(scroll_rows * BOX_H, 1),
            "scroll_rows": scroll_rows,
            "scroll_cols": scroll_cols,
            "col_offset": EXTRA_COLS_LEFT,
            "row_offset": EXTRA_ROWS_SCROLL_BELOW,
        }

    def _draw_selected(self, symbol: str):
        self.chart_canvas.delete("all")
        self.left_axis.delete("all")
        self.right_axis.delete("all")
        self.chart_surface = None
        self.current_symbol_drawn = symbol

        surface = self._build_chart_surface(symbol)
        if not surface:
            self._draw_empty_canvases()
            self.meta_label.config(text="State: N/A | Signal: N/A | Last: N/A | Score: N/A | Priority: N/A")
            return

        self.chart_surface = surface
        plot_w = surface["plot_w"]
        plot_h = surface["plot_h"]
        profile = surface["profile"]
        cols = surface["cols"]
        display_bottom = surface["display_bottom"]
        scroll_rows = surface["scroll_rows"]
        scroll_cols = surface["scroll_cols"]
        col_offset = surface["col_offset"]
        row_offset = surface["row_offset"]
        decimals = self._box_decimals(profile.box_size)

        self.chart_canvas.configure(scrollregion=(0, 0, plot_w, plot_h))
        self.left_axis.configure(scrollregion=(0, 0, LEFT_AXIS_W, plot_h))
        self.right_axis.configure(scrollregion=(0, 0, RIGHT_AXIS_W, plot_h))

        self.chart_canvas.create_rectangle(0, 0, plot_w, plot_h, fill="#101418", outline="")
        self.left_axis.create_rectangle(0, 0, LEFT_AXIS_W, plot_h, fill="#101418", outline="")
        self.right_axis.create_rectangle(0, 0, RIGHT_AXIS_W, plot_h, fill="#101418", outline="")

        for row in range(scroll_rows + 1):
            y = plot_h - row * BOX_H
            self.chart_canvas.create_line(0, y, plot_w, y, fill="#1c2329")
        for col in range(scroll_cols + 1):
            x = col * BOX_W
            self.chart_canvas.create_line(x, 0, x, plot_h, fill="#182026")

        full_bottom = display_bottom - (row_offset * profile.box_size)
        for global_row_idx in range(scroll_rows):
            price = full_bottom + global_row_idx * profile.box_size
            y_top = plot_h - (global_row_idx + 1) * BOX_H
            y_bottom = plot_h - global_row_idx * BOX_H
            y_center = (y_top + y_bottom) / 2
            self.left_axis.create_text(LEFT_AXIS_W - 6, y_center, text=f"{price:.{decimals}f}", fill="#95a3ad", anchor="e", font=("Consolas", 9))
            self.right_axis.create_text(6, y_center, text=f"{price:.{decimals}f}", fill="#95a3ad", anchor="w", font=("Consolas", 9))

        for i, col in enumerate(cols):
            global_col_idx = i + col_offset
            x_left = global_col_idx * BOX_W
            x_right = x_left + BOX_W
            x_center = (x_left + x_right) / 2
            for price in col.levels(profile.box_size):
                local_row_idx = int(round((price - display_bottom) / profile.box_size))
                global_row_idx = local_row_idx + row_offset
                y_top = plot_h - (global_row_idx + 1) * BOX_H
                y_bottom = plot_h - global_row_idx * BOX_H
                y_center = (y_top + y_bottom) / 2
                if col.kind == "X":
                    r = min(BOX_W, BOX_H) * 0.30
                    self.chart_canvas.create_line(x_center - r, y_center - r, x_center + r, y_center + r, fill="#64d2ff", width=2)
                    self.chart_canvas.create_line(x_center - r, y_center + r, x_center + r, y_center - r, fill="#64d2ff", width=2)
                else:
                    r = min(BOX_W, BOX_H) * 0.32
                    self.chart_canvas.create_oval(x_center - r, y_center - r, x_center + r, y_center + r, outline="#ff7b72", width=2)

        snapshot = self.latest_scanner.get(symbol, {})
        self.meta_label.config(
            text=(
                f"State: {snapshot.get('state', 'N/A')} | "
                f"Signal: {snapshot.get('signal', 'N/A')} | "
                f"Last: {snapshot.get('last', 'N/A')} | "
                f"Score: {snapshot.get('score', 'N/A')} | "
                f"Priority: {snapshot.get('priority', 'N/A')}"
            )
        )
        self._update_profile_info()
        self._sync_axes_to_chart_yview()

    def _draw_empty_canvases(self):
        cw = max(self.chart_canvas.winfo_width(), 500)
        ch = max(self.chart_canvas.winfo_height(), 400)
        self.chart_canvas.create_rectangle(0, 0, cw, ch, fill="#101418", outline="")
        self.left_axis.create_rectangle(0, 0, LEFT_AXIS_W, ch, fill="#101418", outline="")
        self.right_axis.create_rectangle(0, 0, RIGHT_AXIS_W, ch, fill="#101418", outline="")
        self.chart_canvas.create_text(cw / 2, ch / 2, text="No columns yet", fill="white", font=("Segoe UI", 16, "bold"))
        self.chart_canvas.configure(scrollregion=(0, 0, cw, ch))
        self.left_axis.configure(scrollregion=(0, 0, LEFT_AXIS_W, ch))
        self.right_axis.configure(scrollregion=(0, 0, RIGHT_AXIS_W, ch))

    def _focus_active_area(self, symbol: str):
        if not self.chart_surface:
            return
        cols = self.chart_surface["cols"]
        if not cols:
            return
        plot_w = self.chart_surface["plot_w"]
        plot_h = self.chart_surface["plot_h"]
        display_bottom = self.chart_surface["display_bottom"]
        profile = self.chart_surface["profile"]
        col_offset = self.chart_surface["col_offset"]
        row_offset = self.chart_surface["row_offset"]
        canvas_w = max(self.chart_canvas.winfo_width(), 200)
        canvas_h = max(self.chart_canvas.winfo_height(), 200)
        last_col = cols[-1]
        last_x_center = (last_col.idx + col_offset) * BOX_W + BOX_W / 2
        current_price = self.engines[symbol].last_price or last_col.top
        local_row_idx = int(round((current_price - display_bottom) / profile.box_size))
        global_row_idx = local_row_idx + row_offset
        target_y_center = plot_h - (global_row_idx + 0.5) * BOX_H
        desired_left = max(0, last_x_center - canvas_w * 0.75)
        desired_top = max(0, target_y_center - canvas_h * 0.5)
        self.chart_canvas.xview_moveto(min(desired_left / max(plot_w, 1), 1.0))
        self.chart_canvas.yview_moveto(min(desired_top / max(plot_h, 1), 1.0))
        self._sync_axes_to_chart_yview()
        self.first_focus_done_for_symbol[symbol] = True

    def _center_latest(self):
        symbol = self.active_symbol
        self.user_panned = False
        self._focus_active_area(symbol)
        self._save_current_view_for_symbol(symbol)

    def _save_current_view_for_symbol(self, symbol=None):
        symbol = symbol or self.active_symbol
        try:
            self.saved_view_by_symbol[symbol] = {"x": self.chart_canvas.xview()[0], "y": self.chart_canvas.yview()[0]}
        except Exception:
            pass

    def _restore_saved_view_for_symbol(self, symbol):
        view = self.saved_view_by_symbol.get(symbol)
        if not view:
            return
        try:
            self.chart_canvas.xview_moveto(view["x"])
            self.chart_canvas.yview_moveto(view["y"])
            self._sync_axes_to_chart_yview()
        except Exception:
            pass

    def _rebuild_selected(self):
        symbol = self.active_symbol
        profile = self._get_profile(symbol)

        def worker():
            try:
                candles = self._load_all_closed_candles(symbol)
                engine = PnFEngine(profile)
                last_processed = None
                for candle in candles:
                    last_processed = int(candle["close_time"])
                    engine.update_from_price(last_processed, candle["close"])
                snapshot = self._build_snapshot(symbol, engine)
                self._save_engine_snapshot(symbol, engine, last_processed, snapshot)

                def apply_rebuild():
                    self.engines[symbol] = engine
                    self.last_processed_close_ts_by_symbol[symbol] = last_processed
                    self.latest_scanner[symbol] = snapshot
                    self._refresh_tree()
                    self._draw_selected(symbol)
                    self._focus_active_area(symbol)
                    self._save_current_view_for_symbol(symbol)
                    self._log(f"{symbol} rebuilt from full history.")

                self.after(0, apply_rebuild)
            except Exception as exc:
                self.after(0, lambda: self._log(f"{symbol} rebuild failed: {exc}"))

        if self._refresh_running:
            self._log("Refresh already running.")
            return
        threading.Thread(target=worker, daemon=True).start()

    def _export_selected(self):
        symbol = self.active_symbol
        engine = self.engines.get(symbol)
        if not engine or not engine.columns:
            messagebox.showwarning("No data", "No columns available for export.")
            return
        profile = self._get_profile(symbol)
        filename = f"{self._safe_filename_part(symbol)}_{self._safe_filename_part(profile.name)}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv"
        with open(filename, "w", encoding="utf-8") as f:
            f.write("idx,kind,top,bottom,start_ts,end_ts\n")
            for c in engine.columns:
                f.write(f"{c.idx},{c.kind},{c.top},{c.bottom},{c.start_ts},{c.end_ts}\n")
        messagebox.showinfo("Export complete", f"Exported to:\n{filename}")

    def _copy_log(self, event=None):
        try:
            text = self.log_text.get("1.0", "end-1c")
            self.clipboard_clear()
            self.clipboard_append(text)
            self.update_idletasks()
        except Exception as e:
            self._log(f"Copy failed: {e}")
        return "break"

    def _log(self, message):
        self.log_text.configure(state="normal")
        self.log_text.insert("end", f"[{datetime.utcnow().strftime('%H:%M:%S')}] {message}\n")
        self.log_text.see("end")
        self.log_text.configure(state="disabled")


if __name__ == "__main__":
    app = App()
    app.mainloop()

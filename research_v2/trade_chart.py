"""Read-only candle/PnF chart and evidence table for completed research trades."""
from __future__ import annotations

import bisect
import csv
import json
import math
import tkinter as tk
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from tkinter import ttk

from research_v2.backtest_workspace import (
    BTC_2024_CANDLES_SHA, BTC_2024_COLUMNS_SHA, _sha, load_job,
)


@dataclass(frozen=True)
class Candle:
    close_ms: int
    open: float
    high: float
    low: float
    close: float


@dataclass(frozen=True)
class Trade:
    trade_id: str
    opportunity_id: str
    entry_ms: int
    exit_ms: int
    classification: str
    result_r: float
    entry: float
    stop: float
    target: float


@dataclass(frozen=True)
class PnfColumn:
    idx: int
    kind: str
    top: float
    bottom: float
    start_ms: int
    end_ms: int


def completed_run_sources(result_root: Path) -> tuple[Path, Path]:
    """Resolve both frozen chart inputs from a completed pinned research job."""
    result_root = result_root.resolve(strict=True)
    job_file = result_root.with_suffix(".job.json")
    job = load_job(job_file)
    if Path(job["output_root"]).resolve() != result_root:
        raise ValueError("chart job/output mismatch")
    with (result_root / "research_job_manifest.json").open(encoding="utf-8") as stream:
        manifest = json.load(stream)
    with (result_root / "causal_manifest.json").open(encoding="utf-8") as stream:
        causal = json.load(stream)
    if (manifest.get("job_sha256") != _sha(job_file)
            or job["columns_sha256"] != BTC_2024_COLUMNS_SHA
            or job["candles_sha256"] != BTC_2024_CANDLES_SHA
            or causal.get("source_columns_sha256") != BTC_2024_COLUMNS_SHA
            or causal.get("source_candles_sha256") != BTC_2024_CANDLES_SHA
            or causal.get("causal_decisions_sha256") != _sha(result_root / "causal_decisions.csv")):
        raise ValueError("chart provenance mismatch")
    columns, candles = Path(job["columns_csv"]), Path(job["candles_csv"])
    if _sha(columns) != BTC_2024_COLUMNS_SHA or _sha(candles) != BTC_2024_CANDLES_SHA:
        raise ValueError("chart frozen input changed")
    return columns, candles


def completed_run_inputs(result_root: Path) -> Path:
    """Retain the existing candle-only interface for earlier callers."""
    return completed_run_sources(result_root)[1]


def _rows(path: Path, required: set[str]) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as stream:
        reader = csv.DictReader(stream)
        if (reader.fieldnames is None or len(reader.fieldnames) != len(set(reader.fieldnames))
                or not required.issubset(reader.fieldnames)):
            raise ValueError("chart CSV schema mismatch")
        result = list(reader)
    if any(None in row for row in result):
        raise ValueError("malformed chart CSV")
    return result


def _number(raw: str) -> float:
    if not raw or len(raw) > 60:
        raise ValueError("invalid chart number")
    value = float(raw)
    if not math.isfinite(value):
        raise ValueError("invalid chart number")
    return value


def load_trades(result_root: Path) -> list[Trade]:
    if not (result_root / "research_job_manifest.json").is_file():
        raise ValueError("completed research job manifest missing")
    decisions = _rows(result_root / "causal_decisions.csv", {
        "decision_id", "direction", "entry_price", "stop_price", "entry_candle_close_ms",
    })
    by_id = {}
    for row in decisions:
        decision_id = row["decision_id"]
        if decision_id in by_id or row["direction"] != "LONG" or not decision_id.startswith("DEC-"):
            raise ValueError("ambiguous decision")
        by_id[decision_id] = row
    records = _rows(result_root / "portfolio" / "portfolio_reality_trade_sequence.csv", {
        "trade_id", "opportunity_id", "direction", "entry_timestamp", "exit_timestamp",
        "classification", "result_R",
    })
    trades = []
    used = set()
    for row in records:
        identity = (row["trade_id"], row["opportunity_id"])
        if (identity in used or row["direction"] != "LONG"
                or not row["opportunity_id"].startswith("OPP-")):
            raise ValueError("ambiguous trade")
        used.add(identity)
        source = by_id.get("DEC-" + row["opportunity_id"][4:])
        if source is None:
            raise ValueError("trade decision missing")
        entry_ms = int(row["entry_timestamp"])
        exit_ms = int(row["exit_timestamp"])
        if (entry_ms < int(source["entry_candle_close_ms"]) or exit_ms < entry_ms
                or entry_ms < 10**12 or exit_ms < 10**12):
            raise ValueError("trade chronology mismatch")
        entry, stop = _number(source["entry_price"]), _number(source["stop_price"])
        if entry <= stop or stop <= 0:
            raise ValueError("invalid chart geometry")
        trades.append(Trade(row["trade_id"], row["opportunity_id"], entry_ms,
                            exit_ms, row["classification"], _number(row["result_R"]),
                            entry, stop, entry + 2.5 * (entry - stop)))
    if not trades:
        raise ValueError("no resolved trades")
    return trades


def load_candles(path: Path) -> tuple[list[int], list[Candle]]:
    candles = []
    with path.open(newline="", encoding="utf-8-sig") as stream:
        reader = csv.DictReader(stream)
        if (reader.fieldnames is None or len(reader.fieldnames) != len(set(reader.fieldnames))
                or not {"close_time", "open", "high", "low", "close"}.issubset(reader.fieldnames)):
            raise ValueError("chart candle schema mismatch")
        for row in reader:
            if None in row:
                raise ValueError("malformed chart candle")
            candle = Candle(int(row["close_time"]), *(_number(row[key]) for key in
                        ("open", "high", "low", "close")))
            if (candle.low <= 0 or candle.low > min(candle.open, candle.close)
                    or candle.high < max(candle.open, candle.close)
                    or candles and candle.close_ms - candles[-1].close_ms != 60_000):
                raise ValueError("invalid chart candle")
            candles.append(candle)
    if not candles:
        raise ValueError("empty chart candles")
    return [c.close_ms for c in candles], candles


def load_pnf_columns(path: Path) -> tuple[list[int], list[PnfColumn], float]:
    """Read the frozen PnF columns, without reconstructing them from future candles."""
    from research_v2.patterns.pole_outcomes import extract_box_size_from_profile_name

    records = _rows(path, {"symbol", "profile_name", "idx", "kind", "top",
                           "bottom", "start_ts", "end_ts"})
    if not records:
        raise ValueError("empty PnF columns")
    profile = records[0]["profile_name"]
    box = extract_box_size_from_profile_name(profile)
    if box is None or not math.isfinite(box) or box <= 0:
        raise ValueError("invalid PnF box size")
    columns = []
    for row in records:
        if row["profile_name"] != profile or row["symbol"] != "BTC":
            raise ValueError("mixed PnF profile or symbol")
        index, start, end = (int(row[key]) for key in ("idx", "start_ts", "end_ts"))
        top, bottom = _number(row["top"]), _number(row["bottom"])
        if (index != len(columns) or row["kind"] not in {"X", "O"}
                or start < 10**12 or end < start or bottom <= 0 or top < bottom
                or columns and (start < columns[-1].start_ms
                                or row["kind"] == columns[-1].kind)):
            raise ValueError("invalid PnF column geometry or chronology")
        columns.append(PnfColumn(index, row["kind"], top, bottom, start, end))
    return [column.start_ms for column in columns], columns, box


def pnf_window(starts: list[int], columns: list[PnfColumn], trade: Trade,
               focus: str, radius: int = 28) -> tuple[list[PnfColumn], int, int]:
    """Map events by column start. Final column boxes are retrospective."""
    if focus not in {"entry", "exit"} or not starts or len(starts) != len(columns):
        raise ValueError("invalid PnF window")
    positions = [bisect.bisect_right(starts, ts) - 1
                 for ts in (trade.entry_ms, trade.exit_ms)]
    if any(index < 0 for index in positions):
        raise ValueError("trade precedes frozen PnF columns")
    center = positions[0 if focus == "entry" else 1]
    first, last = max(0, center - radius), min(len(columns), center + radius + 1)
    return columns[first:last], positions[0] - first, positions[1] - first


def chart_window(times: list[int], candles: list[Candle], trade: Trade,
                 focus: str, radius: int = 60) -> tuple[list[Candle], int]:
    if focus not in {"entry", "exit"}:
        raise ValueError("unknown chart focus")
    timestamp = trade.entry_ms if focus == "entry" else trade.exit_ms
    index = bisect.bisect_left(times, timestamp)
    if index >= len(times) or times[index] != timestamp:
        raise ValueError("trade event outside candle data")
    first = max(0, index - radius)
    last = min(len(candles), index + radius + 1)
    return candles[first:last], index - first


def _utc(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def adjacent_trade_id(ids: list[str], selected: str, step: int) -> str:
    """Navigate the ledger order without wrapping past its first or last row."""
    if step not in (-1, 1) or not ids or len(ids) != len(set(ids)) or selected not in ids:
        raise ValueError("invalid trade navigation")
    return ids[min(max(ids.index(selected) + step, 0), len(ids) - 1)]


def exit_timeline(trade: Trade, candles: list[Candle], trigger_r: float
                  ) -> list[tuple[str, str, str]]:
    """Check the simulator's exit chronology on the bounded fill-to-exit slice."""
    from types import SimpleNamespace
    from research_v2.patterns.pole_be_research_audit import _be_classify

    if (not candles or candles[0].close_ms != trade.entry_ms
            or candles[-1].close_ms != trade.exit_ms
            or any(b.close_ms - a.close_ms != 60_000
                   for a, b in zip(candles, candles[1:]))):
        raise ValueError("trade exit chronology mismatch")
    fill = candles[0]
    if fill.low >= trade.entry:
        raise ValueError("trade fill candle mismatch")
    trigger = trade.entry + trigger_r * (trade.entry - trade.stop)
    if trade.classification == "SAME_CANDLE_FILL_STOP_CONSERVATIVE":
        if (len(candles) != 1 or fill.low > trade.stop
                or not math.isclose(trade.result_r, -1.0)):
            raise ValueError("trade fill-candle outcome mismatch")
        return [("Fill + stop ίδιο κερί", _utc(fill.close_ms),
                 f"low ≤ {trade.stop:g}; συντηρητικά"),
                ("Κερί εξόδου H/L", f"{fill.high:g} / {fill.low:g}", "1m OHLC")]
    if fill.low <= trade.stop or fill.high >= trade.target:
        raise ValueError("trade fill-candle outcome mismatch")
    rep = SimpleNamespace(geometry_status="OBSERVABLE", direction="LONG",
                          entry=trade.entry, stop=trade.stop,
                          observable_entry_ts=trade.entry_ms, replay_includes_anchor=False)
    replay = [SimpleNamespace(ts=c.close_ms, high=c.high, low=c.low)
              for c in candles]
    classification, result_r, exit_ms, _ = _be_classify(rep, replay, trigger_r)
    if classification == "SAME_CANDLE_AMBIGUOUS":
        raise ValueError("trade OHLC exit ambiguity")
    if (classification != trade.classification or exit_ms != trade.exit_ms
            or result_r is None or not math.isclose(result_r, trade.result_r,
                                                     abs_tol=1e-9)):
        raise ValueError("trade exit outcome mismatch")
    armed_at = None
    for candle in candles[1:]:
        if candle.close_ms == trade.exit_ms:
            break
        if armed_at is None and candle.high >= trigger:
            armed_at = candle.close_ms
    if trade.classification == "BREAK_EVEN_EXIT" and armed_at is None:
        raise ValueError("trade BE chronology mismatch")
    timeline = [("BE επίπεδο", f"{trigger:g}", f"entry + {trigger_r:g} R")]
    timeline.append(("BE ενεργό", _utc(armed_at) if armed_at is not None else "όχι",
                     f"high ≥ {trigger:g}" if armed_at is not None else "πριν από έξοδο"))
    label, rule = {
        "BREAK_EVEN_EXIT": ("BE έξοδος", f"low ≤ {trade.entry:g}"),
        "TARGET_FIRST": ("Target έξοδος", f"high ≥ {trade.target:g}"),
        "STOP_FIRST": ("Stop έξοδος", f"low ≤ {trade.stop:g}"),
    }[trade.classification]
    timeline.append((label, _utc(trade.exit_ms), rule))
    timeline.append(("Κερί εξόδου H/L",
                     f"{candles[-1].high:g} / {candles[-1].low:g}", "1m OHLC"))
    return timeline


def build_trade_explanations(result_root: Path, trades: list[Trade],
                             columns: list[PnfColumn], box: float,
                             times: list[int], candles: list[Candle]
                             ) -> dict[str, list[tuple[str, str, str]]]:
    """Explain existing evidence; reject disagreement instead of recreating a signal."""
    from pnf_mvp.patterns.poles import _box_count
    from research_v2.patterns.pole_portfolio_reality_audit import (
        BREAK_EVEN_TRIGGER_R, LIMIT_EXPIRY_CANDLES, TARGET_R,
    )

    if not columns or not math.isfinite(box) or box <= 0:
        raise ValueError("missing PnF evidence")
    source = _rows(result_root / "causal_decisions.csv", {
        "decision_id", "pole_column_index", "reversal_column_index",
        "confirmation_column_index", "decision_known_at_ms",
        "entry_candle_open_ms", "entry_candle_close_ms", "direction",
        "entry_price", "stop_price",
    })
    by_id = {}
    for row in source:
        if row["decision_id"] in by_id:
            raise ValueError("duplicate decision evidence")
        by_id[row["decision_id"]] = row
    explanations = {}
    for trade in trades:
        decision = by_id.get("DEC-" + trade.opportunity_id[4:])
        if decision is None or decision["direction"] != "LONG":
            raise ValueError("trade decision identity mismatch")
        pole_idx, reversal_idx, confirmation_idx = (int(decision[key]) for key in (
            "pole_column_index", "reversal_column_index", "confirmation_column_index"))
        if (pole_idx < 1 or confirmation_idx != reversal_idx + 1
                or reversal_idx != pole_idx + 1 or confirmation_idx >= len(columns)):
            raise ValueError("trade decision column mismatch")
        pole, reversal, confirmation = columns[pole_idx:confirmation_idx + 1]
        previous = next((col for col in reversed(columns[:pole_idx])
                         if col.kind == "O"), None)
        if previous is None or (pole.kind, reversal.kind, confirmation.kind) != ("O", "X", "O"):
            raise ValueError("trade PnF motif mismatch")
        pole_boxes = _box_count(pole.top, pole.bottom, box)
        retrace_boxes = _box_count(reversal.top, reversal.bottom, box)
        breakout = _box_count(previous.bottom, pole.bottom, box) - 1
        retrace_ratio = retrace_boxes / pole_boxes
        if pole_boxes <= 5 or breakout < 3 or retrace_ratio <= 0.5:
            raise ValueError("trade motif thresholds mismatch")
        known = int(decision["decision_known_at_ms"])
        eligible = int(decision["entry_candle_close_ms"])
        candle_idx = bisect.bisect_left(times, eligible)
        if (known != confirmation.start_ms or candle_idx == len(times)
                or times[candle_idx] != eligible
                or int(decision["entry_candle_open_ms"]) != eligible - 59_999
                or known >= eligible - 59_999):
            raise ValueError("trade causal chronology mismatch")
        entry, stop = _number(decision["entry_price"]), _number(decision["stop_price"])
        if (not math.isclose(entry, candles[candle_idx].open, abs_tol=1e-7)
                or not math.isclose(entry, trade.entry, abs_tol=1e-7)
                or not math.isclose(stop, trade.stop, abs_tol=1e-7)
                or not math.isclose(stop, entry - 3 * box, abs_tol=1e-7)
                or not math.isclose(trade.target, entry + TARGET_R * (entry-stop), abs_tol=1e-7)):
            raise ValueError("trade entry geometry mismatch")
        fill_idx = bisect.bisect_left(times, trade.entry_ms)
        elapsed = fill_idx - candle_idx
        if (fill_idx >= len(times) or times[fill_idx] != trade.entry_ms
                or elapsed < 0 or elapsed >= LIMIT_EXPIRY_CANDLES
                or not candles[fill_idx].low < entry):
            raise ValueError("trade fill evidence mismatch")
        exit_idx = bisect.bisect_left(times, trade.exit_ms)
        if exit_idx >= len(times) or times[exit_idx] != trade.exit_ms:
            raise ValueError("trade exit candle unavailable")
        timeline = exit_timeline(trade, candles[fill_idx:exit_idx + 1],
                                 BREAK_EVEN_TRIGGER_R)
        explanations[trade.trade_id] = [
            ("Στρατηγική", "LOW_POLE LONG", "γνωστό στο σήμα"),
            ("Μοτίβο PnF", "O → X → O", "γνωστό στο σήμα"),
            ("Στήλες", f"{pole_idx} → {reversal_idx} → {confirmation_idx}", "γνωστό στο σήμα"),
            ("Pole", f"{pole_boxes} boxes", "> 5"),
            ("Breakout excess", f"{breakout} boxes", "≥ 3"),
            ("Retrace / pole", f"{retrace_boxes}/{pole_boxes} = {retrace_ratio:.2f}", "> 0.50"),
            ("Σήμα γνωστό", _utc(known), "start confirmation"),
            ("Επιλέξιμο κερί", _utc(eligible), "μετά το σήμα"),
            ("Limit entry", f"{entry:g}", "open επιλέξιμου κεριού"),
            ("Stop", f"{stop:g}", "entry − 3 boxes"),
            ("Target", f"{trade.target:g}", f"{TARGET_R:g} R"),
            ("Εκτέλεση limit", f"κερί {elapsed+1}/{LIMIT_EXPIRY_CANDLES}", "low < entry (OHLC)"),
            ("Χρόνος fill", _utc(trade.entry_ms), "προσομοίωση"),
            ("BE trigger", f"{BREAK_EVEN_TRIGGER_R:g} R", "πολιτική simulator"),
            *timeline,
            ("Έξοδος", f"{trade.classification}, {trade.result_r:+g} R", "μεταγενέστερο αποτέλεσμα"),
            ("Χρόνος εξόδου", _utc(trade.exit_ms), "τιμή μη διαθέσιμη"),
            ("Κόστη", "δεν περιλαμβάνονται", "gross R"),
        ]
    return explanations


class TradeChartWindow(tk.Toplevel):
    def __init__(self, master: tk.Misc, result_root: Path, candles_path: Path,
                 columns_path: Path | None = None):
        super().__init__(master)
        self.title("BTC 2024 — research trades")
        self.geometry("1560x720")
        self.trades = load_trades(result_root)
        self.times, self.candles = load_candles(candles_path)
        if columns_path is None:
            columns_path = completed_run_sources(result_root)[0]
        self.pnf_starts, self.pnf_columns, self.box_size = load_pnf_columns(columns_path)
        self.explanations = build_trade_explanations(result_root, self.trades,
            self.pnf_columns, self.box_size, self.times, self.candles)
        self.trade_ids = [trade.trade_id for trade in self.trades]
        self.selected = tk.StringVar(value=self.trades[0].trade_id)
        self.focus = tk.StringVar(value="entry")
        self.view = tk.StringVar(value="candles")
        top = ttk.Frame(self)
        top.pack(fill="x", padx=10, pady=8)
        ttk.Label(top, text="Trade:").pack(side="left")
        picker = ttk.Combobox(top, state="readonly", textvariable=self.selected,
                              values=self.trade_ids, width=18)
        picker.pack(side="left", padx=8)
        picker.bind("<<ComboboxSelected>>", lambda _: self.draw())
        self.prev_button = ttk.Button(top, text="◀ Προηγούμενο",
                                      command=lambda: self.move_trade(-1))
        self.prev_button.pack(side="left", padx=2)
        self.next_button = ttk.Button(top, text="Επόμενο ▶",
                                      command=lambda: self.move_trade(1))
        self.next_button.pack(side="left", padx=2)
        self.position = ttk.Label(top, text="")
        self.position.pack(side="left", padx=5)
        ttk.Radiobutton(top, text="Κεριά", variable=self.view,
                        value="candles", command=self.draw).pack(side="left", padx=8)
        ttk.Radiobutton(top, text="PnF", variable=self.view,
                        value="pnf", command=self.draw).pack(side="left", padx=8)
        ttk.Radiobutton(top, text="Γύρω από entry", variable=self.focus,
                        value="entry", command=self.draw).pack(side="left", padx=8)
        ttk.Radiobutton(top, text="Γύρω από exit", variable=self.focus,
                        value="exit", command=self.draw).pack(side="left", padx=8)
        self.caption = ttk.Label(self, text="")
        self.caption.pack(fill="x", padx=10)
        body = ttk.Frame(self)
        body.pack(fill="both", expand=True, padx=10, pady=8)
        self.canvas = tk.Canvas(body, bg="white", height=510)
        self.canvas.pack(side="left", fill="both", expand=True)
        details = ttk.Frame(body, width=510)
        details.pack(side="right", fill="y", padx=(10, 0))
        details.pack_propagate(False)
        ttk.Label(details, text="Γιατί εκτελέστηκε το trade",
                  font=("TkDefaultFont", 11, "bold")).pack(anchor="w", pady=(0, 8))
        table_frame = ttk.Frame(details)
        table_frame.pack(fill="both", expand=True)
        self.evidence = ttk.Treeview(table_frame, columns=("value", "rule"),
                                     show="tree headings", height=17)
        self.evidence.heading("#0", text="Συνθήκη")
        self.evidence.heading("value", text="Τιμή")
        self.evidence.heading("rule", text="Κριτήριο / πηγή")
        self.evidence.column("#0", width=145, stretch=False)
        self.evidence.column("value", width=205, stretch=False)
        self.evidence.column("rule", width=145, stretch=False)
        scroll = ttk.Scrollbar(table_frame, orient="vertical", command=self.evidence.yview)
        self.evidence.configure(yscrollcommand=scroll.set)
        self.evidence.pack(side="left", fill="both", expand=True)
        scroll.pack(side="right", fill="y")
        ttk.Label(details, text=("Οι κανόνες ήταν γνωστοί στο σήμα. "
                  "Fill/έξοδος προκύπτουν από την προσομοίωση 1m OHLC. "
                  "Η τελική PnF στήλη μπορεί να περιλαμβάνει μεταγενέστερα boxes."),
                  wraplength=490, justify="left").pack(fill="x", pady=(9, 0))
        self.canvas.bind("<Configure>", lambda _: self.draw())
        self.draw()

    def move_trade(self, step: int):
        self.selected.set(adjacent_trade_id(self.trade_ids, self.selected.get(), step))
        self.draw()

    def draw(self):
        trade = next(item for item in self.trades if item.trade_id == self.selected.get())
        index = self.trade_ids.index(trade.trade_id)
        self.position.configure(text=f"{index+1} / {len(self.trade_ids)}")
        self.prev_button.state(["disabled"] if index == 0 else ["!disabled"])
        self.next_button.state(["disabled"] if index == len(self.trade_ids)-1 else ["!disabled"])
        focus = self.focus.get()
        pnf = self.view.get() == "pnf"
        if pnf:
            rows, entry_pos, exit_pos = pnf_window(self.pnf_starts, self.pnf_columns, trade, focus)
        else:
            rows, marker = chart_window(self.times, self.candles, trade, focus)
        note = " | PnF τελικές στήλες: αναδρομική εικόνα, όχι κατάσταση κατά το trade" if pnf else ""
        self.caption.configure(text=(f"{trade.trade_id}: {trade.classification}, {trade.result_r:g} R | "
                                     f"entry {_utc(trade.entry_ms)} | exit {_utc(trade.exit_ms)}{note}"))
        self.evidence.delete(*self.evidence.get_children())
        for name, value, rule in self.explanations[trade.trade_id]:
            self.evidence.insert("", "end", text=name, values=(value, rule))
        canvas = self.canvas
        canvas.delete("all")
        width, height = max(canvas.winfo_width(), 600), max(canvas.winfo_height(), 400)
        left, right, top, bottom = 66, width - 22, 40, height - 58
        values = [value for row in rows for value in
                  ((row.top, row.bottom) if pnf else (row.high, row.low))]
        values += [trade.entry, trade.stop, trade.target]
        minimum, maximum = min(values), max(values)
        margin = max((maximum - minimum) * 0.05, 0.01)
        minimum -= margin
        maximum += margin
        y = lambda value: bottom - (value - minimum) / (maximum - minimum) * (bottom - top)
        step = (right - left) / len(rows)
        for level, color, label in ((trade.target, "#247a46", "target 2.5R"),
                                    (trade.entry, "#2458a6", "entry"),
                                    (trade.stop, "#b34343", "stop")):
            yy = y(level)
            canvas.create_line(left, yy, right, yy, fill=color, dash=(4, 4))
            canvas.create_text(left + 4, yy - 9, text=f"{label} {level:g}", anchor="w", fill=color)
        if pnf:
            for i, col in enumerate(rows):
                x = left + (i + 0.5) * step
                color = "#267a50" if col.kind == "X" else "#be5555"
                # Endpoints are box levels in the frozen fixed-size profile.
                count = int(round((col.top - col.bottom) / self.box_size))
                if count > 1000:
                    raise ValueError("PnF column exceeds chart bounds")
                for n in range(count + 1):
                    yy = y(col.bottom + n * self.box_size)
                    half = min(step * 0.33, 7)
                    if col.kind == "X":
                        canvas.create_line(x-half, yy-4, x+half, yy+4, fill=color)
                        canvas.create_line(x-half, yy+4, x+half, yy-4, fill=color)
                    else:
                        canvas.create_oval(x-half, yy-4, x+half, yy+4, outline=color)
            for pos, color, label in ((entry_pos, "#2458a6", "ENTRY"),
                                      (exit_pos, "#5d35a1", "EXIT (χωρίς τιμή)")):
                if 0 <= pos < len(rows):
                    x = left + (pos + 0.5) * step
                    canvas.create_line(x, top, x, bottom, fill=color, dash=(3, 3), width=2)
                    canvas.create_text(x + 4, top + (8 if label.startswith("ENTRY") else 27),
                                       text=label, anchor="nw", fill=color)
                    if label == "ENTRY":
                        canvas.create_oval(x-5, y(trade.entry)-5, x+5, y(trade.entry)+5,
                                           fill=color, outline="white")
            canvas.create_text(left, bottom + 24, text=_utc(rows[0].start_ms), anchor="w")
            canvas.create_text(right, bottom + 24, text=_utc(rows[-1].start_ms), anchor="e")
        else:
            for i, candle in enumerate(rows):
                x = left + (i + 0.5) * step
                color = "#267a50" if candle.close >= candle.open else "#be5555"
                canvas.create_line(x, y(candle.high), x, y(candle.low), fill=color)
                canvas.create_rectangle(x - max(1, step * 0.32), y(max(candle.open, candle.close)),
                                        x + max(1, step * 0.32), y(min(candle.open, candle.close)),
                                        outline=color, fill=color)
            x = left + (marker + 0.5) * step
            canvas.create_line(x, top, x, bottom, fill="#5d35a1", width=2)
            label = "simulated entry time" if focus == "entry" else "exit time (price unavailable)"
            canvas.create_text(x + 4, top + 8, text=label, anchor="nw", fill="#5d35a1")
            canvas.create_text(left, bottom + 24, text=_utc(rows[0].close_ms), anchor="w")
            canvas.create_text(right, bottom + 24, text=_utc(rows[-1].close_ms), anchor="e")

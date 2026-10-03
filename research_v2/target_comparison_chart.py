"""Read-only Tk chart of gross profit and maximum drawdown by R target."""
from __future__ import annotations

import csv
import json
import tkinter as tk
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from tkinter import messagebox, ttk

from research_v2.backtest_workspace import SWEEP_SCHEMA, _sha, load_job


TARGETS = tuple(Decimal(str(x)) for x in (2.5, 3, 4, 5, 6, 7, 8, 9, 10))
FIELDS = {"target_R", "gross_total_R", "max_drawdown_R"}
EQUITY_FIELDS = {"sequence", "exit_timestamp", "result_R", "cumulative_R", "drawdown_R"}
COLORS = ("#2563b4", "#c45b24", "#16805d", "#8549a4", "#b3860b",
          "#258caa", "#b34270", "#58636e", "#7a6623")


@dataclass(frozen=True)
class TargetResult:
    target: Decimal
    profit: Decimal
    drawdown: Decimal


@dataclass(frozen=True)
class EquityPoint:
    timestamp: int
    cumulative: Decimal


def _integer(value: str) -> int:
    if type(value) is not str or not value.isascii() or not value.isdecimal() or len(value) > 16:
        raise ValueError("invalid equity timestamp or sequence")
    return int(value)


def _decimal(value: str) -> Decimal:
    if type(value) is not str or not value or len(value) > 40:
        raise ValueError("invalid comparison number")
    try:
        result = Decimal(value)
    except InvalidOperation as exc:
        raise ValueError("invalid comparison number") from exc
    if not result.is_finite() or abs(result) > 1_000_000:
        raise ValueError("invalid comparison number")
    return result


def load_comparison(output_root: Path) -> list[TargetResult]:
    """Use only a completed pinned sweep job and its SHA-checked report."""
    root = output_root.resolve(strict=True)
    job_file = root.with_suffix(".job.json")
    job = load_job(job_file)
    if job["schema"] != SWEEP_SCHEMA or Path(job["output_root"]).resolve() != root:
        raise ValueError("comparison job mismatch")
    with (root / "research_job_manifest.json").open(encoding="utf-8") as stream:
        completed = json.load(stream)
    if (completed.get("job_sha256") != _sha(job_file)
            or completed.get("target_sweep_manifest") != "target_sweep/comparison_manifest.json"):
        raise ValueError("comparison provenance mismatch")
    sweep = root / "target_sweep"
    with (sweep / "comparison_manifest.json").open(encoding="utf-8") as stream:
        manifest = json.load(stream)
    report = sweep / "comparison.csv"
    if (manifest.get("schema") != "causal-pole-target-sweep-v1"
            or manifest.get("comparison_sha256") != _sha(report)
            or manifest.get("columns_sha256") != job["columns_sha256"]
            or manifest.get("candles_sha256") != job["candles_sha256"]
            or manifest.get("minimum_entry_ts") != job["minimum_entry_ts"]
            or manifest.get("maximum_entry_ts") != job["maximum_entry_ts"]
            or manifest.get("targets_R") != [float(x) for x in TARGETS]):
        raise ValueError("comparison provenance mismatch")
    with report.open(newline="", encoding="utf-8-sig") as stream:
        reader = csv.DictReader(stream)
        if (reader.fieldnames is None or len(reader.fieldnames) != len(set(reader.fieldnames))
                or not FIELDS.issubset(reader.fieldnames)):
            raise ValueError("comparison CSV schema mismatch")
        rows = list(reader)
    if len(rows) != len(TARGETS) or any(None in row for row in rows):
        raise ValueError("comparison row mismatch")
    results = [TargetResult(_decimal(row["target_R"]), _decimal(row["gross_total_R"]),
                            _decimal(row["max_drawdown_R"])) for row in rows]
    if (tuple(item.target for item in results) != TARGETS
            or any(item.drawdown < 0 for item in results)):
        raise ValueError("comparison target mismatch")
    return results


def load_equity(output_root: Path, results: list[TargetResult]) -> dict[Decimal, list[EquityPoint]]:
    """Check each existing portfolio ledger against the pinned sweep summary."""
    root = output_root.resolve(strict=True)
    # Recheck the completed job and comparison when this view is requested.
    if load_comparison(root) != results:
        raise ValueError("comparison changed")
    with (root / "target_sweep/comparison.csv").open(newline="", encoding="utf-8-sig") as stream:
        summary = list(csv.DictReader(stream))
    series = {}
    for item, record in zip(results, summary, strict=True):
        directory = (root / "portfolio" if item.target == Decimal("2.5") else
                     root / "target_sweep" / f"target_{_fmt(item.target)}R")
        manifest_file = directory / "portfolio_reality_manifest.json"
        if _sha(manifest_file) != record["portfolio_manifest_sha256"]:
            raise ValueError("equity provenance mismatch")
        with manifest_file.open(encoding="utf-8") as stream:
            manifest = json.load(stream)
        if (_decimal(str(manifest.get("target_R"))) != item.target
                or _decimal(str(manifest["summary_metrics"]["total_R"])) != item.profit
                or _decimal(str(manifest["summary_metrics"]["max_drawdown_R"])) != item.drawdown):
            raise ValueError("equity summary mismatch")
        with (directory / "portfolio_reality_equity_curve.csv").open(newline="", encoding="utf-8-sig") as stream:
            reader = csv.DictReader(stream)
            if (reader.fieldnames is None or len(reader.fieldnames) != len(set(reader.fieldnames))
                    or not EQUITY_FIELDS.issubset(reader.fieldnames)):
                raise ValueError("equity CSV schema mismatch")
            rows = list(reader)
        if len(rows) != _integer(record["resolved_trades"]) or len(rows) != manifest["resolved_portfolio_trades"]:
            raise ValueError("equity row mismatch")
        points = []
        cumulative = Decimal(0)
        peak = Decimal(0)
        maximum_drawdown = Decimal(0)
        for number, row in enumerate(rows, 1):
            timestamp = _integer(row["exit_timestamp"])
            if (_integer(row["sequence"]) != number or timestamp < 10**12
                    or (points and timestamp < points[-1].timestamp)):
                raise ValueError("equity chronology mismatch")
            cumulative += _decimal(row["result_R"])
            reported = _decimal(row["cumulative_R"])
            # The existing simulator serializes each cumulative sum to six decimals.
            if abs(cumulative - reported) > Decimal("0.00001"):
                raise ValueError("equity cumulative mismatch")
            cumulative = reported
            peak = max(peak, reported)
            drawdown = peak - reported
            if abs(drawdown - _decimal(row["drawdown_R"])) > Decimal("0.00001"):
                raise ValueError("equity drawdown mismatch")
            maximum_drawdown = max(maximum_drawdown, drawdown)
            points.append(EquityPoint(timestamp, reported))
        if (abs((points[-1].cumulative if points else Decimal(0)) - item.profit) > Decimal("0.00001")
                or abs(maximum_drawdown - item.drawdown) > Decimal("0.00001")):
            raise ValueError("equity total mismatch")
        series[item.target] = points
    return series


def bank_lines(series: dict[Decimal, list[EquityPoint]], selected: Decimal | None,
               initial: Decimal, risk: Decimal) -> dict[Decimal, list[tuple[int, Decimal]]]:
    if (not initial.is_finite() or not risk.is_finite() or initial <= 0 or risk <= 0
            or initial > 1_000_000_000 or risk > initial):
        raise ValueError("positive initial capital and risk up to initial capital required")
    if selected is not None and selected not in series:
        raise ValueError("unknown target")
    chosen = series if selected is None else {selected: series[selected]}
    return {target: [(point.timestamp, initial + risk * point.cumulative) for point in points]
            for target, points in chosen.items()}


def draw_bank(canvas: tk.Canvas, lines: dict[Decimal, list[tuple[int, Decimal]]],
              initial: Decimal, colors: dict[Decimal, str]) -> None:
    """Shared UTC and currency axes; each target keeps its own exit timestamps."""
    canvas.delete("all")
    width, height = 1050, 490
    left, right, top, bottom = 95, width - 30, 35, height - 75
    values = [initial, *(value for points in lines.values() for _, value in points)]
    low, high = min(values), max(values)
    if low == high:
        low -= 1
        high += 1
    times = [timestamp for points in lines.values() for timestamp, _ in points]
    start, end = (min(times), max(times)) if times else (0, 1)
    if start == end:
        end += 1
    def xy(timestamp, value):
        return (left + (timestamp - start) / (end - start) * (right - left),
                bottom - float((value - low) / (high - low)) * (bottom - top))
    canvas.create_line(left, top, left, bottom, right, bottom, fill="#48556a")
    for index in range(5):
        level = low + (high - low) * Decimal(index) / 4
        y = xy(start, level)[1]
        canvas.create_line(left, y, right, y, fill="#e1e5eb")
        canvas.create_text(left - 9, y, text=f"{level:,.2f}", anchor="e", fill="#293345")
    from datetime import datetime, timezone
    for timestamp, x in ((start, left), (end, right)):
        canvas.create_text(x, bottom + 18,
                           text=datetime.fromtimestamp(timestamp / 1000, timezone.utc).strftime("%Y-%m-%d UTC"),
                           anchor="w" if x == left else "e", fill="#293345")
    for target, points in lines.items():
        if not points:
            continue
        coords = [xy(points[0][0], initial)]
        coords.extend(xy(timestamp, value) for timestamp, value in points)
        flat = [coordinate for pair in coords for coordinate in pair]
        if len(coords) > 1:
            canvas.create_line(*flat, fill=colors[target], width=2)
        canvas.create_oval(coords[-1][0]-3, coords[-1][1]-3,
                           coords[-1][0]+3, coords[-1][1]+3, fill=colors[target], outline="")
    for index, target in enumerate(lines):
        x = left + index * 104
        canvas.create_line(x, height-16, x+20, height-16, fill=colors[target], width=3)
        canvas.create_text(x+25, height-16, anchor="w", text=f"{_fmt(target)}R", fill="#293345")


def _fmt(value: Decimal) -> str:
    return format(value.normalize(), "f")


def draw_comparison(canvas: tk.Canvas, results: list[TargetResult]) -> None:
    """Use a single R scale for both series; drawdown extends below zero."""
    baseline = 235
    scale = 185 / max(1.0, *(float(abs(item.profit)) for item in results),
                      *(float(item.drawdown) for item in results))
    canvas.create_line(40, baseline, 925, baseline, fill="#48556a", width=2)
    canvas.create_text(25, baseline, text="0", fill="#48556a")
    for index, item in enumerate(results):
        center = 95 + index * 95
        profit_y = baseline - float(item.profit) * scale
        drawdown_y = baseline + float(item.drawdown) * scale
        canvas.create_rectangle(center - 27, min(baseline, profit_y), center - 5,
                                max(baseline, profit_y), fill="#2563b4", outline="")
        canvas.create_rectangle(center + 5, baseline, center + 27, drawdown_y,
                                fill="#cb4242", outline="")
        canvas.create_text(center - 16, profit_y - 12 if item.profit >= 0 else profit_y + 12,
                           text=_fmt(item.profit), fill="#174b91")
        canvas.create_text(center + 16, drawdown_y + 12,
                           text=_fmt(item.drawdown), fill="#982727")
        canvas.create_text(center, 463, text=f"{_fmt(item.target)}R", fill="#293345")


class TargetComparisonWindow(tk.Toplevel):
    def __init__(self, parent: tk.Misc, output_root: Path):
        results = load_comparison(output_root)
        super().__init__(parent)
        self.output_root = output_root
        self.results = results
        self.series = None
        self.title("Σύγκριση στόχων — Profit / Drawdown")
        self.geometry("1120x680")
        controls = ttk.Frame(self)
        controls.pack(pady=8)
        ttk.Button(controls, text="Profit / Drawdown", command=self.show_comparison).pack(side="left", padx=5)
        ttk.Button(controls, text="Εξέλιξη μπάνκας", command=self.show_bank).pack(side="left", padx=5)
        self.selection = tk.StringVar(value="Όλα")
        ttk.Label(controls, text="Στόχος:").pack(side="left", padx=(15, 3))
        self.selector = ttk.Combobox(controls, textvariable=self.selection, state="readonly", width=9,
                                     values=["Όλα", *(f"{_fmt(item.target)}R" for item in results)])
        self.selector.pack(side="left")
        self.selector.bind("<<ComboboxSelected>>", lambda _event: self.show_bank())
        self.initial = tk.StringVar(value="1000")
        self.risk = tk.StringVar(value="10")
        ttk.Label(controls, text="Αρχική μπάνκα USDT:").pack(side="left", padx=(15, 3))
        ttk.Entry(controls, textvariable=self.initial, width=10).pack(side="left")
        ttk.Label(controls, text="Ρίσκο / trade USDT:").pack(side="left", padx=(12, 3))
        ttk.Entry(controls, textvariable=self.risk, width=9).pack(side="left")
        self.caption = ttk.Label(self, font=("TkDefaultFont", 12, "bold"))
        self.caption.pack(pady=9)
        self.canvas = tk.Canvas(self, width=1050, height=490, bg="white", highlightthickness=0)
        self.canvas.pack(padx=15, pady=8)
        ttk.Label(self, text="Υποθετικό σταθερό ρίσκο ανά trade, χωρίς ανατοκισμό. Gross 1m OHLC: χωρίς fees, slippage, funding ή επιβεβαιωμένα fills.",
                  wraplength=1000).pack(pady=5)
        self.show_comparison()

    def show_comparison(self):
        self.caption.config(text="BTCUSDT 2024 — gross profit και μέγιστο drawdown ανά στόχο (R)")
        self.canvas.delete("all")
        draw_comparison(self.canvas, self.results)

    def show_bank(self):
        try:
            initial = _decimal(self.initial.get())
            risk = _decimal(self.risk.get())
            if self.series is None:
                self.series = load_equity(self.output_root, self.results)
            selected = (None if self.selection.get() == "Όλα" else
                        _decimal(self.selection.get().removesuffix("R")))
            lines = bank_lines(self.series, selected, initial, risk)
        except (OSError, ValueError, KeyError, TypeError) as exc:
            messagebox.showerror("Εξέλιξη μπάνκας", str(exc), parent=self)
            return
        self.caption.config(text="Υποθετική μπάνκα ανά έξοδο trade — UTC / USDT")
        draw_bank(self.canvas, lines, initial,
                  {item.target: COLORS[index] for index, item in enumerate(self.results)})

"""Read-only Tk chart of gross profit and maximum drawdown by R target."""
from __future__ import annotations

import csv
import json
import tkinter as tk
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from tkinter import ttk

from research_v2.backtest_workspace import SWEEP_SCHEMA, _sha, load_job


TARGETS = tuple(Decimal(str(x)) for x in (2.5, 3, 4, 5, 6, 7, 8, 9, 10))
FIELDS = {"target_R", "gross_total_R", "max_drawdown_R"}


@dataclass(frozen=True)
class TargetResult:
    target: Decimal
    profit: Decimal
    drawdown: Decimal


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
        self.title("Σύγκριση στόχων — Profit / Drawdown")
        self.geometry("1000x600")
        ttk.Label(self, text="BTCUSDT 2024 — profit και μέγιστο drawdown ανά στόχο (R)",
                  font=("TkDefaultFont", 12, "bold")).pack(pady=12)
        ttk.Label(self, text="Μπλε: gross profit    Κόκκινο: μέγιστο drawdown (μέγεθος σε R)").pack()
        canvas = tk.Canvas(self, width=950, height=480, bg="white", highlightthickness=0)
        canvas.pack(padx=15, pady=12)
        draw_comparison(canvas, results)
        ttk.Label(self, text="Gross 1m OHLC έρευνα: χωρίς fees, slippage, funding ή επιβεβαιωμένα fills.").pack()

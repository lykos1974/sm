"""
strategy_snapshot_support.py

Create true PnF trade snapshots for future trades.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Optional

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


@dataclass
class SnapshotColumn:
    idx: int
    kind: str
    top: float
    bottom: float
    start_ts: int = 0
    end_ts: int = 0


def _coerce_columns(columns: Iterable[Any]) -> list[SnapshotColumn]:
    out: list[SnapshotColumn] = []
    for c in columns:
        if isinstance(c, SnapshotColumn):
            out.append(c)
            continue
        getter = c.get if isinstance(c, dict) else getattr
        out.append(
            SnapshotColumn(
                idx=int(getter(c, "idx")),
                kind=str(getter(c, "kind")),
                top=float(getter(c, "top")),
                bottom=float(getter(c, "bottom")),
                start_ts=int(getter(c, "start_ts", 0)),
                end_ts=int(getter(c, "end_ts", 0)),
            )
        )
    return out


def render_trade_snapshot_png(
    *,
    symbol: str,
    side: str,
    setup_id: str,
    columns: Iterable[Any],
    box_size: float,
    entry: Optional[float],
    sl: Optional[float],
    tp1: Optional[float],
    tp2: Optional[float],
    support_level: Optional[float] = None,
    resistance_level: Optional[float] = None,
    active_column_index: Optional[int] = None,
    title_note: str = "",
    output_path: str | Path,
) -> str:
    cols = _coerce_columns(columns)
    if not cols:
        raise ValueError("No columns supplied for snapshot rendering.")
    if not box_size or box_size <= 0:
        raise ValueError("box_size must be > 0")

    out_path = Path(output_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    lo = min(c.bottom for c in cols)
    hi = max(c.top for c in cols)
    extra_prices = [p for p in [entry, sl, tp1, tp2, support_level, resistance_level] if p is not None]
    if extra_prices:
        lo = min(lo, min(extra_prices))
        hi = max(hi, max(extra_prices))
    lo -= 2 * box_size
    hi += 2 * box_size

    ncols = len(cols)
    fig_w = max(11, min(24, 4 + ncols * 0.22))
    fig_h = 8
    fig, ax = plt.subplots(figsize=(fig_w, fig_h), dpi=140)
    ax.set_facecolor("#101418")
    fig.patch.set_facecolor("#101418")

    for i, c in enumerate(cols):
        x = i + 1
        steps = int(round((c.top - c.bottom) / box_size))
        if steps < 0:
            continue
        yvals = [c.bottom + j * box_size for j in range(steps + 1)]
        marker_text = "X" if c.kind.upper() == "X" else "O"
        color = "#57d657" if marker_text == "X" else "#ff5a5a"
        for y in yvals:
            ax.text(x, y, marker_text, ha="center", va="center", fontsize=10, color=color, family="monospace")
        if active_column_index is not None and c.idx == active_column_index:
            ax.axvspan(x - 0.45, x + 0.45, alpha=0.16, color="#ffd24d")

    def hline(price: Optional[float], label: str, line_style: str = "-", alpha: float = 0.95):
        if price is None:
            return
        ax.axhline(price, linestyle=line_style, linewidth=1.4, alpha=alpha)
        ax.text(ncols + 1.25, price, f"{label} {price:.3f}", va="center", fontsize=9, color="white")

    hline(entry, "ENTRY")
    hline(sl, "SL")
    hline(tp1, "TP1")
    hline(tp2, "TP2")
    hline(support_level, "SUPPORT", "--", 0.7)
    hline(resistance_level, "RESIST", "--", 0.7)

    title = f"{symbol} | {side} | {setup_id}"
    if title_note:
        title += f"\n{title_note}"
    ax.set_title(title, color="white", fontsize=12, pad=12)

    ax.set_xlim(0, ncols + 2)
    ax.set_ylim(lo, hi)
    ax.tick_params(axis="x", colors="white")
    ax.tick_params(axis="y", colors="white")
    for spine in ax.spines.values():
        spine.set_color("#9aa0a6")
    ax.grid(True, linestyle=":", alpha=0.18)
    ax.set_xlabel("PnF Column", color="white")
    ax.set_ylabel("Price", color="white")

    plt.tight_layout()
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)
    return str(out_path)


def build_snapshot_filename(symbol: str, setup_id: str, snapshots_dir: str | Path = "trade_snapshots") -> str:
    clean_symbol = symbol.replace(":", "_").replace("/", "_")
    return str(Path(snapshots_dir) / f"{clean_symbol}_{setup_id}.png")

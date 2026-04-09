"""
strategy_stats_app.py

Strategy Stats Console with Trade Inspect UI
===========================================

Purpose
-------
- view summary / breakdowns / symbols / trades
- inspect exactly what the engine saw for each trade
- open a per-trade detail window
- show a simple visual ladder ("chart-style" trade map) with:
    SL / ENTRY / TP1 / TP2 / RESOLVED

Notes
-----
- uses existing fields already stored in strategy_validation.db
- reads raw_setup_json and raw_structure_json when available
- does not modify the database
- this is an inspect / analytics UI, not the scanner chart itself
"""

from __future__ import annotations

import json
import math
import os
import sqlite3
import tkinter as tk
from tkinter import ttk
from typing import Any, Dict, List, Optional

DEFAULT_DB_PATH = "strategy_validation.db"
REFRESH_MS = 5000


def safe_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except Exception:
        return None


def safe_json_loads(value: Any) -> Dict[str, Any]:
    if not value:
        return {}
    try:
        obj = json.loads(value)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def realized_r_expr() -> str:
    return """
    CASE
        WHEN resolution_status = 'STOPPED' THEN -1.0
        WHEN resolution_status = 'TP1_PARTIAL_THEN_BE' THEN
            CASE WHEN rr1 IS NOT NULL THEN 0.5 * rr1 ELSE NULL END
        WHEN resolution_status = 'TP2' THEN
            CASE WHEN rr1 IS NOT NULL AND rr2 IS NOT NULL THEN 0.5 * rr1 + 0.5 * rr2 ELSE NULL END
        ELSE NULL
    END
    """


def fmt_num(value: Any, digits: int = 3) -> str:
    x = safe_float(value)
    if x is None:
        return ""
    return f"{x:.{digits}f}"


class TradeInspectWindow(tk.Toplevel):
    def __init__(self, parent: tk.Tk, row: Dict[str, Any]):
        super().__init__(parent)
        self.title(f"Trade Inspect | {row.get('symbol', '')} | {row.get('resolution_status', '')}")
        self.geometry("1200x820")
        self.minsize(980, 700)
        self.row = row
        self.raw_setup = safe_json_loads(row.get("raw_setup_json"))
        self.raw_structure = safe_json_loads(row.get("raw_structure_json"))

        self._build_ui()
        self._fill_text()
        self._draw_trade_map()

    def _build_ui(self) -> None:
        top = ttk.Frame(self, padding=10)
        top.pack(fill="x")

        summary = [
            ("Symbol", self.row.get("symbol", "")),
            ("Side", self.row.get("side", "")),
            ("Status", self.row.get("status", "")),
            ("Resolution", self.row.get("resolution_status", "")),
            ("Score", fmt_num(self.row.get("quality_score"), 2)),
            ("R", fmt_num(self.row.get("realized_r"), 3)),
        ]
        for i, (k, v) in enumerate(summary):
            ttk.Label(top, text=f"{k}:").grid(row=0, column=i * 2, sticky="w", padx=(0, 4))
            ttk.Label(top, text=str(v), width=16).grid(row=0, column=i * 2 + 1, sticky="w", padx=(0, 12))

        body = ttk.Panedwindow(self, orient="horizontal")
        body.pack(fill="both", expand=True, padx=10, pady=(0, 10))

        left = ttk.Frame(body)
        right = ttk.Frame(body)
        body.add(left, weight=1)
        body.add(right, weight=1)

        left_nb = ttk.Notebook(left)
        left_nb.pack(fill="both", expand=True)

        self.tab_explain = ttk.Frame(left_nb, padding=8)
        self.tab_setup = ttk.Frame(left_nb, padding=8)
        self.tab_structure = ttk.Frame(left_nb, padding=8)
        left_nb.add(self.tab_explain, text="Explain")
        left_nb.add(self.tab_setup, text="Setup Snapshot")
        left_nb.add(self.tab_structure, text="Structure Snapshot")

        self.explain_text = tk.Text(self.tab_explain, wrap="word")
        self.explain_text.pack(fill="both", expand=True)

        self.setup_text = tk.Text(self.tab_setup, wrap="none")
        self.setup_text.pack(fill="both", expand=True)

        self.structure_text = tk.Text(self.tab_structure, wrap="none")
        self.structure_text.pack(fill="both", expand=True)

        right_top = ttk.LabelFrame(right, text="Trade Map", padding=8)
        right_top.pack(fill="both", expand=True)

        self.canvas = tk.Canvas(right_top, background="white")
        self.canvas.pack(fill="both", expand=True)

        right_bottom = ttk.LabelFrame(right, text="Key Fields", padding=8)
        right_bottom.pack(fill="both", expand=False, pady=(8, 0))

        self.key_fields_text = tk.Text(right_bottom, height=14, wrap="word")
        self.key_fields_text.pack(fill="both", expand=True)

        self.canvas.bind("<Configure>", lambda _e: self._draw_trade_map())

    def _engine_explanation(self) -> str:
        row = self.row
        setup = self.raw_setup
        structure = self.raw_structure

        lines: List[str] = []
        lines.append("Engine Decision Replay\n")
        lines.append(f"- symbol: {row.get('symbol', '')}")
        lines.append(f"- side: {row.get('side', '')}")
        lines.append(f"- final status at creation: {row.get('status', '')}")
        lines.append(f"- final resolution after validation: {row.get('resolution_status', '')}")
        lines.append("")
        lines.append("Why this trade was started")
        lines.append(f"- reason: {row.get('reason') or setup.get('reason') or ''}")
        if row.get("reject_reason") or setup.get("reject_reason"):
            lines.append(f"- reject_reason: {row.get('reject_reason') or setup.get('reject_reason')}")
        lines.append("")
        lines.append("Trade geometry")
        lines.append(f"- entry: {fmt_num(row.get('ideal_entry'))}")
        lines.append(f"- stop: {fmt_num(row.get('invalidation'))}")
        lines.append(f"- tp1: {fmt_num(row.get('tp1'))}")
        lines.append(f"- tp2: {fmt_num(row.get('tp2'))}")
        lines.append(f"- risk: {fmt_num(row.get('risk'))}")
        lines.append(f"- rr1: {fmt_num(row.get('rr1'), 2)}")
        lines.append(f"- rr2: {fmt_num(row.get('rr2'), 2)}")
        lines.append("")
        lines.append("What the engine saw in structure")
        for key in [
            "trend_state", "trend_regime", "immediate_slope", "breakout_context",
            "is_extended_move", "active_leg_boxes", "current_column_kind",
            "current_column_top", "current_column_bottom", "support_level",
            "resistance_level", "market_state", "latest_signal_name", "last_price"
        ]:
            value = structure.get(key, row.get(key))
            if value is not None:
                lines.append(f"- {key}: {value}")
        lines.append("")
        lines.append("What the engine scored")
        for key in [
            "pullback_quality", "risk_quality", "reward_quality", "quality_score",
            "quality_grade", "setup_maturity", "trigger_quality",
            "impulse_quality", "pullback_to_impulse_ratio", "continuation_quality",
            "extension_severity", "entry_drift_boxes"
        ]:
            value = setup.get(key, row.get(key))
            if value is not None:
                lines.append(f"- {key}: {value}")
        lines.append("")
        lines.append("Outcome interpretation")
        lines.append(f"- tp1_hit: {row.get('tp1_hit', 0)}")
        if row.get("tp1_hit_ts"):
            lines.append(f"- tp1_hit_ts: {row.get('tp1_hit_ts')}")
        if row.get("tp1_price") is not None:
            lines.append(f"- tp1_price: {fmt_num(row.get('tp1_price'))}")
        if row.get("resolved_price") is not None:
            lines.append(f"- resolved_price: {fmt_num(row.get('resolved_price'))}")
        if row.get("resolution_note"):
            lines.append(f"- resolution_note: {row.get('resolution_note')}")
        lines.append(f"- realized_r: {fmt_num(row.get('realized_r'), 3)}")
        return "\n".join(lines)

    def _fill_text(self) -> None:
        self.explain_text.delete("1.0", "end")
        self.explain_text.insert("1.0", self._engine_explanation())
        self.setup_text.delete("1.0", "end")
        self.setup_text.insert("1.0", json.dumps(self.raw_setup, indent=2, ensure_ascii=False, sort_keys=True))
        self.structure_text.delete("1.0", "end")
        self.structure_text.insert("1.0", json.dumps(self.raw_structure, indent=2, ensure_ascii=False, sort_keys=True))

        fields = [
            ("symbol", self.row.get("symbol")),
            ("side", self.row.get("side")),
            ("status", self.row.get("status")),
            ("resolution_status", self.row.get("resolution_status")),
            ("breakout_context", self.row.get("breakout_context")),
            ("quality_score", self.row.get("quality_score")),
            ("quality_grade", self.row.get("quality_grade")),
            ("pullback_quality", self.row.get("pullback_quality")),
            ("risk_quality", self.row.get("risk_quality")),
            ("reward_quality", self.row.get("reward_quality")),
            ("reason", self.row.get("reason")),
            ("reject_reason", self.row.get("reject_reason")),
            ("reference_ts", self.row.get("reference_ts")),
            ("activated_ts", self.row.get("activated_ts")),
            ("resolved_ts", self.row.get("resolved_ts")),
        ]
        self.key_fields_text.delete("1.0", "end")
        self.key_fields_text.insert("1.0", "\n".join(f"{k}: {v}" for k, v in fields))

    def _draw_trade_map(self) -> None:
        c = self.canvas
        c.delete("all")
        width = max(200, c.winfo_width())
        height = max(200, c.winfo_height())

        levels = {
            "SL": safe_float(self.row.get("invalidation")),
            "ENTRY": safe_float(self.row.get("ideal_entry")),
            "TP1": safe_float(self.row.get("tp1")),
            "TP2": safe_float(self.row.get("tp2")),
            "RESOLVED": safe_float(self.row.get("resolved_price")),
            "TP1_PRICE": safe_float(self.row.get("tp1_price")),
        }
        visible = {k: v for k, v in levels.items() if v is not None}
        if not visible:
            c.create_text(width / 2, height / 2, text="No price levels available", font=("Segoe UI", 11))
            return

        values = list(visible.values())
        lo = min(values)
        hi = max(values)
        span = hi - lo
        if math.isclose(span, 0.0):
            span = max(abs(hi), 1.0) * 0.01 + 1.0

        pad_top = 30
        pad_bottom = 30
        pad_left = 80
        pad_right = 50

        def y_of(price: float) -> float:
            frac = (price - lo) / span
            return height - pad_bottom - frac * (height - pad_top - pad_bottom)

        c.create_rectangle(1, 1, width - 1, height - 1)
        c.create_text(width / 2, 16, text="Trade Ladder", font=("Segoe UI", 11, "bold"))

        order = ["TP2", "TP1", "ENTRY", "SL", "RESOLVED", "TP1_PRICE"]
        colors = {
            "TP2": "#2e8b57",
            "TP1": "#3cb371",
            "ENTRY": "#1f77b4",
            "SL": "#c0392b",
            "RESOLVED": "#8e44ad",
            "TP1_PRICE": "#16a085",
        }

        x1 = pad_left
        x2 = width - pad_right
        for name in order:
            price = visible.get(name)
            if price is None:
                continue
            y = y_of(price)
            color = colors.get(name, "black")
            dash = (4, 2) if name in ("RESOLVED", "TP1_PRICE") else None
            c.create_line(x1, y, x2, y, fill=color, width=2, dash=dash)
            c.create_text(45, y, text=name, fill=color, anchor="w", font=("Segoe UI", 10, "bold"))
            c.create_text(width - 8, y, text=f"{price:.3f}", fill=color, anchor="e", font=("Segoe UI", 10))

        side = str(self.row.get("side") or "").upper()
        res = str(self.row.get("resolution_status") or "").upper()
        c.create_text(width / 2, height - 10, text=f"{side} | {res}", font=("Segoe UI", 10))

        entry = safe_float(self.row.get("ideal_entry"))
        sl = safe_float(self.row.get("invalidation"))
        if entry is not None and sl is not None:
            y1 = y_of(max(entry, sl))
            y2 = y_of(min(entry, sl))
            c.create_rectangle(x1 + 150, y1, x2 - 150, y2, outline="#999999", dash=(3, 2))


class StrategyStatsApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Strategy Stats Console")
        self.geometry("1650x960")
        self.minsize(1280, 760)

        self.db_path_var = tk.StringVar(value=DEFAULT_DB_PATH)
        self.refresh_ms_var = tk.StringVar(value=str(REFRESH_MS))
        self.auto_refresh_var = tk.BooleanVar(value=True)
        self.status_var = tk.StringVar(value="Ready")

        self.summary_labels: Dict[str, ttk.Label] = {}
        self.trees: Dict[str, ttk.Treeview] = {}
        self.filter_symbol = tk.StringVar()
        self.filter_side = tk.StringVar()
        self.filter_resolution = tk.StringVar()
        self._explorer_rows_by_iid: Dict[str, Dict[str, Any]] = {}

        self._build_ui()
        self.after(300, self.refresh_all)

    def _build_ui(self) -> None:
        top = ttk.Frame(self, padding=10)
        top.pack(fill="x")
        ttk.Label(top, text="DB Path").grid(row=0, column=0, sticky="w")
        ttk.Entry(top, textvariable=self.db_path_var, width=92).grid(row=0, column=1, sticky="ew", padx=(8, 8))
        ttk.Label(top, text="Refresh ms").grid(row=0, column=2, sticky="w")
        ttk.Entry(top, textvariable=self.refresh_ms_var, width=10).grid(row=0, column=3, sticky="w", padx=(8, 8))
        ttk.Checkbutton(top, text="Auto refresh", variable=self.auto_refresh_var).grid(row=0, column=4, sticky="w", padx=(8, 8))
        ttk.Button(top, text="Refresh now", command=self.refresh_all).grid(row=0, column=5, sticky="e")
        top.columnconfigure(1, weight=1)

        nb = ttk.Notebook(self)
        nb.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        self.tab_summary = ttk.Frame(nb, padding=10)
        self.tab_breakdowns = ttk.Frame(nb, padding=10)
        self.tab_symbols = ttk.Frame(nb, padding=10)
        self.tab_explorer = ttk.Frame(nb, padding=10)
        nb.add(self.tab_summary, text="Summary")
        nb.add(self.tab_breakdowns, text="Breakdowns")
        nb.add(self.tab_symbols, text="Symbols")
        nb.add(self.tab_explorer, text="Trade Explorer")

        self._build_summary_tab()
        self._build_breakdowns_tab()
        self._build_symbols_tab()
        self._build_explorer_tab()

        bottom = ttk.Frame(self, padding=(10, 0, 10, 10))
        bottom.pack(fill="x")
        ttk.Label(bottom, textvariable=self.status_var).pack(anchor="w")

    def _build_summary_tab(self) -> None:
        frame = ttk.LabelFrame(self.tab_summary, text="Summary", padding=10)
        frame.pack(fill="x")
        keys = [
            "total_rows", "candidate_rows", "pending_rows", "resolved_rows",
            "tp1_touched_rows", "stopped_rows", "tp1be_rows", "tp2_rows",
            "win_rate", "avg_r",
        ]
        for idx, key in enumerate(keys):
            r = idx // 4
            c = (idx % 4) * 2
            ttk.Label(frame, text=key.replace("_", " ").title() + ":").grid(row=r, column=c, sticky="w", padx=(0, 6), pady=4)
            lbl = ttk.Label(frame, text="-", width=18)
            lbl.grid(row=r, column=c + 1, sticky="w", padx=(0, 18), pady=4)
            self.summary_labels[key] = lbl

        info = ttk.LabelFrame(self.tab_summary, text="Interpretation", padding=10)
        info.pack(fill="both", expand=True, pady=(10, 0))
        self.summary_text = tk.Text(info, height=18, wrap="word")
        self.summary_text.pack(fill="both", expand=True)

    def _make_tree(self, parent: tk.Widget, cols: List[tuple[str, int]], height: int = 12) -> ttk.Treeview:
        wrap = ttk.Frame(parent)
        wrap.pack(fill="both", expand=True)
        tree = ttk.Treeview(wrap, columns=[name for name, _ in cols], show="headings", height=height)
        for name, width in cols:
            tree.heading(name, text=name.upper())
            tree.column(name, width=width, anchor="center")
        y = ttk.Scrollbar(wrap, orient="vertical", command=tree.yview)
        tree.configure(yscrollcommand=y.set)
        tree.pack(side="left", fill="both", expand=True)
        y.pack(side="right", fill="y")
        return tree

    def _build_breakdowns_tab(self) -> None:
        wrap = ttk.Frame(self.tab_breakdowns)
        wrap.pack(fill="both", expand=True)
        cols = [
            ("group", 220), ("total", 80), ("tp1_touched", 110), ("stopped", 90),
            ("tp1be", 90), ("tp2", 90), ("pending", 80), ("win_rate", 90), ("avg_r", 90),
        ]
        lf1 = ttk.LabelFrame(wrap, text="By Side", padding=6)
        lf2 = ttk.LabelFrame(wrap, text="By Breakout Context", padding=6)
        lf3 = ttk.LabelFrame(wrap, text="By Risk Quality", padding=6)
        lf1.grid(row=0, column=0, sticky="nsew", padx=6, pady=6)
        lf2.grid(row=0, column=1, sticky="nsew", padx=6, pady=6)
        lf3.grid(row=1, column=0, sticky="nsew", padx=6, pady=6)
        self.trees["by_side"] = self._make_tree(lf1, cols, 12)
        self.trees["by_breakout_context"] = self._make_tree(lf2, cols, 12)
        self.trees["by_risk_quality"] = self._make_tree(lf3, cols, 12)
        wrap.columnconfigure(0, weight=1)
        wrap.columnconfigure(1, weight=1)
        wrap.rowconfigure(0, weight=1)
        wrap.rowconfigure(1, weight=1)

    def _build_symbols_tab(self) -> None:
        lf = ttk.LabelFrame(self.tab_symbols, text="By Symbol", padding=6)
        lf.pack(fill="both", expand=True)
        cols = [
            ("group", 220), ("total", 80), ("tp1_touched", 110), ("stopped", 90),
            ("tp1be", 90), ("tp2", 90), ("pending", 80), ("win_rate", 90), ("avg_r", 90),
        ]
        self.trees["by_symbol"] = self._make_tree(lf, cols, 22)

    def _build_explorer_tab(self) -> None:
        top = ttk.Frame(self.tab_explorer)
        top.pack(fill="x", pady=(0, 8))
        ttk.Label(top, text="Symbol contains").grid(row=0, column=0, sticky="w")
        ttk.Entry(top, textvariable=self.filter_symbol, width=24).grid(row=0, column=1, sticky="w", padx=(6, 12))
        ttk.Label(top, text="Side").grid(row=0, column=2, sticky="w")
        ttk.Entry(top, textvariable=self.filter_side, width=12).grid(row=0, column=3, sticky="w", padx=(6, 12))
        ttk.Label(top, text="Resolution").grid(row=0, column=4, sticky="w")
        ttk.Entry(top, textvariable=self.filter_resolution, width=18).grid(row=0, column=5, sticky="w", padx=(6, 12))
        ttk.Button(top, text="Apply filters", command=self.refresh_all).grid(row=0, column=6, sticky="w")
        ttk.Button(top, text="Inspect selected", command=self.inspect_selected_trade).grid(row=0, column=7, sticky="w", padx=(12, 0))

        lf = ttk.LabelFrame(self.tab_explorer, text="Recent Trades", padding=6)
        lf.pack(fill="both", expand=True)
        cols = [
            ("symbol", 170), ("side", 70), ("resolution_status", 170), ("tp1_hit", 70),
            ("tp1_price", 90), ("tp1_hit_ts", 120), ("quality_score", 85),
            ("ideal_entry", 90), ("invalidation", 90), ("tp1", 90), ("tp2", 90), ("realized_r", 90),
        ]
        tree = self._make_tree(lf, cols, 24)
        tree.bind("<Double-1>", lambda _e: self.inspect_selected_trade())
        self.trees["explorer"] = tree

    def _connect(self) -> sqlite3.Connection:
        db_path = self.db_path_var.get().strip()
        if not db_path:
            raise ValueError("DB path is empty")
        if not os.path.exists(db_path):
            raise FileNotFoundError(f"DB not found: {db_path}")
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _fetch_summary(self, conn: sqlite3.Connection) -> Dict[str, Any]:
        expr = realized_r_expr()
        q = f"""
        SELECT
            COUNT(*) AS total_rows,
            SUM(CASE WHEN status='CANDIDATE' THEN 1 ELSE 0 END) AS candidate_rows,
            SUM(CASE WHEN resolution_status='PENDING' THEN 1 ELSE 0 END) AS pending_rows,
            SUM(CASE WHEN resolution_status!='PENDING' THEN 1 ELSE 0 END) AS resolved_rows,
            SUM(CASE WHEN tp1_hit=1 THEN 1 ELSE 0 END) AS tp1_touched_rows,
            SUM(CASE WHEN resolution_status='STOPPED' THEN 1 ELSE 0 END) AS stopped_rows,
            SUM(CASE WHEN resolution_status='TP1_PARTIAL_THEN_BE' THEN 1 ELSE 0 END) AS tp1be_rows,
            SUM(CASE WHEN resolution_status='TP2' THEN 1 ELSE 0 END) AS tp2_rows,
            AVG({expr}) AS avg_r
        FROM strategy_setups
        """
        row = conn.execute(q).fetchone()
        out = dict(row)
        resolved_non_amb = (out["stopped_rows"] or 0) + (out["tp1be_rows"] or 0) + (out["tp2_rows"] or 0)
        wins = (out["tp1be_rows"] or 0) + (out["tp2_rows"] or 0)
        out["win_rate"] = (wins / resolved_non_amb) if resolved_non_amb else 0.0
        return out

    def _fetch_group_table(self, conn: sqlite3.Connection, field_name: str) -> List[Dict[str, Any]]:
        expr = realized_r_expr()
        q = f"""
        SELECT
            COALESCE({field_name}, 'NONE') AS grp,
            COUNT(*) AS total,
            SUM(CASE WHEN tp1_hit=1 THEN 1 ELSE 0 END) AS tp1_touched,
            SUM(CASE WHEN resolution_status='STOPPED' THEN 1 ELSE 0 END) AS stopped,
            SUM(CASE WHEN resolution_status='TP1_PARTIAL_THEN_BE' THEN 1 ELSE 0 END) AS tp1be,
            SUM(CASE WHEN resolution_status='TP2' THEN 1 ELSE 0 END) AS tp2,
            SUM(CASE WHEN resolution_status='PENDING' THEN 1 ELSE 0 END) AS pending,
            AVG({expr}) AS avg_r
        FROM strategy_setups
        GROUP BY COALESCE({field_name}, 'NONE')
        ORDER BY total DESC, grp ASC
        """
        rows = conn.execute(q).fetchall()
        out = []
        for r in rows:
            resolved = (r["stopped"] or 0) + (r["tp1be"] or 0) + (r["tp2"] or 0)
            wins = (r["tp1be"] or 0) + (r["tp2"] or 0)
            out.append({
                "group": r["grp"],
                "total": r["total"] or 0,
                "tp1_touched": r["tp1_touched"] or 0,
                "stopped": r["stopped"] or 0,
                "tp1be": r["tp1be"] or 0,
                "tp2": r["tp2"] or 0,
                "pending": r["pending"] or 0,
                "win_rate": (wins / resolved) if resolved else 0.0,
                "avg_r": r["avg_r"] if r["avg_r"] is not None else 0.0,
            })
        return out

    def _fetch_recent_trades(self, conn: sqlite3.Connection) -> List[Dict[str, Any]]:
        expr = realized_r_expr()
        q = f"""
        SELECT
            setup_id, symbol, side, status, resolution_status,
            tp1_hit, tp1_hit_ts, tp1_price,
            quality_score, quality_grade,
            ideal_entry, invalidation, tp1, tp2, rr1, rr2, risk,
            breakout_context, pullback_quality, risk_quality, reward_quality,
            reason, reject_reason,
            reference_ts, activated_ts, resolved_ts, resolved_price, resolution_note,
            raw_setup_json, raw_structure_json,
            {expr} AS realized_r
        FROM strategy_setups
        WHERE 1=1
        """
        params: List[Any] = []
        if self.filter_symbol.get().strip():
            q += " AND symbol LIKE ?"
            params.append(f"%{self.filter_symbol.get().strip()}%")
        if self.filter_side.get().strip().upper():
            q += " AND side = ?"
            params.append(self.filter_side.get().strip().upper())
        if self.filter_resolution.get().strip().upper():
            q += " AND resolution_status = ?"
            params.append(self.filter_resolution.get().strip().upper())
        q += " ORDER BY COALESCE(resolved_ts, 0) DESC, reference_ts DESC LIMIT 500"
        return [dict(r) for r in conn.execute(q, params).fetchall()]

    def _fill_tree(self, tree: ttk.Treeview, rows: List[Dict[str, Any]], keys: List[str]) -> None:
        for item in tree.get_children():
            tree.delete(item)
        for r in rows:
            vals = []
            for k in keys:
                v = r.get(k)
                if isinstance(v, float):
                    vals.append(f"{v:.3f}")
                else:
                    vals.append(v)
            tree.insert("", "end", values=vals)

    def _fill_explorer(self, rows: List[Dict[str, Any]]) -> None:
        tree = self.trees["explorer"]
        self._explorer_rows_by_iid = {}
        for item in tree.get_children():
            tree.delete(item)
        keys = ["symbol", "side", "resolution_status", "tp1_hit", "tp1_price", "tp1_hit_ts", "quality_score", "ideal_entry", "invalidation", "tp1", "tp2", "realized_r"]
        for r in rows:
            vals = []
            for k in keys:
                v = r.get(k)
                if isinstance(v, float):
                    vals.append(f"{v:.3f}")
                else:
                    vals.append(v)
            iid = tree.insert("", "end", values=vals)
            self._explorer_rows_by_iid[iid] = r

    def inspect_selected_trade(self) -> None:
        tree = self.trees["explorer"]
        selected = tree.selection()
        if not selected:
            self.status_var.set("Inspect: no trade selected")
            return
        row = self._explorer_rows_by_iid.get(selected[0])
        if not row:
            self.status_var.set("Inspect: selected row not found")
            return
        TradeInspectWindow(self, row)

    def _update_summary_text(self, summary: Dict[str, Any]) -> None:
        lines = []
        lines.append("Current interpretation\n\n")
        lines.append(f"- resolved trades: {summary['resolved_rows'] or 0}\n")
        lines.append(f"- pending trades: {summary['pending_rows'] or 0}\n")
        lines.append(f"- tp1 touched rows: {summary['tp1_touched_rows'] or 0}\n")
        lines.append(f"- final tp1be rows: {summary['tp1be_rows'] or 0}\n")
        lines.append(f"- final tp2 rows: {summary['tp2_rows'] or 0}\n")
        lines.append(f"- win rate (tp1be + tp2): {summary['win_rate']:.4f}\n")
        lines.append(f"- avg R: {(summary['avg_r'] or 0):.4f}\n\n")
        lines.append("- use Trade Explorer -> Inspect selected to see exactly what the engine saw\n")
        lines.append("- Trade Map shows SL / ENTRY / TP1 / TP2 / RESOLVED on a simple chart-style ladder\n")
        self.summary_text.delete("1.0", "end")
        self.summary_text.insert("1.0", "".join(lines))

    def refresh_all(self) -> None:
        try:
            conn = self._connect()
            try:
                summary = self._fetch_summary(conn)
                for key, label in self.summary_labels.items():
                    val = summary.get(key)
                    if key in ("win_rate", "avg_r"):
                        label.config(text=f"{(val or 0):.4f}")
                    else:
                        label.config(text=str(val or 0))
                self._update_summary_text(summary)

                for tree_key, field in {
                    "by_side": "side",
                    "by_breakout_context": "breakout_context",
                    "by_risk_quality": "risk_quality",
                    "by_symbol": "symbol",
                }.items():
                    rows = self._fetch_group_table(conn, field)
                    self._fill_tree(self.trees[tree_key], rows, ["group", "total", "tp1_touched", "stopped", "tp1be", "tp2", "pending", "win_rate", "avg_r"])

                self._fill_explorer(self._fetch_recent_trades(conn))
                self.status_var.set("Last refresh: OK")
            finally:
                conn.close()
        except Exception as e:
            self.status_var.set(f"Refresh error: {e}")

        if self.auto_refresh_var.get():
            try:
                ms = max(1000, int(self.refresh_ms_var.get().strip()))
            except Exception:
                ms = REFRESH_MS
            self.after(ms, self.refresh_all)


if __name__ == "__main__":
    app = StrategyStatsApp()
    app.mainloop()

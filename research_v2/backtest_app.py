"""Single-dataset offline research window; never starts the operational scanner."""
from __future__ import annotations

import json
import os
import subprocess
import sys
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, messagebox, ttk

from research_v2.backtest_workspace import prepare_btc_2024_job


class ResearchApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Research Backtests")
        self.geometry("670x375")
        self.dataset = tk.StringVar(value="Δεν έχει επιλεγεί dataset")
        self.status = tk.StringVar(value="Επίλεξε τον φάκελο results του BTC 2024.")
        self.start_date = tk.StringVar(value="2024-01-03")
        self.end_date = tk.StringVar(value="2024-12-31")
        self.target_sweep = tk.BooleanVar(value=True)
        self.process = None
        self.job_path = None
        self.output_path = None
        self.log_path = None

        ttk.Label(self, text="BTCUSDT 2024 — causal LONG pole", font=("TkDefaultFont", 12, "bold")).pack(pady=12)
        ttk.Button(self, text="Επιλογή dataset", command=self.choose).pack(pady=3)
        ttk.Label(self, textvariable=self.dataset, wraplength=620).pack(padx=15, pady=4)
        period = ttk.Frame(self)
        period.pack(pady=6)
        ttk.Label(period, text="Σήματα από (UTC)").grid(row=0, column=0, padx=5)
        ttk.Entry(period, textvariable=self.start_date, width=12).grid(row=0, column=1, padx=5)
        ttk.Label(period, text="έως και (UTC)").grid(row=0, column=2, padx=5)
        ttk.Entry(period, textvariable=self.end_date, width=12).grid(row=0, column=3, padx=5)
        ttk.Label(self, text="Η περίοδος ορίζει νέα επιλέξιμα σήματα. Όλο το frozen ιστορικό δίνει warm-up και μεταγενέστερες εξόδους.",
                  wraplength=620).pack()
        ttk.Checkbutton(self, text="Σύγκριση στόχων 2,5R, 3R έως 10R (επιπλέον προσομοιώσεις)",
                        variable=self.target_sweep).pack(pady=3)
        self.run_button = ttk.Button(self, text="Εκτέλεση backtest", command=self.start)
        self.run_button.pack(pady=8)
        ttk.Button(self, text="Προβολή trades από προηγούμενο run",
                   command=self.open_trades).pack(pady=2)
        ttk.Label(self, textvariable=self.status, wraplength=620).pack(padx=15, pady=6)
        self.after(300, self.poll)

    def choose(self):
        selected = filedialog.askdirectory(title="Επίλεξε τον φάκελο results του BTC 2024")
        if selected:
            self.dataset.set(selected)

    def start(self):
        if self.process is not None:
            return
        try:
            self.job_path = prepare_btc_2024_job(Path(self.dataset.get()),
                                                 self.start_date.get(), self.end_date.get(),
                                                 self.target_sweep.get())
            self.output_path = Path(json.loads(self.job_path.read_text(encoding="utf-8"))["output_root"])
            self.log_path = self.job_path.with_suffix(".run.log")
            with self.log_path.open("x", encoding="utf-8") as log:
                self.process = subprocess.Popen(
                    [sys.executable, "-B", "-m", "research_v2.backtest_workspace", "--job", str(self.job_path)],
                    cwd=str(Path(__file__).resolve().parent.parent), stdout=log, stderr=subprocess.STDOUT,
                    env={key: value for key, value in os.environ.items()
                         if not any(secret in key.upper() for secret in
                                    ("API_KEY", "API_SECRET", "TOKEN", "PASSWORD", "CREDENTIAL"))},
                )
            self.run_button.state(["disabled"])
            self.status.set("Το backtest εκτελείται. Τα frozen αρχεία ελέγχθηκαν.")
        except (OSError, ValueError) as exc:
            self.status.set("Δεν ξεκίνησε backtest.")
            messagebox.showerror("Research Backtests", str(exc))

    def open_trades(self):
        selected = (str(self.output_path) if self.output_path is not None
                    and (self.output_path / "research_job_manifest.json").is_file()
                    else filedialog.askdirectory(title="Επίλεξε τον φάκελο αποτελεσμάτων του backtest"))
        if not selected:
            return
        try:
            from research_v2.trade_chart import TradeChartWindow, completed_run_sources
            root = Path(selected)
            columns, candles = completed_run_sources(root)
            TradeChartWindow(self, root, candles, columns)
        except (OSError, ValueError, KeyError) as exc:
            messagebox.showerror("Research chart", str(exc))

    def poll(self):
        if self.process is not None:
            code = self.process.poll()
            if code is not None:
                self.process = None
                self.run_button.state(["!disabled"])
                if code == 0:
                    try:
                        result = json.loads(self.log_path.read_text(encoding="utf-8"))["result"]
                        self.status.set(
                            f"Ολοκληρώθηκε: {result['decision_count']} decisions, "
                            f"{result['resolved_portfolio_trades']} trades, "
                            f"{result['gross_total_R']} gross R στο 2,5R. "
                            f"{'Σύγκριση: ' + str(self.output_path / 'target_sweep' / 'comparison.csv') if result.get('target_sweep_manifest') else 'Αποτελέσματα: ' + str(self.output_path)}"
                        )
                    except (OSError, ValueError, KeyError):
                        self.status.set(f"Ολοκληρώθηκε. Αποτελέσματα: {self.output_path}")
                else:
                    self.status.set(f"Το backtest απέτυχε. Log: {self.log_path}")
        self.after(300, self.poll)


def main():
    ResearchApp().mainloop()


if __name__ == "__main__":
    main()

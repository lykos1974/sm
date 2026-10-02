"""Standalone offline research window; never starts the operational scanner."""
from __future__ import annotations

import subprocess
import sys
import os
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, messagebox, ttk

from research_v2.backtest_workspace import _check_csv_header, _sha, create_job, load_job, suggest_output_directory


class ResearchApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Research Backtests")
        self.geometry("780x430")
        self.job_path = tk.StringVar()
        self.columns_path = tk.StringVar()
        self.candles_path = tk.StringVar()
        self.output_path = tk.StringVar()
        self.minimum_ts = tk.StringVar(value="1704240000000")
        self.status = tk.StringVar(value="Επίλεξε ένα research job JSON.")
        ttk.Label(self, text="Research Backtests — απομονωμένη εκτέλεση").pack(pady=12)
        ttk.Label(self, text="Προς εκτέλεση: causal_long_pole (BTCUSDT, frozen CSV)").pack()
        ttk.Label(self, text="Άλλα patterns απαιτούν ελεγμένο adapter και κανόνες εκτέλεσης.").pack()
        for label, variable, directory in (
            ("PnF columns CSV", self.columns_path, False),
            ("Candles CSV", self.candles_path, False),
            ("Νέος φάκελος αποτελεσμάτων", self.output_path, True),
        ):
            row = ttk.Frame(self)
            row.pack(fill="x", padx=18, pady=3)
            ttk.Label(row, text=label, width=27).pack(side="left")
            ttk.Entry(row, textvariable=variable).pack(side="left", fill="x", expand=True)
            ttk.Button(row, text="…", command=lambda v=variable, d=directory: self.browse(v, d)).pack(side="left")
        row = ttk.Frame(self)
        row.pack(fill="x", padx=18, pady=3)
        ttk.Label(row, text="Πρώτο επιλέξιμο entry (UTC ms)", width=27).pack(side="left")
        ttk.Entry(row, textvariable=self.minimum_ts).pack(side="left", fill="x", expand=True)
        ttk.Button(self, text="Δημιουργία job και εκτέλεση", command=self.create_and_start).pack(pady=5)
        frame = ttk.Frame(self)
        frame.pack(fill="x", padx=18, pady=8)
        ttk.Label(frame, text="Υπάρχον job:").pack(side="left")
        ttk.Entry(frame, textvariable=self.job_path).pack(side="left", fill="x", expand=True)
        ttk.Button(frame, text="Επιλογή job", command=self.choose).pack(side="left", padx=6)
        self.button = ttk.Button(self, text="Εκτέλεση υπάρχοντος job", command=self.start)
        self.button.pack(pady=4)
        ttk.Label(self, textvariable=self.status, wraplength=650).pack(padx=15, pady=8)
        self.after(200, self.poll)
        self.process = None
        self.log_path = None

    def browse(self, variable, directory):
        selected = filedialog.askdirectory() if directory else filedialog.askopenfilename(
            filetypes=[("CSV", "*.csv")])
        if selected:
            variable.set(str(suggest_output_directory(Path(selected))) if directory else selected)

    def create_and_start(self):
        try:
            if self.process is not None:
                raise ValueError("Υπάρχει ήδη research job σε εξέλιξη.")
            output = Path(self.output_path.get())
            if not output.is_absolute():
                raise ValueError("Απαιτείται πλήρης διαδρομή για τον νέο φάκελο.")
            job_file = output.with_suffix(".job.json")
            create_job(Path(self.columns_path.get()), Path(self.candles_path.get()),
                       int(self.minimum_ts.get()), output, job_file)
            self.job_path.set(str(job_file))
            self.start()
        except (OSError, ValueError) as exc:
            self.status.set("Δεν δημιουργήθηκε research job.")
            messagebox.showerror("Research job", str(exc))

    def choose(self):
        selected = filedialog.askopenfilename(filetypes=[("Research job", "*.json")])
        if selected:
            self.job_path.set(selected)

    def start(self):
        try:
            if self.process is not None:
                raise ValueError("Υπάρχει ήδη research job σε εξέλιξη.")
            job_file = Path(self.job_path.get()).resolve(strict=True)
            job = load_job(job_file)
            output = Path(job["output_root"])
            if output.exists() or not output.parent.is_dir():
                raise ValueError("Νέος φάκελος output με υπάρχοντα parent απαιτείται.")
            for name in ("columns", "candles"):
                source = Path(job[name + "_csv"]).resolve(strict=True)
                if not source.is_file() or _sha(source) != job[name + "_sha256"]:
                    raise ValueError("Ασυμφωνία frozen input: " + name)
                _check_csv_header(source, name)
            # The worker runs in its own process and imports no scanner module.
            self.log_path = job_file.with_suffix(".run.log")
            with self.log_path.open("x", encoding="utf-8") as log:
                self.process = subprocess.Popen(
                    [sys.executable, "-B", "-m", "research_v2.backtest_workspace", "--job", str(job_file)],
                    cwd=str(Path(__file__).resolve().parent.parent),
                    stdout=log, stderr=subprocess.STDOUT,
                    env={key: value for key, value in os.environ.items()
                         if not any(secret in key.upper() for secret in
                                    ("API_KEY", "API_SECRET", "TOKEN", "PASSWORD", "CREDENTIAL"))},
                )
            self.button.state(["disabled"])
            self.status.set("Εκτέλεση research job. Το παράθυρο παραμένει διαθέσιμο.")
        except (OSError, ValueError, KeyError) as exc:
            self.status.set("Ο έλεγχος απέτυχε· δεν ξεκίνησε job.")
            messagebox.showerror("Research job", str(exc))

    def poll(self):
        if self.process is not None:
            result = self.process.poll()
            if result is not None:
                self.process = None
                self.button.state(["!disabled"])
                self.status.set("Ολοκληρώθηκε: " + self.job_path.get() if result == 0
                                else "Το job απέτυχε. Log: " + str(self.log_path))
        self.after(200, self.poll)


def main():
    ResearchApp().mainloop()


if __name__ == "__main__":
    main()

"""Pinned 2024 BTC offline PRZ-context annotation of existing pole replay.

Exploratory subset description only. Never a filtered execution replay.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import tempfile
from collections import Counter, defaultdict
from dataclasses import asdict
from decimal import Decimal
from pathlib import Path

from research_v2.du_plessis_poles_annual import (
    FIRST_CLOSE_MS, LAST_CLOSE_MS, SOURCE_MINUTES, SOURCE_SHA256, summarize,
)
from research_v2.du_plessis_poles_forward_sim import simulate
from research_v2.du_plessis_poles_preview import PnFEngine, PnFProfile
from research_v2.gartley_pole_prz import CausalPrzLedger, number
from research_v2.gartley_pole_prz_smoke import sha256
from research_v2.prz_x_break_pole_context_v2 import PROFILE, XBreakPoleContext
from pnf_mvp.strategies.du_plessis_poles_v1 import PoleDecisionLedger


def _atomic_new_json(path: Path, payload: dict) -> None:
    if path.exists() or not path.parent.is_dir():
        raise ValueError("existing report or missing output directory")
    temporary = None
    try:
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", newline="\n",
                                         prefix=".prz_context_", suffix=".tmp",
                                         dir=path.parent, delete=False) as stream:
            temporary = Path(stream.name)
            json.dump(payload, stream, indent=2, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        # Same-directory hard link gives no-overwrite publication on Windows
        # and POSIX. The complete fsynced file appears only after this call.
        os.link(temporary, path)
    finally:
        if temporary is not None:
            try:
                temporary.unlink()
            except OSError:
                pass


def _describe(rows: list[dict]) -> dict:
    totals: dict[str, list[Decimal]] = defaultdict(list)
    for row in rows:
        if row["status"] != "SIMULATED_CLOSED":
            continue
        entry = Decimal(row["entry_simulated_open_price"])
        delta = Decimal(row["gross_price_delta"])
        if entry <= 0:
            raise ValueError("invalid simulated entry")
        totals[row["context_category"]].append(delta * Decimal(10000) / entry)
    return {name: {"completed": len(values),
                   "gross_average_bps": str(sum(values, Decimal(0))/len(values)) if values else None,
                   "gross_sum_bps_equal_notional_proxy": str(sum(values, Decimal(0)))}
            for name, values in sorted(totals.items())}


def run(path: Path, *, output: Path, expected_sha256: str = SOURCE_SHA256) -> dict:
    if (not path.is_file() or path.suffix.lower() != ".csv"
            or expected_sha256 != SOURCE_SHA256 or output.exists() or not output.parent.is_dir()):
        raise ValueError("pinned research input and new output required")
    if sha256(path) != SOURCE_SHA256:
        raise ValueError("pinned 2024 candle hash mismatch")
    engine = PnFEngine(PnFProfile(PROFILE, 100, 3))
    pole_ledger = PoleDecisionLedger()
    zones = CausalPrzLedger(box_size="100")
    context = XBreakPoleContext(enabled=True)
    previous = None
    first = None
    count = 0
    with path.open(newline="", encoding="utf-8-sig") as stream:
        reader = csv.DictReader(stream)
        if not reader.fieldnames or not {"close_time", "close"} <= set(reader.fieldnames):
            raise ValueError("closed-candle CSV required")
        for row in reader:
            ts = int(row["close_time"])
            if previous is not None and ts != previous + 60000:
                raise ValueError("gap or duplicate in pinned input")
            close = number(row["close"])
            if close <= 0:
                raise ValueError("invalid candle close")
            engine.update_from_price(ts, float(close))
            events = tuple(e for e in pole_ledger.ingest(
                engine.columns, box_size="100", reversal_boxes=3, enabled=True)
                if e.status == "CANDIDATE" and e.action in ("LONG", "SHORT"))
            facts = zones.ingest(engine.columns, close_ts=ts, close=close,
                                 pole_events=events)
            context.ingest(facts, pole_events=events, at=ts)
            if first is None:
                first = ts
            previous = ts
            count += 1
            if count > SOURCE_MINUTES:
                raise ValueError("excess candles")
    if (count != SOURCE_MINUTES or first != FIRST_CLOSE_MS or previous != LAST_CLOSE_MS):
        raise ValueError("incomplete pinned UTC year")
    baseline = simulate(path, box_size=100, reversal_boxes=3,
                        max_candles=SOURCE_MINUTES, full_year_verified=True)
    if (baseline["candles_processed"] != count or baseline["first_close_ts"] != first
            or baseline["last_close_ts"] != previous or sha256(path) != SOURCE_SHA256):
        raise ValueError("input changed or baseline prefix mismatch")
    annotations = {a.event_id: a for a in context.annotations}
    if len(annotations) != len(context.annotations):
        raise ValueError("duplicate pole signal")
    all_observed_ids = {row["event_id"] for row in baseline["trades"] + baseline["skipped"]}
    all_observed_ids.update(e for e in (baseline["pending_entry_event_id"],
                                        baseline["pending_exit_event_id"]) if e is not None)
    # An exit candidate is an event_id suffix, while its entry is annotated.
    pending_exit = baseline["pending_exit_event_id"]
    if pending_exit is not None:
        all_observed_ids.remove(pending_exit)
        all_observed_ids.add(pending_exit.removesuffix(":REVERSAL_EXIT"))
    if not all_observed_ids <= annotations.keys():
        raise ValueError("unannotated baseline signal")
    joined = [{**row, "context_category": annotations[row["event_id"]].category,
               "context_reason": annotations[row["event_id"]].reason}
              for row in baseline["trades"]]
    summary = {"candles": count, "pole_signals": len(annotations),
               "signal_categories": dict(sorted(Counter(a.category for a in context.annotations).items())),
               "baseline": summarize(baseline),
               "baseline_completed_trade_subsets": _describe(joined),
               "interpretation": "observed subset of unchanged baseline; not a filtered replay or net profit"}
    report = {"schema": "prz-x-break-pole-2024-exploratory-v1",
              "profile": PROFILE, "source_sha256": SOURCE_SHA256,
              "period": "2024-01-01T00:00:00Z/2025-01-01T00:00:00Z",
              "box_size": "100", "reversal_boxes": 3,
              "research_only": True, "execution": "OFF",
              "summary": summary,
              "decision_facts": zones.records,
              "signal_annotations": [asdict(a) for a in context.annotations],
              "future_simulated_trade_outcomes_separate": joined,
              "skipped_signals": [{**row,
                                   "context_category": annotations[row["event_id"]].category,
                                   "context_reason": annotations[row["event_id"]].reason}
                                  for row in baseline["skipped"]],
              "pending_entry_event_id": baseline["pending_entry_event_id"],
              "pending_exit_event_id": baseline["pending_exit_event_id"]}
    _atomic_new_json(output, report)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candles", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(run(args.candles, output=args.output), indent=2))


if __name__ == "__main__":
    main()

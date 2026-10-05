"""Bounded, offline PRZ annotation smoke replay on frozen research candle CSV."""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path

from research_v2.du_plessis_poles_preview import PnFEngine, PnFProfile
from research_v2.du_plessis_poles_forward_sim import simulate
from pnf_mvp.strategies.du_plessis_poles_v1 import PoleDecisionLedger
from research_v2.gartley_pole_prz import CausalPrzLedger, SCHEMA, number


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def run(candles: Path, *, expected_sha256: str, output: Path,
        max_candles: int = 10000) -> dict:
    if (type(max_candles) is not int or not 1 <= max_candles <= 10000
            or not candles.is_file() or candles.suffix.lower() != ".csv"
            or output.exists() or not output.parent.is_dir()
            or len(expected_sha256) != 64):
        raise ValueError("invalid bounded research input or existing report")
    actual = sha256(candles)
    if actual != expected_sha256.lower():
        raise ValueError("source hash mismatch")
    engine = PnFEngine(PnFProfile("prz_pole_offline_only", 100, 3))
    pole_ledger = PoleDecisionLedger()
    prz = CausalPrzLedger(box_size="100")
    count = 0
    previous_ts = None
    with candles.open(newline="", encoding="utf-8-sig") as stream:
        reader = csv.DictReader(stream)
        if not reader.fieldnames or not {"close_time", "close"} <= set(reader.fieldnames):
            raise ValueError("closed-candle CSV required")
        for row in reader:
            if count == max_candles:
                break
            ts = int(row["close_time"])
            if previous_ts is not None and ts != previous_ts + 60000:
                raise ValueError("noncontinuous 1m candles")
            previous_ts = ts
            close = row["close"]
            exact_close = number(close)
            if exact_close <= 0:
                raise ValueError("positive candle close required")
            engine.update_from_price(ts, float(exact_close))
            pole_events = tuple(e for e in pole_ledger.ingest(
                engine.columns, box_size="100", reversal_boxes=3, enabled=True)
                if e.mode == "EARLY_ENTRY" and e.action in ("LONG", "SHORT")
                and e.status == "CANDIDATE")
            prz.ingest(engine.columns, close_ts=ts, close=close, pole_events=pole_events)
            count += 1
    manifest = {"schema": SCHEMA, "research_only": True, "execution": "OFF",
                "source_path": str(candles), "source_sha256": actual,
                "box_size": "100", "reversal_boxes": 3,
                "candles_processed": count,
                "pole_signals": sum(r["type"] == "POLE_ANNOTATION" for r in prz.records),
                "annotations": {kind: sum(r["type"] == "POLE_ANNOTATION" and r["category"] == kind
                                      for r in prz.records)
                                for kind in ("PRZ_MATCH", "PRZ_AVAILABLE_NONMATCH", "PRZ_UNAVAILABLE")},
                "limitations": "bounded offline baseline replay only; annual ledger, fees and net performance unavailable"}
    # Second, independent pass through the existing untouched simulator. Outcomes
    # are deliberately segregated from the decision-time event records.
    baseline = simulate(candles, box_size=100, reversal_boxes=3,
                        max_candles=max_candles)
    if baseline["candles_processed"] != count:
        raise ValueError("baseline/annotation prefix mismatch")
    if sha256(candles) != actual:
        raise ValueError("research source changed during replay")
    observed = {r["event_id"] for r in prz.records if r["type"] == "POLE_ANNOTATION"}
    if any(t["event_id"] not in observed for t in baseline["trades"]):
        raise ValueError("baseline trade lacks decision annotation")
    payload = json.dumps({"manifest": manifest, "decision_events": prz.records,
                          "future_outcomes_separate": {"simulated_trades": baseline["trades"],
                                                       "skipped": baseline["skipped"],
                                                       "pending_entry_event_id": baseline["pending_entry_event_id"],
                                                       "pending_exit_event_id": baseline["pending_exit_event_id"]}},
                         indent=2, sort_keys=True) + "\n"
    with output.open("x", encoding="utf-8", newline="\n") as report:
        report.write(payload)
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candles", type=Path, required=True)
    parser.add_argument("--sha256", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--max-candles", type=int, default=10000)
    args = parser.parse_args()
    print(json.dumps(run(args.candles, expected_sha256=args.sha256,
                         output=args.output, max_candles=args.max_candles), indent=2))


if __name__ == "__main__":
    main()

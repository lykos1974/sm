"""Bounded, offline PRZ-tested then X-break pole-context smoke report."""
from __future__ import annotations

import argparse
import csv
import json
from dataclasses import asdict
from pathlib import Path

from research_v2.du_plessis_poles_preview import PnFEngine, PnFProfile
from pnf_mvp.strategies.du_plessis_poles_v1 import PoleDecisionLedger
from research_v2.gartley_pole_prz import CausalPrzLedger, number
from research_v2.gartley_pole_prz_smoke import sha256
from research_v2.prz_x_break_pole_context_v2 import PROFILE, XBreakPoleContext


def run(path: Path, *, expected_sha256: str, output: Path,
        max_candles: int = 10000) -> dict:
    if (type(max_candles) is not int or not 1 <= max_candles <= 10000
            or not path.is_file() or path.suffix.lower() != ".csv"
            or output.exists() or not output.parent.is_dir()
            or len(expected_sha256) != 64):
        raise ValueError("invalid bounded research input or existing output")
    actual = sha256(path)
    if actual != expected_sha256.lower():
        raise ValueError("source hash mismatch")
    engine = PnFEngine(PnFProfile(PROFILE, 100, 3))
    pole_ledger = PoleDecisionLedger()
    zones = CausalPrzLedger(box_size="100")
    context = XBreakPoleContext(enabled=True)
    previous_ts = None
    count = 0
    with path.open(newline="", encoding="utf-8-sig") as stream:
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
            close = number(row["close"])
            if close <= 0:
                raise ValueError("positive close required")
            engine.update_from_price(ts, float(close))
            pole_events = tuple(e for e in pole_ledger.ingest(
                engine.columns, box_size="100", reversal_boxes=3, enabled=True)
                if e.status == "CANDIDATE" and e.action in ("LONG", "SHORT"))
            facts = zones.ingest(engine.columns, close_ts=ts, close=close,
                                 pole_events=pole_events)
            context.ingest(facts, pole_events=pole_events, at=ts)
            count += 1
    if sha256(path) != actual:
        raise ValueError("research source changed during replay")
    manifest = {"schema": PROFILE, "research_only": True,
                "interpretation": "historical context after invalidated Gartley; not a valid Gartley reversal",
                "execution": "OFF", "source_sha256": actual,
                "box_size": "100", "reversal_boxes": 3,
                "candles_processed": count, "pole_signals": len(context.annotations),
                "categories": {kind: sum(a.category == kind for a in context.annotations)
                               for kind in ("CONTEXT_MATCH", "CONTEXT_AVAILABLE_NONMATCH",
                                            "CONTEXT_UNAVAILABLE")},
                "performance": "NOT_EVALUATED"}
    payload = json.dumps({"manifest": manifest,
                          "signal_annotations": [asdict(a) for a in context.annotations]},
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

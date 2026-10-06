"""Describe existing 2024 pole outcomes near already known projected zones.

No execution replay, filtering, trading decision, database or network access.
The projections have no audited expiry; proximity is historical description.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import tempfile
from collections import defaultdict
from decimal import Decimal
from pathlib import Path

from research_v2.du_plessis_poles_annual import (
    FIRST_CLOSE_MS, LAST_CLOSE_MS, SOURCE_MINUTES, SOURCE_SHA256,
)
from research_v2.gartley_pole_prz import number


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def compare(report: dict, zones: list[dict], closes: dict[int, str]) -> dict:
    annotations = report["signal_annotations"]
    trades = report["future_simulated_trade_outcomes_separate"]
    if not isinstance(annotations, list) or not isinstance(trades, list) or not isinstance(zones, list):
        raise ValueError("invalid research input")
    ids = [a["event_id"] for a in annotations]
    if len(ids) != len(set(ids)) or len({z["id"] for z in zones}) != len(zones):
        raise ValueError("duplicate research identity")
    parsed = []
    for z in zones:
        at = z["known_at"]
        lo, hi = number(z["lower"]), number(z["upper"])
        cols = z["pivot_columns"]
        if (z["type"] != "NONCONSECUTIVE_GARTLEY_PROJECTION"
                or z["direction"] not in ("LONG", "SHORT") or type(at) is not int or at < 0
                or lo <= 0 or hi < lo or hi-lo > 1000 or not isinstance(cols, list)
                or len(cols) != 4 or any(type(c) is not int for c in cols)
                or not all(a < b for a,b in zip(cols,cols[1:]))
                or all(a+1 == b for a,b in zip(cols,cols[1:]))):
            raise ValueError("invalid projected zone")
        parsed.append((z, lo, hi))
    by_trade = {}
    for t in trades:
        key = t["event_id"]
        if key in by_trade or key not in ids:
            raise ValueError("duplicate or orphaned trade")
        by_trade[key] = t
    categories = defaultdict(lambda: {"signals": 0, "completed": 0,
                                      "gross_sum_bps_equal_notional_proxy": Decimal(0)})
    matches = []
    for a in annotations:
        key, side, at = a["event_id"], a["direction"], a["pole_signal_at"]
        if type(at) is not int or side not in ("LONG", "SHORT") or at not in closes:
            raise ValueError("missing pole decision close")
        close = number(closes[at])
        if close <= 0:
            raise ValueError("invalid pole decision close")
        eligible = []
        for z, lo, hi in parsed:
            if z["known_at"] < at and z["direction"] == side:
                distance = max(lo-close, Decimal(0), close-hi)
                eligible.append((distance, -z["known_at"], z["id"], z))
        chosen = min(eligible) if eligible else None
        distance = chosen[0] if chosen else None
        category = ("NO_PRIOR_SAME_DIRECTION_ZONE" if distance is None else
                    "IN_ZONE" if distance == 0 else
                    "NEAR_1_COARSE_BOX" if distance <= 1000 else "FAR")
        stats = categories[category]
        stats["signals"] += 1
        t = by_trade.get(key)
        if t is not None:
            if t["direction"] != side or t["signal_ts"] != at or t["status"] != "SIMULATED_CLOSED":
                raise ValueError("inconsistent existing trade")
            entry, delta = number(t["entry_simulated_open_price"]), number(t["gross_price_delta"])
            if entry <= 0:
                raise ValueError("invalid existing outcome")
            stats["completed"] += 1
            stats["gross_sum_bps_equal_notional_proxy"] += delta*Decimal(10000)/entry
        matches.append({"event_id": key, "signal_ts": at, "direction": side,
                        "signal_close": str(close), "category": category,
                        "nearest_zone_id": chosen[2] if chosen else None,
                        "zone_known_at": chosen[3]["known_at"] if chosen else None,
                        "distance_price": str(distance) if distance is not None else None,
                        "completed": t is not None})
    return {"signals": len(annotations), "already_simulated_trades": len(trades),
            "categories": {name: {**v, "gross_sum_bps_equal_notional_proxy":
                                    str(v["gross_sum_bps_equal_notional_proxy"])}
                           for name,v in sorted(categories.items())},
            "observations": matches}


def run(report_path: Path, chart_path: Path, candles_path: Path, output: Path) -> dict:
    if (output.exists() or not output.parent.is_dir()
            or not all(p.is_file() for p in (report_path, chart_path, candles_path))):
        raise ValueError("existing inputs and new output required")
    report_hash, chart_hash = sha256(report_path), sha256(chart_path)
    if sha256(candles_path) != SOURCE_SHA256:
        raise ValueError("pinned candle hash mismatch")
    report = json.loads(report_path.read_text(encoding="utf-8"))
    if (report.get("schema") != "prz-x-break-pole-2024-exploratory-v1"
            or report.get("source_sha256") != SOURCE_SHA256
            or report.get("execution") != "OFF"
            or report.get("research_only") is not True):
        raise ValueError("incompatible existing research report")
    page = chart_path.read_text(encoding="utf-8")
    if (f"Κεριά SHA-256: {SOURCE_SHA256}" not in page
            or f"Αναφορά SHA-256: {report_hash}" not in page):
        raise ValueError("chart input provenance mismatch")
    marker = '<script id="dataset" type="application/json">'
    if page.count(marker) != 1:
        raise ValueError("missing chart dataset")
    data = json.loads(page.split(marker,1)[1].split("</script>",1)[0])
    zones = data["harmonic"]
    closes = {}
    first = last = None
    count = 0
    wanted = {a["pole_signal_at"] for a in report["signal_annotations"]}
    with candles_path.open(newline="", encoding="utf-8-sig") as stream:
        reader = csv.DictReader(stream)
        if not reader.fieldnames or not {"close_time", "close"} <= set(reader.fieldnames):
            raise ValueError("invalid candle columns")
        for row in reader:
            at = int(row["close_time"])
            if last is not None and at != last+60000:
                raise ValueError("non-contiguous candle input")
            if at in wanted:
                closes[at] = row["close"]
            first = at if first is None else first
            last = at
            count += 1
            if count > SOURCE_MINUTES:
                raise ValueError("excess candle input")
    if (count,first,last) != (SOURCE_MINUTES,FIRST_CLOSE_MS,LAST_CLOSE_MS) or sha256(candles_path) != SOURCE_SHA256:
        raise ValueError("incomplete or changed candle input")
    result = compare(report,zones,closes)
    payload = {"schema": "pole-harmonic-overlap-descriptive-v1", "research_only": True,
               "execution": "OFF", "source_sha256": SOURCE_SHA256,
               "report_sha256": report_hash, "chart_sha256": chart_hash,
               "harmonic_zones": len(zones),
               "interpretation": "Existing gross outcomes grouped by historical proximity; no zone expiry or filtered replay, no fee/fill/net claim",
               **result}
    temporary = None
    try:
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", newline="\n", dir=output.parent,
                                         prefix=".pole_overlap_", suffix=".tmp", delete=False) as stream:
            temporary = Path(stream.name)
            json.dump(payload,stream,indent=2,ensure_ascii=False)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.link(temporary,output)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    return {k:v for k,v in payload.items() if k != "observations"}


def main() -> None:
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report",type=Path,required=True)
    parser.add_argument("--chart",type=Path,required=True)
    parser.add_argument("--candles",type=Path,required=True)
    parser.add_argument("--output",type=Path,required=True)
    args=parser.parse_args()
    print(json.dumps(run(args.report,args.chart,args.candles,args.output),indent=2))


if __name__ == "__main__":
    main()

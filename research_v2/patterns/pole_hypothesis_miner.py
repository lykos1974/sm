"""Bounded offline rule discovery over independently validated causal opportunities.

This module does not generate signals, trades, outcomes or exchange requests.
It only groups previously simulated net-R observations by pre-decision facts.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
from decimal import Decimal, InvalidOperation
from pathlib import Path

SCHEMA = "pole-hypothesis-miner-v1"
FIELDS = ("opportunity_id", "decision_ts", "known_at", "symbol", "side",
          "pole_boxes", "reversal_boxes", "retrace_ratio", "relative_pole_size", "net_r")
MAX_ROWS = 1_000_000


def _decimal(value: str) -> Decimal:
    if not value or len(value) > 40 or "\x00" in value:
        raise ValueError("invalid numeric field")
    try:
        number = Decimal(value)
    except InvalidOperation as exc:
        raise ValueError("invalid numeric field") from exc
    if not number.is_finite() or abs(number.as_tuple().exponent) > 12:
        raise ValueError("invalid numeric field")
    return number


def _integer(value: str) -> int:
    if not value or len(value) > 16 or not value.isascii() or not value.isdecimal():
        raise ValueError("invalid numeric field")
    return int(value)


def _read(path: Path) -> list[dict]:
    if not path.is_file() or path.suffix.lower() != ".csv":
        raise ValueError("research CSV required")
    with path.open(newline="", encoding="utf-8-sig") as stream:
        reader = csv.DictReader(stream, restkey="EXTRA")
        if reader.fieldnames is None or tuple(reader.fieldnames) != FIELDS:
            raise ValueError("unsupported input schema")
        rows, ids, previous = [], set(), -1
        for raw in reader:
            if len(rows) >= MAX_ROWS or set(raw) != set(FIELDS) or any(v is None for v in raw.values()):
                raise ValueError("unsupported input schema")
            identity = raw["opportunity_id"]
            if not identity or len(identity) > 100 or "\x00" in identity:
                raise ValueError("invalid opportunity identity")
            if identity in ids:
                raise ValueError("duplicate opportunity")
            ids.add(identity)
            decision, known = _integer(raw["decision_ts"]), _integer(raw["known_at"])
            if decision <= previous or decision < 10**12:
                raise ValueError("invalid chronology")
            if known > decision or known < 10**12:
                raise ValueError("future feature or invalid chronology")
            previous = decision
            symbol = raw["symbol"]
            if not symbol or len(symbol) > 40 or not all(c.isascii() and (c.isalnum() or c in "_:" ) for c in symbol):
                raise ValueError("invalid symbol")
            if raw["side"] not in ("LONG", "SHORT") or raw["relative_pole_size"] not in (
                    "SMALL", "NEAR", "LARGE", "EXTREME"):
                raise ValueError("invalid causal category")
            pole, reversal = _integer(raw["pole_boxes"]), _integer(raw["reversal_boxes"])
            ratio, outcome = _decimal(raw["retrace_ratio"]), _decimal(raw["net_r"])
            if not (1 <= pole <= 10000 and 1 <= reversal <= 100 and 0 <= ratio <= 10):
                raise ValueError("invalid numeric field")
            rows.append({"id": identity, "ts": decision, "symbol": symbol,
                         "side": raw["side"], "pole": pole, "reversal": reversal,
                         "retrace": ratio, "relative": raw["relative_pole_size"],
                         "net_r": outcome})
    return rows


def _rules():
    # Frozen before reading outcomes. No free-form predicates or parameters.
    atoms = [
        ("LONG", lambda r: r["side"] == "LONG"),
        ("SHORT", lambda r: r["side"] == "SHORT"),
        ("POLE_3_4", lambda r: 3 <= r["pole"] <= 4),
        ("POLE_5_PLUS", lambda r: r["pole"] >= 5),
        ("RETRACE_LE_HALF", lambda r: r["retrace"] <= Decimal("0.5")),
        ("RETRACE_GT_HALF", lambda r: r["retrace"] > Decimal("0.5")),
        ("SIZE_NEAR", lambda r: r["relative"] == "NEAR"),
        ("SIZE_LARGE", lambda r: r["relative"] in ("LARGE", "EXTREME")),
    ]
    yield "ALL", lambda r: True
    yield from atoms
    for direction in atoms[:2]:
        for geometry in atoms[2:]:
            yield direction[0] + "__" + geometry[0], lambda r, a=direction[1], b=geometry[1]: a(r) and b(r)


def _score(rows: list[dict], rule) -> dict:
    selected = [r["net_r"] for r in rows if rule(r)]
    count = len(selected)
    total = sum(selected, Decimal(0))
    positives = sum(x > 0 for x in selected)
    return {"trades": count, "wins": positives,
            "win_rate": str(Decimal(positives) / count) if count else None,
            "net_total_R": str(total),
            "net_mean_R": str(total / count) if count else None}


def mine(source: Path, output: Path, *, min_train: int = 100,
         min_validation: int = 50, min_test: int = 50) -> dict:
    """Rank on validation only; open the final 20% once for the frozen finalist."""
    source, output = Path(source), Path(output)
    if source.resolve() == output.resolve() or output.exists():
        raise FileExistsError("new report path required")
    if not output.parent.is_dir() or any(type(v) is not int or v < 1 for v in
                                        (min_train, min_validation, min_test)):
        raise ValueError("invalid output or minimum sample")
    rows = _read(source)
    n = len(rows)
    train_end, validation_end = n * 60 // 100, n * 80 // 100
    if (train_end < min_train or validation_end - train_end < min_validation
            or n - validation_end < min_test):
        raise ValueError("insufficient chronological sample")
    train, validation, test = rows[:train_end], rows[train_end:validation_end], rows[validation_end:]
    catalogue = []
    best = None
    for rule_id, predicate in _rules():
        a, b = _score(train, predicate), _score(validation, predicate)
        eligible = a["trades"] >= min_train and b["trades"] >= min_validation
        catalogue.append({"rule_id": rule_id, "train": a, "validation": b,
                          "eligible": eligible})
        if eligible:
            rank = (Decimal(b["net_mean_R"]), b["trades"], -len(rule_id))
            if best is None or rank > best[0]:
                best = (rank, rule_id, predicate, a, b)
    if best is None:
        raise ValueError("insufficient selected observations")
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    report = {"schema": SCHEMA, "research_only": True, "execution": "OFF",
              "input_sha256": digest, "universe_count": n,
              "split": {"train": len(train), "validation": len(validation), "test": len(test),
                        "train_last_ts": train[-1]["ts"],
                        "validation_last_ts": validation[-1]["ts"]},
              "candidate_count": len(catalogue), "candidates": catalogue,
              "finalist": {"rule_id": best[1], "train": best[3], "validation": best[4],
                           "test": _score(test, best[2])},
              "limitations": "Prevalidated independent net-R outcomes required; selection on validation, test viewed once; no live permission or profitability claim"}
    with output.open("x", encoding="utf-8") as stream:
        json.dump(report, stream, indent=2, ensure_ascii=False)
        stream.write("\n")
    return report


def main():
    parser = argparse.ArgumentParser(description="Bounded offline causal pole hypothesis miner")
    parser.add_argument("--opportunities", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = mine(args.opportunities, args.output)
    print(json.dumps({"output": str(args.output), "input_sha256": report["input_sha256"],
                      "candidate_count": report["candidate_count"],
                      "finalist": report["finalist"]["rule_id"]}))


if __name__ == "__main__":
    main()

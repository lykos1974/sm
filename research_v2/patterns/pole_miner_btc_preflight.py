"""Read-only provenance and identity preflight for the frozen BTC 2024 ledgers."""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
from decimal import Decimal, InvalidOperation
from pathlib import Path

PINNED_HASHES = (
    "d4df0846a8aef4e972f6ffa2e1b92b3c3cff59fdd5f6e97287b30824027ae040",
    "fd7d1400311c87aaa42502f6711d73355aedf3babd7e420b89107e6387e65149",
    "8b56eb214075d9d95d0f7d328a82c0a414ae25b2946cffe1f953e502059326f9",
    "f6a0373b962a3f8c40b513f6a2e3c128b048d947eaa7fb0a239528f382418580",
)
PINNED_COUNTS = (468, 431)
DECISION_HEADER = ("decision_id", "pole_column_index", "reversal_column_index",
                   "confirmation_column_index", "decision_known_at_ms",
                   "entry_candle_open_ms", "entry_candle_close_ms", "direction",
                   "entry_price", "stop_price")
TRADE_REQUIRED = ("trade_id", "opportunity_id", "symbol", "direction",
                  "entry_timestamp", "exit_timestamp", "result_R")


def _sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for part in iter(lambda: f.read(1024 * 1024), b""):
            h.update(part)
    return h.hexdigest()


def _json(path: Path) -> dict:
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate manifest field")
            result[key] = value
        return result
    obj = json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=unique,
                     parse_float=Decimal)
    if type(obj) is not dict:
        raise ValueError("invalid manifest")
    return obj


def _rows(path: Path, required: tuple[str, ...], exact: bool) -> list[dict]:
    with path.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f, restkey="EXTRA")
        names = tuple(reader.fieldnames or ())
        if len(names) != len(set(names)) or (names != required if exact else
                                            not set(required).issubset(names)):
            raise ValueError("unexpected ledger schema")
        rows = list(reader)
    if any(None in row or "EXTRA" in row or any(v is None for v in row.values())
           for row in rows):
        raise ValueError("malformed ledger row")
    return rows


def _int(value: str) -> int:
    if not value or len(value) > 16 or not value.isascii() or not value.isdecimal():
        raise ValueError("invalid timestamp")
    return int(value)


def _num(value) -> Decimal:
    if isinstance(value, bool) or value is None or len(str(value)) > 50:
        raise ValueError("invalid numeric evidence")
    try:
        n = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError("invalid numeric evidence") from exc
    if not n.is_finite():
        raise ValueError("invalid numeric evidence")
    return n


def inspect(decisions: Path, causal: Path, trades: Path, portfolio: Path,
            output: Path, *, expected_hashes=PINNED_HASHES,
            expected_counts=PINNED_COUNTS) -> dict:
    paths = tuple(Path(p) for p in (decisions, causal, trades, portfolio))
    output = Path(output)
    if output.exists() or any(output.resolve() == p.resolve() for p in paths):
        raise FileExistsError("new report path required")
    if not output.parent.is_dir() or any(not p.is_file() for p in paths):
        raise ValueError("existing research files and output parent required")
    hashes = tuple(_sha(p) for p in paths)
    if hashes != tuple(h.lower() for h in expected_hashes):
        raise ValueError("source hash mismatch")
    cm, pm = _json(paths[1]), _json(paths[3])
    decision_rows = _rows(paths[0], DECISION_HEADER, True)
    trade_rows = _rows(paths[2], TRADE_REQUIRED, False)
    if ((len(decision_rows), len(trade_rows)) != tuple(expected_counts)
            or cm.get("stage") != "causal_long_pole_research_v1"
            or cm.get("research_only") is not True or cm.get("symbol") != "BTCUSDT"
            or cm.get("decision_count") != len(decision_rows)
            or cm.get("resolved_portfolio_trades") != len(trade_rows)
            or pm.get("resolved_portfolio_trades") != len(trade_rows)
            or cm.get("causal_decisions_sha256") != hashes[0]):
        raise ValueError("manifest/count identity mismatch")
    for ordinal, row in enumerate(decision_rows, 1):
        if row["decision_id"] != f"DEC-{ordinal:06d}" or row["direction"] != "LONG":
            raise ValueError("decision identity mismatch")
        known, opened, closed = (_int(row[k]) for k in ("decision_known_at_ms",
                                  "entry_candle_open_ms", "entry_candle_close_ms"))
        entry, stop = _num(row["entry_price"]), _num(row["stop_price"])
        if not (10**12 <= known < opened <= closed and entry > stop > 0):
            raise ValueError("decision chronology or geometry mismatch")
    seen_trades, seen_opportunities, gross = set(), set(), Decimal(0)
    for row in trade_rows:
        trade_id, opportunity_id = row["trade_id"], row["opportunity_id"]
        if trade_id in seen_trades or opportunity_id in seen_opportunities:
            raise ValueError("duplicate trade or opportunity")
        seen_trades.add(trade_id)
        seen_opportunities.add(opportunity_id)
        if not opportunity_id.startswith("OPP-") or not opportunity_id[4:].isdigit():
            raise ValueError("opportunity identity mismatch")
        ordinal = int(opportunity_id[4:])
        if (ordinal < 1 or ordinal > len(decision_rows)
                or opportunity_id != f"OPP-{ordinal:06d}" or row["symbol"] != "BTC"
                or row["direction"] != "LONG"):
            raise ValueError("trade identity mismatch")
        decision = decision_rows[ordinal - 1]
        entered, exited = _int(row["entry_timestamp"]), _int(row["exit_timestamp"])
        if (entered < _int(decision["entry_candle_open_ms"]) or exited <= entered):
            raise ValueError("trade chronology mismatch")
        gross += _num(row["result_R"])
    if (abs(gross - _num(cm.get("gross_total_R"))) > Decimal("0.000001")
            or abs(gross - _num(pm.get("summary_metrics", {}).get("total_R")))
            > Decimal("0.000001")):
        raise ValueError("gross result mismatch")
    report = {"schema": "pole-miner-btc-preflight-v1", "status": "GROSS_ONLY_NOT_MINER_INPUT",
              "source_sha256": dict(zip(("decisions", "causal_manifest", "trades",
                                         "portfolio_manifest"), hashes)),
              "decisions": len(decision_rows), "linked_trades": len(trade_rows),
              "gross_total_R": str(gross), "execution": "OFF",
              "limitations": "No exchange fills or audited net R; historical BTC 2024 was previously inspected; no untouched test claim"}
    with output.open("x", encoding="utf-8") as f:
        json.dump(report, f, indent=2, sort_keys=True)
        f.write("\n")
    return report


def main():
    p = argparse.ArgumentParser(description=__doc__)
    for name in ("decisions", "causal", "trades", "portfolio", "output"):
        p.add_argument("--" + name, type=Path, required=True)
    args = p.parse_args()
    print(json.dumps(inspect(args.decisions, args.causal, args.trades,
                             args.portfolio, args.output), indent=2))


if __name__ == "__main__":
    main()

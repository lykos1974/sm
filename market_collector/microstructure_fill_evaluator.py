"""Read-only causal limit-fill evaluator for verified microstructure archives."""

from __future__ import annotations

import argparse
import csv
import json
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path

import pyarrow.parquet as pq

from verify_microstructure_archive import verify


@dataclass(frozen=True)
class Order:
    order_id: str
    symbol: str
    side: str
    available_wall_ns: int
    expires_wall_ns: int
    ideal_entry: Decimal
    tick_size: Decimal


@dataclass(frozen=True)
class ArchiveEvidence:
    path: Path
    manifest: dict
    manifest_sha256: str
    sessions: tuple[dict, ...]


REQUIRED_COLUMNS = {
    "order_id", "symbol", "side", "available_wall_ns", "expires_wall_ns",
    "ideal_entry", "tick_size",
}


def _decimal(value: str, field: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"invalid {field}: {value!r}") from exc
    if not result.is_finite():
        raise ValueError(f"invalid {field}: {value!r}")
    return result


def load_orders(path: str | Path) -> list[Order]:
    with Path(path).open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        missing = REQUIRED_COLUMNS - set(reader.fieldnames or ())
        if missing:
            raise ValueError(f"missing order columns: {sorted(missing)}")
        orders, seen = [], set()
        for number, row in enumerate(reader, start=2):
            order_id = str(row["order_id"] or "").strip()
            if not order_id or order_id in seen:
                raise ValueError(f"blank or duplicate order_id at row {number}")
            seen.add(order_id)
            side = str(row["side"] or "").upper()
            if side not in {"LONG", "SHORT"}:
                raise ValueError(f"invalid side at row {number}")
            available, expires = int(row["available_wall_ns"]), int(row["expires_wall_ns"])
            if available >= expires:
                raise ValueError(f"non-positive order lifetime at row {number}")
            entry, tick = _decimal(row["ideal_entry"], "ideal_entry"), _decimal(row["tick_size"], "tick_size")
            if entry <= 0 or tick <= 0:
                raise ValueError(f"entry and tick must be positive at row {number}")
            orders.append(Order(
                order_id, str(row["symbol"] or "").upper(), side,
                available, expires, entry, tick,
            ))
    return orders


def _rows(path: Path, columns: list[str], filters=None) -> list[dict]:
    return pq.read_table(path, columns=columns, filters=filters).to_pylist()


def _discover_archives(path: Path) -> list[Path]:
    if (path / "manifest.json").is_file():
        return [path]
    archives = sorted({item.parent for item in path.rglob("manifest.json")})
    if not archives:
        raise FileNotFoundError(f"no sealed archives below {path}")
    return archives


def _load_archive(path: Path) -> ArchiveEvidence:
    sealed = (path / "manifest.sha256").read_text(encoding="ascii").split()[0]
    verified = verify(path, sealed)
    if not verified["manifest_sealed"]:
        raise RuntimeError(f"unsealed archive: {path}")
    manifest = json.loads((path / "manifest.json").read_text(encoding="utf-8"))
    sessions = tuple(sorted(_rows(
        path / "sessions.parquet",
        ["session_key", "symbol", "started_wall_ns", "ended_wall_ns", "status"],
    ), key=lambda row: int(row["started_wall_ns"])))
    if not sessions or any(
        row["ended_wall_ns"] is None or str(row["status"]).upper() != "CLOSED"
        for row in sessions
    ):
        raise RuntimeError(f"archive has missing or open session evidence: {path}")
    return ArchiveEvidence(path, manifest, sealed, sessions)


def _overlaps(start: int, end: int, window_start: int, window_end: int) -> bool:
    return end >= window_start and start <= window_end


def _base(order: Order) -> dict[str, object]:
    return {
        "order_id": order.order_id, "symbol": order.symbol, "side": order.side,
        "available_wall_ns": order.available_wall_ns,
        "expires_wall_ns": order.expires_wall_ns,
        "ideal_entry": str(order.ideal_entry), "tick_size": str(order.tick_size),
        "fill_model": "aggressor-confirmed trade-through by one tick",
    }


def evaluate_order(order: Order, archive: Path, manifest: dict) -> dict[str, object]:
    base = _base(order)
    if order.symbol != str(manifest["symbol"]).upper():
        return {**base, "classification": "OUTSIDE_EVIDENCE", "reason": "symbol mismatch"}
    if (order.available_wall_ns < int(manifest["first_receive_wall_ns"]) or
            order.expires_wall_ns > int(manifest["last_receive_wall_ns"])):
        return {**base, "classification": "OUTSIDE_EVIDENCE", "reason": "order window not fully covered"}

    intervals = _rows(
        archive / "quality_intervals.parquet",
        ["start_wall_ns", "end_wall_ns", "reason"],
        filters=[
            ("end_wall_ns", ">=", order.available_wall_ns),
            ("start_wall_ns", "<=", order.expires_wall_ns),
        ],
    )
    overlaps = [row for row in intervals if (
        int(row["end_wall_ns"]) >= order.available_wall_ns and
        int(row["start_wall_ns"]) <= order.expires_wall_ns
    )]
    if overlaps:
        return {
            **base, "classification": "INDETERMINATE_QUALITY_OVERLAP",
            "reason": "order lifetime overlaps recorded uncertain evidence",
            "overlap_count": len(overlaps),
        }

    threshold = (
        order.ideal_entry - order.tick_size
        if order.side == "LONG" else order.ideal_entry + order.tick_size
    )
    qualifying, touched = [], []
    for row in _rows(
        archive / "agg_trades.parquet",
        ["agg_trade_id", "receive_wall_ns", "price", "quantity", "buyer_is_maker"],
        filters=[
            ("receive_wall_ns", ">=", order.available_wall_ns),
            ("receive_wall_ns", "<=", order.expires_wall_ns),
        ],
    ):
        received = int(row["receive_wall_ns"])
        price = Decimal(str(row["price"]))
        aggressive_opposite = (
            bool(row["buyer_is_maker"]) if order.side == "LONG"
            else not bool(row["buyer_is_maker"])
        )
        reached = price <= order.ideal_entry if order.side == "LONG" else price >= order.ideal_entry
        through = price <= threshold if order.side == "LONG" else price >= threshold
        event = {
            "receive_wall_ns": received, "agg_trade_id": int(row["agg_trade_id"]),
            "price": str(price), "quantity": str(row["quantity"]),
        }
        if reached:
            touched.append(event)
        if through and aggressive_opposite:
            qualifying.append(event)
    if qualifying:
        event = min(qualifying, key=lambda item: (item["receive_wall_ns"], item["agg_trade_id"]))
        return {**base, "classification": "FILLED_TRADE_THROUGH", "fill_evidence": event}
    if touched:
        event = min(touched, key=lambda item: (item["receive_wall_ns"], item["agg_trade_id"]))
        return {**base, "classification": "TOUCHED_NO_FILL_PROOF", "first_touch": event}
    return {**base, "classification": "NOT_REACHED", "reason": "no trade reached ideal_entry"}


def evaluate_order_across(order: Order, archives: list[ArchiveEvidence]) -> dict[str, object]:
    """Evaluate only after proving continuous, non-overlapping collector observation."""
    base = _base(order)
    same_symbol = [item for item in archives
                   if str(item.manifest["symbol"]).upper() == order.symbol]
    relevant = [item for item in same_symbol if any(_overlaps(
        int(row["started_wall_ns"]), int(row["ended_wall_ns"]),
        order.available_wall_ns, order.expires_wall_ns,
    ) for row in item.sessions)]
    if not relevant:
        return {**base, "classification": "OUTSIDE_EVIDENCE", "reason": "no archive covers order window"}

    sessions: list[tuple[int, int, str, Path]] = []
    for item in relevant:
        for row in item.sessions:
            start, end = int(row["started_wall_ns"]), int(row["ended_wall_ns"])
            if _overlaps(start, end, order.available_wall_ns, order.expires_wall_ns):
                sessions.append((start, end, str(row["session_key"]), item.path))
    sessions.sort(key=lambda value: (value[0], value[1], value[2]))
    if not sessions or sessions[0][0] > order.available_wall_ns or sessions[-1][1] < order.expires_wall_ns:
        return {**base, "classification": "OUTSIDE_EVIDENCE",
                "reason": "order window not fully enclosed by collector sessions"}

    covered_until = sessions[0][1]
    for start, end, session_key, path in sessions[1:]:
        if start > covered_until:
            return {
                **base, "classification": "INDETERMINATE_OBSERVATION_GAP",
                "reason": "order lifetime crosses a collector session gap",
                "gap_start_wall_ns": covered_until, "gap_end_wall_ns": start,
                "next_session_key": session_key, "next_archive": str(path),
            }
        if start < covered_until:
            return {
                **base, "classification": "INDETERMINATE_ARCHIVE_OVERLAP",
                "reason": "overlapping collector sessions could duplicate evidence",
                "overlap_start_wall_ns": start,
                "overlap_end_wall_ns": min(covered_until, end),
            }
        covered_until = max(covered_until, end)

    quality_overlaps = []
    trades = []
    for item in relevant:
        quality_overlaps.extend(_rows(
            item.path / "quality_intervals.parquet",
            ["start_wall_ns", "end_wall_ns", "reason"],
            filters=[
                ("end_wall_ns", ">=", order.available_wall_ns),
                ("start_wall_ns", "<=", order.expires_wall_ns),
            ],
        ))
        trades.extend(_rows(
            item.path / "agg_trades.parquet",
            ["agg_trade_id", "receive_wall_ns", "price", "quantity", "buyer_is_maker"],
            filters=[
                ("receive_wall_ns", ">=", order.available_wall_ns),
                ("receive_wall_ns", "<=", order.expires_wall_ns),
            ],
        ))
    quality_overlaps = [row for row in quality_overlaps if _overlaps(
        int(row["start_wall_ns"]), int(row["end_wall_ns"]),
        order.available_wall_ns, order.expires_wall_ns,
    )]
    if quality_overlaps:
        return {**base, "classification": "INDETERMINATE_QUALITY_OVERLAP",
                "reason": "order lifetime overlaps recorded uncertain evidence",
                "overlap_count": len(quality_overlaps)}

    threshold = order.ideal_entry - order.tick_size if order.side == "LONG" else order.ideal_entry + order.tick_size
    qualifying, touched = [], []
    seen_events = set()
    for row in trades:
        event_key = (int(row["receive_wall_ns"]), int(row["agg_trade_id"]))
        if event_key in seen_events:
            return {**base, "classification": "INDETERMINATE_ARCHIVE_OVERLAP",
                    "reason": "duplicate trade evidence across archives"}
        seen_events.add(event_key)
        price = Decimal(str(row["price"]))
        aggressive_opposite = bool(row["buyer_is_maker"]) if order.side == "LONG" else not bool(row["buyer_is_maker"])
        reached = price <= order.ideal_entry if order.side == "LONG" else price >= order.ideal_entry
        through = price <= threshold if order.side == "LONG" else price >= threshold
        event = {"receive_wall_ns": event_key[0], "agg_trade_id": event_key[1],
                 "price": str(price), "quantity": str(row["quantity"])}
        if reached:
            touched.append(event)
        if through and aggressive_opposite:
            qualifying.append(event)
    if qualifying:
        return {**base, "classification": "FILLED_TRADE_THROUGH",
                "fill_evidence": min(qualifying, key=lambda item: (item["receive_wall_ns"], item["agg_trade_id"]))}
    if touched:
        return {**base, "classification": "TOUCHED_NO_FILL_PROOF",
                "first_touch": min(touched, key=lambda item: (item["receive_wall_ns"], item["agg_trade_id"]))}
    return {**base, "classification": "NOT_REACHED", "reason": "no trade reached ideal_entry"}


def evaluate(archive_path: str | Path, orders_path: str | Path) -> dict[str, object]:
    archive_root = Path(archive_path).resolve()
    archives = [_load_archive(path) for path in _discover_archives(archive_root)]
    results = [evaluate_order_across(order, archives) for order in load_orders(orders_path)]
    counts: dict[str, int] = {}
    for result in results:
        key = str(result["classification"])
        counts[key] = counts.get(key, 0) + 1
    return {
        "archive_root": str(archive_root),
        "archives_verified": len(archives),
        "manifest_sha256": [item.manifest_sha256 for item in archives],
        "causality_rule": "events before available_wall_ns are never eligible",
        "quality_rule": "any order overlapping uncertainty or an observation gap is indeterminate",
        "counts": counts, "orders": results,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("archive")
    parser.add_argument("orders_csv")
    parser.add_argument("--output")
    args = parser.parse_args()
    try:
        report = evaluate(args.archive, args.orders_csv)
    except (FileNotFoundError, KeyError, ValueError, RuntimeError) as exc:
        print(f"FILL_EVALUATION_FAIL {type(exc).__name__}: {exc}")
        return 1
    rendered = json.dumps(report, indent=2, ensure_ascii=False) + "\n"
    if args.output:
        Path(args.output).write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    print("FILL_EVALUATION_PASS read_only=true archive_verified=true")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

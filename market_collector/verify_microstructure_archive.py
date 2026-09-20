"""Read-only integrity verification and direct query of a Parquet evidence archive."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

try:
    import pyarrow.parquet as pq
except ImportError as exc:  # pragma: no cover
    raise SystemExit(
        "Missing dependency. Run: python -m pip install -r requirements-archive.txt"
    ) from exc


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _update_hash(digest, rows) -> None:
    for row in rows:
        digest.update(json.dumps(row, separators=(",", ":"), ensure_ascii=False).encode())
        digest.update(b"\n")


def _percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * fraction
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    weight = position - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def verify(path: str | Path, expected_manifest_sha: str | None = None) -> dict[str, object]:
    archive = Path(path).resolve()
    manifest_path = archive / "manifest.json"
    if not manifest_path.is_file():
        raise FileNotFoundError(manifest_path)
    manifest_sha = _file_sha256(manifest_path)
    sidecar = archive / "manifest.sha256"
    sealed_sha = None
    if sidecar.is_file():
        sealed_sha = sidecar.read_text(encoding="ascii").split()[0].lower()
        if sealed_sha != manifest_sha:
            raise RuntimeError("manifest sidecar hash mismatch")
    if expected_manifest_sha and expected_manifest_sha.lower() != manifest_sha:
        raise RuntimeError("expected manifest hash mismatch")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    verified = {}
    latest_quote = None
    latest_trade = None
    latencies_by_session: dict[str, list[float]] = {}
    for name, metadata in manifest["tables"].items():
        parquet_path = archive / f"{name}.parquet"
        if _file_sha256(parquet_path) != metadata["file_sha256"]:
            raise RuntimeError(f"file hash mismatch: {name}")
        columns = metadata["columns"]
        digest = hashlib.sha256()
        rows = 0
        parquet = pq.ParquetFile(parquet_path)
        for batch in parquet.iter_batches(batch_size=100_000, columns=columns):
            values = batch.to_pydict()
            tuples = [
                tuple(values[column][index] for column in columns)
                for index in range(batch.num_rows)
            ]
            _update_hash(digest, tuples)
            rows += batch.num_rows
            if name == "book_ticker":
                for row in tuples:
                    candidate = {
                        "receive_wall_ns": row[3], "session_key": row[1],
                        "bid_price": row[5], "bid_qty": row[6],
                        "ask_price": row[7], "ask_qty": row[8],
                    }
                    if latest_quote is None or candidate["receive_wall_ns"] > latest_quote["receive_wall_ns"]:
                        latest_quote = candidate
            elif name == "agg_trades":
                for row in tuples:
                    latency = row[9] / 1_000_000 - row[2]
                    latencies_by_session.setdefault(row[1], []).append(latency)
                    candidate = {
                        "receive_wall_ns": row[9], "session_key": row[1],
                        "agg_trade_id": row[0], "price": row[6], "quantity": row[7],
                    }
                    if latest_trade is None or candidate["receive_wall_ns"] > latest_trade["receive_wall_ns"]:
                        latest_trade = candidate
        if rows != metadata["rows"] or digest.hexdigest() != metadata["evidence_sha256"]:
            raise RuntimeError(f"evidence mismatch: {name}")
        verified[name] = {"rows": rows, "evidence_sha256": digest.hexdigest()}
    latency = {
        session: {
            "count": len(values), "p50_ms": _percentile(values, 0.50),
            "p95_ms": _percentile(values, 0.95), "p99_ms": _percentile(values, 0.99),
            "max_ms": _percentile(values, 1),
        }
        for session, values in latencies_by_session.items()
    }
    return {
        "archive": str(archive), "manifest_sha256": manifest_sha,
        "manifest_sealed": sealed_sha is not None,
        "expected_manifest_verified": expected_manifest_sha is not None,
        "symbol": manifest["symbol"], "utc_date": manifest["utc_date"],
        "verified_tables": verified, "latest_quote": latest_quote,
        "latest_trade": latest_trade, "trade_latency_by_session": latency,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("archive")
    parser.add_argument("--expected-manifest-sha")
    args = parser.parse_args()
    try:
        report = verify(args.archive, args.expected_manifest_sha)
    except (FileNotFoundError, KeyError, ValueError, RuntimeError) as exc:
        print(f"VERIFY_FAIL {type(exc).__name__}: {exc}")
        return 1
    print(json.dumps(report, indent=2, ensure_ascii=False))
    print("VERIFY_PASS parquet_direct_read=true hashes_verified=true")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

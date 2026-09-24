"""Read-only generator for authoritative validation tick-provenance snapshots."""

from __future__ import annotations

import argparse
import hashlib
import json
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Callable, Iterable


SCHEMA = "strategy-validation-tick-provenance-snapshot-v1"
BINANCE_SOURCE = "https://api.binance.com/api/v3/exchangeInfo"
MEXC_SOURCE = "https://contract.mexc.com/api/v1/contract/detail"
DEFAULT_SETTINGS = Path(__file__).with_name("settings.json")
DEFAULT_OUTPUT = Path(__file__).with_name("data") / "tick_provenance" / "strategy_validation_tick_provenance.json"


class SnapshotError(ValueError):
    pass


def canonical_json_bytes(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, indent=2, ensure_ascii=False) + "\n").encode("utf-8")


def _positive_decimal_text(value: Any, context: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise SnapshotError(f"{context} requires an exact string tick size")
    text = value.strip()
    try:
        tick = Decimal(text)
    except InvalidOperation as exc:
        raise SnapshotError(f"{context} has invalid tick size {text!r}") from exc
    if not tick.is_finite() or tick <= 0:
        raise SnapshotError(f"{context} requires a finite positive tick size")
    return text


def _configured_identity(symbol: str) -> dict[str, str]:
    if not isinstance(symbol, str) or not symbol or symbol != symbol.strip().upper():
        raise SnapshotError(f"invalid configured symbol {symbol!r}")
    if ":" not in symbol:
        if not symbol.endswith("USDT") or len(symbol) <= 4:
            raise SnapshotError(f"unknown configured symbol {symbol}")
        return {
            "provider": "BINANCE",
            "venue": "BINANCE_SPOT",
            "instrument_type": "SPOT",
            "native_symbol": symbol,
            "source_symbol": symbol,
        }
    namespace, contract = symbol.split(":", 1)
    if namespace != "MEXC_FUT" or not contract.endswith("USDT") or len(contract) <= 4:
        raise SnapshotError(f"unknown configured symbol {symbol}")
    native = f"{contract[:-4]}_USDT"
    return {
        "provider": "MEXC",
        "venue": "MEXC_FUTURES",
        "instrument_type": "PERPETUAL",
        "native_symbol": native,
        "source_symbol": symbol,
    }


def _extract_binance(payload: Any, expected: set[str], captured_at: str) -> list[dict[str, str]]:
    rows = payload.get("symbols") if isinstance(payload, dict) else None
    if not isinstance(rows, list):
        raise SnapshotError("Binance exchangeInfo missing symbols array")
    found: dict[str, dict[str, str]] = {}
    for row in rows:
        if not isinstance(row, dict):
            raise SnapshotError("Binance exchangeInfo contains an invalid symbol row")
        native = str(row.get("symbol") or "")
        if native not in expected:
            raise SnapshotError(f"Binance exchangeInfo returned unknown symbol {native!r}")
        if native in found:
            raise SnapshotError(f"Binance exchangeInfo returned duplicate symbol {native}")
        if row.get("status") != "TRADING" or row.get("quoteAsset") != "USDT":
            raise SnapshotError(f"Binance symbol {native} is incomplete or not tradable")
        filters = row.get("filters")
        if not isinstance(filters, list):
            raise SnapshotError(f"Binance symbol {native} missing filters")
        price_filters = [item for item in filters if isinstance(item, dict) and item.get("filterType") == "PRICE_FILTER"]
        if len(price_filters) != 1:
            raise SnapshotError(f"Binance symbol {native} requires one PRICE_FILTER")
        identity = _configured_identity(native)
        found[native] = {
            **identity,
            "tick_size": _positive_decimal_text(price_filters[0].get("tickSize"), f"Binance {native}"),
            "metadata_source": BINANCE_SOURCE,
            "provenance_timestamp": captured_at,
            "provenance_version": "binance-spot-exchange-info-v3",
        }
    missing = expected - set(found)
    if missing:
        raise SnapshotError("Binance exchangeInfo missing symbols: " + ", ".join(sorted(missing)))
    return list(found.values())


def _extract_mexc(payload: Any, expected_native: str, source_symbol: str, captured_at: str) -> dict[str, str]:
    if not isinstance(payload, dict) or payload.get("success") is not True:
        raise SnapshotError(f"MEXC contract metadata request failed for {source_symbol}")
    rows = payload.get("data")
    if isinstance(rows, dict):
        rows = [rows]
    if not isinstance(rows, list) or len(rows) != 1 or not isinstance(rows[0], dict):
        raise SnapshotError(f"MEXC metadata for {source_symbol} must contain exactly one row")
    row = rows[0]
    native = str(row.get("symbol") or "")
    if native != expected_native:
        raise SnapshotError(f"MEXC returned unknown symbol {native!r} for {source_symbol}")
    if row.get("state") != 0 or row.get("quoteCoin") != "USDT":
        raise SnapshotError(f"MEXC symbol {native} is incomplete or not tradable")
    identity = _configured_identity(source_symbol)
    return {
        **identity,
        "tick_size": _positive_decimal_text(row.get("priceUnit"), f"MEXC {native}"),
        "metadata_source": MEXC_SOURCE,
        "provenance_timestamp": captured_at,
        "provenance_version": "mexc-futures-contract-detail-v1",
    }


def fetch_official_json(provider: str, native_symbols: list[str]) -> Any:
    if provider == "BINANCE":
        query = urllib.parse.urlencode(
            {"symbols": json.dumps(sorted(native_symbols), separators=(",", ":"))}
        )
        url = f"{BINANCE_SOURCE}?{query}"
    elif provider == "MEXC" and len(native_symbols) == 1:
        url = f"{MEXC_SOURCE}?{urllib.parse.urlencode({'symbol': native_symbols[0]})}"
    else:
        raise SnapshotError(f"unsupported metadata request provider={provider}")
    request = urllib.request.Request(url, headers={"User-Agent": "pnf-tick-provenance-snapshot/1"})
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except Exception as exc:
        raise SnapshotError(f"official metadata request failed: {url}: {exc}") from exc
    return payload


def build_snapshot(
    configured_symbols: Iterable[str],
    *,
    fetch_json: Callable[[str, list[str]], Any] = fetch_official_json,
    captured_at: str | None = None,
) -> dict[str, Any]:
    symbols = list(configured_symbols)
    if len(symbols) != len(set(symbols)):
        raise SnapshotError("configured symbols contain duplicates")
    identities = [_configured_identity(symbol) for symbol in symbols]
    captured_at = captured_at or datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    binance_native = sorted(row["native_symbol"] for row in identities if row["provider"] == "BINANCE")
    entries: list[dict[str, str]] = []
    if binance_native:
        entries.extend(_extract_binance(fetch_json("BINANCE", binance_native), set(binance_native), captured_at))
    for identity in sorted((row for row in identities if row["provider"] == "MEXC"), key=lambda row: row["source_symbol"]):
        entries.append(
            _extract_mexc(
                fetch_json("MEXC", [identity["native_symbol"]]),
                identity["native_symbol"],
                identity["source_symbol"],
                captured_at,
            )
        )
    entries.sort(key=lambda row: row["source_symbol"])
    if len(entries) != len(symbols):
        raise SnapshotError("snapshot row count does not match configured symbols")
    return {"schema": SCHEMA, "captured_at": captured_at, "symbols": entries}


def _write_exclusive(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("xb") as handle:
            handle.write(payload)
    except FileExistsError:
        if path.read_bytes() != payload:
            raise SnapshotError(f"refusing to overwrite existing artifact {path}")


def _changed_symbols(accepted: dict[str, Any], candidate: dict[str, Any]) -> list[str]:
    old = {row["source_symbol"]: row for row in accepted.get("symbols", [])}
    new = {row["source_symbol"]: row for row in candidate.get("symbols", [])}
    return sorted(symbol for symbol in old.keys() | new.keys() if old.get(symbol) != new.get(symbol))


def write_snapshot(snapshot: dict[str, Any], output_path: str | Path) -> dict[str, Any]:
    output = Path(output_path)
    payload = canonical_json_bytes(snapshot)
    digest = hashlib.sha256(payload).hexdigest()
    sha_path = output.with_suffix(output.suffix + ".sha256")
    if not output.exists():
        _write_exclusive(output, payload)
        _write_exclusive(sha_path, f"{digest}  {output.name}\n".encode("ascii"))
        return {"status": "CREATED", "path": str(output), "sha256": digest}
    accepted_payload = output.read_bytes()
    accepted_digest = hashlib.sha256(accepted_payload).hexdigest()
    if accepted_payload == payload:
        return {"status": "UNCHANGED", "path": str(output), "sha256": digest}
    try:
        accepted = json.loads(accepted_payload.decode("utf-8"))
    except Exception as exc:
        raise SnapshotError(f"accepted snapshot is not valid JSON: {output}") from exc
    candidate_path = output.with_name(f"{output.stem}.candidate.{digest[:12]}{output.suffix}")
    diff_path = output.with_name(f"{output.stem}.diff.{digest[:12]}{output.suffix}")
    diff = {
        "schema": "strategy-validation-tick-provenance-diff-v1",
        "accepted_path": str(output),
        "accepted_sha256": accepted_digest,
        "candidate_path": str(candidate_path),
        "candidate_sha256": digest,
        "changed_symbols": _changed_symbols(accepted, snapshot),
    }
    _write_exclusive(candidate_path, payload)
    _write_exclusive(diff_path, canonical_json_bytes(diff))
    return {
        "status": "CHANGED",
        "path": str(output),
        "sha256": digest,
        "candidate_path": str(candidate_path),
        "diff_path": str(diff_path),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--settings", default=str(DEFAULT_SETTINGS))
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    args = parser.parse_args()
    settings = json.loads(Path(args.settings).read_text(encoding="utf-8"))
    symbols = settings.get("symbols")
    if not isinstance(symbols, list) or not symbols:
        raise SnapshotError("settings symbols must be a non-empty array")
    result = write_snapshot(build_snapshot(symbols), args.output)
    print(json.dumps(result, sort_keys=True))
    return 0 if result["status"] in {"CREATED", "UNCHANGED"} else 2


if __name__ == "__main__":
    raise SystemExit(main())

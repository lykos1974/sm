"""Read-only preflight for the accepted validation tick-provenance snapshot."""

from __future__ import annotations

import argparse
import hashlib
import json
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any


SNAPSHOT_SCHEMA = "strategy-validation-tick-provenance-snapshot-v1"
ACCEPTED_SNAPSHOT_SHA256 = "8ad27ceb2d89d2e9ab57b954240189982fdbaa5ceb02f99d474f540ea2dfa562"
DEFAULT_SETTINGS = Path(__file__).with_name("settings.json")
DEFAULT_SNAPSHOT = (
    Path(__file__).with_name("data")
    / "tick_provenance"
    / "strategy_validation_tick_provenance.json"
)

IDENTITY_FIELDS = (
    "provider",
    "venue",
    "instrument_type",
    "native_symbol",
    "source_symbol",
)
PROVENANCE_FIELDS = IDENTITY_FIELDS + (
    "tick_size",
    "metadata_source",
    "provenance_timestamp",
    "provenance_version",
)


def _identity(
    provider: str,
    venue: str,
    instrument_type: str,
    native_symbol: str,
    source_symbol: str,
    tick_size: str,
) -> dict[str, str]:
    return {
        "provider": provider,
        "venue": venue,
        "instrument_type": instrument_type,
        "native_symbol": native_symbol,
        "source_symbol": source_symbol,
        "tick_size": tick_size,
    }


ACCEPTED_IDENTITIES = {
    "BTCUSDT": _identity("BINANCE", "BINANCE_SPOT", "SPOT", "BTCUSDT", "BTCUSDT", "0.01"),
    "ETHUSDT": _identity("BINANCE", "BINANCE_SPOT", "SPOT", "ETHUSDT", "ETHUSDT", "0.01"),
    "BNBUSDT": _identity("BINANCE", "BINANCE_SPOT", "SPOT", "BNBUSDT", "BNBUSDT", "0.01"),
    "SOLUSDT": _identity("BINANCE", "BINANCE_SPOT", "SPOT", "SOLUSDT", "SOLUSDT", "0.01"),
    "XRPUSDT": _identity("BINANCE", "BINANCE_SPOT", "SPOT", "XRPUSDT", "XRPUSDT", "0.0001"),
    "MEXC_FUT:SUIUSDT": _identity("MEXC", "MEXC_FUTURES", "PERPETUAL", "SUI_USDT", "MEXC_FUT:SUIUSDT", "0.0001"),
    "MEXC_FUT:TAOUSDT": _identity("MEXC", "MEXC_FUTURES", "PERPETUAL", "TAO_USDT", "MEXC_FUT:TAOUSDT", "0.01"),
    "MEXC_FUT:HYPEUSDT": _identity("MEXC", "MEXC_FUTURES", "PERPETUAL", "HYPE_USDT", "MEXC_FUT:HYPEUSDT", "0.001"),
    "MEXC_FUT:BTCUSDT": _identity("MEXC", "MEXC_FUTURES", "PERPETUAL", "BTC_USDT", "MEXC_FUT:BTCUSDT", "0.1"),
    "MEXC_FUT:ETHUSDT": _identity("MEXC", "MEXC_FUTURES", "PERPETUAL", "ETH_USDT", "MEXC_FUT:ETHUSDT", "0.01"),
    "MEXC_FUT:ENAUSDT": _identity("MEXC", "MEXC_FUTURES", "PERPETUAL", "ENA_USDT", "MEXC_FUT:ENAUSDT", "0.00001"),
    "MEXC_FUT:SOLUSDT": _identity("MEXC", "MEXC_FUTURES", "PERPETUAL", "SOL_USDT", "MEXC_FUT:SOLUSDT", "0.01"),
}


class PreflightError(ValueError):
    pass


def _read_json(path: Path) -> tuple[bytes, Any]:
    try:
        raw = path.read_bytes()
        return raw, json.loads(raw.decode("utf-8"), parse_float=Decimal, parse_int=Decimal)
    except Exception as exc:
        raise PreflightError(f"invalid JSON: {path}") from exc


def _validate_tick(value: Any, symbol: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise PreflightError(f"{symbol} tick_size must be an exact decimal string")
    try:
        tick = Decimal(value)
    except InvalidOperation as exc:
        raise PreflightError(f"{symbol} tick_size is malformed") from exc
    if not tick.is_finite() or tick <= 0:
        raise PreflightError(f"{symbol} tick_size must be finite and positive")
    canonical = format(tick, "f").rstrip("0").rstrip(".") if "." in format(tick, "f") else format(tick, "f")
    if value != canonical:
        raise PreflightError(f"{symbol} tick_size is not canonical")
    return value


def load_validation_tick_provenance(
    *,
    settings_path: str | Path = DEFAULT_SETTINGS,
    snapshot_path: str | Path = DEFAULT_SNAPSHOT,
) -> dict[str, Any]:
    """Validate and return provenance maps without opening or writing any database."""
    settings_path = Path(settings_path)
    snapshot_path = Path(snapshot_path)
    _, settings = _read_json(settings_path)
    raw, snapshot = _read_json(snapshot_path)

    digest = hashlib.sha256(raw).hexdigest()
    if digest != ACCEPTED_SNAPSHOT_SHA256:
        raise PreflightError(f"snapshot SHA-256 mismatch: {digest}")
    if not isinstance(snapshot, dict) or snapshot.get("schema") != SNAPSHOT_SCHEMA:
        raise PreflightError("snapshot schema mismatch")
    if set(snapshot) != {"captured_at", "schema", "symbols"}:
        raise PreflightError("snapshot root is malformed")
    rows = snapshot.get("symbols")
    if not isinstance(rows, list) or len(rows) != 12:
        raise PreflightError("snapshot must contain exactly 12 identities")

    configured = settings.get("symbols") if isinstance(settings, dict) else None
    if not isinstance(configured, list) or len(configured) != 12 or len(set(configured)) != 12:
        raise PreflightError("configured storage symbols must contain 12 unique identities")
    if set(configured) != set(ACCEPTED_IDENTITIES):
        raise PreflightError("configured storage symbols do not match the accepted identities")

    by_symbol: dict[str, dict[str, str]] = {}
    for row in rows:
        if not isinstance(row, dict) or set(row) != set(PROVENANCE_FIELDS):
            raise PreflightError("snapshot identity is malformed")
        symbol = row.get("source_symbol")
        if not isinstance(symbol, str) or symbol in by_symbol:
            raise PreflightError("snapshot contains a missing or duplicate source_symbol")
        expected = ACCEPTED_IDENTITIES.get(symbol)
        if expected is None:
            raise PreflightError(f"snapshot contains unknown identity {symbol}")
        for field in PROVENANCE_FIELDS:
            if not isinstance(row.get(field), str) or not row[field]:
                raise PreflightError(f"{symbol} requires exact field {field}")
        _validate_tick(row["tick_size"], symbol)
        for field, value in expected.items():
            if row[field] != value:
                raise PreflightError(f"{symbol} does not match configured {field}")
        by_symbol[symbol] = dict(row)

    if set(by_symbol) != set(configured):
        raise PreflightError("snapshot identities do not match configured storage symbols")

    identities = {
        symbol: {field: by_symbol[symbol][field] for field in IDENTITY_FIELDS}
        for symbol in configured
    }
    ticks = {
        symbol: {
            **by_symbol[symbol],
            "source": by_symbol[symbol]["metadata_source"],
        }
        for symbol in configured
    }
    return {"sha256": digest, "symbol_identities": identities, "symbol_ticks": ticks}


def _available_snapshot_sha256(path: Path) -> str | None:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        return None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--settings",
        type=Path,
        default=DEFAULT_SETTINGS,
        help="scanner settings JSON (default: repository pnf_mvp/settings.json)",
    )
    parser.add_argument(
        "--snapshot",
        type=Path,
        default=DEFAULT_SNAPSHOT,
        help="accepted provenance snapshot JSON (default: repository snapshot)",
    )
    args = parser.parse_args(argv)
    try:
        result = load_validation_tick_provenance(
            settings_path=args.settings,
            snapshot_path=args.snapshot,
        )
        output = {
            "status": "PASS",
            "snapshot_sha256": result["sha256"],
            "verified_symbol_count": len(result["symbol_identities"]),
        }
        exit_code = 0
    except Exception as exc:
        output = {
            "status": "FAIL",
            "snapshot_sha256": _available_snapshot_sha256(args.snapshot),
            "verified_symbol_count": 0,
            "error": str(exc),
        }
        exit_code = 1
    print(json.dumps(output, sort_keys=True, separators=(",", ":")))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())

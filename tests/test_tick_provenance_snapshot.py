import hashlib
import json
import sys
import tempfile
from pathlib import Path
from unittest import TestCase


ROOT = Path(__file__).resolve().parents[1]
PNF_ROOT = ROOT / "pnf_mvp"
if str(PNF_ROOT) not in sys.path:
    sys.path.insert(0, str(PNF_ROOT))

from tick_provenance_snapshot import (  # noqa: E402
    SnapshotError,
    build_snapshot,
    canonical_json_bytes,
    write_snapshot,
)


SYMBOLS = [
    "BTCUSDT",
    "ETHUSDT",
    "BNBUSDT",
    "SOLUSDT",
    "XRPUSDT",
    "MEXC_FUT:SUIUSDT",
    "MEXC_FUT:TAOUSDT",
    "MEXC_FUT:HYPEUSDT",
    "MEXC_FUT:BTCUSDT",
    "MEXC_FUT:ETHUSDT",
    "MEXC_FUT:ENAUSDT",
    "MEXC_FUT:SOLUSDT",
]


BINANCE_TICKS = {
    "BTCUSDT": "0.01000000",
    "ETHUSDT": "0.01000000",
    "BNBUSDT": "0.01000000",
    "SOLUSDT": "0.01000000",
    "XRPUSDT": "0.00010000",
}
MEXC_TICKS = {
    "SUI_USDT": "0.0001",
    "TAO_USDT": "0.01",
    "HYPE_USDT": "0.001",
    "BTC_USDT": "0.1",
    "ETH_USDT": "0.01",
    "ENA_USDT": "0.0001",
    "SOL_USDT": "0.001",
}


def binance_payload(symbols=None):
    selected = symbols or list(BINANCE_TICKS)
    return {
        "symbols": [
            {
                "symbol": symbol,
                "status": "TRADING",
                "baseAsset": symbol.removesuffix("USDT"),
                "quoteAsset": "USDT",
                "filters": [
                    {"filterType": "PRICE_FILTER", "tickSize": BINANCE_TICKS[symbol]}
                ],
            }
            for symbol in selected
        ]
    }


def mexc_payload(native_symbol, **overrides):
    row = {
        "symbol": native_symbol,
        "state": 0,
        "baseCoin": native_symbol.split("_", 1)[0],
        "quoteCoin": "USDT",
        "priceUnit": MEXC_TICKS[native_symbol],
        "priceScale": len(MEXC_TICKS[native_symbol].split(".", 1)[-1]),
    }
    row.update(overrides)
    return {"success": True, "code": 0, "data": [row]}


class FixtureFetcher:
    def __call__(self, provider, native_symbols):
        if provider == "BINANCE":
            return binance_payload(native_symbols)
        if provider == "MEXC":
            self.assert_single(native_symbols)
            return mexc_payload(native_symbols[0])
        raise AssertionError(provider)

    @staticmethod
    def assert_single(symbols):
        if len(symbols) != 1:
            raise AssertionError(symbols)


class TickProvenanceSnapshotTests(TestCase):
    def test_builds_complete_deterministic_snapshot_for_configured_symbols(self):
        timestamp = "2026-09-24T10:00:00Z"
        first = build_snapshot(SYMBOLS, fetch_json=FixtureFetcher(), captured_at=timestamp)
        second = build_snapshot(list(reversed(SYMBOLS)), fetch_json=FixtureFetcher(), captured_at=timestamp)
        self.assertEqual(canonical_json_bytes(first), canonical_json_bytes(second))
        self.assertEqual(len(first["symbols"]), 12)
        self.assertEqual(
            [row["source_symbol"] for row in first["symbols"]], sorted(SYMBOLS)
        )
        by_symbol = {row["source_symbol"]: row for row in first["symbols"]}
        self.assertEqual(
            by_symbol["BTCUSDT"] | {},
            {
                "provider": "BINANCE",
                "venue": "BINANCE_SPOT",
                "instrument_type": "SPOT",
                "native_symbol": "BTCUSDT",
                "source_symbol": "BTCUSDT",
                "tick_size": "0.01000000",
                "metadata_source": "https://api.binance.com/api/v3/exchangeInfo",
                "provenance_timestamp": timestamp,
                "provenance_version": "binance-spot-exchange-info-v3",
            },
        )
        self.assertEqual(by_symbol["MEXC_FUT:BTCUSDT"]["native_symbol"], "BTC_USDT")
        self.assertEqual(by_symbol["MEXC_FUT:BTCUSDT"]["tick_size"], "0.1")

    def test_rejects_duplicate_unknown_incomplete_and_invalid_metadata(self):
        cases = []

        def duplicate(provider, native_symbols):
            if provider == "BINANCE":
                payload = binance_payload(native_symbols)
                payload["symbols"].append(dict(payload["symbols"][0]))
                return payload
            return mexc_payload(native_symbols[0])

        cases.append(duplicate)

        def unknown(provider, native_symbols):
            if provider == "BINANCE":
                payload = binance_payload(native_symbols)
                payload["symbols"][0]["symbol"] = "UNKNOWNUSDT"
                return payload
            return mexc_payload(native_symbols[0])

        cases.append(unknown)

        def incomplete(provider, native_symbols):
            if provider == "BINANCE":
                payload = binance_payload(native_symbols)
                payload["symbols"][0].pop("filters")
                return payload
            return mexc_payload(native_symbols[0])

        cases.append(incomplete)

        def invalid(provider, native_symbols):
            if provider == "BINANCE":
                payload = binance_payload(native_symbols)
                payload["symbols"][0]["filters"][0]["tickSize"] = "0"
                return payload
            return mexc_payload(native_symbols[0])

        cases.append(invalid)

        for index, fetcher in enumerate(cases):
            with self.subTest(case=index), self.assertRaises(SnapshotError):
                build_snapshot(SYMBOLS, fetch_json=fetcher, captured_at="2026-09-24T10:00:00Z")

    def test_rejects_unknown_or_duplicate_configured_symbols_before_fetch(self):
        for symbols in (("UNKNOWN:FAKE",), ("BTCUSDT", "BTCUSDT")):
            with self.subTest(symbols=symbols), self.assertRaises(SnapshotError):
                build_snapshot(symbols, fetch_json=FixtureFetcher(), captured_at="2026-09-24T10:00:00Z")

    def test_never_overwrites_accepted_snapshot_and_writes_explicit_diff(self):
        timestamp = "2026-09-24T10:00:00Z"
        original = build_snapshot(SYMBOLS, fetch_json=FixtureFetcher(), captured_at=timestamp)
        changed = json.loads(json.dumps(original))
        changed["symbols"][0]["tick_size"] = "0.02000000"

        with tempfile.TemporaryDirectory() as temp_dir:
            output = Path(temp_dir) / "tick-provenance.json"
            accepted = write_snapshot(original, output)
            original_bytes = output.read_bytes()
            repeated = write_snapshot(original, output)
            candidate = write_snapshot(changed, output)

            self.assertEqual(accepted["status"], "CREATED")
            self.assertEqual(repeated["status"], "UNCHANGED")
            self.assertEqual(candidate["status"], "CHANGED")
            self.assertEqual(output.read_bytes(), original_bytes)
            self.assertEqual(
                accepted["sha256"], hashlib.sha256(original_bytes).hexdigest()
            )
            self.assertTrue(Path(candidate["candidate_path"]).exists())
            diff = json.loads(Path(candidate["diff_path"]).read_text(encoding="utf-8"))
            self.assertEqual(diff["accepted_sha256"], accepted["sha256"])
            self.assertEqual(diff["candidate_sha256"], candidate["sha256"])
            self.assertEqual(diff["changed_symbols"], ["BNBUSDT"])


if __name__ == "__main__":
    import unittest

    unittest.main()

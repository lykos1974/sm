import json
import ssl
import time
from threading import Event
from urllib.parse import urlencode
from urllib.request import urlopen
from typing import Callable, Dict, List, Optional

from storage import Storage


def interval_to_ms(interval: str) -> int:
    unit = interval[-1]
    value = int(interval[:-1])

    if unit == "m":
        return value * 60_000
    if unit == "h":
        return value * 3_600_000
    if unit == "d":
        return value * 86_400_000
    if unit == "w":
        return value * 7 * 86_400_000
    if unit == "M":
        return value * 30 * 86_400_000

    raise ValueError(f"Unsupported interval: {interval}")


class BaseCollector:
    exchange_name = "BASE"
    default_base_url = ""
    max_kline_limit = 500
    closed_candle_grace_ms = 5000

    def __init__(
        self,
        base_url: str,
        interval: str,
        poll_seconds: int,
        symbols: List[str],
        storage: Storage,
        logger: Callable[[str], None],
        symbol_prefix: str = "",
    ):
        self.base_url = base_url.rstrip("/")
        self.interval = interval
        self.poll_seconds = poll_seconds
        self.symbols = symbols
        self.storage = storage
        self.logger = logger
        self.symbol_prefix = symbol_prefix
        self.stop_event = Event()
        self.ssl_context = ssl.create_default_context()

    def stop(self):
        self.stop_event.set()

    def reset_stop(self):
        self.stop_event.clear()

    def storage_symbol(self, raw_symbol: str) -> str:
        return f"{self.symbol_prefix}{raw_symbol}" if self.symbol_prefix else raw_symbol

    def fetch_klines(
        self,
        symbol: str,
        limit: int,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ):
        params = {
            "symbol": symbol,
            "interval": self.interval,
            "limit": min(limit, self.max_kline_limit),
        }
        if start_time is not None:
            params["startTime"] = int(start_time)
        if end_time is not None:
            params["endTime"] = int(end_time)

        url = f"{self.base_url}/api/v3/klines?{urlencode(params)}"
        with urlopen(url, context=self.ssl_context, timeout=20) as resp:
            raw = resp.read().decode("utf-8")
        payload = json.loads(raw)
        if not isinstance(payload, list):
            raise RuntimeError(f"{self.exchange_name}: unexpected response for {symbol}: {payload}")
        return payload

    def _upsert_kline(self, storage_symbol: str, kline: list, cutoff_ms: Optional[int] = None) -> bool:
        # Match the scanner's close-confirmation margin. Never persist a
        # provisional candle and later mistake elapsed time for final OHLC.
        if cutoff_ms is None:
            cutoff_ms = int(time.time() * 1000) - self.closed_candle_grace_ms
        if int(kline[6]) > cutoff_ms:
            return False
        self.storage.upsert_candle(
            symbol=storage_symbol,
            interval="1m",
            open_time=int(kline[0]),
            close_time=int(kline[6]),
            open_price=float(kline[1]),
            high=float(kline[2]),
            low=float(kline[3]),
            close=float(kline[4]),
            volume=float(kline[5]),
        )
        return True

    def _save_current_state(self, storage_symbol: str):
        # Derive the watermark from committed candles, including empty polls.
        last_open = self.storage.get_last_open_time(storage_symbol, "1m")
        last_close = last_open + interval_to_ms("1m") - 1 if last_open is not None else None
        self.storage.save_collector_state(
            storage_symbol, "1m", last_open, last_close, int(time.time())
        )

    def bootstrap_symbol(self, raw_symbol: str, bootstrap_bars: int):
        cutoff_ms = int(time.time() * 1000) - self.closed_candle_grace_ms
        self.logger(f"{self.exchange_name}:{raw_symbol} bootstrap started ({bootstrap_bars} bars)")
        storage_symbol = self.storage_symbol(raw_symbol)

        total_inserted = 0
        limit = min(max(1, bootstrap_bars), self.max_kline_limit)
        klines = self.fetch_klines(raw_symbol, limit=limit)

        for k in klines:
            if self._upsert_kline(storage_symbol, k, cutoff_ms):
                total_inserted += 1

        self._save_current_state(storage_symbol)
        self.logger(f"{self.exchange_name}:{raw_symbol} bootstrap complete ({total_inserted} rows, key={storage_symbol})")

    def bootstrap_all(self, bootstrap_bars: int):
        for symbol in self.symbols:
            if self.stop_event.is_set():
                return
            try:
                self.bootstrap_symbol(symbol, bootstrap_bars)
            except Exception as e:
                self.logger(f"{self.exchange_name}:{symbol} bootstrap failed: {type(e).__name__}: {e}")

    def poll_symbol_once(self, raw_symbol: str):
        cutoff_ms = int(time.time() * 1000) - self.closed_candle_grace_ms
        storage_symbol = self.storage_symbol(raw_symbol)
        interval_ms = interval_to_ms("1m")
        last_open = self.storage.get_last_open_time(storage_symbol, "1m")

        inserted = 0

        if last_open is None:
            batches = [self.fetch_klines(raw_symbol, limit=min(10, self.max_kline_limit))]
        else:
            batches = []
            start_time = int(last_open)  # Re-fetch the last row to finalize older collector output.
            safety_loops = 0

            while safety_loops < 50 and not self.stop_event.is_set():
                safety_loops += 1
                klines = self.fetch_klines(raw_symbol, limit=self.max_kline_limit, start_time=start_time)
                if not klines:
                    break

                batches.append(klines)

                if len(klines) < self.max_kline_limit:
                    break

                newest_open = int(klines[-1][0])
                next_start = newest_open + interval_ms
                if next_start <= start_time:
                    break
                start_time = next_start

        for klines in batches:
            for k in klines:
                if self._upsert_kline(storage_symbol, k, cutoff_ms):
                    inserted += 1

        self._save_current_state(storage_symbol)
        latest_close_price = self.storage.get_latest_close(storage_symbol, "1m")
        self.logger(
            f"{self.exchange_name}:{raw_symbol} updated ({inserted} rows, last={latest_close_price}, key={storage_symbol})"
        )

    def poll_once(self):
        for symbol in self.symbols:
            if self.stop_event.is_set():
                return
            try:
                self.poll_symbol_once(symbol)
            except Exception as e:
                self.logger(f"{self.exchange_name}:{symbol} update failed: {type(e).__name__}: {e}")

    def run_forever(self):
        self.logger(f"{self.exchange_name}: live collector started.")
        while not self.stop_event.is_set():
            self.poll_once()
            for _ in range(max(1, self.poll_seconds * 10)):
                if self.stop_event.is_set():
                    break
                time.sleep(0.1)
        self.logger(f"{self.exchange_name}: live collector stopped.")


class BinanceCollector(BaseCollector):
    exchange_name = "BINANCE"
    default_base_url = "https://api.binance.com"
    max_kline_limit = 1000


class MexcFuturesCollector(BaseCollector):
    exchange_name = "MEXC_FUT"
    default_base_url = "https://api.mexc.com"
    max_kline_limit = 2000

    def api_symbol(self, raw_symbol: str) -> str:
        if "_" in raw_symbol:
            return raw_symbol
        if raw_symbol.endswith("USDT"):
            return f"{raw_symbol[:-4]}_USDT"
        return raw_symbol

    def storage_symbol(self, raw_symbol: str) -> str:
        normalized = raw_symbol.replace("_", "")
        return f"{self.symbol_prefix}{normalized}" if self.symbol_prefix else normalized

    def fetch_klines(
        self,
        symbol: str,
        limit: int,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ):
        api_symbol = self.api_symbol(symbol)
        params = {"interval": self.interval}
        if start_time is not None:
            params["start"] = int(start_time)
        if end_time is not None:
            params["end"] = int(end_time)

        url = f"{self.base_url}/api/v1/contract/kline/{api_symbol}?{urlencode(params)}"
        with urlopen(url, context=self.ssl_context, timeout=20) as resp:
            raw = resp.read().decode("utf-8")
        payload = json.loads(raw)
        if not isinstance(payload, dict) or not payload.get("success", False):
            raise RuntimeError(f"{self.exchange_name}: unexpected response for {symbol}: {payload}")

        data = payload.get("data", {})
        times = data.get("time", [])
        opens = data.get("open", [])
        highs = data.get("high", [])
        lows = data.get("low", [])
        closes = data.get("close", [])
        vols = data.get("vol", [])

        result = []
        for t, o, h, l, c, v in zip(times, opens, highs, lows, closes, vols):
            open_ms = int(t) * 1000
            close_ms = open_ms + interval_to_ms("1m") - 1
            result.append([open_ms, o, h, l, c, v, close_ms])

        return result[-min(limit, self.max_kline_limit):]

    def bootstrap_symbol(self, raw_symbol: str, bootstrap_bars: int):
        cutoff_ms = int(time.time() * 1000) - self.closed_candle_grace_ms
        self.logger(f"{self.exchange_name}:{raw_symbol} bootstrap started ({bootstrap_bars} bars)")
        storage_symbol = self.storage_symbol(raw_symbol)

        chunk = min(self.max_kline_limit, max(1, bootstrap_bars))
        collected: Dict[int, list] = {}

        end_s = int(time.time())

        while len(collected) < bootstrap_bars and not self.stop_event.is_set():
            start_s = max(0, end_s - (chunk - 1) * 60)
            klines = self.fetch_klines(raw_symbol, limit=chunk, start_time=start_s, end_time=end_s)
            if not klines:
                break

            for k in klines:
                collected[int(k[0])] = k

            earliest_open_ms = int(klines[0][0])
            next_end_s = earliest_open_ms // 1000 - 60
            if next_end_s >= end_s:
                break
            end_s = next_end_s

            if len(klines) < chunk:
                break

        ordered = [collected[k] for k in sorted(collected.keys())][-bootstrap_bars:]

        inserted = 0
        for k in ordered:
            if self._upsert_kline(storage_symbol, k, cutoff_ms):
                inserted += 1

        self._save_current_state(storage_symbol)
        self.logger(f"{self.exchange_name}:{raw_symbol} bootstrap complete ({inserted} rows, key={storage_symbol})")

    def poll_symbol_once(self, raw_symbol: str):
        cutoff_ms = int(time.time() * 1000) - self.closed_candle_grace_ms
        storage_symbol = self.storage_symbol(raw_symbol)
        last_open = self.storage.get_last_open_time(storage_symbol, "1m")

        inserted = 0

        if last_open is None:
            now_s = int(time.time())
            start_s = max(0, now_s - 9 * 60)
            batches = [self.fetch_klines(raw_symbol, limit=10, start_time=start_s, end_time=now_s)]
        else:
            batches = []
            start_s = int(last_open) // 1000  # Include the last stored candle.
            now_s = int(time.time())
            safety_loops = 0

            while safety_loops < 20 and not self.stop_event.is_set() and start_s <= now_s:
                safety_loops += 1
                end_s = min(now_s, start_s + (self.max_kline_limit - 1) * 60)
                klines = self.fetch_klines(raw_symbol, limit=self.max_kline_limit, start_time=start_s, end_time=end_s)
                if not klines:
                    break

                filtered = [k for k in klines if int(k[0]) >= int(last_open)]
                if filtered:
                    batches.append(filtered)

                newest_open_s = int(klines[-1][0]) // 1000
                next_start_s = newest_open_s + 60
                if next_start_s <= start_s:
                    break
                start_s = next_start_s

                if len(klines) < self.max_kline_limit:
                    break

        for klines in batches:
            for k in klines:
                if self._upsert_kline(storage_symbol, k, cutoff_ms):
                    inserted += 1

        self._save_current_state(storage_symbol)
        latest_close_price = self.storage.get_latest_close(storage_symbol, "1m")
        self.logger(
            f"{self.exchange_name}:{raw_symbol} updated ({inserted} rows, last={latest_close_price}, key={storage_symbol})"
        )


def build_collectors(config: dict, storage: Storage, logger: Callable[[str], None]):
    collectors = []

    for exchange_key, exchange_cfg in config.get("exchanges", {}).items():
        if not exchange_cfg.get("enabled", True):
            continue

        cls = {
            "binance": BinanceCollector,
            "mexc_fut": MexcFuturesCollector,
        }.get(exchange_key.lower())

        if cls is None:
            raise ValueError(f"Unsupported exchange config: {exchange_key}")

        collectors.append(
            cls(
                base_url=exchange_cfg.get("base_url", cls.default_base_url),
                interval=exchange_cfg["interval"],
                poll_seconds=int(exchange_cfg["poll_seconds"]),
                symbols=list(exchange_cfg["symbols"]),
                storage=storage,
                logger=logger,
                symbol_prefix=str(exchange_cfg.get("symbol_prefix", "")),
            )
        )

    return collectors

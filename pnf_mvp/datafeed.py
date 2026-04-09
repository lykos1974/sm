import json
import threading
import time
import urllib.parse
import urllib.request


class BinanceKlineFeed:
    def __init__(self, base_url, interval, poll_seconds, symbols, out_queue):
        self.base_url = base_url.rstrip("/")
        self.interval = interval
        self.poll_seconds = poll_seconds
        self.symbols = symbols
        self.out_queue = out_queue
        self._stop = threading.Event()
        self._thread = None
        self._last_close = {}

    def start(self):
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2)

    def _run(self):
        self.out_queue.put({"type": "log", "message": "Feed thread started."})
        while not self._stop.is_set():
            for symbol in self.symbols:
                try:
                    self._poll_symbol(symbol)
                except Exception as exc:
                    self.out_queue.put({"type": "log", "message": f"{symbol} poll failed: {exc}"})
            self._stop.wait(self.poll_seconds)

    def _poll_symbol(self, symbol):
        params = urllib.parse.urlencode({"symbol": symbol, "interval": self.interval, "limit": 2})
        url = f"{self.base_url}/api/v3/klines?{params}"
        with urllib.request.urlopen(url, timeout=10) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
        if not payload:
            return
        row = payload[-1]
        event = {
            "type": "kline",
            "symbol": symbol,
            "interval": self.interval,
            "open_time": int(row[0]),
            "open": float(row[1]),
            "high": float(row[2]),
            "low": float(row[3]),
            "close": float(row[4]),
            "volume": float(row[5]),
            "close_time": int(row[6]),
        }
        key = (symbol, event["close_time"])
        if self._last_close.get(symbol) == key:
            return
        self._last_close[symbol] = key
        self.out_queue.put(event)

import asyncio
import logging
import threading
import time
from collections import defaultdict

from dhanhq import marketfeed


log = logging.getLogger(__name__)


CONFIG = {
    "min_turnover_cr": 3.0,
    "max_spread_pct": 0.0035,
    "spread_tight_pct": 0.0010,
    "spread_medium_pct": 0.0020,
    "absorption_strong": 0.85,
    "absorption_mild": 0.95,
    "wall_pct_of_30min_vol": 0.03,
    "runway_target_pct": 0.02,
    "bid_ask_support_ratio": 0.80,
    "min_entry_score": 50,
    "iceberg_persist_pct": 0.60,
    "iceberg_min_qty": 300,
    "iceberg_price_tolerance": 0.05,
    "floor_pct_below_entry": 0.005,
    "velocity_high_thresh": 20,
    "velocity_mid_thresh": 10,
    "neg_velocity_exit_secs": 120,
    "spring_dip_pct": 0.005,
    "spring_bid_growth_pct": 1.10,
    "wall_persist_secs": 3.0,
    "tsl_target_pct": 0.03,
    "depth_buffer_secs": 60,
}


def compute_book_velocity(depth_buffer: list) -> float:
    if len(depth_buffer) < 10:
        return 0.0

    recent = depth_buffer[-5:]
    older = depth_buffer[-10:-5]

    ask_recent = sum(snap["sell_total"] for snap in recent) / 5
    ask_older = sum(snap["sell_total"] for snap in older) / 5
    bid_recent = sum(snap["buy_total"] for snap in recent) / 5
    bid_older = sum(snap["buy_total"] for snap in older) / 5

    if ask_older <= 0 or bid_older <= 0:
        return 0.0

    ask_velocity = (ask_older - ask_recent) / ask_older
    bid_velocity = (bid_recent - bid_older) / bid_older
    return round((ask_velocity + bid_velocity) * 100, 2)


def entry_gate(
    symbol: str,
    signal_price: float,
    depth_now: dict | None,
    depth_5s_ago: dict | None,
    daily_turnover_cr: float,
    recent_30min_volume: int,
) -> tuple[bool, int, str]:
    if not depth_now or not depth_now.get("buy") or not depth_now.get("sell"):
        return False, 0, "SKIP: no depth snapshot"

    if daily_turnover_cr < CONFIG["min_turnover_cr"]:
        return False, 0, f"SKIP: illiquid turnover {daily_turnover_cr:.2f}Cr"

    best_bid = depth_now["buy"][0]["price"]
    best_ask = depth_now["sell"][0]["price"]
    if best_bid <= 0:
        return False, 0, "SKIP: invalid best bid"

    spread_pct = (best_ask - best_bid) / best_bid
    if spread_pct > CONFIG["max_spread_pct"]:
        return False, 0, f"SKIP: spread {spread_pct * 100:.3f}% > {CONFIG['max_spread_pct'] * 100:.2f}%"

    score = 50

    if spread_pct < CONFIG["spread_tight_pct"]:
        score += 20
    elif spread_pct < CONFIG["spread_medium_pct"]:
        score += 10

    if depth_5s_ago and depth_5s_ago.get("sell"):
        ask_qty_now = sum(level["quantity"] for level in depth_now["sell"][:5])
        ask_qty_prev = sum(level["quantity"] for level in depth_5s_ago["sell"][:5])
        if ask_qty_prev > 0:
            absorption_ratio = ask_qty_now / ask_qty_prev
            if absorption_ratio < CONFIG["absorption_strong"]:
                score += 25
            elif absorption_ratio < CONFIG["absorption_mild"]:
                score += 10

    wall_threshold = recent_30min_volume * CONFIG["wall_pct_of_30min_vol"]
    target_price = signal_price * (1 + CONFIG["runway_target_pct"])
    resistance_qty = sum(
        level["quantity"]
        for level in depth_now["sell"]
        if signal_price < level["price"] <= target_price and level["quantity"] > wall_threshold
    )
    if resistance_qty == 0:
        score += 20
    elif resistance_qty < wall_threshold:
        score += 10

    top5_bid = sum(level["quantity"] for level in depth_now["buy"][:5])
    top5_ask = sum(level["quantity"] for level in depth_now["sell"][:5])
    if top5_ask > 0 and (top5_bid / top5_ask) >= CONFIG["bid_ask_support_ratio"]:
        score += 15

    if score < CONFIG["min_entry_score"]:
        return False, score, f"SKIP: depth score {score} < {CONFIG['min_entry_score']}"

    return True, score, f"PASS: depth score {score}"


def _detect_iceberg(signal_price: float, depth_buffer_60s: list) -> bool:
    if len(depth_buffer_60s) < 5:
        return False

    floor_price = signal_price * (1 - CONFIG["floor_pct_below_entry"])
    tolerance = CONFIG["iceberg_price_tolerance"]
    min_qty = CONFIG["iceberg_min_qty"]

    candidate_levels = set()
    for snap in depth_buffer_60s:
        for level in snap.get("buy", [])[:5]:
            if level["price"] >= floor_price and level["quantity"] >= min_qty:
                candidate_levels.add(round(level["price"], 2))

    required_appearances = max(1, int(len(depth_buffer_60s) * CONFIG["iceberg_persist_pct"]))
    for level_price in candidate_levels:
        appearances = sum(
            1
            for snap in depth_buffer_60s
            if any(
                abs(level["price"] - level_price) < tolerance and level["quantity"] >= min_qty
                for level in snap.get("buy", [])[:5]
            )
        )
        if appearances >= required_appearances:
            return True

    return False


def rank_signal(
    symbol: str,
    signal_price: float,
    gate_score: int,
    depth_now: dict | None,
    depth_buffer_60s: list,
    recent_30min_volume: int,
) -> int:
    del symbol, depth_now, recent_30min_volume

    score = gate_score
    if _detect_iceberg(signal_price, depth_buffer_60s):
        score += 35

    velocity = compute_book_velocity(depth_buffer_60s)
    if velocity > CONFIG["velocity_high_thresh"]:
        score += 25
    elif velocity > CONFIG["velocity_mid_thresh"]:
        score += 10
    elif velocity < 0:
        score -= 15

    return score


def allocate_slots(signals: list, max_slots: int = 8) -> list:
    return sorted(signals, key=lambda item: item["rank_score"], reverse=True)[:max_slots]


def watchdog_tick(
    position: dict,
    depth_now: dict | None,
    depth_buffer_60s: list,
    wall_timers: dict,
    recent_30min_volume: int,
) -> tuple[str, str]:
    if not depth_now or not depth_now.get("buy") or not depth_now.get("sell"):
        return "CONTINUE", "NO_DEPTH"

    entry_price = position["entry_price"]
    peak_price = position["peak_price"]
    symbol = position["symbol"]
    current_price = depth_now["buy"][0]["price"]

    velocity = compute_book_velocity(depth_buffer_60s)
    if velocity < 0:
        if position.get("neg_velocity_since") is None:
            position["neg_velocity_since"] = time.time()

        time_negative = time.time() - position["neg_velocity_since"]
        made_new_high = current_price >= peak_price * 0.999
        if time_negative >= CONFIG["neg_velocity_exit_secs"] and not made_new_high:
            return "EXIT", f"VELOCITY: negative {time_negative:.0f}s without new high"
    else:
        position["neg_velocity_since"] = None

    if current_price < entry_price * (1 - CONFIG["spring_dip_pct"]) and len(depth_buffer_60s) >= 6:
        top_bid_now = sum(level["quantity"] for level in depth_now["buy"][:5])
        top_bid_prev = sum(level["quantity"] for level in depth_buffer_60s[-6]["buy"][:5])
        if top_bid_prev > 0:
            bid_growth = top_bid_now / top_bid_prev
            if bid_growth >= CONFIG["spring_bid_growth_pct"]:
                return "HOLD", f"SPRING: bid growth {bid_growth:.2f}x"

    wall_threshold = recent_30min_volume * CONFIG["wall_pct_of_30min_vol"]
    tsl_ceiling = entry_price * (1 + CONFIG["tsl_target_pct"])
    active_walls = {
        round(level["price"], 2)
        for level in depth_now["sell"]
        if level["quantity"] > wall_threshold and entry_price < level["price"] <= tsl_ceiling
    }

    now_ts = time.time()
    for wall_price in active_walls:
        key = f"{symbol}_{wall_price}"
        if key not in wall_timers:
            wall_timers[key] = now_ts

        age = now_ts - wall_timers[key]
        if age >= CONFIG["wall_persist_secs"]:
            return "EXIT", f"WALL: ask wall {wall_price:.2f} persisted {age:.1f}s"

    for key in list(wall_timers.keys()):
        if not key.startswith(symbol):
            continue
        wall_price = float(key.split("_", 1)[1])
        still_present = any(
            abs(level["price"] - wall_price) < 0.05 and level["quantity"] > wall_threshold
            for level in depth_now["sell"][:5]
        )
        if not still_present:
            wall_timers.pop(key, None)

    return "CONTINUE", f"OK velocity={velocity}"


class DepthBuffer:
    def __init__(self):
        self._buffers: dict[str, list] = defaultdict(list)
        self._lock = threading.RLock()

    def push(self, symbol: str, packet: dict):
        raw_depth = packet.get("depth", [])
        buy = []
        sell = []
        for level in raw_depth:
            buy.append(
                {
                    "price": float(level["bid_price"]),
                    "quantity": int(level["bid_quantity"]),
                }
            )
            sell.append(
                {
                    "price": float(level["ask_price"]),
                    "quantity": int(level["ask_quantity"]),
                }
            )

        snap = {
            "ts": time.time(),
            "buy": buy,
            "sell": sell,
            "buy_total": sum(level["quantity"] for level in buy[:5]),
            "sell_total": sum(level["quantity"] for level in sell[:5]),
            "ltp": float(packet.get("LTP", 0) or 0),
            "volume": int(packet.get("volume", 0) or 0),
        }

        cutoff = time.time() - CONFIG["depth_buffer_secs"]
        with self._lock:
            buf = self._buffers[symbol]
            buf.append(snap)
            self._buffers[symbol] = [item for item in buf if item["ts"] > cutoff]

    def get(self, symbol: str) -> tuple[dict | None, dict | None, list]:
        with self._lock:
            buf = list(self._buffers.get(symbol, []))

        if not buf:
            return None, None, []

        depth_now = buf[-1]
        cutoff_5s = time.time() - 5
        older = [item for item in buf if item["ts"] <= cutoff_5s]
        depth_5s_ago = older[-1] if older else buf[0]
        return depth_now, depth_5s_ago, buf

    def stats(self) -> dict:
        with self._lock:
            return {symbol: len(buf) for symbol, buf in self._buffers.items()}


class DepthFeedManager:
    def __init__(self, client_id: str, access_token: str, security_resolver, enabled: bool = True):
        self.client_id = client_id
        self.access_token = access_token
        self.security_resolver = security_resolver
        self.enabled = enabled
        self.buffer = DepthBuffer()
        self._desired_symbols: set[str] = set()
        self._sid_to_symbol: dict[int, str] = {}
        self._lock = threading.RLock()
        self._thread = None
        self._running = False
        self._feed = None
        self._subscribed_symbols: set[str] = set()
        self._status = {
            "enabled": enabled,
            "running": False,
            "connected": False,
            "last_tick_ts": None,
            "last_error": None,
            "subscribed_count": 0,
        }

    def start(self):
        if not self.enabled or self._thread is not None:
            return

        self._running = True
        self._thread = threading.Thread(target=self._run, daemon=True, name="depth-feed")
        self._thread.start()

    def ensure_symbols(self, symbols: list[str]):
        if not self.enabled:
            return
        with self._lock:
            for symbol in symbols:
                if symbol:
                    self._desired_symbols.add(symbol.upper())

    def get(self, symbol: str):
        return self.buffer.get(symbol.upper())

    def status(self) -> dict:
        with self._lock:
            status = dict(self._status)
            status["desired_count"] = len(self._desired_symbols)
            status["desired_symbols"] = sorted(self._desired_symbols)[:40]
        return status

    def _instrument_triplets(self, symbols: list[str]) -> list[tuple[int, str, int]]:
        triplets = []
        for symbol in symbols:
            sec_id = self.security_resolver(symbol)
            if str(sec_id).isdigit():
                triplets.append((1, str(sec_id), marketfeed.Full))
                self._sid_to_symbol[int(sec_id)] = symbol
        return triplets

    def _refresh_subscriptions(self):
        with self._lock:
            desired = sorted(self._desired_symbols)

        if not desired:
            return

        if self._feed is None:
            instruments = self._instrument_triplets(desired)
            if not instruments:
                return

            self._feed = marketfeed.DhanFeed(self.client_id, self.access_token, instruments, version="v2")
            self._feed.run_forever()
            self._subscribed_symbols = {symbol for symbol in desired if symbol in self._sid_to_symbol.values()}
            with self._lock:
                self._status["connected"] = True
                self._status["subscribed_count"] = len(self._subscribed_symbols)
            log.info("Depth feed connected for %s symbols.", len(self._subscribed_symbols))
            return

        new_symbols = [symbol for symbol in desired if symbol not in self._subscribed_symbols]
        if new_symbols:
            instruments = self._instrument_triplets(new_symbols)
            if instruments:
                self._feed.subscribe_symbols(instruments)
                self._subscribed_symbols.update(new_symbols)
                with self._lock:
                    self._status["subscribed_count"] = len(self._subscribed_symbols)
                log.info("Depth feed subscribed to %s new symbols.", len(new_symbols))

    def _reset_feed(self, exc: Exception | None = None):
        if exc is not None:
            log.warning("Depth feed reconnect due to error: %s", exc)
        self._feed = None
        self._subscribed_symbols = set()
        with self._lock:
            self._status["connected"] = False
            self._status["last_error"] = str(exc) if exc else None

    def _run(self):
        asyncio.set_event_loop(asyncio.new_event_loop())
        with self._lock:
            self._status["running"] = True

        while self._running:
            try:
                self._refresh_subscriptions()
                if self._feed is None:
                    time.sleep(1)
                    continue

                packet = self._feed.get_data()
                if not isinstance(packet, dict):
                    continue
                if packet.get("type") != "Full Data":
                    continue

                sec_id = int(packet.get("security_id", 0) or 0)
                symbol = self._sid_to_symbol.get(sec_id)
                if not symbol:
                    continue

                self.buffer.push(symbol, packet)
                with self._lock:
                    self._status["last_tick_ts"] = time.time()
                    self._status["last_error"] = None
            except Exception as exc:
                self._reset_feed(exc)
                time.sleep(3)

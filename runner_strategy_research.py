import argparse
import bisect
import csv
import os
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, time, timedelta
from pathlib import Path

import pandas as pd
from zoneinfo import ZoneInfo


IST = ZoneInfo("Asia/Kolkata")

ROOT = Path(__file__).resolve().parent
DATA_DIR_DEFAULT = Path(r"C:\Users\cavis\OneDrive\Desktop\test scenrio")
GAINER_FILE_DEFAULT = Path(r"C:\Users\cavis\Downloads\Backtest scan more than 6% (13).csv")
CACHE_DIR_DEFAULT = ROOT / "historical_chart_cache"
OUTPUT_DIR_DEFAULT = ROOT / "backtest_outputs"

ROLLING_WINDOW_MINS = 25
TOP_1M = 15
TOP_3M = 20
TOP_3M_NO_1M = 35
MASTER_SIZE = 15
MAX_SLOTS = 8
SLOT_CAPITAL = 50000
SMART_ENTRY_MINS = 3
MAX_RED_CANDLE_PCT = 0.008
SL_PCT = 0.013
TSL_TRIGGER_PCT = 0.020
TSL_TRAIL_PCT = 0.020
TIME_EXIT_MINS = 45
WARMUP_END = time(9, 40)
EOD_EXIT_TIME = time(15, 29)


VARIANTS = {
    "baseline": {
        "override_score": 10_000,
        "runner_score": 10_000,
        "fast_score": 10_000,
        "runner_stall_mins": 75,
        "runner_stall_peak": 0.035,
        "runner_tsl_trigger": 0.025,
        "runner_tsl_trail": 0.025,
    },
    "override_only": {
        "override_score": 75,
        "runner_score": 10_000,
        "fast_score": 10_000,
        "runner_stall_mins": 75,
        "runner_stall_peak": 0.035,
        "runner_tsl_trigger": 0.025,
        "runner_tsl_trail": 0.025,
    },
    "runner_hold_85": {
        "override_score": 80,
        "runner_score": 85,
        "fast_score": 10_000,
        "runner_stall_mins": 90,
        "runner_stall_peak": 0.04,
        "runner_tsl_trigger": 0.025,
        "runner_tsl_trail": 0.025,
    },
    "runner_hold_95": {
        "override_score": 85,
        "runner_score": 95,
        "fast_score": 10_000,
        "runner_stall_mins": 90,
        "runner_stall_peak": 0.04,
        "runner_tsl_trigger": 0.025,
        "runner_tsl_trail": 0.025,
    },
    "supreme_v1": {
        "override_score": 70,
        "runner_score": 90,
        "fast_score": 95,
        "runner_stall_mins": 75,
        "runner_stall_peak": 0.035,
        "runner_tsl_trigger": 0.025,
        "runner_tsl_trail": 0.025,
    },
}

for override_score in (70, 75, 80, 85, 90):
    for runner_score_threshold in (90, 95, 100, 110):
        if runner_score_threshold < override_score:
            continue
        VARIANTS[f"grid_o{override_score}_r{runner_score_threshold}"] = {
            "override_score": override_score,
            "runner_score": runner_score_threshold,
            "fast_score": 10_000,
            "runner_stall_mins": 90,
            "runner_stall_peak": 0.04,
            "runner_tsl_trigger": 0.025,
            "runner_tsl_trail": 0.025,
        }


@dataclass
class Position:
    symbol: str
    entry: float
    entry_time: datetime
    entry_candle_time: datetime
    peak: float
    hard_sl: float
    sl: float
    tsl_on: bool
    qty: int
    trigger: str
    mode: str
    runner_score: int


@dataclass
class PendingSignal:
    symbol: str
    queued_at: datetime
    expires_at: datetime
    signal_high: float
    signal_close: float
    runner_score: int
    in_master: bool
    mode: str


class MinuteStore:
    def __init__(self, cache_dir: Path):
        self.cache_dir = cache_dir
        self.memory: dict[tuple[str, str], dict] = {}

    def get_day(self, symbol: str, day: str) -> dict | None:
        key = (symbol, day)
        if key in self.memory:
            return self.memory[key]

        path = self.cache_dir / day / f"{symbol.upper()}.csv"
        if not path.exists():
            self.memory[key] = None
            return None

        rows = []
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                ts = datetime.fromtimestamp(float(row["timestamp"]), IST).replace(tzinfo=None)
                rows.append(
                    {
                        "ts": ts,
                        "open": float(row["open"]),
                        "high": float(row["high"]),
                        "low": float(row["low"]),
                        "close": float(row["close"]),
                        "volume": int(float(row.get("volume", 0) or 0)),
                    }
                )
        rows.sort(key=lambda item: item["ts"])
        timestamps = [row["ts"] for row in rows]
        by_ts = {row["ts"]: row for row in rows}
        cum_volume = []
        total = 0
        pv_total = 0.0
        cum_pv = []
        for row in rows:
            total += row["volume"]
            typical = (row["high"] + row["low"] + row["close"]) / 3
            pv_total += typical * row["volume"]
            cum_volume.append(total)
            cum_pv.append(pv_total)

        payload = {"rows": rows, "timestamps": timestamps, "by_ts": by_ts, "cum_volume": cum_volume, "cum_pv": cum_pv}
        self.memory[key] = payload
        return payload

    def get_candle(self, symbol: str, day: str, ts: datetime) -> dict | None:
        payload = self.get_day(symbol, day)
        if not payload:
            return None
        return payload["by_ts"].get(ts)

    def get_recent(self, symbol: str, day: str, ts: datetime, mins: int) -> list[dict]:
        payload = self.get_day(symbol, day)
        if not payload:
            return []
        idx = bisect.bisect_right(payload["timestamps"], ts)
        return payload["rows"][max(0, idx - mins):idx]

    def cumulative_volume(self, symbol: str, day: str, ts: datetime) -> int:
        payload = self.get_day(symbol, day)
        if not payload:
            return 0
        idx = bisect.bisect_right(payload["timestamps"], ts) - 1
        return int(payload["cum_volume"][idx]) if idx >= 0 else 0

    def open_price(self, symbol: str, day: str) -> float | None:
        payload = self.get_day(symbol, day)
        if not payload or not payload["rows"]:
            return None
        return float(payload["rows"][0]["open"])

    def vwap(self, symbol: str, day: str, ts: datetime) -> float | None:
        payload = self.get_day(symbol, day)
        if not payload:
            return None
        idx = bisect.bisect_right(payload["timestamps"], ts) - 1
        if idx < 0 or payload["cum_volume"][idx] <= 0:
            return None
        return payload["cum_pv"][idx] / payload["cum_volume"][idx]


def parse_dt(value: str) -> datetime:
    return datetime.strptime(value, "%d-%m-%Y %H:%M")


def load_scan(path: Path, shift_minutes: int = 1) -> pd.DataFrame:
    df = pd.read_csv(path)
    df["dt"] = df["date"].map(parse_dt)
    df["symbol"] = df["symbol"].astype(str).str.strip().str.upper()
    df = df[df["dt"].dt.strftime("%H:%M") != "09:15"].copy()
    if shift_minutes:
        df["dt"] = df["dt"] + timedelta(minutes=shift_minutes)
    df["day"] = df["dt"].dt.strftime("%Y-%m-%d")
    return df


def build_timestamp_map(df: pd.DataFrame, day: str) -> dict[datetime, list[str]]:
    day_df = df[df["day"] == day]
    if day_df.empty:
        return {}
    return day_df.groupby("dt")["symbol"].apply(list).to_dict()


def prune_pool(pool: dict[str, list[datetime]], cutoff: datetime) -> dict[str, int]:
    counts = {}
    for symbol in list(pool.keys()):
        valid = [stamp for stamp in pool[symbol] if stamp >= cutoff]
        if valid:
            pool[symbol] = valid
            counts[symbol] = len(valid)
        else:
            pool.pop(symbol, None)
    return counts


def rank_with_volume(counts: dict[str, int], symbols: list[str], limit: int, day: str, ts: datetime, store: MinuteStore) -> list[str]:
    unique = list(dict.fromkeys(symbols))
    return sorted(unique, key=lambda symbol: (-counts.get(symbol, 0), -store.cumulative_volume(symbol, day, ts), symbol))[:limit]


def rebuild_master(now: datetime, day: str, has_1m: bool, pools: dict[str, dict[str, list[datetime]]], store: MinuteStore):
    cutoff = now - timedelta(minutes=ROLLING_WINDOW_MINS)
    counts_1m = prune_pool(pools["1m"], cutoff)
    counts_3m = prune_pool(pools["3m"], cutoff)
    counts_5m = prune_pool(pools["5m"], cutoff)

    if has_1m:
        top_1m = rank_with_volume(counts_1m, list(counts_1m), TOP_1M, day, now, store)
        top_3m = rank_with_volume(counts_3m, list(counts_3m), TOP_3M, day, now, store)
    else:
        top_1m = []
        top_3m = rank_with_volume(counts_3m, list(counts_3m), TOP_3M_NO_1M, day, now, store)

    combined = list(dict.fromkeys(top_1m + top_3m))
    master = rank_with_volume(counts_5m, combined, MASTER_SIZE, day, now, store)
    counts = {"1m": counts_1m, "3m": counts_3m, "5m": counts_5m}
    return master, counts


def runner_score(symbol: str, day: str, ts: datetime, counts: dict[str, dict[str, int]], store: MinuteStore, final_buy_counts: Counter) -> tuple[int, dict]:
    candle = store.get_candle(symbol, day, ts)
    recent_10 = store.get_recent(symbol, day, ts, 10)
    open_price = store.open_price(symbol, day)
    vwap = store.vwap(symbol, day, ts)
    if not candle or not open_price:
        return 0, {"reason": "NO_CANDLE"}

    day_gain = (candle["close"] - open_price) / open_price
    last_10_vol = sum(row["volume"] for row in recent_10)
    elapsed = max(1, int((ts - datetime.combine(ts.date(), time(9, 15))).total_seconds() // 60) + 1)
    cum_vol = store.cumulative_volume(symbol, day, ts)
    expected_10 = (cum_vol / elapsed) * min(10, elapsed) if elapsed > 0 else 0
    rel_vol = (last_10_vol / expected_10) if expected_10 > 0 else 0.0
    above_vwap = vwap is not None and candle["close"] >= vwap

    score = 0
    score += min(30, counts["5m"].get(symbol, 0) * 12)
    score += min(25, counts["3m"].get(symbol, 0) * 6)
    score += min(20, counts["1m"].get(symbol, 0) * 5)
    score += min(20, final_buy_counts[symbol] * 8)
    if day_gain >= 0.02:
        score += 15
    if day_gain >= 0.04:
        score += 10
    if above_vwap:
        score += 10
    if rel_vol >= 1.5:
        score += 10
    if rel_vol >= 2.5:
        score += 10
    if day_gain > 0.09:
        score -= 15
    if candle["close"] < candle["open"] and day_gain < 0.04:
        score -= 8

    return int(score), {
        "day_gain_pct": round(day_gain * 100, 2),
        "rel_vol_10m": round(rel_vol, 2),
        "above_vwap": above_vwap,
        "counts_1m": counts["1m"].get(symbol, 0),
        "counts_3m": counts["3m"].get(symbol, 0),
        "counts_5m": counts["5m"].get(symbol, 0),
        "final_buy_count": final_buy_counts[symbol],
    }


def entry_from_candle(candle: dict, signal_high: float, allow_immediate: bool, signal_close: float):
    if allow_immediate and candle["close"] >= signal_close and candle["close"] > candle["open"]:
        return float(candle["close"]), "RUNNER_FAST"
    if candle["high"] > signal_high:
        return float(signal_high if candle["open"] <= signal_high else candle["open"]), "BREAKOUT"
    if candle["open"] > 0 and candle["close"] < candle["open"]:
        dip_pct = (candle["open"] - candle["close"]) / candle["open"]
        if dip_pct <= MAX_RED_CANDLE_PCT:
            return float(candle["close"]), "HEALTHY_DIP"
    return None, None


def exit_position(pos: Position, candle: dict, now: datetime, is_eod: bool, config: dict):
    if is_eod:
        return float(candle["open"]), "EOD_EXIT"
    if now <= pos.entry_candle_time:
        return None, None

    elapsed = (now - pos.entry_time).total_seconds() / 60.0
    runner_mode = pos.mode == "RUNNER"
    if not runner_mode and elapsed >= TIME_EXIT_MINS:
        return float(candle["close"]), "TIME_EXIT"

    if candle["low"] <= pos.hard_sl:
        return pos.hard_sl, "SL_HIT"

    peak = max(pos.peak, float(candle["high"]))
    tsl_on = pos.tsl_on
    sl = pos.sl
    trigger = TSL_TRIGGER_PCT if not runner_mode else config["runner_tsl_trigger"]
    trail = TSL_TRAIL_PCT if not runner_mode else config["runner_tsl_trail"]

    if not tsl_on and candle["high"] >= pos.entry * (1 + trigger):
        tsl_on = True
    if tsl_on:
        trailing_floor = peak * (1 - trail)
        if trailing_floor > sl:
            sl = round(trailing_floor, 2)
        if candle["low"] <= sl:
            pos.peak = peak
            pos.tsl_on = tsl_on
            pos.sl = sl
            return sl, "TSL_HIT"

    if runner_mode:
        # Once the runner has failed to extend for 75 minutes, free the slot.
        if elapsed >= config["runner_stall_mins"] and peak < pos.entry * (1 + config["runner_stall_peak"]):
            return float(candle["close"]), "RUNNER_STALL_EXIT"

    pos.peak = peak
    pos.tsl_on = tsl_on
    pos.sl = sl
    return None, None


def load_gainers(path: Path) -> dict[str, set[str]]:
    df = pd.read_csv(path)
    df["day"] = pd.to_datetime(df["date"], format="%d-%m-%Y").dt.strftime("%Y-%m-%d")
    df["symbol"] = df["symbol"].astype(str).str.strip().str.upper()
    return {day: set(group["symbol"]) for day, group in df.groupby("day")}


def simulate_day(day: str, maps: dict[str, dict[datetime, list[str]]], has_1m: bool, store: MinuteStore, variant: str, gainer_set: set[str]):
    config = VARIANTS[variant]
    all_times = sorted(set().union(*[set(value.keys()) for value in maps.values()]))
    if not all_times:
        return [], []

    pools = {"1m": defaultdict(list), "3m": defaultdict(list), "5m": defaultdict(list)}
    final_buy_counts = Counter()
    pending: dict[str, PendingSignal] = {}
    positions: dict[str, Position] = {}
    trades = []
    rejects = []
    master = []
    counts = {"1m": {}, "3m": {}, "5m": {}}

    current = min(all_times).replace(second=0, microsecond=0)
    end = datetime.strptime(day + " 15:29", "%Y-%m-%d %H:%M")
    while current <= end:
        for tf in ("1m", "3m", "5m"):
            for symbol in maps[tf].get(current, []):
                pools[tf][symbol].append(current)
        if maps["1m"].get(current) or maps["3m"].get(current) or maps["5m"].get(current):
            master, counts = rebuild_master(current, day, has_1m, pools, store)

        if current.time() >= WARMUP_END:
            for symbol in maps["buy"].get(current, []):
                final_buy_counts[symbol] += 1
                if symbol in positions or symbol in pending:
                    continue
                score, features = runner_score(symbol, day, current, counts, store, final_buy_counts)
                in_master = symbol in master
                allow = in_master
                mode = "BASE"
                if score >= config["override_score"]:
                    allow = True
                    mode = "RUNNER" if score >= config["runner_score"] else "BASE"
                if not allow:
                    rejects.append({"symbol": symbol, "ts": current, "reason": "NOT_QUALIFIED", "runner_score": score, **features})
                    continue
                candle = store.get_candle(symbol, day, current)
                if not candle:
                    rejects.append({"symbol": symbol, "ts": current, "reason": "NO_CANDLE", "runner_score": score})
                    continue
                pending[symbol] = PendingSignal(
                    symbol=symbol,
                    queued_at=current,
                    expires_at=current + timedelta(minutes=SMART_ENTRY_MINS),
                    signal_high=float(candle["high"]),
                    signal_close=float(candle["close"]),
                    runner_score=score,
                    in_master=in_master,
                    mode=mode,
                )

        is_eod = current.time() >= EOD_EXIT_TIME
        for symbol, pos in list(positions.items()):
            candle = store.get_candle(symbol, day, current)
            if not candle:
                continue
            exit_price, reason = exit_position(pos, candle, current, is_eod, config)
            if exit_price is not None:
                pnl_rs = ((exit_price - pos.entry) / pos.entry) * SLOT_CAPITAL
                trades.append(
                    {
                        "day": day,
                        "symbol": symbol,
                        "entry_time": pos.entry_time,
                        "exit_time": current,
                        "entry_p": round(pos.entry, 2),
                        "exit_p": round(exit_price, 2),
                        "trigger": pos.trigger,
                        "exit_reason": reason,
                        "mode": pos.mode,
                        "runner_score": pos.runner_score,
                        "pnl_rs": round(pnl_rs, 2),
                        "is_6pct_runner": symbol in gainer_set,
                    }
                )
                positions.pop(symbol, None)

        for symbol, meta in list(pending.items()):
            if current >= meta.expires_at:
                pending.pop(symbol, None)
                continue
            if len(positions) >= MAX_SLOTS:
                continue
            candle = store.get_candle(symbol, day, current)
            if not candle:
                continue
            allow_fast = meta.mode == "RUNNER" and meta.runner_score >= config["fast_score"]
            entry_price, trigger = entry_from_candle(candle, meta.signal_high, allow_fast, meta.signal_close)
            if entry_price is None:
                continue
            positions[symbol] = Position(
                symbol=symbol,
                entry=entry_price,
                entry_time=current,
                entry_candle_time=current,
                peak=entry_price,
                hard_sl=round(entry_price * (1 - SL_PCT), 2),
                sl=round(entry_price * (1 - SL_PCT), 2),
                tsl_on=False,
                qty=max(1, int(SLOT_CAPITAL / entry_price)),
                trigger=trigger,
                mode=meta.mode,
                runner_score=meta.runner_score,
            )
            pending.pop(symbol, None)

        current += timedelta(minutes=1)

    return trades, rejects


def summarize(trades: list[dict]) -> dict:
    pnl = round(sum(row["pnl_rs"] for row in trades), 2)
    return {
        "trades": len(trades),
        "pnl_rs": pnl,
        "wins": sum(1 for row in trades if row["pnl_rs"] > 0),
        "losses": sum(1 for row in trades if row["pnl_rs"] < 0),
        "runner_trades": sum(1 for row in trades if row["is_6pct_runner"]),
        "runner_pnl_rs": round(sum(row["pnl_rs"] for row in trades if row["is_6pct_runner"]), 2),
        "non_runner_pnl_rs": round(sum(row["pnl_rs"] for row in trades if not row["is_6pct_runner"]), 2),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default=str(DATA_DIR_DEFAULT))
    parser.add_argument("--gainer-file", default=str(GAINER_FILE_DEFAULT))
    parser.add_argument("--cache-dir", default=str(CACHE_DIR_DEFAULT))
    parser.add_argument("--output-dir", default=str(OUTPUT_DIR_DEFAULT))
    parser.add_argument("--shift-minutes", type=int, default=1)
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    store = MinuteStore(Path(args.cache_dir))
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    scans = {
        "1m": load_scan(data_dir / "Backtest 1 min scan.csv", args.shift_minutes),
        "3m": load_scan(data_dir / "Backtest 3 min scan.csv", args.shift_minutes),
        "5m": load_scan(data_dir / "Backtest 5 min scan.csv", args.shift_minutes),
        "buy": load_scan(data_dir / "Backtest 5 MIn buy scanner.csv", args.shift_minutes),
    }
    gainers = load_gainers(Path(args.gainer_file))
    days = sorted(set(scans["buy"]["day"]).intersection({p.name for p in Path(args.cache_dir).iterdir() if p.is_dir()}))

    summaries = []
    all_trades = []
    for variant in VARIANTS:
        variant_trades = []
        variant_rejects = []
        for day in days:
            maps = {name: build_timestamp_map(df, day) for name, df in scans.items()}
            has_1m = not scans["1m"][scans["1m"]["day"] == day].empty
            trades, rejects = simulate_day(day, maps, has_1m, store, variant, gainers.get(day, set()))
            variant_trades.extend(trades)
            variant_rejects.extend(rejects)
            day_summary = summarize(trades)
            day_summary.update({"variant": variant, "day": day, "rejects": len(rejects)})
            summaries.append(day_summary)

        for row in variant_trades:
            row = row.copy()
            row["variant"] = variant
            all_trades.append(row)

        print(variant, summarize(variant_trades), "rejects", len(variant_rejects))

    summary_path = output_dir / "runner_strategy_summary.csv"
    trades_path = output_dir / "runner_strategy_trades.csv"
    pd.DataFrame(summaries).to_csv(summary_path, index=False)
    pd.DataFrame(all_trades).to_csv(trades_path, index=False)
    print("wrote", summary_path)
    print("wrote", trades_path)


if __name__ == "__main__":
    main()

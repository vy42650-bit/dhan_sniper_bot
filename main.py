import os, logging, threading, time, csv
from datetime import datetime, timedelta
from flask import Flask, request, jsonify
from dhanhq import dhanhq

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
CLIENT_ID    = os.getenv("DHAN_CLIENT_ID",    "1105120853")
ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN",
    "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzc4NTA3NjMzLCJpYXQiOjE3Nzg0MjEyMzMsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTA1MTIwODUzIn0.fOp6B0Wd_5hfrIAv6dokxrFyGRYJztiVxvWKR6yvEWt6ZR7rqrpgfeZBq0j09RL0yQTWaeo1IJJ6nvmtO6IY8w"
)
# NOTE: Keep SANDBOX until you are ready for real money. Set TRADING_MODE=LIVE in Railway env vars.
TRADING_MODE = os.getenv("TRADING_MODE", "SANDBOX")  # SANDBOX | LIVE

# ── Strategy Constants (Final Optimized) ──────────────────────────────────────
BLACKLIST          = {"MEESHO", "MEESHO-BE"}
MAX_SLOTS          = 8
SL_PCT             = 0.013    # 1.3% hard stop
TSL_TRIGGER_PCT    = 0.020    # ✅ 2.0% trigger (upgraded from 3%)
TSL_TRAIL_PCT      = 0.020    # 2.0% trail
TIME_EXIT_MINS     = 45
ROLLING_WINDOW     = 25       # look-back minutes for hit counting
REFRESH_MINS       = 5        # ✅ Rolling 5m refresh (upgraded from fixed 25m)
TOP_1M             = 15
TOP_3M             = 25
SLOT_CAPITAL       = 50_000
MAX_RED_CANDLE_PCT = 0.008    # 0.8% healthy dip
EXHAUSTION_MULT    = 3.0
SMART_ENTRY_MINS   = 3
EMA_GUARD_PCT      = 0.04     # ✅ Reject if price >4% above EMA9

# ── State ─────────────────────────────────────────────────────────────────────
pool_1m:   dict = {}   # symbol -> list of hit timestamps
pool_3m:   dict = {}
master:    list = []
positions: dict = {}
pending:   dict = {}
trades_today: list = []  # for EOD report

# ── Dhan Client ───────────────────────────────────────────────────────────────
try:
    from dhanhq import DhanContext
    dhan = dhanhq(DhanContext(CLIENT_ID, ACCESS_TOKEN))
except ImportError:
    dhan = dhanhq(CLIENT_ID, ACCESS_TOKEN)

app = Flask(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _parse_stocks(req) -> list:
    try:
        data = req.get_json(force=True, silent=True) or {}
        raw  = data.get("stocks", data.get("stock", ""))
    except Exception:
        raw = ""
    return [s.strip().upper() for s in raw.split(",") if s.strip()]


def _market_open(now=None) -> bool:
    now = now or datetime.now()
    if now.weekday() >= 5: return False
    s = now.replace(hour=9,  minute=15, second=0, microsecond=0)
    e = now.replace(hour=15, minute=15, second=0, microsecond=0)
    return s <= now <= e


def _resolve_security_id(symbol: str) -> str:
    try:
        import pandas as pd
        path = os.path.join(os.path.dirname(__file__), "security_map.csv")
        df   = pd.read_csv(path, low_memory=False)
        row  = df[(df["SEM_TRADING_SYMBOL"] == symbol) &
                  (df["SEM_INSTRUMENT_NAME"] == "EQUITY")]
        if not row.empty:
            return str(row.iloc[0]["SEM_SMST_SECURITY_ID"])
    except Exception:
        pass
    return symbol


def _get_ltp(symbol: str) -> float | None:
    try:
        resp = dhan.get_quote(symbol)
        return float(resp["data"]["last_price"])
    except Exception:
        return None


def _get_1min_candles(symbol: str, n: int = 10) -> list | None:
    try:
        today = datetime.now().strftime("%Y-%m-%d")
        resp  = dhan.intraday_minute_data(
            _resolve_security_id(symbol), "NSE_EQ", "EQUITY", today, today
        )
        if resp.get("status") == "success" and resp.get("data"):
            data = resp["data"]
            return data[-n:] if len(data) >= n else data
    except Exception:
        pass
    return None


def _calculate_ema9(candles: list) -> float | None:
    """EMA-9 on close prices."""
    if len(candles) < 9:
        return None
    closes = [c["close"] for c in candles]
    k = 2 / (9 + 1)
    ema = closes[0]
    for price in closes[1:]:
        ema = price * k + ema * (1 - k)
    return ema


def _place_order(symbol: str, qty: int, side: str) -> dict:
    if TRADING_MODE != "LIVE":
        log.info(f"[SANDBOX] {side} {qty} x {symbol}")
        return {"status": "SANDBOX_OK"}
    return dhan.place_order(
        security_id=_resolve_security_id(symbol),
        exchange_segment=dhan.NSE,
        transaction_type=dhan.BUY if side == "BUY" else dhan.SELL,
        quantity=qty,
        order_type=dhan.MARKET,
        product_type=dhan.INTRADAY,
        price=0,
    )


def _exit(symbol: str, pos: dict, reason: str, exit_price: float):
    pnl = (exit_price - pos["entry"]) * pos["qty"]
    pnl_rs = ((exit_price - pos["entry"]) / pos["entry"]) * SLOT_CAPITAL
    log.info(
        f"EXIT {symbol} reason={reason} entry={pos['entry']:.2f} "
        f"exit={exit_price:.2f} pnl=Rs.{pnl_rs:+.2f}"
    )
    _place_order(symbol, pos["qty"], "SELL")
    trades_today.append({
        "symbol":     symbol,
        "entry_p":    pos["entry"],
        "entry_t":    pos["entry_time"].strftime("%H:%M:%S"),
        "exit_p":     round(exit_price, 2),
        "exit_t":     datetime.now().strftime("%H:%M:%S"),
        "exit_reason":reason,
        "pnl_rs":     round(pnl_rs, 2),
    })
    positions.pop(symbol, None)


def _save_eod_report():
    fname = f"trades_{datetime.now().strftime('%Y%m%d')}.csv"
    path  = os.path.join(os.path.dirname(__file__), fname)
    if not trades_today:
        log.info("EOD: No trades today.")
        return
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=trades_today[0].keys())
        w.writeheader(); w.writerows(trades_today)
    total = sum(t["pnl_rs"] for t in trades_today)
    log.info(f"EOD REPORT saved → {path}  |  Total P/L: Rs.{total:,.2f}")


# ─────────────────────────────────────────────────────────────────────────────
# ROLLING MASTER LIST  (replaces fixed 25-min block logic)
# ─────────────────────────────────────────────────────────────────────────────

def _get_rolling_volume(symbol: str, cutoff: datetime) -> int:
    """Fetch total volume traded since cutoff using cached minute data."""
    try:
        today = datetime.now().strftime("%Y-%m-%d")
        resp  = dhan.intraday_minute_data(
            _resolve_security_id(symbol), "NSE_EQ", "EQUITY", today, today
        )
        if resp.get("status") == "success" and resp.get("data"):
            import pytz
            ist = pytz.timezone('Asia/Kolkata')
            rows = resp["data"]
            vol  = 0
            for r in rows:
                try:
                    ts = datetime.fromtimestamp(r.get("start_Time", r.get("timestamp", 0)))
                    if ts >= cutoff:
                        vol += int(r.get("volume", 0))
                except Exception:
                    pass
            return vol
    except Exception:
        pass
    return 0


def _rebuild_master(stocks_5m: list):
    """
    Rolling 5-min refresh of Master List.
    Ranking: Total hit count (1m + 3m) in the 25-min window.
    Tie-Breaker: If two symbols have EQUAL hit count, the one with
    higher traded volume in that window wins.
    """
    global master
    now    = datetime.now()
    cutoff = now - timedelta(minutes=ROLLING_WINDOW)

    # Prune stale timestamps outside the 25-min window
    for sym in list(pool_1m): pool_1m[sym] = [t for t in pool_1m[sym] if t > cutoff]
    for sym in list(pool_3m): pool_3m[sym] = [t for t in pool_3m[sym] if t > cutoff]

    # Pure hit count (equal weight — 1m and 3m both count as 1 hit each)
    all_syms = set(pool_1m) | set(pool_3m)
    candidates = []
    for sym in all_syms:
        hits = len(pool_1m.get(sym, [])) + len(pool_3m.get(sym, []))
        candidates.append({"sym": sym, "hits": hits, "vol": 0})

    # Volume tie-breaker: fetch volume only for symbols with tied scores
    # Group by hit count
    from itertools import groupby
    candidates.sort(key=lambda x: -x["hits"])
    enriched = []
    for hit_count, group in groupby(candidates, key=lambda x: x["hits"]):
        group_list = list(group)
        if len(group_list) > 1:  # tie — fetch volumes
            for item in group_list:
                item["vol"] = _get_rolling_volume(item["sym"], cutoff)
        enriched.extend(group_list)

    # Final sort: primary = hits DESC, secondary = volume DESC
    enriched.sort(key=lambda x: (-x["hits"], -x["vol"]))

    top_1m   = set(e["sym"] for e in enriched[:TOP_1M])
    top_3m   = set(e["sym"] for e in enriched[:TOP_3M])
    combined = top_1m | top_3m

    master = [s for s in stocks_5m if s in combined]
    log.info(
        f"[ROLLING] Master={len(master)} | "
        f"Top5: {[e['sym'] for e in enriched[:5]]} | "
        f"Window={ROLLING_WINDOW}m cutoff={cutoff.strftime('%H:%M')}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# SMART ENTRY ENGINE
# ─────────────────────────────────────────────────────────────────────────────

def _check_exhaustion(candles: list) -> bool:
    if len(candles) < 6: return False
    sig  = abs(candles[-1]["close"] - candles[-1]["open"])
    avg  = sum(abs(c["close"] - c["open"]) for c in candles[-6:-1]) / 5
    return avg > 0 and sig >= avg * EXHAUSTION_MULT


def _check_ema_guard(candles: list, signal_price: float) -> bool:
    """Return True (REJECT) if price is >4% above EMA9."""
    ema9 = _calculate_ema9(candles)
    if ema9 is None or ema9 == 0: return False
    dist = (signal_price - ema9) / ema9
    if dist > EMA_GUARD_PCT:
        log.info(f"EMA GUARD: {dist*100:.1f}% above EMA9 → REJECT")
        return True
    return False


def _evaluate_smart_entry(symbol: str):
    meta = pending.get(symbol)
    if not meta: return

    now     = datetime.now()
    elapsed = (now - meta["queued_at"]).total_seconds() / 60

    if elapsed >= SMART_ENTRY_MINS:
        log.info(f"SMART ENTRY TIMEOUT: {symbol}")
        pending.pop(symbol, None); return

    if len(positions) >= MAX_SLOTS:
        pending.pop(symbol, None); return

    candles = _get_1min_candles(symbol, n=2)
    if not candles: return

    c       = candles[-1]
    is_red  = c["close"] < c["open"]

    if is_red:
        drop = (c["open"] - c["close"]) / c["open"]
        if drop <= MAX_RED_CANDLE_PCT:
            _execute_entry(symbol, c["close"])
        else:
            log.info(f"REVERSAL CANCEL: {symbol} drop={drop*100:.2f}%")
            pending.pop(symbol, None)
    elif c["close"] > meta["signal_high"]:
        _execute_entry(symbol, c["close"])


def _execute_entry(symbol: str, price: float):
    pending.pop(symbol, None)
    qty = max(1, int(SLOT_CAPITAL / price))
    _place_order(symbol, qty, "BUY")
    positions[symbol] = {
        "entry":      price,
        "entry_time": datetime.now(),
        "peak":       price,
        "sl":         round(price * (1 - SL_PCT), 2),
        "tsl_on":     False,
        "qty":        qty,
    }
    log.info(f"POSITION OPEN {symbol} qty={qty} entry={price:.2f} sl={positions[symbol]['sl']:.2f}")


# ─────────────────────────────────────────────────────────────────────────────
# WATCHDOG
# ─────────────────────────────────────────────────────────────────────────────

def watchdog():
    eod_saved = False
    while True:
        try:
            now = datetime.now()

            # Smart entry checks
            for sym in list(pending):
                _evaluate_smart_entry(sym)

            # Position management
            for sym in list(positions):
                pos = positions[sym]

                if not _market_open(now):
                    ltp = _get_ltp(sym) or pos["entry"]
                    _exit(sym, pos, "MARKET_CLOSE", ltp)
                    continue

                ltp = _get_ltp(sym)
                if ltp is None: continue

                if ltp > pos["peak"]: pos["peak"] = ltp

                if not pos["tsl_on"] and ltp >= pos["entry"] * (1 + TSL_TRIGGER_PCT):
                    pos["tsl_on"] = True
                    log.info(f"TSL ACTIVATED {sym} ltp={ltp:.2f}")

                if pos["tsl_on"]:
                    new_floor = pos["peak"] * (1 - TSL_TRAIL_PCT)
                    if new_floor > pos["sl"]: pos["sl"] = new_floor

                mins_held = (now - pos["entry_time"]).total_seconds() / 60
                reason = None
                if ltp <= pos["sl"]:             reason = "SL_HIT"
                elif mins_held >= TIME_EXIT_MINS: reason = "TIME_EXIT"
                if reason: _exit(sym, pos, reason, ltp)

            # EOD square-off at 15:15
            eod = now.replace(hour=15, minute=15, second=0, microsecond=0)
            if now >= eod and not eod_saved:
                for sym in list(positions):
                    ltp = _get_ltp(sym) or positions[sym]["entry"]
                    _exit(sym, positions[sym], "EOD", ltp)
                _save_eod_report()
                eod_saved = True
            if now.hour < 15: eod_saved = False  # reset for next day

        except Exception as exc:
            log.exception(f"Watchdog error: {exc}")
        time.sleep(1)


# ─────────────────────────────────────────────────────────────────────────────
# WEBHOOKS
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/")
def health():
    total_pnl = sum(t["pnl_rs"] for t in trades_today)
    return jsonify({
        "status":      "ONLINE",
        "mode":        TRADING_MODE,
        "master":      master,
        "positions":   {s: {
            "entry":     p["entry"],
            "entry_time":p["entry_time"].strftime("%H:%M"),
            "sl":        p["sl"],
            "tsl_on":    p["tsl_on"],
            "peak":      p["peak"],
        } for s, p in positions.items()},
        "pending":     list(pending.keys()),
        "trades_today":trades_today,
        "total_pnl_rs":round(total_pnl, 2),
    })


@app.post("/webhook/1min")
def wh_1min():
    stocks = _parse_stocks(request)
    now    = datetime.now()
    for s in stocks[:TOP_1M]:
        pool_1m.setdefault(s, []).append(now)
    log.info(f"1m hit: {stocks[:5]}")
    return jsonify(status="ok")


@app.post("/webhook/3min")
def wh_3min():
    stocks = _parse_stocks(request)
    now    = datetime.now()
    for s in stocks[:TOP_3M]:
        pool_3m.setdefault(s, []).append(now)
    log.info(f"3m hit: {stocks[:5]}")
    return jsonify(status="ok")


@app.post("/webhook/5min")
def wh_5min():
    """Rolling 5-min refresh of Master List."""
    stocks_5m = _parse_stocks(request)
    now = datetime.now()
    # Only build master after 09:40
    if now.hour > 9 or (now.hour == 9 and now.minute >= 40):
        _rebuild_master(stocks_5m)
    return jsonify(status="ok", master=master)


@app.post("/webhook/final_buy")
def wh_final_buy():
    stocks  = _parse_stocks(request)
    queued  = []
    skipped = []
    now     = datetime.now()

    # Ignore pre-09:40 and 09:15 ghost signals
    if not _market_open(now) or (now.hour == 9 and now.minute < 40):
        return jsonify(status="ok", skipped=["pre_start"])

    for symbol in stocks:
        if symbol in BLACKLIST:
            skipped.append({"symbol": symbol, "reason": "BLACKLISTED"}); continue
        if symbol not in master:
            skipped.append({"symbol": symbol, "reason": "NOT_IN_MASTER"}); continue
        if len(positions) + len(pending) >= MAX_SLOTS:
            break
        if symbol in positions or symbol in pending:
            continue

        candles = _get_1min_candles(symbol, n=10)
        if not candles:
            skipped.append({"symbol": symbol, "reason": "NO_DATA"}); continue

        # Exhaustion filter
        if _check_exhaustion(candles):
            skipped.append({"symbol": symbol, "reason": "EXHAUSTION"}); continue

        # EMA Guard (4% rule)
        if _check_ema_guard(candles, candles[-1]["close"]):
            skipped.append({"symbol": symbol, "reason": "EMA_EXTENDED"}); continue

        pending[symbol] = {
            "queued_at":   now,
            "signal_high": candles[-1]["high"],
        }
        log.info(f"QUEUED {symbol} signal_high={candles[-1]['high']:.2f}")
        queued.append(symbol)

    return jsonify(
        status="ok",
        queued=queued,
        skipped=skipped,
        positions=len(positions),
        pending=len(pending),
    )


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    threading.Thread(target=watchdog, daemon=True).start()
    log.info(f"Sniper Bot v4.0 starting | mode={TRADING_MODE} | TSL={TSL_TRIGGER_PCT*100}% | SL={SL_PCT*100}% | EMA={EMA_GUARD_PCT*100}%")
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))

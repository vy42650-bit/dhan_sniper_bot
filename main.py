import os
import logging
import threading
import time
from datetime import datetime, timedelta
from flask import Flask, request, jsonify
from dhanhq import dhanhq

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ── Config (set in Railway Variables) ────────────────────────────────────────
CLIENT_ID     = os.getenv("DHAN_CLIENT_ID",    "1105120853")
ACCESS_TOKEN  = os.getenv("DHAN_ACCESS_TOKEN", "")
TRADING_MODE  = os.getenv("TRADING_MODE",      "SANDBOX")   # SANDBOX | LIVE

# ── Strategy Constants ────────────────────────────────────────────────────────
BLACKLIST       = {"MEESHO", "MEESHO-BE"}   # Compliance hard-block
MAX_SLOTS       = 8                         # Maximum concurrent positions
SL_PCT          = 0.013                     # 1.3 % stop-loss below entry
TSL_TRIGGER_PCT = 0.030                     # Activate trailing SL at +3 %
TSL_TRAIL_PCT   = 0.020                     # Trail 2 % below peak
TIME_EXIT_MINS  = 45                        # Hard square-off after 45 min
BLOCK_MINS      = 25                        # Candidate pool resets every 25 min
TOP_1M          = 15                        # Top N from 1-min scanner
TOP_3M          = 25                        # Top N from 3-min scanner
TOP_MASTER      = 15                        # Final master list size

# ── State ─────────────────────────────────────────────────────────────────────
pool_1m:   dict[str, int] = {}   # symbol -> hit count from 1-min webhooks
pool_3m:   dict[str, int] = {}   # symbol -> hit count from 3-min webhooks
master:    list[str]       = []  # validated Top-15 for current block
positions: dict            = {}  # symbol -> position dict
block_ts   = datetime.now()

# ── Dhan client ───────────────────────────────────────────────────────────────
try:
    from dhanhq import DhanContext
    dhan = dhanhq(DhanContext(CLIENT_ID, ACCESS_TOKEN))
except ImportError:
    dhan = dhanhq(CLIENT_ID, ACCESS_TOKEN)  # fallback for older SDK

# ── Flask app ─────────────────────────────────────────────────────────────────
app = Flask(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# WATCHDOG  –  runs in background, checks SL / TSL / time-exit every second
# ─────────────────────────────────────────────────────────────────────────────
def watchdog():
    global pool_1m, pool_3m, block_ts
    while True:
        try:
            now = datetime.now()

            # 25-minute block reset
            if (now - block_ts).total_seconds() >= BLOCK_MINS * 60:
                pool_1m.clear()
                pool_3m.clear()
                block_ts = now
                log.info("=== BLOCK RESET: candidate pools cleared ===")

            # Monitor every open position
            for sym in list(positions):
                pos = positions[sym]

                # Skip if market is closed (before 9:15 or after 15:30 IST)
                if now.hour < 9 or (now.hour == 9 and now.minute < 15):
                    continue
                if now.hour > 15 or (now.hour == 15 and now.minute >= 30):
                    _exit(sym, pos, "MARKET_CLOSE", pos["entry"])
                    continue

                ltp = _get_ltp(sym)
                if ltp is None:
                    continue

                # Update peak price
                if ltp > pos["peak"]:
                    pos["peak"] = ltp

                # Activate trailing SL when profit >= TSL_TRIGGER_PCT
                if not pos["tsl_on"] and ltp >= pos["entry"] * (1 + TSL_TRIGGER_PCT):
                    pos["tsl_on"] = True
                    log.info(f"TSL ACTIVATED  {sym}  ltp={ltp:.2f}")

                # Slide TSL floor up
                if pos["tsl_on"]:
                    new_floor = pos["peak"] * (1 - TSL_TRAIL_PCT)
                    if new_floor > pos["sl"]:
                        pos["sl"] = new_floor

                # Evaluate exit conditions
                mins_held = (now - pos["entry_time"]).total_seconds() / 60
                reason = None
                if ltp <= pos["sl"]:             reason = "SL_HIT"
                elif mins_held >= TIME_EXIT_MINS: reason = "TIME_EXIT"

                if reason:
                    _exit(sym, pos, reason, ltp)

        except Exception as exc:
            log.exception(f"Watchdog error: {exc}")

        time.sleep(1)


def _get_ltp(symbol: str) -> float | None:
    """Fetch last traded price from Dhan. Returns None on error."""
    try:
        resp = dhan.get_quote(symbol)          # adjust if API differs
        return float(resp["data"]["last_price"])
    except Exception:
        return None


def _place_order(symbol: str, qty: int, side: str) -> dict:
    """Place an order; in SANDBOX mode just log and return a mock response."""
    if TRADING_MODE != "LIVE":
        log.info(f"[SANDBOX] {side} {qty} x {symbol}")
        return {"status": "SANDBOX_OK", "symbol": symbol, "side": side}
    order = dhan.place_order(
        security_id=_resolve_security_id(symbol),
        exchange_segment=dhan.NSE,
        transaction_type=dhan.BUY if side == "BUY" else dhan.SELL,
        quantity=qty,
        order_type=dhan.MARKET,
        product_type=dhan.INTRADAY,
        price=0,
    )
    return order


def _resolve_security_id(symbol: str) -> str:
    """Placeholder: map trading symbol -> Dhan security_id. 
    In production, load from security_map.csv at startup."""
    return symbol   # replace with real lookup


def _exit(symbol: str, pos: dict, reason: str, exit_price: float):
    """Square off a position and remove it from the portfolio."""
    pnl = (exit_price - pos["entry"]) * pos["qty"]
    log.info(
        f"EXIT {symbol}  reason={reason}  "
        f"entry={pos['entry']:.2f}  exit={exit_price:.2f}  "
        f"pnl={pnl:+.2f}"
    )
    _place_order(symbol, pos["qty"], "SELL")
    positions.pop(symbol, None)


# ─────────────────────────────────────────────────────────────────────────────
# WEBHOOK ENDPOINTS  –  called by Chartink Premium alerts
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/")
def health():
    return jsonify({
        "status":    "ONLINE",
        "mode":      TRADING_MODE,
        "positions": len(positions),
        "master":    master,
        "pool_1m":   dict(sorted(pool_1m.items(), key=lambda x: -x[1])[:5]),
        "pool_3m":   dict(sorted(pool_3m.items(), key=lambda x: -x[1])[:5]),
    })


@app.post("/webhook/1min")
def wh_1min():
    """Chartink 1-min scanner fires here.  
    Payload: { "stocks": "SBI,RELIANCE,HDFC" }"""
    stocks = _parse_stocks(request)
    for s in stocks[:TOP_1M]:
        pool_1m[s] = pool_1m.get(s, 0) + 1
    log.info(f"1-min hit: {stocks[:5]}…  pool size={len(pool_1m)}")
    return jsonify(status="ok")


@app.post("/webhook/3min")
def wh_3min():
    """Chartink 3-min scanner fires here."""
    stocks = _parse_stocks(request)
    for s in stocks[:TOP_3M]:
        pool_3m[s] = pool_3m.get(s, 0) + 1
    log.info(f"3-min hit: {stocks[:5]}…  pool size={len(pool_3m)}")
    return jsonify(status="ok")


@app.post("/webhook/5min")
def wh_5min():
    """Chartink 5-min scanner fires here. Computes and updates master list."""
    global master
    stocks_5m = _parse_stocks(request)

    # Build combined candidate pool from top-N of each scanner
    top15_1m = {s for s, _ in sorted(pool_1m.items(), key=lambda x: -x[1])[:TOP_1M]}
    top25_3m = {s for s, _ in sorted(pool_3m.items(), key=lambda x: -x[1])[:TOP_3M]}
    combined = top15_1m | top25_3m

    # Keep only those that also appear in the 5-min scanner output
    master = [s for s in stocks_5m if s in combined][:TOP_MASTER]
    log.info(f"Master list updated ({len(master)} stocks): {master}")
    return jsonify(status="ok", master=master)


@app.post("/webhook/final_buy")
def wh_final_buy():
    """Chartink FINAL BUY scanner fires here. Places entry if conditions met."""
    stocks = _parse_stocks(request)

    entered = []
    for symbol in stocks:
        # ── Compliance block ──────────────────────────────────────────────
        if symbol.upper() in BLACKLIST:
            log.warning(f"COMPLIANCE BLOCK: {symbol} is blacklisted")
            continue

        # ── Must be in validated master list ──────────────────────────────
        if symbol not in master:
            log.info(f"SKIP {symbol}: not in master list")
            continue

        # ── Portfolio slot check ───────────────────────────────────────────
        if len(positions) >= MAX_SLOTS:
            log.info(f"SKIP {symbol}: portfolio full ({MAX_SLOTS}/{MAX_SLOTS})")
            break

        # ── No duplicate ───────────────────────────────────────────────────
        if symbol in positions:
            continue

        # ── Market hours guard (09:15 – 15:15 IST) ────────────────────────
        now = datetime.now()
        if not _market_open(now):
            log.warning(f"SKIP {symbol}: outside market hours")
            continue

        # ── Fetch entry price and place order ─────────────────────────────
        ltp = _get_ltp(symbol)
        if ltp is None:
            log.error(f"SKIP {symbol}: could not fetch LTP")
            continue

        qty = max(1, int(50000 / ltp))   # ~₹50k per slot
        _place_order(symbol, qty, "BUY")

        positions[symbol] = {
            "entry":      ltp,
            "entry_time": now,
            "peak":       ltp,
            "sl":         round(ltp * (1 - SL_PCT), 2),
            "tsl_on":     False,
            "qty":        qty,
        }
        log.info(
            f"BUY {symbol}  qty={qty}  entry={ltp:.2f}  "
            f"sl={positions[symbol]['sl']:.2f}"
        )
        entered.append(symbol)

    return jsonify(status="ok", entered=entered)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _parse_stocks(req) -> list[str]:
    """Accept both JSON and form payloads from Chartink."""
    try:
        data = req.get_json(force=True, silent=True) or {}
        raw  = data.get("stocks", data.get("stock", ""))
    except Exception:
        raw = ""
    return [s.strip().upper() for s in raw.split(",") if s.strip()]


def _market_open(now: datetime) -> bool:
    if now.weekday() >= 5:          # Saturday / Sunday
        return False
    start = now.replace(hour=9,  minute=15, second=0, microsecond=0)
    end   = now.replace(hour=15, minute=15, second=0, microsecond=0)
    return start <= now <= end


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # Start watchdog in background
    t = threading.Thread(target=watchdog, daemon=True)
    t.start()
    log.info(f"Sniper Bot starting  mode={TRADING_MODE}")
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)

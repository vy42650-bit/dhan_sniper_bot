# DHAN SNIPER BOT v4.0 - STABLE PRODUCTION BUILD
import os, logging, threading, time, csv, json
from datetime import datetime, timedelta
from flask import Flask, request, jsonify
from dhanhq import dhanhq
import pandas as pd

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ── Configuration ─────────────────────────────────────────────────────────────
# MAIN APP (For Market Data)
MAIN_CLIENT_ID    = "1105120853"
MAIN_ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzc4NTk3OTI2LCJpYXQiOjE3Nzg1MTE1MjYsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTA1MTIwODUzIn0.dFWhEopjMzjdrBOPbv1yXNzHJqn7AEDUy_Ett-e8TOKlWTgSaj0j_vaxsg9lFNnk0Veg2E_uYVvdpXRG1IuwYg"

# SANDBOX (For Order Execution)
SANDBOX_CLIENT_ID    = "2605019607"
SANDBOX_ACCESS_TOKEN = "eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJ0b2tlbkNvbnN1bWVyVHlwZSI6IlNFTEYiLCJwYXJ0bmVySWQiOiIiLCJkaGFuQ2xpZW50SWQiOiIyNjA1MDE5NjA3Iiwid2ViaG9va1VybCI6IiIsImlzcyI6ImRoYW4iLCJleHAiOjE3Nzg1OTg2MjV9.51ENYq_S8LqRQdJ8QEGstmnZPa5zvxhxBofGEVqW3tkXLjnIkchHVmwial5HM7hkO5fA7YIeo1ZzuxMT9pbmsA"

TRADING_MODE = os.getenv("TRADING_MODE", "SANDBOX")  # SANDBOX | LIVE

# ── Strategy Constants ────────────────────────────────────────────────────────
BLACKLIST          = {"MEESHO", "MEESHO-BE"}
MAX_SLOTS          = 8
SL_PCT             = 0.013    # 1.3% hard stop
TSL_TRIGGER_PCT    = 0.020    # 2.0% trigger
TSL_TRAIL_PCT      = 0.020    # 2.0% trail
TIME_EXIT_MINS     = 45
ROLLING_WINDOW     = 25       
REFRESH_MINS       = 5        
TOP_1M             = 15
TOP_3M             = 25
SLOT_CAPITAL       = 50000
MAX_RED_CANDLE_PCT = 0.008    # 0.8% healthy dip
EXHAUSTION_MULT    = 3.0
SMART_ENTRY_MINS   = 3
EMA_GUARD_PCT      = 0.04     # Reject if >4% above EMA9

# ── State ─────────────────────────────────────────────────────────────────────
pool_1m:   dict = {}   # {sym: [timestamps]}
pool_3m:   dict = {}   # {sym: [timestamps]}
pool_5m:   dict = {}   # {sym: [timestamps]}
volumes:   dict = {}   # {sym: absolute_volume}
master:    list = []
positions: dict = {}
pending:   dict = {}
trades_today: list = []

MASTER_FILE = "master_list.json"

def _save_master():
    try:
        with open(MASTER_FILE, "w") as f:
            json.dump(master, f)
    except: pass

def _load_master():
    global master
    try:
        if os.path.exists(MASTER_FILE):
            with open(MASTER_FILE, "r") as f:
                master = json.load(f)
            log.info(f"Restored Master List: {len(master)} stocks")
    except: pass

# ── Dhan Clients ──────────────────────────────────────────────────────────────
try:
    from dhanhq import DhanContext
    
    # DATA CLIENT (Main Token for real-time market data)
    data_ctx = DhanContext(MAIN_CLIENT_ID, MAIN_ACCESS_TOKEN)
    dhan_data = dhanhq(data_ctx)
    
    # ORDER CLIENT (Sandbox Token for simulated execution)
    sb_ctx = DhanContext(SANDBOX_CLIENT_ID, SANDBOX_ACCESS_TOKEN)
    dhan_orders = dhanhq(sb_ctx)
    
    # CRITICAL: Manually override to point to Sandbox API
    if hasattr(dhan_orders, 'dhan_http'):
        dhan_orders.dhan_http.base_url = "https://sandbox.dhan.co/v2"
        
    log.info("Dhan Hybrid Clients Initialized: Data (Main) | Orders (Sandbox)")
except Exception as e:
    log.error(f"Dhan init failed: {e}")
    dhan_data = None
    dhan_orders = None

# ── Security Map ──────────────────────────────────────────────────────────────
_SECURITY_MAP: dict = {}
try:
    _map_path = os.path.join(os.path.dirname(__file__), "security_map.csv")
    if os.path.exists(_map_path):
        _df = pd.read_csv(_map_path, low_memory=False)
        _eq = _df[_df["SEM_INSTRUMENT_NAME"] == "EQUITY"]
        for _, row in _eq.iterrows():
            _SECURITY_MAP[str(row["SEM_TRADING_SYMBOL"]).upper()] = str(row["SEM_SMST_SECURITY_ID"])
        log.info(f"Security map loaded: {len(_SECURITY_MAP)} equity symbols")
except Exception as e:
    log.warning(f"Security map load failed: {e}")

app = Flask(__name__)

# ── Helpers ───────────────────────────────────────────────────────────────────
def _resolve_security_id(symbol: str) -> str:
    return _SECURITY_MAP.get(symbol.upper(), symbol)

def _get_ltp(symbol: str) -> float | None:
    try:
        resp = dhan.get_quote(symbol)
        return float(resp["data"]["last_price"])
    except: return None

def _get_1min_candles(symbol: str, n: int = 10) -> list | None:
    try:
        sec_id = _resolve_security_id(symbol)
        today = datetime.now().strftime("%Y-%m-%d")
        resp = dhan_data.intraday_minute_data(sec_id, "NSE_EQ", "EQUITY", today, today)
        if resp.get("status") == "success" and resp.get("data"):
            d = resp["data"]
            # Convert dictionary of lists to list of dictionaries
            # Dhan V2 format: {'open': [...], 'high': [...], 'timestamp': [...]}
            ts_key = 'timestamp' if 'timestamp' in d else 'start_Time'
            l = len(d.get('close', []))
            if l == 0: return None
            
            candles = []
            for i in range(l):
                candles.append({
                    'open': d['open'][i], 'high': d['high'][i],
                    'low': d['low'][i], 'close': d['close'][i],
                    'volume': d.get('volume', [0]*l)[i],
                    'time': d.get(ts_key, [0]*l)[i]
                })
            return candles[-n:]
        else:
            log.warning(f"Dhan Data API Error for {symbol}: {resp}")
    except Exception as e: 
        log.error(f"Dhan Data API Exception for {symbol}: {e}")
    return None

def _calculate_ema9(candles: list) -> float | None:
    if len(candles) < 9: return None
    closes = [c["close"] for c in candles]
    k = 2 / (9 + 1)
    ema = closes[0]
    for price in closes[1:]:
        ema = price * k + ema * (1 - k)
    return ema

def _place_order(symbol: str, qty: int, side: str):
    log.info(f"PLACING {side} ORDER: {qty} x {symbol} (Target: SANDBOX)")
    try:
        # We manually use the sandbox endpoint if needed, but usually the token determines it
        return dhan_orders.place_order(
            security_id=_resolve_security_id(symbol),
            exchange_segment="NSE_EQ",
            transaction_type="BUY" if side == "BUY" else "SELL",
            quantity=qty,
            order_type="MARKET",
            product_type="INTRA",
            price=0
        )
    except Exception as e:
        log.error(f"Order Placement Failed for {symbol}: {e}")
        return {"status": "error", "message": str(e)}

def _exit(symbol: str, pos: dict, reason: str, exit_price: float):
    pnl_rs = ((exit_price - pos["entry"]) / pos["entry"]) * SLOT_CAPITAL
    log.info(f"EXIT {symbol} {reason} P/L: Rs.{pnl_rs:+.2f}")
    _place_order(symbol, pos["qty"], "SELL")
    trades_today.append({
        "symbol": symbol, "entry_p": pos["entry"], "exit_p": round(exit_price, 2),
        "reason": reason, "pnl_rs": round(pnl_rs, 2), "time": datetime.now().strftime("%H:%M:%S")
    })
    positions.pop(symbol, None)

# ── Core Engine ───────────────────────────────────────────────────────────────
def _get_bulk_volumes(symbols: list):
    if not symbols: return
    try:
        # We fetch volumes in one go to prevent API lag
        # Assuming symbols is a list of strings
        # Dhan get_quote can take a list or we can iterate quickly
        for s in symbols:
            # Check if we already have a recent volume to avoid spamming
            if s not in volumes:
                candles = _get_1min_candles(s, n=1)
                if candles: volumes[s] = candles[-1].get("volume", 0)
    except: pass

def _rebuild_master():
    global master
    now = datetime.now()
    cutoff = now - timedelta(minutes=ROLLING_WINDOW)
    
    def get_top_n(pool, limit):
        # Filter by time and count frequencies
        counts = {}
        for s, t_list in pool.items():
            valid_t = [t for t in t_list if t > cutoff]
            pool[s] = valid_t # Prune pool
            if valid_t: counts[s] = len(valid_t)
            
        if not counts: return []
        
        # Get volumes for all candidates to break ties
        _get_bulk_volumes(list(counts.keys()))
        
        # Sort by (frequency, volume)
        sorted_stocks = sorted(counts.keys(), key=lambda x: (counts[x], volumes.get(x, 0)), reverse=True)
        return sorted_stocks[:limit]

    list_1m = get_top_n(pool_1m, 15)
    list_3m = get_top_n(pool_3m, 20)
    list_5m = get_top_n(pool_5m, 15)
    
    # Selection: (Top 15 of 1m OR Top 20 of 3m) AND Top 15 of 5m
    vetted = set(list_1m) | set(list_3m)
    master = [s for s in list_5m if s in vetted]
    
    _save_master()
    log.info(f"MASTER UPDATED ({len(master)}): {master}")

def _evaluate_smart_entry(symbol: str):
    # (Existing logic remains but remove processed check elsewhere)
    meta = pending.get(symbol)
    if not meta: return
    if (datetime.now() - meta["queued_at"]).total_seconds() / 60 >= SMART_ENTRY_MINS:
        pending.pop(symbol, None); return
    if len(positions) >= MAX_SLOTS: return

    candles = _get_1min_candles(symbol, n=2)
    if not candles: return
    c = candles[-1]
    if c["close"] < c["open"]: # Healthy dip
        if (c["open"] - c["close"]) / c["open"] <= MAX_RED_CANDLE_PCT:
            _execute_entry(symbol, c["close"])
    elif c["close"] > meta["signal_high"]: # Breakout
        _execute_entry(symbol, c["close"])

def _execute_entry(symbol: str, price: float):
    pending.pop(symbol, None)
    qty = max(1, int(SLOT_CAPITAL / price))
    _place_order(symbol, qty, "BUY")
    positions[symbol] = {
        "entry": price, "entry_time": datetime.now(), "peak": price,
        "sl": round(price * (1 - SL_PCT), 2), "tsl_on": False, "qty": qty
    }

def watchdog():
    while True:
        try:
            now = datetime.now()
            for sym in list(pending): _evaluate_smart_entry(sym)
            for sym in list(positions):
                pos = positions[sym]
                ltp = _get_ltp(sym)
                if not ltp: continue
                if ltp > pos["peak"]: pos["peak"] = ltp
                if not pos["tsl_on"] and ltp >= pos["entry"] * (1 + TSL_TRIGGER_PCT):
                    pos["tsl_on"] = True
                if pos["tsl_on"]:
                    floor = pos["peak"] * (1 - TSL_TRAIL_PCT)
                    if floor > pos["sl"]: pos["sl"] = floor
                
                reason = None
                if ltp <= pos["sl"]: reason = "SL_HIT"
                elif (now - pos["entry_time"]).total_seconds() / 60 >= TIME_EXIT_MINS: reason = "TIME_EXIT"
                if reason: _exit(sym, pos, reason, ltp)
        except Exception as e: log.error(f"Watchdog error: {e}")
        time.sleep(1)

# ── Endpoints ─────────────────────────────────────────────────────────────────
@app.get("/")
def dashboard():
    return jsonify({
        "status": "ONLINE", "mode": TRADING_MODE, "master": master,
        "positions": len(positions), "trades": trades_today,
        "total_pnl": round(sum(t["pnl_rs"] for t in trades_today), 2)
    })

@app.post("/webhook/1min")
def wh1():
    stocks = [s.strip().upper() for s in request.get_json(silent=True).get("stocks", "").split(",") if s]
    log.info(f"[WH-1m] Received: {stocks}")
    for s in stocks:
        pool_1m.setdefault(s, []).append(datetime.now())
    return jsonify(status="ok")

@app.post("/webhook/3min")
def wh3():
    stocks = [s.strip().upper() for s in request.get_json(silent=True).get("stocks", "").split(",") if s]
    log.info(f"[WH-3m] Received: {stocks}")
    for s in stocks:
        pool_3m.setdefault(s, []).append(datetime.now())
    return jsonify(status="ok")

@app.post("/webhook/5min")
def wh5():
    stocks = [s.strip().upper() for s in request.get_json(silent=True).get("stocks", "").split(",") if s]
    log.info(f"[WH-5m] Received: {stocks}")
    now = datetime.now()
    for s in stocks:
        pool_5m.setdefault(s, []).append(now)
    return jsonify(status="ok")

@app.post("/webhook/final_buy")
def wh_buy():
    # 09:15 - 09:40 Warm-up block
    now = datetime.now()
    if now.hour == 9 and now.minute < 40:
        log.info("WARM-UP: Signal received but skipping trades until 09:40")
        return jsonify(status="warmup")

    stocks = [s.strip().upper() for s in request.get_json(silent=True).get("stocks", "").split(",") if s]
    log.info(f"[WH-BUY] Received signal for: {stocks}")
    
    # Rebuild master on every buy signal to ensure latest ranking
    _rebuild_master()
    
    for s in stocks:
        if s in master and s not in positions and s not in pending and len(positions) < MAX_SLOTS:
            candles = _get_1min_candles(s, n=10)
            if candles:
                ema9 = _calculate_ema9(candles)
                curr = candles[-1]["close"]
                if ema9 and (curr - ema9)/ema9 <= EMA_GUARD_PCT:
                    pending[s] = {"queued_at": datetime.now(), "signal_high": candles[-1]["high"]}
                    log.info(f"QUEUED: {s} | Signal High: {candles[-1]['high']}")
                else:
                    log.info(f"REJECTED: {s} | Reason: EMA_EXTENDED or NO_EMA")
            else:
                log.info(f"SKIPPED: {s} | Reason: API_NO_CANDLES")
        else:
            reason = "NOT_IN_MASTER_RANKING" if s not in master else "ALREADY_OPEN_OR_PENDING"
            log.info(f"SKIPPED: {s} | Reason: {reason}")
    return jsonify(status="ok")

if __name__ == "__main__":
    _load_master()
    threading.Thread(target=watchdog, daemon=True).start()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))

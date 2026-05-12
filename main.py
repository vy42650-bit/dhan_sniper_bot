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
# DATA CLIENT (Main Token for real-time market data)
def _ensure_dhan_data():
    global dhan_data
    if dhan_data is None:
        try:
            dhan_data = dhanhq(client_id=MAIN_CLIENT_ID, access_token=MAIN_ACCESS_TOKEN)
            log.info("Dhan Data Client initialized successfully via self-healing block.")
        except Exception as e:
            log.error(f"Dhan Data Client init failed: {e}", exc_info=True)
            dhan_data = None

dhan_data = None
_ensure_dhan_data()

# ORDER CLIENT (Sandbox Token for simulated execution)
try:
    dhan_orders = dhanhq(client_id=SANDBOX_CLIENT_ID, access_token=SANDBOX_ACCESS_TOKEN)
    if hasattr(dhan_orders, 'dhan_http'):
        dhan_orders.dhan_http.base_url = "https://sandbox.dhan.co/v2"
    log.info("Dhan Orders Client (Sandbox Token) Initialized Successfully.")
except Exception as e:
    log.error(f"Dhan Orders Client init failed: {e}", exc_info=True)
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
        _ensure_dhan_data()
        if dhan_data is None: return None
        sec_id = _resolve_security_id(symbol)
        if not sec_id.isdigit(): return None
        resp = dhan_data.quote_data({"NSE_EQ": [int(sec_id)]})
        if resp.get("status") == "success":
            data_dict = resp["data"]["data"]["NSE_EQ"][str(sec_id)]
            return float(data_dict["last_price"])
    except Exception as e: pass
    return None

def _get_1min_candles(symbol: str, n: int = 10) -> list | None:
    try:
        _ensure_dhan_data()
        if dhan_data is None:
            log.warning(f"Dhan Data Client offline. Skipping candles for {symbol}")
            return None
        sec_id = _resolve_security_id(symbol)
        today = datetime.now().strftime("%Y-%m-%d")
        resp = dhan_data.intraday_minute_data(sec_id, "NSE_EQ", "EQUITY", today, today)
        if resp.get("status") == "success" and resp.get("data"):
            d = resp["data"]
            ts_key = 'timestamp' if 'timestamp' in d else ('start_Time' if 'start_Time' in d else 'start_time')
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
    _ensure_dhan_data()
    if not symbols or dhan_data is None: return
    try:
        sec_ids = []
        id_to_sym = {}
        for s in symbols:
            if s not in volumes:
                sid = _resolve_security_id(s)
                if sid.isdigit():
                    sec_ids.append(int(sid))
                    id_to_sym[str(sid)] = s
        
        if sec_ids:
            for i in range(0, len(sec_ids), 25):
                chunk = sec_ids[i:i+25]
                resp = dhan_data.quote_data({"NSE_EQ": chunk})
                if resp.get("status") == "success":
                    data_map = resp["data"]["data"]["NSE_EQ"]
                    for sid_str, data_dict in data_map.items():
                        sym = id_to_sym.get(sid_str)
                        if sym: volumes[sym] = int(data_dict.get("volume", 0))
    except: pass

def _rebuild_master():
    global master
    now = datetime.now()
    cutoff = now - timedelta(minutes=ROLLING_WINDOW)
    
    def get_top_n(pool, limit):
        # Filter by time and count frequencies
        counts = {}
        # Iterate over a copy of keys to safely prune pool in-place
        for s in list(pool.keys()):
            valid_t = [t for t in pool[s] if t > cutoff]
            if valid_t:
                pool[s] = valid_t # Prune pool keeping valid timestamps
                counts[s] = len(valid_t)
            else:
                pool.pop(s, None)
            
        if not counts: return []
        
        # Get volumes for all candidates to break ties
        _get_bulk_volumes(list(counts.keys()))
        
        # Sort by (frequency, volume)
        sorted_stocks = sorted(counts.keys(), key=lambda x: (counts[x], volumes.get(x, 0)), reverse=True)
        return sorted_stocks[:limit]

    list_1m = get_top_n(pool_1m, 15)
    list_3m = get_top_n(pool_3m, 20)
    
    # Combined universe of top 1m (15) and top 3m (20) candidates (max 35 stocks)
    combined_35 = list(set(list_1m) | set(list_3m))
    
    # Rank candidates from combined_35 against pool_5m arrival frequencies
    counts_5m = {}
    for s in combined_35:
        valid_t = [t for t in pool_5m.get(s, []) if t > cutoff]
        if valid_t: counts_5m[s] = len(valid_t)
    
    if counts_5m:
        _get_bulk_volumes(list(counts_5m.keys()))
        sorted_5m = sorted(counts_5m.keys(), key=lambda x: (counts_5m[x], volumes.get(x, 0)), reverse=True)
        master = sorted_5m[:15]
    else:
        master = combined_35[:15]
    
    _save_master()
    log.info(f"MASTER UPDATED ({len(master)}): {master}")

def _evaluate_smart_entry(symbol: str):
    meta = pending.get(symbol)
    if not meta: return
    if (datetime.now() - meta["queued_at"]).total_seconds() / 60 >= SMART_ENTRY_MINS:
        log.info(f"PENDING EXPIRED: {symbol} exceeded 3-minute entry window.")
        pending.pop(symbol, None); return
    if len(positions) >= MAX_SLOTS: return

    ltp = _get_ltp(symbol)
    if not ltp: return
    
    # Breakout entry logic: if live price crosses signal high
    if ltp > meta["signal_high"]:
        log.info(f"🚀 BREAKOUT ENTRY TRIGGERED for {symbol} at {ltp} (Signal High: {meta['signal_high']})")
        _execute_entry(symbol, ltp)
    # Healthy dip entry logic: if live price dips below signal close but stays within max dip limit
    elif ltp < meta["signal_close"]:
        dip_pct = (meta["signal_close"] - ltp) / meta["signal_close"]
        if dip_pct <= MAX_RED_CANDLE_PCT:
            log.info(f"📉 HEALTHY DIP ENTRY TRIGGERED for {symbol} at {ltp} (Dip: {dip_pct*100:.2f}%)")
            _execute_entry(symbol, ltp)

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
            # Use list() to create a copy of keys so we can mutate dictionaries safely during iteration
            for sym in list(pending.keys()): _evaluate_smart_entry(sym)
            for sym in list(positions.keys()):
                if sym not in positions: continue
                pos = positions[sym]
                # Avoid calling live quote API continuously in loops if data client supports quote
                # Or retrieve quote safely. Let's ensure _get_ltp works correctly.
                ltp = _get_ltp(sym)
                if not ltp: continue
                if ltp > pos["peak"]: pos["peak"] = ltp
                if not pos["tsl_on"] and ltp >= pos["entry"] * (1 + TSL_TRIGGER_PCT):
                    pos["tsl_on"] = True
                    log.info(f"TSL ACTIVATED for {sym} at {ltp}")
                if pos["tsl_on"]:
                    floor = pos["peak"] * (1 - TSL_TRAIL_PCT)
                    if floor > pos["sl"]: pos["sl"] = round(floor, 2)
                
                reason = None
                if ltp <= pos["sl"]: reason = "SL_HIT"
                elif (now - pos["entry_time"]).total_seconds() / 60 >= TIME_EXIT_MINS: reason = "TIME_EXIT"
                if reason: _exit(sym, pos, reason, ltp)
        except Exception as e: log.error(f"Watchdog error: {e}")
        time.sleep(1)

# ── Endpoints ─────────────────────────────────────────────────────────────────
@app.get("/")
def dashboard():
    safe_pos = {}
    for sym, p in positions.items():
        safe_pos[sym] = {k: (v.isoformat() if isinstance(v, datetime) else v) for k, v in p.items()}
    return jsonify({
        "status": "ONLINE", "mode": TRADING_MODE, "master": master,
        "positions": safe_pos, "pending": list(pending.keys()),
        "pools": {"1m": len(pool_1m), "3m": len(pool_3m), "5m": len(pool_5m)},
        "trades": trades_today, "total_pnl": round(sum(t["pnl_rs"] for t in trades_today), 2)
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
        if s in master and s not in positions and len(positions) < MAX_SLOTS:
            candles = _get_1min_candles(s, n=10)
            if candles:
                ema9 = _calculate_ema9(candles)
                curr = candles[-1]["close"]
                if ema9 and (curr - ema9)/ema9 <= EMA_GUARD_PCT:
                    log.info(f"🎯 HIGH-CONVICTION SIGNAL VERIFIED: Executing Immediate Entry for {s} at {curr}")
                    _execute_entry(s, curr)
                else:
                    log.info(f"REJECTED: {s} | Reason: EMA_EXTENDED (>4% above EMA9)")
            else:
                log.info(f"SKIPPED: {s} | Reason: API_NO_CANDLES")
        else:
            reason = "NOT_IN_MASTER_RANKING" if s not in master else "ALREADY_OPEN"
            log.info(f"SKIPPED: {s} | Reason: {reason}")
    return jsonify(status="ok")

if __name__ == "__main__":
    _load_master()
    threading.Thread(target=watchdog, daemon=True).start()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))

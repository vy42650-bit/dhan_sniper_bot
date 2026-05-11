
import pandas as pd
import json, os, time
from datetime import datetime, timedelta
from dhanhq import dhanhq

# --- Config ---
TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzc4NTk3OTI2LCJpYXQiOjE3Nzg1MTE1MjYsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTA1MTIwODUzIn0.dFWhEopjMzjdrBOPbv1yXNzHJqn7AEDUy_Ett-e8TOKlWTgSaj0j_vaxsg9lFNnk0Veg2E_uYVvdpXRG1IuwYg"
CLIENT_ID = "1105120853"
dhan = dhanhq(CLIENT_ID, TOKEN)

# --- Params ---
SL_PCT = 0.013; TSL_TRIGGER = 0.02; TSL_TRAIL = 0.02; EMA_GUARD_PCT = 0.04; DIP_PCT = 0.008; MAX_SLOTS = 8

DOWNLOAD_PATH = r"C:\Users\cavis\Downloads"
F_1M = os.path.join(DOWNLOAD_PATH, "Backtest 1 min scanner new (1).csv")
F_3M = os.path.join(DOWNLOAD_PATH, "Backtest 3 min timeframe (7).csv")
F_5M = os.path.join(DOWNLOAD_PATH, "Backtest NEW 5 MIIN SCAN (19).csv")
F_BUY = os.path.join(DOWNLOAD_PATH, "Backtest 5 MIn buy scanner (1).csv")

# Load Security Map
_SECURITY_MAP = {}
if os.path.exists('security_map.csv'):
    df_map = pd.read_csv('security_map.csv', low_memory=False)
    df_map = df_map[df_map["SEM_INSTRUMENT_NAME"] == "EQUITY"]
    for _, row in df_map.iterrows():
        _SECURITY_MAP[str(row["SEM_TRADING_SYMBOL"]).upper()] = str(row["SEM_SMST_SECURITY_ID"])

# Cache for Dhan Data
data_cache = {}
volume_cache = {}

def get_data(sym):
    if sym in data_cache: return data_cache[sym]
    sid = _SECURITY_MAP.get(sym.upper())
    if not sid: return None
    today = "2026-05-11"
    resp = dhan.intraday_minute_data(sid, "NSE_EQ", "EQUITY", today, today)
    if resp.get("status") == "success" and resp.get("data") and 'close' in resp['data']:
        d = resp["data"]; l = len(d['close']); ts = d.get('timestamp', [0]*l)
        candles = []
        for i in range(l):
            candles.append({'time': ts[i], 'open': d['open'][i], 'high': d['high'][i], 'low': d['low'][i], 'close': d['close'][i], 'volume': d['volume'][i]})
        data_cache[sym] = candles
        volume_cache[sym] = candles[-1]['volume'] if candles else 0
        return candles
    return None

def calc_ema(prices, n=9):
    if len(prices) < n: return None
    k = 2/(n+1); ema = prices[0]
    for p in prices[1:]: ema = p*k + ema*(1-k)
    return ema

# 1. Pre-load all CSVs
df1 = pd.read_csv(F_1M); df1['date'] = pd.to_datetime(df1['date'], dayfirst=True)
df3 = pd.read_csv(F_3M); df3['date'] = pd.to_datetime(df3['date'], dayfirst=True)
df5 = pd.read_csv(F_5M); df5['date'] = pd.to_datetime(df5['date'], dayfirst=True)
dfb = pd.read_csv(F_BUY); dfb['date'] = pd.to_datetime(dfb['date'], dayfirst=True)

# 2. Master Funnel Logic
def get_master_at_time(target_time):
    cutoff = target_time - timedelta(minutes=25)
    
    def get_top_n(df, limit):
        sub = df[(df['date'] > cutoff) & (df['date'] <= target_time)]
        if sub.empty: return []
        counts = sub['symbol'].value_counts().to_dict()
        # Sort by (frequency, volume)
        # For backtest, we'll pre-fetch volumes for candidates if needed, but for speed 
        # we'll just use frequency here and volume as secondary if we have it.
        sorted_stocks = sorted(counts.keys(), key=lambda x: (counts[x], volume_cache.get(x, 0)), reverse=True)
        return sorted_stocks[:limit]

    l1 = get_top_n(df1, 15)
    l3 = get_top_n(df3, 20)
    l5 = get_top_n(df5, 15)
    
    vetted = set(l1) | set(l3)
    return [s for s in l5 if s in vetted]

# 3. Chronological Simulation
all_events = []
# Pre-fetch some volumes for common stocks
for s in dfb['symbol'].unique()[:50]: get_data(s)

dfb = dfb.sort_values('date')
active_trades = []
final_results = []

print("Starting Power-Scorer Simulation...")

for _, b_row in dfb.iterrows():
    sym = str(b_row['symbol']).strip().upper()
    sig_time = b_row['date']
    
    # 09:15 - 09:40 Warm-up
    if sig_time.hour == 9 and sig_time.minute < 40: continue
    
    # Clean slots
    active_trades = [t for t in active_trades if t['exit_time'] > sig_time]
    if len(active_trades) >= MAX_SLOTS: continue
    
    # Check Funnel
    master = get_master_at_time(sig_time)
    if sym not in master: continue
    
    # Tech Check
    data = get_data(sym)
    if not data: continue
    sig_idx = -1
    for i, c in enumerate(data):
        c_time = datetime.fromtimestamp(c['time'])
        if c_time.hour == sig_time.hour and c_time.minute == sig_time.minute:
            sig_idx = i; break
    if sig_idx == -1: continue
    
    hist_closes = [c['close'] for c in data[max(0, sig_idx-20):sig_idx+1]]
    ema9 = calc_ema(hist_closes)
    if not ema9 or (data[sig_idx]['close'] - ema9)/ema9 > EMA_GUARD_PCT: continue
    
    entry_p = None; sig_high = data[sig_idx]['high']
    for j in range(sig_idx+1, min(len(data), sig_idx+4)):
        c = data[j]
        if c['close'] < c['open'] and (c['open'] - c['close'])/c['open'] <= DIP_PCT: entry_p = c['close']; break
        if c['close'] > sig_high: entry_p = c['close']; break
    if not entry_p: continue
    
    # Trade Execution
    peak = entry_p; sl = entry_p * (1 - SL_PCT); tsl_on = False; reason = "TIME_EXIT"
    exit_time = datetime.fromtimestamp(data[min(len(data)-1, j+45)]['time'])
    exit_p = data[min(len(data)-1, j+45)]['close']
    
    for k in range(j+1, min(len(data), j+46)):
        c = data[k]
        if c['high'] > peak: peak = c['high']
        if not tsl_on and c['close'] >= entry_p * (1 + TSL_TRIGGER): tsl_on = True
        if tsl_on:
            floor = peak * (1 - TSL_TRAIL)
            if floor > sl: sl = floor
        if c['low'] <= sl:
            exit_time = datetime.fromtimestamp(c['time']); exit_p = sl; reason = "STOP_LOSS"; break
            
    pnl = (exit_p - entry_p)/entry_p
    active_trades.append({"sym": sym, "exit_time": exit_time})
    final_results.append({
        "sym": sym, "entry_t": sig_time.strftime("%H:%M"), "exit_t": exit_time.strftime("%H:%M"),
        "entry_p": entry_p, "exit_p": exit_p, "reason": reason, "pnl": round(pnl*100, 2)
    })

# 4. Generate Final CSV
res_df = pd.DataFrame(final_results)
res_df.to_csv(os.path.join(DOWNLOAD_PATH, "Power_Scorer_May_11_Audit.csv"), index=False)

print(f"Simulation Complete. Total Trades: {len(final_results)} | Net P/L: {round(res_df['pnl'].sum(), 2)}%")

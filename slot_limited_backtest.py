
import pandas as pd
import json, os, time
from datetime import datetime, timedelta
from dhanhq import dhanhq

# --- Config ---
TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzc4NTk3OTI2LCJpYXQiOjE3Nzg1MTE1MjYsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTA1MTIwODUzIn0.dFWhEopjMzjdrBOPbv1yXNzHJqn7AEDUy_Ett-e8TOKlWTgSaj0j_vaxsg9lFNnk0Veg2E_uYVvdpXRG1IuwYg"
CLIENT_ID = "1105120853"
dhan = dhanhq(CLIENT_ID, TOKEN)

# --- Params ---
SL_PCT = 0.013; TSL_TRIGGER = 0.02; TSL_TRAIL = 0.02; EMA_GUARD = 0.04; DIP_PCT = 0.008; MAX_SLOTS = 8

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

def get_data(sym):
    sid = _SECURITY_MAP.get(sym.upper())
    if not sid: return None
    today = "2026-05-11"
    resp = dhan.intraday_minute_data(sid, "NSE_EQ", "EQUITY", today, today)
    if resp.get("status") == "success" and resp.get("data") and 'close' in resp['data']:
        d = resp["data"]; l = len(d['close']); ts = d.get('timestamp', [0]*l)
        candles = []
        for i in range(l):
            candles.append({'time': ts[i], 'open': d['open'][i], 'high': d['high'][i], 'low': d['low'][i], 'close': d['close'][i]})
        return candles
    return None

def calc_ema(prices, n=9):
    if len(prices) < n: return None
    k = 2/(n+1); ema = prices[0]
    for p in prices[1:]: ema = p*k + ema*(1-k)
    return ema

# Load and sort CSVs
df1 = pd.read_csv(F_1M); df1['date'] = pd.to_datetime(df1['date'], dayfirst=True)
df3 = pd.read_csv(F_3M); df3['date'] = pd.to_datetime(df3['date'], dayfirst=True)
df5 = pd.read_csv(F_5M); df5['date'] = pd.to_datetime(df5['date'], dayfirst=True)
dfb = pd.read_csv(F_BUY); dfb['date'] = pd.to_datetime(dfb['date'], dayfirst=True)
dfb = dfb.sort_values('date')

active_trades = []
final_trades = []

for _, b_row in dfb.iterrows():
    sym = str(b_row['symbol']).strip().upper()
    sig_time = b_row['date']
    active_trades = [t for t in active_trades if t['exit_time'] > sig_time]
    if len(active_trades) >= MAX_SLOTS: continue
    
    # Funnel
    window_25 = sig_time - timedelta(minutes=25)
    in_5m = any(df5[(df5['symbol'] == sym) & (df5['date'] >= window_25) & (df5['date'] <= sig_time)])
    if not in_5m: continue
    window_flow = sig_time - timedelta(minutes=10)
    in_1m = any(df1[(df1['symbol'] == sym) & (df1['date'] >= window_flow) & (df1['date'] <= sig_time)])
    in_3m = any(df3[(df3['symbol'] == sym) & (df3['date'] >= window_flow) & (df3['date'] <= sig_time)])
    if not (in_1m and in_3m): continue

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
    if not ema9 or (data[sig_idx]['close'] - ema9)/ema9 > EMA_GUARD: continue
    entry_p = None; sig_high = data[sig_idx]['high']
    for j in range(sig_idx+1, min(len(data), sig_idx+4)):
        c = data[j]
        if c['close'] < c['open'] and (c['open']-c['close'])/c['open'] <= DIP_PCT: entry_p = c['close']; break
        if c['close'] > sig_high: entry_p = c['close']; break
    if not entry_p: continue
        
    # Simulate
    peak = entry_p; sl = entry_p * (1 - SL_PCT); tsl_on = False; reason = "TIME_EXIT"
    exit_time = datetime.fromtimestamp(data[min(len(data)-1, j+45)]['time'])
    final_exit_p = data[min(len(data)-1, j+45)]['close']
    for k in range(j+1, min(len(data), j+46)):
        c = data[k]
        if c['high'] > peak: peak = c['high']
        if not tsl_on and c['close'] >= entry_p * (1 + TSL_TRIGGER): tsl_on = True
        if tsl_on:
            floor = peak * (1 - TSL_TRAIL)
            if floor > sl: sl = floor
        if c['low'] <= sl:
            exit_time = datetime.fromtimestamp(c['time']); final_exit_p = sl; reason = "STOP_LOSS"; break
            
    pnl = (final_exit_p - entry_p)/entry_p
    active_trades.append({"sym": sym, "exit_time": exit_time})
    final_trades.append({
        "sym": sym, "entry": sig_time.strftime("%H:%M"), "exit": exit_time.strftime("%H:%M"), 
        "entry_price": entry_p, "exit_price": final_exit_p, "reason": reason, "pnl": round(pnl*100, 2)
    })

with open('slot_limited_results.json', 'w') as f:
    json.dump(final_trades, f)

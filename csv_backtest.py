
import pandas as pd
import json, os, time
from datetime import datetime, timedelta
from dhanhq import dhanhq

# --- Config ---
TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzc4NTk3OTI2LCJpYXQiOjE3Nzg1MTE1MjYsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTA1MTIwODUzIn0.dFWhEopjMzjdrBOPbv1yXNzHJqn7AEDUy_Ett-e8TOKlWTgSaj0j_vaxsg9lFNnk0Veg2E_uYVvdpXRG1IuwYg"
CLIENT_ID = "1105120853"
dhan = dhanhq(CLIENT_ID, TOKEN)

# --- Params ---
SL_PCT = 0.013
TSL_TRIGGER = 0.02
TSL_TRAIL = 0.02
EMA_GUARD = 0.04
DIP_PCT = 0.008

DOWNLOAD_PATH = r"C:\Users\cavis\Downloads"
F_MASTER = os.path.join(DOWNLOAD_PATH, "Backtest NEW 5 MIIN SCAN (19).csv")
F_SIGNALS = os.path.join(DOWNLOAD_PATH, "Backtest 5 MIn buy scanner (1).csv")

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
        d = resp["data"]
        l = len(d['close'])
        ts = d.get('timestamp', [0]*l)
        candles = []
        for i in range(l):
            candles.append({
                'time': ts[i], 'open': d['open'][i], 'high': d['high'][i],
                'low': d['low'][i], 'close': d['close'][i]
            })
        return candles
    return None

def calc_ema(prices, n=9):
    if len(prices) < n: return None
    k = 2/(n+1)
    ema = prices[0]
    for p in prices[1:]: ema = p*k + ema*(1-k)
    return ema

# 1. Load Master History
df_m = pd.read_csv(F_MASTER)
m_col_sym = 'symbol'
m_col_time = 'date'
df_m[m_col_time] = pd.to_datetime(df_m[m_col_time], dayfirst=True)

# 2. Load Signal History
df_s = pd.read_csv(F_SIGNALS)
s_col_sym = 'symbol'
s_col_time = 'date'
df_s[m_col_time] = pd.to_datetime(df_s[m_col_time], dayfirst=True)

final_results = []
processed_entries = set()

for _, s_row in df_s.iterrows():
    sym = str(s_row[s_col_sym]).strip().upper()
    sig_time = s_row[s_col_time]
    
    # Master List Check: Was it in the 5m scan in the last 25 mins?
    master_window_start = sig_time - timedelta(minutes=25)
    in_master = any(df_m[(df_m[m_col_sym] == sym) & 
                        (df_m[m_col_time] >= master_window_start) & 
                        (df_m[m_col_time] <= sig_time)])
    
    if not in_master:
        final_results.append({"sym": sym, "t": sig_time.strftime("%H:%M"), "res": "REJECTED_MASTER"})
        continue

    # Fetch Data
    data = get_data(sym)
    if not data: continue
    
    # Find signal candle
    sig_idx = -1
    for i, c in enumerate(data):
        c_time = datetime.fromtimestamp(c['time'])
        if c_time.hour == sig_time.hour and c_time.minute == sig_time.minute:
            sig_idx = i; break
    if sig_idx == -1: continue
    
    # EMA Guard
    hist_closes = [c['close'] for c in data[max(0, sig_idx-20):sig_idx+1]]
    ema9 = calc_ema(hist_closes)
    if not ema9 or (data[sig_idx]['close'] - ema9)/ema9 > EMA_GUARD:
        final_results.append({"sym": sym, "t": sig_time.strftime("%H:%M"), "res": "REJECTED_EMA"})
        continue
        
    # Smart Entry
    entry_p = None
    sig_high = data[sig_idx]['high']
    for j in range(sig_idx+1, min(len(data), sig_idx+4)):
        c = data[j]
        if c['close'] < c['open'] and (c['open']-c['close'])/c['open'] <= DIP_PCT:
            entry_p = c['close']; break
        if c['close'] > sig_high:
            entry_p = c['close']; break
    
    if not entry_p:
        final_results.append({"sym": sym, "t": sig_time.strftime("%H:%M"), "res": "NO_SMART_ENTRY"})
        continue
        
    # Execute Trade
    peak = entry_p
    sl = entry_p * (1 - SL_PCT)
    tsl_on = False
    outcome = "EXIT_45M"
    exit_p = data[min(len(data)-1, j+45)]['close']
    
    for k in range(j+1, min(len(data), j+46)):
        c = data[k]
        if c['high'] > peak: peak = c['high']
        if not tsl_on and c['close'] >= entry_p * (1 + TSL_TRIGGER): tsl_on = True
        if tsl_on:
            floor = peak * (1 - TSL_TRAIL)
            if floor > sl: sl = floor
        if c['low'] <= sl:
            outcome = "SL_HIT"; exit_p = sl; break
            
    pnl = (exit_p - entry_p)/entry_p
    final_results.append({"sym": sym, "t": sig_time.strftime("%H:%M"), "res": outcome, "pnl": round(pnl*100, 2)})

with open('csv_backtest_results.json', 'w') as f:
    json.dump(final_results, f)


import json, os, time
from datetime import datetime, timedelta
from dhanhq import dhanhq
import pandas as pd

TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzc4NTk3OTI2LCJpYXQiOjE3Nzg1MTE1MjYsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTA1MTIwODUzIn0.dFWhEopjMzjdrBOPbv1yXNzHJqn7AEDUy_Ett-e8TOKlWTgSaj0j_vaxsg9lFNnk0Veg2E_uYVvdpXRG1IuwYg"
CLIENT_ID = "1105120853"
dhan = dhanhq(CLIENT_ID, TOKEN)

# --- Params ---
SL_PCT = 0.013
TSL_TRIGGER = 0.02
TSL_TRAIL = 0.02
EMA_GUARD = 0.04
DIP_PCT = 0.008

# We pick top momentum stocks of the day for a global backtest
TARGET_STOCKS = [
    "RATNAMANI", "SHAILY", "TIIL", "OMAXE", "LLOYDSENGG", "BIOCON", 
    "HGINFRA", "MOREPENLAB", "CORONA", "BALRAMCHIN", "SUNDROP", 
    "EXPLEOSOL", "NGLFINE", "WELCORP", "SWIGGY", "JINDWORLD", 
    "INDOTECH", "SMSPHARMA", "JBMA", "MEDIASSIS"
]

_SECURITY_MAP = {}
if os.path.exists('security_map.csv'):
    df = pd.read_csv('security_map.csv', low_memory=False)
    df = df[df["SEM_INSTRUMENT_NAME"] == "EQUITY"]
    for _, row in df.iterrows():
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

full_results = []

for sym in TARGET_STOCKS:
    data = get_data(sym)
    if not data: continue
    
    # Simulate a signal scanner looking every minute
    for i in range(20, len(data) - 45): # Skip early morning
        # EMA Guard check
        hist_closes = [c['close'] for c in data[i-20:i+1]]
        ema9 = calc_ema(hist_closes)
        if not ema9: continue
        
        # Signal: Simple momentum spike (simulating Chartink)
        # Price > 1% up in 5 mins
        if (data[i]['close'] - data[i-5]['close'])/data[i-5]['close'] > 0.01:
            # EMA Guard
            if (data[i]['close'] - ema9)/ema9 > EMA_GUARD: continue
            
            # Smart Entry Check (next 3 mins)
            entry_p = None
            sig_high = data[i]['high']
            for j in range(i+1, i+4):
                c = data[j]
                if c['close'] < c['open'] and (c['open']-c['close'])/c['open'] <= DIP_PCT:
                    entry_p = c['close']; break
                if c['close'] > sig_high:
                    entry_p = c['close']; break
            
            if not entry_p: continue
            
            # Execute Trade
            peak = entry_p
            sl = entry_p * (1 - SL_PCT)
            tsl_on = False
            outcome = "EXIT_45M"
            exit_p = data[j+45]['close']
            
            for k in range(j+1, j+46):
                c = data[k]
                if c['high'] > peak: peak = c['high']
                if not tsl_on and c['close'] >= entry_p * (1 + TSL_TRIGGER): tsl_on = True
                if tsl_on:
                    floor = peak * (1 - TSL_TRAIL)
                    if floor > sl: sl = floor
                if c['low'] <= sl:
                    outcome = "SL_HIT"; exit_p = sl; break
            
            pnl = (exit_p - entry_p)/entry_p
            ts = datetime.fromtimestamp(data[i]['time']).strftime("%H:%M")
            full_results.append({"sym": sym, "t": ts, "res": outcome, "pnl": round(pnl*100, 2)})
            break # One trade per stock for simplicity

with open('full_day_backtest.json', 'w') as f:
    json.dump(full_results, f)

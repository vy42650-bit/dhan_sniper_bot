
import json, os, time
from datetime import datetime, timedelta
from dhanhq import dhanhq
import pandas as pd

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

with open('audit_data.json', 'r') as f:
    audit = json.load(f)

# Load Security Map
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
        candles = []
        l = len(d.get('close', []))
        times = d.get('timestamp', [0]*l)
        for i in range(l):
            candles.append({
                'time': times[i],
                'open': d['open'][i], 'high': d['high'][i],
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

results = []
processed_signals = set()

for sig in audit['signals']:
    sig_time_utc = datetime.strptime(sig['t'], "%Y-%m-%d %H:%M:%S")
    # Convert UTC logs to IST (+5.5 hours)
    sig_time = sig_time_utc + timedelta(hours=5, minutes=30)
    for sym in sig['s']:
        if (sym, sig['t']) in processed_signals: continue
        processed_signals.add((sym, sig['t']))
        
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
        if not ema9 or (data[sig_idx]['close'] - ema9)/ema9 > EMA_GUARD:
            results.append({"sym": sym, "t": sig['t'], "res": "REJECTED_EMA"})
            continue
            
        entry_p = None
        sig_high = data[sig_idx]['high']
        for j in range(sig_idx+1, min(len(data), sig_idx+4)):
            c = data[j]
            if c['close'] < c['open'] and (c['open']-c['close'])/c['open'] <= DIP_PCT:
                entry_p = c['close']; break
            if c['close'] > sig_high:
                entry_p = c['close']; break
        
        if not entry_p:
            results.append({"sym": sym, "t": sig['t'], "res": "NO_SMART_ENTRY"})
            continue
            
        res = "EXIT_EOD"
        pnl = 0
        peak = entry_p
        sl = entry_p * (1 - SL_PCT)
        tsl_on = False
        for k in range(j+1, min(len(data), j+46)):
            c = data[k]
            if c['high'] > peak: peak = c['high']
            if not tsl_on and c['close'] >= entry_p * (1 + TSL_TRIGGER): tsl_on = True
            if tsl_on:
                floor = peak * (1 - TSL_TRAIL)
                if floor > sl: sl = floor
            if c['low'] <= sl:
                res = "SL_HIT"; pnl = (sl - entry_p)/entry_p; break
        
        if res == "EXIT_EOD":
            pnl = (data[min(len(data)-1, j+45)]['close'] - entry_p)/entry_p
        results.append({"sym": sym, "t": sig['t'], "res": res, "pnl": round(pnl*100, 2)})

with open('backtest_results.json', 'w') as f:
    json.dump(results, f)

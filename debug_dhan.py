
import json, os
from datetime import datetime, timedelta
from dhanhq import dhanhq
import pandas as pd

TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzc4NTk3OTI2LCJpYXQiOjE3Nzg1MTE1MjYsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTA1MTIwODUzIn0.dFWhEopjMzjdrBOPbv1yXNzHJqn7AEDUy_Ett-e8TOKlWTgSaj0j_vaxsg9lFNnk0Veg2E_uYVvdpXRG1IuwYg"
CLIENT_ID = "1105120853"
dhan = dhanhq(CLIENT_ID, TOKEN)

_SECURITY_MAP = {}
if os.path.exists('security_map.csv'):
    df = pd.read_csv('security_map.csv', low_memory=False)
    df = df[df["SEM_INSTRUMENT_NAME"] == "EQUITY"]
    for _, row in df.iterrows():
        _SECURITY_MAP[str(row["SEM_TRADING_SYMBOL"]).upper()] = str(row["SEM_SMST_SECURITY_ID"])

sym = "DLINKINDIA"
sid = _SECURITY_MAP.get(sym)
print(f"Checking {sym} (ID: {sid})")

today = "2026-05-11"
resp = dhan.intraday_minute_data(sid, "NSE_EQ", "EQUITY", today, today)
if resp.get("status") == "success":
    d = resp["data"]
    l = len(d['close'])
    print(f"Fetched {l} candles.")
    times = d.get('start_Time', d.get('start_time'))
    for i in range(min(10, l)):
        dt = datetime.fromtimestamp(times[i])
        print(f"Candle {i}: {dt} | Close: {d['close'][i]}")
else:
    print(f"API Failed: {resp}")

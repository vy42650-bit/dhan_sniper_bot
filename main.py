import os
import pandas as pd
from datetime import datetime, timedelta
from fastapi import FastAPI, Request
import uvicorn
from dhanhq import dhanhq
import threading
import time

# --- SECURE CONFIGURATION ---
CLIENT_ID = os.getenv("DHAN_CLIENT_ID", "1105120853")
ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")
MODE = os.getenv("TRADING_MODE", "SANDBOX") 
BLACKLIST = ["MEESHO", "MEESHO-BE"]

app = FastAPI()
dhan = dhanhq(CLIENT_ID, ACCESS_TOKEN)

# --- GLOBAL STATE ---
candidate_pool_1m = {} 
candidate_pool_3m = {} 
master_top_15 = []     
active_positions = {}  
block_start_time = datetime.now()

def risk_manager_loop():
    while True:
        try:
            now = datetime.now()
            global block_start_time, candidate_pool_1m, candidate_pool_3m
            if (now - block_start_time).total_seconds() >= 1500: # 25 mins
                candidate_pool_1m = {}
                candidate_pool_3m = {}
                block_start_time = now

            for symbol in list(active_positions.keys()):
                pos = active_positions[symbol]
                
                # Fetch LTP (Real-time monitoring)
                # Note: In production, switch to Dhan WebSockets for <50ms latency
                try:
                    quote = dhan.get_quote_data(symbol, "NSE", "EQUITY")
                    ltp = quote['data']['last_price']
                except: continue
                
                if ltp > pos['max_price']:
                    pos['max_price'] = ltp
                
                if not pos['tsl_active'] and (ltp >= pos['entry_price'] * 1.03):
                    pos['tsl_active'] = True
                
                time_held = (now - pos['entry_time']).total_seconds() / 60
                exit_reason = None
                
                if ltp <= pos['sl_price']: exit_reason = "STOP_LOSS"
                elif pos['tsl_active'] and ltp <= pos['max_price'] * 0.98: exit_reason = "TSL"
                elif time_held >= 45: exit_reason = "TIME_EXIT"
                
                if exit_reason:
                    print(f"EXIT {symbol} | {exit_reason} | LTP: {ltp}")
                    # dhan.place_order(...) logic for exit
                    del active_positions[symbol]
                    
        except Exception as e:
            print(f"Risk Loop Err: {e}")
        time.sleep(1)

@app.get("/")
def home():
    return {"status": "Sniper Bot Online", "positions": len(active_positions), "candidates": len(master_top_15)}

@app.post("/webhook/1min")
async def handle_1min(request: Request):
    data = await request.json()
    stocks = data.get("stocks", "").split(",")
    for s in stocks:
        if s and s not in BLACKLIST:
            candidate_pool_1m[s] = candidate_pool_1m.get(s, 0) + 1
    return {"status": "received"}

@app.post("/webhook/3min")
async def handle_3min(request: Request):
    data = await request.json()
    stocks = data.get("stocks", "").split(",")
    for s in stocks:
        if s and s not in BLACKLIST:
            candidate_pool_3m[s] = candidate_pool_3m.get(s, 0) + 1
    return {"status": "received"}

@app.post("/webhook/5min")
async def handle_5min(request: Request):
    data = await request.json()
    potential_5m = data.get("stocks", "").split(",")
    
    top_15_1m = sorted(candidate_pool_1m.items(), key=lambda x: x[1], reverse=True)[:15]
    top_25_3m = sorted(candidate_pool_3m.items(), key=lambda x: x[1], reverse=True)[:25]
    
    combined = set([x[0] for x in top_15_1m] + [x[0] for x in top_25_3m])
    
    global master_top_15
    master_top_15 = [s for s in potential_5m if s in combined]
    print(f"Master List Updated: {len(master_top_15)} stocks.")
    return {"status": "validated"}

@app.post("/webhook/final_buy")
async def handle_buy(request: Request):
    data = await request.json()
    symbol = data.get("stock")
    
    if symbol in master_top_15 and len(active_positions) < 8:
        if symbol not in active_positions and symbol not in BLACKLIST:
            try:
                quote = dhan.get_quote_data(symbol, "NSE", "EQUITY")
                entry_p = quote['data']['last_price']
                
                # dhan.place_order(...)
                
                active_positions[symbol] = {
                    "entry_price": entry_p,
                    "entry_time": datetime.now(),
                    "max_price": entry_p,
                    "sl_price": entry_p * 0.987,
                    "tsl_active": False
                }
                print(f"BOUGHT {symbol} @ {entry_p}")
            except: pass
            
    return {"status": "processed"}

if __name__ == "__main__":
    threading.Thread(target=risk_manager_loop, daemon=True).start()
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))

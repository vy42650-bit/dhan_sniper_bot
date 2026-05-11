
import json, os

def brute_parse():
    signals = []
    masters = []
    # Using utf-16 to handle the BOM \xff\xfe
    with open('full_day_logs.txt', 'r', encoding='utf-16', errors='ignore') as f:
        for line in f:
            if '[WH-BUY]' in line:
                try:
                    ts = line.split(',')[0].strip()
                    stocks_part = line.split('signal for:')[1].strip()
                    stocks = stocks_part.replace('[','').replace(']','').replace("'","").split(',')
                    stocks = [s.strip() for s in stocks if s]
                    signals.append({"t": ts, "s": stocks})
                except: pass
            if 'MASTER UPDATED' in line or 'ROLLING MASTER' in line:
                try:
                    ts = line.split(',')[0].strip()
                    parts = line.split('):')[1].strip() if '):' in line else line.split('UPDATED:')[1].strip()
                    stocks = parts.replace('[','').replace(']','').replace("'","").replace('...','').split(',')
                    stocks = [s.strip() for s in stocks if s]
                    masters.append({"t": ts, "s": stocks})
                except: pass

    print(f"Captured {len(signals)} signals and {len(masters)} master updates.")
    with open('audit_data.json', 'w') as f:
        json.dump({"masters": masters, "signals": signals}, f)

if __name__ == "__main__":
    brute_parse()

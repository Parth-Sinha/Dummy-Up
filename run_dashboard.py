import json
import time
import os
from datetime import datetime

DASHBOARD_FILE = "dashboard_state.json"

def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')

def main():
    print("Waiting for Data Feed...")
    while True:
        try:
            if not os.path.exists(DASHBOARD_FILE):
                time.sleep(1)
                continue

            with open(DASHBOARD_FILE, 'r') as f:
                data = json.load(f)

            clear_screen()
            last_updated = data.get("last_updated", "N/A")
            
            # Account Info
            account = data.get("account", {})
            avail_cash = account.get("available_cash", 0.0)
            realized_pnl = account.get("realized_pnl", 0.0)
            used_margin = account.get("used_margin", 0.0)
            
            # Header
            print(f"=====================================================================================================")
            print(f" 📊 OEMS LIVE DASHBOARD | Last Update: {last_updated}")
            print(f" 💵 CASH: {avail_cash:,.2f} | 📉 USED MARGIN: {used_margin:,.2f} | 💰 REALIZED P&L: {realized_pnl:,.2f}")
            print(f"=====================================================================================================")
            print(f"{'SYMBOL':<15} | {'LTP':<10} | {'RSI':<8} | {'SL':<10} | {'POS':<6} | {'ENTRY':<10} | {'P&L':<10} | {'TIME':<10}")
            print(f"-----------------------------------------------------------------------------------------------------")

            # Rows
            symbols = data.get("symbols", {})
            sorted_syms = sorted(symbols.keys())
            
            total_pnl = 0.0
            
            for sym in sorted_syms:
                s_data = symbols[sym]
                name = sym.split(":")[-1]
                ltp = s_data.get("ltp", 0)
                rsi = s_data.get("rsi", 0)
                sl = s_data.get("sl", 0.0)
                pos = s_data.get("pos", 0)
                entry = s_data.get("entry", 0)
                tick_time = s_data.get("last_tick", "--:--")
                
                # Calculate PnL
                pnl = 0.0
                pnl_str = "-"
                if pos > 0:
                    pnl = (ltp - entry) * pos
                    total_pnl += pnl
                    pnl_str = f"{pnl:.2f}"
                
                sl_str = f"{sl:.2f}" if sl > 0 else "-"
                    
                print(f"{name:<15} | {ltp:<10} | {rsi:<8.2f} | {sl_str:<10} | {pos:<6} | {entry:<10.2f} | {pnl_str:<10} | {tick_time}")

            print(f"-----------------------------------------------------------------------------------------------------")
            print(f" 💰 TOTAL UNREALIZED P&L: {total_pnl:.2f}")
            print(f"=====================================================================================================")
            print(f"\n(Press Ctrl+C to exit dashboard)")
            
            time.sleep(0.5)

        except Exception as e:
            # print(f"Error: {e}")
            time.sleep(1)

if __name__ == "__main__":
    main()
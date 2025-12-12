import os
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
import time

class TradeRecorder:
    def __init__(self, filename="trade_log.csv", initial_capital=100000, view_mode=False):
        self.filename = filename
        self.initial_capital = initial_capital
        self.view_mode = view_mode
        self.columns = ["Timestamp", "Action", "Symbol", "Price", "Quantity", "PnL", "Balance", "Reason"]
        
        if not os.path.exists(self.filename):
            df = pd.DataFrame(columns=self.columns)
            df.to_csv(self.filename, index=False)

        if self.view_mode:
            plt.ion()
            plt.style.use('ggplot')
            self.fig, self.ax = plt.subplots(figsize=(10, 6))
            self.line, = self.ax.plot([], [], color='green', linewidth=2)
            self.buy_scat = self.ax.scatter([], [], marker='^', color='blue', s=100, zorder=5, label='Buy')
            self.sell_scat = self.ax.scatter([], [], marker='v', color='red', s=100, zorder=5, label='Sell')
            self.ax.set_title("Real-Time Equity Curve")
            self.ax.legend()

    def log_trade(self, action, symbol, price, qty, pnl=0.0, reason=""):
        # Fast Append for Strategy
        last_balance = self.initial_capital
        try:
            if os.path.exists(self.filename) and os.path.getsize(self.filename) > 50:
                history = pd.read_csv(self.filename)
                if not history.empty: last_balance = history.iloc[-1]["Balance"]
        except: pass

        new_balance = last_balance + pnl 
        record = {
            "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "Action": action, "Symbol": symbol, "Price": round(price, 2),
            "Quantity": qty, "PnL": round(pnl, 2), "Balance": round(new_balance, 2), "Reason": reason
        }
        pd.DataFrame([record]).to_csv(self.filename, mode='a', header=False, index=False)
        print(f"📝 Logged: {action} {symbol} (Bal: {new_balance})")

    def get_total_pnl(self):
        try:
            if os.path.exists(self.filename):
                df = pd.read_csv(self.filename)
                if "PnL" in df.columns:
                    return df["PnL"].sum()
        except:
            pass
        return 0.0

    def start_dashboard_loop(self, refresh_rate=1):
        if not self.view_mode: return
        print(f"📊 Dashboard Watching {self.filename}...")
        while True:
            try:
                self._update_plot_data()
                plt.pause(refresh_rate)
            except KeyboardInterrupt: break
            except Exception as e: print(f"Plot Error: {e}"); time.sleep(1)

    def _update_plot_data(self):
        if not os.path.exists(self.filename): return
        try:
            df = pd.read_csv(self.filename)
            if df.empty: return
            df['Timestamp'] = pd.to_datetime(df['Timestamp'])
            
            self.line.set_data(df['Timestamp'], df['Balance'])
            
            buys = df[df['Action'] == 'BUY']
            sells = df[df['Action'] == 'SELL']
            
            # Safe scatter update
            if not buys.empty:
                self.buy_scat.set_offsets(list(zip(buys['Timestamp'].apply(lambda x: x.toordinal()), buys['Balance'])))
            if not sells.empty:
                self.sell_scat.set_offsets(list(zip(sells['Timestamp'].apply(lambda x: x.toordinal()), sells['Balance'])))

            self.ax.relim()
            self.ax.autoscale_view()
            self.ax.set_title(f"Current Balance: {df.iloc[-1]['Balance']}")
        except: pass
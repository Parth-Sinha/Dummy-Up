import os
import time
import requests
import math
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dotenv import load_dotenv
import urllib.parse


# Local Imports
from Upstox.upstox import upstox
from Upstox.base.constants import ExchangeCode, Side
from trade_logger import TradeRecorder

# --- CONFIGURATION ---
load_dotenv()
SANDBOX_TOKEN = os.getenv("UPSTOX_SANDBOX_TOKEN")
PROD_TOKEN = os.getenv("UPSTOX_PROD_TOKEN") 
ALLOCATED_CAPITAL = float(os.getenv("ALLOCATED_CAPITAL", 100000)) 

# SYMBOL = "NSE_EQ:VBL"
# INSTRUMENT_KEY = "NSE_EQ%7CINE200M01039" 
# INSTRUMENT_TOKEN = "NSE_EQ|INE200M01039"

# SYMBOL = "NSE_EQ:RELIANCE"
# INSTRUMENT_KEY = "NSE_EQ%7CINE002A01018" 
# INSTRUMENT_TOKEN = "NSE_EQ|INE002A01018"

SYMBOL = "NSE_EQ:MARUTI"
INSTRUMENT_KEY = "NSE_EQ%7CINE585B01010" 
INSTRUMENT_TOKEN = "NSE_EQ|INE585B01010"


# SYMBOL = "NSE_EQ:63MOONS"
# INSTRUMENT_TOKEN = "NSE_EQ|INE111B01023"
# INSTRUMENT_KEY = "NSE_EQ%7CINE111B01023"

# SYMBOL = "NSE_EQ:HCC"
# INSTRUMENT_TOKEN ="NSE_EQ|INE549A01026"
# INSTRUMENT_KEY = "NSE_EQ%7CINE549A01026"

# SYMBOL = "NSE_EQ:IIFLCAPS"
# INSTRUMENT_TOKEN ="NSE_EQ|INE489L01022"
# INSTRUMENT_KEY = "NSE_EQ%7CINE489L01022"
TIMEFRAME_RESAMPLE = '3min' 


# API SESSIONS
sandbox_session = requests.Session()
prod_session = requests.Session()

prod_session.headers.update({"Authorization": f"Bearer {PROD_TOKEN}", "Accept": "application/json"})
sandbox_session.headers.update({"Authorization": f"Bearer {SANDBOX_TOKEN}", "Accept": "application/json"})

# --- ROBUST DATA MANAGER (UNCHANGED) ---
class MarketDataManager:
    def __init__(self, instrument_key, max_len=5000):
        self.instrument_key = instrument_key
        self.max_len = max_len
        self.df_1m = pd.DataFrame() 
        self.last_timestamp = None
        self.current_ltp = 0.0

    def fetch_ltp(self):
        # print("⏱️ Fetching LTP...", end='\r')
        url = f"https://api.upstox.com/v2/market-quote/ltp?instrument_key={self.instrument_key}"
        # params = {"instrument_key": self.instrument_key}
        try:
            resp = prod_session.get(url, timeout=2)
            # print(f"\n⏱️ Fetched LTP Response: {resp.json()}", end='\r')
            if resp.status_code == 200:
                # print(f"✅ Fetched LTP Response: {resp.json()}", end='\r')
                data = resp.json().get('data', {})
                instrument_data = data.get(SYMBOL, {})
                self.current_ltp = instrument_data.get('last_price', 0.0)
                # print(f"✅ LTP: {self.current_ltp:.2f}", end='\r')
                return self.current_ltp
            else:
                print(f"❌ LTP Fetch Failed: {resp.text}", end='\r')
                #we will wait and try again
                return 0.0
        except Exception as e:
            print(f"❌ LTP Fetch Exception: {e}", end='\r')
            pass

    def fetch_historical_warmup(self):
        print(f"🔥 Warming up data cache for {self.instrument_key}...")
        # today = datetime.now().date()
        # from_date = (today - timedelta(days=5)).strftime('%Y-%m-%d')
        # to_date = today.strftime('%Y-%m-%d')
        
        # url = f"https://api.upstox.com/v2/historical-candle/intraday/{self.instrument_key}/1minute"
        encoded_key = urllib.parse.quote(self.instrument_key)
        
        # Calculate dates for last 5 days
        now = datetime.now()
        to_date = now.strftime("%Y-%m-%d")
        from_date = (now - timedelta(days=5)).strftime("%Y-%m-%d")
        
        # Use historical endpoint to get last 5 days of data
        url = f"https://api.upstox.com/v3/historical-candle/{encoded_key}/minutes/1/{to_date}/{from_date}"
        
        try:
            resp = prod_session.get(url)
            if resp.status_code == 200:
                data = resp.json().get('data', {}).get('candles', [])
                if data:
                    df = pd.DataFrame(data, columns=['timestamp', 'Open', 'High', 'Low', 'Close', 'Volume', 'OI'])
                    df['timestamp'] = pd.to_datetime(df['timestamp'])
                    df.set_index('timestamp', inplace=True)
                    df.sort_index(inplace=True)
                    
                    self.df_1m = df
                    self.last_timestamp = df.index[-1]
                    self.current_ltp = df['Close'].iloc[-1] 
                    print(f"✅ Warmup Complete. Last Candle: {self.last_timestamp}")
                    return True
            print(f"❌ Warmup Failed: {resp.text}")
            return False
        except Exception as e:
            print(f"❌ Warmup Exception: {e}")
            return False

    def update_latest_candle(self):
        url = f"https://api.upstox.com/v2/historical-candle/intraday/{self.instrument_key}/1minute"
        try:
            resp = prod_session.get(url, timeout=5)
            if resp.status_code == 200:
                data = resp.json().get('data', {}).get('candles', [])
                if not data: return False
                
                incoming_df = pd.DataFrame(data, columns=['timestamp', 'Open', 'High', 'Low', 'Close', 'Volume', 'OI'])
                incoming_df['timestamp'] = pd.to_datetime(incoming_df['timestamp'])
                incoming_df.set_index('timestamp', inplace=True)
                incoming_df.sort_index(inplace=True)
                
                new_data = incoming_df[incoming_df.index > self.last_timestamp]
                
                if not new_data.empty:
                    self.df_1m = pd.concat([self.df_1m, new_data])
                    if len(self.df_1m) > self.max_len:
                        self.df_1m = self.df_1m.iloc[-self.max_len:]
                    self.last_timestamp = self.df_1m.index[-1]
                    return True 
            return False 
        except Exception:
            return False

    def get_resampled_data(self, rule='3min'):
        if self.df_1m.empty: return None
        ohlc_dict = {'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last'}
        return self.df_1m.resample(rule).agg(ohlc_dict).dropna()

# --- INDICATORS (UNCHANGED) ---
def calc_rsi(series, period=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).ewm(com=period-1, min_periods=period).mean()
    loss = (-delta.where(delta < 0, 0)).ewm(com=period-1, min_periods=period).mean()
    rs = gain / (loss + 1e-10)
    return 100 - (100 / (1 + rs))

def calc_atr(df, period=14):
    high_low = df['High'] - df['Low']
    high_close = np.abs(df['High'] - df['Close'].shift())
    low_close = np.abs(df['Low'] - df['Close'].shift())
    ranges = pd.concat([high_low, high_close, low_close], axis=1)
    true_range = np.max(ranges, axis=1)
    return pd.Series(true_range).ewm(com=period-1, min_periods=period).mean()

# --- UPDATED STRATEGY ENGINE ---
class StrategyEngine:
    def __init__(self):
        self.in_position = False
        self.trailing_stop = 0.0
        self.qty = 0 
        self.symbol = SYMBOL
        self.entry_price = 0.0 
        self.logger = TradeRecorder("live_trades.csv", view_mode=False)
        self.sync_broker_state()

    def sync_broker_state(self):
        """
        FIX: Checks BOTH Positions (Intraday/Today) AND Holdings (Delivery/Yesterday).
        """
        print("🔄 Syncing with Broker (Positions + Holdings)...")
        total_qty = 0
        weighted_price_sum = 0.0
        
        try:
            # 1. Check POSITIONS (Intraday / Today's Delivery)
            url_pos = "https://api-sandbox.upstox.com/v2/portfolio/short-term-positions"
            resp_pos = sandbox_session.get(url_pos)
            if resp_pos.status_code == 200:
                data = resp_pos.json().get('data', [])
                for pos in data:
                    if pos.get('trading_symbol') == self.symbol:
                        qty = pos.get('quantity', 0)
                        price = pos.get('average_price', 0.0)
                        # Note: In positions, sell qty is negative. We care about net.
                        if qty != 0:
                            total_qty += qty
                            weighted_price_sum += (qty * price)
            
            # 2. Check HOLDINGS (Delivery T+1)
            url_hold = "https://api-sandbox.upstox.com/v2/portfolio/long-term-holdings"
            resp_hold = sandbox_session.get(url_hold)
            if resp_hold.status_code == 200:
                data = resp_hold.json().get('data', [])
                for hold in data:
                    if hold.get('instrument_token') == INSTRUMENT_TOKEN:
                        qty = hold.get('quantity', 0)
                        price = hold.get('average_price', 0.0)
                        # Holdings are always long (positive)
                        if qty > 0:
                            total_qty += qty
                            weighted_price_sum += (qty * price)

            # 3. Update Internal State
            if total_qty > 0:
                self.in_position = True
                self.qty = int(total_qty)
                self.entry_price = weighted_price_sum / total_qty
                print(f"⚠️ FOUND EXISTING LONG: {self.qty} Qty @ {self.entry_price:.2f}")
            else:
                print("✅ No Net Position Found. System Flat.")
                self.in_position = False
                self.qty = 0
                self.entry_price = 0.0
                
        except Exception as e:
            print(f"⚠️ Sync Warning: {e}")

    def calculate_position_size(self, current_price):
        if current_price <= 0: return 0
        investable = ALLOCATED_CAPITAL - 2000
        qty = math.floor(investable / current_price)
        return max(1, qty)

    def cancel_all_open_orders(self):
        try:
            url = "https://api-sandbox.upstox.com/v2/order/retrieve-all"
            resp = sandbox_session.get(url)
            if resp.status_code == 200:
                orders = resp.json().get('data', [])
                for order in orders:
                    if order.get('status') == 'open':
                        oid = order.get('order_id')
                        sandbox_session.delete("https://api-sandbox.upstox.com/v2/order/cancel", params={'order_id': oid})
        except Exception:
            pass

    def check_signals(self, df_3m, current_price):
        if len(df_3m) < 30: 
            print("⚠️ Not enough data to analyze signals.")
            return 

        rsi_short = calc_rsi(df_3m['Close'], 9)
        rsi_long = calc_rsi(df_3m['Close'], 15)
        rsi = (rsi_short + rsi_long) / 2
        atr = calc_atr(df_3m, 22)
        highest_high = df_3m['High'].rolling(22).max()
        chandelier = highest_high - (atr * 3)

        if np.isnan(chandelier.iloc[-1]) or np.isnan(rsi.iloc[-1]): return

        curr_rsi = rsi.iloc[-1]
        prev_rsi = rsi.iloc[-2]
        curr_chandelier = chandelier.iloc[-1]

        # Re-Calculate Trailing Stop if we just synced a position
        if self.in_position and self.trailing_stop == 0.0:
             # Be conservative: Use Chandelier or Entry Price, whichever is closer to LTP
             self.trailing_stop = curr_chandelier
             print(f"🛡️ Initialized Trailing Stop to {self.trailing_stop:.2f}")

        if self.in_position:
            self.trailing_stop = max(self.trailing_stop, curr_chandelier)
            print(f"🛡️ Trailing Stop Updated: {self.trailing_stop:.2f}")

        # --- ENTRY LOGIC ---
        if not self.in_position:
            # self.trailing_stop = max(self.trailing_stop, curr_chandelier)  # Reset trailing stop
            print(f"\n🔍 Checking Entry Signal | RSI: {curr_rsi:.2f} | Chandelier: {curr_chandelier:.2f} | TRAILING_SL: {self.trailing_stop}")
            if curr_rsi < 50.0 and curr_rsi > prev_rsi and current_price > curr_chandelier:
                buy_qty = self.calculate_position_size(current_price)
                print(f"🚀 BUY SIGNAL @ {current_price} | Qty: {buy_qty}")
                
                self.cancel_all_open_orders()
                if self.execute_order_with_retry(Side.BUY, buy_qty):
                    self.logger.log_trade("BUY", self.symbol, current_price, buy_qty, 0.0, "Signal")
                    self.in_position = True
                    self.qty = buy_qty
                    self.entry_price = current_price
                    self.trailing_stop = curr_chandelier if not np.isnan(curr_chandelier) else (current_price - 5)


    def check_fast_exit(self, current_price):
        if self.in_position and self.trailing_stop > 0:
            unrealized_pnl = (current_price - self.entry_price) * self.qty
            print(f"   >> Live PnL: {unrealized_pnl:.2f} | LTP: {current_price} | Stop: {self.trailing_stop:.2f}", end='\r')

            if current_price <= self.trailing_stop:
                self.logger.log_trade("SELL", self.symbol, current_price, self.qty, unrealized_pnl, "Trailing Stop Triggered")
                print(f"\n🛑 STOP HIT: {self.trailing_stop:.2f} (LTP: {current_price})")
                self.cancel_all_open_orders()
                self.sync_broker_state() # RE-SYNC TO SELL EXACT HOLDING QTY
                
                if self.qty > 0:
                    if self.execute_order_with_retry(Side.SELL, self.qty):
                        pnl = (current_price - self.entry_price) * self.qty
                        self.logger.log_trade("SELL", self.symbol, current_price, self.qty, pnl, "Stop Hit")
                        self.in_position = False
                        self.trailing_stop = 0.0
                        self.entry_price = 0.0

    def execute_order_with_retry(self, side, qty, retries=3):
        url = "https://api-sandbox.upstox.com/v2/order/place"
        payload = {
            "quantity": qty, "product": "D", "validity": "DAY", "price": 0,
            "tag": "algo_v3", "instrument_token": INSTRUMENT_TOKEN, 
            "order_type": "MARKET", "transaction_type": side, "disclosed_quantity": 0,
            "trigger_price": 0, "is_amo": False
        }
        for _ in range(retries):
            try:
                resp = sandbox_session.post(url, json=payload)
                print(f"⏱️ Order Response: {resp.json()}")
                data = resp.json()
                if resp.status_code == 200 and data.get('status') == 'success':
                    print(f"⚡ Order Filled. ID: {data.get('data', {}).get('order_id')}")
                    return True
                else:
                    print(f"⚠️ Rejected: {data}")
            except Exception:
                time.sleep(0.5)
        return False

# --- MAIN EXECUTION ---
def run_optimized_bot():
    print(f"--- 🚀 INITIALIZING BOT (Hybrid: Live Data + Sandbox Exec) ---")
    data_mgr = MarketDataManager(INSTRUMENT_TOKEN)
    strategy = StrategyEngine()
    
    if not data_mgr.fetch_historical_warmup(): return

    print("--- 🎧 MONITORING (1s Tick / 60s Logic) ---")
    
    while True:
        try:
            ltp = data_mgr.fetch_ltp()
            print(f"⏱️ LTP: {ltp:.2f} | Time: {datetime.now().strftime('%H:%M:%S')}", end='\r')
            
            if ltp > 0:
                strategy.check_fast_exit(ltp)

            now = datetime.now()
            if now.second <= 2: 
                data_updated = data_mgr.update_latest_candle()
                if data_updated:
                    print(f"\n⏱️ New Candle: {data_mgr.last_timestamp} | Close: {data_mgr.df_1m['Close'].iloc[-1]}")
                    df_3m = data_mgr.get_resampled_data(TIMEFRAME_RESAMPLE)
                    strategy.check_signals(df_3m, ltp)
                    time.sleep(2) 

            time.sleep(1) 
            
        except KeyboardInterrupt:
            print("\nStopping Bot...")
            break
        except Exception as e:
            print(f"\nError: {e}")
            time.sleep(5)

if __name__ == "__main__":
    run_optimized_bot()
    # url_hold = "https://api.upstox.com/v2/portfolio/short-term-positions"
    # resp_hold = prod_session.get(url_hold)
    # print(resp_hold.json())
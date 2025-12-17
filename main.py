
import os
import time
import math
import random
import json
import queue
import threading
import shutil
import traceback
from datetime import datetime
from dotenv import load_dotenv
import ssl
# Bypass SSL Verification globally
try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    pass
else:
    ssl._create_default_https_context = _create_unverified_https_context

# Local Imports
from Upstox.upstox import upstox
from core.data_feed import RobustDataFeed
from Strategies.rsi_chandelier_tight import RSIChandelierStrategy
from trade_logger import TradeRecorder
from Upstox.base.constants import ExchangeCode
from Upstox.base.constants import Product
from Upstox.base.constants import Validity
from Upstox.base.constants import Side

# --- CONFIGURATION ---
load_dotenv()
API_TOKEN = os.getenv("UPSTOX_PROD_TOKEN") 
SANDBOX_TOKEN = os.getenv("UPSTOX_SANDBOX_TOKEN")
ALLOCATED_CAPITAL = float(os.getenv("ALLOCATED_CAPITAL", 100000))

# Define your Universe here
SYMBOLS_MAP = {
    "NSE_EQ:MARUTI": "NSE_EQ|INE585B01010",
    "NSE_EQ:RELIANCE": "NSE_EQ|INE002A01018", 
    # "NSE_EQ:TCS": "NSE_EQ|INE467B01029",
    # "NSE_EQ:HDFCBANK": "NSE_EQ|INE040A01034",
    # "NSE_EQ:INFY": "NSE_EQ|INE009A01021",
    # "NSE_EQ:ICICIBANK": "NSE_EQ|INE090A01021",
    # "NSE_EQ:KOTAKBANK": "NSE_EQ|INE237A01034",
    # "NSE_EQ:SBIN": "NSE_EQ|INE062A01020",
    # "NSE_EQ:AXISBANK": "NSE_EQ|INE238A01034",
    # "NSE_EQ:HINDUNILVR": "NSE_EQ|INE030A01027",
    # "NSE_EQ:LT": "NSE_EQ|INE018A01030",
    "NSE_EQ:BHARTIARTL": "NSE_EQ|INE397D01024",
    # "NSE_EQ:ITC": "NSE_EQ|INE154A01025",
    # "NSE_EQ:ASIANPAINT": "NSE_EQ|INE021A01026",
    # "NSE_EQ:MARICO": "NSE_EQ|INE196A01026",
    # "NSE_EQ:DIVISLAB": "NSE_EQ|INE361B01018",
    # "NSE_EQ:ULTRACEMCO": "NSE_EQ|INE481G01011",
    # Add smallcap shares like 63moons, POCL, DOLATALGO, SUVEN, GOLDIAM, HCC, BHAGCHEM, MOREPENLAB, KITEX, PURVA etc.
    "NSE_EQ:63MOONS": "NSE_EQ|INE111B01023",
    "NSE_EQ:POCL": "NSE_EQ|INE063E01053",
    "NSE_EQ:DOLATALGO": "NSE_EQ|INE966A01022",
    "NSE_EQ:SUVEN": "NSE_EQ|INE495B01038",
    "NSE_EQ:GOLDIAM": "NSE_EQ|INE025B01025",
    "NSE_EQ:HCC": "NSE_EQ|INE549A01026",
    "NSE_EQ:BHAGCHEM": "NSE_EQ|INE414D01027",
    "NSE_EQ:MOREPENLAB": "NSE_EQ|INE083A01026",
    "NSE_EQ:KITEX": "NSE_EQ|INE602G01020",
    "NSE_EQ:PURVA": "NSE_EQ|INE323I01011",
    #Add Ashok leyland,knrcon, indigo, ace, aartiind
    "NSE_EQ:ASHOKLEY": "NSE_EQ|INE208A01029",
    "NSE_EQ:KNRCON": "NSE_EQ|INE634I01029",
    "NSE_EQ:INDIGO": "NSE_EQ|INE646L01027",
    "NSE_EQ:ACE": "NSE_EQ|INE731H01025",
    "NSE_EQ:AARTIIND": "NSE_EQ|INE769A01020",
    #Add MRPL, CHAMBLFERT, NATCOPHARM, DEEPAKFERT, BHARATIHEXA, SUPREMEIND
    "NSE_EQ:MRPL": "NSE_EQ|INE103A01014",
    "NSE_EQ:CHAMBLFERT": "NSE_EQ|INE085A01013",
    "NSE_EQ:NATCOPHARM": "NSE_EQ|INE987B01026",
    "NSE_EQ:DEEPAKFERT": "NSE_EQ|INE501A01019",
    "NSE_EQ:BHARATIHEXA": "NSE_EQ|INE343G01021",
    "NSE_EQ:SUPREMEIND": "NSE_EQ|INE195A01028",
    #ADD BHARTIARTL, GMRAIRPORT, GODREJCP
    "NSE_EQ:GMRAIRPORT": "NSE_EQ|INE776C01039",
    "NSE_EQ:GODREJCP": "NSE_EQ|INE102D01028",
    #AHLUCONT, ASAHISONG, NIRAJ, AMNPLST GIVE ISIN CODE FOR THESE
    "NSE_EQ:AHLUCONT": "NSE_EQ|INE758C01029",
    "NSE_EQ:ASAHISONG": "NSE_EQ|INE228I01012",
    "NSE_EQ:NIRAJ": "NSE_EQ|INE368I01016",
    "NSE_EQ:AMNPLST": "NSE_EQ|INE275D01022",
    # Add more symbols here
}

# --- MONKEY PATCH: FORCE V3 PRODUCTION URLS ---
# Since we cannot edit upstox.py, we overwrite the URLs here at runtime.
print("🔧 Patching Upstox Driver to V3 Production...")
# upstox.base_urls['base'] = "https://api.upstox.com/v3"
# upstox.urls["place_order"] = "https://api.upstox.com/v3/order/place"
# Add other URLs if needed later (modify, cancel, etc.)

# Defining a Class which will store different symbols, allocated percentage capital and finally place trades

class TradeManager:
    def __init__(self, state_file="trade_state.json"):
        self.state_file = state_file
        self.positions = {} # { "SYMBOL": { "qty": 10, "order_id": "...", "entry_price": ... } }
        self.pending_locks = set() # Lock for pending orders
        self.load_state()

    def acquire_lock(self, symbol):
        if symbol in self.pending_locks: return False
        self.pending_locks.add(symbol)
        return True

    def release_lock(self, symbol):
        self.pending_locks.discard(symbol)

    def load_state(self):
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r') as f:
                    self.positions = json.load(f)
                print(f"📂 Loaded State: {len(self.positions)} active positions.")
            except Exception as e:
                print(f"⚠️ Error loading state: {e}")
                self.positions = {}
        else:
            self.positions = {}

    def save_state(self):
        # Atomic write to avoid corrupted state if process crashes during write
        try:
            temp_file = self.state_file + ".tmp"
            with open(temp_file, 'w') as f:
                json.dump(self.positions, f, indent=4)
            shutil.move(temp_file, self.state_file)
        except Exception as e:
            print(f"⚠️ Error saving state: {e}")

    def register_buy(self, symbol, qty, order_id, entry_price):
        self.positions[symbol] = {
            "qty": qty,
            "order_id": order_id,
            "entry_price": entry_price,
            "entry_time": time.time()
        }
        self.save_state()
        print(f"💾 State Saved: Bought {symbol} (Qty: {qty})")

    def cleanup_position(self, symbol):
        if symbol in self.positions:
            del self.positions[symbol]
            self.save_state()
            print(f"💾 State Saved: Sold {symbol}")

    def get_holdings_qty(self, symbol):
        return self.positions.get(symbol, {}).get('qty', 0)
    
    def get_entry_price(self, symbol):
        return self.positions.get(symbol, {}).get('entry_price', 0.0)

    def generate_unique_id(self, prefix="Algo"):
        timestamp = int(time.time() * 1000)
        rand_num = random.randint(1000, 9999)
        return f"{prefix}_{timestamp}_{rand_num}"

class ExecutionEngine:
    def __init__(self, trade_manager, logger, broker_headers):
        self.queue = queue.Queue()
        self.trade_manager = trade_manager
        self.logger = logger
        self.broker_headers = broker_headers
        self.running = True
        self.worker_thread = threading.Thread(target=self._worker, daemon=True)
        self.worker_thread.start()

    def submit_order(self, task):
        """
        task = { 
            "action": "BUY" | "SELL", 
            "symbol": str, 
            "qty": int, 
            "ltp": float, 
            "reason": str,
            "strategy": object 
        }
        """
        self.queue.put(task)

    def _worker(self):
        print("⚙️ Execution Engine Started (Threaded).")
        while self.running:
            try:
                task = self.queue.get(timeout=1)
            except queue.Empty:
                continue

            symbol = task.get("symbol")
            action = task.get("action")
            qty = task.get("qty")
            ltp = task.get("ltp")
            reason = task.get("reason")
            strategy = task.get("strategy")
            
            # print(f"⚙️ Processing {action} for {symbol}...")
            
            try:
                if action == "BUY":
                    self._execute_buy(symbol, qty, ltp, reason, strategy)
                elif action == "SELL":
                    self._execute_sell(symbol, qty, ltp, reason, strategy)
            except Exception as e:
                print(f"❌ Execution Error [{symbol}]: {e}")
            finally:
                # Release Lock
                self.trade_manager.release_lock(symbol)
            
            self.queue.task_done()
            time.sleep(0.2) # Rate Limit Safety (5 req/sec max)

    def _execute_buy(self, symbol, qty, ltp, reason, strategy):
        print(f"\n⚡ [{symbol}] Executing BUY Order for {qty} Qty...")
        try:
            unique_id = self.trade_manager.generate_unique_id("BUY")
            # Note: Using SANDBOX_TOKEN for Buy as per original code, change if needed
            resp = upstox.market_order_eq(
                exchange=ExchangeCode.NSE,
                symbol=symbol.split(":")[1], 
                quantity=qty,
                side=Side.BUY,
                unique_id=unique_id,
                headers=self.broker_headers,
                product=Product.NRML,
                validity=Validity.DAY
            )
            print(f"in main.py buy response: {resp}")
            if resp.get('status') == 'FILLED':
                order_id = resp.get('id', {})
                self.logger.log_trade("BUY", symbol, ltp, qty, 0.0, reason)
                
                # Update Persistent State
                self.trade_manager.register_buy(symbol, qty, order_id, ltp)
                
                print(f"✅ BUY SUCCESS [{symbol}]. Order ID: {order_id}")
            else:
                print(f"❌ BUY FAILED [{symbol}]. Response: {resp}")
        except Exception as e:
            print(f"❌ BUY Exception [{symbol}]: {e}")

    def _execute_sell(self, symbol, qty, ltp, reason, strategy):
        print(f"\n⚡ [{symbol}] Executing SELL Order for {qty} Qty...")
        try:
            unique_id = self.trade_manager.generate_unique_id("SELL")
            resp = upstox.market_order_eq(
                exchange=ExchangeCode.NSE,
                symbol=symbol.split(":")[1], 
                quantity=qty,
                side=Side.SELL,
                unique_id=unique_id,
                headers=self.broker_headers,
                product=Product.NRML,
                validity=Validity.DAY
            )
            print(f"in main.py sell response: {resp}")
            if resp.get('status') == 'FILLED':
                order_id = resp.get('id', {})
                
                entry_price = self.trade_manager.get_entry_price(symbol)
                pnl = (ltp - entry_price) * qty
                
                self.logger.log_trade("SELL", symbol, ltp, qty, pnl, reason)
                
                # Update Persistent State
                self.trade_manager.cleanup_position(symbol)
                
                print(f"✅ SELL SUCCESS [{symbol}]. Order ID: {order_id}")
            else:
                print(f"❌ SELL FAILED [{symbol}]. Response: {resp}")
        except Exception as e:
            print(f"❌ SELL Exception [{symbol}]: {e}")



def main():
    print("🚀 Initializing OEMS (Multi-Symbol V3 Hybrid)...")

    if not API_TOKEN:
        print(f"[Error] API Token not found. Please set UPSTOX_PROD_TOKEN in environment.")
        return

    # 1. Prepare Headers for upstox.py
    broker_headers = {
        "headers": {
            "Authorization": f"Bearer {SANDBOX_TOKEN}",
            "Accept": "application/json"
        }
    }

    # 2. Initialize Components
    # Pass the SYMBOLS_MAP to Data Feed
    data_feed = RobustDataFeed(API_TOKEN, SYMBOLS_MAP)
    
    logger = TradeRecorder("production_trades.csv", initial_capital=ALLOCATED_CAPITAL)
    
    # Initialize Strategy for EACH symbol
    strategies = {
        symbol: RSIChandelierStrategy(symbol) for symbol in SYMBOLS_MAP
    }
    
    trade_manager = TradeManager()
    
    # Initialize Execution Engine
    execution_engine = ExecutionEngine(trade_manager, logger, broker_headers)

    # Calculate Capital Per Symbol
    CAPITAL_PER_SYMBOL = ALLOCATED_CAPITAL / len(SYMBOLS_MAP)
    print(f"💰 Capital Allocation: ₹{CAPITAL_PER_SYMBOL:.2f} per symbol")

    # 3. Warmup & Start Data
    # Retry warmup a few times before giving up
    attempts = 0
    warmed = False
    while attempts < 3 and not warmed:
        if data_feed.initialize_data():
            warmed = True
            break
        attempts += 1
        wait = 2 ** attempts
        print(f"⚠️ Warmup attempt {attempts} failed; retrying in {wait}s...")
        time.sleep(wait)

    if not warmed:
        print("❌ Critical: Data Warmup Failed after retries. Exiting.")
        return

    data_feed.start_feed()
    
    print(f"--- 🎧 System Live for {len(SYMBOLS_MAP)} Symbols ---")
    # Print initial blank lines for dashboard to overwrite
    print("\n" * (len(SYMBOLS_MAP) + 2))
    
    try:
        while True:
            # A. Circuit Breaker
            if not data_feed.is_healthy:
                print("⚠️ System Paused: Waiting for Data Feed...", end='\r')
                time.sleep(1)
                continue

            try:
                # --- MULTI-SYMBOL LOOP ---
                for symbol in SYMBOLS_MAP:
                    # Per-symbol protections: skip if tick too old or no data
                    last_tick = None
                    try:
                        last_tick = data_feed.get_last_tick_time(symbol)
                    except Exception:
                        last_tick = None

                    if last_tick is None or (datetime.now() - last_tick).total_seconds() > 10:
                        # Data stale for this symbol -> skip
                        # print(f"⚠️ Stale data for {symbol}; skipping.")
                        continue

                    # Get Data for THIS symbol
                    try:
                        ltp = data_feed.get_ltp(symbol)
                        df_3m = data_feed.get_resampled_data(symbol, '3min')
                    except Exception as e:
                        print(f"⚠️ Error getting data for {symbol}: {e}")
                        continue

                    if ltp == 0 or df_3m is None:
                        continue

                    strategy = strategies[symbol]
                    
                    # Get Current Position State (Single Source of Truth)
                    current_qty = trade_manager.get_holdings_qty(symbol)
                    entry_price = trade_manager.get_entry_price(symbol) if current_qty > 0 else 0.0

                    try:
                        # B. Fast Tick Logic (Exits)
                        signal, reason = strategy.on_tick(ltp, current_qty, entry_price)
                        
                        if signal == "SELL":
                            sell_qty = current_qty # Use the source of truth
                            if sell_qty > 0:
                                if not trade_manager.acquire_lock(symbol):
                                    # Order in progress for symbol
                                    continue

                                print(f"\n⚡ [{symbol}] Queuing SELL Order for {sell_qty} Qty...")
                                execution_engine.submit_order({
                                    "action": "SELL",
                                    "symbol": symbol,
                                    "qty": sell_qty,
                                    "ltp": ltp,
                                    "reason": reason,
                                    "strategy": strategy
                                })

                        # C. Candle Logic (Entries)
                        signal, reason = strategy.on_candle_closed(df_3m, ltp, current_qty, entry_price)
                        
                        if signal == "BUY":
                            # Use Per-Symbol Capital
                            qty = math.floor(CAPITAL_PER_SYMBOL / ltp)
                            if qty < 1: 
                                continue
                            
                            if not trade_manager.acquire_lock(symbol):
                                continue

                            print(f"\n⚡ [{symbol}] Queuing BUY Order for {qty} Qty...")
                            execution_engine.submit_order({
                                "action": "BUY",
                                "symbol": symbol,
                                "qty": qty,
                                "ltp": ltp,
                                "reason": reason,
                                "strategy": strategy
                            })
                    except Exception as e:
                        print(f"⚠️ Strategy processing error for {symbol}: {e}")
                        traceback.print_exc()
                        continue

                # print(f"💓 Monitoring {len(SYMBOLS_MAP)} Symbols... | Healthy: {data_feed.is_healthy} | ltp: {ltp}   ", end='\r')
                
                # --- LIVE DASHBOARD EXPORT ---
                # Export state to JSON for external dashboard viewer
                try:
                    # Calculate Account Metrics
                    realized_pnl = logger.get_total_pnl()

                    # Treat capital + realized PnL as current equity and then back out margin used
                    account_balance = ALLOCATED_CAPITAL + realized_pnl

                    used_margin = 0.0
                    for s in SYMBOLS_MAP:
                        q = trade_manager.get_holdings_qty(s)
                        e = trade_manager.get_entry_price(s)
                        if q > 0:
                            used_margin += (q * e)
                            
                    # Cash that is still free to deploy
                    available_cash = account_balance - used_margin

                    dashboard_data = {
                        "last_updated": datetime.now().strftime('%H:%M:%S'),
                        "account": {
                            "capital": ALLOCATED_CAPITAL,
                            "balance": account_balance,
                            "realized_pnl": realized_pnl,
                            "used_margin": used_margin,
                            "available_cash": available_cash
                        },
                        "symbols": {}
                    }
                    
                    for sym in SYMBOLS_MAP:
                        l = data_feed.get_ltp(sym)
                        t = data_feed.get_last_tick_time(sym)
                        ts = t.strftime('%H:%M:%S') if t else "--:--:--"
                        
                        strat = strategies[sym]
                        rsi_val = getattr(strat, 'current_rsi', 0.0)
                        sl_val = getattr(strat, 'trailing_stop', 0.0)
                        
                        qty = trade_manager.get_holdings_qty(sym)
                        entry = trade_manager.get_entry_price(sym)
                        
                        dashboard_data["symbols"][sym] = {
                            "ltp": l,
                            "rsi": rsi_val,
                            "sl": sl_val,
                            "pos": qty,
                            "entry": entry,
                            "last_tick": ts
                        }
                    
                    # Atomic Write
                    temp_dash = "dashboard_state.json.tmp"
                    with open(temp_dash, 'w') as f:
                        json.dump(dashboard_data, f)
                    shutil.move(temp_dash, "dashboard_state.json")
                    
                except Exception as e:
                    pass
                
                # print(f"💓 Monitoring {len(SYMBOLS_MAP)} Symbols... | Healthy: {data_feed.is_healthy} | ltp: {ltp}   ", end='\r')
                time.sleep(0.5)

            except Exception as e:
                # Catch-all for loop-level exceptions to avoid process exit
                print(f"❌ Unexpected error in main loop: {e}")
                traceback.print_exc()
                time.sleep(5)

    except KeyboardInterrupt:
        print("\n🛑 Shutting down.")
        data_feed.stop_event.set()
        execution_engine.running = False # Stop worker

if __name__ == "__main__":
    main()
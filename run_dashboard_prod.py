import os
import time
import threading
import sys
from datetime import datetime
from dotenv import load_dotenv

# Upstox SDK Imports
import upstox_client
from upstox_client.rest import ApiException
from upstox_client.feeder.portfolio_data_streamer import PortfolioDataStreamer
from upstox_client.feeder.market_data_streamer_v3 import MarketDataStreamerV3

# Load Environment Variables
load_dotenv()

class ProdDashboard:
    def __init__(self):
        self.api_token = os.getenv("UPSTOX_PROD_TOKEN")
        if not self.api_token:
            print("❌ Error: UPSTOX_PROD_TOKEN not found in .env file.")
            sys.exit(1)

        # Configure API Client
        self.config = upstox_client.Configuration()
        self.config.access_token = self.api_token
        self.api_client = upstox_client.ApiClient(self.config)
        
        # APIs
        self.user_api = upstox_client.UserApi(self.api_client)
        self.portfolio_api = upstox_client.PortfolioApi(self.api_client)

        # State
        self.positions = {}      # { instrument_token: { quantity, average_price, ... } }
        self.market_data = {}    # { instrument_token: { ltp: 0.0 } }
        self.funds = {
            "available_margin": 0.0,
            "used_margin": 0.0,
            "total_realized_pnl": 0.0
        }
        
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.subscribed_instruments = set()
        
        # Streamers
        self.portfolio_streamer = None
        self.market_streamer = None

    # --- DATA FETCHING ---
    def fetch_funds(self):
        try:
            resp = self.user_api.get_user_fund_margin(segment="SEC", api_version="v2")
            print(f"\n--- FUNDS RESPONSE ---\n{resp}\n----------------------\n")
            if resp and resp.data:
                funds = resp.data.funds
                with self.lock:
                    self.funds["available_margin"] = funds.available_margin
                    self.funds["used_margin"] = funds.used_margin
        except Exception as e:
            print(f"Error fetching funds: {e}")
            pass

    def fetch_positions_snapshot(self):
        try:
            resp = self.portfolio_api.get_positions()
            print(f"\n--- POSITIONS RESPONSE ---\n{resp}\n--------------------------\n")
            print(f"Fetched {len(resp.data)} positions")
            if resp and resp.data:
                with self.lock:
                    # Reset realized PnL calculation on full snapshot
                    self.funds["total_realized_pnl"] = 0.0
                    
                    for pos in resp.data:
                        # Convert object to dict if necessary, or access attributes directly
                        # The SDK usually returns objects with snake_case attributes
                        token = pos.instrument_token
                        qty = pos.quantity
                        
                        self.positions[token] = {
                            "symbol": pos.trading_symbol,
                            "quantity": pos.quantity,
                            "buy_price": pos.buy_price, # Average buy price
                            "sell_price": pos.sell_price, # Average sell price
                            "value": pos.value, # Current Value
                            "pnl": pos.pnl, # Realized PnL usually
                            "realized_pnl": pos.realized_pnl,
                            "unrealized_pnl": pos.unrealized_pnl,
                            "average_price": pos.average_price,
                            "instrument_token": pos.instrument_token
                        }
                        
                        # Sum up realized PnL
                        if pos.realized_pnl:
                            self.funds["total_realized_pnl"] += pos.realized_pnl

                        # Subscribe to LTP if active position
                        if qty != 0:
                            self.subscribe_market_data(token)
                            
        except Exception as e:
            # print(f"Error fetching positions: {e}")
            pass

    # --- WEBSOCKET HANDLERS ---
    
    def on_portfolio_update(self, message):
        # Handle Portfolio Streamer Updates
        # Message structure depends on SDK. Usually it's a list of updates.
        try:
            # print(f"Portfolio Update: {message}")
            pass 
            # Note: For simplicity in this version, we will rely on 
            # periodic polling of get_positions() + Market Data Streamer for LTP.
            # Portfolio Streamer implementation can be complex due to message parsing.
            # But we will initialize it as requested.
        except Exception as e:
            pass

    def on_market_data_update(self, message):
        # Handle LTP Updates
        try:
            feeds = message.get('feeds', {})
            with self.lock:
                for key, feed in feeds.items():
                    ltpc = feed.get('fullFeed', {}).get('marketFF', {}).get('ltpc', {})
                    ltp = ltpc.get('ltp')
                    if ltp:
                        if key not in self.market_data:
                            self.market_data[key] = {}
                        self.market_data[key]['ltp'] = ltp
                        self.market_data[key]['last_updated'] = datetime.now()
        except Exception:
            pass

    def subscribe_market_data(self, instrument_token):
        if self.market_streamer and instrument_token not in self.subscribed_instruments:
            self.subscribed_instruments.add(instrument_token)
            # print(f"Subscribing to {instrument_token}")
            try:
                self.market_streamer.subscribe([instrument_token], "full")
            except:
                pass

    # --- INITIALIZATION ---
    
    def start(self):
        print("🚀 Starting Production Dashboard...")
        
        # 1. Initial Fetch
        self.fetch_funds()
        self.fetch_positions_snapshot()
        
        # 2. Start Market Data Streamer
        self.market_streamer = MarketDataStreamerV3(self.api_client)
        self.market_streamer.on("message", self.on_market_data_update)
        self.market_streamer.connect()
        
        # 3. Start Portfolio Streamer (As requested, though polling is often safer for simple dashboards)
        # self.portfolio_streamer = PortfolioDataStreamer(self.api_client, position_update=True)
        # self.portfolio_streamer.on("message", self.on_portfolio_update)
        # self.portfolio_streamer.connect()
        
        # 4. Start Background Poller for Funds & Positions (Sync)
        # We poll positions to ensure we don't miss anything from the streamer
        t = threading.Thread(target=self.background_poller, daemon=True)
        t.start()
        
        # 5. UI Loop
        self.ui_loop()

    def background_poller(self):
        while not self.stop_event.is_set():
            time.sleep(5)
            self.fetch_funds()
            self.fetch_positions_snapshot()

    def clear_screen(self):
        os.system('cls' if os.name == 'nt' else 'clear')

    def ui_loop(self):
        try:
            while not self.stop_event.is_set():
                self.render()
                time.sleep(1)
        except KeyboardInterrupt:
            print("\nExiting...")
            self.stop_event.set()

    def render(self):
        with self.lock:
            # self.clear_screen()
            now = datetime.now().strftime('%H:%M:%S')
            
            avail = self.funds["available_margin"]
            used = self.funds["used_margin"]
            realized = self.funds["total_realized_pnl"]
            
            # Calculate Total Unrealized PnL
            total_unrealized = 0.0
            
            # print(f"=====================================================================================================")
            # print(f" 📊 OEMS PROD DASHBOARD (API CONNECTED) | Time: {now}")
            # print(f" 💵 CASH: {avail:,.2f} | 📉 USED MARGIN: {used:,.2f} | 💰 REALIZED P&L: {realized:,.2f}")
            # print(f"=====================================================================================================")
            # print(f"{'SYMBOL':<15} | {'LTP':<10} | {'QTY':<6} | {'AVG PRICE':<10} | {'P&L':<10} | {'VAL':<10}")
            # print(f"-----------------------------------------------------------------------------------------------------")
            
            # # Filter for active positions or positions with PnL
            # active_positions = [p for p in self.positions.values() if p['quantity'] != 0]
            
            # for pos in active_positions:
            #     token = pos['instrument_token']
            #     symbol = pos['symbol']
            #     qty = pos['quantity']
            #     avg_price = pos['average_price']
                
            #     # Get LTP
            #     ltp = self.market_data.get(token, {}).get('ltp', 0.0)
                
            #     # Calculate PnL
            #     # If LTP is available, calculate MTM
            #     pnl = 0.0
            #     if ltp > 0:
            #         # For Long: (LTP - Avg) * Qty
            #         # For Short: (Avg - LTP) * Qty (Qty is negative for short usually? Upstox Qty is signed)
            #         # Upstox Qty: +ve for Buy, -ve for Sell
            #         pnl = (ltp - avg_price) * qty
            #     else:
            #         # Fallback to API provided unrealized PnL if LTP not ready
            #         pnl = pos.get('unrealized_pnl', 0.0)
                
            #     total_unrealized += pnl
                
            #     val = ltp * abs(qty)
                
            #     print(f"{symbol:<15} | {ltp:<10.2f} | {qty:<6} | {avg_price:<10.2f} | {pnl:<10.2f} | {val:<10.2f}")

            # print(f"-----------------------------------------------------------------------------------------------------")
            # print(f" 💰 TOTAL UNREALIZED P&L: {total_unrealized:,.2f}")
            # print(f"=====================================================================================================")
            # print(f"\n(Press Ctrl+C to exit)")

if __name__ == "__main__":
    dashboard = ProdDashboard()
    dashboard.start()

# import time
# import threading
# import pandas as pd
# import requests
# import urllib.parse
# from datetime import datetime
# import json
# import websocket # pip install websocket-client
# import ssl

# # Attempt to import Upstox V3 SDK components
# # We use a try-block because SDK internal paths change frequently
# try:
#     import upstox_client
#     from upstox_client.feeder.streamer import Streamer
#     # from upstox_client.feeder.market_data_feed import MarketDataFeed
#     SDK_AVAILABLE = True
# except ImportError:
#     print("⚠️ Upstox SDK Feeder modules not found. Using Direct WebSocket fallback.")
#     SDK_AVAILABLE = False

# class RobustDataFeed:
#     def __init__(self, access_token, instrument_key, symbol):
#         self.access_token = access_token
#         self.instrument_key = instrument_key
#         self.symbol = symbol
        
#         # Data Containers (Thread-Safe)
#         self.df = pd.DataFrame()
#         self.ltp = 0.0
#         self.last_candle_time = None
        
#         # State
#         self.is_healthy = False
#         self.lock = threading.Lock()
#         self.stop_event = threading.Event()
#         self.ws = None
        
#         # REST Session for History (Gap Filling)
#         self.session = requests.Session()
#         self.session.headers.update({
#             "Authorization": f"Bearer {self.access_token}",
#             "Accept": "application/json"
#         })

#     def _get_v3_history(self):
#         """Fetches historical candles for gap recovery (REST API)."""
#         encoded_key = urllib.parse.quote(self.instrument_key)
#         # V3 Endpoint
#         url = f"https://api.upstox.com/v3/historical-candle/intraday/{encoded_key}/minutes/1"
#         try:
#             response = self.session.get(url, timeout=5)
#             if response.status_code == 200:
#                 data = response.json().get('data', {}).get('candles', [])
#                 if data:
#                     df = pd.DataFrame(data, columns=['timestamp', 'Open', 'High', 'Low', 'Close', 'Volume', 'OI'])
                    
#                     # 1. Convert to Datetime
#                     df['timestamp'] = pd.to_datetime(df['timestamp'])
                    
#                     # 2. FIX: Remove Timezone info to make it "Naive" (compatible with datetime.now)
#                     if df['timestamp'].dt.tz is not None:
#                         df['timestamp'] = df['timestamp'].dt.tz_localize(None)

#                     df.set_index('timestamp', inplace=True)
#                     df.sort_index(inplace=True)
#                     return df
#         except Exception as e:
#             print(f"❌ History Fetch Error: {e}")
#         return None

#     def initialize_data(self):
#         """Blocking Warmup."""
#         print(f"🔥 Warming up {self.symbol}...")
#         df = self._get_v3_history()
#         if df is not None and not df.empty:
#             with self.lock:
#                 self.df = df
#                 self.last_candle_time = df.index[-1]
#                 self.ltp = df['Close'].iloc[-1]
#                 self.is_healthy = True
#             print(f"✅ Warmup Complete. LTP: {self.ltp}")
#             return True
#         return False

#     def _recover_sync(self):
#         """Background Gap Filler."""
#         if self.lock.locked(): return
        
#         with self.lock: self.is_healthy = False
        
#         df = self._get_v3_history()
#         if df is not None:
#             with self.lock:
#                 self.df = pd.concat([self.df, df])
#                 self.df = self.df[~self.df.index.duplicated(keep='last')].iloc[-5000:]
#                 self.last_candle_time = self.df.index[-1]
#                 # Update LTP from latest candle if needed
#                 if self.ltp == 0: self.ltp = df['Close'].iloc[-1]
#                 self.is_healthy = True

#     # --- DIRECT WEBSOCKET IMPLEMENTATION (Robust Fallback) ---
#     # Since V3 SDK imports are flaky, this manually connects to the V3 socket.
#     # Note: V3 uses Protobuf. Without the .proto file, we cannot decode binary.
#     # So we use the "Market Quote" polling if SDK fails, OR we use the V2 socket 
#     # which is JSON based if your token supports it (Upstox is phasing it out).
    
#     # BEST APPROACH: Use the HTTP Poller I wrote earlier if SDK fails.
#     # It is 100% reliable for V3 without needing complex Protobuf compilers.
    
#     def _run_poll_stream(self):
#         """High-Frequency Polling (0.5s) for reliable V3 Data."""
#         print("📡 Starting High-Frequency Poller (SDK Backup)...")
#         url = "https://api.upstox.com/v3/market-quote/ltp"
#         params = {"instrument_key": self.instrument_key}
        
#         while not self.stop_event.is_set():
#             try:
#                 resp = self.session.get(url, params=params, timeout=2)
#                 if resp.status_code == 200:
#                     data = resp.json()
#                     # Parse generic V3 response
#                     quotes = data.get('data', {})
#                     # Find our key
#                     for key, val in quotes.items():
#                         if self.instrument_key in key:
#                             ltp = val.get('last_price')
#                             if ltp:
#                                 with self.lock:
#                                     self.ltp = ltp
#                                     self.is_healthy = True
                
#                 # Check candles
#                 now = datetime.now()
#                 if self.last_candle_time and (now - self.last_candle_time).seconds > 65:
#                     threading.Thread(target=self._recover_sync, daemon=True).start()
                    
#             except Exception as e:
#                 print(f"Printing excetption here {e}")
#                 with self.lock: self.is_healthy = False
            
#             time.sleep(0.5)

#     def start_feed(self):
#         # Always use the Poller for now as it solves the SDK Import Error definitively
#         # and guarantees V3 API usage.
#         t = threading.Thread(target=self._run_poll_stream, daemon=True)
#         t.start()

#     def get_resampled_data(self, timeframe):
#         with self.lock:
#             if self.df.empty: return None
#             return self.df.resample(timeframe).agg({'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last'}).dropna()

import time
import threading
import pandas as pd
import requests
import urllib.parse
from datetime import datetime, timedelta
import json
from concurrent.futures import ThreadPoolExecutor

# --- SDK IMPORT FIX ---
# We attempt to import the V3 Streamer correctly.
SDK_AVAILABLE = False
try:
    import upstox_client
    # The correct import for V3 in SDK v2.12+
    from upstox_client.feeder.market_data_streamer_v3 import MarketDataStreamerV3
    from upstox_client.feeder.streamer import Streamer
    SDK_AVAILABLE = True
except ImportError as e:
    print(f"⚠️ SDK Import Error: {e}")
    # Fallback to polling if SDK is still broken
    SDK_AVAILABLE = False

class RobustDataFeed:
    def __init__(self, access_token, symbol_map):
        """
        symbol_map: dict { "SYMBOL_NAME": "INSTRUMENT_KEY" }
        Example: { "NSE_EQ:MARUTI": "NSE_EQ|INE...", "NSE_EQ:RELIANCE": "NSE_EQ|INE..." }
        """
        self.access_token = access_token
        self.symbol_map = symbol_map
        self.key_to_symbol = {v: k for k, v in symbol_map.items()} # Reverse map for lookup
        
        # Data Containers (Dicts for Multi-Symbol)
        self.dfs = {sym: pd.DataFrame() for sym in symbol_map}
        self.ltps = {sym: 0.0 for sym in symbol_map}
        self.last_candle_times = {sym: None for sym in symbol_map}
        # Track last tick arrival time per symbol (freshness)
        self.last_tick_times = {sym: None for sym in symbol_map}
        
        # State
        self.is_healthy = False
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.streamer = None 
        self.executor = ThreadPoolExecutor(max_workers=5)
        
        # Setup Upstox Config
        if SDK_AVAILABLE:
            self.config = upstox_client.Configuration()
            self.config.access_token = self.access_token
        
        # REST Session for History (Gap Filling)
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {self.access_token}",
            "Accept": "application/json"
        })

    def _get_v3_history(self, instrument_key, full_history=True):
        """Fetches historical candles via REST (Gap Recovery)."""
        encoded_key = urllib.parse.quote(instrument_key)
        df = None
        
        # 1. Fetch Last 5 Days History (Only if requested)
        if full_history:
            now = datetime.now()
            to_date = now.strftime("%Y-%m-%d")
            from_date = (now - timedelta(days=5)).strftime("%Y-%m-%d")
            
            hist_url = f"https://api.upstox.com/v3/historical-candle/{encoded_key}/minutes/1/{to_date}/{from_date}"
            
            try:
                response = self.session.get(hist_url, timeout=5)
                if response.status_code == 200:
                    data = response.json().get('data', {}).get('candles', [])
                    if data:
                        df = pd.DataFrame(data, columns=['timestamp', 'Open', 'High', 'Low', 'Close', 'Volume', 'OI'])
                        df['timestamp'] = pd.to_datetime(df['timestamp'])
                        if df['timestamp'].dt.tz is not None:
                            df['timestamp'] = df['timestamp'].dt.tz_localize(None)
                        df.set_index('timestamp', inplace=True)
            except Exception as e:
                # print(f"   ⚠️ History Fetch Error: {e}")
                pass

        # 2. Fetch Intraday Data (Today)
        intra_url = f"https://api.upstox.com/v3/historical-candle/intraday/{encoded_key}/minutes/1"
        
        try:
            response = self.session.get(intra_url, timeout=5)
            if response.status_code == 200:
                data = response.json().get('data', {}).get('candles', [])
                if data:
                    df_intra = pd.DataFrame(data, columns=['timestamp', 'Open', 'High', 'Low', 'Close', 'Volume', 'OI'])
                    df_intra['timestamp'] = pd.to_datetime(df_intra['timestamp'])
                    if df_intra['timestamp'].dt.tz is not None:
                        df_intra['timestamp'] = df_intra['timestamp'].dt.tz_localize(None)
                    df_intra.set_index('timestamp', inplace=True)
                    
                    if df is not None:
                        df = pd.concat([df, df_intra])
                    else:
                        df = df_intra
        except Exception as e:
            # print(f"   ⚠️ Intraday Fetch Error: {e}")
            pass

        # 3. Cleanup
        if df is not None and not df.empty:
            df = df[~df.index.duplicated(keep='last')]
            df.sort_index(inplace=True)
            print(f"   ✅ Fetched {len(df)} candles for {instrument_key}")
            return df
            
        return None

    def initialize_data(self):
        """Blocking Warmup for ALL symbols."""
        print(f"🔥 Warming up {len(self.symbol_map)} symbols...")
        success_count = 0
        
        for symbol, key in self.symbol_map.items():
            print(f"   ⏳ Fetching history for {symbol}...")
            df = self._get_v3_history(key, full_history=True)
            if df is not None and not df.empty:
                with self.lock:
                    self.dfs[symbol] = df
                    self.last_candle_times[symbol] = df.index[-1]
                    self.ltps[symbol] = df['Close'].iloc[-1]
                success_count += 1
            time.sleep(0.02) # Rate limit protection

        if success_count > 0:
            self.is_healthy = True
            print(f"✅ Warmup Complete. {success_count}/{len(self.symbol_map)} symbols ready.")
            return True
        return False

    def _recover_sync(self, symbol):
        """Background Gap Filler for a specific symbol."""
        # if self.lock.locked(): return # Avoid contention
        
        key = self.symbol_map[symbol]
        df = self._get_v3_history(key, full_history=False)
        
        if df is not None:
            with self.lock:
                current_df = self.dfs[symbol]
                updated_df = pd.concat([current_df, df])
                # Remove duplicates and keep last 5000
                self.dfs[symbol] = updated_df[~updated_df.index.duplicated(keep='last')].iloc[-5000:]
                self.last_candle_times[symbol] = self.dfs[symbol].index[-1]
                
                # Update LTP if 0
                if self.ltps[symbol] == 0: 
                    self.ltps[symbol] = df['Close'].iloc[-1]

    # --- TRUE V3 WEBSOCKET IMPLEMENTATION ---

    def on_open(self):
        print("📡 WebSocket Connected (V3).")
        # FIX: Subscribe ONLY after connection is open
        if self.streamer:
            keys_to_subscribe = list(self.symbol_map.values())
            print(f"🚀 Subscribing to {len(keys_to_subscribe)} instruments...")
            self.streamer.subscribe(keys_to_subscribe, "full")

    def on_close(self, code, reason):
        print(f"🔌 WebSocket Closed: {reason}")
        self.is_healthy = False
        
    def on_error(self, error):
        print(f"❌ WebSocket Error: {error}")
        self.is_healthy = False

    def on_message(self, message):
        """Handles V3 Protobuf Message."""
        try:
            feeds = message.get('feeds', {})
            
            with self.lock:
                for key, feed in feeds.items():
                    if key in self.key_to_symbol:
                        symbol = self.key_to_symbol[key]
                        
                        # V3 Structure: feeds -> key -> ltpc -> ltp
                        ltpc = feed.get('fullFeed', {}).get('marketFF', {}).get('ltpc', {})
                        new_ltp = ltpc.get('ltp')
                        
                        if new_ltp:
                            # print(f"   🔄 {symbol}: {new_ltp}", end='\r')
                            self.ltps[symbol] = new_ltp
                            # update per-symbol tick time for freshness checks
                            try:
                                self.last_tick_times[symbol] = datetime.now()
                            except Exception:
                                pass
                            self.is_healthy = True
            
            # Sync logic moved to Watchdog
                
        except Exception as e:
            print(f"⚠️ Parse Error: {e}")

    def _run_watchdog(self):
        """Background Monitor to check for stale data."""
        print("🐶 Watchdog started.")
        while not self.stop_event.is_set():
            try:
                now = datetime.now()
                for symbol in self.symbol_map:
                    last_time = self.last_candle_times.get(symbol)
                    if last_time and (now - last_time).seconds > 65:
                        # Trigger sync for this symbol in background using ThreadPool
                        self.executor.submit(self._recover_sync, symbol)
            except Exception as e:
                print(f"🐶 Watchdog Error: {e}")
            
            time.sleep(10) # Check every 10 seconds

    def _run_websocket(self):
        """Runs the V3 SDK Streamer."""
        self.streamer = MarketDataStreamerV3(upstox_client.ApiClient(self.config))
        self.streamer.on("open", self.on_open)
        self.streamer.on("message", self.on_message)
        self.streamer.on("error", self.on_error)
        self.streamer.on("close", self.on_close)
        
        self.streamer.connect()
        
        while not self.stop_event.is_set():
            time.sleep(1)

    def start_feed(self):
        if SDK_AVAILABLE:
            t = threading.Thread(target=self._run_websocket, daemon=True)
            t.start()
            
            # Start Watchdog
            w = threading.Thread(target=self._run_watchdog, daemon=True)
            w.start()
        else:
            print("⚠️ SDK not available. Polling fallback not implemented for multi-symbol yet.")

    def get_resampled_data(self, symbol, timeframe):
        with self.lock:
            df = self.dfs.get(symbol)
            if df is None or df.empty: return None
            return df.resample(timeframe).agg({'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last'}).dropna()
    
    def get_ltp(self, symbol):
        return self.ltps.get(symbol, 0.0)

    def get_last_tick_time(self, symbol):
        return self.last_tick_times.get(symbol)
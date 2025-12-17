import numpy as np
import pandas as pd
from Strategies.base import BaseStrategy

class RSIChandelierStrategy(BaseStrategy):
    def __init__(self, symbol):
        super().__init__("RSI_Chand_Dynamic_V4", symbol)
        self.trailing_stop = 0.0
        
        # Parameters
        self.rsi_short = 9
        self.rsi_long = 15
        self.atr_per = 22
        
        # Multipliers
        self.mult_standard = 3.0   # Normal breathing room
        self.mult_tight = 1.0      # Tight trail when RSI is hot
        self.be_trigger = 1.5      # Move to BE if profit > 1.5 ATR
        
        self.current_rsi = 0.0

    def _calc(self, df):
        df = df.copy()
        delta = df['Close'].diff()
        
        # RSI Calculation
        gain = (delta.where(delta > 0, 0)).ewm(com=self.rsi_short-1).mean()
        loss = (-delta.where(delta < 0, 0)).ewm(com=self.rsi_short-1).mean()
        rs_s = gain / (loss + 1e-10)
        
        gain_l = (delta.where(delta > 0, 0)).ewm(com=self.rsi_long-1).mean()
        loss_l = (-delta.where(delta < 0, 0)).ewm(com=self.rsi_long-1).mean()
        rs_l = gain_l / (loss_l + 1e-10)
        
        df['RSI'] = ( (100 - (100/(1+rs_s))) + (100 - (100/(1+rs_l))) ) / 2

        # ATR Calculation
        high_low = df['High'] - df['Low']
        high_close = np.abs(df['High'] - df['Close'].shift())
        low_close = np.abs(df['Low'] - df['Close'].shift())
        tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        df['ATR'] = tr.ewm(com=self.atr_per-1).mean()
        
        # We calculate the basic Chandelier here for visualization, 
        # but the specific stop value is handled dynamically in on_candle_closed
        df['Chand'] = df['High'].rolling(self.atr_per).max() - (df['ATR'] * self.mult_standard)
        
        return df

    def on_tick(self, ltp, current_qty, entry_price):
        # Intra-candle stop loss execution
        if current_qty > 0 and self.trailing_stop > 0:
            if ltp <= self.trailing_stop:
                return "SELL", f"Stop Hit {self.trailing_stop:.2f}"
        return None, None

    def on_candle_closed(self, df, ltp, current_qty, entry_price):
        if len(df) < 30: return None, None
        
        df = self._calc(df)
        
        curr_rsi = df['RSI'].iloc[-1]
        self.current_rsi = curr_rsi
        curr_atr = df['ATR'].iloc[-1]
        
        # Calculate Highest High for Chandelier logic
        highest_high = df['High'].rolling(self.atr_per).max().iloc[-1]

        # --- EXIT & TRAILING LOGIC ---
        if current_qty > 0:
            
            # 1. Determine Dynamic Multiplier
            # If RSI is > 70 (Overbought), tighten the stop. Otherwise use standard.
            current_mult = self.mult_tight if curr_rsi > 70 else self.mult_standard
            
            # Calculate the potential new stop price based on this multiplier
            dynamic_chand_stop = highest_high - (curr_atr * current_mult)
            
            # 2. Breakeven Check
            # If we haven't hit BE yet, check if we are far enough in profit
            if self.trailing_stop < entry_price:
                profit_distance = ltp - entry_price
                if profit_distance > (curr_atr * self.be_trigger):
                    # Move stop to Entry Price (or slightly above to cover fees)
                    self.trailing_stop = entry_price
                    print(f"Breakeven Triggered. Stop moved to {entry_price}")
            
            # 3. Update Trailing Stop (Standard/Dynamic)
            # Only update if the new calculated Chandelier is HIGHER than our current stop.
            # This ensures we never lower the stop, even if RSI cools down.
            if not np.isnan(dynamic_chand_stop):
                if dynamic_chand_stop > self.trailing_stop:
                    self.trailing_stop = dynamic_chand_stop

            return None, None

        # --- ENTRY LOGIC ---
        else:
            # Re-calculate standard chandelier for entry condition
            entry_chand = highest_high - (curr_atr * self.mult_standard)
            
            prev_rsi = df['RSI'].iloc[-2]
            
            # Buy if RSI < 50, RSI is increasing, and Price > Chandelier + 1 ATR
            if curr_rsi < 50 and curr_rsi > prev_rsi and ltp > (entry_chand + curr_atr):
                self.trailing_stop = entry_chand
                return "BUY", "Signal"
        
        return None, None
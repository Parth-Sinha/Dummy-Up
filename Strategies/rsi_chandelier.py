import numpy as np
import pandas as pd
from Strategies.base import BaseStrategy

class RSIChandelierStrategy(BaseStrategy):
    def __init__(self, symbol):
        super().__init__("RSI_Chandelier_V3", symbol)
        self.trailing_stop = 0.0
        self.rsi_short = 9
        self.rsi_long = 15
        self.atr_per = 22
        self.mult = 3
        self.current_rsi = 0.0

    def _calc(self, df):
        df = df.copy()
        delta = df['Close'].diff()
        
        gain = (delta.where(delta > 0, 0)).ewm(com=self.rsi_short-1).mean()
        loss = (-delta.where(delta < 0, 0)).ewm(com=self.rsi_short-1).mean()
        rs_s = gain / (loss + 1e-10)
        
        gain_l = (delta.where(delta > 0, 0)).ewm(com=self.rsi_long-1).mean()
        loss_l = (-delta.where(delta < 0, 0)).ewm(com=self.rsi_long-1).mean()
        rs_l = gain_l / (loss_l + 1e-10)
        
        df['RSI'] = ( (100 - (100/(1+rs_s))) + (100 - (100/(1+rs_l))) ) / 2

        high_low = df['High'] - df['Low']
        high_close = np.abs(df['High'] - df['Close'].shift())
        low_close = np.abs(df['Low'] - df['Close'].shift())
        tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        atr = tr.ewm(com=self.atr_per-1).mean()
        df['ATR'] = atr
        df['Chand'] = df['High'].rolling(self.atr_per).max() - (atr * self.mult)
        return df

    def on_tick(self, ltp, current_qty, entry_price):
        if current_qty > 0 and self.trailing_stop > 0:
            
            if ltp <= self.trailing_stop:
                return "SELL", f"Stop Hit {self.trailing_stop}"
        return None, None

    def on_candle_closed(self, df, ltp, current_qty, entry_price):
        if len(df) < 30: return None, None
        df = self._calc(df)
        curr_rsi = df['RSI'].iloc[-1]
        self.current_rsi = curr_rsi # Store for dashboard
        curr_chand = df['Chand'].iloc[-1]
        curr_atr = df['ATR'].iloc[-1]
        
        if current_qty > 0:
            if not np.isnan(curr_chand) and curr_chand > self.trailing_stop:
                self.trailing_stop = curr_chand
        # Only buy if ltp > curr_chand + 1 * atr
        elif curr_rsi < 50 and curr_rsi > df['RSI'].iloc[-2] and ltp > (curr_chand + curr_atr):
            self.trailing_stop = curr_chand
            return "BUY", "Signal"
        
        return None, None
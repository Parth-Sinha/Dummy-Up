import pandas as pd
from backtesting import Backtest, Strategy


def rsi_pd(series: pd.Series, n: int) -> pd.Series:
    """Calculates Relative Strength Index (RSI) using Wilder's smoothing."""
    delta = series.diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    
    # Use exponential moving average (Wilder's smoothing)
    avg_gain = gain.ewm(com=n - 1, min_periods=n).mean()
    avg_loss = loss.ewm(com=n - 1, min_periods=n).mean()
    
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

def atr_pd(high: pd.Series, low: pd.Series, close: pd.Series, n: int) -> pd.Series:
    """Calculates Average True Range (ATR) using Wilder's smoothing."""
    tr1 = pd.DataFrame(high - low)
    tr2 = pd.DataFrame(abs(high - close.shift(1)))
    tr3 = pd.DataFrame(abs(low - close.shift(1)))
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    
    # Use exponential moving average (Wilder's smoothing)
    return tr.ewm(com=n - 1, min_periods=n).mean()

# --- Main Strategy Class ---

class RsiMeanReversion(Strategy):
    # These can be optimized by the backtesting framework.
    short_rsi_period = 9
    long_rsi_period = 15
    oversold_level = 50.0
    atr_period = 22
    atr_multiplier = 3.0

    def init(self):

        
        # Make a copy of the original 1-minute data DataFrame
        price_df_1m = self.data.df.copy()

        # Resample 1-minute data to 3-minute data.
        # We define how to aggregate the OHLC columns.
        ohlc_dict = {'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last'}
        df_3m = price_df_1m.resample('3T').agg(ohlc_dict).dropna()
        


        # 1. Custom Dual RSI
        rsi9 = rsi_pd(df_3m['Close'], self.short_rsi_period)
        rsi15 = rsi_pd(df_3m['Close'], self.long_rsi_period)
        dual_rsi_5m = (rsi9 + rsi15) / 2
        
        # 2. Chandelier Exit
        atr_5m = atr_pd(df_3m['High'], df_3m['Low'], df_3m['Close'], self.atr_period)
        highest_high_5m = df_3m['High'].rolling(self.atr_period).max()
        chandelier_exit_5m = highest_high_5m - (atr_5m * self.atr_multiplier)

        # Combine calculated indicators into a single 5-minute DataFrame
        indicators_5m = pd.DataFrame(index=df_3m.index)
        indicators_5m['dual_rsi'] = dual_rsi_5m
        indicators_5m['chandelier_exit'] = chandelier_exit_5m
        
        aligned_indicators = indicators_5m.reindex(price_df_1m.index, method='ffill')
        
        self.dual_rsi = self.I(lambda: aligned_indicators['dual_rsi'])
        self.chandelier_exit = self.I(lambda: aligned_indicators['chandelier_exit'])
        
        # Initialize the trailing stop variable for active trades
        self.trailing_stop = 0.0

    def next(self):
        
        # First, check if we are in a position. If so, manage the exit.
        if self.position:
            new_chandelier_value = self.chandelier_exit[-1]
            
            if pd.notna(new_chandelier_value):
                self.trailing_stop = max(self.trailing_stop, new_chandelier_value)
            
            # Check if the low of the current 1-minute bar has breached the stop
            if self.data.Low[-1] <= self.trailing_stop:
                self.position.close()
                self.trailing_stop = 0.0 
                return 


        # If we are not in a position, check for entry signals.
        if not self.position:
            is_oversold = self.dual_rsi[-1] < self.oversold_level

            is_turning_up = self.dual_rsi[-1] > self.dual_rsi[-2]
            
            # If all conditions are met, execute a buy order
            if is_oversold and is_turning_up:
                self.buy()
                self.trailing_stop = self.chandelier_exit[-1]

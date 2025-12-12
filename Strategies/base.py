from abc import ABC, abstractmethod

class BaseStrategy(ABC):
    def __init__(self, name, symbol):
        self.name = name
        self.symbol = symbol
        # State is now managed externally (Single Source of Truth)

    @abstractmethod
    def on_tick(self, ltp, current_qty, entry_price):
        pass

    @abstractmethod
    def on_candle_closed(self, df, ltp, current_qty, entry_price):
        pass

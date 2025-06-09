import pandas as pd
from typing import Tuple, Dict
#lets call the load_data from data.py
from data import load_data

#lets first create a signal series that is 1.0 when fast EMA > slow EMA, else 0.0
#also create a position series which is the change in the signal (i.e +1 on buy cross, or -1 on sell cross)

def generate_ema_signals(
    df: pd.DataFrame,
    fast_period: int =20,
    slow_period: int=50
) -> pd.DataFrame:
    """Given a dataframe with a close column, compute the fast and slow EMAS, 
    a signal (1 when fast > slow, else 0)
    a position series (+1.0 on buy, -1.0 on sell, 0 otherwise)
    Returns a new DataFrame including these columns

    Args:
        df (pd.DataFrame):
        fast_period (int, optional):. Defaults to 20.
        slow_period (int, optional):. Defaults to 50.

    Returns:
        pd.DataFrame: returns columns for each ticker of the signal and position
    """
    signals = pd.DataFrame(index=df.index)
    signals['close'] = df['close']
    
    #Fast and Slow EMAs
    signals[f'ema_fast_{fast_period}'] = df['close'].ewm(span=fast_period, adjust=False).mean()
    signals[f'ema_slow_{slow_period}'] = df['close'].ewm(span=slow_period, adjust=False).mean()

    # Signal: 1.0 if fast EMA above slow EMA, else 0.0
    signals['signal'] = 0.0
    signals.loc[
        signals[f'ema_fast_{fast_period}'] > signals[f'ema_slow_{slow_period}'],
        'signal'
    ] = 1.0
    
    #Position = difference in signal (1 means buy; -1 means sell)
    signals['position'] = signals['signal'].diff().fillna(0.0)
    
    return signals

if __name__ == "__main__":
    # 1. Fetch full history for AAPL
    df_dict = load_data(["AAPL"], output_size="full")
    aapl_df = df_dict["AAPL"]

    # 2. Compute 20/50 EMA signals
    signals = generate_ema_signals(aapl_df, fast_period=20, slow_period=50)

    # 3. Show last few rows
    print(signals.tail(10))
import pandas as pd

def genericStrength(df):
    # Ensure the DataFrame is sorted by date
    df = df.sort_index()

    # Initialize the strength column with empty strings
    df['Strength'] = ''

    for i in range(2, len(df)):
        curr_candle = df.iloc[i]
        prev_candle_1 = df.iloc[i - 1]
        prev_candle_2 = df.iloc[i - 2]

        # Get the highest high and the lowest low of the previous two candles
        highest_high = max(prev_candle_1['high'], prev_candle_2['high'])
        lowest_low = min(prev_candle_1['low'], prev_candle_2['low'])
        range_high_low = highest_high - lowest_low

        if curr_candle['close'] > curr_candle['open']:  # Bullish candle
            if curr_candle['high'] > highest_high:
                df.at[df.index[i], 'Strength'] = 'SG'
            elif lowest_low + 0.5 * range_high_low <= curr_candle['high'] < lowest_low + 0.98 * range_high_low:
                df.at[df.index[i], 'Strength'] = 'MG'
            elif curr_candle['high'] < lowest_low + 0.5 * range_high_low:
                df.at[df.index[i], 'Strength'] = 'WG'
        else:  # Bearish candle
            if curr_candle['low'] < lowest_low:
                df.at[df.index[i], 'Strength'] = 'SR'
            elif highest_high - 0.98 * range_high_low < curr_candle['low'] <= highest_high - 0.5 * range_high_low:
                df.at[df.index[i], 'Strength'] = 'MR'
            elif curr_candle['low'] > highest_high - 0.5 * range_high_low:
                df.at[df.index[i], 'Strength'] = 'WR'

    return df



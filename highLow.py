import pandas as pd

# Read the CSV file
df = pd.read_csv('NIFTY_2020.csv')

# Parse the time
df['DateTime'] = pd.to_datetime(df['Date'].astype(str) + ' ' + df['Time'], format='%Y%m%d %H:%M')

# Set DateTime column as index
df.set_index('DateTime', inplace=True)

# Group in 120-minute chunks starting from 9:15
t = df.groupby(pd.Grouper(freq='1min', origin='9:15', closed='left')).agg({
    "Open": "first",
    "Close": "last",
    "Low": "min",
    "High": "max"
})
t.columns = ["open", "close", "low", "high"]

# Filter to keep only intervals between 9:15 and 15:15
t = t.between_time('9:15', '15:15')

# Drop rows where any 'open' value is NaN
t = t.dropna(subset=['open'])

def highLowComparison(ohlc_df, minute_df):
    """
    Compare the high of the current OHLC data with the previous day's high from the minute data.

    Parameters:
    ohlc_df (pd.DataFrame): DataFrame containing the OHLC data to be compared.
    minute_df (pd.DataFrame): DataFrame containing the one-minute data to create daily data for comparison.

    Returns:
    pd.DataFrame: DataFrame with the 'open', 'high', 'low', 'close', 'Previous_High', 'Percentage_Increase', and 'Signal' columns.
    """

    # Ensure the DataFrames have datetime indices
    ohlc_df.index = pd.to_datetime(ohlc_df.index)
    minute_df.index = pd.to_datetime(minute_df.index)

    # Filter minute data to the trading hours and resample to daily
    daily_df = minute_df.between_time('9:15', '15:30').resample('D').agg({
        'Open': 'first',
        'High': 'max',
        'Low': 'min',
        'Close': 'last'
    })

    # Calculate previous day's high
    daily_df['Previous_High'] = daily_df['High'].shift(1)
    
    # Merge the previous day's high with the OHLC data
    ohlc_df['Date'] = ohlc_df.index.date
    ohlc_df['Time'] = ohlc_df.index.time
    daily_df['Date'] = daily_df.index.date
    ohlc_df = pd.merge(ohlc_df, daily_df[['Date', 'Previous_High']], on='Date', how='left')
    ohlc_df.set_index(['Date', 'Time'], inplace=True)

    # Initialize the signal and percentage increase columns
    ohlc_df['Signal'] = 'No Signal'
    ohlc_df['Percentage_Increase'] = 0.0

    # Set signals based on whether the current high breaks the previous day's high or low
    for i in range(len(ohlc_df)):
        curr_candle = ohlc_df.iloc[i]

        # Check if the current high breaks the previous day's high
        if curr_candle['high'] > curr_candle['Previous_High']:
            ohlc_df.at[ohlc_df.index[i], 'Signal'] = 'Buy'
            # Calculate percentage increase
            ohlc_df.at[ohlc_df.index[i], 'Percentage_Increase'] = (curr_candle['high'] - curr_candle['Previous_High']) / curr_candle['Previous_High'] * 100
        # Check if the current low breaks the previous day's low
        elif curr_candle['low'] < curr_candle['Previous_High']:  # assuming using Previous_High for comparison, change as needed
            ohlc_df.at[ohlc_df.index[i], 'Signal'] = 'Sell'
            # Calculate percentage decrease
            ohlc_df.at[ohlc_df.index[i], 'Percentage_Increase'] = (curr_candle['low'] - curr_candle['Previous_High']) / curr_candle['Previous_High'] * 100

    return ohlc_df[['open', 'high', 'low', 'close', 'Previous_High', 'Percentage_Increase', 'Signal']]

# Adjust pandas display options to show all rows and columns
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

# Example usage
result_df = highLowComparison(t, df)
print(result_df.head(1080))

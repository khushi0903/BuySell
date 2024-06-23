from tweezer01 import highCheck
import pandas as pd

# Load the OHLC data from the CSV file, ensuring Date and Time are loaded as strings
df = pd.read_csv('NIFTY_2012.csv', dtype={'Date': str, 'Time': str})

# Combine the 'Date' and 'Time' columns into a single datetime column
df['datetime'] = pd.to_datetime(df['Date'] + ' ' + df['Time'])

# Set 'datetime' as the index
df.set_index('datetime', inplace=True)

# Filter data for the last 3 months
last_3_months = df.index >= df.index.max() - pd.DateOffset(months=3)
filtered_df = df[last_3_months]

# Define trading hours
start_time = pd.to_datetime('9:15').time()
end_time = pd.to_datetime('15:30').time()

# Resample to 1 day timeframe starting at 9:15 and ending at 15:30
daily_df = filtered_df.between_time(start_time, end_time).resample('D').agg({
    'Open': 'first',
    'High': 'max',
    'Low': 'min',
    'Close': 'last'
})

# Calculate percentage increase from previous day's high
daily_df['Previous_High'] = daily_df['High'].shift(1)
daily_df['High_Broken'] = daily_df['High'] > daily_df['Previous_High']
daily_df['Percentage_Increase'] = (daily_df['High'] - daily_df['Previous_High']) / daily_df['Previous_High'] * 100

# Initialize new columns with null values
daily_df['60T'] = None
daily_df['30T'] = None
daily_df['15T'] = None
daily_df['5T'] = None
daily_df['candlesticPattern'] = None
daily_df['chartPattern'] = None
daily_df['strength'] = None

# Create result_df with required columns
result_df = daily_df.loc[:, ['High', 'Previous_High', 'Percentage_Increase', 'High_Broken', '60T', '30T', '15T', '5T', 'candlesticPattern', 'chartPattern', 'strength']]

# Filter and print the relevant columns for daily data
result_df_filtered = result_df[result_df['High_Broken']].loc[:, [
    'High', 'Previous_High', 'Percentage_Increase',
    '60T', '30T', '15T', '5T', 'candlesticPattern', 'chartPattern', 'strength'
]]

# Print the result for daily data in expanded form
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.expand_frame_repr', False)
print("Daily Resampled Data:")
print(result_df_filtered.to_string())





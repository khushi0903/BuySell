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

# Resample to 5-minute intervals within trading hours
fiveMin_df = filtered_df.between_time(start_time, end_time).resample('5min').agg({
    'Open': 'first',
    'High': 'max',
    'Low': 'min',
    'Close': 'last'
})

# Calculate percentage increase from previous 5-minute interval's high
fiveMin_df['Previous_High'] = fiveMin_df['High'].shift(1)
fiveMin_df['High_Broken'] = fiveMin_df['High'] > fiveMin_df['Previous_High']
fiveMin_df['Percentage_Increase'] = (fiveMin_df['High'] - fiveMin_df['Previous_High']) / fiveMin_df['Previous_High'] * 100

# Resample to 15-minute intervals within trading hours
fifteenMin_df = filtered_df.between_time(start_time, end_time).resample('15min').agg({
    'Open': 'first',
    'High': 'max',
    'Low': 'min',
    'Close': 'last'
})

# Calculate percentage increase from previous 15-minute interval's high
fifteenMin_df['Previous_High'] = fifteenMin_df['High'].shift(1)
fifteenMin_df['High_Broken'] = fifteenMin_df['High'] > fifteenMin_df['Previous_High']
fifteenMin_df['Percentage_Increase'] = (fifteenMin_df['High'] - fifteenMin_df['Previous_High']) / fifteenMin_df['Previous_High'] * 100

# Resample to 30-minute intervals within trading hours
thirtyMin_df = filtered_df.between_time(start_time, end_time).resample('30min').agg({
    'Open': 'first',
    'High': 'max',
    'Low': 'min',
    'Close': 'last'
})

# Calculate percentage increase from previous 30-minute interval's high
thirtyMin_df['Previous_High'] = thirtyMin_df['High'].shift(1)
thirtyMin_df['High_Broken'] = thirtyMin_df['High'] > thirtyMin_df['Previous_High']
thirtyMin_df['Percentage_Increase'] = (thirtyMin_df['High'] - thirtyMin_df['Previous_High']) / thirtyMin_df['Previous_High'] * 100

# Resample to 60-minute intervals within trading hours
sixtyMin_df = filtered_df.between_time(start_time, end_time).resample('60min').agg({
    'Open': 'first',
    'High': 'max',
    'Low': 'min',
    'Close': 'last'
})

# Calculate percentage increase from previous 60-minute interval's high
sixtyMin_df['Previous_High'] = sixtyMin_df['High'].shift(1)
sixtyMin_df['High_Broken'] = sixtyMin_df['High'] > sixtyMin_df['Previous_High']
sixtyMin_df['Percentage_Increase'] = (sixtyMin_df['High'] - sixtyMin_df['Previous_High']) / sixtyMin_df['Previous_High'] * 100

# Add empty columns for 60T, 30T, 15T, 5T with NaN values
daily_df['60T'] = None
daily_df['30T'] = None
daily_df['15T'] = None
daily_df['5T'] = None

# Fill the 60T column in result_df
# Create a boolean Series indicating if the high was broken in 60T interval
sixtyMin_breaks = sixtyMin_df['High_Broken'].groupby(sixtyMin_df.index.date).any()

# Map 'Yes' or 'No' based on whether the date is in sixtyMin_breaks index
result_df = daily_df.loc[:, ['High', 'Previous_High', 'Percentage_Increase', '60T', '30T', '15T', '5T']]
result_df['High_Broken'] = daily_df['High_Broken']  # Include High_Broken column in result_df

# Convert daily_df index dates to a pandas Series for mapping
daily_dates = pd.Series(daily_df.index.date)

# Map 'Yes' or 'No' based on whether the date is in sixtyMin_breaks index
result_df['60T'] = daily_dates.map(lambda x: 'Yes' if x in sixtyMin_breaks.index and sixtyMin_breaks.loc[x] else 'No')

# Resample to 30-minute intervals within trading hours
thirtyMin_breaks = thirtyMin_df['High_Broken'].groupby(thirtyMin_df.index.date).any()

# Map 'Yes' or 'No' based on whether the date is in thirtyMin_breaks index
result_df['30T'] = daily_dates.map(lambda x: 'Yes' if x in thirtyMin_breaks.index and thirtyMin_breaks.loc[x] else 'No')

# Resample to 15-minute intervals within trading hours
fifteenMin_breaks = fifteenMin_df['High_Broken'].groupby(fifteenMin_df.index.date).any()

# Map 'Yes' or 'No' based on whether the date is in fifteenMin_breaks index
result_df['15T'] = daily_dates.map(lambda x: 'Yes' if x in fifteenMin_breaks.index and fifteenMin_breaks.loc[x] else 'No')

# Resample to 5-minute intervals within trading hours
fiveMin_breaks = fiveMin_df['High_Broken'].groupby(fiveMin_df.index.date).any()

# Map 'Yes' or 'No' based on whether the date is in fiveMin_breaks index
result_df['5T'] = daily_dates.map(lambda x: 'Yes' if x in fiveMin_breaks.index and fiveMin_breaks.loc[x] else 'No')

# Add new columns and fill them with null values
result_df['candlesticPattern'] = None
result_df['chartPattern'] = None
result_df['strength'] = None

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

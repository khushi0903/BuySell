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

# Calculate previous day's high
daily_df['Previous_High'] = daily_df['High'].shift(1)

# Filter the 5-minute intervals within trading hours
fiveMin_df = filtered_df.between_time(start_time, end_time).resample('5min').agg({
    'Open': 'first',
    'High': 'max',
    'Low': 'min',
    'Close': 'last'
})

# Map the previous day's high to each 5-minute interval
fiveMin_df['Previous_High'] = pd.Series(fiveMin_df.index.date).map(
    lambda x: daily_df.loc[daily_df.index.date == (pd.to_datetime(x) - pd.Timedelta(days=1)).date(), 'High'].values[0]
    if (pd.to_datetime(x) - pd.Timedelta(days=1)).date() in daily_df.index.date
    else None
).values

# Calculate whether the current 5-minute high is greater than the previous day's high
fiveMin_df['High_Broken'] = fiveMin_df['High'] > fiveMin_df['Previous_High']

# Calculate the percentage increase
fiveMin_df['Percentage_Increase'] = (fiveMin_df['High'] - fiveMin_df['Previous_High']) / fiveMin_df['Previous_High'] * 100

# Add empty columns for daily resampled data
daily_df['60T'] = None
daily_df['30T'] = None
daily_df['15T'] = None
daily_df['5T'] = None

# Create a boolean Series indicating if the high was broken in 5-minute intervals
fiveMin_breaks = fiveMin_df['High_Broken'].groupby(fiveMin_df.index.date).any()

# Map 'Yes' or 'No' based on whether the date is in fiveMin_breaks index
result_df = daily_df.loc[:, ['High', 'Previous_High', 'Percentage_Increase', '60T', '30T', '15T', '5T']]
result_df['High_Broken'] = daily_df['High_Broken']  # Include High_Broken column in result_df

# Convert daily_df index dates to a pandas Series for mapping
daily_dates = pd.Series(daily_df.index.date)

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

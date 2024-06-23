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

# Filter and print the relevant columns
result_df = daily_df[daily_df['High_Broken']].loc[:, ['High', 'Previous_High', 'Percentage_Increase']]

# Print the result
print("Daily Resampled Data:")
print(result_df)
print()

# Resample to 5-minute intervals within trading hours
fiveMin_df = filtered_df.between_time(start_time, end_time).resample('5T').agg({
    'Open': 'first',
    'High': 'max',
    'Low': 'min',
    'Close': 'last'
})

# Calculate percentage increase from previous 5-minute interval's high
fiveMin_df['Previous_High'] = fiveMin_df['High'].shift(1)
fiveMin_df['High_Broken'] = fiveMin_df['High'] > fiveMin_df['Previous_High']
fiveMin_df['Percentage_Increase'] = (fiveMin_df['High'] - fiveMin_df['Previous_High']) / fiveMin_df['Previous_High'] * 100

# Filter and print the relevant columns
T5_df = fiveMin_df[fiveMin_df['High_Broken']].loc[:, ['High', 'Previous_High', 'Percentage_Increase']]

# Print the result
print("5-Minute Resampled Data:")
print(T5_df)
print()

# Resample to 15-minute intervals within trading hours
fifteenMin_df = filtered_df.between_time(start_time, end_time).resample('15T').agg({
    'Open': 'first',
    'High': 'max',
    'Low': 'min',
    'Close': 'last'
})

# Calculate percentage increase from previous 15-minute interval's high
fifteenMin_df['Previous_High'] = fifteenMin_df['High'].shift(1)
fifteenMin_df['High_Broken'] = fifteenMin_df['High'] > fifteenMin_df['Previous_High']
fifteenMin_df['Percentage_Increase'] = (fifteenMin_df['High'] - fifteenMin_df['Previous_High']) / fifteenMin_df['Previous_High'] * 100

# Filter and print the relevant columns
T15_df = fifteenMin_df[fifteenMin_df['High_Broken']].loc[:, ['High', 'Previous_High', 'Percentage_Increase']]

# Print the result
print("15-Minute Resampled Data:")
print(T15_df)
print()

# Resample to 30-minute intervals within trading hours
thirtyMin_df = filtered_df.between_time(start_time, end_time).resample('30T').agg({
    'Open': 'first',
    'High': 'max',
    'Low': 'min',
    'Close': 'last'
})

# Calculate percentage increase from previous 30-minute interval's high
thirtyMin_df['Previous_High'] = thirtyMin_df['High'].shift(1)
thirtyMin_df['High_Broken'] = thirtyMin_df['High'] > thirtyMin_df['Previous_High']
thirtyMin_df['Percentage_Increase'] = (thirtyMin_df['High'] - thirtyMin_df['Previous_High']) / thirtyMin_df['Previous_High'] * 100

# Filter and print the relevant columns
T30_df = thirtyMin_df[thirtyMin_df['High_Broken']].loc[:, ['High', 'Previous_High', 'Percentage_Increase']]

# Print the result
print("30-Minute Resampled Data:")
print(T30_df)
print()

# Resample to 60-minute intervals within trading hours
sixtyMin_df = filtered_df.between_time(start_time, end_time).resample('60T').agg({
    'Open': 'first',
    'High': 'max',
    'Low': 'min',
    'Close': 'last'
})

# Calculate percentage increase from previous 60-minute interval's high
sixtyMin_df['Previous_High'] = sixtyMin_df['High'].shift(1)
sixtyMin_df['High_Broken'] = sixtyMin_df['High'] > sixtyMin_df['Previous_High']
sixtyMin_df['Percentage_Increase'] = (sixtyMin_df['High'] - sixtyMin_df['Previous_High']) / sixtyMin_df['Previous_High'] * 100

# Filter and print the relevant columns
T60_df = sixtyMin_df[sixtyMin_df['High_Broken']].loc[:, ['High', 'Previous_High', 'Percentage_Increase']]

# Print the result
print("60-Minute Resampled Data:")
print(T60_df)
print()

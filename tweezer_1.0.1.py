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

# Function to calculate breakouts for each interval DataFrame
def calculate_breakouts(interval, interval_df, result_df):
    # Resample to the specified interval within trading hours
    resampled_df = interval_df.between_time(start_time, end_time).resample(interval).agg({
        'Open': 'first',
        'High': 'max',
        'Low': 'min',
        'Close': 'last'
    })
    
    # Debugging: Print the resampled data
    print(f"{interval} Resampled Data:")
    print(resampled_df)
    print()
    
    if resampled_df.empty:
        print(f"No data found for {interval} interval within trading hours.")
        print()
        return
    
    # Merge with result_df to get the correct Previous_High for each day
    merged_df = resampled_df.merge(result_df[['Previous_High']], left_index=True, right_index=True, how='left')
    
    # Calculate percentage increase from previous interval's high
    merged_df['Previous_High_daily'] = merged_df['Previous_High'].shift(1)
    merged_df['High_Broken'] = merged_df['High'] > merged_df['Previous_High_daily']
    merged_df['Percentage_Increase'] = (merged_df['High'] - merged_df['Previous_High_daily']) / merged_df['Previous_High_daily'] * 100
    
    # Filter for breakout occurrences
    result = merged_df[merged_df['High_Broken']].loc[:, ['High', 'Previous_High_daily', 'Percentage_Increase']]
    
    # Print the result
    print(f"Breakouts for {interval}:")
    print(result)
    print()

# Resample to 5-minute intervals within trading hours
fiveMin_df = filtered_df.between_time(start_time, end_time).resample('5T').agg({
    'Open': 'first',
    'High': 'max',
    'Low': 'min',
    'Close': 'last'
})

calculate_breakouts('5T', fiveMin_df, result_df)

# Resample to 15-minute intervals within trading hours
fifteenMin_df = filtered_df.between_time(start_time, end_time).resample('15T').agg({
    'Open': 'first',
    'High': 'max',
    'Low': 'min',
    'Close': 'last'
})

calculate_breakouts('15T', fifteenMin_df, result_df)

# Resample to 30-minute intervals within trading hours
thirtyMin_df = filtered_df.between_time(start_time, end_time).resample('30T').agg({
    'Open': 'first',
    'High': 'max',
    'Low': 'min',
    'Close': 'last'
})

calculate_breakouts('30T', thirtyMin_df, result_df)

# Resample to 60-minute intervals within trading hours
sixtyMin_df = filtered_df.between_time(start_time, end_time).resample('60T').agg({
    'Open': 'first',
    'High': 'max',
    'Low': 'min',
    'Close': 'last'
})

calculate_breakouts('60T', sixtyMin_df, result_df)

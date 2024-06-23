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

# Resample to 1 day timeframe starting at 9:15 and ending at 15:30
daily_df = filtered_df.between_time('9:15', '15:30').resample('D').agg({
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
#print(result_df)
#rint()

# Ensure the DataFrame has a datetime index
result_df.index = pd.to_datetime(result_df.index)

# Access the 'Previous_High' column
previous_high_column = result_df['Previous_High']
specific_date = '2012-10-04'
previous_high_value = result_df.loc[specific_date, 'Previous_High']
print(f"Previous High on {specific_date}: {previous_high_value}")


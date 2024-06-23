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

# Calculate whether the current day's high is greater than the previous day's high
daily_df['High_Broken'] = daily_df['High'] > daily_df['Previous_High']

# Calculate percentage increase from previous day's high
daily_df['Percentage_Increase'] = (daily_df['High'] - daily_df['Previous_High']) / daily_df['Previous_High'] * 100

# Filter minute-by-minute intervals within trading hours
minute_df = filtered_df.between_time(start_time, end_time)

# Ensure the result_df has a datetime index
result_df = daily_df.copy()
result_df.index = pd.to_datetime(result_df.index)

# Access the 'Previous_High' column
previous_high_column = result_df['Previous_High']

# Define the specific date
def highCheck(date):
    specific_date = date #'2012-10-04'
    previous_high_value = result_df.loc[specific_date, 'Previous_High']
    print(f"Previous High on {specific_date}: {previous_high_value}")

    # Filter minute_df for the specific date
    specific_date_df = minute_df.loc[specific_date]

    # Check for each minute whether the previous_high_value is crossed
    times_previous_high_broken = specific_date_df[specific_date_df['High'] > previous_high_value]

    # Calculate the percentage increase when the previous high is broken
    times_previous_high_broken['Percentage_Increase'] = (times_previous_high_broken['High'] - previous_high_value) / previous_high_value * 100

    # Display the times and percentage increase when the previous_high_value was broken
    print(f"Times and percentage increase when the previous high value was broken on {specific_date}:")
    previous_percentage_increase = 0
    for index, row in times_previous_high_broken.iterrows():
        time = index.time()
        percentage_increase = row['Percentage_Increase']
        action = "enter" if percentage_increase > previous_percentage_increase else "do not enter"
        print(f"Time: {time}, Percentage Increase: {percentage_increase:.2f}%, Action: {action}")
        previous_percentage_increase = percentage_increase
    return percentage_increase
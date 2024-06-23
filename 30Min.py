import pandas as pd

# Load the CSV file into a DataFrame
df = pd.read_csv('NIFTY_2012.csv')

# parse the time. 
df['DateTime'] = pd.to_datetime(df['Date'].astype(str) + ' ' + df['Time'], format='%Y%m%d %H:%M')

# Set DateTime column as index
df.set_index('DateTime', inplace=True)

# group in 5-minute chunks. 
t = df.groupby(pd.Grouper(freq='30T', origin='9:15', closed='left')).agg({"Open": "first", 
                                             "Close": "last", 
                                             "Low": "min", 
                                             "High": "max"})
t.columns = ["open", "close", "low", "high"]
print(t)
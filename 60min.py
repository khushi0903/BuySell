import pandas as pd
from candlestick.candlestick_patterns import *
from genericStrength import genericStrength

# Load the CSV file into a DataFrame
df = pd.read_csv('NIFTY_2020.csv')

# Parse the time
df['DateTime'] = pd.to_datetime(df['Date'].astype(str) + ' ' + df['Time'], format='%Y%m%d %H:%M')

# Set DateTime column as index
df.set_index('DateTime', inplace=True)

# Group in 60-minute chunks starting from 9:15
t = df.groupby(pd.Grouper(freq='120T', origin='9:15', closed='left')).agg({"Open": "first", 
                                             "Close": "last", 
                                             "Low": "min", 
                                             "High": "max"})
t.columns = ["open", "close", "low", "high"]

# Filter to keep only intervals between 9:15 and 15:15
t = t.between_time('9:15', '15:15')

# Drop rows where any 'open' value is NaN
t = t.dropna(subset=['open'])

#print(genericStrength(t).head(50))

# Detect each candlestick pattern and assign results to columns in `t`
t['BearishEngulfing'] = bearish_engulfing(t)["BearishEngulfing"]
t['BullishEngulfing'] = bullish_engulfing(t)["BullishEngulfing"]
t['Hammer'] = hammer(t)["Hammer"]
t['InvertedHammer'] = inverted_hammer(t)["InvertedHammer"]
t['MorningStar'] = morning_star(t)["MorningStar"]
t['EveningStar'] = evening_star(t)["EveningStar"]
t['ShootingStar'] = shooting_star(t)["ShootingStar"]
t['HangingMan'] = hanging_man(t)["HangingMan"]
t['BullishHarami'] = bullish_harami(t)["BullishHarami"]
t['BearishHarami'] = bearish_harami(t)["BearishHarami"]
t['DarkCloudCover'] = dark_cloud_cover(t)["DarkCloudCover"]
t['Doji'] = doji(t)["Doji"]
t['DojiStar'] = doji_star(t)["DojiStar"]
t['DragonflyDoji'] = dragonfly_doji(t)["DragonflyDoji"]
t['GravestoneDoji'] = gravestone_doji(t)["GravestoneDoji"]
t['MorningStarDoji'] = morning_star_doji(t)["MorningStarDoji"]
t['PiercingPattern'] = piercing_pattern(t)["PiercingPattern"]
t['RainDrop'] = rain_drop(t)["RainDrop"]
t['RainDropDoji'] = rain_drop_doji(t)["RainDropDoji"]
t['Star'] = star(t)["Star"]
t['EveningStarDoji'] = evening_star_doji(t)["EveningStarDoji"]
t['TweezerTop'] = tweezer_top(t)["TweezerTop"]
t['TweezerBottom'] = tweezer_bottom(t)["TweezerBottom"]

# Extract datetime indices where TweezerTop and TweezerBottom are True, handling NaN values
tweezer_top_dates = t.index[t['TweezerTop'].fillna(False)].tolist()
tweezer_bottom_dates = t.index[t['TweezerBottom'].fillna(False)].tolist()

# Create a DataFrame to display tweezer top and bottom dates
tweezer_dates_df = pd.DataFrame({
    'DateTime': tweezer_top_dates + tweezer_bottom_dates,
    'Pattern': ['Tweezer Top'] * len(tweezer_top_dates) + ['Tweezer Bottom'] * len(tweezer_bottom_dates)
})

# Sort DataFrame by DateTime for better readability
tweezer_dates_df.sort_values(by='DateTime', inplace=True)

# Print the DataFrame with detected patterns
#print("Detected Tweezer Tops and Bottoms:")
#print(tweezer_dates_df)

# Function to return the DataFrame `t`
def oneHrdf():
    return t

import sys
import pandas as pd


# read the data into data frame

df = pd.read_csv(r'C:\Users\saiam\OneDrive\Desktop\rejected_2007_to_2018q4.csv',sep=',',header=0,encoding='utf-8')
df.head(2)

# filter for 2007

df['year'] = pd.DatetimeIndex(df['Application Date']).year
df = df[df['year'] == 2007]

# calculate simple moving average for 50 days

print(df.columns)
df['MA50'] = df['Risk_Score'].rolling(50,min_periods=1).mean().round(2)
df.head(5)


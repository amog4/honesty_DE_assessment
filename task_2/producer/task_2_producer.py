from kafka import KafkaProducer
import json
import time
import pandas as pd
import sys


args = sys.argv[1]
# producer 
print(args)

producer = KafkaProducer(bootstrap_servers=['localhost:29092'],
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))


df = pd.read_csv(f'{args}',sep=',',header=0,encoding='utf-8')
df.head(2)

# filter for 2008 and 2009

df['year'] = pd.DatetimeIndex(df['Application Date']).year

df.head()
df = df[(df['year'] == 2008)]

for index in range(df.shape[0]):
    record  = df.iloc[index,:].to_json()
    producer.send('test2', value=record)


import sys
import pandas as pd
import argparse
import math
from sqlalchemy import create_engine


def main(args):
    host = args.host
    username = args.username
    password = args.password
    database = args.database
    table = args.table
    file1 = args.file1

    engine = create_engine(f"postgresql://{host}:5432/{database}?user={username}&password={password}")
    conn  = engine.connect()
    # read the data into data frame

    df = pd.read_csv(f'{file1}',sep=',',header=0,encoding='utf-8')
    df.head(2)

    # filter for 2007

    df['year'] = pd.DatetimeIndex(df['Application Date']).year
    df = df[df['year'] == 2007]

    # calculate simple moving average for 50 days

    df['MA50'] = df['Risk_Score'].rolling(50,min_periods=1).mean().round(2)

    df.head(0).to_sql(name = table,con = conn,if_exists='replace')

    start = 0
    end = 10000
    print(df.shape)
    r = math.ceil(df.shape[0]/10000)
    for index in range(1,r+1):        
        df.iloc[start:end*r,:].to_sql(name = table,con = conn,if_exists='append')
        start += end
    print(f'done {df.shape[0]}')
       


if __name__ == '__main__':


    parser = argparse.ArgumentParser()

    parser.add_argument("--host",type=str, required=True, help="DB host")
    parser.add_argument("--username",type=str, required=True, help="DB username")
    parser.add_argument("--password",type=str, required=True, help="DB password")
    parser.add_argument("--database",type=str, required=True, help="DB host")
    parser.add_argument("--table",type=str, required=True, help="DB host")
    parser.add_argument("--file1",type=str, required=True, help="file1")
    args = parser.parse_args()

    print(args)
    main(args)




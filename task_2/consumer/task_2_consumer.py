from kafka import KafkaConsumer
import json
import time
import pandas as pd
import argparse
from sqlalchemy import create_engine



def main(args):
    host = args.host
    username = args.username
    password = args.password
    database = args.database
    table = args.table

    engine = create_engine(f"postgresql://{host}:5432/{database}?user={username}&password={password}")
    conn  = engine.connect()

    
# consumer

    consumer = KafkaConsumer(
        'test2',
        bootstrap_servers=['localhost:29092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    col = True
    for message in consumer:
        message = message.value
        if col == True:
            df = pd.DataFrame([json.loads(message)],index=[0])
            df.head(0).to_sql(name = 'data',con = conn,if_exists='replace')
            col = False
        else:
            df = pd.DataFrame([json.loads(message)],index=[0])
            df.to_sql(name = 'data',con = conn,if_exists='append')
    
       


if __name__ == '__main__':


    parser = argparse.ArgumentParser()

    parser.add_argument("--host",type=str, required=True, help="DB host")
    parser.add_argument("--username",type=str, required=True, help="DB username")
    parser.add_argument("--password",type=str, required=True, help="DB password")
    parser.add_argument("--database",type=str, required=True, help="DB host")
    parser.add_argument("--table",type=str, required=True, help="DB host")
    args = parser.parse_args()

    print(args)
    main(args)








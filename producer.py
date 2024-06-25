from kafka import KafkaProducer
from kafka.errors import KafkaError

import json as js
import pandas as pd
import numpy as np

import datetime as dt
import time

def getVehicles():
    df = pd.read_csv('vehicle_dataset.csv')# read csv file
    df.drop(columns=['Unnamed: 0'], inplace=True)
    return df

def dataFormat(data): # data must be in built in types
    arr = {}
    for k in data.keys():
        if isinstance(data[k], np.integer):
            arr[k]=int(data[k])
        elif isinstance(data[k], np.floating):
            arr[k]=float(data[k])
        else :
            arr[k]=data[k]
    # u must add a timestamp
    return arr

def main():
    # create a local kafka producer
    producer = KafkaProducer(value_serializer=lambda m: js.dumps(m).encode('ascii'))
    run_time = dt.datetime.today()
    # get vecicles
    df = getVehicles()
    # iterate over vehicles
    for i in range(0 , int(df['t'].max()*10/100) , 5):# start is 0 , end is max time , move every 5 seconds
        currentTime = run_time + dt.timedelta(seconds=i)
        print(str(currentTime))
        # 1. find what to send
        to_send = df[(df['t'] <= i + 5) & (df['t'] > i) & (df['v']>0)]
        # 2. each car is sent in a seperate message
        for j in range(len(to_send)) :
            try : # AttributeError when no car
                data = dataFormat(to_send.iloc[j])
                data['time'] = currentTime.strftime('%Y-%m-%d %H:%M:%S')
                future = producer.send('vehicle_positions' , data)
            except AttributeError :
                print("No Cars")
        time.sleep(2)

if __name__=='__main__':
    main()

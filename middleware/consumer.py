from kafka import KafkaConsumer

import json as js

def main():

    #try :
    consumer = KafkaConsumer('vehicle_positions')
    batches = {}
    for i in consumer :# for every message in q
        json_data = js.loads(i.value.decode('utf-8'))
        # consumer must make batches
        # 1 Extract time
        current_time = json_data['time']
        print(str(json_data))
'''# 2 Time does not exist
        if not (str(current_time) in batches.keys()):
            batches[current_time] = {'time' : current_time , 'link' : json_data['link'] , 'vcount' : 0 , 'vspeed' : 0}
        # 3 Add data in the correct batch
        batches[current_time]['vcount'] += 1
        batches[current_time]['vspeed'] = (batches[current_time]['vspeed'] + json_data['v'])/2
        for i in batches :
            print(str(batches[i]))
'''
if __name__=='__main__':
    main()

# -*- coding: utf-8 -*-
__author__ = 'gzp'

import random
import time
import json
import paho.mqtt.client as mqtt

if __name__ == '__main__':
    publisher = mqtt.Client(client_id='A')
    publisher.username_pw_set('admin', 'admin')
    publisher.connect('localhost', 1883)
    # for word in cycle(['35', '36', '35', '37']):

    while True:
        weather_data = {
            'light': random.randrange(0, 100),
            'humidity': random.randrange(0, 100),
            'temp_celcius': random.randrange(0, 100),
            'heat_index': random.randrange(0, 100),
            'capture_dttm': random.randrange(0, 100),
            'pressure_pa': random.randrange(0, 100),
            'baro_temp_celcius': random.randrange(0, 100),
            'mq135': random.randrange(0, 100),
            'mq5': random.randrange(0, 100),
            'mq6': random.randrange(0, 100),
            'mq9': random.randrange(0, 100),
            'device': 'test'
        }
        publisher.publish(topic='/weather', payload=json.dumps(weather_data), qos=0, retain=False)
        time.sleep(1)

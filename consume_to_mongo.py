# -*- coding: utf-8 -*-
__author__ = 'gzp'

import kafka_consume as kafka_cons
from kafka.errors import KafkaError, NoBrokersAvailable
import psycopg2
import os
import time
import json

from pymongo import MongoClient
from mongoengine import connect, Document, fields

"""

use mqtt_to_kafka;
db.createUser({user:'mqtt_to_kafka',pwd:'mqtt_to_kafka',roles: [{role:'readWrite',db:'mqtt_to_kafka'}]})})
"""
connect('mqtt_to_kafka', host='localhost', port=27017, username='mqtt_to_kafka', password='mqtt_to_kafka')


class Weather(Document):
    light = fields.IntField(required=True)
    humidity = fields.IntField(required=True)
    temp_celcius = fields.IntField(required=True)
    heat_index = fields.FloatField(required=True)
    capture_dttm = fields.IntField(required=True)
    device = fields.StringField(required=True, max_length=200)
    pressure_pa = fields.IntField(required=True)
    baro_temp_celcius = fields.IntField(required=True)
    mq135 = fields.IntField(required=True)
    mq5 = fields.IntField(required=True)
    mq6 = fields.IntField(required=True)
    mq9 = fields.IntField(required=True)


def write_to_db(message):
    try:
        weather = Weather(**json.loads(message))
        weather.save()
    except Exception as e:
        print(e)


if __name__ == '__main__':
    attempts = 0
    max_attempts = os.environ.get('MAX_CONNECTION_RETRIES', 10)

    while attempts < int(max_attempts):
        try:

            ## KAFKA
            consumer_group = "weather_consumer_pg"
            consumer_device = "weather_consumer_pg_test"
            kafka_topic = "weather"

            consumer = kafka_cons.start_consumer(consumer_group,
                                                 consumer_device,
                                                 kafka_topic)
            print('Start consuming')
            for message in consumer:
                print(message.value.decode('utf-8'))
                write_to_db(message.value.decode('utf-8'))


        except NoBrokersAvailable:
            print("No Brokers. Attempt %s" % attempts)
            attempts = attempts + 1
            time.sleep(2)

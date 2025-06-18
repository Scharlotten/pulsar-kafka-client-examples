#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
from io import BytesIO
from fastavro import schemaless_reader, parse_schema
from confluent_kafka import Consumer
import os
#from dotenv import load_dotenv

#load_dotenv()
group_id = os.getenv("GROUP_ID")
tenant = os.getenv("TENANT")
password = os.getenv("TOKEN")
kafka_bootstrap_servers = os.getenv("BOOTSTRAP_SERVER")
topic = os.getenv("TOPIC")


consumer_conf = {'group.id': group_id,
                'auto.offset.reset': "earliest",
                "sasl.username": tenant,
                "sasl.password": password,
                "bootstrap.servers":  kafka_bootstrap_servers,
                "security.protocol": "SASL_SSL",
                "sasl.mechanism": "PLAIN"
                }

consumer = Consumer(consumer_conf)
consumer.subscribe([topic])

while True:
    try:
        msg = consumer.poll(1.0)
        if msg is None:
            continue


        value = msg.value().decode("utf-8")

        #print("decoded key: %s", key)
        print("decoded value: %s", value)

    except KeyboardInterrupt:
        break

consumer.close()



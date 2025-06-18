import os
from random import randint
from time import sleep
import ccloud_lib
from confluent_kafka import Producer, KafkaError
import json
#from dotenv import load_dotenv

#load_dotenv()

def acked(err, msg):
    global delivered_records
    """Delivery report handler called on
    successful or failed delivery of message
    """
    if err is not None:
        print("Failed to deliver message: {}".format(err))
    else:
        delivered_records += 1
        print("Produced record to topic {} partition [{}] @ offset {}"
              .format(msg.topic(), msg.partition(), msg.offset()))



tenant = os.getenv("TENANT")
password = os.getenv("TOKEN")
kafka_bootstrap_servers = os.getenv("BOOTSTRAP_SERVER")
topic = os.getenv("TOPIC")

if __name__ == "__main__":
  

    topic = "persistent://asemjen-tenant-1/kafka/topic1"
    #conf = ccloud_lib.read_ccloud_config(config_file)

    conf = {"bootstrap.servers": kafka_bootstrap_servers,
            "security.protocol":"SASL_SSL",
            "sasl.mechanisms" : "PLAIN",
            "sasl.username": "tenant",
            "sasl.password" : password}
    
    # Create Producer instance
    producer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)
    producer = Producer(producer_conf)

    # Create topic if needed
    #ccloud_lib.create_topic(conf, topic)


    #sleep(20)
    for i in range(0,100):
        record_key = i
        record_value = f"Hello {i}"

        print("Producing record: {}\t{}".format(record_key, record_value))
        producer.produce(topic, key=str(record_key), value=json.dumps(record_value), on_delivery=acked)
        sleep(2)
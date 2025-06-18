import pulsar,time
from pulsar import InitialPosition, ConsumerType
import os
#from dotenv import load_dotenv

#load_dotenv()

group_id = os.getenv("GROUP_ID")
tenant = os.getenv("TENANT")
password = os.getenv("TOKEN")
topic = os.getenv("TOPIC")

service_url = 'pulsar+ssl://' + tenant


client = pulsar.Client(service_url,
                        authentication=pulsar.AuthenticationToken(password))

consumer = client.subscribe(topic, 
                            'test-subscription', 
                            initial_position=InitialPosition.Earliest, 
                            consumer_type=ConsumerType.KeyShared )

waitingForMsg = True
while waitingForMsg:
    try:
        msg = consumer.receive(timeout_millis=2000, )
        print("Received message '{}' id='{}'".format(msg.data(), msg.message_id()))

        # Acknowledging the message to remove from message backlog
        consumer.acknowledge(msg)

        #waitingForMsg = False
    except:
        print("Still waiting for a message...")

    time.sleep(1)

client.close()
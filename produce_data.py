import pulsar
import os
#from dotenv import load_dotenv

#load_dotenv()

group_id = os.getenv("GROUP_ID")
tenant = os.getenv("TENANT")
password = os.getenv("TOKEN")
kafka_bootstrap_servers = os.getenv("BOOTSTRAP_SERVER")
topic = os.getenv("TOPIC")

service_url = 'pulsar+ssl://pulsar-aws-useast2.streaming.datastax.com:6651'


client = pulsar.Client(service_url,
                        authentication=pulsar.AuthenticationToken(password), 
                        # tls_allow_insecure_connection=True
                        )

producer = client.create_producer(topic)

for i in range(10):
    producer.send(('Hello World! %d' % i).encode('utf-8'), partition_key=str(i))
    print(f"send hello world {i}")

client.close()
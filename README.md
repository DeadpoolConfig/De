import json
import time
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

TOPIC_NAME = "weather_data"
BOOTSTRAP_SERVERS = "localhost:9092"

# Create topic if not exists
def create_topic():
    admin_client = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
    try:
        topic = NewTopic(
            name=TOPIC_NAME,
            num_partitions=2,        # ✅ 2 partitions
            replication_factor=1
        )
        admin_client.create_topics([topic])
        print(f"Topic '{TOPIC_NAME}' created.")
    except Exception as e:
        print(f"Topic may already exist: {e}")

def produce_messages():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    with open("../data/sample_weather.json", "r") as f:
        weather_data = json.load(f)

    for reading in weather_data:
        key = str(reading["reading_id"]).encode("utf-8")   # ✅ unique key
        producer.send(TOPIC_NAME, key=key, value=reading)
        print(f"Produced: {reading}")
        time.sleep(1)   # simulate real-time

    producer.flush()

if __name__ == "__main__":
    create_topic()
    produce_messages()
    

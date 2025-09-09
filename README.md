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
    
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from utils.schema import weather_schema

TOPIC_NAME = "weather_data"
BOOTSTRAP_SERVERS = "localhost:9092"

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("WeatherConsumer") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Read stream from Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
        .option("subscribe", TOPIC_NAME) \
        .option("startingOffsets", "earliest") \
        .load()

    # Extract JSON value
    df_parsed = df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), weather_schema).alias("data")) \
        .select("data.*")

    # Deduplicate using reading_id
    df_unique = df_parsed.dropDuplicates(["reading_id"])

    # Write to console (Milestone 1 output)
    query = df_unique.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .option("checkpointLocation", "/tmp/spark-checkpoints/weather") \
        .start()

    query.awaitTermination()

    from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType

weather_schema = StructType([
    StructField("reading_id", IntegerType(), False),
    StructField("station_id", StringType(), True),
    StructField("temperature", StringType(), True),
    StructField("humidity", StringType(), True),
    StructField("wind_speed", StringType(), True),
    StructField("precipitation", StringType(), True),
    StructField("pressure", StringType(), True),
    StructField("reading_timestamp", TimestampType(), True),
    StructField("location", StringType(), True)
])

    

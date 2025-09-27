from kafka import KafkaConsumer
import json
from influx_writer import InfluxWriter
from dotenv import load_dotenv

load_dotenv()

TOPICS = ["stock.prices", "sentiment.scores"]
BOOTSTRAP_SERVERS = ["localhost:9092"]


def handle_sentiment_score(data, influx):
    influx.write_sentiment(data)

def handle_stock_price(data, influx):
    influx.write_price(data)


TOPIC_HANDLERS = {"stock.prices": handle_stock_price, "sentiment.scores": handle_sentiment_score}

def start_consumer():
    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset='latest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        key_deserializer=lambda k: k.decode('utf-8') if k else None
    )

    influx = InfluxWriter()

    for msg in consumer:
        topic = msg.topic
        print(f'Current topic:{topic}')
        data = msg.value
        handler = TOPIC_HANDLERS.get(topic)

        if handler:
            handler(data, influx)
            print(f"Received: {data}")
        else:
            print(f"No handler for topic: {topic}")


if __name__ == "__main__":
    start_consumer()


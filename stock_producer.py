import finnhub
import os
from dotenv import load_dotenv
import time
from datetime import datetime 
from kafka import KafkaProducer
import json

load_dotenv()

fh_key = os.environ.get("KEY", "finhubb key not found")
fh_client = finnhub.Client(api_key=fh_key)


STOCKS = ["AAPL", "MSFT", "TSLA", "GOOGL", "OANDA:XAU_USD"]
TOPIC = "stock.prices"

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8')
)

def fetch_price(symbol):
    data = fh_client.quote(symbol)
    return {
        "symbol": symbol,
        "price": data['c'],  # current price
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "source": "finnhub"
    }

while True:
    for symbol in STOCKS:
        event = fetch_price(symbol)
        producer.send(
            TOPIC,
            key=event['symbol'],
            value=event
        )
        print(f"Sent: {event}")
    time.sleep(15)  # API call limit: 60 calls / minute, 30 calls / second


import finnhub
import os
from dotenv import load_dotenv
import time
from datetime import datetime, timedelta 
from kafka import KafkaProducer
import json
from textblob import TextBlob

load_dotenv()

fh_key = os.environ.get("KEY", "finhubb key not found")
fh_client = finnhub.Client(api_key=fh_key)


STOCKS = ["AAPL", "MSFT", "TSLA", "GOOGL"]
TOPIC = "headline.sentiment"
today_date = datetime.now().date().strftime("%Y-%m-%d")
time_delta_months = 3
earlier_date = (datetime.now().date() - timedelta(time_delta_months * 30)).strftime("%Y-%m-%d")

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8')
)

def fetch_news(symbol):
    news_dump = fh_client.company_news(symbol, _from=earlier_date, to=today_date)
    num_news = len(news_dump)
    news_sent = [{"headline":TextBlob(news["headline"]).sentiment, "summary":TextBlob(news["summary"]).sentiment} for news in news_dump]
    news_sent_avg = {"headline": { "p":0, "s":0}, "summary": {"p":0,"s":0}}
    for news in news_sent:
        news_sent_avg["headline"]["p"] += news["headline"].polarity
        news_sent_avg["headline"]["s"] += news["headline"].subjectivity
        news_sent_avg["summary"]["p"] += news["summary"].polarity
        news_sent_avg["summary"]["s"] += news["summary"].subjectivity
    news_sent_avg["headline"]["p"] = news_sent_avg["headline"]["p"] / num_news
    news_sent_avg["headline"]["s"] = news_sent_avg["headline"]["s"] / num_news
    news_sent_avg["summary"]["p"] = news_sent_avg["summary"]["p"] / num_news
    news_sent_avg["summary"]["s"] = news_sent_avg["summary"]["s"] / num_news
    return {
        "symbol": symbol,
        "news": news_sent_avg,  # current price
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "source": "finnhub"
    }

while True:
    for symbol in STOCKS:
        event = fetch_news(symbol)
        producer.send(
            TOPIC,
            key=event['symbol'],
            value=event
        )
        print(f"Sent: {event}")
    time.sleep(15)  # API call limit: 60 calls / minute, 30 calls / second


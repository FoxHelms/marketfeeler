from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import os
from dotenv import load_dotenv

load_dotenv()

class InfluxWriter:
    def __init__(self, bucket="stock_data"):
        self.client = InfluxDBClient(
            url="http://localhost:8086",
            token="admin-token",
            org="market_data"
        )
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
        self.bucket = bucket

    def write_price(self, data):
        point = (
            Point("stock_price")
            .tag("symbol", data["symbol"])
            .field("price", float(data["price"]))
            .time(data["timestamp"])
        )
        self.write_api.write(bucket=self.bucket, record=point)

    def write_sentiment(self, data):
        point = (
            Point("sentiment_score")
            .tag("symbol", data["symbol"])
            .field("headline_p", float(data["headline_p"]))
            .field("headline_s", float(data["headline_s"]))
            .field("summary_p", float(data["summary_p"]))
            .field("summary_s", float(data["summary_s"]))
            .time(data["timestamp"])
        )
        self.write_api.write(bucket=self.bucket, record=point)

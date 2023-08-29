from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client import InfluxDBClient
from dotenv import load_dotenv
import os

load_dotenv()

INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG")
INFLUXDB_USERNAME = os.getenv("INFLUXDB_USERNAME")
INFLUXDB_PASSWORD = os.getenv("INFLUXDB_PASSWORD")
INFLUXDB_URL = os.getenv("INFLUXDB_URL")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN")


def influx_client():
    return InfluxDBClient(url=INFLUXDB_URL, org=INFLUXDB_ORG, token=INFLUXDB_TOKEN)

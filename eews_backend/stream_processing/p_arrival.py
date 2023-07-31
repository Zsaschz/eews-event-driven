from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StringType, FloatType, TimestampNTZType, StructField, IntegerType

from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client import Point, WritePrecision, InfluxDBClient
#import datetime, timedelta
from obspy.signal.trigger import recursive_sta_lta, trigger_onset
from kafka import KafkaProducer
import json
import redis
import os
from topics import PREPROCESSED_TOPIC, P_ARRIVAL_TOPIC

BOOTSTRAP_SERVER = os.getenv("BOOTSTRAP_SERVER")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN")
INFLUXDB_URL = os.getenv("INFLUXDB_URL")
REDIS_HOST = os.getenv("REDIS_HOST")

# Inisialisasi SparkSession
spark = SparkSession \
    .builder \
    .appName("PArrival") \
    .getOrCreate()

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVER) \
    .option("subscribe", PREPROCESSED_TOPIC) \
    .load()

# Skema JSON
schema = StructType() \
        .add("station", StringType()) \
        .add("channel", StringType()) \
        .add("time", StringType()) \
        .add("data", FloatType())

# Parse JSON
df_listen = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .select(from_json("value", schema).alias("data")) \
    .select("data.*")

client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)

class PArrival:

    def __init__(self,influxdb_client):
        self.client = influxdb_client
        self.write_api = client.write_api(write_options=SYNCHRONOUS)

    def open(self, partition_id, epoch_id):
        print("Opened %d, %d" % (partition_id, epoch_id))
        return True

    def process(self, row):
        measurement = "seismograf"
        station = row["station"]
        channel = row["channel"]
        time = row["time"]
        '''
        # Konversi string menjadi objek waktu
        waktu_titik_saat_ini = datetime.strptime(time, "%Y-%m-%d %H:%M:%S.%f")

        # Kurangi 30 detik dari waktu awal
        waktu_titik_30s_lalu = waktu_titik_saat_ini - timedelta(seconds=30)

        query_api = self.client.query_api()

        query = 'from(bucket:"' + bucket + '")\
        |> range(start:'+ waktu_titik_30s_lalu +',stop:'+ waktu_titik_saat_ini +')\
        |> filter(fn:(r) => r._measurement == "' + measurement + '")\
        |> filter(fn:(r) => r.station == "'+ station +'")\
        |> filter(fn:(r) => r.channel == "' + channel +'")\
        |> filter(fn:(r) => r._field == "data")'

        result = query_api.query(org=org, query=query)
        list_data = []
        
        for poin in result.get_points():
            data = poin["field"]
            list_data.append(data)
        
        sampling = 25
        cft = recursive_sta_lta(list_data, int(2.5 * sampling), int(10. * sampling))
        on_of = trigger_onset(cft, 3.3, 0.5)
        '''
        on_of = [1]
        if len(on_of) > 0 :
            # Buat objek koneksi ke server Redis
            redis_client = redis.StrictRedis(host=REDIS_HOST, port=6379, db=0)
            p_arrival_flag = redis_client.hget(station,channel)

            if p_arrival_flag == None :
                redis_client.hset(station,'channel',channel)
                redis_client.expire(station, 10)
                self.find_p_arrival(station,time, channel)

    def close(self, error):
        self.write_api.__del__()
        self.client.__del__()
        print("Closed with error: %s" % str(error))

    def find_p_arrival(self,station,time,channel):
        json_data = {'station': station,
                    'time': time}
        # Konversi JSON ke bytes
        value = json.dumps(json_data).encode('utf-8')

        point = Point("p_arrival").tag("time_input", time).tag("channel", channel).tag("station", station).field("p_arrival", 1)
        self.write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)

        self.producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVER)
        self.producer.send(P_ARRIVAL_TOPIC, value=value)
        self.producer.flush()

df_listen.writeStream.foreach(PArrival(client)).start().awaitTermination()
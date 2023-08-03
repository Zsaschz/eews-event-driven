from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StringType, FloatType, TimestampNTZType, StructField, IntegerType

from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client import Point, WritePrecision, InfluxDBClient
from datetime import datetime, timedelta
from obspy.signal.trigger import recursive_sta_lta, trigger_onset
from kafka import KafkaProducer
import json
import redis
#from topics import PREPROCESSED_TOPIC, P_ARRIVAL_TOPIC
from dateutil import parser
import time

import yaml  # atau pustaka lain untuk membaca file konfigurasi

def load_config(file_path):
    with open(file_path, "r") as config_file:
        config = yaml.load(config_file, Loader=yaml.FullLoader)
    return config
config = load_config("config.yaml")

BOOTSTRAP_SERVER = config["BOOTSTRAP_SERVER"]
INFLUXDB_ORG = config["INFLUXDB_ORG"]
INFLUXDB_BUCKET = config["INFLUXDB_BUCKET"]
INFLUXDB_TOKEN = config["INFLUXDB_TOKEN"]
INFLUXDB_URL = config["INFLUXDB_URL"]
REDIS_HOST = config["REDIS_HOST"]
MONITOR_TOPIC = "monitor_p_arrival"
PREPROCESSED_TOPIC = 'preprocessed'
P_ARRIVAL_TOPIC = 'p-arrival'
PREDICTION_TOPIC = 'prediction'

def nearest_datetime_rounded(datetime: datetime, step_in_micros: int = 40000):
    microsecond = datetime.time().microsecond
    remainder = microsecond % step_in_micros
    rounded = datetime
    if remainder < (step_in_micros / 2):
        rounded -= timedelta(microseconds=remainder)
    else:
        rounded += timedelta(microseconds=(step_in_micros - remainder))
    return rounded

def normalizations(array):
    # res = array/np.amax(np.abs(array))
    res = array/100000
    return res

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
        now = row["time"]
        
        #Mencari waktu 30 detik yang lalu
        past_30s = parser.parse(now) - timedelta(seconds=30)
        # Menghapus offset zona waktu
        past_30s_no_offset = past_30s.replace(tzinfo=None)
        # Format kembali dalam format yang diinginkan
        output_format = '%Y-%m-%dT%H:%M:%S.%fZ'
        past_30s_format = past_30s_no_offset.strftime(output_format)

        query_api = self.client.query_api()

        start = time.monotonic_ns()

        query = 'from(bucket:"' + INFLUXDB_BUCKET + '")\
        |> range(start:'+ past_30s_format +',stop:'+ now +')\
        |> filter(fn:(r) => r._measurement == "' + measurement + '")\
        |> filter(fn:(r) => r.station == "'+ station +'")\
        |> filter(fn:(r) => r.channel == "' + channel +'")\
        |> filter(fn:(r) => r._field == "data")'

        result = query_api.query(org=INFLUXDB_ORG, query=query)
        waktu_query = (time.monotonic_ns() - start) / 10**9

        if len(result)>0:  #sementara 0 dulu asumsi sudah diinterpolasi sehingga pasti ada 25*30 data
            list_data = []
            
            for table in result:
                for value in table.records:
                    norm = normalizations(value.values["_value"])
                    list_data.append(norm)
            
            sampling = 25
            start_hitung_p_arrival = time.monotonic_ns()

            #Algortima mencari P_arrival
            cft = recursive_sta_lta(list_data, int(2.5 * sampling), int(10. * sampling))
            on_of = trigger_onset(cft, 3.3, 0.5)

            waktu_hitung_p_arrival = (time.monotonic_ns() - start_hitung_p_arrival) / 10**9
            self.monitor(station,channel,now,waktu_query,waktu_hitung_p_arrival, len(list_data))

            on_of = [1] #untuk testing produce topic p-arrival
            if len(on_of) > 0 :
                # Buat objek koneksi ke server Redis
                redis_client = redis.StrictRedis(host='172.17.0.1', port=6379, db=0)
                p_arrival_flag = redis_client.hget(station,"channel")

                if p_arrival_flag == None :
                    redis_client.hset(station,'channel',channel)
                    redis_client.expire(station, 10)
                    self.find_p_arrival(station,now, channel)

    def close(self, error):
        self.write_api.__del__()
        self.client.__del__()
        print("Closed with error: %s" % str(error))

    def monitor(self,station,channel,time_data,waktu_query,waktu_hitung_p_arrival,banyak_data):
        json_data = {'station':station,
                     'channel':channel,
                     'time_data':time_data,
                    'waktu_query':waktu_query,
                    'waktu_hitung_p_arrival':waktu_hitung_p_arrival,
                    'banyak_data': banyak_data}
        # Konversi JSON ke bytes
        value = json.dumps(json_data).encode('utf-8')
        self.producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVER)
        self.producer.send(MONITOR_TOPIC, value=value)
        self.producer.flush()

    def find_p_arrival(self,station,time_data,channel):
        json_data = {'station': station,
                    'time': time_data}
        # Konversi JSON ke bytes
        value = json.dumps(json_data).encode('utf-8')

        point = Point("p_arrival").time(time_data, write_precision=WritePrecision.MS).tag("channel", channel).tag("station", station).field("time_data", time_data)
        self.write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)

        self.producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVER)
        self.producer.send(P_ARRIVAL_TOPIC, value=value)
        self.producer.flush()

df_listen.writeStream.foreach(PArrival(client)).start().awaitTermination()
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StringType, ArrayType, StructField, FloatType

from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client import Point, WritePrecision, InfluxDBClient
from kafka import KafkaProducer
from topics import PREPROCESSED_TOPIC, P_ARRIVAL_TOPIC,MONITOR_P_ARRIVAL_TOPIC
from utils import *

import json
import redis
import time

config = load_config_yaml("config.yaml")

BOOTSTRAP_SERVER = config["BOOTSTRAP_SERVER"]
INFLUXDB_ORG = config["INFLUXDB_ORG"]
INFLUXDB_BUCKET = config["INFLUXDB_BUCKET"]
INFLUXDB_TOKEN = config["INFLUXDB_TOKEN"]
INFLUXDB_URL = config["INFLUXDB_URL"]
REDIS_HOST = config["REDIS_HOST"]

def main():
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

    schema = StructType([
        StructField("BHE", StructType([
            StructField("data", ArrayType(FloatType(), True), True),
            StructField("endtime", StringType(), True),
            StructField("starttime", StringType(), True)
        ]), True),
        StructField("BHN", StructType([
            StructField("data", ArrayType(FloatType(), True), True),
            StructField("endtime", StringType(), True),
            StructField("starttime", StringType(), True)
        ]), True),
        StructField("BHZ", StructType([
            StructField("data", ArrayType(FloatType(), True), True),
            StructField("endtime", StringType(), True),
            StructField("starttime", StringType(), True)
        ]), True),
        StructField("injected_to_preprocessed_at", StringType(), True),
        StructField("station", StringType(), True)
    ])

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
            station = row["station"]
            time_injected = row["injected_to_preprocessed_at"] #UTC String
            bhe_data = row.BHE.data
            bhn_data = row.BHN.data
            bhz_data = row.BHZ.data
            
            #anggap data sudah bersih
            sampling = 25
            start_hitung_p_arrival = time.monotonic_ns()
            search_p_arrival = get_Parrival(bhe_data,bhn_data,bhz_data, sampling)
            waktu_hitung_p_arrival = (time.monotonic_ns() - start_hitung_p_arrival) / 10**9
            
            #data = [bhe_data,bhz_data,bhn_data]
            #self.find_p_arrival(station,time_injected,data)
            start_redis = time.monotonic_ns()

            redis_client = redis.StrictRedis(host=REDIS_HOST, port=6379, db=0)
            p_arrival_flag = redis_client.hget(station,"p_arrival")
            search_p_arrival = [1] #untuk testing produce topic p-arrival
            data = [bhe_data,bhz_data,bhn_data]

            waktu_redis = (time.monotonic_ns() - start_redis) / 10**9

            self.monitor(station, time_injected, waktu_hitung_p_arrival, waktu_redis)
            #Mengecek deteksi P arrival 4 kali berturut-turut
            if p_arrival_flag == None :
                if len(search_p_arrival) > 0 :
                    redis_client.hset(station,'p_arrival',1)
                    redis_client.expire(station, 10)
            else :
                if len(search_p_arrival) > 0 :
                    if int(p_arrival_flag) < 3 :
                        redis_client.hset(station,'p_arrival',int(p_arrival_flag) + 1)
                        redis_client.expire(station, 10)
                    else :
                        redis_client.delete(station)
                        #sudah menemukan 4 p arrival berturut-turut
                        self.find_p_arrival(station,time_injected,data)
                else :
                    redis_client.delete(station)
            

        def close(self, error):
            #self.write_api.__del__()
            #self.client.__del__()
            print("Closed with error: %s" % str(error))

        def monitor(self,station, time_injected, waktu_hitung_p_arrival, waktu_redis):
            json_data = {'station':station,
                        'time_injected': time_injected,
                        'waktu_hitung_p_arrival':waktu_hitung_p_arrival,
                        'waktu_redis': waktu_redis}
            # Konversi JSON ke bytes
            value = json.dumps(json_data).encode('utf-8')

            self.producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVER)
            self.producer.send(MONITOR_P_ARRIVAL_TOPIC, value=value)
            self.producer.flush()

        def find_p_arrival(self,station,time_data,data):
            point = Point("p_arrival").time(time_data, write_precision=WritePrecision.MS).tag("station", station).field("time_data", time_data)
            self.write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)
            
            json_data = {'station': station,
                        'time': time_data,
                        'data': data}
            # Konversi JSON ke bytes
            value = json.dumps(json_data).encode('utf-8')
            
            self.producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVER)
            self.producer.send(P_ARRIVAL_TOPIC, value=value)
            self.producer.flush()

    df_listen.writeStream.foreach(PArrival(client)).start().awaitTermination()

if __name__ == '__main__':
    main()
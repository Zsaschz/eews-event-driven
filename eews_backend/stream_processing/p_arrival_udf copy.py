from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, udf,col
from pyspark.sql.types import StructType, StringType, ArrayType, StructField, FloatType

from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client import Point, WritePrecision, InfluxDBClient
from kafka import KafkaProducer
from topics import PREPROCESSED_TOPIC, P_ARRIVAL_TOPIC,MONITOR_P_ARRIVAL_TOPIC
from utils import *

import json
import redis
import time
import datetime

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
    waktu_read_kafka = str(datetime.datetime.now())

    client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)

    def process(bhe, bhn, bhz, injected_to_preprocessed_at, station):
            waktu_mulai_function = str(datetime.datetime.now())
            start_awal = time.monotonic_ns()
            bhe_data = bhe.data
            bhn_data = bhn.data
            bhz_data = bhz.data
            
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

            #Mengecek deteksi P arrival 4 kali berturut-turut
            if p_arrival_flag == None :
                if len(search_p_arrival) > 0 :
                    redis_client.hset(station,'p_arrival',1)
                    redis_client.expire(station, 10)
                    waktu_kirim = str(datetime.datetime.now())
                    json_data = {
                        'status':"initiate redis"
                        ,'station': station,
                        'time': injected_to_preprocessed_at,
                        'data': data,
                        'waktu_hitung_p_arrival':waktu_hitung_p_arrival,
                        'waktu_redis': waktu_redis,
                        'waktu_read_kafka':waktu_read_kafka,
                        'waktu_sebelum_panggil_function':waktu_sebelum_panggil_function,
                        'waktu_mulai_function':waktu_mulai_function,
                        'waktu_kirim':waktu_kirim}
                    #return json.dumps(json_data)
                    return None
            else :
                if len(search_p_arrival) > 0 :
                    if int(p_arrival_flag) < 3 :
                        redis_client.hset(station,'p_arrival',int(p_arrival_flag) + 1)
                        redis_client.expire(station, 10)
                        waktu_kirim = str(datetime.datetime.now())
                        json_data = {
                        'status':"sebelum 4 kali redis"
                        ,'station': station,
                        'time': injected_to_preprocessed_at,
                        'data': data,
                        'waktu_hitung_p_arrival':waktu_hitung_p_arrival,
                        'waktu_redis': waktu_redis,
                        'waktu_read_kafka':waktu_read_kafka,
                        'waktu_sebelum_panggil_function':waktu_sebelum_panggil_function,
                        'waktu_mulai_function':waktu_mulai_function,
                        'waktu_kirim':waktu_kirim}
                        #return json.dumps(json_data)
                        return None
                    else :
                        redis_client.delete(station)
                        #sudah menemukan 4 p arrival berturut-turut
                        start_influx = time.monotonic_ns()
                        point = Point("p_arrival").time(injected_to_preprocessed_at, write_precision=WritePrecision.MS).tag("station", station).field("time_data", injected_to_preprocessed_at)
                        write_api = client.write_api(write_options=SYNCHRONOUS)
                        write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)
                        waktu_influx = (time.monotonic_ns() - start_influx) / 10**9

                        waktu_akhir = (time.monotonic_ns() - start_awal) / 10**9
                        waktu_sebelum_ke_kafka = str(datetime.datetime.now())
                        json_data = {'station': station,
                        'time': injected_to_preprocessed_at,
                        'data': data,
                        'waktu_hitung_p_arrival':waktu_hitung_p_arrival,
                        'waktu_redis': waktu_redis,
                        'waktu_influx':waktu_influx,
                        'waktu_akhir':waktu_akhir,
                        'waktu_read_kafka':waktu_read_kafka,
                        'waktu_sebelum_panggil_function':waktu_sebelum_panggil_function,
                        'waktu_mulai_function':waktu_mulai_function,
                        'waktu_sebelum_ke_kafka':waktu_sebelum_ke_kafka}

                        value = json.dumps(json_data).encode('utf-8')
                        producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVER)
                        producer.send(P_ARRIVAL_TOPIC, value=value)
                        producer.flush()
                        return json.dumps(json_data)
                else :
                    redis_client.delete(station)
                    return None
            
                
    prediction_udf = udf(lambda data1, data2, data3, data4, data5: process(data1, data2, data3, data4, data5), StringType())
                
    df_listen= df.selectExpr("CAST(value AS STRING) as json") \
        .select(from_json("json", schema).alias("data")) \
        .select("data.*")
    waktu_sebelum_panggil_function = str(datetime.datetime.now())
    
    df_processed = df_listen.withColumn("p_arrival",prediction_udf("BHE","BHN","BHZ","injected_to_preprocessed_at","station"))
    df_not_null = df_processed.filter('p_arrival is not null').select(col("p_arrival").alias("value"))
    '''
    query = df_not_null\
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVER) \
    .option("topic", P_ARRIVAL_TOPIC) \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .start()
    '''
    #.filter(df_not_null['value'].isNotNull())\
    query = df_not_null\
    .filter(df_not_null['value'].isNotNull())\
    .writeStream \
    .format("console") \
    .start()
    
    #.select(col("p_arrival").alias("value"))\
    #.filter("value is not null")\
    #.option("kafka.bootstrap.servers", BOOTSTRAP_SERVER) \
    #.option("topic", P_ARRIVAL_TOPIC) \
    #.option("checkpointLocation", "/tmp/checkpoint") \
    #.start()
    
    query.awaitTermination()

if __name__ == '__main__':
    main()
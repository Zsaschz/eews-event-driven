from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType
import pyspark.sql.functions as f

from kafka import KafkaProducer
from influxdb_client import InfluxDBClient
from tensorflow.keras.models import load_model
from tensorflow.keras.metrics import MeanAbsoluteError
from pymongo import MongoClient
import numpy as np
import pandas as pd

import datetime
import json
import pause
import yaml


with open("config.yaml", "r") as config_file:
    config = yaml.load(config_file, Loader=yaml.FullLoader)

BOOTSTRAP_SERVER = config["BOOTSTRAP_SERVER"]
INFLUXDB_ORG = config["INFLUXDB_ORG"]
INFLUXDB_BUCKET = config["INFLUXDB_BUCKET"]
INFLUXDB_TOKEN = config["INFLUXDB_TOKEN"]
INFLUXDB_URL = config["INFLUXDB_URL"]
MODEL_FILE = config["MODEL_FILE"]
MONGO_URL = config["MONGO_URL"]
MONGO_DATABASE = config["MONGO_DATABASE"]
P_ARRIVAL_TOPIC = "p-arrival"
PREDICTION_TOPIC = "prediction"


class Predict:
    
    def __init__(self, influxdb_client, model):
        self.client = influxdb_client
        self.query_api = self.client.query_api()
        self.model = model

    def open(self, partition_id, epoch_id):
        print("Opened %d, %d" % (partition_id, epoch_id))
        return True
    
    def process(self, row):
        station = row["station"]
        time = row["time"]

        # Init mongo
        client = MongoClient(MONGO_URL)
        db = client[MONGO_DATABASE]

        # Get nearest station from Mongo
        coordinates = db["station"].find_one({ "name": station })["location"]["coordinates"]
        stations = db["station"].find({ "location": {"$nearSphere": {"$geometry": {"type": "Point", "coordinates": coordinates}, "$maxDistance": 200000}}}, {"name": 1, "_id": 0})
        stations = names if len(names:=[station["name"] for station in stations]) <= 3 else names[:3]

        # Get each station data from Influx
        pause.until(datetime.datetime.strptime(time, "%Y-%m-%dT%H:%M:%S.%fZ") + datetime.timedelta(seconds=10))
        influx_data = self.read_seis_influx(stations, datetime.datetime.strptime(time, "%Y-%m-%dT%H:%M:%S.%fZ"))
        
        # Preprocess (interpolation and transformation) data
        seis_data = list(map(lambda station : self.preprocess(list(station.values())), influx_data))

        # Fill data
        if len(seis_data) == 1:
            seis_data *= 3
        elif len(seis_data) == 2:
            seis_data.append(seis_data[0])

        # Predict data
        preds = self.model.predict(np.array(seis_data), batch_size=4)
        result = pd.DataFrame(columns=["lat", "long", "depth", "magnitude", "time"])

        for prediction, col_result in zip(np.array(preds), ["lat", "long", "depth", "magnitude", "time"]):
            result[col_result] = prediction.squeeze()

        data_mtr = self.denormalization(result.iloc[0])
        data_mtr["p-arrival"] = datetime.datetime.strptime(time, "%Y-%m-%dT%H:%M:%S.%fZ")
        data_mtr["expired"] = data_mtr["p-arrival"] + datetime.timedelta(0,60)
        data_mtr["station"] = station

        # Insert data to Mongo and produce to Kafka
        pred_id = db["prediction"].insert_one(data_mtr).inserted_id

        self.producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVER)
        self.producer.send(PREDICTION_TOPIC, value=json.dumps({"id": str(pred_id)}).encode("utf-8"))
        self.producer.flush()

    def close(self, error):
        self.query_api.__del__()
        self.client.__del__()
        print("Closed with error: %s" % str(error))

    def read_seis_influx(self, stations, time):
        # Define query
        start_time = (time - datetime.timedelta(0,10)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        stop_time = (time + datetime.timedelta(0,10)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

        query = f"""from(bucket: "eews")
        |> range(start: {start_time}, stop: {stop_time})
        |> filter(fn: (r) => r._measurement == "seismograf" and contains(value: r.station, set: {str(stations).replace("'", '"')}) )""" 
        tables = self.query_api.query(query, org="eews")
        
        # Parse query result
        dct = {k:{} for k in stations}
        for table in tables:
            res = []
            for record in table.records:
                res.append(record.get_value())
            dct[record.values.get("station")][record.values.get("channel")] = res
        
        data = list(filter(None, dct.values()))
        return data

    def preprocess(self, data):
        data_interpolated = list(map(lambda x : self.letInterpolate(x, 2000), data))
        data_interpolated_transformed = []
        for i in range(len(data_interpolated[0])):
            data_interpolated_transformed.append([data_interpolated[0][i], data_interpolated[1][i], data_interpolated[2][i]])
        
        return data_interpolated_transformed
    
    def letInterpolate(self, inp, new_len):
        delta = (len(inp)-1) / (new_len-1)
        outp = [self.interpolate(inp, i*delta) for i in range(new_len)]
        return outp
    
    def interpolate(self, lst, fi):
        i, f = int(fi // 1), fi % 1  # Split floating-point index into whole & fractional parts.
        j = i+1 if f > 0 else i  # Avoid index error.
        return (1-f) * lst[i] + f * lst[j]

    def denormalization(self, data):
        max,min = {},{}
        max["lat"] = -6.64264
        min["lat"] = -11.5152
        max["long"] = 115.033
        min["long"] = 111.532
        max["depth"] = 588.426
        min["depth"] = 1.16
        max["magnitude"] = 6.5
        min["magnitude"] = 3.0
        max["time"] = 74.122
        min["time"] = 4.502

        dats = {}
        for col in data.index:
            dats[col] = data[col]*(max[col] - min[col])+min[col]
        return dats


# INIT SPARK AND VARIABLES
spark = SparkSession\
        .builder\
        .appName("Prediction")\
        .getOrCreate()
sc = spark.sparkContext

dependencies = {
    "mean_absolute_error": MeanAbsoluteError,
    "function": MeanAbsoluteError
}
model = load_model(MODEL_FILE, custom_objects=dependencies)

influx_client = InfluxDBClient(
    url=INFLUXDB_URL,
    org=INFLUXDB_ORG,
    token=INFLUXDB_TOKEN
)

# CONSUME FROM KAFKA
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVER) \
    .option("subscribe", P_ARRIVAL_TOPIC) \
    .load()

# SCHEMA INPUT OUTPUT
schema = StructType([StructField("station", StringType()), \
                     StructField("time", StringType())])
schema_output = StructType([StructField("lat", StringType()), \
                            StructField("long", StringType()), \
                            StructField("depth", StringType()), \
                            StructField("magnitude", StringType()), \
                            StructField("time", StringType())])

# PARSE INPUT JSON
df_json = df.selectExpr("CAST(value AS STRING) as json") \
        .select(f.from_json("json", schema).alias("data")) \
        .select("data.*")

# PROCESS EACH EVENT
df_json.writeStream.foreach(Predict(influx_client, model)).start().awaitTermination()

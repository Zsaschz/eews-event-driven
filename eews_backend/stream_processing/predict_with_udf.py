from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType
import pyspark.sql.functions as f

from influxdb_client import InfluxDBClient
from tensorflow.keras.models import load_model
from tensorflow.keras.metrics import MeanAbsoluteError

import datetime
import json
import yaml

# INIT SPARK AND VARIABLES
with open("config.yaml", "r") as config_file:
    config = yaml.load(config_file, Loader=yaml.FullLoader)

BOOTSTRAP_SERVER = config["BOOTSTRAP_SERVER"]
MODEL_FILE = config["MODEL_FILE"]
P_ARRIVAL_TOPIC = "p-arrival"
PREDICTION_TOPIC = "prediction"

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
broadcasted_model = sc.broadcast(model)

# CONSUME FROM KAFKA
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVER) \
    .option("subscribe", P_ARRIVAL_TOPIC) \
    .load()

# SCHEMA INPUT
schema = StructType([\
    StructField("station", StringType()), \
    StructField("time", StringType())])

# PARSE INPUT JSON
df_json = df.selectExpr("CAST(value AS STRING) as json") \
        .select(f.from_json("json", schema).alias("data")) \
        .select("data.*")

def apply_prediction(station, time, config):
    from pymongo import MongoClient
    import numpy as np
    import pandas as pd
    import pause
    
    def read_seis_influx(stations, time):
        # Init influx
        INFLUXDB_ORG = config["INFLUXDB_ORG"]
        INFLUXDB_BUCKET = config["INFLUXDB_BUCKET"]
        INFLUXDB_TOKEN = config["INFLUXDB_TOKEN"]
        INFLUXDB_URL = config["INFLUXDB_URL"]
        client = InfluxDBClient(
            url=INFLUXDB_URL,
            org=INFLUXDB_ORG,
            token=INFLUXDB_TOKEN
        )
        query_api = client.query_api()

        # Define query
        start_time = (time - datetime.timedelta(0,10)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        stop_time = (time + datetime.timedelta(0,10)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

        query = f"""from(bucket: "eews")
        |> range(start: {start_time}, stop: {stop_time})
        |> filter(fn: (r) => r._measurement == "seismograf" and contains(value: r.station, set: {str(stations).replace("'", '"')}) )""" 
        tables = query_api.query(query, org="eews")
        
        # Parse query result
        dct = {k:{} for k in stations}
        for table in tables:
            res = []
            for record in table.records:
                res.append(record.get_value())
            dct[record.values.get("station")][record.values.get("channel")] = res
        
        data = list(filter(None, dct.values()))
        return data

    def preprocess(data):
        def letInterpolate(inp, new_len):
            delta = (len(inp)-1) / (new_len-1)
            outp = [interpolate(inp, i*delta) for i in range(new_len)]
            return outp

        def interpolate(lst, fi):
            i, f = int(fi // 1), fi % 1  # Split floating-point index into whole & fractional parts.
            j = i+1 if f > 0 else i  # Avoid index error.
            return (1-f) * lst[i] + f * lst[j]

        data_interpolated = list(map(lambda x : letInterpolate(x, 2000), data))
        data_interpolated_transformed = []
        for i in range(len(data_interpolated[0])):
            data_interpolated_transformed.append([data_interpolated[0][i], data_interpolated[1][i], data_interpolated[2][i]])
        
        return data_interpolated_transformed

    def denormalization(data):
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

    # Init mongo
    MONGO_URL = config["MONGO_URL"]
    MONGO_DATABASE = config["MONGO_DATABASE"]
    client = MongoClient(MONGO_URL)
    db = client[MONGO_DATABASE]

    # Get nearest station from Mongo
    coordinates = db["station"].find_one({ "name": station })["location"]["coordinates"]
    stations = db["station"].find({ "location": {"$nearSphere": {"$geometry": {"type": "Point", "coordinates": coordinates}, "$maxDistance": 200000}}}, {"name": 1, "_id": 0})
    stations = names if len(names:=[station["name"] for station in stations]) <= 3 else names[:3]

    # Get each station data from Influx
    pause.until(datetime.datetime.strptime(time, "%Y-%m-%dT%H:%M:%S.%fZ") + datetime.timedelta(seconds=10))
    influx_data = read_seis_influx(stations, datetime.datetime.strptime(time, "%Y-%m-%dT%H:%M:%S.%fZ"))

    # Preprocess (interpolation and transformation) data
    seis_data = list(map(lambda station : preprocess(list(station.values())), influx_data))

    # Fill data so that len(seis_data)==3 (need three station data to make prediction)
    if len(seis_data) == 1:
        seis_data *= 3
    elif len(seis_data) == 2:
        seis_data.append(seis_data[0])

    # Predict data
    preds = broadcasted_model.value.predict(np.array(seis_data), batch_size=4)
    result = pd.DataFrame(columns=["lat", "long", "depth", "magnitude", "time"])

    for prediction, col_result in zip(np.array(preds), ["lat", "long", "depth", "magnitude", "time"]):
        result[col_result] = prediction.squeeze()

    data_mtr = denormalization(result.iloc[0])
    data_mtr["p-arrival"] = datetime.datetime.strptime(time, "%Y-%m-%dT%H:%M:%S.%fZ")
    data_mtr["expired"] = data_mtr["p-arrival"] + datetime.timedelta(0,60)
    data_mtr["station"] = station

    # Insert data to Mongo
    pred_id = db["prediction"].insert_one(data_mtr).inserted_id
    
    json_message = {"id": str(pred_id)}
    return json.dumps(json_message)

# CONVERT FUNCTION TO UDF
prediction_udf = f.udf(lambda data1, data2: apply_prediction(data1, data2, config), StringType())

# APPLY UDF TO DATAFRAME AND PRODUCE TO KAFKA
query = df_json.select((prediction_udf(df_json.station, df_json.time)).alias("value"))\
    .selectExpr("CAST(value AS STRING)")\
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVER) \
    .option("topic", PREDICTION_TOPIC) \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .start()

query.awaitTermination()

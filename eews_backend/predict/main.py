from database.influxdb import *
from database.mongodb import mongo_client_sync
from stream_processing.kafka import KafkaConsumer, KafkaProducer
from stream_processing.topics import P_ARRIVAL_TOPIC, PREDICTION_TOPIC
from predict.predictor import Predictor
from utils.helper_functions import letInterpolate, denormalization

from dotenv import load_dotenv
import datetime
import pause

load_dotenv()

consumer = KafkaConsumer(P_ARRIVAL_TOPIC, "eews", {})
producer = KafkaProducer(PREDICTION_TOPIC)

client = InfluxDBClient(
    url=INFLUXDB_URL,
    org=INFLUXDB_ORG,
    token=INFLUXDB_TOKEN
)
query_api = client.query_api()
_, db = mongo_client_sync()

predictor = Predictor()

def main(msg):
    # Get nearest station from Mongo
    stations = get_nearest_station(msg["station"], 200000)

    # Get each station data from Influx
    pause.until(datetime.datetime.strptime(msg["time"], "%Y-%m-%dT%H:%M:%S.%fZ") + datetime.timedelta(seconds=10))
    influx_data = read_seis_influx(stations, datetime.datetime.strptime(msg["time"],'%Y-%m-%dT%H:%M:%S.%fZ'))
    
    # Preprocess (interpolation and transformation) data
    seis_data = list(map(lambda station : preprocess(list(station.values())), influx_data))

    if len(seis_data) == 1:
        seis_data *= 3
    elif len(seis_data) == 2:
        seis_data.append(seis_data[0])

    # Predict data
    preds = predictor.predict(seis_data)
    
    data_mtr = denormalization(preds.iloc[0])
    data_mtr["p-arrival"] = datetime.datetime.strptime(msg["time"],'%Y-%m-%dT%H:%M:%S.%fZ')
    data_mtr["expired"] = data_mtr["p-arrival"] + datetime.timedelta(0,60)
    data_mtr["station"] = msg["station"]

    # Insert data to Mongo and produce to Kafka
    pred_id = db['prediction'].insert_one(data_mtr).inserted_id
    producer.produce_message({"id": str(pred_id)})

def read_seis_influx(stations: str, time: datetime.datetime):
    start_time = (time - datetime.timedelta(0,10)).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    stop_time = (time + datetime.timedelta(0,10)).strftime('%Y-%m-%dT%H:%M:%S.%fZ')

    query = f"""from(bucket: "eews")
    |> range(start: {start_time}, stop: {stop_time})
    |> filter(fn: (r) => r._measurement == "seismograf" and contains(value: r.station, set: {str(stations).replace("'", '"')}) )""" 
    tables = query_api.query(query, org="eews")
    
    dct = {k:{} for k in stations}
    for table in tables:
        res = []
        for record in table.records:
            res.append(record.get_value())
        dct[record.values.get('station')][record.values.get('channel')] = res
    
    data = list(filter(None, dct.values()))
    return data

def get_nearest_station(name, max_distance):
    coordinates = db['station'].find_one({ 'name': name })['location']['coordinates']
    stations = db['station'].find({ 'location': {'$nearSphere': {'$geometry': {'type': 'Point', 'coordinates': coordinates}, '$maxDistance': max_distance}}}, {'name': 1, '_id': 0})
    
    return names if len(names:=[station['name'] for station in stations]) <= 3 else names[:3]

def preprocess(data):
    data_interpolated = list(map(lambda x : letInterpolate(x, 2000), data))
    data_interpolated_transformed = []
    for i in range(len(data_interpolated[0])):
        data_interpolated_transformed.append([data_interpolated[0][i], data_interpolated[1][i], data_interpolated[2][i]])
    
    return data_interpolated_transformed

if __name__ == "__main__":
    consumer.consume(on_message=main, on_error=None)

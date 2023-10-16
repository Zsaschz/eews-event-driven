from datetime import timedelta, timezone
import random
from dateutil import parser
from math import ceil
import uuid
from dotenv import load_dotenv
from fastapi import (
    FastAPI,
    HTTPException,
    Response,
    WebSocket,
    WebSocketDisconnect,
    status,
    UploadFile,
    BackgroundTasks,
)
from fastapi.params import Body
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from influxdb_client.client.flux_table import TableList, FluxRecord
from typing import Dict, List
from influxdb_client import Point, WritePrecision
from obspy import Stream, Trace, read
from pprint import pprint
from logging.config import dictConfig

from database.mongodb import mongo_client
from database.influxdb import influx_client
from stream import KafkaProducer, KafkaConsumer, PREPROCESSED_TOPIC, RAW_TOPIC
from utils import *
from .model import *
from .websocket import ConnectionManager
from log_config import logging_config

import logging
import haversine as hs
import time
import asyncio
import pandas as pd
import io
import os
import json

load_dotenv()
dictConfig(logging_config)

MODULE_DIR = "./rest/"
STATIC_DIR = "static/"
SIMULATE_REALTIME = False if os.getenv("SIMULATE_REALTIME") == "False" else True
MSEED_RANGE_IN_SECONDS = 30
BACKEND_IP = os.getenv("BACKEND_IP") if os.getenv("BACKEND_IP") else "localhost"

origins = [
    "http://host.docker.internal",
    "http://localhost",
    "http://localhost:3000",
    f"http://{BACKEND_IP}"
]

app = FastAPI()
log = logging.getLogger("rest")


app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

log.info(f"{SIMULATE_REALTIME=}")

manager = ConnectionManager()
# consumer = KafkaConsumer(RAW_TOPIC, uuid.uuid4())
producer = KafkaProducer(PREPROCESSED_TOPIC)
_, db = mongo_client()
client = influx_client()

HTML = f"""
<!DOCTYPE html>
<html>
    <head>
        <title>Chat</title>
    </head>
    <body>
        <h1>WebSocket Chat</h1>
        <form action="" onsubmit="sendMessage(event)">
            <input type="text" id="messageText" autocomplete="off"/>
            <button>Send</button>
        </form>
        <ul id='messages'>
        </ul>
        <script>
            var ws = new WebSocket("ws://{BACKEND_IP}:80/ws"); """ + """
            ws.onmessage = function(event) {
                var messages = document.getElementById('messages')
                var message = document.createElement('li')
                var content = document.createTextNode(event.data)
                if (window != undefined) {
                    window.console.log(JSON.parse(event.data))
                }
                message.appendChild(content)
                messages.appendChild(message)
            };
            function sendMessage(event) {
                var input = document.getElementById("messageText")
                ws.send(input.value)
                input.value = ''
                event.preventDefault()
            }
        </script>
    </body>
</html>
"""


# @threaded
# def listen(consumer: KafkaConsumer, manager: ConnectionManager):
#     log.info("Listening for messages")
#     try:
#         consumer.consume(on_message=lambda message: manager.broadcast(message))
#     except Exception as e:
#         log.error(e)


# @app.on_event("startup")
# async def startup_event():
#     try:
#         listen(consumer, manager)
#     except Exception as e:
#         log.error(e)


@app.get("/post", status_code=201)
async def post():
    pass
    # data = {
    #     "lat": random.uniform(5.626791, -9),
    #     "long": random.uniform(95.429725, 140.884050),
    #     "depth": 8889.901495235445,
    #     "magnitude": 42.41240072250366,
    #     "time": 1548.9176885528566,
    #     "p-arrival": parser.parse("2015-08-20T15:11:00.000Z"),
    #     "expired": parser.parse("2015-08-20T15:12:00.000Z"),
    #     "station": "ABJI",
    # }
    # print(data)
    # await db["prediction"].insert_one(data)

    # time = datetime(2015, 8, 20, 15, 11, 47, tzinfo=timezone.utc)
    # records = []
    # with client.write_api() as writer:
    #     for i in range(10):
    #         records.append(
    #             Point("p_arrival")
    #             .time(time + timedelta(seconds=i), write_precision=WritePrecision.S)
    #             .tag("station", "KHK")
    #             .field("time_data", (time + timedelta(seconds=i)).isoformat())
    #         )
    #     print(records)
    #     writer.write(bucket="eews", record=records)


@app.get("/test")
async def test():
    query_api = client.query_api()
    now = datetime(2015, 8, 20, 15, 11, 47, tzinfo=timezone.utc)
    query = f"""
            from(bucket: "eews") 
                |> range(start: {(now - timedelta(seconds=1)).isoformat()}, stop: {now.isoformat()}) 
                |> filter(fn: (r) => r["_measurement"] == "p_arrival" or r["_measurement"] == "seismograf")"""
    data: TableList = query_api.query(query=query)
    result = {}
    for records in data:
        row: FluxRecord
        for row in records:
            station = row.values["station"]
            _time = row.values["_time"]

            current_station = result.get(
                station, {"BHE": [], "BHN": [], "BHZ": [], "p_arrival": []}
            )

            if row.values["_measurement"] == "seismograf":
                channel = row.values["channel"]
                data = row.values["_value"]
                current_channel = current_station[channel]
                current_channel.append(
                    {
                        "time": _time.isoformat(),
                        "data": data,
                    }
                )
                current_station[channel] = current_channel
                result[station] = current_station

            elif row.values["_measurement"] == "p_arrival":
                current_station["p_arrival"].append(_time.isoformat())
                result[station] = current_station

    prediction = (
        await db["prediction"]
        .find(
            {
                "p-arrival": {"$lte": now - timedelta(seconds=1)},
                "expired": {"$gte": now},
            }
        )
        .to_list(1000000000)
    )
    for p in prediction:
        del p["_id"]

    print(now - timedelta(seconds=1))
    print(now)
    return {"data": result, "prediction": prediction}
    # return extended_data.to_dict()


@app.get("/")
async def get():
    return HTMLResponse(HTML)


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        query_api = client.query_api()
        now = datetime(2023, 10, 9, 2, 18, 19, tzinfo=timezone.utc) # Date for simulation purposes
        if SIMULATE_REALTIME:
            now = datetime.now(tz=timezone.utc) - timedelta(
                seconds=MSEED_RANGE_IN_SECONDS
            )
        while True:
            start = time.monotonic_ns()
            query = f"""
            from(bucket: "eews") 
                |> range(start: {(now - timedelta(seconds=1)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")}, stop: {now.strftime("%Y-%m-%dT%H:%M:%S.%fZ")}) 
                |> filter(fn: (r) => r["_measurement"] == "p_arrival" or r["_measurement"] == "seismograf")"""
            data: TableList = query_api.query(query=query)
            result = {}
            for records in data:
                row: FluxRecord
                for row in records:
                    station = row.values["station"]
                    _time = row.values["_time"]

                    current_station = result.get(
                        station, {"BHE": [], "BHN": [], "BHZ": [], "p_arrival": []}
                    )

                    if row.values["_measurement"] == "seismograf":
                        channel = row.values["channel"]
                        data = row.values["_value"]
                        current_channel = current_station[channel]
                        current_channel.append(
                            {
                                "time": _time.isoformat(),
                                "data": data,
                            }
                        )
                        current_station[channel] = current_channel
                        result[station] = current_station

                    elif row.values["_measurement"] == "p_arrival":
                        current_station["p_arrival"].append(_time.isoformat())
                        result[station] = current_station

            prediction = (
                await db["prediction"]
                .find(
                    {
                        "p-arrival": {"$lte": now - timedelta(seconds=1)},
                        "expired": {"$gte": now},
                    }
                )
                .to_list(1000000000)
            )
            for p in prediction:
                del p["_id"]
                del p["p-arrival"]
                del p["expired"]

            json_data = json.dumps({"data": result, "prediction": prediction})
            now += timedelta(seconds=1)
            await manager.broadcast(json_data)
            diff = (time.monotonic_ns() - start) / 10**9
            await asyncio.sleep(1 - diff)
    except Exception as e:
        log.error(e)
        log.warning(f"Client {websocket} has been disconnected")
        manager.disconnect(websocket)


@app.get("/station", response_model=List[StationModel])
async def list_seismometer():
    list_data = await db["station"].find().to_list(1000000000)
    return list_data


@app.get("/station/{name}", response_model=StationModel)
async def get_seismometer(name: str):
    data = await db["station"].find_one({"name": name})
    if data is not None:
        return data
    raise HTTPException(
        status_code=404, detail=f"Seismometer with name {name} not found"
    )


@app.put("/station/{name}", response_model=StationModel)
async def update_seismometer(name: str, data: StationModel = Body(...)):
    data = data.model_dump()

    if len(data) >= 1:
        update_result = await db["station"].update_one({"name": name}, {"$set": data})

        if update_result.modified_count == 1:
            if (
                updated_data := await db["station"].find_one({"name": name})
            ) is not None:
                return updated_data

    if (existing_data := await db["station"].find_one({"name": name})) is not None:
        return existing_data

    raise HTTPException(
        status_code=404, detail=f"Seismometer with name {name} not found"
    )


@app.post("/station", response_model=StationModel, status_code=status.HTTP_201_CREATED)
async def create_seismometer(data: StationModel = Body(...)):
    data = data.model_dump()
    if (
        existing_data := await db["station"].find_one({"name": data["name"]})
    ) is not None:
        raise HTTPException(
            status_code=400,
            detail=f"Seismometer with name {data['name']} already exists",
        )

    new_data = await db["station"].insert_one(data)
    if (
        existing_data := await db["station"].find_one({"_id": new_data.inserted_id})
    ) is not None:
        return existing_data


@app.delete("/station/{name}")
async def delete_seismometer(name: str):
    delete_result = await db["station"].delete_one({"name": name})

    if delete_result.deleted_count == 1:
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    raise HTTPException(
        status_code=404, detail=f"Seismometer with name {name} not found"
    )


@app.post("/mseed", status_code=status.HTTP_201_CREATED)
async def upload_mseed(file: UploadFile, background_tasks: BackgroundTasks):
    filename = file.filename
    log.debug(f"Received mseed with name {filename}")
    contents = await file.read()
    background_tasks.add_task(save_mseed, contents, filename)
    return {"file_size": file.size, "filename": filename}


@measure_execution_time
def save_mseed(contents: bytes, filename: str):
    log.info("Saving mseed on the background")
    st = read(io.BytesIO(contents))

    log.debug(f"Stream {st}")

    if len(st) > 0:
        first_starttime = min([trace.stats["starttime"] for trace in st])
        first_endtime = min([trace.stats["endtime"] for trace in st])

        processed = process_data(st)
        produce_windowed_data(processed, first_starttime, first_endtime)
        save_to_influx(st)


@measure_execution_time
def produce_windowed_data(stream: Stream, first_starttime, first_endtime):
    rounded_starttime = nearest_datetime_rounded(first_starttime, 0.04 * 10**6)
    dt = UTCDateTime(rounded_starttime)

    log.info("Producing windowed events to kafka")

    while dt + 8 <= first_endtime:
        trimmed = stream.slice(dt, dt + 8, keep_empty_traces=True)
        if len(trimmed) > 0:
            event = {
                "station": trimmed[0].stats["station"],
            }
            for detail in trimmed:
                event[detail.stats["channel"]] = {
                    "starttime": str(detail.stats.starttime),
                    "endtime": str(detail.stats.endtime),
                    "data": detail.data.tolist(),
                }
            producer.produce_message(event, event["station"])
        dt += 0.04
    return dt


@measure_execution_time
def save_to_influx(stream: Stream):
    trace: Trace
    records = []
    for trace in stream:
        starttime: datetime = UTCDateTime(trace.stats.starttime).datetime
        delta = 1 / int(trace.stats.sampling_rate)
        channel = trace.stats.channel
        station = trace.stats.station
        starttime = nearest_datetime_rounded(starttime, delta * 10**6)

        for data_point in trace.data:
            point = (
                Point("seismograf")
                .time(starttime, write_precision=WritePrecision.MS)
                .tag("channel", channel)
                .tag("station", station)
                .field("data", data_point)
            )
            records.append(point)
            starttime += timedelta(seconds=delta)

    with client.write_api() as writer:
        log.info(f"Start batch save of {len(records)} data to InfluxDB")
        writer.write(bucket="eews", record=records)


@measure_execution_time
def process_data(stream: Stream):
    mseed_data = stream
    new_stream = Stream()
    detail: Trace
    for detail in mseed_data:
        trace = detail.copy()
        fs = detail.stats.sampling_rate
        lowcut = 1.0
        highcut = 5.0
        order = 5
        data_before = detail.data
        data_processed = butter_bandpass_filter(data_before, lowcut, highcut, fs, order)
        trace.data = data_processed
        trace.interpolate(25)
        trace.stats["delta"] = 1 / 25
        trace.stats["sampling_rate"] = 25
        new_stream.append(trace)
    return new_stream

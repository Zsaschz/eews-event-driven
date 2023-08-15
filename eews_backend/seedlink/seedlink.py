from math import ceil
from typing import List
from influxdb_client import Point, WritePrecision
from obspy import Stream, Trace, UTCDateTime
from obspy.clients.fdsn import Client
from obspy.clients.seedlink import Client as SeedlinkClient
from stream.main import KafkaProducer
from stream.topics import RAW_TOPIC, PREPROCESSED_TOPIC
from database.influxdb import influx_client
from utils import *
from pprint import pprint

import logging
import logging
import time
import pandas as pd
import os

logger = logging.getLogger(__name__)
logger.setLevel("DEBUG")


class Seedlink:
    def __init__(
        self,
        stations: str,
        network: str = "GE",
        channels: str = "BH*",
        buffer_size: int = 9,
        poll_interval: int = 30,
    ) -> None:
        self.buffer = []
        self.buffer_size = buffer_size
        self.poll_interval = poll_interval
        self.stations = stations
        self.channels = channels
        self.network = network

        self.geofon_client = client = Client("GEOFON")
        self.inventory = client.get_stations(
            network="*",
            station="*",
            starttime="2011-03-11T00:00:00",
            endtime="2018-03-11T00:00:00",
        )
        logger.info(f"Geofon inventory \n {self.inventory}")
        self.seedlink_client = SeedlinkClient("geofon.gfz-potsdam.de", 18000)
        self.influx_client = influx_client()
        self.producer = KafkaProducer(PREPROCESSED_TOPIC)

    def start(self):
        # Set the start and end times for the plot
        endtime = UTCDateTime.now()  # now
        starttime = endtime - self.poll_interval

        while True:
            start = time.monotonic_ns()
            diff = 0
            logger.debug("Getting waveform data")
            st = self.seedlink_client.get_waveforms(
                self.network, self.stations, "*", self.channels, starttime, endtime
            )
            logger.debug(f"Stream {st}")

            # Append the new data to the buffer
            self.buffer.append(st)
            logger.debug(f"Buffer {self.buffer}")

            # If the buffer has grown larger than the buffer size, remove the oldest data
            if len(self.buffer) > self.buffer_size:
                self.buffer.pop(0)

            if len(st) > 0:
                first_starttime = min([trace.stats["starttime"] for trace in st])
                first_endtime = min([trace.stats["endtime"] for trace in st])

                processed = self.process_data(st)
                last_windowed_endtime = self.produce_windowed_data(
                    processed, first_starttime, first_endtime
                )
                self.save_to_influx(st)

                diff = (time.monotonic_ns() - start) / 10**9
                logger.debug(diff)

                # Update starttime for next iteration
                starttime = last_windowed_endtime
                endtime = UTCDateTime().now()
            else:
                starttime = endtime
                endtime = UTCDateTime().now()

            time.sleep(max(self.poll_interval - diff, 0))

    @measure_execution_time
    def produce_windowed_data(self, stream: Stream, first_starttime, first_endtime):
        dt = UTCDateTime(first_starttime)

        while dt + 8 <= first_endtime:
            windowed_data = [None, None, None]
            trimmed = stream.slice(dt, dt + 8, keep_empty_traces=True)
            if len(trimmed) > 0:
                event = {
                    "station": trimmed[0].stats["station"],
                }
                for detail in trimmed:
                    if detail.stats["channel"] == "BHE":
                        windowed_data[0] = detail.data
                        event["BHE"] = {
                            "starttime": str(detail.stats.starttime),
                            "endtime": str(detail.stats.endtime),
                            "data": detail.data.tolist(),
                        }
                    elif detail.stats["channel"] == "BHN":
                        windowed_data[1] = detail.data
                        event["BHN"] = {
                            "starttime": str(detail.stats.starttime),
                            "endtime": str(detail.stats.endtime),
                            "data": detail.data.tolist(),
                        }
                    elif detail.stats["channel"] == "BHZ":
                        windowed_data[2] = detail.data
                        event["BHZ"] = {
                            "starttime": str(detail.stats.starttime),
                            "endtime": str(detail.stats.endtime),
                            "data": detail.data.tolist(),
                        }
                self.producer.produce_message(event, event["station"])
            dt += 0.04
        return dt

    @measure_execution_time
    def save_to_influx(self, stream: Stream):
        trace: Trace
        records = []
        for trace in stream:
            starttime: datetime = UTCDateTime(trace.stats.starttime).datetime
            delta = 1 / int(trace.stats.sampling_rate)
            channel = trace.stats.channel
            station = trace.stats.station

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

        with self.influx_client.write_api() as writer:
            logger.info(f"Start batch save of {len(records)} data to InfluxDB")
            writer.write(bucket="eews", record=records)

    @measure_execution_time
    def process_data(self, stream: Stream):
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
            data_processed = butter_bandpass_filter(
                data_before, lowcut, highcut, fs, order
            )
            trace.data = data_processed
            trace.interpolate(25)
            trace.stats["delta"] = 1 / 25
            trace.stats["sampling_rate"] = 25
            new_stream.append(trace)
        return new_stream
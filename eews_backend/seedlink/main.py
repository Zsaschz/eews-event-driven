from typing import List
from dotenv import load_dotenv

from .seedlink import Seedlink

import multiprocessing
import os
import random
import string

load_dotenv()

station_list = os.getenv("STATION_LIST")
if type(station_list) == str:
    station_list = station_list.split(",")
num_of_stations = int(os.getenv("NUM_OF_STATIONS"))

global process_list
process_list: List[multiprocessing.Process] = []


def seedlink_process(station: str, override: str):
    client = Seedlink(station, override_station=override)
    client.start()


def main():
    stations = station_list if station_list else ["JAGI"]
    station_set = set()
    alphabet = string.ascii_uppercase
    while len(station_set) < num_of_stations:
        random_combination = "".join(random.choice(alphabet) for _ in range(4))
        station_set.add(random_combination)

    for index, station in enumerate(list(station_set)):
        process = multiprocessing.Process(
            target=seedlink_process, args=(stations[index % len(stations)], station)
        )
        process.name = f"seedlink_{station}"
        process_list.append(process)
    for process in process_list:
        process.start()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        for process in process_list:
            process.join()

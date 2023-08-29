from scipy.signal import butter, filtfilt, lfilter
from obspy.signal.trigger import recursive_sta_lta, trigger_onset
from obspy import UTCDateTime
from datetime import datetime, timedelta
from dateutil import parser
from pprint import pprint
import pandas as pd
import numpy as np
import pytz

from .wrapper import measure_execution_time


def normalizations(array):
    # res = array/np.amax(np.abs(array))
    res = array / 100000
    return res


def array_to_str_limit_dec(array):
    lst = ""
    for i in array:
        if i == None:
            i = "None"
            lst += i + " "
        else:
            lst += "{:.10f}".format(np.round_(i, 10)) + " "
    return lst


# band pass filter
def butter_bandpass(lowcut, highcut, fs, order):
    nyq = 0.5 * fs
    low = lowcut / nyq
    high = highcut / nyq
    b, a = butter(order, [low, high], btype="band")
    return b, a


# band pass filter with filtfilt
def butter_bandpass_filter(data, lowcut, highcut, fs, order):
    b, a = butter_bandpass(lowcut, highcut, fs, order=order)
    y = lfilter(b, a, data)
    # y = filtfilt(b, a, data)
    return y


def get_Parrival(data1, data2, data3, sampling):
    E_p = search_Parrival(data1, sampling)
    N_p = search_Parrival(data2, sampling)
    Z_p = search_Parrival(data3, sampling)

    if len(E_p) == 0 and len(N_p) == 0 and len(Z_p) == 0:
        return -1

    else:
        lens = [E_p, N_p, Z_p]
        r = []
        for i in lens:
            if len(i) != 0:
                r.append(i[0])
        s = []
        for i in r:
            if len(i) != 0:
                s.append(i[0])
        return min(s)


def search_Parrival(data, sampling):
    cft = recursive_sta_lta(data, int(2.5 * sampling), int(10.0 * sampling))
    on_of = trigger_onset(cft, 3.3, 0.5)
    return on_of


def split(strr):
    strr = list(filter(None, strr.split(" ")))
    lst = []
    for i in strr:
        if i == "None":
            j = None
            lst.append(j)
        else:
            lst.append(float(i))
    return lst


def s_add_starttime(e, n, z, data):
    l_enz = [("e", e.timestamp), ("n", n.timestamp), ("z", z.timestamp)]
    l_enz.sort(key=lambda a: a[1])
    sample = data[0]["sampling_rate"]

    print(l_enz)
    l_diff = []
    l_diff.append((l_enz[2][0], int(np.around((l_enz[2][1] - l_enz[0][1]) * sample))))
    l_diff.append((l_enz[1][0], int(np.around((l_enz[1][1] - l_enz[0][1]) * sample))))
    l_diff.append((l_enz[0][0], 0))

    l_diff.sort()
    print(l_diff)

    data_e = split(data[0]["data_interpolated"])
    data_n = split(data[1]["data_interpolated"])
    data_z = split(data[2]["data_interpolated"])

    if l_diff[0][1] != 0:
        for i in range(l_diff[0][1]):
            data_e.insert(0, 0)

    if l_diff[1][1] != 0:
        for i in range(l_diff[1][1]):
            data_n.insert(0, 0)

    if l_diff[2][1] != 0:
        for i in range(l_diff[2][1]):
            data_z.insert(0, 0)

    lst = [data_e, data_n, data_z]

    return lst, l_enz[0][0]


def add_null_station(gmji, jagi, pwji):
    gmji = sorted(gmji, key=lambda a: a.stats.starttime)
    jagi = sorted(jagi, key=lambda a: a.stats.starttime)
    pwji = sorted(pwji, key=lambda a: a.stats.starttime)

    l_gjp = [
        (
            "gmji",
            UTCDateTime(gmji[0].stats.starttime).timestamp,
            gmji[0].stats.sampling_rate,
            gmji[0].stats.channel,
            gmji[0].stats.starttime,
            gmji[0].stats.npts,
        ),
        (
            "jagi",
            UTCDateTime(jagi[0].stats.starttime).timestamp,
            jagi[0].stats.sampling_rate,
            jagi[0].stats.channel,
            jagi[0].stats.starttime,
            gmji[0].stats.npts,
        ),
        (
            "pwji",
            UTCDateTime(pwji[0].stats.starttime).timestamp,
            pwji[0].stats.sampling_rate,
            pwji[0].stats.channel,
            pwji[0].stats.starttime,
            gmji[0].stats.npts,
        ),
    ]
    l_gjp = sorted(l_gjp, key=lambda a: a[1])
    print(l_gjp)
    l_diff = []
    l_diff.append(
        (
            l_gjp[2][0],
            int(np.around((l_gjp[2][1] - l_gjp[0][1]) * l_gjp[0][2])),
            l_gjp[2][3],
        )
    )
    l_diff.append(
        (
            l_gjp[1][0],
            int(np.around((l_gjp[1][1] - l_gjp[0][1]) * l_gjp[1][2])),
            l_gjp[1][3],
        )
    )
    l_diff.append((l_gjp[0][0], 0, l_gjp[0][3]))
    data_first = l_gjp[0][4]
    # npts_first = l_gjp[0][5]
    l_diff.sort()
    print(l_diff)

    return l_diff, data_first


def interpolate(lst, fi):
    i, f = (
        int(fi // 1),
        fi % 1,
    )  # Split floating-point index into whole & fractional parts.
    j = i + 1 if f > 0 else i  # Avoid index error.
    return (1 - f) * lst[i] + f * lst[j]


def letInterpolate(inp, new_len):
    delta = (len(inp) - 1) / (new_len - 1)
    outp = [interpolate(inp, i * delta) for i in range(new_len)]
    return outp


def get_current_utc_datetime():
    return datetime.now(pytz.utc).strftime("%Y-%m-%d %H:%M:%S.%f")


def nearest_datetime_rounded(
    datetime: str | datetime | UTCDateTime, step_in_micros: int = 40000
):
    if type(datetime) == str:
        datetime = parser.parse(datetime)
    if type(datetime) == UTCDateTime:
        datetime = datetime.datetime
    microsecond = datetime.time().microsecond
    remainder = microsecond % step_in_micros
    rounded = datetime
    if remainder < (step_in_micros / 2):
        rounded -= timedelta(microseconds=remainder)
    else:
        rounded += timedelta(microseconds=(step_in_micros - remainder))
    return rounded


def fill_empty_timestamp(
    start: datetime,
    end: datetime,
    data: pd.DataFrame,
    data_key: str = "_time",
    step_in_micros: int = 40000,
) -> pd.DataFrame:
    diff = end - start
    diff_in_micros = (diff.seconds * 10**6) + (diff.microseconds)
    time_list = set(
        [
            (start + timedelta(microseconds=i))
            for i in range(0, diff_in_micros, step_in_micros)
        ]
    )
    time_in_data = set([_time for _time in data[data_key].to_list()])
    time_not_in_data = pd.DataFrame(list(time_list - time_in_data), columns=[data_key])
    extended_data = pd.concat([data, time_not_in_data], ignore_index=True)
    return extended_data.sort_values(by=["_time"], ignore_index=True).fillna(0)

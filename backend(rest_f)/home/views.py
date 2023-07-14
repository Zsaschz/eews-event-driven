import os
import numpy as np

from django.http.response import JsonResponse
from django.shortcuts import render
from math import ceil
from obspy import read
from obspy import UTCDateTime
from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

from .models import *
from .serializer import *
from .timer import *
from .helper_functions import *
from .firebase_config import *
# from .plotting import *
# from .crop_plot3_final import *
# from .model_prediction import *

dir = "storage/"
            
class ReactView(APIView):
    
    serializer_class = MseedSerializer
    
    def get(self, request):
        mseed = Mseed.objects.all()
        serializer = MseedSerializer(mseed, many=True)
        return Response(serializer.data)
    
    def create(self, request):
        pass
    # def posts(self, request):
    #     datas = read("storage/Jatim/20090118_064750.mseed")
    
    def post(self, request):
        mseeds = ["20090118_064750", "20100208_112154", "20090119_211141", "20120915_163224"]
        for mseed in mseeds:
            datas = read(f"storage/Jatim/{mseed}.mseed")
            serializer = MseedSerializer(data=request.data)
            if serializer.is_valid(raise_exception=True):
                serializer.validated_data['name'] = mseed
                gmji = []
                jagi = []
                pwji = []
                for i in datas:
                    if i.stats.station == "GMJI":
                        gmji.append(i)
                    elif i.stats.station == "JAGI":
                        jagi.append(i)
                    elif i.stats.station == "PWJI":
                        pwji.append(i)
                l_diff, first_starttime = add_null_station(gmji, jagi, pwji)
                for detail in datas:
                    if (detail.stats.station == "GMJI") or (detail.stats.station == "JAGI") or (detail.stats.station == "PWJI"):
                        datas = {}
                        fs = detail.stats.sampling_rate
                        lowcut = 1.0
                        highcut = 5.0
                        order = 5
                        datas['network'] = detail.stats.network
                        datas['station'] = detail.stats.station
                        datas['channel'] = detail.stats.channel
                        datas['location'] = detail.stats.location
                        datas['starttime'] = str(detail.stats.starttime)
                        datas['endtime'] = str(detail.stats.endtime)
                        datas['delta'] = detail.stats.delta
                        datas['npts'] = detail.stats.npts
                        datas['calib'] = detail.stats.calib
                        data_before = detail.data
                        data_processed = butter_bandpass_filter(data_before, lowcut, highcut, fs,order)
                        data_processed = normalizations(data_processed)
                        data_to = list(data_processed)
                        datas['data'] = array_to_str_limit_dec(data_to)
                        data_to = letInterpolate(data_to, int(ceil(len(data_to)*25/detail.stats.sampling_rate)))
                        if detail.stats.station == "GMJI":
                            print(l_diff[0][2])
                            if detail.stats.channel == l_diff[0][2]:
                                print('masuk')
                                if l_diff[0][1] != 0:
                                    for i in range(l_diff[0][1]):
                                        data_to.insert(0, None)
                                    print(data_processed[0:100])
                        elif detail.stats.station == "JAGI":
                            if detail.stats.channel == l_diff[1][2]:
                                if l_diff[1][1] != 0:
                                    for i in range(l_diff[1][1]):
                                        data_to.insert(0, None)
                        elif detail.stats.station == "PWJI":
                            if detail.stats.channel == l_diff[2][2]:
                                if l_diff[2][1] != 0:
                                    for i in range(l_diff[2][1]):
                                        data_to.insert(0, None)
                        datas['sampling_rate'] = 25.0
                        datas['data_interpolated'] = array_to_str_limit_dec(data_to)
                        datas['starttime_station'] = str(first_starttime)
                        # datas['first_npts'] = first_npts
                        # print(data_processed)
                        serializer.validated_data['traces'].append(datas)
                serializer.save()
                dbs.child(mseed).set(serializer.data)

        return Response(serializer.data)
    
    def update(self, request, name_mseed):
        pass
    
    def get_p(self, request, name_mseed):
        get_p_arrival(name_mseed)
        return Response()
    
    def delete(self, request):
        Mseed.objects.all().delete()
        # for data in Mseed.objects.all():
        #     if data.name == "20120915_163224":
        #         data.delete()
        return Response()
    
    def mseed_dir(self, request):
        lst_jatim = os.listdir(dir+'Jatim')
        lst_jatimdangkal = os.listdir(dir+'Jatimdangkal')
        for file_jatim in lst_jatim:
            datas = read(dir+'Jatim/'+file_jatim)
            serializer = MseedSerializer(data=request.data)
            if serializer.is_valid(raise_exception=True):
                serializer.validated_data['name'] = file_jatim.replace('.mseed', '')
                for detail in datas:
                    if (detail.stats.station == "GMJI") or (detail.stats.station == "JAGI") or (detail.stats.station == "PWJI"):
                        datas = {}
                        fs = detail.stats.sampling_rate
                        lowcut = 1.0
                        highcut = 5.0
                        order = 5
                        datas['network'] = detail.stats.network
                        datas['station'] = detail.stats.station
                        datas['channel'] = detail.stats.channel
                        datas['starttime'] = str(detail.stats.starttime)
                        datas['endtime'] = str(detail.stats.endtime)
                        datas['sampling_rate'] = detail.stats.sampling_rate
                        datas['delta'] = detail.stats.delta
                        datas['npts'] = detail.stats.npts
                        datas['calib'] = detail.stats.calib
                        data_processed = butter_bandpass_filter(detail.data, lowcut, highcut, fs,order)
                        data_processed = normalizations(data_processed)
                        datas['data'] = array_to_str_limit_dec(data_processed)
                        # print(data_processed)
                        serializer.validated_data['traces'].append(datas)
                serializer.save()
                dbs.set(serializer.data)
        
class GMJIView(APIView):
    serializer_class = MseedSerializer
    def get(self, request, name):
        # return Response(get_GMJI_data(name))
        return Response(get_GMJI_data_firebase(name))

class JAGIView(APIView):
    serializer_class = MseedSerializer
    def get(self, request, name):
        return Response(get_JAGI_data_firebase(name))
    
class PWJIView(APIView):
    serializer_class = MseedSerializer
    def get(self, request, name):
        return Response(get_PWJI_data_firebase(name))


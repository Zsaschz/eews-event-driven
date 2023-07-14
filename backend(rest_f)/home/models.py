from django.db import models
import firebase_admin
from firebase_admin import credentials, db
# import pyrebase

# from jsonfield import JSONField

paths = 'creds/firebase_admin_creds.json'
creds = credentials.Certificate(paths)

firebase_admin.initialize_app(creds, {
  'databaseURL': "https://eews-eventdriven-default-rtdb.asia-southeast1.firebasedatabase.app/"
})

dbs = db.reference('/mseed_data')

def get_GMJI_data(name):
    lst = []
    for data in Mseed.objects.all():
        if data.name == name:
            for datas in data.traces:
                if datas['station'] == "GMJI":
                    lst.append(datas)
    return lst

def get_JAGI_data(name):
    lst = []
    for data in Mseed.objects.all():
        if data.name == name:
            for datas in data.traces:
                if datas['station'] == "JAGI":
                    lst.append(datas)
    return lst


def get_PWJI_data(name):
    lst = []
    for data in Mseed.objects.all():
        if data.name == name:
            for datas in data.traces:
                if datas['station'] == "PWJI":
                    lst.append(datas)
    return lst

def delete(name):
    for data in Mseed.objects.all():
        if data.name == name:
            data.delete()

def get_GMJI_data_firebase(name):
    lst = []
    store = []
    for data in dbs.child(name).child('traces').get():
        store.append(data)
         
    for datas in store:
        if datas['station'] == "GMJI":
            lst.append(datas)
    return lst

def get_JAGI_data_firebase(name):
    lst = []
    store = []
    for data in dbs.child(name).child('traces').get():
        store.append(data)     
           
    for datas in store:
        if datas['station'] == "JAGI":
            lst.append(datas)
    return lst

def get_PWJI_data_firebase(name):
    lst = []
    store = []
    for data in dbs.child(name).child('traces').get():
        store.append(data)
        
    for datas in store:
        if datas['station'] == "PWJI":
            lst.append(datas)
    return lst

def get_p_arrival(name):
    data = {}
    p_arrival = ""
    data['p_arrival'] = p_arrival
    db.child('prediction/'+name).set(data)

class Mseed(models.Model):
    name = models.CharField(null=True, max_length=45)
    traces = models.TextField(blank=True)

class Mseed_Data(models.Model): 
    network = models.CharField(null=True, max_length=30)
    station = models.CharField(null=True, max_length=30)
    location = models.CharField(null=True, max_length=150)
    channel = models.CharField(null=True, max_length=40)
    starttime = models.CharField(null=True, max_length=50)
    endtime = models.CharField(null=True, max_length=50)
    sampling_rate = models.FloatField(null=True)
    delta = models.FloatField(null=True)
    npts = models.IntegerField(null=True)
    calib = models.FloatField(null=True)
    data = models.TextField(null=True, max_length=165535)
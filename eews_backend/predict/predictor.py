from tensorflow.keras.models import load_model
import numpy as np
import pandas as pd
import pickle
import threading

class Predictor():
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(Predictor, cls).__new__(cls)
        
        return cls._instance
    
    def __init__(self):
        # self.model = pickle.load(open('predict\model\model.pkl', 'rb'))
        self.model = load_model('predict\model\model.h5')

    def predict(self, data):
        predictions = self.model.predict(np.array(data), batch_size=4)
        result = pd.DataFrame(columns=['lat','long','depth','magnitude','time'])

        for prediction, col_result in zip(np.array(predictions), ['lat','long','depth','magnitude','time']):
            result[col_result] = prediction.squeeze()
        
        return result

from dotenv import load_dotenv
from motor import motor_asyncio
from typing import Optional
from pymongo import MongoClient
import os

load_dotenv()

DEFAULT_MONGO_DATABASE = "db"
MONGO_URL = os.getenv("MONGO_URL")
MONGO_DATABASE = (
    os.getenv("MONGO_DATABASE")
    if os.getenv("MONGO_DATABASE")
    else DEFAULT_MONGO_DATABASE
)

<<<<<<< HEAD
client = motor_asyncio.AsyncIOMotorClient(MONGO_URL)
db = client[MONGO_DATABASE]
        
=======

>>>>>>> 1b1693c80444dd866bde20d9ac203407977458aa
def mongo_client():
    mongo_url = MONGO_URL
    mongo_db = MONGO_DATABASE
    client = motor_asyncio.AsyncIOMotorClient(mongo_url)
    db = client[mongo_db]
    return client, db

def mongo_client_sync():
    mongo_url = MONGO_URL
    mongo_db = MONGO_DATABASE
    client = MongoClient(mongo_url)
    db = client[mongo_db]
    return client, db

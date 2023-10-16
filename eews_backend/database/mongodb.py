from dotenv import load_dotenv
from motor import motor_asyncio
from typing import Optional
import os

load_dotenv()

DEFAULT_MONGO_DATABASE = "db"
MONGO_URL = os.getenv("MONGO_URL")
MONGO_DATABASE = (
    os.getenv("MONGO_DATABASE")
    if os.getenv("MONGO_DATABASE")
    else DEFAULT_MONGO_DATABASE
)


def mongo_client():
    mongo_url = MONGO_URL
    mongo_db = MONGO_DATABASE
    MONGO_USERNAME = os.getenv("MONGO_USERNAME")
    MONGO_PASSWORD =  os.getenv("MONGO_PASSWORD")
    client = motor_asyncio.AsyncIOMotorClient(mongo_url, username=MONGO_USERNAME, password=MONGO_PASSWORD)
    db = client[mongo_db]
    return client, db

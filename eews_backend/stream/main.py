from dotenv import load_dotenv
from collections.abc import Callable
from confluent_kafka import Consumer, Producer
from confluent_kafka import KafkaError
from confluent_kafka import Message
from utils.helper_functions import get_current_utc_datetime
from typing import Any, Dict
from typing import Optional
import logging
import os
import json

load_dotenv()

BOOTSTRAP_SERVER = os.getenv("BOOTSTRAP_SERVER")
if not BOOTSTRAP_SERVER:
    raise Exception("BOOTSTRAP_SERVER env is required")

DEFAULT_CONSUMER_CONFIG = {
    "bootstrap.servers": BOOTSTRAP_SERVER,
    "default.topic.config": {
        "auto.offset.reset": "latest",  # if there's no initial offset, use latest
    },
}

DEFAULT_PRODUCER_CONFIG = {
    "bootstrap.servers": BOOTSTRAP_SERVER,
    # 'compression.type': 'lz4',
    # 'linger.ms': 100,
    # 'batch.size': 131072, # 128 KB
}

logging.debug(f"Consumer config: {DEFAULT_CONSUMER_CONFIG}")
logging.info(f"Producer config: {DEFAULT_PRODUCER_CONFIG}")


def serialize_json(obj):
    return json.dumps(obj).encode("utf-8")


def deserialize_json(obj: bytes):
    decoded = obj.decode("utf-8")
    try:
        return json.loads(decoded)

    except:
        return decoded


class KafkaProducer:
    def __init__(
        self,
        topic_name: str,
        value_serializer: Optional[Callable[[object], bytes]] = None,
        extra_config: Optional[Dict] = None,
    ):
        logging.debug("Create producer")

        if extra_config is None:
            extra_config = {}

        self.producer = Producer({**DEFAULT_PRODUCER_CONFIG, **extra_config})
        self.topic_name = topic_name

        self.value_serializer = value_serializer
        if self.value_serializer is None:
            self.value_serializer = serialize_json

        logging.debug("Finish creating producer")

    def produce_message(
        self,
        value: object,
        key: Optional[Any] = None,
        callback_function: Optional[Callable[[str, str], None]] = None,
    ):
        value[f"injected_to_{self.topic_name}_at"] = get_current_utc_datetime()

        self.producer.produce(
            topic=self.topic_name,
            value=self.value_serializer(value),
            on_delivery=self.get_on_delivery_function(callback_function),
            key=key,
        )

        self.producer.poll(0)

    def log_on_kafka_message_delivery(self, error: Optional[str], message: str):
        if error is not None:
            logging.error(
                f"Failed to produce message: {message.value()}, topic: {self.topic_name} error: {error}"
            )

        else:
            logging.debug(
                f"Successfully produced message: {message.value()}, topic: {self.topic_name}"
            )

    def get_on_delivery_function(
        self, extra_function: Optional[Callable[[str, str], None]]
    ):
        if extra_function is None:
            return self.log_on_kafka_message_delivery

        return lambda error, message: (
            self.log_on_kafka_message_delivery(error, message),
            extra_function(error, message),
        )


class KafkaConsumer:
    def __init__(
        self,
        topic_name: str,
        group_id: str,
        extra_config: Dict,
        value_deserializer: Optional[Callable[[object], bytes]] = None,
    ):
        self.consumer = Consumer(
            {
                **DEFAULT_CONSUMER_CONFIG,
                "group.id": group_id,
                **extra_config,
            }
        )
        self.consumer.subscribe([topic_name])

        self.value_deserializer = value_deserializer
        if self.value_deserializer is None:
            self.value_deserializer = deserialize_json

    def consume(
        self,
        on_message: Callable[[object], None],
        on_error: Callable[[str], None],
        on_wait: Callable[[], None] = None,
    ):
        try:
            while True:
                msg: Message = self.consumer.poll(1.0)

                if msg is None:
                    if on_wait is not None:
                        on_wait()

                    continue

                if msg.error():
                    on_error(msg.error())
                    continue

                on_message(self.value_deserializer(msg.value()))

        except Exception as e:
            logging.error("Error: %s", e)
            self.consumer.close()

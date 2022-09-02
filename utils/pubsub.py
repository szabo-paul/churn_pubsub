from google.cloud import pubsub_v1
from google.oauth2 import service_account
import os
from settings import PS_SERVICE_ACCOUNT, GCP_PROJECT, TOPIC_ID
from google.cloud.pubsub_v1.types import (
    LimitExceededBehavior,
    PublisherOptions,
    PublishFlowControl,
)
from concurrent import futures

os.environ['PUBSUB_APPLICATION_CREDENTIALS'] = PS_SERVICE_ACCOUNT


def ps_authenticate(file_name: str = os.environ['PUBSUB_APPLICATION_CREDENTIALS']) -> service_account.Credentials:
    credentials = service_account.Credentials.from_service_account_file(file_name)
    return credentials


def send_message(msg):
    msg = msg.encode("utf-8")
    credentials = ps_authenticate()

    publisher = pubsub_v1.PublisherClient(credentials=credentials)
    topic_path = publisher.topic_path(GCP_PROJECT, TOPIC_ID)
    future = publisher.publish(topic_path, msg)

    return future

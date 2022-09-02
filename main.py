from utils import bigquery
import pandas as pd
from google.cloud import pubsub_v1
from google.oauth2 import service_account
import os
from settings import PS_SERVICE_ACCOUNT_PROD, GCP_PROJECT_PROD, TOPIC_ID
from google.cloud.pubsub_v1.types import (
    LimitExceededBehavior,
    PublisherOptions,
    PublishFlowControl,
)
from concurrent import futures

fake_customers = pd.read_excel('Hotfix2.xls', converters={'Customer_Number__c': str})

sql = """
    SELECT * 
    FROM salesforce_input.CHURN_CUSTOMER_AGG
    WHERE Customer_Number__C IN ("{}")
""".format('", "'.join(map(str, fake_customers['Customer_Number__c'].to_list())))

churn_customer_result = bigquery.query_run(sql)
# churn_customer_result = churn_customer_result.append([churn_customer_result] * 9999, ignore_index=True)
lines = churn_customer_result.to_json(orient='records', lines=True)

credentials = service_account.Credentials.from_service_account_file(PS_SERVICE_ACCOUNT_PROD)

flow_control_settings = PublishFlowControl(
    message_limit=100,  # 100 messages
    byte_limit=10 * 1024 * 1024,  # 10 MiB
    limit_exceeded_behavior=LimitExceededBehavior.BLOCK,
)
publisher = pubsub_v1.PublisherClient(credentials=credentials,
                                      publisher_options=PublisherOptions(flow_control=flow_control_settings))

topic_path = publisher.topic_path(GCP_PROJECT_PROD, TOPIC_ID)
publish_futures = []


# Resolve the publish future in a separate thread.
def callback(publish_future: pubsub_v1.publisher.futures.Future) -> None:
    message_id = publish_future.result()
    print(message_id)


for line in lines.splitlines():
    msg = line.encode("utf-8")
    future = publisher.publish(topic_path, msg)
    future.add_done_callback(callback)
    publish_futures.append(future)

futures.wait(publish_futures, return_when=futures.ALL_COMPLETED)

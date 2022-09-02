import os
from google.oauth2 import service_account
from google.cloud import bigquery

from settings import BQ_SERVICE_ACCOUNT, GCP_PROJECT, BQ_DATASET

os.environ['BIGQUERY_APPLICATION_CREDENTIALS'] = BQ_SERVICE_ACCOUNT


def bq_authenticate(file_name: str = os.environ['BIGQUERY_APPLICATION_CREDENTIALS']) -> service_account.Credentials:
    credentials = service_account.Credentials.from_service_account_file(file_name)
    return credentials


def query_run(sql: str):
    bq_credentials = bq_authenticate()
    bq_client = bigquery.Client(credentials=bq_credentials)
    query = bq_client.query(sql)
    return query.result().to_dataframe()



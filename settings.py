import os

BASE_DIR = os.path.dirname(os.path.realpath(__file__))

# Google Cloud Settings
GCP_PROJECT = 'vasistas-dev-vestal15'
GCP_PROJECT_PROD = 'vasistas-prod-section1'
BQ_DATASET = 'PRZ_ART_CHURN'
TOPIC_ID = 'churn-pubsub'
BQ_SERVICE_ACCOUNT = os.path.join(BASE_DIR, 'secret_files/bq_sa.json')
PS_SERVICE_ACCOUNT = os.path.join(BASE_DIR, 'secret_files/ps_sa.json')
PS_SERVICE_ACCOUNT_PROD = os.path.join(BASE_DIR, 'secret_files/ps_sa_prod.json')

import datetime
import os

from dotenv import load_dotenv
from kombu import Queue, Exchange

load_dotenv()

# ######################################################
# # App
# ######################################################
BOOKS_ETL_READER_BATCH_SIZE = int(os.getenv("BOOKS_ETL_READER_BATCH_SIZE"))
BOOKS_ETL_READER_TIMEOUT_BY_EMPTY = int(os.getenv("BOOKS_ETL_READER_TIMEOUT_BY_EMPTY"))
BOOKS_ETL_DB_STATE_KEY = os.getenv("BOOKS_ETL_DB_STATE_KEY")

KVDB_HOST = os.getenv("KVDB_HOST")
KVDB_PORT = int(os.getenv("KVDB_PORT"))
KVDB_USERNAME = os.getenv("KVDB_USERNAME")
KVDB_PASSWORD = os.getenv("KVDB_PASSWORD")

# ######################################################
# # External Integrations
# ######################################################
EXTERNAL_AMQP_HOST = os.getenv("EXTERNAL_AMQP_HOST")
EXTERNAL_AMQP_PORT = int(os.getenv("EXTERNAL_AMQP_PORT"))
EXTERNAL_AMQP_USER = os.getenv("EXTERNAL_AMQP_USER")
EXTERNAL_AMQP_PASSWORD = os.getenv("EXTERNAL_AMQP_PASSWORD")

EXTERNAL_PG_HOST = os.getenv("EXTERNAL_PG_HOST")
EXTERNAL_PG_PORT = int(os.getenv("EXTERNAL_PG_PORT"))
EXTERNAL_PG_DB_NAME = os.getenv("EXTERNAL_PG_DB_NAME")
EXTERNAL_PG_USER = os.getenv("EXTERNAL_PG_USER")
EXTERNAL_PG_PASSWORD = os.getenv("EXTERNAL_PG_PASSWORD")

EXTERNAL_ELASTICSEARCH_SCHEME = os.getenv("EXTERNAL_ELASTICSEARCH_SCHEME")
EXTERNAL_ELASTICSEARCH_HOST = os.getenv("EXTERNAL_ELASTICSEARCH_HOST")
EXTERNAL_ELASTICSEARCH_PORT = int(os.getenv("EXTERNAL_ELASTICSEARCH_PORT"))

# ######################################################
# # Celery
# ######################################################
CELERY_broker = os.getenv("CELERY_BROKER_URL")
CELERY_result_backend = os.getenv("CELERY_RESULT_BACKEND")
CELERY_broker_connection_retry_on_startup = bool(int(os.getenv("CELERY_BROKER_CONNECTION_RETRY_ON_STARTUP")))
CELERY_RESULT_EXPIRES = datetime.timedelta(minutes=int(os.getenv("CELERY_RESULT_EXPIRES_MINUTES")))
CELERY_accept_content = [
    "application/json"
]
CELERY_result_serializer = "json"
CELERY_task_serializer = "json"
CELERY_WORKER_PREFETCH_MULTIPLIER = 1

CELERY_TASK_ALWAYS_EAGER = bool(int(os.getenv("CELERY_TASK_ALWAYS_EAGER")))

CELERY_TASK_QUEUES = (
    Queue('transformers', Exchange('transformers'), routing_key='transformers'),
    Queue('loaders', Exchange('loaders'), routing_key='loaders'),
)

CELERY_TASK_ROUTES = {
    'transformers.*': {'queue': 'transformers'},
    'loaders.*': {'queue': 'loaders'},
}

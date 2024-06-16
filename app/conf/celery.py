from celery import Celery

import internal
from conf import settings

task_packages = [
    internal
]

celery_app = Celery(
    "conf",
    broker_connection_retry_on_startup=settings.CELERY_broker_connection_retry_on_startup
)

celery_app.config_from_object('conf.settings', namespace='CELERY_')
celery_app.autodiscover_tasks([
    package.__name__
    for package in task_packages
])

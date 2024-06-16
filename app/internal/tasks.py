from celery import shared_task

from internal.etl.executors import ETLTransformerExecutor, ETLLoaderExecutor
from internal.etl.loaders import BooksElasticsearchLoader, BooksSubscriptionLoader
from internal.etl.transformers import BooksElasticsearchTransformer, BooksSubscriptionTransformer
from internal.etl.utils import make_pipeline


@shared_task(
    name="transformers.task_es_transformer"
)
def task_es_transformer(data: dict):
    executor = ETLTransformerExecutor(
        transformer=BooksElasticsearchTransformer(data=data),
        pipeline=make_pipeline([task_es_loader])
    )
    executor.run()


@shared_task(
    name="transformers.task_subscription_transformer"
)
def task_subscription_transformer(data: dict):
    executor = ETLTransformerExecutor(
        transformer=BooksSubscriptionTransformer(data=data),
        pipeline=make_pipeline([task_subscription_loader])
    )
    executor.run()


@shared_task(
    name="loaders.task_es_loader"
)
def task_es_loader(data: dict):
    executor = ETLLoaderExecutor(
        loader=BooksElasticsearchLoader(data=data)
    )
    executor.run()


@shared_task(
    name="loaders.task_subscription_loader"
)
def task_subscription_loader(data: dict):
    executor = ETLLoaderExecutor(
        loader=BooksSubscriptionLoader(data=data)
    )
    executor.run()

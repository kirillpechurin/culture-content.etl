from conf import settings
from internal.etl.executors import ETLReaderExecutor
from internal.etl.readers import BooksDBReader
from internal.tasks import (
    task_es_transformer,
    task_subscription_transformer
)
from .states import ETLState
from .utils import make_pipeline


class BooksETLProcessor:

    @classmethod
    def run(cls):
        executor = ETLReaderExecutor(
            reader=BooksDBReader(),
            pipeline=make_pipeline([
                task_es_transformer,
                task_subscription_transformer,
            ]),
            state=ETLState(key=settings.BOOKS_ETL_DB_STATE_KEY)
        )
        executor.run()

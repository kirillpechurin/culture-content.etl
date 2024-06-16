import time

from conf import settings
from internal.utils import str_to_datetime, datetime_to_str
from .loaders import ETLLoaderInterface
from .readers import ETLReaderInterface
from .states import ETLState
from .transformers import ETLTransformerInterface


class ETLReaderExecutor:

    def __init__(self,
                 reader: ETLReaderInterface,
                 pipeline: callable,
                 state: ETLState):
        self._reader = reader
        self._pipeline = pipeline
        self._state = state

    def run(self):
        try:
            while True:
                state = self._state.payload
                books_count = self._reader.get_count(
                    start=str_to_datetime(state["mark"])
                )
                if books_count == 0:
                    time.sleep(settings.BOOKS_ETL_READER_TIMEOUT_BY_EMPTY)
                    continue

                batch_size = settings.BOOKS_ETL_READER_BATCH_SIZE

                for batch_number in range(0, books_count // batch_size + 1):
                    books_batch = self._reader.get(str_to_datetime(state["mark"]), limit=batch_size * (batch_number + 1))

                    self._pipeline.send(books_batch)

                    state["mark"] = datetime_to_str(books_batch["books"][-1]["created_at"])
                    self._state.payload = state
        except Exception as ex:
            raise ex
        finally:
            self._reader.close()


class ETLTransformerExecutor:

    def __init__(self,
                 transformer: ETLTransformerInterface,
                 pipeline: callable):
        self._transformer = transformer
        self._pipeline = pipeline

    def run(self):
        data = self._transformer.run()
        self._pipeline.send(data)


class ETLLoaderExecutor:

    def __init__(self,
                 loader: ETLLoaderInterface):
        self._loader = loader

    def run(self):
        self._loader.run()

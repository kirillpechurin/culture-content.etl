import math
import time
from typing import Generator

from internal.utils import str_to_datetime, datetime_to_str
from .loaders import ETLLoaderInterface
from .readers import ETLReaderInterface
from .states import ETLState
from .transformers import ETLTransformerInterface


class ETLReaderExecutor:

    def __init__(self,
                 reader: ETLReaderInterface,
                 pipeline: Generator,
                 state: ETLState,
                 batch_size: int,
                 sleep_timeout: int):
        self._reader = reader
        self._pipeline = pipeline
        self._state = state
        self._batch_size = batch_size
        self._sleep_timeout = sleep_timeout

    def _check_updates(self, state: dict) -> int:
        count = self._reader.get_count(
            start=str_to_datetime(state["mark"])
        )
        return count

    def _execute_updates(self, state: dict, count: int) -> None:
        for batch_number in range(0, math.ceil(count / self._batch_size)):
            batch, last_created_at = self._reader.get(
                str_to_datetime(state["mark"]),
                limit=self._batch_size
            )

            self._pipeline.send(batch)

            state["mark"] = datetime_to_str(last_created_at)
            self._state.payload = state

    def _get_run(self):
        # For mocks in tests.
        return True

    def run(self):
        try:
            while self._get_run():
                state = self._state.payload
                count = self._check_updates(state)
                if count == 0:
                    time.sleep(self._sleep_timeout)

                self._execute_updates(state, count)
        except Exception as ex:
            raise ex
        finally:
            self._reader.close()


class ETLTransformerExecutor:

    def __init__(self,
                 transformer: ETLTransformerInterface,
                 pipeline: Generator):
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

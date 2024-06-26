import inspect
from unittest.mock import patch

from conf import settings
from internal.etl.readers import BooksDBReader


class MockPGSQLAPI:

    def get_cursor(self):
        return None


class TestCaseUnitBooksETLProcessor:

    def test_called_executor(self):
        with (
            patch("internal.etl.executors.ETLReaderExecutor") as mock,
            patch("internal.etl.states.ETLState") as mock_state,
            patch("internal.etl.readers.BooksDBReader._get_api") as mock_reader,
        ):
            from internal.etl.processors import BooksETLProcessor
            mock_state.return_value = "test-state"
            mock_reader.return_value = MockPGSQLAPI()

            BooksETLProcessor.run()

            mock.assert_called_once()
            mock_call = mock.mock_calls[0]
            assert len(mock_call.args) == 0
            assert len(mock_call.kwargs) == 5

            assert isinstance(mock_call.kwargs["reader"], BooksDBReader)
            assert mock_call.kwargs["state"] == "test-state"

            assert inspect.getgeneratorstate(mock_call.kwargs["pipeline"]) == "GEN_SUSPENDED"

            assert mock_call.kwargs["batch_size"] == settings.BOOKS_ETL_READER_BATCH_SIZE
            assert mock_call.kwargs["sleep_timeout"] == settings.BOOKS_ETL_READER_TIMEOUT_BY_EMPTY

            mock_state.assert_called_once_with(key=settings.BOOKS_ETL_DB_STATE_KEY)

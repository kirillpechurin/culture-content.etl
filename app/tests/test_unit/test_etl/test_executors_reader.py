import copy
import datetime
from collections import defaultdict
from unittest.mock import patch, call

import pytest

from internal.etl.executors import ETLReaderExecutor
from internal.etl.readers import ETLReaderInterface
from internal.utils import datetime_to_str, str_to_datetime


class MockETLReader:

    def __init__(self):
        self.methods = defaultdict(list)

    def get(self, *args, **kwargs):
        self.methods["get"].append((args, kwargs))
        return [], None

    def close(self, *args, **kwargs):
        self.methods["close"].append((args, kwargs))


class MockETLPipeline:

    def __init__(self):
        self.methods = defaultdict(list)

    def send(self, *args, **kwargs):
        self.methods["send"].append((args, kwargs))


class MockETLState:

    def __init__(self, payload: dict = None):
        self._payload = payload if payload else {"mark": None}
        self.methods = defaultdict(list)

    @property
    def payload(self):
        self.methods["payload_getter"].append(self._payload)
        return self._payload

    @payload.setter
    def payload(self, value):
        self.methods["payload_setter"].append(copy.deepcopy(value))
        self._payload = value


class TestCaseUnitETLExecutorsReader:

    def setup_method(self):
        patch.stopall()

    @property
    def default_state_mark(self):
        return datetime.datetime(2024, 6, 15, 10, 11, 12, 1234, tzinfo=datetime.timezone.utc)

    def test_private_check_updates_get_count_called(self):
        with patch("internal.etl.readers.ETLReaderInterface.get_count") as mock:
            mock.return_value = None

            executor = ETLReaderExecutor(
                reader=ETLReaderInterface(),
                pipeline=MockETLPipeline(),
                state=MockETLState(),
                batch_size=100,
                sleep_timeout=1
            )

            result = executor._check_updates({"mark": self.default_state_mark})
            assert result is None

            mock.assert_called_once()
            assert mock.mock_calls[0].kwargs["start"] == self.default_state_mark

    def test_private_check_updates_get_count_empty(self):
        with patch("internal.etl.readers.ETLReaderInterface.get_count") as mock:
            mock.return_value = 0
            executor = ETLReaderExecutor(
                reader=ETLReaderInterface(),
                pipeline=MockETLPipeline(),
                state=MockETLState(),
                batch_size=100,
                sleep_timeout=1
            )

            result = executor._check_updates({"mark": self.default_state_mark})
            assert result == 0

            mock.assert_called_once()
            assert mock.mock_calls[0].kwargs["start"] == self.default_state_mark

    def test_private_execute_updates_get_called(self):
        reader_patcher = patch("internal.etl.readers.ETLReaderInterface.get")
        reader_mock = reader_patcher.start()
        reader_mock.return_value = [], None

        executor = ETLReaderExecutor(
            reader=ETLReaderInterface(),
            pipeline=MockETLPipeline(),
            state=MockETLState({"mark": self.default_state_mark}),
            batch_size=100,
            sleep_timeout=1
        )
        executor._execute_updates(executor._state.payload, 5)

        print(reader_mock.mock_calls)
        reader_mock.assert_called_once()
        assert reader_mock.mock_calls[0].args[0] == self.default_state_mark
        assert reader_mock.mock_calls[0].kwargs["limit"] == 100

    def test_private_execute_updates_get_called_with_state(self):
        reader_patcher = patch("internal.etl.readers.ETLReaderInterface.get")
        reader_mock = reader_patcher.start()
        reader_mock.return_value = [], None

        executor = ETLReaderExecutor(
            reader=ETLReaderInterface(),
            pipeline=MockETLPipeline(),
            state=MockETLState({"mark": self.default_state_mark}),
            batch_size=100,
            sleep_timeout=1
        )
        executor._execute_updates(executor._state.payload, 5)

        reader_mock.assert_called_once_with(
            self.default_state_mark,
            limit=100
        )

    def test_private_execute_updates_get_called_by_batch_number(self):
        reader_patcher = patch("internal.etl.readers.ETLReaderInterface.get")
        reader_mock = reader_patcher.start()
        reader_mock.return_value = [], self.default_state_mark

        executor = ETLReaderExecutor(
            reader=ETLReaderInterface(),
            pipeline=MockETLPipeline(),
            state=MockETLState({"mark": self.default_state_mark}),
            batch_size=100,
            sleep_timeout=1
        )
        executor._execute_updates(executor._state.payload, 500)

        assert len(reader_mock.mock_calls) == 5
        assert reader_mock.mock_calls[0] == call(self.default_state_mark, limit=100)
        assert reader_mock.mock_calls[1] == call(self.default_state_mark, limit=100)
        assert reader_mock.mock_calls[2] == call(self.default_state_mark, limit=100)
        assert reader_mock.mock_calls[3] == call(self.default_state_mark, limit=100)
        assert reader_mock.mock_calls[4] == call(self.default_state_mark, limit=100)

    def test_private_execute_updates_pipeline_check(self):
        reader_patcher = patch("internal.etl.readers.ETLReaderInterface.get")
        reader_mock = reader_patcher.start()
        reader_mock.return_value = [{
            "sample": "value"
        }, {
            "sample2": "value2"
        }], None

        executor = ETLReaderExecutor(
            reader=ETLReaderInterface(),
            pipeline=MockETLPipeline(),
            state=MockETLState({"mark": self.default_state_mark}),
            batch_size=100,
            sleep_timeout=1
        )

        executor._execute_updates(executor._state.payload, 5)

        mocked_pipeline = executor._pipeline
        print(mocked_pipeline.methods)
        assert len(mocked_pipeline.methods["send"]) == 1
        assert mocked_pipeline.methods["send"][0] == (([{
            "sample": "value"
        }, {
            "sample2": "value2"
        }],), {})

    def test_private_execute_updates_pipeline_check_by_batch_number(self):
        reader_patcher = patch("internal.etl.readers.ETLReaderInterface.get")
        reader_mock = reader_patcher.start()
        batches = []
        for batch_number in range(0, 5):
            batch = []
            for i in range(0, 10):
                batch.append({
                    f"sample{batch_number}{i}": f"value{batch_number}{i}"
                })
            batches.append(batch)
        reader_mock.side_effect = [(batch, None) for batch in batches]

        executor = ETLReaderExecutor(
            reader=ETLReaderInterface(),
            pipeline=MockETLPipeline(),
            state=MockETLState({"mark": self.default_state_mark}),
            batch_size=10,
            sleep_timeout=1
        )

        executor._execute_updates(executor._state.payload, 50)

        mocked_pipeline = executor._pipeline
        assert len(mocked_pipeline.methods["send"]) == 5
        assert mocked_pipeline.methods["send"][0] == ((batches[0],), {})
        assert mocked_pipeline.methods["send"][1] == ((batches[1],), {})
        assert mocked_pipeline.methods["send"][2] == ((batches[2],), {})
        assert mocked_pipeline.methods["send"][3] == ((batches[3],), {})
        assert mocked_pipeline.methods["send"][4] == ((batches[4],), {})

    def test_private_execute_updates_state_update(self):
        reader_patcher = patch("internal.etl.readers.ETLReaderInterface.get")
        reader_mock = reader_patcher.start()
        new_mark = self.default_state_mark + datetime.timedelta(days=1)
        reader_mock.return_value = [{
            "sample": "value"
        }, {
            "sample2": "value2"
        }], new_mark

        executor = ETLReaderExecutor(
            reader=ETLReaderInterface(),
            pipeline=MockETLPipeline(),
            state=MockETLState({"mark": self.default_state_mark}),
            batch_size=100,
            sleep_timeout=1
        )

        executor._execute_updates(executor._state.payload, 5)

        mocked_state = executor._state
        assert len(mocked_state.methods["payload_setter"]) == 1
        assert mocked_state.methods["payload_setter"][0] == {"mark": datetime_to_str(new_mark)}

    def test_private_execute_updates_state_update_by_batch_number(self):
        reader_patcher = patch("internal.etl.readers.ETLReaderInterface.get")
        reader_mock = reader_patcher.start()
        batches = []
        for batch_number in range(1, 6):
            batch = []
            for i in range(0, 10):
                batch.append({
                    f"sample{batch_number}{i}": f"value{batch_number}{i}"
                })
            new_mark = self.default_state_mark + datetime.timedelta(days=batch_number)
            batches.append((batch, new_mark))
        reader_mock.side_effect = [(batch, new_mark) for batch, new_mark in batches]

        executor = ETLReaderExecutor(
            reader=ETLReaderInterface(),
            pipeline=MockETLPipeline(),
            state=MockETLState({"mark": self.default_state_mark}),
            batch_size=10,
            sleep_timeout=1
        )

        executor._execute_updates(executor._state.payload, 50)

        mocked_state = executor._state
        assert len(mocked_state.methods["payload_setter"]) == 5
        assert mocked_state.methods["payload_setter"][0] == {"mark": datetime_to_str(batches[0][1])}
        assert mocked_state.methods["payload_setter"][1] == {"mark": datetime_to_str(batches[1][1])}
        assert mocked_state.methods["payload_setter"][2] == {"mark": datetime_to_str(batches[2][1])}
        assert mocked_state.methods["payload_setter"][3] == {"mark": datetime_to_str(batches[3][1])}
        assert mocked_state.methods["payload_setter"][4] == {"mark": datetime_to_str(batches[4][1])}

    def test_run_called_check_updates(self):
        with (
            patch("internal.etl.executors.ETLReaderExecutor._get_run") as mock_run,
            patch("internal.etl.executors.ETLReaderExecutor._check_updates") as mock_check
        ):
            mock_run.side_effect = [True, False]
            mock_check.return_value = 0

            executor = ETLReaderExecutor(
                reader=MockETLReader(),
                pipeline=MockETLPipeline(),
                state=MockETLState({"mark": self.default_state_mark}),
                batch_size=100,
                sleep_timeout=1
            )
            executor.run()

            mock_check.assert_called_once_with({"mark": self.default_state_mark})

    def test_run_empty_count(self):
        with (
            patch("internal.etl.executors.ETLReaderExecutor._get_run") as mock_run,
            patch("internal.etl.executors.ETLReaderExecutor._check_updates") as mock_check,
            patch("time.sleep") as mock_time_sleep
        ):
            mock_run.side_effect = [True, False]
            mock_check.return_value = 0

            executor = ETLReaderExecutor(
                reader=MockETLReader(),
                pipeline=MockETLPipeline(),
                state=MockETLState({"mark": self.default_state_mark}),
                batch_size=100,
                sleep_timeout=3
            )
            executor.run()

            mock_time_sleep.assert_called_once_with(3)

    def test_run_called_execute_updates(self):
        with (
            patch("internal.etl.executors.ETLReaderExecutor._get_run") as mock_run,
            patch("internal.etl.executors.ETLReaderExecutor._check_updates") as mock_check,
            patch("internal.etl.executors.ETLReaderExecutor._execute_updates") as mock_execute,
        ):
            mock_run.side_effect = [True, False]
            mock_check.return_value = 5

            executor = ETLReaderExecutor(
                reader=MockETLReader(),
                pipeline=MockETLPipeline(),
                state=MockETLState({"mark": self.default_state_mark}),
                batch_size=100,
                sleep_timeout=3
            )
            executor.run()

            mock_execute.assert_called_once_with({"mark": self.default_state_mark}, 5)

    def test_run_called_close_on_exception(self):
        class MockEtlReaderError(MockETLReader):
            def get_count(self, *args, **kwargs):
                raise TypeError

        with (
            patch("internal.etl.executors.ETLReaderExecutor._get_run") as mock_run,
            pytest.raises(TypeError)
        ):
            mock_run.side_effect = [True, False]

            executor = ETLReaderExecutor(
                reader=MockEtlReaderError(),
                pipeline=MockETLPipeline(),
                state=MockETLState({"mark": self.default_state_mark}),
                batch_size=100,
                sleep_timeout=3
            )
            executor.run()

        mocked_reader = executor._reader
        assert len(mocked_reader.methods["close"]) == 1
        assert mocked_reader.methods["close"][0] == ((), {})

    def test_get_run(self):
        executor = ETLReaderExecutor(
            reader=MockETLReader(),
            pipeline=MockETLPipeline(),
            state=MockETLState({"mark": self.default_state_mark}),
            batch_size=100,
            sleep_timeout=3
        )
        assert executor._get_run() is True

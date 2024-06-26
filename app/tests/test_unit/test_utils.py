import datetime
from inspect import getgeneratorstate

import pytest

from internal.utils import str_to_datetime, datetime_to_str, coroutine


class TestCaseUnitUtilsDates:

    @property
    def default(self):
        return datetime.datetime(2024, 6, 15, 10, 11, 12, 1234, tzinfo=datetime.timezone.utc)

    def test_compatible(self):
        source = self.default
        result = str_to_datetime(datetime_to_str(source))
        assert source == result

    def test_str_to_datetime_format(self):
        source = self.default.strftime("%Y-%m-%dT%H:%M:%S.%f%z")
        result = str_to_datetime(source)
        assert self.default == result

        source = self.default.strftime("%Y-%m-%dT%H:%M:%S.%f")
        with pytest.raises(ValueError):
            str_to_datetime(source)

    def test_str_to_datetime_null(self):
        result = str_to_datetime(None)
        assert result is None

    def test_str_to_datetime_already_datetime(self):
        source = self.default
        result = str_to_datetime(source)
        assert result == source

    def test_datetime_to_str_format(self):
        source = self.default
        result = datetime_to_str(source)
        assert source.strftime("%Y-%m-%dT%H:%M:%S.%f%z") == result

    def test_datetime_to_str_null(self):
        result = datetime_to_str(None)
        assert result is None

    def test_datetime_to_str_already_str(self):
        source = self.default.strftime("%Y-%m-%dT%H:%M:%S.%f%z")
        result = datetime_to_str(source)
        assert source == result


class TestCaseUnitUtilsWrappers:

    def example_generator(self):
        while True:
            val = yield
            print(val)

    def test_generator_default(self):
        gen = self.example_generator()
        assert getgeneratorstate(gen) == "GEN_CREATED"

    def test_coroutine_wrapper(self):
        func = coroutine(self.example_generator)
        gen = func()
        assert getgeneratorstate(gen) == "GEN_SUSPENDED"

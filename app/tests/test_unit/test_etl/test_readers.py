from unittest.mock import patch

import pytest

from internal.etl.readers import ETLReaderInterface, ETLReader


class TestCaseUnitETLReaderInterfaces:

    def test_interface(self):
        with pytest.raises(NotImplementedError):
            ETLReaderInterface().get(start=None, limit=1)
        with pytest.raises(NotImplementedError):
            ETLReaderInterface().get_count(start=None)

        ETLReaderInterface().close()

    def test_abstract(self):
        with pytest.raises(NotImplementedError):
            ETLReader()

        with patch("internal.etl.readers.ETLReader._get_api") as mock:
            mock.return_value = "test"
            reader = ETLReader()

            assert reader._api == "test"

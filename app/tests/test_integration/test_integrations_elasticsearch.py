import datetime
from unittest.mock import patch

import pytest
import requests.exceptions

from conf import settings
from internal.integrations import ElasticsearchAPI


class MockRequestsResponse:

    def __init__(self, status_code: int):
        self.status_code = status_code

    def raise_for_status(self):
        pass

    def json(self):
        return None


class TestCaseIntegrationElasticsearchAPIIntegrations:

    def setup_method(self):
        resp = requests.delete(self._get_expected_url() + "/test_index")
        if resp.status_code != 404:
            resp.raise_for_status()

    def _get_api(self):
        return ElasticsearchAPI(
            scheme=settings.EXTERNAL_ELASTICSEARCH_SCHEME,
            host=settings.EXTERNAL_ELASTICSEARCH_HOST,
            port=settings.EXTERNAL_ELASTICSEARCH_PORT,
        )

    def _get_expected_url(self):
        expected_url = settings.EXTERNAL_ELASTICSEARCH_SCHEME
        expected_url += "://"
        expected_url += settings.EXTERNAL_ELASTICSEARCH_HOST
        expected_url += ":"
        expected_url += str(settings.EXTERNAL_ELASTICSEARCH_PORT)
        return expected_url

    def test_url(self):
        es = self._get_api()
        assert es._url == self._get_expected_url()

    def test_connection_invalid_check_index_exists(self):
        with pytest.raises(requests.exceptions.ConnectionError):
            es = ElasticsearchAPI(
                scheme=settings.EXTERNAL_ELASTICSEARCH_SCHEME,
                host="0.0.0.0",
                port=1000,
            )
            es.check_index_exists("test_index")

    def test_connection_invalid_create_index(self):
        with pytest.raises(requests.exceptions.ConnectionError):
            es = ElasticsearchAPI(
                scheme=settings.EXTERNAL_ELASTICSEARCH_SCHEME,
                host="0.0.0.0",
                port=1000,
            )
            es.create_index("test_index", {}, {})

    def test_connection_invalid_bulk(self):
        with pytest.raises(requests.exceptions.ConnectionError):
            es = ElasticsearchAPI(
                scheme=settings.EXTERNAL_ELASTICSEARCH_SCHEME,
                host="0.0.0.0",
                port=1000,
            )
            es.bulk("test_index", [{"_id": 1, "test": "value"}])

    def test_check_index_exists(self):
        es = self._get_api()
        result = es.check_index_exists("test_index")
        assert result is False

        result = es.create_index("test_index", {}, {})
        assert result is True

        result = es.check_index_exists("test_index")
        assert result is True

    def test_create_index_invalid_settings(self):
        es = self._get_api()
        with pytest.raises(requests.exceptions.HTTPError) as ex:
            es.create_index("test_index", {"invalid_settings": "val"}, {})

            assert ex.response.status_code == 400

    def test_create_index_invalid_mappings(self):
        es = self._get_api()
        with pytest.raises(requests.exceptions.HTTPError) as ex:
            es.create_index("test_index", {}, {"invalid_mappings": "val"})

            assert ex.response.status_code == 400

    def test_create_index_already_exists(self):
        es = self._get_api()
        result = es.create_index("test_index", {}, {})
        assert result is True

        with pytest.raises(requests.exceptions.HTTPError) as ex:
            es.create_index("test_index", {}, {})

            assert ex.response.status_code == 400

    def test_bulk_index_not_exists(self):
        es = self._get_api()

        result = es.check_index_exists("test_index")
        assert result is False

        es.bulk(
            index_name="test_index",
            items=[{
                "_id": 1,
            }, {
                "_id": 2,
            }]
        )

        result = es.check_index_exists("test_index")
        assert result is True

    def test_bulk_use_deepcopy(self):
        es = self._get_api()
        es.create_index("test_index", {}, {})

        data = [{
            "_id": 1,
        }, {
            "_id": 2,
        }]
        result = es.bulk(
            index_name="test_index",
            items=data
        )
        print(result)
        assert result["errors"] is False
        assert len(result["items"]) == 2
        assert result["items"][0]["index"]["_index"] == "test_index"
        assert result["items"][0]["index"]["_type"] == "_doc"
        assert result["items"][0]["index"]["_id"] == '1'
        assert result["items"][0]["index"]["_version"] == 1
        assert result["items"][0]["index"]["result"] == "created"

        assert result["items"][1]["index"]["_index"] == "test_index"
        assert result["items"][1]["index"]["_type"] == "_doc"
        assert result["items"][1]["index"]["_id"] == '2'
        assert result["items"][1]["index"]["_version"] == 1
        assert result["items"][1]["index"]["result"] == "created"

        assert data == [{
            "_id": 1,
        }, {
            "_id": 2,
        }]

    def test_bulk_request(self):
        with patch("requests.post") as mock:
            mock.return_value = MockRequestsResponse(status_code=200)
            es = self._get_api()
            es.create_index("test_index", {}, {})
            es.bulk(
                index_name="test_index",
                items=[{
                    "_id": 1,
                    "field1": "val1",
                    "field2": {"key2": "val2"},
                    "field3": [{"key3": "val3"}]
                }, {
                    "_id": 2,
                    "field1": "val1_2",
                    "field2": {"key2": "val2_2"},
                    "field3": [{"key3": "val3_2"}]
                }]
            )

            data_string = "\n"
            data_string += '{"index": {"_index": "test_index", "_id": 1}}' + "\n"
            data_string += (
                               '{"field1": "val1", "field2": {"key2": "val2"}, '
                               '"field3": [{"key3": "val3"}]}'
                           ) + "\n"
            data_string += '{"index": {"_index": "test_index", "_id": 2}}' + "\n"
            data_string += (
                               '{"field1": "val1_2", "field2": {"key2": "val2_2"}, '
                               '"field3": [{"key3": "val3_2"}]}'
                           ) + "\n"
            mock.assert_called_once()
            mock.assert_called_once_with(
                self._get_expected_url() + "/_bulk",
                data=data_string,
                headers={
                    "Content-Type": "application/x-ndjson"
                }
            )

    def test_bulk_only_primitive_types(self):
        with pytest.raises(TypeError) as ex:
            es = self._get_api()
            es.create_index("test_index", {}, {})
            es.bulk(
                index_name="test_not_exists_index",
                items=[{
                    "_id": 1,
                    "field1": datetime.date.today(),
                    "field2": object(),
                }]
            )

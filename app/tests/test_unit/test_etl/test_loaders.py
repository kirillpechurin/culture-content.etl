from collections import defaultdict
from unittest.mock import patch

import pytest

import conf
from conf import settings
from internal.etl.loaders import BooksElasticsearchLoader, BooksSubscriptionLoader, ETLLoaderInterface, ETLLoader


class MockElasticsearchAPI:

    def __init__(self, *args, **kwargs):
        self._url = f"sample-url"
        self.methods = defaultdict(list)
        self.return_methods = {}

        self.methods["__init__"].append((args, kwargs))

    def check_index_exists(self, *args, **kwargs):
        self.methods["check_index_exists"].append((args, kwargs))
        return self.return_methods.get("check_index_exists")

    def create_index(self, *args, **kwargs):
        self.methods["create_index"].append((args, kwargs))
        return self.return_methods.get("create_index")

    def bulk(self, *args, **kwargs):
        self.methods["bulk"].append((args, kwargs))
        return self.return_methods.get("bulk")


class MockAMQPAPI:

    def __init__(self, *args, **kwargs):
        self.methods = defaultdict(list)
        self.return_methods = {}

        self.methods["__init__"].append((args, kwargs))

    def publish(self, *args, **kwargs):
        self.methods["publish"].append((args, kwargs))
        return self.return_methods.get("publish")

    def close(self, *args, **kwargs):
        self.methods["close"].append((args, kwargs))
        return self.return_methods.get("close")


class TestCaseUnitETLLoaderInterfaces:

    def test_interface(self):
        with pytest.raises(NotImplementedError):
            ETLLoaderInterface().run()

    def test_abstract(self):
        loader = ETLLoader(data={"sample": "val"})
        assert loader._data == {"sample": "val"}

        with pytest.raises(NotImplementedError):
            ETLLoader({}).run()


class TestCaseUnitBooksElasticsearchLoader:

    def test_called_check_index_exists(self):
        with patch("internal.etl.loaders.BooksElasticsearchLoader._get_api") as mock:
            mock.return_value = MockElasticsearchAPI()

            BooksElasticsearchLoader(data={"sample": "value"}).run()

            mocked_api = mock.return_value
            assert len(mocked_api.methods["check_index_exists"]) == 1
            assert mocked_api.methods["check_index_exists"][0] == (
                (conf.external.BOOKS_ELASTICSEARCH_INDEX_NAME,),
                {}
            )

    def test_index_not_exists_called_create_index(self):
        with patch("internal.etl.loaders.BooksElasticsearchLoader._get_api") as mock:
            mock.return_value = MockElasticsearchAPI()
            mocked_api = mock.return_value
            mocked_api.return_methods["check_index_exists"] = False

            BooksElasticsearchLoader(data={"sample": "value"}).run()

            assert len(mocked_api.methods["create_index"]) == 1
            assert mocked_api.methods["create_index"][0] == (
                (
                    conf.external.BOOKS_ELASTICSEARCH_INDEX_NAME,
                    conf.external.BOOKS_ELASTICSEARCH_INDEX_SETTINGS,
                    conf.external.BOOKS_ELASTICSEARCH_INDEX_MAPPINGS
                ),
                {}
            )

    def test_index_exists_not_called_create_index(self):
        with patch("internal.etl.loaders.BooksElasticsearchLoader._get_api") as mock:
            mock.return_value = MockElasticsearchAPI()
            mocked_api = mock.return_value
            mocked_api.return_methods["check_index_exists"] = True

            BooksElasticsearchLoader(data={"sample": "value"}).run()

            assert len(mocked_api.methods["create_index"]) == 0

    def test_called_bulk(self):
        with patch("internal.etl.loaders.BooksElasticsearchLoader._get_api") as mock:
            mock.return_value = MockElasticsearchAPI()

            BooksElasticsearchLoader(data={"sample": "value"}).run()

            mocked_api = mock.return_value
            assert len(mocked_api.methods["bulk"]) == 1
            assert mocked_api.methods["bulk"][0] == (
                (conf.external.BOOKS_ELASTICSEARCH_INDEX_NAME, {"sample": "value"},),
                {}
            )

    def test_get_api(self):
        loader = BooksElasticsearchLoader(data={"sample": "value"})
        loader._api_class = MockElasticsearchAPI

        api = loader._get_api()
        assert isinstance(api, MockElasticsearchAPI)

        assert len(api.methods.keys()) == 1
        assert len(api.methods["__init__"]) == 1
        assert api.methods["__init__"][0] == (
            (
                settings.EXTERNAL_ELASTICSEARCH_SCHEME,
                settings.EXTERNAL_ELASTICSEARCH_HOST,
                settings.EXTERNAL_ELASTICSEARCH_PORT,
            ),
            {}
        )


class TestCaseUnitBooksSubscriptionLoader:

    def test_called_publish_by_keys(self):
        with patch("internal.etl.loaders.BooksSubscriptionLoader._get_api") as mock:
            mock.return_value = MockAMQPAPI()

            BooksSubscriptionLoader(data={
                "sample": "value",
                "sample2": {"body": "value2"}
            }).run()

            mocked_api = mock.return_value
            assert len(mocked_api.methods["publish"]) == 2
            assert mocked_api.methods["publish"][0] == (
                (),
                dict(exchange="subscription", queue="sample", body='"value"')
            )
            assert mocked_api.methods["publish"][1] == (
                (),
                dict(exchange="subscription", queue="sample2", body='{"body": "value2"}')
            )

            assert len(mocked_api.methods["close"]) == 1
            assert mocked_api.methods["close"][0] == ((), {})

    def test_called_when_error(self):
        class MockAMQPAPIError(MockAMQPAPI):
            def publish(self, *args, **kwargs):
                raise TypeError

        with (
            patch("internal.etl.loaders.BooksSubscriptionLoader._get_api") as mock,
            pytest.raises(TypeError)
        ):
            mock.return_value = MockAMQPAPIError()

            BooksSubscriptionLoader(data={
                "sample": "value",
                "sample2": {"body": "value2"}
            }).run()

            mocked_api = mock.return_value
            assert len(mocked_api.methods["close"]) == 1
            assert mocked_api.methods["close"][0] == ((), {})

    def test_get_api(self):
        loader = BooksSubscriptionLoader(data={"sample": "value"})
        loader._api_class = MockAMQPAPI

        api = loader._get_api()
        assert isinstance(api, MockAMQPAPI)

        assert len(api.methods.keys()) == 1
        assert len(api.methods["__init__"]) == 1
        assert api.methods["__init__"][0] == (
            (
                settings.EXTERNAL_AMQP_HOST,
                settings.EXTERNAL_AMQP_PORT,
                settings.EXTERNAL_AMQP_USER,
                settings.EXTERNAL_AMQP_PASSWORD,
            ),
            {}
        )

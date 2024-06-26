import pytest
import redis.exceptions
from redis import Redis

from conf import settings
from internal.bootstrap.servers import KVDBServer


class TestCaseIntegrationKVDBServer:

    def setup_method(self):
        self._default_setup()

    def _default_setup(self):
        KVDBServer.setup(
            host=settings.KVDB_HOST,
            port=settings.KVDB_PORT,
            username=settings.KVDB_USERNAME,
            password=settings.KVDB_PASSWORD
        )

    def teardown_method(self):
        KVDBServer.close()

    def test_connection(self):
        client = KVDBServer.get_client()
        assert isinstance(client, Redis)

    def test_invalid_connection(self):
        KVDBServer.close()

        with pytest.raises(redis.exceptions.ConnectionError):
            KVDBServer.setup(
                host="invalid",
                port=1000,
                username="invalid",
                password="invalid"
            )

    def test_client_setup_singleton(self):
        with pytest.raises(AssertionError):
            self._default_setup()

    def test_client_null(self):
        KVDBServer.close()
        with pytest.raises(AssertionError):
            KVDBServer.get_client()

    def test_client_singleton(self):
        client1 = KVDBServer.get_client()
        client2 = KVDBServer.get_client()

        assert client1 == client2

    def test_client_close(self):
        KVDBServer.close()

        with pytest.raises(AssertionError):
            KVDBServer.get_client()

        assert KVDBServer._client is None

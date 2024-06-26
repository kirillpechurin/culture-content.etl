import json

from redis import Redis

from conf import settings
from internal.bootstrap import KVDBServer
from internal.etl.states import ETLState


class TestCaseIntegrationETLState:

    def setup_method(self):
        KVDBServer.setup(
            host=settings.KVDB_HOST,
            port=settings.KVDB_PORT,
            username=settings.KVDB_USERNAME,
            password=settings.KVDB_PASSWORD
        )

        client = KVDBServer.get_client()
        client.delete("test_state")

    def teardown_method(self):
        KVDBServer.close()

    def test_constructor(self):
        state = ETLState(key="test_state")
        assert state._key == "test_state"
        assert isinstance(state._client, Redis)

        assert state._payload == {"mark": None}

    def test_getter(self):
        state = ETLState(key="test_state")
        assert state.payload == {"mark": None}

        state._payload = {"sample": "value"}
        assert state.payload == {"sample": "value"}

    def test_setter(self):
        state = ETLState(key="test_state")
        raw = state._client.get("test_state")
        assert raw is None

        state.payload = {"sample": "value"}

        assert state._payload == {"sample": "value"}
        raw = state._client.get("test_state")
        assert raw == b'{"sample": "value"}'

        assert json.loads(raw) == {"sample": "value"}

    def test_payload_exists(self):
        state = ETLState(key="test_state")
        state.payload = {"sample": "value"}

        state = ETLState(key="test_state")
        assert state.payload == {"sample": "value"}

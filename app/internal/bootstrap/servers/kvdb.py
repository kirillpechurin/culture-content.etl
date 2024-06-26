from typing import Optional

import redis
from redis import Redis


class KVDBServer:
    _client: Optional[redis.Redis] = None

    @classmethod
    def setup(cls, host: str, port: int, username: str, password: str):
        assert cls._client is None
        cls._client = Redis(
            host=host,
            port=port,
            username=username,
            password=password
        )
        cls._client.ping()

    @classmethod
    def get_client(cls) -> redis.Redis:
        assert cls._client is not None
        return cls._client

    @classmethod
    def close(cls):
        del cls._client
        cls._client = None

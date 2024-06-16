import redis
from redis import Redis


class KVDBServer:
    _client: redis.Redis = None

    @classmethod
    def setup(cls, host: str, port: int, username: str, password: str):
        cls._client = Redis(
            host=host,
            port=port,
            username=username,
            password=password
        )

    @classmethod
    def get_client(cls) -> redis.Redis:
        assert cls._client is not None
        return cls._client

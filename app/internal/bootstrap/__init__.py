from conf import settings
from .servers import KVDBServer


def init_kvdb_server():
    KVDBServer.setup(
        host=settings.KVDB_HOST,
        port=settings.KVDB_PORT,
        username=settings.KVDB_USERNAME,
        password=settings.KVDB_PASSWORD
    )


def init_all():
    init_kvdb_server()

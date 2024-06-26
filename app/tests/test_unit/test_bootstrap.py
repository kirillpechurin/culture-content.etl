from unittest.mock import patch

from conf import settings


class TestCaseBootstrapInit:

    def test_init_all(self):
        with patch("internal.bootstrap.init_kvdb_server") as mock:
            from internal.bootstrap import init_all
            init_all()

            mock.assert_called_once_with()

    def test_init_kvdb_server(self):
        with patch("internal.bootstrap.servers.KVDBServer.setup") as mock:
            from internal.bootstrap import init_kvdb_server
            init_kvdb_server()

            mock.assert_called_once_with(
                host=settings.KVDB_HOST,
                port=settings.KVDB_PORT,
                username=settings.KVDB_USERNAME,
                password=settings.KVDB_PASSWORD
            )

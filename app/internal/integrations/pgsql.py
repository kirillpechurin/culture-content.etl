import psycopg2
import psycopg2.extras


class PGSQLAPI:

    def __init__(self, host: str, port: int, db_name: str, username: str, password: str):
        self._connection = self._get_connection(
            host=host,
            port=port,
            db_name=db_name,
            username=username,
            password=password,
        )

    def _get_connection(self, host: str, port: int, db_name: str, username: str, password: str):
        return psycopg2.connect(
            host=host,
            port=port,
            dbname=db_name,
            user=username,
            password=password,
            cursor_factory=psycopg2.extras.DictCursor
        )

    def get_cursor(self):
        return self._connection.cursor()

    def close(self):
        self._connection.close()

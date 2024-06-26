import psycopg2
import psycopg2.extras
import pytest

from . import settings


def _get_pgsql_connection(dbname: str = None):
    options = dict(
        host=settings.TEST_PG_HOST,
        port=settings.TEST_PG_PORT,
        user=settings.TEST_PG_USER,
        password=settings.TEST_PG_PASSWORD,
        cursor_factory=psycopg2.extras.DictCursor,
    )
    if dbname:
        options["dbname"] = dbname
    else:
        options["dbname"] = "postgres"
    return psycopg2.connect(**options)


@pytest.fixture(scope="session")
def psql_database():
    print('start psql_database')
    test_db_name = settings.TEST_PG_DB_NAME

    conn = _get_pgsql_connection()
    conn.autocommit = True
    with conn.cursor() as cursor:
        cursor.execute(f"DROP DATABASE IF EXISTS {test_db_name}")
        cursor.execute(f"CREATE DATABASE {test_db_name}")
    conn.close()

    with open("tests/docs/db/postgresql/create_table.sql", mode="r") as f:
        ddl = f.read()
    conn = _get_pgsql_connection(dbname=test_db_name)
    conn.autocommit = True
    with conn.cursor() as cursor:
        cursor.execute(ddl)
    conn.close()

    yield test_db_name

    conn = _get_pgsql_connection()
    conn.autocommit = True
    with conn.cursor() as cursor:
        cursor.execute(f"DROP DATABASE IF EXISTS {test_db_name}")
    conn.close()
    print('end psql_database')


@pytest.fixture(scope="class")
def psql_connection(psql_database):
    conn = _get_pgsql_connection(dbname=psql_database)
    yield conn
    conn.close()

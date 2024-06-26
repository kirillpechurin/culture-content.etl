import datetime
import random
import uuid
from unittest.mock import patch

import psycopg2.extras
import pytest

from internal.etl.readers import BooksDBReader
from internal.integrations import PGSQLAPI


@pytest.fixture(scope="class")
def books_reader(psql_connection):
    with patch("internal.integrations.pgsql.PGSQLAPI._get_connection") as mock:
        mock.return_value = psql_connection

        reader = BooksDBReader()
        print("reader")

        yield reader


def _get_last_id(conn, table_name):
    with conn.cursor() as cursor:
        cursor.execute(
            f"SELECT {table_name}.id FROM {table_name} ORDER BY {table_name}.id DESC LIMIT 1"
        )
        last = cursor.fetchone()
    return int(last["id"]) if last else 0


def generate_books(conn, params):
    assert params["count"] > 0

    last_book_id = _get_last_id(conn, "books")
    last_author_id = _get_last_id(conn, "authors")
    last_genre_id = _get_last_id(conn, "genres")

    authors = []
    for i in range(last_author_id + 1, last_author_id + 5 + 1):
        authors.append({
            "id": i,
            "name": str(uuid.uuid4())
        })
    genres = []
    for i in range(last_genre_id + 1, last_genre_id + 5 + 1):
        genres.append({
            "id": i,
            "title": str(uuid.uuid4())
        })
    books = []
    books_authors = []
    books_genres = []
    for i in range(last_book_id + 1, last_book_id + params["count"] + 1):
        books.append({
            "id": i,
            "title": str(uuid.uuid4()),
            "description": str(uuid.uuid4()),
            "publisher": str(uuid.uuid4()),
            "publication_date": datetime.date.today(),
            "isbn": str(uuid.uuid4())[0:13],
            "number_of_pages": random.randint(100, 500),
            "first_row": str(uuid.uuid4()),
            "created_at": params["mark"],
            "updated_at": params["mark"],
        })
        for j in range(0, random.randint(2, 3)):
            books_authors.append({
                "book_id": i,
                "author_id": authors[j]["id"]
            })
        for j in range(0, random.randint(2, 3)):
            books_genres.append({
                "book_id": i,
                "genre_id": genres[j]["id"]
            })

    with conn.cursor() as cursor:
        sql = """
        INSERT INTO books (
            id,
            title, 
            description, 
            publisher, 
            publication_date, 
            isbn, 
            number_of_pages, 
            first_row, 
            created_at, 
            updated_at
        ) VALUES
        """ + ','.join(cursor.mogrify(
            "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", [val for _, val in x.items()]
        ).decode() for x in books)
        cursor.execute(sql)

        sql = "INSERT INTO authors (id, name) VALUES" + ','.join(cursor.mogrify(
            "(%s,%s)", [val for _, val in x.items()]
        ).decode() for x in authors)
        cursor.execute(sql)

        sql = "INSERT INTO genres (id, title) VALUES" + ','.join(cursor.mogrify(
            "(%s,%s)", [val for _, val in x.items()]
        ).decode() for x in genres)
        cursor.execute(sql)

        sql = "INSERT INTO books_authors (book_id, author_id) VALUES" + ','.join(cursor.mogrify(
            "(%s,%s)", [val for _, val in x.items()]
        ).decode() for x in books_authors)
        cursor.execute(sql)

        sql = "INSERT INTO books_genres (book_id, genre_id) VALUES" + ','.join(cursor.mogrify(
            "(%s,%s)", [val for _, val in x.items()]
        ).decode() for x in books_genres)
        cursor.execute(sql)

        conn.commit()


def clear_books(conn):
    with conn.cursor() as cursor:
        cursor.execute("DELETE FROM books")
        conn.commit()


@pytest.mark.usefixtures("psql_database")
class TestCaseIntegrationBooksDBReader:

    def test_constructor(self, books_reader):
        assert isinstance(books_reader._api, PGSQLAPI)
        assert isinstance(books_reader._cursor, psycopg2.extras.DictCursor)

    @pytest.mark.parametrize("generation_params,mark,count", [
        (
                (dict(
                    count=10,
                    mark=datetime.datetime.now(),
                ),),
                datetime.datetime.now() - datetime.timedelta(seconds=5),
                10
        ),
        (
                (dict(
                    count=7,
                    mark=datetime.datetime.now() - datetime.timedelta(seconds=10),
                ), dict(
                    count=9,
                    mark=datetime.datetime.now() - datetime.timedelta(seconds=5),
                )),
                datetime.datetime.now() - datetime.timedelta(seconds=6),
                9
        ),
        (
                (dict(
                    count=10,
                    mark=datetime.datetime.now() - datetime.timedelta(seconds=10),
                ),),
                datetime.datetime.now() - datetime.timedelta(seconds=9),
                0
        ),
        (
                (dict(
                    count=7,
                    mark=datetime.datetime.now(),
                ),),
                None,
                7
        ),
    ])
    def test_get_count(self, generation_params, mark, count, books_reader, psql_connection):
        for generation_param in generation_params:
            generate_books(psql_connection, generation_param)

        print(count, books_reader.get_count(start=mark))
        assert books_reader.get_count(start=mark) == count

        clear_books(psql_connection)

    @pytest.mark.parametrize("generation_params,mark,count", [
        (
                (dict(
                    count=10,
                    mark=datetime.datetime.now(),
                ),),
                datetime.datetime.now() - datetime.timedelta(seconds=5),
                10
        ),
        (
                (dict(
                    count=10,
                    mark=datetime.datetime.now(),
                ), dict(
                    count=10,
                    mark=datetime.datetime.now() + datetime.timedelta(seconds=5),
                ),),
                datetime.datetime.now() - datetime.timedelta(seconds=5),
                20
        ),
        (
                (dict(
                    count=7,
                    mark=datetime.datetime.now() - datetime.timedelta(seconds=10),
                ), dict(
                    count=9,
                    mark=datetime.datetime.now() - datetime.timedelta(seconds=5),
                )),
                datetime.datetime.now() - datetime.timedelta(seconds=6),
                9
        ),
        (
                (dict(
                    count=10,
                    mark=datetime.datetime.now() - datetime.timedelta(seconds=10),
                ),),
                datetime.datetime.now() - datetime.timedelta(seconds=9),
                0
        ),
        (
                (dict(
                    count=7,
                    mark=datetime.datetime.now(),
                ),),
                None,
                7
        ),
    ])
    def test_get(self, generation_params, mark, count, books_reader, psql_connection):
        last_author_id = _get_last_id(psql_connection, "authors")
        last_genre_id = _get_last_id(psql_connection, "genres")

        for generation_param in generation_params:
            generate_books(psql_connection, generation_param)

        data, last_mark = books_reader.get(start=mark, limit=5)
        assert len(data["books"]) == (5 if count > 5 else count)

        source_book_ids = [item["id"] for item in data["books"]]
        source_author_ids = [item["id"] for item in data["authors"]]
        source_genre_ids = [item["id"] for item in data["genres"]]

        with psql_connection.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT
                  b.id as id
                FROM 
                    books b
                {"" if not mark else f"WHERE b.created_at > '{mark}' "}
                ORDER BY 
                  b.created_at 
                LIMIT 5
                """
            )
            expected_book_ids = [item["id"] for item in cursor.fetchall()]

            if expected_book_ids:
                cursor.execute(
                    """
                    SELECT
                        ba.author_id as author_id
                    FROM
                        books_authors ba
                    WHERE
                        ba.book_id IN %s
                    """,
                    (tuple(expected_book_ids),)
                )
                expected_author_ids = [item["author_id"] for item in cursor.fetchall()]

                cursor.execute(
                    """
                    SELECT
                        bg.genre_id as genre_id
                    FROM
                        books_genres bg
                    WHERE
                        bg.book_id IN %s
                    """,
                    (tuple(expected_book_ids),)
                )
                expected_genre_ids = [item["genre_id"] for item in cursor.fetchall()]
            else:
                expected_author_ids = []
                expected_genre_ids = []

        assert set(source_book_ids) == set(expected_book_ids)
        assert set(expected_author_ids) == set(source_author_ids)
        assert set(expected_genre_ids) == set(source_genre_ids)

        data, last_mark = books_reader.get(start=mark, limit=count)
        assert len(data["books"]) == count

        clear_books(psql_connection)

    def test_close_method(self, books_reader):
        with patch("internal.integrations.pgsql.PGSQLAPI.close") as mock:
            books_reader.close()

            mock.assert_called_once_with()

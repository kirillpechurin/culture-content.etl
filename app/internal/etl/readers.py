import datetime
from typing import List, Dict, Optional, Tuple, Any

from conf import settings
from internal.integrations import PGSQLAPI


class ETLReaderInterface:

    def get_count(self, start):
        raise NotImplementedError

    def get(self, start, limit: int) -> Tuple[List[Dict], Any]:
        raise NotImplementedError

    def close(self):
        pass


class ETLReader(ETLReaderInterface):

    def __init__(self):
        self._api = self._get_api()

    def _get_api(self):
        raise NotImplementedError


class ETLDBReader(ETLReader):
    pass


class BooksDBReader(ETLDBReader):

    def _get_api(self):
        return PGSQLAPI(
            host=settings.EXTERNAL_PG_HOST,
            port=settings.EXTERNAL_PG_PORT,
            db_name=settings.EXTERNAL_PG_DB_NAME,
            username=settings.EXTERNAL_PG_USER,
            password=settings.EXTERNAL_PG_PASSWORD,
        )

    def __init__(self):
        super().__init__()
        self._cursor = self._api.get_cursor()

    def get_count(self, start: Optional[datetime.date]):
        sql = f"""
        SELECT
            COUNT(b.id) as count_rows
        FROM
            books b
        {"" if not start else f"WHERE b.created_at > '{start}'"}
        """
        self._cursor.execute(sql)
        data = dict(self._cursor.fetchone())
        return data["count_rows"]

    def get(self, start: Optional[datetime.date], limit: int):
        sql = f"""
        SELECT
            b.id as id,
            b.title as title,
            b.description as description,
            b.publisher as publisher,
            b.publication_date as publication_date,
            b.isbn as isbn,
            b.number_of_pages as number_of_pages,
            b.first_row as first_row,
            b.created_at as created_at,
            b.updated_at as updated_at
        FROM
            books b
        {"" if not start else f"WHERE b.created_at > '{start}'"}
        ORDER BY
            b.created_at
        LIMIT {limit}
        """
        self._cursor.execute(sql)
        books = self._cursor.fetchall()
        books = [dict(item) for item in books]

        book_ids = [item["id"] for item in books]

        sql = """
        SELECT
            a.id as id,
            a.name as name
        FROM
            books_authors ba
        INNER JOIN
            authors a ON a.id = ba.author_id
        WHERE
            ba.book_id IN %s
        ORDER BY
            a.id
        """
        if len(book_ids):
            self._cursor.execute(sql, (tuple(book_ids),))
            authors = self._cursor.fetchall()
            authors = [dict(item) for item in authors]
        else:
            authors = []

        sql = """
        SELECT
            g.id as id,
            g.title as title
        FROM
            books_genres bg
        INNER JOIN
            genres g ON g.id = bg.genre_id
        WHERE
            bg.book_id IN %s
        ORDER BY
            g.id
        """
        if len(book_ids):
            self._cursor.execute(sql, (tuple(book_ids),))
            genres = self._cursor.fetchall()
            genres = [dict(item) for item in genres]
        else:
            genres = []

        last_created_at = books[-1]["created_at"] if books else None
        return {
            "books": books,
            "authors": authors,
            "genres": genres
        }, last_created_at

    def close(self):
        self._api.close()

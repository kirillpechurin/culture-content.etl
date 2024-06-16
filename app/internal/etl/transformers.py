from collections import defaultdict


class ETLTransformerInterface:

    def run(self):
        raise NotImplementedError


class ETLTransformer(ETLTransformerInterface):

    def __init__(self, data: dict):
        self._data = data

    def run(self):
        raise NotImplementedError


def _aggregate_authors_by_book_id(data: dict):
    aggregated_authors = defaultdict(list)
    for author in data["authors"]:
        aggregated_authors[author["book_id"]].append({
            "id": author["id"],
            "name": author["name"]
        })
    return aggregated_authors


def _aggregate_genres_by_book_id(data: dict):
    aggregated_genres = defaultdict(list)
    for genre in data["genres"]:
        aggregated_genres[genre["book_id"]].append({
            "id": genre["id"],
            "title": genre["title"]
        })
    return aggregated_genres


class BooksElasticsearchTransformer(ETLTransformer):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._authors = _aggregate_authors_by_book_id(self._data)
        self._genres = _aggregate_genres_by_book_id(self._data)

    def run(self):
        result = []
        for item in self._data["books"]:
            result.append({
                "_id": item["id"],
                "title": item["title"],
                "description": item["description"],
                "publisher": item["publisher"],
                "publication_date": item["publication_date"],
                "isbn": item["isbn"],
                "number_of_pages": item["number_of_pages"],
                "first_row": item["first_row"],
                "authors": [
                    {
                        "id": author["id"],
                        "name": author["name"]
                    }
                    for author in self._authors[item["id"]]
                ],
                "genres": [
                    {
                        "id": genre["id"],
                        "title": genre["title"]
                    }
                    for genre in self._genres[item["id"]]
                ]
            })
        return result


class BooksSubscriptionTransformer(ETLTransformer):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._authors = _aggregate_authors_by_book_id(self._data)
        self._genres = _aggregate_genres_by_book_id(self._data)

    def run(self):
        new_books_from_authors = defaultdict(list)
        new_books = []
        new_books_in_genres = defaultdict(list)
        for item in self._data["books"]:
            payload = {
                "title": item["title"],
                "description": item["description"],
                "publisher": item["publisher"],
                "publication_date": item["publication_date"],
                "isbn": item["isbn"],
                "number_of_pages": item["number_of_pages"],
                "first_row": item["first_row"],
            }
            new_books.append(payload)
            for author in self._authors[item["id"]]:
                new_books_from_authors[author["id"]].append(payload)
            for genre in self._genres[item["id"]]:
                new_books_in_genres[genre["id"]].append(payload)
        return {
            "new_books_from_authors": new_books_from_authors,
            "new_books_in_genres": new_books_in_genres,
            "new_books": new_books
        }

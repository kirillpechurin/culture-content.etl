BOOKS_ELASTICSEARCH_INDEX_NAME = "books"

BOOKS_ELASTICSEARCH_INDEX_SETTINGS = {
    "refresh_interval": "1s",
    "analysis": {
        "filter": {
            "english_stop": {
                "type": "stop",
                "stopwords": "_english_"
            },
            "english_stemmer": {
                "type": "stemmer",
                "language": "english"
            },
            "english_possessive_stemmer": {
                "type": "stemmer",
                "language": "possessive_english"
            },
            "russian_stop": {
                "type": "stop",
                "stopwords": "_russian_"
            },
            "russian_stemmer": {
                "type": "stemmer",
                "language": "russian"
            }
        },
        "analyzer": {
            "ru_en": {
                "tokenizer": "standard",
                "filter": [
                    "lowercase",
                    "english_stop",
                    "english_stemmer",
                    "english_possessive_stemmer",
                    "russian_stop",
                    "russian_stemmer"
                ]
            }
        }
    }
}

BOOKS_ELASTICSEARCH_INDEX_MAPPINGS = {
    "dynamic": "strict",
    "properties": {
        "title": {
            "type": "text",
            "analyzer": "ru_en",
            "fields": {
                "keyword": {
                    "type": "keyword"
                }
            }
        },
        "description": {
            "type": "text",
            "analyzer": "ru_en"
        },
        "publisher": {
            "type": "text",
        },
        "publication_date": {
            "type": "date",
        },
        "isbn": {
            "type": "text",
        },
        "number_of_pages": {
            "type": "number",
        },
        "first_row": {
            "type": "text",
            "analyzer": "ru_en"
        },
        "authors": {
            "type": "nested",
            "dynamic": "strict",
            "properties": {
                "id": {
                    "type": "keyword"
                },
                "name": {
                    "type": "text",
                    "analyzer": "ru_en"
                }
            }
        },
        "genres": {
            "type": "nested",
            "dynamic": "strict",
            "properties": {
                "id": {
                    "type": "keyword"
                },
                "title": {
                    "type": "text",
                    "analyzer": "ru_en"
                }
            }
        },
    }
}

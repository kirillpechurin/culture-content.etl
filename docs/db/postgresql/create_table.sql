-- -----------------------------------------------------------------------------

DROP SEQUENCE IF EXISTS books_id_seq CASCADE;

CREATE SEQUENCE books_id_seq START 1;

DROP TABLE IF EXISTS books CASCADE;

CREATE TABLE books(
    id BIGINT DEFAULT nextval('books_id_seq') PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    description TEXT NOT NULL,
    publisher VARCHAR(255) NOT NULL,
    publication_date DATE NOT NULL,
    isbn VARCHAR(13) NOT NULL,
    number_of_pages INTEGER NOT NULL,
    first_row TEXT NOT NULL,
    created_at timestamp with time zone NOT NULL DEFAULT current_timestamp,
    updated_at timestamp with time zone NOT NULL DEFAULT current_timestamp
);

-- -----------------------------------------------------------------------------

DROP SEQUENCE IF EXISTS authors_id_seq CASCADE;

CREATE SEQUENCE authors_id_seq START 1;

DROP TABLE IF EXISTS authors CASCADE;

CREATE TABLE authors(
    id BIGINT DEFAULT nextval('books_id_seq') PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    created_at timestamp with time zone NOT NULL DEFAULT current_timestamp,
    updated_at timestamp with time zone NOT NULL DEFAULT current_timestamp
);

-- -----------------------------------------------------------------------------

DROP SEQUENCE IF EXISTS genres_id_seq CASCADE;

CREATE SEQUENCE genres_id_seq START 1;

DROP TABLE IF EXISTS genres CASCADE;

CREATE TABLE genres(
    id BIGINT DEFAULT nextval('genres_id_seq') PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    created_at timestamp with time zone NOT NULL DEFAULT current_timestamp,
    updated_at timestamp with time zone NOT NULL DEFAULT current_timestamp
);

-- -----------------------------------------------------------------------------

DROP SEQUENCE IF EXISTS books_authors_id_seq CASCADE;

CREATE SEQUENCE books_authors_id_seq START 1;

DROP TABLE IF EXISTS books_authors CASCADE;

CREATE TABLE books_authors(
    id BIGINT DEFAULT nextval('books_authors_id_seq') PRIMARY KEY,
    book_id INTEGER REFERENCES books(id) ON DELETE CASCADE,
    author_id INTEGER REFERENCES authors(id) ON DELETE CASCADE,
    created_at timestamp with time zone NOT NULL DEFAULT current_timestamp,
    updated_at timestamp with time zone NOT NULL DEFAULT current_timestamp
);

-- -----------------------------------------------------------------------------

DROP SEQUENCE IF EXISTS books_genres_id_seq CASCADE;

CREATE SEQUENCE books_genres_id_seq START 1;

DROP TABLE IF EXISTS books_genres CASCADE;

CREATE TABLE books_genres(
    id BIGINT DEFAULT nextval('books_genres_id_seq') PRIMARY KEY,
    book_id INTEGER REFERENCES books(id) ON DELETE CASCADE,
    genre_id INTEGER REFERENCES genres(id) ON DELETE CASCADE,
    created_at timestamp with time zone NOT NULL DEFAULT current_timestamp,
    updated_at timestamp with time zone NOT NULL DEFAULT current_timestamp
);

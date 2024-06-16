# Culture Content | ETL

[![ru](https://img.shields.io/badge/lang-ru-green.svg)](README.ru.md)
[![en](https://img.shields.io/badge/lang-en-red.svg)](README.md)
___

1. [Project objective](#project-objective)
2. [Realization](#realization)
    + [Technical part](#technical-part)
    + [Business tasks](#business-tasks)
3. [Project structure](#project-structure)
4. [Description of environment variables](#description-of-environment-variables)
5. [Launch](#launch)
    + [Requirements](#requirements)
    + [Start local](#start-local)
6. [System design](#system-design)
7. [Licensing](#licensing)

___

## Project objective

One of the services of the "Culture Content" project,
which implements an ETL mechanism for transferring data about books to related sources:
- RabbitMQ - for subscriptions to changes.
- Elasticsearch - for the search engine.

## Realization

___

### Technical part

* The service is implemented using an ETL mechanism based on [Celery](https://docs.celeryq.dev/en/stable /).
* Connections to an external PostgreSQL database are used to extract data.
* Data transformations are performed by means of the programming language.
* Entering data into Elasticsearch for the search engine and into RabbitMQ for further subscription processing.
* Redis is used to save the state of the ETL mechanism.

### Business tasks

* Transferring data about books to the search engine.
* Processing book data to respond to subscriptions. There are three types of subscriptions:
  * For a book.
  * On the author.
  * On the genre.
* When new subscription data becomes available, a corresponding notification to the user is implied.

## Project structure

___

- `app` - service
  - `conf` - configuration of the service, celery and external databases.
  - `internal` - business logic.
    - `bootstrap` is a service preparation package.
    - `etl` is an implementation of ETL.
    - `integrations` - Integration with external servers.
    - `utils` - common utilities of the service.
    - `tasks.py` - celery-service tasks, including ETL.
  - `.env.example` is an example of environment variables.
  - `main.py ` is the ETL launch point.
  - `requirements.txt ` - Service dependencies.
- `docs` - documentation.

## Description of environment variables

___

* `BOOKS_ETL_READER_BATCH_SIZE` - Size of the batch of one-time data reading.
* `BOOKS_ETL_READER_TIMEOUT_BY_EMPTY` - "sleep" time for ETL in case of missing data.
* `BOOKS_ETL_DB_STATE_KEY` - Key for saving the ETL state.

* `KVDB_HOST` - Host on which the key-value database server is running.
* `KVDB_PORT` - Port on which the key-value database server is running.
* `KVDB_USERNAME` - User for key-value database server.
* `KVDB_PASSWORD` - Password for key-value database server user.

* `EXTERNAL_AMQP_HOST` - The host where the external AMQP server is running.
* `EXTERNAL_AMQP_PORT` - Port on which the external AMQP server is running.
* `EXTERNAL_AMQP_USER` - User of the external AMQP server.
* `EXTERNAL_AMQP_PASSWORD` - Password for the user of the external AMQP server.

* `EXTERNAL_PG_HOST` - Host on which the external PostgreSQL database server is running.
* `EXTERNAL_PG_PORT` - Port on which the external PostgreSQL database server is running
* `EXTERNAL_PG_DB_NAME` - Name of the database from the external PostgreSQL database server.
* `EXTERNAL_PG_USER` - User of the external PostgreSQL database server.
* `EXTERNAL_PG_PASSWORD` - Password for the user of the external PostgreSQL database server.

* `EXTERNAL_ELASTICSEARCH_SCHEME` - Connection scheme to the external Elasticsearch server (http/https).
* `EXTERNAL_ELASTICSEARCH_HOST` - Host on which the external Elasticsearch server is running.
* `EXTERNAL_ELASTICSEARCH_PORT` - Port on which the external Elasticsearch server is running.

* `CELERY_BROKER_URL` - URL of the connection to the message broker.
* `CELERY_RESULT_BACKEND` - URL for saving task results.
* `CELERY_BROKER_CONNECTION_RETRY_ON_STARTUP` - Whether to try to connect again at startup.
* `CELERY_RESULT_EXPIRES_MINUTES` - Value in minutes according to which the results of the tasks will be deleted.
* `CELERY_TASK_ALWAYS_EAGER` - Running tasks without asynchronous call (For testing).

## Launch

___

### Requirements

* Installed Python version `3.10` or higher.

### Start local

1. Create a PostgreSQL database to use and execute the [create_table.sql] script (docs/db/postgresql/create_table.sql).
2. Create a `src/.env` file and specify the environment variables in it, following the example of `src/.en.example`.
3. Run `python3 --version` and make sure you have python version `3.10` or higher.
4. `python3 -m venv venv` - Create a virtual python environment in the venv folder.
5. `source venv/bin/activate` - Activate the virtual environment.
6. `pip3 install -r requirements.txt ` - Installation of dependencies.
7. `cd app` - Go to `app`.
8. You are ready to start! Run ETL:

- Launch Loaders
  ```shell
  celery --app=conf worker -E -Ofair --loglevel=info --queues=loaders --concurrency=1 --hostname=loaders
  ```

- Launch Transformers
  ```shell
  celery --app=conf worker -E -Ofair --loglevel=info --queues=transformers --concurrency=1 --hostname=transformers
  ```

- Launch Readers
  ```shell
  python main.py
  ```

## System design

- [The overall design and future of the system](docs/design/common.png)
- [Service design](docs/design/current.png)

## Licensing

___
See [LICENSE](LICENSE)
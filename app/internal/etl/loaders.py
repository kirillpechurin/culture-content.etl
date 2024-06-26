import json
from typing import Union

import conf
from conf import settings
from internal.integrations import ElasticsearchAPI, AMQPAPI


class ETLLoaderInterface:

    def run(self):
        raise NotImplementedError


class ETLLoader(ETLLoaderInterface):
    def __init__(self, data: Union[list, dict]):
        self._data = data

    def run(self):
        raise NotImplementedError


class BooksElasticsearchLoader(ETLLoader):
    _index_name = conf.external.BOOKS_ELASTICSEARCH_INDEX_NAME
    _index_settings = conf.external.BOOKS_ELASTICSEARCH_INDEX_SETTINGS
    _index_mappings = conf.external.BOOKS_ELASTICSEARCH_INDEX_MAPPINGS

    _api_class = ElasticsearchAPI

    def _get_api(self):
        return self._api_class(
            settings.EXTERNAL_ELASTICSEARCH_SCHEME,
            settings.EXTERNAL_ELASTICSEARCH_HOST,
            settings.EXTERNAL_ELASTICSEARCH_PORT,
        )

    def run(self):
        elasticsearch = self._get_api()
        if not elasticsearch.check_index_exists(self._index_name):
            elasticsearch.create_index(
                self._index_name,
                self._index_settings,
                self._index_mappings
            )

        elasticsearch.bulk(self._index_name, self._data)


class BooksSubscriptionLoader(ETLLoader):

    _api_class = AMQPAPI

    def _get_api(self):
        return self._api_class(
            settings.EXTERNAL_AMQP_HOST,
            settings.EXTERNAL_AMQP_PORT,
            settings.EXTERNAL_AMQP_USER,
            settings.EXTERNAL_AMQP_PASSWORD,
        )

    def run(self):
        amqp = self._get_api()
        try:
            for key in self._data:
                amqp.publish(
                    exchange="subscription",
                    queue=key,
                    body=json.dumps(self._data[key])
                )
        except Exception as ex:
            raise ex
        finally:
            amqp.close()

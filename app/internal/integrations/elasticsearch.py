import copy
import json

import requests


class ElasticsearchAPI:

    def __init__(self,
                 scheme: str,
                 host: str,
                 port: str):
        self._url = f"{scheme}://{host}:{port}"

    def check_index_exists(self,
                           index_name: str):
        response = requests.head(
            f"{self._url}/{index_name}"
        )
        if response.status_code == 200:
            return True
        elif response.status_code == 404:
            return False
        else:
            raise NotImplementedError

    def create_index(self,
                     index_name: str,
                     index_settings: dict,
                     index_mappings: dict):
        response = requests.put(
            f"{self._url}/{index_name}",
            json={
                "settings": index_settings,
                "mappings": index_mappings
            }
        )
        if response.status_code == 201:
            return True

        return False

    def bulk(self,
             index_name: str,
             items: list):
        data_string = "\n"
        items = copy.deepcopy(items)
        for item in items:
            data_string += json.dumps({"index": {"_index": index_name, "_id": item.pop("_id")}}) + "\n"
            data_string += json.dumps(item, default=str) + "\n"
        response = requests.post(
            f"{self._url}/_bulk",
            data=data_string,
            headers={
                "Content-Type": "application/x-ndjson"
            }
        )
        response.raise_for_status()

        return response.json()

import json

from internal.bootstrap import KVDBServer


class ETLState:

    def __init__(self, key: str):
        self._key = key
        self._client = KVDBServer.get_client()
        payload = self._client.get(self._key)
        if payload:
            self._payload = json.loads(payload)
        else:
            self._payload = {"mark": None}

    @property
    def payload(self):
        return self._payload

    @payload.setter
    def payload(self, value):
        self._payload = value
        self._client.set(self._key, json.dumps(self._payload))

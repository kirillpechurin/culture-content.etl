from collections import defaultdict
from unittest.mock import patch

import pika
import pika.exceptions
import pytest

from conf import settings
from internal.integrations import AMQPAPI


class MockPikaBlockingChannel:

    def __init__(self):
        self.methods = defaultdict(list)

    def queue_declare(self, *args, **kwargs): self.methods["queue_declare"].append((args, kwargs))

    def exchange_declare(self, *args, **kwargs): self.methods["exchange_declare"].append((args, kwargs))

    def basic_publish(self, *args, **kwargs): self.methods["basic_publish"].append((args, kwargs))


class MockPikaBlockingConnection:

    def __init__(self, parameters: pika.ConnectionParameters):
        self.parameters = parameters
        self.channels = []
        self.methods = defaultdict(list)

    def channel(self):
        channel = MockPikaBlockingChannel()
        self.channels.append(channel)
        return channel

    def close(self, *args, **kwargs): self.methods["close"].append((args, kwargs))


class TestCaseIntegrationAMQPIntegrations:

    def _get_api(self):
        return AMQPAPI(
            host=settings.EXTERNAL_AMQP_HOST,
            port=settings.EXTERNAL_AMQP_PORT,
            username=settings.EXTERNAL_AMQP_USER,
            password=settings.EXTERNAL_AMQP_PASSWORD,
        )

    def test_connection(self):
        amqp = self._get_api()
        assert isinstance(amqp._connection, pika.BlockingConnection)

    def test_connection_invalid(self):
        with pytest.raises(pika.exceptions.AMQPConnectionError):
            AMQPAPI(
                host="0.0.0.0",
                port=1000,
                username="invalid",
                password="invalid",
            )

    def test_interface_publish(self):
        with patch("pika.BlockingConnection") as mock:
            mock.return_value = MockPikaBlockingConnection(parameters=pika.ConnectionParameters())

            amqp = self._get_api()
            amqp.publish("test_exchange", "test_queue", "sample-body")

            assert len(amqp._connection.channels) == 1
            channel = amqp._connection.channels[0]

            assert len(channel.methods) == 3
            assert channel.methods["queue_declare"] == [(("test_queue",), {})]
            assert channel.methods["exchange_declare"] == [(("test_exchange",), {})]
            assert channel.methods["basic_publish"] == [((), dict(
                exchange="test_exchange",
                routing_key="test_queue",
                body="sample-body"
            ))]

    def test_interface_close(self):
        with patch("pika.BlockingConnection") as mock:
            mock.return_value = MockPikaBlockingConnection(parameters=pika.ConnectionParameters())

            amqp = self._get_api()

            amqp.close()

            assert amqp._connection.methods["close"] == [((), {})]

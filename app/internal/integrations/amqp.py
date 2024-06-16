import pika


class AMQPAPI:

    def __init__(self, host: str, port: int, username: str, password: str):
        self._parameters = pika.ConnectionParameters(
            host=host,
            port=port,
            credentials=pika.PlainCredentials(
                username=username,
                password=password
            )
        )
        self._connection = pika.BlockingConnection(parameters=self._parameters)
        self._channel = self._connection.channel()

    def publish(self,
                exchange: str,
                queue: str,
                body: str):
        self._channel.queue_declare(queue)
        self._channel.exchange_declare(exchange)
        self._channel.basic_publish(
            exchange=exchange,
            routing_key=queue,
            body=body
        )

    def close(self):
        self._connection.close()

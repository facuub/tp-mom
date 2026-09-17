from contextlib import contextmanager

import pika
import pika.exceptions

from .middleware import (
    MessageMiddlewareCloseError,
    MessageMiddlewareDeleteError,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareExchange,
    MessageMiddlewareMessageError,
    MessageMiddlewareQueue,
)

PREFETCH_COUNT = 1
EXCHANGE_TYPE = "direct"

DISCONNECTION_ERRORS = (
    pika.exceptions.AMQPConnectionError,
    pika.exceptions.ConnectionWrongStateError,
    pika.exceptions.ChannelWrongStateError,
    OSError,
)


@contextmanager
def _rabbitmq_errors(internal_error=MessageMiddlewareMessageError):
    try:
        yield
    except DISCONNECTION_ERRORS as raised:
        raise MessageMiddlewareDisconnectedError(str(raised)) from raised
    except pika.exceptions.AMQPError as raised:
        raise internal_error(str(raised)) from raised


class _RabbitMQMiddleware:

    def __init__(self, host):
        self._consumer_tag = None
        with _rabbitmq_errors():
            self._connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
            self._channel = self._connection.channel()
            self._channel.basic_qos(prefetch_count=PREFETCH_COUNT)

    def start_consuming(self, on_message_callback):
        with _rabbitmq_errors():
            queue_name = self._consumption_queue()
            self._consumer_tag = self._channel.basic_consume(
                queue=queue_name,
                on_message_callback=_delivery_handler(on_message_callback),
            )
            try:
                self._channel.start_consuming()
            finally:
                self._consumer_tag = None

    def stop_consuming(self):
        if self._consumer_tag is None:
            return
        with _rabbitmq_errors(MessageMiddlewareDisconnectedError):
            self._channel.stop_consuming(self._consumer_tag)

    def close(self):
        try:
            if self._connection.is_open:
                self._connection.close()
        except (pika.exceptions.AMQPError, OSError) as raised:
            raise MessageMiddlewareCloseError(str(raised)) from raised

    def delete(self):
        try:
            self._delete_resource()
        except (pika.exceptions.AMQPError, OSError) as raised:
            raise MessageMiddlewareDeleteError(str(raised)) from raised


def _delivery_handler(on_message_callback):
    def handler(channel, method, properties, body):
        on_message_callback(
            body,
            lambda: channel.basic_ack(method.delivery_tag),
            lambda: channel.basic_nack(method.delivery_tag),
        )

    return handler


class MessageMiddlewareQueueRabbitMQ(_RabbitMQMiddleware, MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        super().__init__(host)
        self._queue_name = queue_name
        with _rabbitmq_errors():
            self._channel.queue_declare(queue=self._queue_name, durable=True)

    def send(self, message):
        with _rabbitmq_errors():
            self._channel.basic_publish(
                exchange="",
                routing_key=self._queue_name,
                body=message,
            )

    def _consumption_queue(self):
        return self._queue_name

    def _delete_resource(self):
        self._channel.queue_delete(queue=self._queue_name)


class MessageMiddlewareExchangeRabbitMQ(_RabbitMQMiddleware, MessageMiddlewareExchange):

    def __init__(self, host, exchange_name, routing_keys):
        super().__init__(host)
        self._exchange_name = exchange_name
        self._routing_keys = list(routing_keys)
        self._queue_name = None
        with _rabbitmq_errors():
            self._channel.exchange_declare(
                exchange=self._exchange_name,
                exchange_type=EXCHANGE_TYPE,
                durable=True,
            )

    def send(self, message):
        with _rabbitmq_errors():
            for routing_key in self._routing_keys:
                self._channel.basic_publish(
                    exchange=self._exchange_name,
                    routing_key=routing_key,
                    body=message,
                )

    def _consumption_queue(self):
        if self._queue_name is None:
            declaration = self._channel.queue_declare(queue="", exclusive=True)
            self._queue_name = declaration.method.queue
            for routing_key in self._routing_keys:
                self._channel.queue_bind(
                    exchange=self._exchange_name,
                    queue=self._queue_name,
                    routing_key=routing_key,
                )
        return self._queue_name

    def _delete_resource(self):
        self._channel.exchange_delete(exchange=self._exchange_name)

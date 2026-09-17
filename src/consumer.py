import io
import time

from datetime import datetime, timezone

from confluent_kafka import Consumer, Producer
from fastavro import schemaless_reader, schemaless_writer

from config import (
    BOOTSTRAP_SERVERS,
    CONSUMER_GROUP,
    DLQ_TOPIC,
    MAX_RETRIES,
    ORDERS_TOPIC,
    RETRY_BACKOFF_SECONDS,
    RETRY_TOPIC,
)

from schema_utils import load_schema


class TemporaryProcessingError(Exception):
    pass


class PermanentProcessingError(Exception):
    pass


def deserialize_data(data, schema):
    buffer = io.BytesIO(data)
    return schemaless_reader(buffer, schema)


def serialize_data(record, schema):
    buffer = io.BytesIO()
    schemaless_writer(buffer, schema, record)
    return buffer.getvalue()


def get_retry_count(headers):

    if not headers:
        return 0

    for key, value in headers:

        if key == "retry_count":

            if isinstance(value, bytes):
                value = value.decode("utf-8")

            return int(value)

    return 0


def process_order(order, retry_count):

    # Simulated temporary failure.
    if (
        order["product"] == "TemporaryItem"
        and retry_count < 2
    ):
        raise TemporaryProcessingError(
            "Simulated temporary service failure."
        )

    # Simulated permanent failure.
    if order["product"] == "InvalidItem":
        raise PermanentProcessingError(
            "Invalid product cannot be processed."
        )


def send_to_dlq(
    producer,
    order,
    error,
    retry_count,
    dlq_schema,
):

    failed_order = {
        "orderId": order["orderId"],
        "product": order["product"],
        "price": float(order["price"]),
        "errorType": type(error).__name__,
        "errorMessage": str(error),
        "retryCount": retry_count,
        "failedAt": datetime.now(
            timezone.utc
        ).isoformat(),
    }

    avro_data = serialize_data(
        failed_order,
        dlq_schema,
    )

    producer.produce(
        topic=DLQ_TOPIC,
        key=order["orderId"],
        value=avro_data,
    )

    producer.flush()

    print(
        f"DLQ SENT -> {DLQ_TOPIC} | "
        f"orderId={order['orderId']}"
    )


def main():

    order_schema = load_schema(
        "order.avsc"
    )

    dlq_schema = load_schema(
        "dlq.avsc"
    )

    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": CONSUMER_GROUP,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )

    retry_producer = Producer(
        {
            "bootstrap.servers":
                BOOTSTRAP_SERVERS
        }
    )

    dlq_producer = Producer(
        {
            "bootstrap.servers":
                BOOTSTRAP_SERVERS
        }
    )

    consumer.subscribe(
        [
            ORDERS_TOPIC,
            RETRY_TOPIC,
        ]
    )

    total_price = 0.0
    order_count = 0

    print(
        f"Listening on: "
        f"{ORDERS_TOPIC}, {RETRY_TOPIC}"
    )

    print("Press Ctrl+C to stop.")
    print()

    try:

        while True:

            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                print(
                    f"Consumer error: "
                    f"{msg.error()}"
                )
                continue

            order = deserialize_data(
                msg.value(),
                order_schema,
            )

            retry_count = get_retry_count(
                msg.headers()
            )

            try:

                process_order(
                    order,
                    retry_count,
                )

                order_count += 1

                total_price += float(
                    order["price"]
                )

                running_average = (
                    total_price /
                    order_count
                )

                print(
                    f"SUCCESS | "
                    f"orderId={order['orderId']} | "
                    f"product={order['product']} | "
                    f"price={order['price']:.2f} | "
                    f"retry={retry_count}"
                )

                print(
                    f"Running Average = "
                    f"{running_average:.2f}"
                )

                print(
                    f"Orders Processed = "
                    f"{order_count}"
                )

                print("-" * 60)

                consumer.commit(
                    message=msg,
                    asynchronous=False,
                )

            except TemporaryProcessingError as exc:

                next_retry = (
                    retry_count + 1
                )

                print(
                    f"TEMPORARY FAILURE | "
                    f"orderId={order['orderId']} | "
                    f"retry="
                    f"{next_retry}/{MAX_RETRIES}"
                )

                print(
                    f"Reason: {exc}"
                )

                if next_retry <= MAX_RETRIES:

                    delay = (
                        RETRY_BACKOFF_SECONDS
                        * next_retry
                    )

                    print(
                        f"Waiting {delay} seconds "
                        f"before retry..."
                    )

                    time.sleep(delay)

                    avro_data = serialize_data(
                        order,
                        order_schema,
                    )

                    retry_producer.produce(
                        topic=RETRY_TOPIC,
                        key=order["orderId"],
                        value=avro_data,
                        headers=[
                            (
                                "retry_count",
                                str(
                                    next_retry
                                ).encode(
                                    "utf-8"
                                ),
                            )
                        ],
                    )

                    retry_producer.flush()

                    print(
                        f"RETRY SENT -> "
                        f"{RETRY_TOPIC} | "
                        f"orderId="
                        f"{order['orderId']} | "
                        f"retry={next_retry}"
                    )

                else:

                    print(
                        f"RETRIES EXHAUSTED | "
                        f"orderId="
                        f"{order['orderId']}"
                    )

                    send_to_dlq(
                        dlq_producer,
                        order,
                        exc,
                        retry_count,
                        dlq_schema,
                    )

                print("-" * 60)

                consumer.commit(
                    message=msg,
                    asynchronous=False,
                )

            except PermanentProcessingError as exc:

                print(
                    f"PERMANENT FAILURE | "
                    f"orderId={order['orderId']}"
                )

                print(
                    f"Reason: {exc}"
                )

                send_to_dlq(
                    dlq_producer,
                    order,
                    exc,
                    retry_count,
                    dlq_schema,
                )

                print("-" * 60)

                consumer.commit(
                    message=msg,
                    asynchronous=False,
                )

    except KeyboardInterrupt:

        print("\nStopping consumer...")

    finally:

        consumer.close()
        retry_producer.flush()
        dlq_producer.flush()


if __name__ == "__main__":
    main()
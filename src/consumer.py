import io
import time

from confluent_kafka import Consumer, Producer
from fastavro import schemaless_reader, schemaless_writer

from config import (
    BOOTSTRAP_SERVERS,
    CONSUMER_GROUP,
    MAX_RETRIES,
    ORDERS_TOPIC,
    RETRY_BACKOFF_SECONDS,
    RETRY_TOPIC,
)

from schema_utils import load_schema


class TemporaryProcessingError(Exception):
    pass


def deserialize_order(data, schema):
    buffer = io.BytesIO(data)

    return schemaless_reader(
        buffer,
        schema,
    )


def serialize_order(order, schema):
    buffer = io.BytesIO()

    schemaless_writer(
        buffer,
        schema,
        order,
    )

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

    # Simulated temporary error.
    #
    # TemporaryItem fails on:
    # attempt 0
    # retry 1
    #
    # It succeeds on retry 2.

    if (
        order["product"] == "TemporaryItem"
        and retry_count < 2
    ):
        raise TemporaryProcessingError(
            "Simulated temporary service failure."
        )


def main():

    schema = load_schema("order.avsc")

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
            "bootstrap.servers": BOOTSTRAP_SERVERS
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
                    f"Consumer error: {msg.error()}"
                )
                continue

            order = deserialize_order(
                msg.value(),
                schema,
            )

            retry_count = get_retry_count(
                msg.headers()
            )

            try:

                process_order(
                    order,
                    retry_count,
                )

                # -------------------------
                # Successful processing
                # -------------------------

                order_count += 1

                total_price += float(
                    order["price"]
                )

                running_average = (
                    total_price / order_count
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

                next_retry = retry_count + 1

                print(
                    f"TEMPORARY FAILURE | "
                    f"orderId={order['orderId']} | "
                    f"retry={next_retry}/{MAX_RETRIES}"
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

                    avro_data = serialize_order(
                        order,
                        schema,
                    )

                    retry_producer.produce(
                        topic=RETRY_TOPIC,
                        key=order["orderId"],
                        value=avro_data,
                        headers=[
                            (
                                "retry_count",
                                str(next_retry).encode(
                                    "utf-8"
                                ),
                            )
                        ],
                    )

                    retry_producer.flush()

                    print(
                        f"RETRY SENT -> "
                        f"{RETRY_TOPIC} | "
                        f"orderId={order['orderId']} | "
                        f"retry={next_retry}"
                    )

                else:

                    print(
                        f"RETRIES EXHAUSTED | "
                        f"orderId={order['orderId']}"
                    )

                    print(
                        "DLQ handling will be "
                        "added in the next stage."
                    )

                print("-" * 60)

                # Commit only after the retry copy
                # has successfully been produced.

                consumer.commit(
                    message=msg,
                    asynchronous=False,
                )

    except KeyboardInterrupt:

        print("\nStopping consumer...")

    finally:

        consumer.close()

        retry_producer.flush()


if __name__ == "__main__":
    main()
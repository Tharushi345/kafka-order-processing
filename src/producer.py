import argparse
import io
import random
import time

from confluent_kafka import Producer
from fastavro import schemaless_writer

from config import BOOTSTRAP_SERVERS, ORDERS_TOPIC
from schema_utils import load_schema


NORMAL_PRODUCTS = [
    "Item1",
    "Item2",
    "Item3",
    "Item4",
    "Item5",
]

RETRY_DEMO_PRODUCTS = [
    "Item1",
    "TemporaryItem",
    "Item2",
    "Item3",
]

ERROR_DEMO_PRODUCTS = [
    "Item1",
    "TemporaryItem",
    "Item2",
    "InvalidItem",
    "Item3",
]


def serialize_order(order, schema):
    buffer = io.BytesIO()
    schemaless_writer(buffer, schema, order)
    return buffer.getvalue()


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(
            f"Delivered to {msg.topic()} "
            f"[partition={msg.partition()}, offset={msg.offset()}]"
        )


def main():

    parser = argparse.ArgumentParser(
        description="Produce Avro serialized order messages."
    )

    parser.add_argument(
        "--count",
        type=int,
        default=10,
    )

    parser.add_argument(
        "--interval",
        type=float,
        default=1.0,
    )

    parser.add_argument(
        "--demo-retries",
        action="store_true",
    )

    parser.add_argument(
        "--demo-errors",
        action="store_true",
        help="Generate temporary and permanent failure examples.",
    )

    args = parser.parse_args()

    schema = load_schema("order.avsc")

    producer = Producer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS
        }
    )

    if args.demo_errors:
        products = ERROR_DEMO_PRODUCTS
    elif args.demo_retries:
        products = RETRY_DEMO_PRODUCTS
    else:
        products = NORMAL_PRODUCTS

    for i in range(args.count):

        order = {
            "orderId": str(1001 + i),
            "product": products[i % len(products)],
            "price": round(
                random.uniform(10.0, 500.0),
                2,
            ),
        }

        avro_data = serialize_order(
            order,
            schema,
        )

        print(f"Producing: {order}")

        producer.produce(
            topic=ORDERS_TOPIC,
            key=order["orderId"],
            value=avro_data,
            callback=delivery_report,
        )

        producer.poll(0)

        time.sleep(args.interval)

    producer.flush()

    print("Finished producing orders.")


if __name__ == "__main__":
    main()
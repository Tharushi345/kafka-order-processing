import argparse
import io
import random
import time

from confluent_kafka import Producer
from fastavro import schemaless_writer

from config import BOOTSTRAP_SERVERS, ORDERS_TOPIC
from schema_utils import load_schema


PRODUCTS = [
    "Item1",
    "Item2",
    "Item3",
    "Item4",
    "Item5",
]


def serialize_order(order, schema):
    buffer = io.BytesIO()

    schemaless_writer(
        buffer,
        schema,
        order,
    )

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
        description="Produce Avro serialized order messages to Kafka."
    )

    parser.add_argument(
        "--count",
        type=int,
        default=10,
        help="Number of orders to produce.",
    )

    parser.add_argument(
        "--interval",
        type=float,
        default=1.0,
        help="Delay between messages in seconds.",
    )

    args = parser.parse_args()

    schema = load_schema("order.avsc")

    producer = Producer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS
        }
    )

    for i in range(args.count):

        order = {
            "orderId": str(1001 + i),
            "product": random.choice(PRODUCTS),
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
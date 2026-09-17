import io

from confluent_kafka import Consumer
from fastavro import schemaless_reader

from config import (
    BOOTSTRAP_SERVERS,
    CONSUMER_GROUP,
    ORDERS_TOPIC,
)

from schema_utils import load_schema


def deserialize_order(data, schema):
    buffer = io.BytesIO(data)

    return schemaless_reader(
        buffer,
        schema,
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

    consumer.subscribe(
        [ORDERS_TOPIC]
    )

    total_price = 0.0
    order_count = 0

    print(f"Listening for orders on topic: {ORDERS_TOPIC}")
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

            order_count += 1

            total_price += float(
                order["price"]
            )

            running_average = (
                total_price / order_count
            )

            print(
                f"Received Order: "
                f"orderId={order['orderId']}, "
                f"product={order['product']}, "
                f"price={order['price']:.2f}"
            )

            print(
                f"Running Average: "
                f"{running_average:.2f}"
            )

            print(
                f"Orders Processed: "
                f"{order_count}"
            )

            print("-" * 50)

            consumer.commit(
                message=msg,
                asynchronous=False,
            )

    except KeyboardInterrupt:

        print("\nStopping consumer...")

    finally:

        consumer.close()


if __name__ == "__main__":
    main()
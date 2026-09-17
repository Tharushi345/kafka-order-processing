import io

from confluent_kafka import Consumer
from fastavro import schemaless_reader

from config import (
    BOOTSTRAP_SERVERS,
    DLQ_TOPIC,
)

from schema_utils import load_schema


def deserialize_data(data, schema):
    buffer = io.BytesIO(data)
    return schemaless_reader(
        buffer,
        schema,
    )


def main():

    schema = load_schema(
        "dlq.avsc"
    )

    consumer = Consumer(
        {
            "bootstrap.servers":
                BOOTSTRAP_SERVERS,

            "group.id":
                "dlq-monitor-group",

            "auto.offset.reset":
                "earliest",
        }
    )

    consumer.subscribe(
        [DLQ_TOPIC]
    )

    print(
        f"Watching DLQ: {DLQ_TOPIC}"
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

            failed_order = (
                deserialize_data(
                    msg.value(),
                    schema,
                )
            )

            print("DLQ MESSAGE")

            print(
                f"Order ID: "
                f"{failed_order['orderId']}"
            )

            print(
                f"Product: "
                f"{failed_order['product']}"
            )

            print(
                f"Price: "
                f"{failed_order['price']:.2f}"
            )

            print(
                f"Error Type: "
                f"{failed_order['errorType']}"
            )

            print(
                f"Error: "
                f"{failed_order['errorMessage']}"
            )

            print(
                f"Retry Count: "
                f"{failed_order['retryCount']}"
            )

            print(
                f"Failed At: "
                f"{failed_order['failedAt']}"
            )

            print("-" * 60)

    except KeyboardInterrupt:

        print(
            "\nStopping DLQ consumer..."
        )

    finally:

        consumer.close()


if __name__ == "__main__":
    main()
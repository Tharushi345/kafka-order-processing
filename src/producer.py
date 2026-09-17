import argparse
import io
import random
import time

from confluent_kafka import Producer
from fastavro import schemaless_writer

from config import BOOTSTRAP_SERVERS, ORDERS_TOPIC
from schema_utils import load_schema


# ---------------------------------------------------------
# Normal product set
# ---------------------------------------------------------

NORMAL_PRODUCTS = [
    "Item1",
    "Item2",
    "Item3",
    "Item4",
    "Item5",
]


# ---------------------------------------------------------
# Retry-only demonstration set
# ---------------------------------------------------------

RETRY_DEMO_PRODUCTS = [
    "Item1",
    "TemporaryItem",
    "Item2",
    "Item3",
]


# ---------------------------------------------------------
# Full demonstration workload
#
# 20 orders:
#   16 normal orders
#   2 temporary failures
#   2 permanent failures
#
# TemporaryItem -> retry mechanism
# InvalidItem   -> Dead Letter Queue
# ---------------------------------------------------------

ERROR_DEMO_PRODUCTS = [
    "Item1",          # 1001
    "Item2",          # 1002
    "Item3",          # 1003
    "Item4",          # 1004
    "TemporaryItem",  # 1005 - retry

    "Item5",          # 1006
    "Item1",          # 1007
    "Item2",          # 1008
    "Item3",          # 1009
    "InvalidItem",    # 1010 - DLQ

    "Item4",          # 1011
    "Item5",          # 1012
    "Item1",          # 1013
    "Item2",          # 1014
    "TemporaryItem",  # 1015 - retry

    "Item3",          # 1016
    "Item4",          # 1017
    "Item5",          # 1018
    "Item1",          # 1019
    "InvalidItem",    # 1020 - DLQ
]


# ---------------------------------------------------------
# Avro serialization
# ---------------------------------------------------------

def serialize_order(order, schema):
    """
    Serialize a Python order dictionary into Avro binary data.
    """

    buffer = io.BytesIO()

    schemaless_writer(
        buffer,
        schema,
        order,
    )

    return buffer.getvalue()


# ---------------------------------------------------------
# Kafka delivery callback
# ---------------------------------------------------------

def delivery_report(err, msg):
    """
    Called by Kafka when a message is delivered or delivery fails.
    """

    if err is not None:

        print(
            f"Delivery failed: {err}"
        )

    else:

        print(
            f"Delivered to {msg.topic()} "
            f"[partition={msg.partition()}, "
            f"offset={msg.offset()}]"
        )


# ---------------------------------------------------------
# Main producer
# ---------------------------------------------------------

def main():

    parser = argparse.ArgumentParser(
        description=(
            "Produce Avro serialized order messages "
            "to Apache Kafka."
        )
    )

    parser.add_argument(
        "--count",
        type=int,
        default=10,
        help="Number of orders to generate.",
    )

    parser.add_argument(
        "--interval",
        type=float,
        default=1.0,
        help=(
            "Delay between produced orders "
            "in seconds."
        ),
    )

    parser.add_argument(
        "--demo-retries",
        action="store_true",
        help=(
            "Generate TemporaryItem messages "
            "to demonstrate retry handling."
        ),
    )

    parser.add_argument(
        "--demo-errors",
        action="store_true",
        help=(
            "Generate both temporary and permanent "
            "processing failure test cases."
        ),
    )

    args = parser.parse_args()


    # -----------------------------------------------------
    # Load Avro schema
    # -----------------------------------------------------

    schema = load_schema(
        "order.avsc"
    )


    # -----------------------------------------------------
    # Configure Kafka producer
    # -----------------------------------------------------

    producer = Producer(
        {
            "bootstrap.servers":
                BOOTSTRAP_SERVERS
        }
    )


    # -----------------------------------------------------
    # Select workload
    # -----------------------------------------------------

    if args.demo_errors:

        products = ERROR_DEMO_PRODUCTS

        print()
        print(
            "Running FULL FAILURE DEMONSTRATION"
        )
        print(
            "TemporaryItem -> Retry mechanism"
        )
        print(
            "InvalidItem   -> Dead Letter Queue"
        )
        print()

    elif args.demo_retries:

        products = RETRY_DEMO_PRODUCTS

        print()
        print(
            "Running RETRY DEMONSTRATION"
        )
        print()

    else:

        products = NORMAL_PRODUCTS

        print()
        print(
            "Running NORMAL ORDER GENERATION"
        )
        print()


    # -----------------------------------------------------
    # Produce orders
    # -----------------------------------------------------

    for i in range(args.count):

        product = products[
            i % len(products)
        ]

        order = {

            "orderId":
                str(1001 + i),

            "product":
                product,

            "price":
                round(
                    random.uniform(
                        10.0,
                        500.0,
                    ),
                    2,
                ),
        }


        # ---------------------------------------------
        # Convert order into Avro bytes
        # ---------------------------------------------

        avro_data = serialize_order(
            order,
            schema,
        )


        # ---------------------------------------------
        # Display generated order
        # ---------------------------------------------

        print(
            f"Producing | "
            f"orderId={order['orderId']} | "
            f"product={order['product']} | "
            f"price={order['price']:.2f}"
        )


        # ---------------------------------------------
        # Send to Kafka
        # ---------------------------------------------

        producer.produce(

            topic=ORDERS_TOPIC,

            key=order["orderId"],

            value=avro_data,

            callback=delivery_report,
        )


        # Allow Kafka callbacks to execute
        producer.poll(0)


        # Delay between generated orders
        time.sleep(
            args.interval
        )


    # -----------------------------------------------------
    # Ensure all buffered messages are sent
    # -----------------------------------------------------

    producer.flush()


    print()
    print(
        f"Finished producing "
        f"{args.count} orders."
    )
    print()


# ---------------------------------------------------------
# Program entry point
# ---------------------------------------------------------

if __name__ == "__main__":
    main()
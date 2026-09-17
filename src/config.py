BOOTSTRAP_SERVERS = "localhost:9092"

ORDERS_TOPIC = "orders"
RETRY_TOPIC = "orders-retry"
DLQ_TOPIC = "orders-dlq"

CONSUMER_GROUP = "order-processing-group"

MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 2
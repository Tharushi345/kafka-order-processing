# Kafka Order Processing System

A Kafka-based order processing system developed to demonstrate
real-time order processing using Apache Kafka and Avro.

The system supports:

- Kafka producer and consumer communication
- Avro serialization and deserialization
- Real-time running average calculation
- Retry handling for temporary processing failures
- Dead Letter Queue handling for permanent failures
- Incremental Git-based development

---

# 1. Order Message

Each order follows the Avro schema:

`schemas/order.avsc`

The order contains:

| Field | Type | Description |
|---|---|---|
| orderId | string | Unique identifier for an order |
| product | string | Purchased product |
| price | float | Price of the product |

Example logical order:

```json
{
  "orderId": "1001",
  "product": "Item1",
  "price": 125.50
}
```

Before being transmitted through Kafka, the Python dictionary
is serialized into Avro binary data.

---

# 2. Architecture

```text
                   +----------------+
                   |    Producer    |
                   +-------+--------+
                           |
                           | Avro Order
                           v
                   +----------------+
                   |     orders     |
                   +-------+--------+
                           |
                           v
                   +----------------+
                   |    Consumer    |
                   +-------+--------+
                           |
          +----------------+----------------+
          |                |                |
          v                v                v
       SUCCESS      TEMPORARY FAILURE   PERMANENT FAILURE
          |                |                |
          v                v                v
   Running Average    orders-retry      orders-dlq
                           |                |
                           |                v
                           |          DLQ Consumer
                           |
                           +-------> Consumer
```

---

# 3. Kafka Topics

Three Kafka topics are used.

## orders

Contains newly generated orders.

## orders-retry

Contains orders that experienced temporary processing failures.

## orders-dlq

Contains orders that cannot be successfully processed.

---

# 4. Avro Serialization

The project uses:

- Apache Avro
- fastavro
- schemas/order.avsc
- schemas/dlq.avsc

Producer flow:

```text
Python Dictionary
       |
       v
Avro Schema
       |
       v
fastavro
       |
       v
Binary Avro Data
       |
       v
Kafka
```

Consumer flow:

```text
Kafka
  |
  v
Binary Avro Data
  |
  v
fastavro
  |
  v
Python Dictionary
```

---

# 5. Real-Time Aggregation

The consumer maintains a running average of successfully
processed order prices.

```text
                   Sum of Successful Order Prices
Running Average = ---------------------------------
                  Number of Successful Orders
```

Only successfully processed orders contribute to the average.

Temporary failures contribute only after they eventually
succeed.

Permanently failed messages sent to the DLQ are not included.

---

# 6. Retry Handling

The product:

```text
TemporaryItem
```

is deliberately used to simulate a temporary processing
failure.

The retry flow is:

```text
Order
  |
  v
Consumer
  |
  X
Temporary Failure
  |
  v
Increment Retry Count
  |
  v
Backoff Delay
  |
  v
orders-retry
  |
  v
Consumer
```

The current configuration uses:

```text
MAX_RETRIES = 3
```

The retry count is stored in the Kafka message header:

```text
retry_count
```

During the demonstration, each `TemporaryItem` fails twice
and then succeeds.

---

# 7. Dead Letter Queue

The product:

```text
InvalidItem
```

is deliberately used to represent a permanently invalid
message.

It causes:

```text
PermanentProcessingError
```

The failed order is then written to:

```text
orders-dlq
```

A DLQ record contains:

- order ID
- product
- price
- error type
- error message
- retry count
- failure timestamp

Example:

```text
DLQ MESSAGE
Order ID: 1010
Product: InvalidItem
Price: 185.57
Error Type: PermanentProcessingError
Error: Invalid product cannot be processed.
Retry Count: 0
Failed At: 2026-09-17T13:08:48+00:00
```

---

# 8. Project Structure

```text
kafka-order-processing/
│
├── docker-compose.yml
├── requirements.txt
├── README.md
│
├── schemas/
│   ├── order.avsc
│   └── dlq.avsc
│
└── src/
    ├── config.py
    ├── schema_utils.py
    ├── create_topics.py
    ├── producer.py
    ├── consumer.py
    └── dlq_consumer.py
```

---

# 9. Start Kafka

Run:

```bash
docker compose up -d
```

Check:

```bash
docker compose ps
```

Kafka should show:

```text
Up ... (healthy)
```

---

# 10. Python Environment

Create the environment:

```bash
python -m venv .venv
```

Activate it:

```bash
source .venv/bin/activate
```

Install dependencies:

```bash
pip install -r requirements.txt
```

---

# 11. Create Kafka Topics

Run:

```bash
python src/create_topics.py
```

The topics are:

```text
orders
orders-retry
orders-dlq
```

Verify:

```bash
docker exec kafka kafka-topics \
  --bootstrap-server kafka:29092 \
  --list
```

---

# 12. Live Demonstration

The live demonstration uses 20 original order messages.

The workload contains:

```text
Normal orders       = 16
TemporaryItem       = 2
InvalidItem         = 2
Total               = 20
```

Because both `TemporaryItem` messages eventually succeed:

```text
Successfully processed orders = 18
DLQ orders                     = 2
```

---

## Demo Order Sequence

```text
1001  Item1
1002  Item2
1003  Item3
1004  Item4
1005  TemporaryItem

1006  Item5
1007  Item1
1008  Item2
1009  Item3
1010  InvalidItem

1011  Item4
1012  Item5
1013  Item1
1014  Item2
1015  TemporaryItem

1016  Item3
1017  Item4
1018  Item5
1019  Item1
1020  InvalidItem
```

---

# 13. Terminal 1 - Main Consumer

Open the first terminal.

Activate the environment:

```bash
source .venv/bin/activate
```

Run:

```bash
python src/consumer.py
```

This terminal demonstrates:

- normal order processing
- running average
- temporary failures
- retries
- permanent failures
- DLQ routing

---

# 14. Terminal 2 - DLQ Monitor

Open a second terminal.

Activate:

```bash
source .venv/bin/activate
```

Run:

```bash
python src/dlq_consumer.py
```

This terminal displays messages arriving in:

```text
orders-dlq
```

---

# 15. Terminal 3 - Producer

Open a third terminal.

Activate:

```bash
source .venv/bin/activate
```

Start the demonstration:

```bash
python src/producer.py \
  --count 20 \
  --interval 0.5 \
  --demo-errors
```

---

# 16. Expected Normal Processing

Normal items should produce output such as:

```text
SUCCESS | orderId=1001 | product=Item1 | price=120.45
Running Average = 120.45
Orders Processed = 1
```

The running average changes after each successfully
processed order.

---

# 17. Expected Retry Processing

For:

```text
TemporaryItem
```

the consumer should show:

```text
TEMPORARY FAILURE
        |
        v
Retry 1
        |
        v
TEMPORARY FAILURE
        |
        v
Retry 2
        |
        v
SUCCESS
```

Two `TemporaryItem` orders are generated.

Each one generates two retry messages.

Therefore the demonstration produces:

```text
4 retry-topic messages
```

---

# 18. Expected DLQ Processing

For:

```text
InvalidItem
```

the consumer should show:

```text
PERMANENT FAILURE
        |
        v
orders-dlq
```

The DLQ consumer should display:

```text
DLQ MESSAGE
Order ID: ...
Product: InvalidItem
Error Type: PermanentProcessingError
...
```

Two `InvalidItem` orders are produced.

Therefore:

```text
DLQ records = 2
```

---

# 19. Check Topic Offsets

Check the original orders:

```bash
docker exec kafka kafka-get-offsets \
  --bootstrap-server kafka:29092 \
  --topic orders
```

Check retries:

```bash
docker exec kafka kafka-get-offsets \
  --bootstrap-server kafka:29092 \
  --topic orders-retry
```

Check DLQ:

```bash
docker exec kafka kafka-get-offsets \
  --bootstrap-server kafka:29092 \
  --topic orders-dlq
```

For a completely fresh Kafka environment, the expected
logical values after one demonstration are:

```text
orders:0:20
orders-retry:0:4
orders-dlq:0:2
```

If previous tests have already been executed, these offsets
will be larger because Kafka offsets are cumulative.

---

# 20. Stop the Application

Stop Python consumers using:

```text
Ctrl + C
```

Stop Kafka with:

```bash
docker compose down
```

---

# Technologies

- Apache Kafka
- Kafka KRaft mode
- Docker
- Docker Compose
- Python
- confluent-kafka
- Apache Avro
- fastavro
- Git
- GitHub
- GitHub Codespaces

---

# Failure Simulation

`TemporaryItem` and `InvalidItem` are deliberately injected
test cases.

Kafka is responsible for transporting and storing messages.

The Python application implements:

- retry policy
- retry backoff
- retry count tracking
- permanent failure detection
- DLQ routing
- running average calculation
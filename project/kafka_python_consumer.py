from kafka import KafkaConsumer
import json
import os
import pyarrow as pa
import pyarrow.parquet as pq


KAFKA_SERVER = "localhost:9092"
TOPIC = "ecommerce_orders"

BRONZE_DIR = "D:/DataEngineering/data/bronze/orders"


consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_SERVER,
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)


print("Connected to Kafka.")
print("Waiting for orders...")
print("Press Ctrl+C to stop.\n")


records = []

try:
    for message in consumer:
        order = message.value

        records.append(order)

        print(
            f"Received: {order.get('order_id')} | "
            f"{order.get('city')} | "
            f"₹{order.get('amount')}"
        )

        # Write every 10 records
        if len(records) >= 10:

            os.makedirs(BRONZE_DIR, exist_ok=True)

            table = pa.Table.from_pylist(records)

            existing_files = [
                f for f in os.listdir(BRONZE_DIR)
                if f.startswith("kafka_batch_") and f.endswith(".parquet")
            ]

            batch_id = len(existing_files)

            output_file = os.path.join(
                BRONZE_DIR,
                f"kafka_batch_{batch_id}.parquet"
            )

            pq.write_table(
                table,
                output_file,
                compression="snappy"
            )

            print(
                f"\nBronze batch written: {output_file}"
            )
            print(
                f"Records written: {len(records)}\n"
            )

            records = []

except KeyboardInterrupt:

    print("\nConsumer stopped.")

    # Save remaining records
    if records:

        os.makedirs(BRONZE_DIR, exist_ok=True)

        table = pa.Table.from_pylist(records)

        existing_files = [
            f for f in os.listdir(BRONZE_DIR)
            if f.startswith("kafka_batch_") and f.endswith(".parquet")
        ]

        batch_id = len(existing_files)

        output_file = os.path.join(
            BRONZE_DIR,
            f"kafka_batch_{batch_id}.parquet"
        )

        pq.write_table(
            table,
            output_file,
            compression="snappy"
        )

        print(
            f"Final Bronze batch written: {output_file}"
        )

finally:
    consumer.close()
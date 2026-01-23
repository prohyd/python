from kafka import KafkaConsumer
import json
import sqlite3
import logging
import time
import sys

BOOTSTRAP_SERVERS = "141.105.71.190:9092"
TOPIC = "auto-topic"
GROUP_ID = "json-to-sqlite"
DB_NAME = "data.db"

WAIT_TIMEOUT_SEC = 30


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)


def save_to_sqlite(data: dict):
    table = data["table"]
    columns = data["columns"]
    rows = data["rows"]

    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    columns_sql = ", ".join(f"{c} TEXT" for c in columns)
    cursor.execute(
        f"CREATE TABLE IF NOT EXISTS {table} ({columns_sql})"
    )

    placeholders = ", ".join("?" for _ in columns)
    cursor.executemany(
        f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})",
        rows
    )

    conn.commit()
    conn.close()

    logging.info(f"Inserted {len(rows)} rows into table '{table}'")


def run_consumer():
    logging.info("Connecting to Kafka...")

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        consumer_timeout_ms=1000
    )

    logging.info(f"Subscribed to topic '{TOPIC}'")
    logging.info("Waiting for messages...")

    start_time = time.time()

    while True:
        for message in consumer:
            data = message.value
            if {"table", "columns", "rows"} <= data.keys():
                save_to_sqlite(data)
                consumer.close()
                logging.info("Done")
                return

        if time.time() - start_time > WAIT_TIMEOUT_SEC:
            logging.warning("Timeout reached, no messages received")
            consumer.close()
            sys.exit(1)


if __name__ == "__main__":
    run_consumer()

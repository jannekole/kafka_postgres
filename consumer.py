from kafka import KafkaConsumer
from psycopg2.extras import RealDictCursor
import psycopg2

import constants
import secrets


class Consumer:
    def __init__(self):

        self.consumer = KafkaConsumer(
            constants.TOPIC,
            bootstrap_servers=secrets.KAFKA_URL,
            client_id="client-1",
            group_id="group-1",
            security_protocol="SSL",
            ssl_cafile="ca.pem",
            ssl_certfile="service.cert",
            ssl_keyfile="service.key",
        )
        print('Connected to kafka.')

        self.sql_connection = psycopg2.connect(secrets.SQL_URL)
        self.sql_connection.autocommit = True
        self.cursor = self.sql_connection.cursor(cursor_factory=RealDictCursor)
        print('Connected to PostgreSQL.')

    def poll(self):
        raw_msgs = self.consumer.poll(timeout_ms=1000)
        for tp, msgs in raw_msgs.items():
            for msg in msgs:
                self.handle_message(msg)

    def handle_message(self, message):
        self.cursor.execute("INSERT INTO test VALUES (%s);", (message.value,))
        print('received')


if __name__ == "__main__":
    consumer = Consumer()
    while True:
        consumer.poll()

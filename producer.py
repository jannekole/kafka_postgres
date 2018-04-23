from kafka import KafkaProducer

import constants
import secrets


class Producer:
    def __init__(self):

        self.producer = KafkaProducer(
            bootstrap_servers=secrets.KAFKA_URL,
            security_protocol="SSL",
            ssl_cafile="ca.pem",
            ssl_certfile="service.cert",
            ssl_keyfile="service.key",
        )

    def send(self, message):
        print("Sending: {}".format(message))
        self.producer.send(constants.TOPIC, message.encode("utf-8"))
        self.producer.flush()


if __name__ == "__main__":
    producer = Producer()
    producer.send("Test message")

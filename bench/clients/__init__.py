from __future__ import annotations

from .base import ConsumerClient, OnAck, ProducerClient


def make_producer(name: str) -> ProducerClient:
    if name == "kafka-python":
        from .kafka_python import KafkaPythonProducer

        return KafkaPythonProducer()
    if name == "confluent":
        from .confluent import ConfluentProducer

        return ConfluentProducer()
    if name == "aiokafka":
        from .aiokafka_client import AiokafkaProducer

        return AiokafkaProducer()
    raise ValueError(f"unknown client: {name}")


def make_consumer(name: str) -> ConsumerClient:
    if name == "kafka-python":
        from .kafka_python import KafkaPythonConsumer

        return KafkaPythonConsumer()
    if name == "confluent":
        from .confluent import ConfluentConsumer

        return ConfluentConsumer()
    if name == "aiokafka":
        from .aiokafka_client import AiokafkaConsumer

        return AiokafkaConsumer()
    raise ValueError(f"unknown client: {name}")


__all__ = ["ProducerClient", "ConsumerClient", "OnAck", "make_producer", "make_consumer"]

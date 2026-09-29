"""
AeroStream Kafka Producer Scenarios Registry.
"""

from typing import Dict, Type

from scenarios.base import BaseScenario
from scenarios.basic import BasicProduceScenario
from scenarios.compression import CompressionScenario
from scenarios.headers import MessageHeadersScenario
from scenarios.idempotence import IdempotentProducerScenario
from scenarios.keys import PartitionRoutingScenario
from scenarios.large import LargePayloadScenario
from scenarios.security import SecurityScenario
from scenarios.transactions import TransactionalProducerScenario

SCENARIO_REGISTRY: Dict[str, Type[BaseScenario]] = {
    "basic": BasicProduceScenario,
    "compression": CompressionScenario,
    "headers": MessageHeadersScenario,
    "keys": PartitionRoutingScenario,
    "large": LargePayloadScenario,
    "idempotence": IdempotentProducerScenario,
    "transactions": TransactionalProducerScenario,
    "security": SecurityScenario,
}

__all__ = [
    "BaseScenario",
    "BasicProduceScenario",
    "CompressionScenario",
    "MessageHeadersScenario",
    "PartitionRoutingScenario",
    "LargePayloadScenario",
    "IdempotentProducerScenario",
    "TransactionalProducerScenario",
    "SecurityScenario",
    "SCENARIO_REGISTRY",
]

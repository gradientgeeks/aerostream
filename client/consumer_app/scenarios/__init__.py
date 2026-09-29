"""AeroStream Kafka Consumer Test Scenarios Package."""

from typing import Dict, Type

from .base import BaseScenario
from .group_rebalance import GroupRebalanceScenario
from .header_payload import HeaderPayloadScenario
from .long_polling import LongPollingScenario
from .multi_consumer import MultiConsumerScenario
from .offset_management import OffsetManagementScenario
from .security_auth import SecurityAuthScenario
from .simple_consumer import SimpleConsumerScenario
from .txn_isolation import TxnIsolationScenario

SCENARIO_MAP: Dict[str, Type[BaseScenario]] = {
    "simple": SimpleConsumerScenario,
    "group": GroupRebalanceScenario,
    "rebalance": MultiConsumerScenario,
    "offsets": OffsetManagementScenario,
    "longpoll": LongPollingScenario,
    "isolation": TxnIsolationScenario,
    "headers": HeaderPayloadScenario,
    "security": SecurityAuthScenario,
}

__all__ = [
    "BaseScenario",
    "SimpleConsumerScenario",
    "GroupRebalanceScenario",
    "MultiConsumerScenario",
    "OffsetManagementScenario",
    "LongPollingScenario",
    "TxnIsolationScenario",
    "HeaderPayloadScenario",
    "SecurityAuthScenario",
    "SCENARIO_MAP",
]

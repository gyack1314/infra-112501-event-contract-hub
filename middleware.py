"""顶层兼容入口，便于 ``from middleware import ContractHub``。"""

from typing import Any

from contract_hub.core import (
    ConflictError,
    ContractHub,
    ContractValidationError,
    HubError,
    NotFoundError,
    fresh_state,
)

EventContractMiddleware = ContractHub
ContractMiddleware = ContractHub
Middleware = ContractHub


def create_state() -> dict[str, Any]:
    """创建可 JSON 持久化的空状态。"""

    return fresh_state()


def handle_event(state: dict[str, Any], event: dict[str, Any]) -> dict[str, Any]:
    """在给定状态上应用一条操作并返回结果；调用方负责持久化 state。"""

    return ContractHub(state).apply(event)


process_event = handle_event

__all__ = [
    "ContractHub",
    "EventContractMiddleware",
    "ContractMiddleware",
    "Middleware",
    "HubError",
    "NotFoundError",
    "ConflictError",
    "ContractValidationError",
    "fresh_state",
    "create_state",
    "handle_event",
    "process_event",
]

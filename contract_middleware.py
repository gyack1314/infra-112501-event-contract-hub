"""模块名兼容入口：contract_middleware。"""

from event_contract_middleware import *  # noqa: F401,F403
from event_contract_middleware import ContractHub, EventContractMiddleware, Middleware

__all__ = ["ContractHub", "EventContractMiddleware", "Middleware"]

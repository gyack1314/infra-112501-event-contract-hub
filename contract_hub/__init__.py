"""事件契约协商、严格续投与更正中间件。

该包不依赖第三方库。核心 :class:`ContractHub` 可以嵌入程序，CLI 则提供
可实际操作的登记、发布、轮询、确认、停用、更正和检查入口。
"""

from .core import (
    ContractHub,
    HubError,
    NotFoundError,
    ConflictError,
    ContractValidationError,
    fresh_state,
)

# 兼容更贴近任务描述的命名。
Middleware = ContractHub
EventContractMiddleware = ContractHub
Hub = ContractHub

__all__ = [
    "ContractHub",
    "Middleware",
    "EventContractMiddleware",
    "Hub",
    "HubError",
    "NotFoundError",
    "ConflictError",
    "ContractValidationError",
    "fresh_state",
]

__version__ = "1.0.0"

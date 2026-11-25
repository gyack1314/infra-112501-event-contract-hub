"""顶层命令入口：``python main.py ...`` 等价于 ``python -m contract_hub ...``。"""

from contract_hub import (
    ConflictError,
    ContractHub,
    EventContractMiddleware,
    Hub,
    HubError,
    NotFoundError,
)
from contract_hub.cli import main

Middleware = ContractHub

if __name__ == "__main__":
    raise SystemExit(main())

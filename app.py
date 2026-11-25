"""常见应用入口命名兼容。"""

from contract_hub import ContractHub, EventContractMiddleware
from contract_hub.cli import main

__all__ = ["ContractHub", "EventContractMiddleware", "main"]


if __name__ == "__main__":
    raise SystemExit(main())
